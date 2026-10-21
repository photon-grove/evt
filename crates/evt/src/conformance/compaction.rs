//! The journal compaction cases (ADR 0005, X2): the snapshot and the `410 Gone` handoff, canonical
//! chunks, the lease with takeover and force release, rebuild and epoch fencing, the allocation
//! floor, and the refusals. Every case namespaces its scopes and streams, so a durable table can
//! be shared across runs.

use std::time::Duration;

use super::*;
use crate::features::{JOURNAL_COMPACTION_FEATURE, JOURNAL_COMPACTION_VERSION};
use crate::journal::{
    ChunkBuilder, CompactionStore, CompactionWrite, CompactionWritten, Expect, Journal,
    JournalRepository, JournalRow, LEASE_SORT_KEY, LeaseRow, MANIFEST_SORT_KEY, Scope, allocations,
    begin_compaction, chunk_sk, compact, compact_with_lease, compaction_pk, compaction_view,
    end_compaction, force_release_compaction, page, page_at_epoch, read_audit, read_chunk,
    read_floor, read_lease, read_manifest, rebuild_with_lock, renew_compaction, row_sk,
};
use crate::projection::ViewRow;
use crate::rebuild::observe_rebuild_epoch;
use crate::views::ViewRepository;

/// The lease the cases compact under.
const LEASE: Duration = Duration::from_secs(60);

/// Runs the compaction cases against a repository that writes views, with `views` reading and
/// writing the same view table and `clock` the repository's clock (`with_clock`), which the lease
/// cases move forward. Like [`journal_cases`](super::journal_cases), it panics after running every
/// case if any failed.
///
/// The cases declare `journal` and `journal-compaction` in the feature manifest, and take and
/// release the table's rebuild lock, so run them on a table you can reset and nothing else is
/// rebuilding.
pub async fn compaction_cases<R, V>(repo: &R, views: &V, clock: &TestClock)
where
    R: Repository + CompactionStore + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let mut failures = Vec::new();

    let mut record = |name: &str, result: CaseResult| match result {
        Ok(()) => println!("ok   {name}"),
        Err(Failure(message)) => {
            println!("FAIL {name}");
            failures.push(format!("{name}: {message}"));
        }
    };

    record(
        "a compaction snapshots the prefix, deletes its rows, and hands readers below it a 410",
        Box::pin(snapshot_and_gone(repo, views)).await,
    );
    record(
        "chunks are canonical: compacting in steps stores what one compaction stores",
        Box::pin(chunks_are_canonical(repo)).await,
    );
    record(
        "a rebuild restores compacted rows, readers are served them, and compacting again prunes them",
        Box::pin(rebuild_restores_rows(repo, views)).await,
    );
    record(
        "a lease excludes others until it lapses or is force-released, and a lost lease writes nothing",
        Box::pin(lease_lifecycle(repo, clock)).await,
    );
    record(
        "compaction is fenced on the rebuild epoch",
        Box::pin(epoch_fencing(repo)).await,
    );
    record(
        "a lost counter allocates above the floor, never at a compacted position",
        Box::pin(allocation_respects_the_floor(repo, views)).await,
    );
    record(
        "a writer whose counter read went stale cannot reallocate a compacted position",
        Box::pin(stale_writers_fail_on_the_counter(repo)).await,
    );
    record(
        "a compaction past the watermark, below the floor, or over a gap is refused before any write",
        Box::pin(refusals(repo, views)).await,
    );
    record(
        "a gap after chunks that would close is refused before any chunk is written",
        Box::pin(gapped_range_writes_nothing(repo, views)).await,
    );
    record(
        "a compaction row the engine did not write is refused, never trusted",
        Box::pin(foreign_rows_are_refused(repo, views)).await,
    );
    record(
        "a chunk read for a superseded manifest is reported moved",
        Box::pin(moved_snapshot(repo)).await,
    );

    if !failures.is_empty() {
        panic!(
            "{} compaction case(s) failed:\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }
}

fn world(ns: &str) -> Scope {
    Scope::new(format!("{ns}-world")).expect("a valid scope")
}

/// A journal over the namespace's `ledger` type, every event in the namespace's `world` scope.
fn journal(ns: &str) -> Journal {
    let scope = world(ns);

    Journal::new([format!("{ns}-ledger")], move |_: &SerializedEvent| {
        vec![scope.clone()]
    })
    .expect("a journal")
    .with_retry(RetryPolicy::immediate(4))
}

/// Commits `count` events, one per commit, into the world scope, and returns the live rows.
async fn fill<R>(repo: &R, ns: &str, count: i64) -> Result<Vec<JournalRow>, Failure>
where
    R: Repository + CompactionStore + Clone,
    R::Operation: Clone,
{
    let journaled = JournalRepository::new(repo.clone(), journal(ns));
    let stream = Stream::new(ns, "a", "ledger");

    for sequence in 1..=count {
        stream
            .commit(&journaled, stream.events(sequence..=sequence), None)
            .await?;
    }

    Ok(repo.read_rows(&world(ns), 0, 1000).await?)
}

fn positions(rows: &[JournalRow]) -> Vec<u64> {
    rows.iter().map(|row| row.position).collect()
}

fn owner(ns: &str) -> String {
    format!("{ns}-compactor")
}

async fn snapshot_and_gone<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + CompactionStore + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let scope = world(&ns);
    let live = fill(repo, &ns, 6).await?;

    ensure_eq!(read_floor(repo, &scope).await?, 0, "an uncompacted floor");

    let report = compact_with_lease(repo, &scope, 4, &owner(&ns), LEASE).await?;
    ensure_eq!(
        (
            report.checkpoint,
            report.chunk_count,
            report.chunks_written,
            report.rows_deleted
        ),
        (4, 1, 1, 4),
        "the report"
    );

    // The snapshot: one chunk holding the live rows 1..=4, byte for byte.
    let manifest = read_manifest(repo, &scope)
        .await?
        .ok_or(Failure("no manifest".into()))?;
    ensure_eq!(manifest.checkpoint, 4, "the floor");
    ensure_eq!(manifest.digest(), report.digest, "the reported digest");
    let chunk = read_chunk(repo, &manifest, 0).await?;
    ensure_eq!((chunk.from, chunk.through), (1, 4), "the chunk's range");
    ensure_eq!(
        chunk.rows,
        live[..4].to_vec(),
        "the chunk holds the live rows"
    );

    // The rows at or below the floor are gone; the rows above it are untouched.
    ensure_eq!(
        positions(&repo.read_rows(&scope, 0, 100).await?),
        vec![5, 6],
        "the remaining rows"
    );
    let stored = views
        .get_view_at(&compaction_pk(&scope), MANIFEST_SORT_KEY)
        .await?
        .ok_or(Failure("the manifest row".into()))?;
    ensure_eq!(stored.entity_id, "SYSTEM#JOURNAL", "the manifest's owner");
    ensure_eq!(
        stored.entity_type,
        "journal-compaction",
        "the manifest's type"
    );

    // A reader below the floor is handed the snapshot, not a gap.
    for after in [0, 2, 3] {
        match page(repo, &scope, after, 10).await {
            Err(Error::JournalGone(gone)) => {
                ensure_eq!(
                    (
                        gone.after,
                        gone.checkpoint,
                        gone.chunk_count,
                        gone.digest.clone()
                    ),
                    (after, 4, 1, manifest.digest()),
                    "the handoff"
                );
                ensure_eq!(gone.resume_after(), 4, "the resume cursor");
            }
            other => return Err(Failure(format!("a page after {after}: got {other:?}"))),
        }
    }
    expect_err(
        page_at_epoch(repo, &scope, 2, 10).await,
        "an epoch-checked page below the floor",
        |err| matches!(err, Error::JournalGone(_)),
    )?;

    // Resuming from the checkpoint reads the live tail with no gap.
    let resumed = page(repo, &scope, 4, 10).await?;
    ensure_eq!(positions(&resumed.rows), vec![5, 6], "the resumed page");
    ensure!(resumed.is_caught_up(), "{resumed:?}");
    ensure_eq!(
        repo.read_counter(&scope).await?,
        6,
        "the watermark is untouched"
    );

    // The audit row, the released lease, and the feature marker.
    let audit = read_audit(repo, &scope, 4)
        .await?
        .ok_or(Failure("no audit row".into()))?;
    ensure_eq!(
        (
            audit.checkpoint,
            audit.digest.clone(),
            audit.chunks,
            audit.deleted,
            audit.owner.clone()
        ),
        (4, manifest.digest(), 1, 4, owner(&ns)),
        "the audit row"
    );
    ensure!(
        matches!(
            read_lease(repo, &scope).await?,
            Some(LeaseRow::Released { token: 1, .. })
        ),
        "the lease is released"
    );
    let marker = repo
        .read_manifest()
        .await?
        .ok_or(Failure("no feature manifest".into()))?;
    ensure_eq!(
        marker.features.get(JOURNAL_COMPACTION_FEATURE),
        Some(&JOURNAL_COMPACTION_VERSION),
        "the declaration"
    );

    // Compacting the same checkpoint again is a no-op that rewrites the audit row.
    let again = compact_with_lease(repo, &scope, 4, &owner(&ns), LEASE).await?;
    ensure_eq!(
        (again.chunks_written, again.rows_deleted, again.digest),
        (0, 0, manifest.digest()),
        "a repeated run"
    );

    // And a later checkpoint extends the snapshot.
    let further = compact_with_lease(repo, &scope, 6, &owner(&ns), LEASE).await?;
    ensure_eq!(
        (further.chunk_count, further.rows_deleted),
        (1, 2),
        "the extension"
    );
    let extended = read_manifest(repo, &scope)
        .await?
        .ok_or(Failure("no manifest".into()))?;
    ensure_eq!(
        read_chunk(repo, &extended, 0).await?.rows,
        live,
        "every row is in the snapshot"
    );
    let caught_up = page(repo, &scope, 6, 10).await?;
    ensure!(
        caught_up.rows.is_empty() && caught_up.is_caught_up(),
        "a reader at the checkpoint is caught up: {caught_up:?}"
    );

    Ok(())
}

async fn chunks_are_canonical<R>(repo: &R) -> CaseResult
where
    R: Repository + CompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let scope = world(&ns);
    // Rows of about 133,000 bytes, so a chunk holds two.
    let journaled = JournalRepository::new(repo.clone(), journal(&ns).with_max_row_bytes(200_000)?);
    let stream = Stream::new(&ns, "a", "ledger");
    for sequence in 1..=7 {
        let mut event = stream.event(sequence, sequence);
        event.payload = format!(
            r#"{{"amount":{sequence},"note":"{}"}}"#,
            "x".repeat(100_000)
        )
        .into_bytes();
        stream.commit(&journaled, vec![event], None).await?;
    }
    let live = repo.read_rows(&scope, 0, 100).await?;

    // In steps: through 3 is chunks [1,2] [3]; through 7 grows the tail to [3,4] and adds
    // [5,6] [7].
    let first = compact_with_lease(repo, &scope, 3, &owner(&ns), LEASE).await?;
    ensure_eq!(
        (first.chunk_count, first.chunks_written),
        (2, 2),
        "through 3"
    );
    let second = compact_with_lease(repo, &scope, 7, &owner(&ns), LEASE).await?;
    ensure_eq!(
        (second.chunk_count, second.chunks_written),
        (4, 3),
        "through 7"
    );

    // One compaction through 7, computed from the live rows, is the same snapshot.
    let mut builder = ChunkBuilder::new(&scope, 0, 1);
    let mut expected = Vec::new();
    for row in &live {
        expected.extend(builder.push(row)?);
    }
    expected.push(builder.build());

    let manifest = read_manifest(repo, &scope)
        .await?
        .ok_or(Failure("no manifest".into()))?;
    ensure_eq!(
        manifest.chunks,
        expected
            .iter()
            .map(|chunk| chunk.digest.clone())
            .collect::<Vec<_>>(),
        "the chunk digests"
    );
    for chunk in &expected {
        let stored = repo
            .read_compaction_row(&scope, &chunk_sk(chunk.index))
            .await?
            .ok_or(Failure(format!("chunk {}", chunk.index)))?;
        ensure!(
            stored.payload == chunk.payload,
            "chunk {} differs",
            chunk.index
        );
        let read = read_chunk(repo, &manifest, chunk.index as usize).await?;
        ensure_eq!(
            (read.from, read.through),
            (chunk.from, chunk.through),
            "a chunk's range"
        );
    }
    ensure_eq!(
        expected
            .iter()
            .map(|chunk| (chunk.from, chunk.through))
            .collect::<Vec<_>>(),
        vec![(1, 2), (3, 4), (5, 6), (7, 7)],
        "the canonical boundaries"
    );

    Ok(())
}

async fn rebuild_restores_rows<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + CompactionStore + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let scope = world(&ns);
    let live = fill(repo, &ns, 5).await?;
    compact_with_lease(repo, &scope, 3, &owner(&ns), LEASE).await?;
    let manifest = read_manifest(repo, &scope).await?;

    // The rebuild derives every row from the stored events, the compacted ones included, and leaves
    // the compaction partition alone.
    let events = repo.events(&Stream::new(&ns, "a", "ledger").id).await?;
    rebuild_with_lock(repo, &events, LEASE).await?;
    ensure_eq!(
        repo.read_rows(&scope, 0, 100).await?,
        live,
        "the restored rows"
    );
    ensure_eq!(
        read_manifest(repo, &scope).await?,
        manifest,
        "the manifest stays"
    );

    // A reader below the floor is served the restored rows, which are the snapshot's rows.
    let served = page(repo, &scope, 1, 10).await?;
    ensure_eq!(
        positions(&served.rows),
        vec![2, 3, 4, 5],
        "a page over restored rows"
    );

    // A row missing below the floor is a gap the floor explains: gone.
    views.delete_view(&scope.row_pk(), &row_sk(2)).await?;
    expect_err(
        page(repo, &scope, 1, 10).await,
        "a page whose gap is below the floor",
        |err| matches!(err, Error::JournalGone(gone) if gone.checkpoint == 3),
    )?;

    // Compacting at the floor prunes the restored rows without building anything.
    let pruned = compact_with_lease(repo, &scope, 3, &owner(&ns), LEASE).await?;
    ensure_eq!(
        (pruned.chunks_written, pruned.rows_deleted),
        (0, 2),
        "the prune"
    );
    ensure_eq!(
        positions(&repo.read_rows(&scope, 0, 100).await?),
        vec![4, 5],
        "the rows above the floor"
    );

    Ok(())
}

async fn lease_lifecycle<R>(repo: &R, clock: &TestClock) -> CaseResult
where
    R: Repository + CompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let scope = world(&ns);
    fill(repo, &ns, 4).await?;
    let (a, b, c) = (format!("{ns}-a"), format!("{ns}-b"), format!("{ns}-c"));

    let held = begin_compaction(repo, &scope, &a, Duration::from_secs(10)).await?;
    ensure_eq!(held.token, 1, "the first token");
    expect_err(
        begin_compaction(repo, &scope, &b, LEASE).await,
        "a second compaction of the scope",
        |err| {
            matches!(err, Error::CompactionLeaseHeld { owner, lease_expires_at, .. }
                if *owner == a && *lease_expires_at == held.lease_expires_at)
        },
    )?;

    // Renewing extends it; the old lease value no longer names the hold.
    clock.advance(1);
    let renewed = renew_compaction(repo, &held, Duration::from_secs(10)).await?;
    expect_err(
        compact(repo, &mut held.clone(), 2).await,
        "a compaction under the lease as it was before a renew",
        |err| matches!(err, Error::CompactionLeaseLost { .. }),
    )?;

    // Lapsed, it is taken over with a new token, and the old holder writes nothing.
    clock.advance(11);
    let taken = begin_compaction(repo, &scope, &b, LEASE).await?;
    ensure_eq!(taken.token, 2, "the takeover's token");
    for (what, result) in [
        (
            "compact",
            compact(repo, &mut renewed.clone(), 2).await.map(|_| ()),
        ),
        ("end", end_compaction(repo, &renewed).await),
        (
            "renew",
            renew_compaction(repo, &renewed, LEASE).await.map(|_| ()),
        ),
    ] {
        expect_err(result, &format!("a superseded lease's {what}"), |err| {
            matches!(err, Error::CompactionLeaseLost { .. })
        })?;
    }
    ensure_eq!(
        read_manifest(repo, &scope).await?,
        None,
        "the lost lease wrote nothing"
    );
    ensure_eq!(
        repo.read_rows(&scope, 0, 10).await?.len(),
        4,
        "no row was deleted"
    );

    // The operator force-releases the holder by name; the holder's next write fails, and another
    // compaction can begin at once.
    expect_err(
        force_release_compaction(repo, &scope, &a).await,
        "a force-release naming an owner that does not hold the lease",
        |err| matches!(err, Error::CompactionLeaseLost { .. }),
    )?;
    force_release_compaction(repo, &scope, &b).await?;
    expect_err(
        compact(repo, &mut taken.clone(), 2).await,
        "a force-released lease's compaction",
        |err| matches!(err, Error::CompactionLeaseLost { .. }),
    )?;
    let third = begin_compaction(repo, &scope, &c, LEASE).await?;
    ensure_eq!(third.token, 3, "the next token");

    // A lease that lapsed by the repository's clock stops its own run before writing.
    clock.advance(61);
    expect_err(
        compact(repo, &mut third.clone(), 2).await,
        "a lapsed lease's compaction",
        |err| matches!(err, Error::CompactionLeaseLost { .. }),
    )?;
    let report = compact(repo, &mut renew_compaction(repo, &third, LEASE).await?, 2).await?;
    ensure_eq!(report.checkpoint, 2, "a renewed lapsed lease compacts");

    // A released lease cannot be released again.
    let renewed = LeaseRow::decode(
        &scope,
        &repo
            .read_compaction_row(&scope, LEASE_SORT_KEY)
            .await?
            .ok_or(Failure("the lease row".into()))?
            .payload,
    )?;
    let LeaseRow::Held(current) = renewed else {
        return Err(Failure("the lease should be held".into()));
    };
    end_compaction(repo, &current).await?;
    expect_err(
        end_compaction(repo, &current).await,
        "a second release",
        |err| matches!(err, Error::CompactionLeaseLost { .. }),
    )?;

    // A run renews a lease with less than half of its length left, in place, so the caller
    // releases the lease the run ended with.
    let mut short = begin_compaction(repo, &scope, &c, Duration::from_secs(10)).await?;
    clock.advance(6);
    compact(repo, &mut short, 3).await?;
    ensure_eq!(
        short.lease_expires_at,
        clock.seconds() + 10,
        "the run renewed the lease"
    );
    end_compaction(repo, &short).await?;

    Ok(())
}

async fn epoch_fencing<R>(repo: &R) -> CaseResult
where
    R: Repository + CompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let scope = world(&ns);
    fill(repo, &ns, 4).await?;
    let mut held = begin_compaction(repo, &scope, &owner(&ns), LEASE).await?;

    // While a rebuild holds the fence, compaction does not start, and neither does a lease.
    let rebuild = repo.begin_rebuild(&format!("{ns}-rebuild"), LEASE).await?;
    expect_err(
        compact(repo, &mut held.clone(), 2).await,
        "a compaction during a rebuild",
        |err| matches!(err, Error::RebuildUnavailable { .. }),
    )?;
    expect_err(
        begin_compaction(repo, &scope, &format!("{ns}-other"), LEASE).await,
        "a lease during a rebuild",
        |err| matches!(err, Error::RebuildUnavailable { .. }),
    )?;
    repo.end_rebuild(&rebuild).await?;

    // A write made under an epoch a rebuild has since moved writes nothing.
    let epoch = observe_rebuild_epoch(repo).await?;
    let rebuild = repo.begin_rebuild(&format!("{ns}-rebuild"), LEASE).await?;
    repo.end_rebuild(&rebuild).await?;
    let probe = compaction_view(&scope, "probe", b"{}".to_vec());
    ensure_eq!(
        repo.write_compaction(
            Some(epoch),
            &[CompactionWrite::Put {
                row: probe.clone(),
                expect: Expect::Any
            }]
        )
        .await?,
        CompactionWritten::EpochChanged,
        "a write under a stale epoch"
    );
    ensure_eq!(
        repo.read_compaction_row(&scope, "probe").await?,
        None,
        "the fenced write wrote nothing"
    );

    // A failed condition names its write and writes nothing either.
    let current = observe_rebuild_epoch(repo).await?;
    ensure_eq!(
        repo.write_compaction(
            Some(current),
            &[
                CompactionWrite::Put {
                    row: probe.clone(),
                    expect: Expect::Any
                },
                CompactionWrite::Check {
                    pk: compaction_pk(&scope),
                    sk: LEASE_SORT_KEY.into(),
                    expect: Expect::Absent
                },
            ]
        )
        .await?,
        CompactionWritten::Refused(1),
        "a write whose check fails"
    );
    ensure_eq!(
        repo.read_compaction_row(&scope, "probe").await?,
        None,
        "the refused write wrote nothing"
    );

    // Under the current epoch the lease's run completes.
    let report = compact(repo, &mut held, 2).await?;
    ensure_eq!(report.rows_deleted, 2, "the run");
    end_compaction(repo, &held).await?;

    Ok(())
}

async fn allocation_respects_the_floor<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + CompactionStore + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let scope = world(&ns);
    fill(repo, &ns, 6).await?;
    compact_with_lease(repo, &scope, 6, &owner(&ns), LEASE).await?;

    // Every row is compacted and the counter is lost: without the floor a commit would allocate
    // position 1 again, which no row guards any more.
    views.delete_view(&scope.counter_pk(), "COUNTER").await?;

    let journaled = JournalRepository::new(repo.clone(), journal(&ns));
    let stream = Stream::new(&ns, "a", "ledger");
    stream
        .commit(&journaled, stream.events(7..=7), None)
        .await?;

    let stored = repo.events(&stream.id).await?;
    let held = allocations(&stored[6].metadata)?;
    ensure_eq!(held.len(), 1, "one allocation");
    ensure_eq!(held[0].position, 7, "the position after the floor");
    ensure_eq!(
        repo.read_counter(&scope).await?,
        7,
        "the counter is written back"
    );
    let resumed = page(repo, &scope, 6, 10).await?;
    ensure_eq!(positions(&resumed.rows), vec![7], "the new row");

    Ok(())
}

async fn stale_writers_fail_on_the_counter<R>(repo: &R) -> CaseResult
where
    R: Repository + CompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let scope = world(&ns);
    fill(repo, &ns, 4).await?;
    compact_with_lease(repo, &scope, 4, &owner(&ns), LEASE).await?;

    // A writer that read the counter at 2, before the commits of 3 and 4 and the compaction that
    // deleted their rows. Its rows at 3 would be absent now: only the counter's condition (the
    // stored counter is 4, not the 2 it read) refuses the stale attempt, which then reads again.
    let lagging = super::journal::Lagging::new(repo.clone(), 1).stale_at(2);
    let journaled = JournalRepository::new(lagging.clone(), journal(&ns));
    let stream = Stream::new(&ns, "a", "ledger");
    stream
        .commit(&journaled, stream.events(5..=5), None)
        .await?;

    let stored = repo.events(&stream.id).await?;
    let held = allocations(&stored[4].metadata)?;
    ensure_eq!(held[0].position, 5, "the position after the live counter");
    ensure!(lagging.backoffs() >= 1, "the stale attempt was retried");
    ensure_eq!(repo.read_counter(&scope).await?, 5, "the counter");

    let positions_held: Vec<u64> = stored
        .iter()
        .map(|event| allocations(&event.metadata).map(|held| held[0].position))
        .collect::<Result<_, _>>()?;
    ensure_eq!(
        positions_held,
        vec![1, 2, 3, 4, 5],
        "no position is held twice"
    );

    Ok(())
}

async fn refusals<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + CompactionStore + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let scope = world(&ns);
    fill(repo, &ns, 5).await?;
    let mut held = begin_compaction(repo, &scope, &owner(&ns), LEASE).await?;

    for through in [0, 6] {
        expect_err(
            compact(repo, &mut held.clone(), through).await,
            &format!("a compaction through {through}"),
            |err| matches!(err, Error::InvalidArgument(_)),
        )?;
    }

    // A missing row is a gap compaction cannot snapshot.
    views.delete_view(&scope.row_pk(), &row_sk(2)).await?;
    expect_err(
        compact(repo, &mut held.clone(), 4).await,
        "a compaction over a gap",
        |err| matches!(err, Error::InvalidJournal(_)),
    )?;
    ensure_eq!(
        read_manifest(repo, &scope).await?,
        None,
        "nothing was written"
    );
    ensure_eq!(
        positions(&repo.read_rows(&scope, 0, 10).await?),
        vec![1, 3, 4, 5],
        "nothing was deleted"
    );

    // Above the gap it compacts once the row is back; below the floor it is refused.
    let restored = ViewRow::new(
        scope.row_pk(),
        row_sk(2),
        Stream::new(&ns, "a", "ledger").id.as_str(),
        "journal",
        crate::journal::encode_row(
            &scope,
            2,
            &repo.events(&Stream::new(&ns, "a", "ledger").id).await?[1],
        )?,
    );
    views.put_view(&restored).await?;
    compact(repo, &mut held, 4).await?;
    expect_err(
        compact(repo, &mut held.clone(), 3).await,
        "a compaction below the floor",
        |err| matches!(err, Error::InvalidArgument(_)),
    )?;
    end_compaction(repo, &held).await?;

    Ok(())
}

async fn gapped_range_writes_nothing<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + CompactionStore + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let scope = world(&ns);
    // Rows of about 133,000 bytes, so a chunk holds two: [1,2] and [3,4] close before the gap.
    let journaled = JournalRepository::new(repo.clone(), journal(&ns).with_max_row_bytes(200_000)?);
    let stream = Stream::new(&ns, "a", "ledger");
    for sequence in 1..=6 {
        let mut event = stream.event(sequence, sequence);
        event.payload = format!(
            r#"{{"amount":{sequence},"note":"{}"}}"#,
            "x".repeat(100_000)
        )
        .into_bytes();
        stream.commit(&journaled, vec![event], None).await?;
    }
    views.delete_view(&scope.row_pk(), &row_sk(5)).await?;

    expect_err(
        compact_with_lease(repo, &scope, 6, &owner(&ns), LEASE).await,
        "a compaction over a gap after closed chunks",
        |err| matches!(err, Error::InvalidJournal(_)),
    )?;
    for sort_key in [chunk_sk(0), chunk_sk(1), MANIFEST_SORT_KEY.to_owned()] {
        ensure_eq!(
            repo.read_compaction_row(&scope, &sort_key).await?,
            None,
            format!("{sort_key} was not written")
        );
    }
    ensure_eq!(
        positions(&repo.read_rows(&scope, 0, 10).await?),
        vec![1, 2, 3, 4, 6],
        "no row was deleted"
    );

    Ok(())
}

async fn foreign_rows_are_refused<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + CompactionStore + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let scope = world(&ns);
    let stream = Stream::new(&ns, "a", "ledger");
    fill(repo, &ns, 4).await?;

    // A row at the manifest's key, written by a stream (as an application projector or a direct
    // view write would): it claims the scope is compacted through 4 with a well-formed payload.
    let forged = crate::journal::CompactionManifest {
        scope: scope.clone(),
        checkpoint: 4,
        chunks: vec!["0".repeat(64)],
    };
    let foreign = ViewRow::new(
        compaction_pk(&scope),
        MANIFEST_SORT_KEY,
        stream.id.as_str(),
        "journal-compaction",
        forged.payload(),
    );
    views.put_view(&foreign).await?;

    let refused = |err: &Error| matches!(err, Error::ForeignJournalRow { .. });
    expect_err(
        compact_with_lease(repo, &scope, 4, &owner(&ns), LEASE).await,
        "a compaction over a foreign manifest",
        refused,
    )?;
    expect_err(
        read_manifest(repo, &scope).await,
        "reading a foreign manifest",
        refused,
    )?;
    ensure_eq!(
        positions(&repo.read_rows(&scope, 0, 10).await?),
        vec![1, 2, 3, 4],
        "no row was deleted"
    );

    // A reader that meets a gap does not take a foreign floor for a 410.
    views.delete_view(&scope.row_pk(), &row_sk(1)).await?;
    expect_err(
        page(repo, &scope, 0, 10).await,
        "a page under a foreign manifest",
        refused,
    )?;

    // A foreign lease row is refused the same way.
    views
        .delete_view(&compaction_pk(&scope), MANIFEST_SORT_KEY)
        .await?;
    let lease = ViewRow::new(
        compaction_pk(&scope),
        LEASE_SORT_KEY,
        stream.id.as_str(),
        "journal-compaction",
        crate::journal::released_payload(&scope, 7),
    );
    views.put_view(&lease).await?;
    expect_err(
        begin_compaction(repo, &scope, &owner(&ns), LEASE).await,
        "a lease over a foreign lease row",
        refused,
    )?;

    Ok(())
}

async fn moved_snapshot<R>(repo: &R) -> CaseResult
where
    R: Repository + CompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let scope = world(&ns);
    fill(repo, &ns, 6).await?;

    compact_with_lease(repo, &scope, 3, &owner(&ns), LEASE).await?;
    let old = read_manifest(repo, &scope)
        .await?
        .ok_or(Failure("no manifest".into()))?;
    compact_with_lease(repo, &scope, 6, &owner(&ns), LEASE).await?;

    expect_err(
        read_chunk(repo, &old, 0).await,
        "a chunk of a superseded manifest",
        |err| {
            matches!(err, Error::JournalSnapshotMoved { checkpoint: 3, .. }) && err.is_retryable()
        },
    )?;
    expect_err(
        read_chunk(repo, &old, 1).await,
        "a chunk the manifest does not have",
        |err| matches!(err, Error::InvalidArgument(_)),
    )?;

    Ok(())
}
