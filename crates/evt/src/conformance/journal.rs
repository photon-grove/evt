//! The journal cases (ADR 0005, X1): allocation, counters and rows written in the commit, gap
//! detection, the row-size guard, contention retry, the feature marker, and rebuild determinism.
//! Every case namespaces its scopes, streams, and entity types, so a durable table can be shared
//! across runs.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::features::{JOURNAL_FEATURE, JOURNAL_VERSION};
use crate::journal::{
    ALLOCATION_KEY, Gap, Journal, JournalCompactionStore, JournalRepository, JournalStore, Scope,
    allocations, counter_view, encode_allocations, rebuild_with_lock, replay, row_size, row_sk,
};
use crate::projection::{ProjectedView, ViewOperation, ViewRow, group_commits, replay_views};
use crate::rebuild::RebuildFence;
use crate::repository::TransactionCommit;
use crate::views::ViewRepository;

/// Runs the journal cases against a repository that writes views, with `views` reading and writing
/// the same view table. Like [`outcome_cases`], it panics after running every case if any failed.
///
/// The cases declare the `journal` feature and namespaced history-dependent entity types in the
/// feature manifest and leave them there, so run them on a table you can reset.
pub async fn journal_cases<R, V>(repo: &R, views: &V)
where
    R: Repository + FeatureMarker + JournalCompactionStore + RebuildFence + Clone,
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
        "positions are allocated per scope across streams, with rows, counters, and metadata",
        Box::pin(allocates_and_writes(repo, views)).await,
    );
    record(
        "an allocation a caller forged in metadata is replaced, and uncovered events are untouched",
        Box::pin(forged_allocations(repo)).await,
    );
    record(
        "a multi-stream commit allocates in stream-ID then sequence order",
        Box::pin(multi_stream_commit(repo)).await,
    );
    record(
        "a row over the cap is refused before any write, at the cap it passes, and a pre-check agrees",
        Box::pin(size_guard(repo, views)).await,
    );
    record(
        "a writer that lost a position re-reads the counters and commits at the next one",
        Box::pin(contention_is_retried(repo)).await,
    );
    record(
        "contention that outlasts the policy is a conflict that wrote nothing",
        Box::pin(contention_is_bounded(repo)).await,
    );
    record(
        "a conflict on the stream itself is not retried by the journal, even when a counter moved",
        Box::pin(stream_conflicts_are_not_retried(repo)).await,
    );
    record(
        "a transient failure is retried within the policy and returned when it runs out",
        Box::pin(transient_failures_are_retried(repo)).await,
    );
    record(
        "a counter behind its rows is repaired from the highest row, not wedged",
        Box::pin(counter_behind_rows_is_repaired(repo, views)).await,
    );
    record(
        "a writer whose counter read went stale fails on the counter even where its rows are gone",
        Box::pin(stale_counter_reads_fail_on_the_counter(repo, views)).await,
    );
    record(
        "a repository that does not write views, or ignores the counter's condition, is refused",
        Box::pin(views_unsupported_is_refused(repo)).await,
    );
    record(
        "a commit the repository would refuse declares nothing",
        Box::pin(invalid_commits_declare_nothing(repo)).await,
    );
    record(
        "a full page shows that rows follow it when the counter is missing or behind",
        Box::pin(full_pages_show_more_rows(repo, views)).await,
    );
    record(
        "a commit that cannot be built declares nothing",
        Box::pin(refused_commits_declare_nothing(repo)).await,
    );
    record(
        "pages are contiguous, report the first gap, and never pass it",
        Box::pin(pages_and_gaps(repo, views)).await,
    );
    record(
        "the first journaled commit declares the feature and the history, once",
        Box::pin(declares_feature_and_history(repo)).await,
    );
    record(
        "a rebuild under the lock refuses live commits and reproduces the live journal",
        Box::pin(rebuild_under_the_lock(repo, views)).await,
    );
    record(
        "a failed rebuild that wiped the counters keeps journaled commits refused after a force-release",
        Box::pin(force_released_rebuild_refuses_journaled_commits(
            repo, views,
        ))
        .await,
    );
    record(
        "replaying stored events in any order reproduces the live rows and counters",
        Box::pin(replay_reproduces_the_journal(repo, views)).await,
    );

    if !failures.is_empty() {
        panic!(
            "{} journal case(s) failed:\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }
}

/// The lease the cases rebuild under.
const LEASE: std::time::Duration = std::time::Duration::from_secs(60);

/// The scope of a name in a namespace.
fn scope(ns: &str, name: &str) -> Scope {
    Scope::new(format!("{ns}-{name}")).expect("a valid scope")
}

/// A journal over the namespace's `ledger` type: every event goes to the namespace's `world` scope
/// and to a `place-<stream suffix>` scope of its own stream.
fn journal(ns: &str) -> Journal {
    let ns = ns.to_owned();

    Journal::new([format!("{ns}-ledger")], move |event: &SerializedEvent| {
        let place = event.entity_id.as_str().rsplit('-').next().unwrap_or("");

        vec![scope(&ns, "world"), scope(&ns, &format!("place-{place}"))]
    })
    .expect("a journal")
    .with_retry(RetryPolicy::immediate(4))
}

fn positions(page: &crate::journal::JournalPage) -> Vec<u64> {
    page.rows.iter().map(|row| row.position).collect()
}

async fn allocates_and_writes<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let journaled = JournalRepository::new(repo.clone(), journal(&ns));
    let (a, b) = (
        Stream::new(&ns, "a", "ledger"),
        Stream::new(&ns, "b", "ledger"),
    );
    let (world, place_a, place_b) = (
        scope(&ns, "world"),
        scope(&ns, "place-a"),
        scope(&ns, "place-b"),
    );

    ensure_eq!(
        journaled.high_watermark(&world).await?,
        0,
        "an untouched scope has watermark 0"
    );
    let empty = journaled.page(&world, 0, 10).await?;
    ensure!(
        empty.rows.is_empty() && empty.gap.is_none() && empty.is_caught_up(),
        "an untouched scope pages empty with no gap: {empty:?}"
    );

    a.commit(&journaled, a.events(1..=3), None).await?;
    b.commit(&journaled, b.events(1..=2), None).await?;
    a.commit(&journaled, a.events(4..=4), None).await?;

    ensure_eq!(journaled.high_watermark(&world).await?, 6, "world counter");
    ensure_eq!(
        journaled.high_watermark(&place_a).await?,
        4,
        "place a counter"
    );
    ensure_eq!(
        journaled.high_watermark(&place_b).await?,
        2,
        "place b counter"
    );

    let page = journaled.page(&world, 0, 100).await?;
    ensure_eq!(positions(&page), vec![1, 2, 3, 4, 5, 6], "world positions");
    ensure!(page.is_caught_up(), "world page is caught up: {page:?}");
    ensure_eq!(
        page.rows
            .iter()
            .map(|row| row.event.id.to_string())
            .collect::<Vec<_>>(),
        vec![
            format!("{}:1", a.id),
            format!("{}:2", a.id),
            format!("{}:3", a.id),
            format!("{}:1", b.id),
            format!("{}:2", b.id),
            format!("{}:4", a.id),
        ],
        "world rows are in commit order"
    );
    ensure!(
        page.rows.iter().all(|row| row.owner == row.event.entity_id),
        "every row is owned by its event's stream"
    );

    // The same events, in the place scopes.
    let place = journaled.page(&place_a, 0, 100).await?;
    ensure_eq!(positions(&place), vec![1, 2, 3, 4], "place a positions");

    // The allocations are in the stored events' metadata, and agree with the rows.
    let stored = repo.events(&a.id).await?;
    ensure_eq!(stored.len(), 4, "a's events");
    for (index, event) in stored.iter().enumerate() {
        let held = allocations(&event.metadata)?;
        let by_scope: BTreeMap<_, _> = held
            .iter()
            .map(|allocation| (allocation.scope.clone(), allocation.position))
            .collect();

        let want = index as u64 + 1;

        ensure_eq!(held.len(), 2, "two scopes per event");
        ensure_eq!(
            by_scope.get(place_a.as_str()),
            Some(&want),
            "the place position"
        );
        ensure!(
            event.metadata.extensions[ALLOCATION_KEY] == encode_allocations(&held),
            "the stored value is the canonical encoding"
        );
    }

    // The rows are plain view rows in the views table: partition, 20-digit sort key, owner.
    let row = views
        .get_view_at(&world.row_pk(), &row_sk(4))
        .await?
        .ok_or(Failure("the world row at position 4".into()))?;
    ensure_eq!(row.entity_id, b.id.to_string(), "the row's owner column");
    let counter = views
        .get_view_at(&world.counter_pk(), "COUNTER")
        .await?
        .ok_or(Failure("the world counter row".into()))?;
    ensure_eq!(counter, counter_view(&world, 6), "the stored counter row");

    // A paged read walks the scope.
    let mut after = 0;
    let mut seen = Vec::new();
    loop {
        let page = journaled.page(&world, after, 4).await?;
        seen.extend(positions(&page));
        after = page.next_after();
        if !page.has_more() {
            break;
        }
    }
    ensure_eq!(
        seen,
        vec![1, 2, 3, 4, 5, 6],
        "paging by 4 reads every row once"
    );

    // Events of an entity type the journal does not cover add nothing.
    let other = Stream::new(&ns, "x", "other");
    other.commit(&journaled, other.events(1..=2), None).await?;
    ensure_eq!(
        journaled.high_watermark(&world).await?,
        6,
        "world is unmoved"
    );
    ensure!(
        repo.events(&other.id)
            .await?
            .iter()
            .all(|event| !event.metadata.extensions.contains_key(ALLOCATION_KEY)),
        "uncovered events carry no allocation"
    );

    Ok(())
}

async fn forged_allocations<R>(repo: &R) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let journaled = JournalRepository::new(repo.clone(), journal(&ns));
    let (covered, uncovered) = (
        Stream::new(&ns, "a", "ledger"),
        Stream::new(&ns, "u", "other"),
    );
    let forged = encode_allocations(&[crate::journal::Allocation::new(&scope(&ns, "forged"), 99)]);

    for stream in [&covered, &uncovered] {
        let mut events = stream.events(1..=1);
        events[0]
            .metadata
            .extensions
            .insert(ALLOCATION_KEY.to_owned(), forged.clone());
        stream.commit(&journaled, events, None).await?;
    }

    let stored = repo.events(&covered.id).await?;
    ensure!(
        !stored[0].metadata.extensions[ALLOCATION_KEY].contains("forged"),
        "the engine's allocation replaced the forged one"
    );
    ensure_eq!(
        journaled.high_watermark(&scope(&ns, "forged")).await?,
        0,
        "no counter for a forged scope"
    );
    ensure!(
        !repo.events(&uncovered.id).await?[0]
            .metadata
            .extensions
            .contains_key(ALLOCATION_KEY),
        "a forged key on an uncovered event is removed"
    );

    Ok(())
}

async fn multi_stream_commit<R>(repo: &R) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let journaled = JournalRepository::new(repo.clone(), journal(&ns));
    let (a, c) = (
        Stream::new(&ns, "a", "ledger"),
        Stream::new(&ns, "c", "ledger"),
    );

    // Given in the opposite order: allocation follows the canonical order, not the given one.
    let commit = |stream: &Stream, range| Commit {
        entity_id: stream.id.clone(),
        events: stream.events(range),
        snapshot: None,
        operations: Vec::new(),
        views: Vec::new(),
        outcome: None,
    };
    journaled
        .commit_transaction(TransactionCommit::new(vec![
            commit(&c, 1..=2),
            commit(&a, 1..=1),
        ]))
        .await?;

    let page = journaled.page(&scope(&ns, "world"), 0, 10).await?;
    ensure_eq!(
        page.rows
            .iter()
            .map(|row| row.event.id.to_string())
            .collect::<Vec<_>>(),
        vec![
            format!("{}:1", a.id),
            format!("{}:1", c.id),
            format!("{}:2", c.id)
        ],
        "stream a before stream c, each in sequence order"
    );
    ensure_eq!(positions(&page), vec![1, 2, 3], "contiguous positions");

    // A fence-only or event-less transaction commits without touching the journal.
    journaled
        .commit_transaction(TransactionCommit::new(Vec::new()))
        .await?;
    ensure_eq!(
        journaled.high_watermark(&scope(&ns, "world")).await?,
        3,
        "an empty transaction moves nothing"
    );

    Ok(())
}

async fn size_guard<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let reference = JournalRepository::new(repo.clone(), journal(&ns));
    let s1 = Stream::new(&ns, "s1", "ledger");

    // Learn the size of the widest row of an event this shape of stream writes.
    s1.commit(&reference, s1.events(1..=1), None).await?;
    let world = scope(&ns, "world");
    let stored = views
        .get_view_at(&world.row_pk(), &row_sk(1))
        .await?
        .ok_or(Failure("the first world row".into()))?;
    let place = scope(&ns, "place-s1");
    let place_row = views
        .get_view_at(&place.row_pk(), &row_sk(1))
        .await?
        .ok_or(Failure("the first place row".into()))?;
    let widest = stored.payload.len().max(place_row.payload.len());
    ensure_eq!(
        row_size(&world, 1, &repo.events(&s1.id).await?[0])?,
        stored.payload.len(),
        "the guard's measure is the stored payload length"
    );

    // The same shape of event on another stream of the same name length, at position 1 of its own
    // scopes... the shared `world` scope is at 2 now, which is the same width.
    let at_cap = |name: &str, cap: usize| {
        (
            Stream::new(&ns, name, "ledger"),
            JournalRepository::new(repo.clone(), journal(&ns).with_max_row_bytes(cap).unwrap()),
        )
    };

    let (s2, exactly) = at_cap("s2", widest);
    s2.commit(&exactly, s2.events(1..=1), None).await?;
    ensure_eq!(
        exactly.high_watermark(&world).await?,
        2,
        "a row exactly at the cap is written"
    );

    let (s3, under) = at_cap("s3", widest - 1);
    let refused = s3.commit(&under, s3.events(1..=1), None).await;
    let Err(Error::JournalRowTooLarge(row)) = refused else {
        return Err(Failure(format!(
            "one byte over the cap must be refused: {refused:?}"
        )));
    };
    ensure_eq!(
        (row.size, row.limit, row.event_id.clone()),
        (widest, widest - 1, format!("{}:1", s3.id)),
        "the refusal names the widest row"
    );
    ensure!(
        row.scope == world.as_str() || row.scope.contains("place-s3"),
        "the refusal names a scope of the event: {}",
        row.scope
    );
    ensure!(
        repo.events(&s3.id).await?.is_empty(),
        "a refused commit wrote no event"
    );
    ensure_eq!(
        under.high_watermark(&world).await?,
        2,
        "a refused commit moved no counter"
    );
    ensure_eq!(
        under.high_watermark(&scope(&ns, "place-s3")).await?,
        0,
        "a refused commit moved no counter of its own scope"
    );

    // The pre-check measures every row at the widest position, so it is never more lenient than
    // the guard: a cap it passes is a cap the commit passes.
    let events = Stream::new(&ns, "s5", "ledger").events(1..=1);
    let Err(Error::JournalRowTooLarge(measured)) = journal(&ns)
        .with_max_row_bytes(0)
        .unwrap()
        .pre_check(&events)
    else {
        return Err(Failure("a zero cap refuses every row".into()));
    };
    ensure_eq!(measured.position, u64::MAX, "the pre-check's position");
    ensure!(
        measured.size > widest,
        "the widest position makes the widest row: {} > {widest}",
        measured.size
    );
    ensure!(
        journal(&ns)
            .with_max_row_bytes(measured.size)
            .unwrap()
            .pre_check(&events)
            .is_ok(),
        "a cap at the measured size passes the pre-check"
    );
    let result = journal(&ns)
        .with_max_row_bytes(measured.size - 1)
        .unwrap()
        .pre_check(&events);
    let Err(Error::JournalRowTooLarge(row)) = result else {
        return Err(Failure(format!(
            "a cap one under the measured size is refused: {result:?}"
        )));
    };
    ensure_eq!(row.size, measured.size, "the refused row is the widest");
    let rejection = row.to_rejection();
    ensure_eq!(
        rejection.code.as_str(),
        "journal-row-too-large",
        "the application's rejection"
    );

    let (s5, passing) = at_cap("s5", measured.size);
    s5.commit(&passing, s5.events(1..=1), None).await?;
    ensure_eq!(
        passing.high_watermark(&world).await?,
        3,
        "a commit under a cap its pre-check passed is written"
    );

    // An event wider than any cap is refused whole, even where another scope would have fit.
    let big = Stream::new(&ns, "s4", "ledger");
    let mut events = big.events(1..=1);
    events[0].payload = format!(r#"{{"amount":1,"note":"{}"}}"#, "x".repeat(40_000)).into_bytes();
    let default = JournalRepository::new(repo.clone(), journal(&ns));
    expect_err(
        big.commit(&default, events, None).await,
        "an event over the default cap",
        |err| matches!(err, Error::JournalRowTooLarge(_)),
    )?;
    ensure!(
        repo.events(&big.id).await?.is_empty(),
        "the oversized event was not written"
    );

    Ok(())
}

/// Takes one from a counter that counts down to zero: `true` while it had one to give.
fn take(counter: &AtomicUsize) -> bool {
    let mut left = counter.load(Ordering::SeqCst);

    loop {
        if left == 0 {
            return false;
        }

        match counter.compare_exchange(left, left - 1, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(current) => left = current,
        }
    }
}

/// A repository whose journal reads are stale for a number of counter reads (the position scan
/// that backs repair is stale for as long), standing in for a writer that read the counters just
/// before another writer committed, and whose next commits can fail transiently. It counts
/// backoffs.
#[derive(Clone)]
pub(super) struct Lagging<R> {
    inner: R,
    stale_reads: Arc<AtomicUsize>,
    /// The counter a stale read returns.
    stale_counter: u64,
    transient_commits: Arc<AtomicUsize>,
    backoffs: Arc<AtomicUsize>,
    writes_views: bool,
    applies_expected_payload: bool,
}

impl<R> Lagging<R> {
    pub(super) fn new(inner: R, stale_reads: usize) -> Self {
        Self {
            inner,
            stale_reads: Arc::new(AtomicUsize::new(stale_reads)),
            stale_counter: 0,
            transient_commits: Arc::new(AtomicUsize::new(0)),
            backoffs: Arc::new(AtomicUsize::new(0)),
            writes_views: true,
            applies_expected_payload: true,
        }
    }

    /// Reports that the repository does not write view operations, as a custom backend written
    /// before they existed does.
    fn without_views(mut self) -> Self {
        self.writes_views = false;

        self
    }

    /// Reports that the repository writes views but ignores the expected-payload condition, as a
    /// third-party backend written before it existed does.
    fn ignoring_expected_payload(mut self) -> Self {
        self.applies_expected_payload = false;

        self
    }

    /// The next `commits` commits fail with a transient error without being applied.
    fn failing(self, commits: usize) -> Self {
        self.transient_commits.store(commits, Ordering::SeqCst);

        self
    }

    /// A stale counter read returns `counter` instead of 0: a writer that read the counter some
    /// commits ago.
    pub(super) fn stale_at(mut self, counter: u64) -> Self {
        self.stale_counter = counter;

        self
    }

    pub(super) fn backoffs(&self) -> usize {
        self.backoffs.load(Ordering::SeqCst)
    }

    fn stale(&self) -> bool {
        self.stale_reads.load(Ordering::SeqCst) > 0
    }
}

impl<R: JournalStore> JournalStore for Lagging<R> {
    async fn read_counter(&self, scope: &Scope) -> Result<u64, Error> {
        if take(&self.stale_reads) {
            return Ok(self.stale_counter);
        }

        self.inner.read_counter(scope).await
    }

    async fn read_last_position(&self, scope: &Scope) -> Result<u64, Error> {
        if self.stale() {
            return Ok(0);
        }

        self.inner.read_last_position(scope).await
    }

    fn read_rows(
        &self,
        scope: &Scope,
        after: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<crate::journal::JournalRow>, Error>> + Send {
        self.inner.read_rows(scope, after, limit)
    }
}

impl<R: JournalCompactionStore> JournalCompactionStore for Lagging<R> {
    fn read_compaction_row(
        &self,
        scope: &Scope,
        sort_key: &str,
    ) -> impl Future<Output = Result<Option<ViewRow>, Error>> + Send {
        self.inner.read_compaction_row(scope, sort_key)
    }
}

impl<R: FeatureMarker> FeatureMarker for Lagging<R> {
    fn read_manifest(
        &self,
    ) -> impl Future<Output = Result<Option<features::FeatureManifest>, Error>> + Send {
        self.inner.read_manifest()
    }

    fn update_manifest(
        &self,
        change: impl Fn(&mut features::FeatureManifest) + Send,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        self.inner.update_manifest(change)
    }
}

impl<R: Repository> Repository for Lagging<R> {
    type Operation = R::Operation;

    fn commit(
        &self,
        commit: Commit<Self::Operation>,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        let fail = take(&self.transient_commits);

        async move {
            if fail {
                return Err(Error::Transient("an injected transient failure".into()));
            }

            self.inner.commit(commit).await
        }
    }

    fn commit_transaction(
        &self,
        commit: TransactionCommit<Self::Operation>,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        self.inner.commit_transaction(commit)
    }

    fn writes_views(&self) -> bool {
        self.writes_views && self.inner.writes_views()
    }

    fn applies_expected_payload(&self) -> bool {
        self.applies_expected_payload && self.inner.applies_expected_payload()
    }

    fn declare_history_dependent(
        &self,
        types: &[EntityType],
    ) -> impl Future<Output = Result<(), Error>> + Send {
        self.inner.declare_history_dependent(types)
    }

    fn backoff(&self, _: std::time::Duration) -> impl Future<Output = ()> + Send {
        self.backoffs.fetch_add(1, Ordering::SeqCst);

        async {}
    }

    fn outcome(
        &self,
        command_id: &CommandId,
    ) -> impl Future<Output = Result<Option<Outcome>, Error>> + Send {
        self.inner.outcome(command_id)
    }

    fn events(
        &self,
        entity_id: &EntityId,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, Error>> + Send {
        self.inner.events(entity_id)
    }

    fn events_after(
        &self,
        entity_id: &EntityId,
        after: EventSequence,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, Error>> + Send {
        self.inner.events_after(entity_id, after)
    }

    fn latest(
        &self,
        entity_id: &EntityId,
        n: usize,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, Error>> + Send {
        self.inner.latest(entity_id, n)
    }

    fn snapshot(
        &self,
        entity_id: &EntityId,
    ) -> impl Future<Output = Result<Option<SerializedSnapshot>, Error>> + Send {
        self.inner.snapshot(entity_id)
    }

    fn put_snapshot(
        &self,
        snapshot: &SerializedSnapshot,
    ) -> impl Future<Output = Result<bool, Error>> + Send {
        self.inner.put_snapshot(snapshot)
    }

    fn compact_below(
        &self,
        entity_id: &EntityId,
        through: EventSequence,
    ) -> impl Future<Output = Result<usize, Error>> + Send {
        self.inner.compact_below(entity_id, through)
    }

    fn entity_heads(
        &self,
        entity_type: Option<&EntityType>,
    ) -> impl Future<Output = Result<BTreeMap<EntityId, EventSequence>, Error>> + Send {
        self.inner.entity_heads(entity_type)
    }
}

async fn contention_is_retried<R>(repo: &R) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let (rival, loser) = (
        Stream::new(&ns, "a", "ledger"),
        Stream::new(&ns, "b", "ledger"),
    );
    let world = scope(&ns, "world");

    // The rival commits world position 1 and 2; the loser's first counter read is stale.
    let winner = JournalRepository::new(repo.clone(), journal(&ns));
    rival.commit(&winner, rival.events(1..=2), None).await?;

    let lagging = Lagging::new(repo.clone(), 2);
    let late = JournalRepository::new(lagging.clone(), journal(&ns));
    loser.commit(&late, loser.events(1..=1), None).await?;

    ensure_eq!(lagging.backoffs(), 1, "one retry after one lost attempt");
    let page = winner.page(&world, 0, 10).await?;
    ensure_eq!(
        positions(&page),
        vec![1, 2, 3],
        "the loser took the next position"
    );
    ensure_eq!(
        page.rows[2].event.id.to_string(),
        format!("{}:1", loser.id),
        "the third row is the loser's event"
    );
    ensure_eq!(
        winner.high_watermark(&world).await?,
        3,
        "the counter follows the retried commit"
    );
    ensure_eq!(
        repo.events(&loser.id).await?.len(),
        1,
        "the loser's event was written once"
    );

    // The retried attempt allocated afresh: the event's stored allocation is the final position.
    let held = allocations(&repo.events(&loser.id).await?[0].metadata)?;
    ensure!(
        held.iter()
            .any(|allocation| allocation.scope == world.as_str() && allocation.position == 3),
        "the stored allocation is the one that committed: {held:?}"
    );

    Ok(())
}

async fn contention_is_bounded<R>(repo: &R) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let (rival, loser) = (
        Stream::new(&ns, "a", "ledger"),
        Stream::new(&ns, "b", "ledger"),
    );
    let world = scope(&ns, "world");

    let winner = JournalRepository::new(repo.clone(), journal(&ns));
    rival.commit(&winner, rival.events(1..=1), None).await?;

    // Every counter read is stale, so every attempt lands on the taken position.
    let lagging = Lagging::new(repo.clone(), usize::MAX);
    let late = JournalRepository::new(lagging.clone(), journal(&ns));
    let result = loser.commit(&late, loser.events(1..=1), None).await;

    expect_conflict(result, "contention past the retry policy")?;
    ensure_eq!(
        lagging.backoffs(),
        3,
        "four attempts wait three times (the policy's attempts, less the first)"
    );
    ensure!(
        repo.events(&loser.id).await?.is_empty(),
        "the exhausted commit wrote nothing"
    );
    ensure_eq!(
        winner.high_watermark(&world).await?,
        1,
        "the exhausted commit moved no counter"
    );

    Ok(())
}

async fn stream_conflicts_are_not_retried<R>(repo: &R) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let stream = Stream::new(&ns, "a", "ledger");

    let lagging = Lagging::new(repo.clone(), 0);
    let journaled = JournalRepository::new(lagging.clone(), journal(&ns));
    stream
        .commit(&journaled, stream.events(1..=2), None)
        .await?;

    // The same sequences again: the stream's own head conflicts, which no journal re-read fixes.
    let result = stream.commit(&journaled, stream.events(2..=3), None).await;

    expect_conflict(result, "a stale commit of the stream")?;
    ensure_eq!(
        lagging.backoffs(),
        0,
        "the journal did not retry a stream conflict"
    );
    ensure_eq!(
        journaled.high_watermark(&scope(&ns, "world")).await?,
        2,
        "the refused commit moved no counter"
    );

    // The same race with the counter moved under the loser: its first counter read is stale, so it
    // allocates positions another writer holds, and its stream is behind too. The stream's
    // conflict outranks the journal's own failure, so it is the caller's to re-prepare: the
    // journal does not resend.
    let other = Stream::new(&ns, "b", "ledger");
    let winner = JournalRepository::new(repo.clone(), journal(&ns));
    other.commit(&winner, other.events(1..=1), None).await?;
    stream.commit(&winner, stream.events(3..=3), None).await?;

    let stale = Lagging::new(repo.clone(), 2);
    let late = JournalRepository::new(stale.clone(), journal(&ns));
    let result = stream.commit(&late, stream.events(3..=3), None).await;

    expect_conflict(result, "a stream conflict under a moved counter")?;
    ensure_eq!(stale.backoffs(), 0, "no resend for a stream conflict");
    ensure_eq!(
        winner.high_watermark(&scope(&ns, "world")).await?,
        4,
        "the losing commit moved no counter"
    );

    Ok(())
}

async fn transient_failures_are_retried<R>(repo: &R) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let stream = Stream::new(&ns, "a", "ledger");
    let world = scope(&ns, "world");

    // Two transient failures, then success: the commit is allocated afresh each time and written
    // once.
    let flaky = Lagging::new(repo.clone(), 0).failing(2);
    let journaled = JournalRepository::new(flaky.clone(), journal(&ns));
    stream
        .commit(&journaled, stream.events(1..=1), None)
        .await?;

    ensure_eq!(flaky.backoffs(), 2, "one wait per transient failure");
    ensure_eq!(journaled.high_watermark(&world).await?, 1, "written once");
    ensure_eq!(repo.events(&stream.id).await?.len(), 1, "one event");

    // More failures than attempts: the transient error itself is returned, and nothing is written.
    let other = Stream::new(&ns, "b", "ledger");
    let doomed = Lagging::new(repo.clone(), 0).failing(usize::MAX);
    let journaled = JournalRepository::new(doomed.clone(), journal(&ns));
    let result = other.commit(&journaled, other.events(1..=1), None).await;

    expect_err(result, "transient failures past the policy", |err| {
        matches!(err, Error::Transient(_))
    })?;
    ensure_eq!(doomed.backoffs(), 3, "four attempts wait three times");
    ensure!(repo.events(&other.id).await?.is_empty(), "nothing written");
    ensure_eq!(
        journaled.high_watermark(&world).await?,
        1,
        "no counter moved"
    );

    Ok(())
}

async fn counter_behind_rows_is_repaired<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let journaled = JournalRepository::new(repo.clone(), journal(&ns));
    let (a, b) = (
        Stream::new(&ns, "a", "ledger"),
        Stream::new(&ns, "b", "ledger"),
    );
    let world = scope(&ns, "world");

    a.commit(&journaled, a.events(1..=3), None).await?;

    // A counter lowered under live rows (a rebuild beside writers, a restore): every writer would
    // collide on the same position forever.
    views.put_view(&counter_view(&world, 1)).await?;
    b.commit(&journaled, b.events(1..=1), None).await?;
    ensure_eq!(
        positions(&journaled.page(&world, 0, 10).await?),
        vec![1, 2, 3, 4],
        "the next position follows the highest row"
    );
    ensure_eq!(
        journaled.high_watermark(&world).await?,
        4,
        "the commit wrote the repaired counter back"
    );

    // A counter that is gone.
    views.delete_view(&world.counter_pk(), "COUNTER").await?;
    b.commit(&journaled, b.events(2..=2), None).await?;
    ensure_eq!(
        positions(&journaled.page(&world, 0, 10).await?),
        vec![1, 2, 3, 4, 5],
        "a lost counter is repaired the same way"
    );
    ensure_eq!(journaled.high_watermark(&world).await?, 5, "counter");

    Ok(())
}

async fn stale_counter_reads_fail_on_the_counter<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let a = Stream::new(&ns, "a", "ledger");
    let (world, place) = (scope(&ns, "world"), scope(&ns, "place-a"));
    a.commit(
        &JournalRepository::new(repo.clone(), journal(&ns)),
        a.events(1..=4),
        None,
    )
    .await?;

    // The rows at 3 and 4 are gone (compaction deletes rows, X2), and a writer read both counters
    // at 2 before they were written. Its rows at 3 would be absent: only the counter's
    // compare-and-set refuses the stale attempt, which then reads the counters again.
    for scope in [&world, &place] {
        for position in [3, 4] {
            views
                .delete_view(&scope.row_pk(), &row_sk(position))
                .await?;
        }
    }
    let lagging = Lagging::new(repo.clone(), 2).stale_at(2);
    let journaled = JournalRepository::new(lagging.clone(), journal(&ns));
    a.commit(&journaled, a.events(5..=5), None).await?;

    let stored = repo.events(&a.id).await?;
    ensure_eq!(
        allocations(&stored[4].metadata)?
            .iter()
            .map(|allocation| allocation.position)
            .collect::<Vec<_>>(),
        vec![5, 5],
        "the positions after the live counters"
    );
    ensure!(lagging.backoffs() >= 1, "the stale attempt was retried");
    ensure_eq!(journaled.high_watermark(&world).await?, 5, "the counter");

    Ok(())
}

async fn refused_commits_declare_nothing<R>(repo: &R) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let journaled = JournalRepository::new(repo.clone(), journal(&ns).with_max_row_bytes(10)?);
    let stream = Stream::new(&ns, "a", "ledger");

    // The guard refuses it, so the table must not end up declaring a journal or the type's history.
    expect_err(
        stream.commit(&journaled, stream.events(1..=1), None).await,
        "a row over the cap",
        |err| matches!(err, Error::JournalRowTooLarge(_)),
    )?;
    let manifest = repo.read_manifest().await?;
    ensure!(
        !manifest
            .as_ref()
            .is_some_and(|manifest| manifest.is_history_dependent(&stream.entity_type)),
        "a refused commit declared the history: {manifest:?}"
    );

    // A scope resolver that fails, the same.
    struct Failing;

    impl crate::journal::ScopeResolver for Failing {
        fn scopes(&self, _: &SerializedEvent) -> Result<Vec<Scope>, BoxError> {
            Err("no scope for this event".into())
        }
    }

    let other = Stream::new(&ns, "b", "ledger");
    let failing = JournalRepository::new(
        repo.clone(),
        Journal::new([format!("{ns}-ledger")], Failing)?,
    );
    expect_err(
        other.commit(&failing, other.events(1..=1), None).await,
        "a failing scope resolver",
        |err| matches!(err, Error::InvalidJournal(_)),
    )?;
    let manifest = repo.read_manifest().await?;
    ensure!(
        !manifest
            .as_ref()
            .is_some_and(|manifest| manifest.is_history_dependent(&other.entity_type)),
        "a failed resolver declared the history: {manifest:?}"
    );

    Ok(())
}

async fn pages_and_gaps<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + RebuildFence + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let journaled = JournalRepository::new(repo.clone(), journal(&ns));
    let a = Stream::new(&ns, "a", "ledger");
    let world = scope(&ns, "world");

    a.commit(&journaled, a.events(1..=5), None).await?;

    for bad in [0, crate::journal::MAX_PAGE_ROWS + 1] {
        expect_err(
            journaled.page(&world, 0, bad).await,
            "a page limit out of range",
            |err| matches!(err, Error::InvalidArgument(_)),
        )?;
    }

    // A row missing in the middle: the page stops before it and names it.
    views.delete_view(&world.row_pk(), &row_sk(3)).await?;
    let page = journaled.page(&world, 0, 10).await?;
    ensure_eq!(positions(&page), vec![1, 2], "rows before the gap");
    ensure_eq!(page.gap, Some(Gap { from: 3, to: 3 }), "the gap");
    ensure_eq!(page.next_after(), 2, "the reader may not pass the gap");
    ensure!(
        !page.has_more() && !page.is_caught_up(),
        "a gapped page is neither done nor continuable: {page:?}"
    );
    ensure_eq!(page.high_watermark, 5, "the watermark is the counter's");

    // Starting after the gap, the rows are contiguous again.
    let beyond = journaled.page(&world, 3, 10).await?;
    ensure_eq!(positions(&beyond), vec![4, 5], "rows after the gap");
    ensure!(beyond.gap.is_none(), "no gap after it: {beyond:?}");

    // Starting inside the gap: it is the page's first thing.
    let inside = journaled.page(&world, 2, 10).await?;
    ensure!(
        inside.rows.is_empty() && inside.gap == Some(Gap { from: 3, to: 3 }),
        "a page that starts at a gap holds no rows: {inside:?}"
    );

    // The last rows missing: the counter is ahead of the rows, a gap at the tail.
    views.delete_view(&world.row_pk(), &row_sk(5)).await?;
    let tail = journaled.page(&world, 3, 10).await?;
    ensure_eq!(positions(&tail), vec![4], "rows before the tail gap");
    ensure_eq!(tail.gap, Some(Gap { from: 5, to: 5 }), "the tail gap");

    // A rebuild from the stored events repairs both.
    rebuild_with_lock(repo, &repo.events(&a.id).await?, LEASE).await?;
    let repaired = journaled.page(&world, 0, 10).await?;
    ensure_eq!(positions(&repaired), vec![1, 2, 3, 4, 5], "repaired rows");
    ensure!(repaired.is_caught_up(), "repaired: {repaired:?}");

    // A lost counter does not hide rows: the watermark follows the rows it finds.
    views.delete_view(&world.counter_pk(), "COUNTER").await?;
    ensure_eq!(journaled.high_watermark(&world).await?, 0, "no counter row");
    let uncounted = journaled.page(&world, 0, 10).await?;
    ensure_eq!(uncounted.rows.len(), 5, "the rows are still read");
    ensure_eq!(
        uncounted.high_watermark,
        5,
        "the watermark follows the rows"
    );
    ensure!(uncounted.gap.is_none(), "no gap: {uncounted:?}");

    Ok(())
}

async fn declares_feature_and_history<R>(repo: &R) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let journaled = JournalRepository::new(repo.clone(), journal(&ns));
    let (a, plain) = (
        Stream::new(&ns, "a", "ledger"),
        Stream::new(&ns, "p", "other"),
    );

    // Uncovered events declare nothing.
    plain.commit(&journaled, plain.events(1..=1), None).await?;
    let before = repo.read_manifest().await?;
    ensure!(
        !before
            .as_ref()
            .is_some_and(|manifest| manifest.is_history_dependent(&a.entity_type)),
        "nothing is declared before a covered event"
    );

    a.commit(&journaled, a.events(1..=2), None).await?;
    let manifest = repo
        .read_manifest()
        .await?
        .ok_or(Failure("no manifest".into()))?;
    ensure_eq!(
        manifest.features.get(JOURNAL_FEATURE),
        Some(&JOURNAL_VERSION),
        "the journal feature at its format version"
    );
    ensure!(
        manifest.is_history_dependent(&a.entity_type),
        "the journaled type is history-dependent"
    );
    ensure!(
        !manifest.is_history_dependent(&plain.entity_type),
        "an uncovered type is not"
    );

    // Another handle finds both declared and writes nothing to the manifest.
    let again = JournalRepository::new(repo.clone(), journal(&ns));
    a.commit(&again, a.events(3..=3), None).await?;
    let after = repo
        .read_manifest()
        .await?
        .ok_or(Failure("no manifest".into()))?;
    ensure_eq!(
        after.version,
        manifest.version,
        "a handle that finds the declaration does not bump the manifest"
    );

    // Compaction of the journaled type is refused in every process; an uncovered one is not.
    repo.put_snapshot(&a.snapshot(1, 3, "{}")).await?;
    expect_err(
        repo.compact_below(&a.id, EventSequence(3)).await,
        "compacting a journaled stream",
        |err| matches!(err, Error::HistoryDependentProjection { .. }),
    )?;
    ensure_eq!(repo.events(&a.id).await?.len(), 3, "nothing was deleted");

    Ok(())
}

async fn replay_reproduces_the_journal<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + RebuildFence + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let journaled = JournalRepository::new(repo.clone(), journal(&ns));
    let (a, b, c) = (
        Stream::new(&ns, "a", "ledger"),
        Stream::new(&ns, "b", "ledger"),
        Stream::new(&ns, "c", "ledger"),
    );

    a.commit(&journaled, a.events(1..=2), None).await?;
    b.commit(&journaled, b.events(1..=3), None).await?;
    let commit = |stream: &Stream, range| Commit {
        entity_id: stream.id.clone(),
        events: stream.events(range),
        snapshot: None,
        operations: Vec::new(),
        views: Vec::new(),
        outcome: None,
    };
    journaled
        .commit_transaction(TransactionCommit::new(vec![
            commit(&c, 1..=2),
            commit(&a, 3..=3),
        ]))
        .await?;
    b.commit(&journaled, b.events(4..=4), None).await?;

    let mut events = Vec::new();
    for stream in [&a, &b, &c] {
        events.extend(repo.events(&stream.id).await?);
    }
    ensure_eq!(events.len(), 9, "nine stored events");

    // Whatever the order the events come in, the state is the same.
    let forward = replay(&events)?;
    let mut reversed = events.clone();
    reversed.reverse();
    let mut interleaved = events.clone();
    interleaved.sort_by_key(|event| (event.sequence, std::cmp::Reverse(event.entity_id.clone())));

    ensure_eq!(replay(&reversed)?, forward, "reverse order");
    ensure_eq!(replay(&interleaved)?, forward, "interleaved order");

    // It is exactly what the commits wrote.
    let world = scope(&ns, "world");
    let live_rows = views.list_views_by_pk(&world.row_pk()).await?;
    let replayed: Vec<ViewRow> = forward
        .rows
        .iter()
        .filter(|((scope, _), _)| *scope == world)
        .map(|(_, row)| row.clone())
        .collect();
    ensure_eq!(live_rows.len(), 9, "nine live world rows");
    ensure_eq!(
        replayed,
        live_rows,
        "the replayed world rows are the live rows"
    );
    for (scope, position) in &forward.counters {
        ensure_eq!(
            journaled.high_watermark(scope).await?,
            *position,
            "the replayed counter is the live counter"
        );
    }

    // The commit-wide projector alone reproduces the rows too (counters aside), through the
    // engine's regroup-and-replay, which also proves no row is shared between owners.
    let rows = replay_views(
        &[journaled.projector()],
        &group_commits(events.iter().cloned())?,
    )?;
    ensure_eq!(
        rows.iter()
            .filter(|row| row.pk == world.row_pk())
            .cloned()
            .collect::<Vec<_>>(),
        live_rows,
        "replay_views reproduces the live world rows"
    );

    // Rebuilding into an emptied scope restores it.
    for row in &live_rows {
        views.delete_view(&row.pk, &row.sk).await?;
    }
    views.delete_view(&world.counter_pk(), "COUNTER").await?;
    ensure!(
        journaled.page(&world, 0, 10).await?.rows.is_empty(),
        "the scope was wiped"
    );
    rebuild_with_lock(repo, &events, LEASE).await?;
    ensure_eq!(
        views.list_views_by_pk(&world.row_pk()).await?,
        live_rows,
        "the rebuilt rows"
    );
    ensure_eq!(
        journaled.high_watermark(&world).await?,
        9,
        "the rebuilt counter"
    );

    Ok(())
}

async fn rebuild_under_the_lock<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + RebuildFence + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let journaled = JournalRepository::new(repo.clone(), journal(&ns));
    let (a, b) = (
        Stream::new(&ns, "a", "ledger"),
        Stream::new(&ns, "b", "ledger"),
    );
    let world = scope(&ns, "world");

    a.commit(&journaled, a.events(1..=3), None).await?;
    b.commit(&journaled, b.events(1..=2), None).await?;

    let live_rows = views.list_views_by_pk(&world.row_pk()).await?;
    let live_counters: Vec<u64> = {
        let mut counters = Vec::new();
        for name in ["world", "place-a", "place-b"] {
            counters.push(journaled.high_watermark(&scope(&ns, name)).await?);
        }

        counters
    };
    let mut events = repo.events(&a.id).await?;
    events.extend(repo.events(&b.id).await?);

    // Wipe the derived state, as a rebuild starts from lost rows, then rebuild under the lock.
    for row in &live_rows {
        views.delete_view(&row.pk, &row.sk).await?;
    }
    views.delete_view(&world.counter_pk(), "COUNTER").await?;

    let lease = repo.begin_rebuild(&format!("{ns}-journal"), LEASE).await?;

    // A live journal commit is refused while the lock is held, and writes nothing.
    expect_err(
        b.commit(&journaled, b.events(3..=3), None).await,
        "a live journal commit during a rebuild",
        |err| matches!(err, Error::RebuildInProgress),
    )?;
    ensure_eq!(
        repo.events(&b.id).await?.len(),
        2,
        "the refused commit wrote no event"
    );

    // Readers are fenced for the duration, and the epoch-checked reads say so.
    expect_err(
        crate::journal::page_at_epoch(repo, &world, 0, 10).await,
        "an epoch-checked page during a rebuild",
        |err| matches!(err, Error::RebuildUnavailable { .. }),
    )?;

    crate::journal::rebuild(repo, &events, &lease).await?;
    repo.end_rebuild(&lease).await?;

    ensure_eq!(
        views.list_views_by_pk(&world.row_pk()).await?,
        live_rows,
        "the rebuilt rows are the live rows"
    );
    let mut rebuilt = Vec::new();
    for name in ["world", "place-a", "place-b"] {
        rebuilt.push(journaled.high_watermark(&scope(&ns, name)).await?);
    }
    ensure_eq!(
        rebuilt,
        live_counters,
        "the rebuilt counters are the live counters"
    );

    // The lock is gone: live commits resume at the next position, and the epoch-checked page works.
    b.commit(&journaled, b.events(3..=3), None).await?;
    let page = crate::journal::page_at_epoch(repo, &world, 0, 10).await?;
    ensure_eq!(
        page.rows.len(),
        6,
        "the live commit continued the rebuilt scope"
    );
    ensure!(page.is_caught_up(), "{page:?}");

    // A lease that was lost writes nothing: the rebuild fences on owner and fencing token.
    let stale = repo.begin_rebuild(&format!("{ns}-journal"), LEASE).await?;
    repo.end_rebuild(&stale).await?;
    expect_err(
        crate::journal::rebuild(repo, &events, &stale).await,
        "a rebuild on a released lease",
        |err| matches!(err, Error::RebuildLeaseLost { .. }),
    )?;

    Ok(())
}

/// A rebuild wipes the journal's rows and counters, writes its first batch, and fails; an operator
/// force-releases it. A journaled commit would now allocate positions from the wiped counters and
/// stamp duplicates of positions earlier events hold into its immutable metadata, so it is refused
/// with the error a held lock gives, and writes nothing. A rebuild that runs to its end restores
/// the counters, and the same commit then continues each scope at the next position.
async fn force_released_rebuild_refuses_journaled_commits<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + RebuildFence + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let journaled = JournalRepository::new(repo.clone(), journal(&ns));
    let stream = Stream::new(&ns, "a", "ledger");
    let (world, place) = (scope(&ns, "world"), scope(&ns, "place-a"));

    stream
        .commit(&journaled, stream.events(1..=3), None)
        .await?;
    let events = repo.events(&stream.id).await?;
    let state = replay(&events)?;

    // The rebuild wipes the derived state, writes its first batch, and fails without ending.
    let lease = repo.begin_rebuild(&format!("{ns}-failed"), LEASE).await?;
    for scope in [&world, &place] {
        for row in views.list_views_by_pk(&scope.row_pk()).await? {
            views.delete_view(&row.pk, &row.sk).await?;
        }
        views.delete_view(&scope.counter_pk(), "COUNTER").await?;
    }
    let first = state.views().remove(0);
    repo.write_rebuild_views(
        &lease,
        &[ProjectedView::new(
            "journal-rebuild",
            0,
            0,
            ViewOperation::put(
                first.pk,
                first.sk,
                first.entity_id,
                first.entity_type,
                first.payload,
            ),
        )],
    )
    .await?;
    ensure_eq!(
        journaled.high_watermark(&world).await?,
        0,
        "the failed rebuild left the counter wiped"
    );

    let forced = repo.force_release_rebuild(&lease.owner).await?;
    ensure!(forced.force_released, "{forced:?}");

    // The journaled commit is refused, and writes no event and no allocation.
    expect_err(
        stream.commit(&journaled, stream.events(4..=4), None).await,
        "a journaled commit after the force-release",
        |err| matches!(err, Error::RebuildInProgress),
    )?;
    ensure_eq!(
        repo.events(&stream.id).await?.len(),
        3,
        "the refused commit wrote no event"
    );
    ensure_eq!(
        journaled.high_watermark(&world).await?,
        0,
        "the refused commit allocated nothing"
    );
    expect_err(
        crate::journal::page_at_epoch(repo, &world, 0, 10).await,
        "an epoch-checked page after the force-release",
        |err| {
            matches!(
                err,
                Error::RebuildUnavailable {
                    force_released: true,
                    ..
                }
            )
        },
    )?;

    // A rebuild that runs to its end restores the counters and settles the generation; the commit
    // then continues each scope at the next position.
    rebuild_with_lock(repo, &events, LEASE).await?;
    stream
        .commit(&journaled, stream.events(4..=4), None)
        .await?;
    for scope in [&world, &place] {
        ensure_eq!(
            journaled.high_watermark(scope).await?,
            4,
            "the commit continued {scope} after the rebuilt counter"
        );
    }
    let page = crate::journal::page_at_epoch(repo, &world, 0, 10).await?;
    ensure_eq!(
        positions(&page),
        vec![1, 2, 3, 4],
        "no position is held twice"
    );
    ensure!(page.is_caught_up(), "{page:?}");

    Ok(())
}

async fn views_unsupported_is_refused<R>(repo: &R) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let stream = Stream::new(&ns, "a", "ledger");
    let dropping =
        JournalRepository::new(Lagging::new(repo.clone(), 0).without_views(), journal(&ns));

    expect_err(
        stream.commit(&dropping, stream.events(1..=1), None).await,
        "a repository that does not write views",
        |err| matches!(err, Error::ViewsUnsupported { .. }),
    )?;
    ensure!(
        repo.events(&stream.id).await?.is_empty(),
        "no event, and so no allocation, was written"
    );
    let manifest = repo.read_manifest().await?;
    ensure!(
        !manifest
            .as_ref()
            .is_some_and(|manifest| manifest.is_history_dependent(&stream.entity_type)),
        "nothing was declared: {manifest:?}"
    );

    // Events of a type the journal does not cover are not its concern.
    let other = Stream::new(&ns, "x", "other");
    other.commit(&dropping, other.events(1..=1), None).await?;

    // A repository that writes views but does not apply the counter's compare-and-set would write
    // the counter unconditionally: refused the same way, before anything is planned or declared.
    let unconditional = JournalRepository::new(
        Lagging::new(repo.clone(), 0).ignoring_expected_payload(),
        journal(&ns),
    );
    let next = Stream::new(&ns, "b", "ledger");
    expect_err(
        next.commit(&unconditional, next.events(1..=1), None).await,
        "a repository that ignores the expected-payload condition",
        |err| matches!(err, Error::ExpectedPayloadUnsupported { .. }),
    )?;
    ensure!(
        repo.events(&next.id).await?.is_empty(),
        "the refused commit wrote nothing"
    );

    Ok(())
}

async fn invalid_commits_declare_nothing<R>(repo: &R) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
{
    let ns = namespace();
    let journaled = JournalRepository::new(repo.clone(), journal(&ns));
    let covered = EntityType::new(format!("{ns}-ledger"));

    // Earlier cases declared the journal feature; this type's history is the new declaration.
    let untouched = |manifest: Option<features::FeatureManifest>| {
        !manifest.is_some_and(|manifest| manifest.is_history_dependent(&covered))
    };

    // An event whose ID is not `{entity}:{sequence}`, which the repository refuses.
    let stream = Stream::new(&ns, "a", "ledger");
    let mut events = stream.events(1..=1);
    events[0].id = "not-the-id".into();
    expect_err(
        stream.commit(&journaled, events, None).await,
        "an event with the wrong ID",
        |err| matches!(err, Error::InvalidCommit(_)),
    )?;
    let manifest = repo.read_manifest().await?;
    ensure!(
        untouched(manifest.clone()),
        "a malformed commit declared: {manifest:?}"
    );

    // A reserved entity ID.
    let reserved = Stream {
        id: EntityId::new(format!("SYSTEM#{ns}")),
        entity_type: covered.clone(),
    };
    expect_err(
        reserved
            .commit(&journaled, reserved.events(1..=1), None)
            .await,
        "a reserved entity ID",
        |err| matches!(err, Error::ReservedEntityId { .. }),
    )?;

    // The same through a transaction.
    let transaction = TransactionCommit::new(vec![Commit {
        entity_id: reserved.id.clone(),
        events: reserved.events(1..=1),
        snapshot: None,
        operations: Vec::new(),
        views: Vec::new(),
        outcome: None,
    }]);
    expect_err(
        journaled.commit_transaction(transaction).await,
        "a reserved entity ID in a transaction",
        |err| matches!(err, Error::ReservedEntityId { .. }),
    )?;
    let manifest = repo.read_manifest().await?;
    ensure!(
        untouched(manifest.clone()),
        "a refused commit declared: {manifest:?}"
    );

    Ok(())
}

async fn full_pages_show_more_rows<R, V>(repo: &R, views: &V) -> CaseResult
where
    R: Repository + FeatureMarker + JournalCompactionStore + Clone,
    R::Operation: Clone,
    V: ViewRepository,
{
    let ns = namespace();
    let journaled = JournalRepository::new(repo.clone(), journal(&ns));
    let a = Stream::new(&ns, "a", "ledger");
    let world = scope(&ns, "world");

    a.commit(&journaled, a.events(1..=5), None).await?;

    for (name, lower) in [
        ("a counter behind its rows", true),
        ("a missing counter", false),
    ] {
        if lower {
            views.put_view(&counter_view(&world, 1)).await?;
        } else {
            views.delete_view(&world.counter_pk(), "COUNTER").await?;
        }

        let first = journaled.page(&world, 0, 2).await?;
        ensure_eq!(positions(&first), vec![1, 2], "{name}: the first page");
        ensure!(
            first.gap.is_none() && first.has_more() && !first.is_caught_up(),
            "{name}: a full page with rows after it is not caught up: {first:?}"
        );
        ensure_eq!(
            first.high_watermark,
            3,
            "{name}: the watermark follows the next row"
        );

        let mut after = 0;
        let mut seen = Vec::new();
        loop {
            let page = journaled.page(&world, after, 2).await?;
            seen.extend(positions(&page));
            after = page.next_after();
            if !page.has_more() {
                ensure!(
                    page.is_caught_up(),
                    "{name}: the last page is caught up: {page:?}"
                );
                break;
            }
        }
        ensure_eq!(
            seen,
            vec![1, 2, 3, 4, 5],
            "{name}: paging by 2 reads every row"
        );
    }

    Ok(())
}
