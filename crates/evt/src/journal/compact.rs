//! Journal compaction (ADR 0005, X2): the per-scope lease, the compaction run, and the snapshot
//! reads a client installs after a `410 Gone`.
//!
//! A run compacts a scope's journal through a checkpoint, in an order that keeps every reader
//! correct without stopping writers:
//!
//! 1. it builds the scope's **canonical chunks** of rows `1..=checkpoint` (resuming the previous
//!    manifest's chunks, so only the tail is rebuilt) and writes the new chunks;
//! 2. it **swaps the manifest** in one atomic write with the one chunk it replaces, which makes the
//!    checkpoint the scope's floor;
//! 3. it **deletes the journal rows** at or below the checkpoint, in batches;
//! 4. it writes the **audit** row of the checkpoint.
//!
//! Every write checks the lease (owner and fencing token) and the rebuild epoch the run started
//! under, so a run that lost its lease, or that a rebuild began under, writes nothing more. A
//! reader that meets a gap below the floor reads the manifest after the rows, so it always sees the
//! floor that explains the gap and gets [`Error::JournalGone`] instead of a gap.

use std::time::Duration;

use super::compaction::{
    BuiltChunk, ChunkBuilder, CompactionAudit, CompactionChunk, CompactionLease,
    CompactionManifest, CompactionStore, CompactionWrite, CompactionWritten, Expect,
    LEASE_SORT_KEY, LeaseRow, MANIFEST_SORT_KEY, MAX_CHUNK_BYTES, MAX_CHUNK_ROWS,
    MAX_COMPACTION_WRITES, audit_sk, chunk_sk, compaction_pk, compaction_view, released_payload,
};
use super::read::{JournalStore, MAX_PAGE_ROWS};
use super::scope::{Scope, row_sk};
use crate::error::Error;
use crate::features::{JOURNAL_COMPACTION_FEATURE, JOURNAL_COMPACTION_VERSION};
use crate::rebuild::{self, RebuildEpoch, observe_rebuild_epoch};

/// How often a lease acquire reads the lease again after losing a race, before it fails with a
/// transient error.
const MAX_LEASE_ATTEMPTS: usize = 8;

/// The journal rows one delete batch removes: the batch's last write is the lease check.
const DELETE_BATCH: usize = MAX_COMPACTION_WRITES - 1;

/// The owner token [`compact_with_lease`] callers commonly pass; any owner token works.
pub const COMPACTION_OWNER: &str = "journal-compaction";

/// What a compaction run did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionReport {
    /// The scope.
    pub scope: Scope,
    /// The checkpoint: the scope's floor after the run.
    pub checkpoint: u64,
    /// The manifest digest.
    pub digest: String,
    /// The number of chunks in the manifest.
    pub chunk_count: usize,
    /// The chunks this run wrote (0 when the manifest already covered the checkpoint).
    pub chunks_written: usize,
    /// The journal rows this run deleted.
    pub rows_deleted: u64,
}

/// The scope's manifest, if it has been compacted. Strongly consistent.
pub async fn read_manifest<S: JournalStore + ?Sized>(
    store: &S,
    scope: &Scope,
) -> Result<Option<CompactionManifest>, Error> {
    store
        .read_compaction_row(scope, MANIFEST_SORT_KEY)
        .await?
        .map(|row| CompactionManifest::decode(scope, &row.payload))
        .transpose()
}

/// The scope's floor: the checkpoint of its manifest, `0` when it has none.
pub async fn read_floor<S: JournalStore + ?Sized>(store: &S, scope: &Scope) -> Result<u64, Error> {
    Ok(read_manifest(store, scope)
        .await?
        .map_or(0, |manifest| manifest.checkpoint))
}

/// Chunk `index` of `manifest`, checked against the manifest's digest for it.
///
/// A chunk that is missing or does not match is [`Error::JournalSnapshotMoved`]: a compaction
/// swapped the manifest since it was read. Read the manifest again and start over; it is
/// transient.
pub async fn read_chunk<S: JournalStore + ?Sized>(
    store: &S,
    manifest: &CompactionManifest,
    index: usize,
) -> Result<CompactionChunk, Error> {
    let scope = &manifest.scope;
    let Some(digest) = manifest.chunks.get(index) else {
        return Err(Error::InvalidArgument(format!(
            "the snapshot of {scope} has {} chunks; there is no chunk {index}",
            manifest.chunks.len()
        )));
    };

    let moved = || Error::JournalSnapshotMoved {
        scope: scope.as_str().to_owned(),
        checkpoint: manifest.checkpoint,
    };
    let row = store
        .read_compaction_row(scope, &chunk_sk(index as u64))
        .await?
        .ok_or_else(moved)?;

    if crate::sha256::sha256_hex(&row.payload) != *digest {
        return Err(moved());
    }

    let chunk = CompactionChunk::decode(scope, &row.payload)?;
    if chunk.index != index as u64 {
        return Err(Error::InvalidRecord(format!(
            "chunk {index} of {scope} says it is chunk {}",
            chunk.index
        )));
    }

    Ok(chunk)
}

/// The audit row of `checkpoint`, if a run completed it.
pub async fn read_audit<S: JournalStore + ?Sized>(
    store: &S,
    scope: &Scope,
    checkpoint: u64,
) -> Result<Option<CompactionAudit>, Error> {
    store
        .read_compaction_row(scope, &audit_sk(checkpoint))
        .await?
        .map(|row| CompactionAudit::decode(scope, &row.payload))
        .transpose()
}

/// The scope's lease row, if one was ever written.
pub async fn read_lease<S: JournalStore + ?Sized>(
    store: &S,
    scope: &Scope,
) -> Result<Option<LeaseRow>, Error> {
    store
        .read_compaction_row(scope, LEASE_SORT_KEY)
        .await?
        .map(|row| LeaseRow::decode(scope, &row.payload))
        .transpose()
}

/// Declares `journal-compaction` on the feature marker before the first compaction, so a build that
/// does not know compaction (and would read a compacted scope's missing rows as lost, or allocate
/// below its floor) refuses the table. The marker is checked first: a table declaring a feature
/// this build does not model is refused.
async fn ensure_declared<S: CompactionStore + ?Sized>(store: &S) -> Result<(), Error> {
    let manifest = store.read_manifest().await?;

    if let Some(manifest) = &manifest {
        manifest.validate_supported()?;
    }

    let declared = manifest
        .as_ref()
        .and_then(|manifest| manifest.features.get(JOURNAL_COMPACTION_FEATURE).copied());
    if declared == Some(JOURNAL_COMPACTION_VERSION) {
        return Ok(());
    }

    store
        .update_manifest(|manifest| {
            manifest.features.insert(
                JOURNAL_COMPACTION_FEATURE.to_owned(),
                JOURNAL_COMPACTION_VERSION,
            );
        })
        .await
}

/// Maps a write fenced out by the rebuild generation: a rebuild began (or began and ended) since
/// `epoch` was observed.
async fn epoch_error<S: CompactionStore + ?Sized>(store: &S, epoch: RebuildEpoch) -> Error {
    match store.rebuild_generation().await {
        Err(err) => err,
        Ok(generation) => match epoch.verify(generation) {
            Err(err) => err,
            Ok(()) => {
                Error::Transient("the rebuild generation moved during a compaction write".into())
            }
        },
    }
}

fn lease_lost(scope: &Scope, owner: &str) -> Error {
    Error::CompactionLeaseLost {
        scope: scope.as_str().to_owned(),
        owner: owner.to_owned(),
    }
}

/// Acquires the scope's compaction lease as `owner` for `lease`, or takes over a lapsed one.
///
/// A scope has at most one compaction at a time. The lease row records the holder, a fencing token
/// that rises with every acquire and takeover, and the second the lease lapses (the repository's
/// clock). An unexpired lease is refused with [`Error::CompactionLeaseHeld`]. The acquire is
/// refused while a rebuild holds or left the fence ([`Error::RebuildUnavailable`]), and the first
/// one declares `journal-compaction` on the feature marker. When its write loses a race it reads the
/// lease again, a few times, and then fails transiently.
pub async fn begin_compaction<S: CompactionStore + ?Sized>(
    store: &S,
    scope: &Scope,
    owner: &str,
    lease: Duration,
) -> Result<CompactionLease, Error> {
    let seconds = rebuild::check_lease(owner, lease)?;
    let epoch = observe_rebuild_epoch(store).await?;

    ensure_declared(store).await?;

    for _ in 0..MAX_LEASE_ATTEMPTS {
        let stored = store.read_compaction_row(scope, LEASE_SORT_KEY).await?;
        let now = store.compaction_now();
        let (token, expect) = match &stored {
            None => (1, Expect::Absent),
            Some(row) => match LeaseRow::decode(scope, &row.payload)? {
                LeaseRow::Held(held) if held.lease_expires_at >= now => {
                    return Err(Error::CompactionLeaseHeld {
                        scope: scope.as_str().to_owned(),
                        owner: held.owner,
                        lease_expires_at: held.lease_expires_at,
                    });
                }
                other => (other.token() + 1, Expect::Payload(row.payload.clone())),
            },
        };

        let acquired = CompactionLease {
            scope: scope.clone(),
            owner: owner.to_owned(),
            token,
            lease_expires_at: now.saturating_add(seconds),
            lease_seconds: seconds,
        };
        let write = CompactionWrite::Put {
            row: compaction_view(scope, LEASE_SORT_KEY, acquired.payload()),
            expect,
        };

        match store.write_compaction(Some(epoch), &[write]).await? {
            CompactionWritten::Written => return Ok(acquired),
            CompactionWritten::EpochChanged => return Err(epoch_error(store, epoch).await),
            CompactionWritten::Refused(_) => {}
        }
    }

    Err(Error::Transient(
        format!("the compaction lease of {scope} kept changing").into(),
    ))
}

/// Puts the lease row to `payload` if it still holds `lease`, else [`Error::CompactionLeaseLost`].
async fn replace_lease<S: CompactionStore + ?Sized>(
    store: &S,
    lease: &CompactionLease,
    payload: Vec<u8>,
) -> Result<(), Error> {
    let write = CompactionWrite::Put {
        row: compaction_view(&lease.scope, LEASE_SORT_KEY, payload),
        expect: Expect::Payload(lease.payload()),
    };

    match store.write_compaction(None, &[write]).await? {
        CompactionWritten::Written => Ok(()),
        _ => Err(lease_lost(&lease.scope, &lease.owner)),
    }
}

/// Extends `lease` to `extend` from now and returns it with its new expiry. A lease that was taken
/// over or released is [`Error::CompactionLeaseLost`]; a lapsed lease nobody took over can still
/// be renewed.
pub async fn renew_compaction<S: CompactionStore + ?Sized>(
    store: &S,
    lease: &CompactionLease,
    extend: Duration,
) -> Result<CompactionLease, Error> {
    let seconds = rebuild::check_lease(&lease.owner, extend)?;
    let renewed = CompactionLease {
        lease_expires_at: store.compaction_now().saturating_add(seconds),
        lease_seconds: seconds,
        ..lease.clone()
    };

    replace_lease(store, lease, renewed.payload()).await?;

    Ok(renewed)
}

/// Releases `lease`, keeping its token. A lease that was taken over or released is
/// [`Error::CompactionLeaseLost`].
pub async fn end_compaction<S: CompactionStore + ?Sized>(
    store: &S,
    lease: &CompactionLease,
) -> Result<(), Error> {
    replace_lease(store, lease, lease.released_payload()).await
}

/// The operator's release of a named owner's lease, for a lost or wedged worker: releases the lease
/// whatever its token or expiry, so a new compaction can begin at once, and the old worker's next
/// write fails its lease check. [`Error::CompactionLeaseLost`] when no lease of that owner is held.
/// Compaction leaves the scope consistent at every step, so it needs no cleanup: run the
/// compaction again.
pub async fn force_release_compaction<S: CompactionStore + ?Sized>(
    store: &S,
    scope: &Scope,
    owner: &str,
) -> Result<(), Error> {
    rebuild::check_owner(owner)?;

    let Some(row) = store.read_compaction_row(scope, LEASE_SORT_KEY).await? else {
        return Err(lease_lost(scope, owner));
    };
    let LeaseRow::Held(held) = LeaseRow::decode(scope, &row.payload)? else {
        return Err(lease_lost(scope, owner));
    };
    if held.owner != owner {
        return Err(lease_lost(scope, owner));
    }

    replace_lease(store, &held, released_payload(scope, held.token)).await
}

/// A compaction run's writer: every batch checks the lease and the rebuild epoch.
struct Run<'a, S: ?Sized> {
    store: &'a S,
    lease: &'a mut CompactionLease,
    epoch: RebuildEpoch,
}

impl<S: CompactionStore + ?Sized> Run<'_, S> {
    /// Writes `writes` with the lease check appended. The lease must not have lapsed by the
    /// repository's clock: a lapsed lease may be taken over, so the worker stops. A lease with
    /// less than half of its length left is renewed first, so a long run keeps it.
    async fn write(&mut self, mut writes: Vec<CompactionWrite>) -> Result<(), Error> {
        let now = self.store.compaction_now();

        if self.lease.lease_expires_at < now {
            return Err(lease_lost(&self.lease.scope, &self.lease.owner));
        }

        let seconds = self.lease.lease_seconds;
        if seconds > 0 && self.lease.lease_expires_at.saturating_sub(now) < (seconds + 1) / 2 {
            *self.lease = renew_compaction(
                self.store,
                self.lease,
                Duration::from_secs(seconds.unsigned_abs()),
            )
            .await?;
        }

        let scope = &self.lease.scope;
        let check = writes.len();
        writes.push(CompactionWrite::Check {
            pk: compaction_pk(scope),
            sk: LEASE_SORT_KEY.to_owned(),
            expect: Expect::Payload(self.lease.payload()),
        });

        match self
            .store
            .write_compaction(Some(self.epoch), &writes)
            .await?
        {
            CompactionWritten::Written => Ok(()),
            CompactionWritten::EpochChanged => Err(epoch_error(self.store, self.epoch).await),
            // Only a lease holder writes the manifest, so a manifest that changed under the run
            // means it lost the lease as surely as a failed lease check.
            CompactionWritten::Refused(at)
                if at == check || writes[at].key().1 == MANIFEST_SORT_KEY =>
            {
                Err(lease_lost(scope, &self.lease.owner))
            }
            CompactionWritten::Refused(at) => Err(Error::InvalidRecord(format!(
                "a compaction write to {:?} of {scope} found a row it does not own",
                writes[at].key()
            ))),
        }
    }

    fn chunk_write(&self, chunk: &BuiltChunk) -> CompactionWrite {
        CompactionWrite::Put {
            row: compaction_view(
                &self.lease.scope,
                chunk_sk(chunk.index),
                chunk.payload.clone(),
            ),
            expect: Expect::Any,
        }
    }
}

/// Compacts the lease's scope through `through` under `lease` ([`begin_compaction`]): builds and
/// stores the snapshot of rows `1..=through`, makes `through` the scope's floor, deletes the rows
/// at or below it, and records the audit row.
///
/// `through` is between the scope's current floor and its high watermark, and the rows above the
/// floor up to it must all be there: a gap is [`Error::InvalidJournal`], before any write. A
/// `through` equal to the floor builds nothing and deletes whatever rows at or below it remain,
/// which is how an interrupted run, or a rebuild that restored the rows, is finished. Running it
/// again is always safe.
///
/// The run is refused while a rebuild holds or left the fence ([`Error::RebuildUnavailable`]) and
/// stops with [`Error::RebuildEpochChanged`] (nothing more written) if one begins. It stops with
/// [`Error::CompactionLeaseLost`] if the lease was taken over, released, or lapsed. Either way the
/// scope is consistent and readers are unaffected: compact again.
///
/// The run renews `lease` in place (by its own length) whenever less than half of it is left, so
/// `lease` is the one to release afterwards. A lease read back from its row
/// ([`read_lease`]) has no length and is not renewed.
pub async fn compact<S: CompactionStore + ?Sized>(
    store: &S,
    lease: &mut CompactionLease,
    through: u64,
) -> Result<CompactionReport, Error> {
    let scope = lease.scope.clone();
    let scope = &scope;
    let owner = lease.owner.clone();
    let token = lease.token;
    let epoch = observe_rebuild_epoch(store).await?;
    let mut run = Run {
        store,
        lease,
        epoch,
    };

    let stored = store.read_compaction_row(scope, MANIFEST_SORT_KEY).await?;
    let old = stored
        .as_ref()
        .map(|row| CompactionManifest::decode(scope, &row.payload))
        .transpose()?;
    let floor = old.as_ref().map_or(0, |manifest| manifest.checkpoint);

    if through == 0 || through < floor {
        return Err(Error::InvalidArgument(format!(
            "{scope} is compacted through {floor}; compact through {floor} or more, not {through}"
        )));
    }

    let watermark = store.read_counter(scope).await?;
    if through > watermark {
        return Err(Error::InvalidArgument(format!(
            "{scope} has allocated through {watermark}; it cannot be compacted through {through}"
        )));
    }

    let (manifest, chunks_written) = if through > floor {
        build(&mut run, old, stored.map(|row| row.payload), through).await?
    } else {
        (old.expect("a floor comes from a manifest"), 0)
    };

    let rows_deleted = delete_rows(&mut run, through).await?;
    let digest = manifest.digest();
    let audit = CompactionAudit {
        scope: scope.clone(),
        checkpoint: through,
        digest: digest.clone(),
        chunks: manifest.chunks.len(),
        deleted: rows_deleted,
        owner,
        token,
        completed_at: store.compaction_now(),
    };
    run.write(vec![CompactionWrite::Put {
        row: compaction_view(scope, audit_sk(through), audit.payload()),
        expect: Expect::Any,
    }])
    .await?;

    Ok(CompactionReport {
        scope: scope.clone(),
        checkpoint: through,
        digest,
        chunk_count: manifest.chunks.len(),
        chunks_written,
        rows_deleted,
    })
}

/// [`compact`] holding a lease for the run: acquires it as `owner` ([`begin_compaction`]),
/// compacts, and releases it, also when the run fails (the run leaves the scope consistent).
pub async fn compact_with_lease<S: CompactionStore + ?Sized>(
    store: &S,
    scope: &Scope,
    through: u64,
    owner: &str,
    lease: Duration,
) -> Result<CompactionReport, Error> {
    let mut held = begin_compaction(store, scope, owner, lease).await?;
    let result = compact(store, &mut held, through).await;

    match (result, end_compaction(store, &held).await) {
        (Ok(report), Ok(())) => Ok(report),
        (Ok(_), Err(err)) | (Err(err), _) => Err(err),
    }
}

/// Builds the snapshot through `through` and swaps the manifest. Returns the new manifest and how
/// many chunks were written.
///
/// Closed chunks never change (the canonical rule closes a chunk on rows before it only), so it
/// resumes the previous manifest's last chunk and rebuilds from there. Chunks at new indexes are
/// written first, unreferenced; the rewritten last chunk goes in the same atomic write as the
/// manifest, so a reader never finds a manifest whose chunks are not there.
async fn build<S: CompactionStore + ?Sized>(
    run: &mut Run<'_, S>,
    old: Option<CompactionManifest>,
    old_payload: Option<Vec<u8>>,
    through: u64,
) -> Result<(CompactionManifest, usize), Error> {
    let scope = &run.lease.scope.clone();
    let floor = old.as_ref().map_or(0, |manifest| manifest.checkpoint);

    let (mut digests, mut builder, tail) = match &old {
        None => (Vec::new(), ChunkBuilder::new(scope, 0, 1), None),
        Some(manifest) => {
            let tail = manifest.chunks.len() - 1;
            let row = run
                .store
                .read_compaction_row(scope, &chunk_sk(tail as u64))
                .await?
                .filter(|row| crate::sha256::sha256_hex(&row.payload) == manifest.chunks[tail])
                .ok_or_else(|| {
                    Error::InvalidJournal(format!(
                        "chunk {tail} of {scope} does not match its manifest"
                    ))
                })?;

            (
                manifest.chunks[..tail].to_vec(),
                ChunkBuilder::resume(scope, &row.payload)?,
                Some(tail as u64),
            )
        }
    };

    // The fewest chunks the manifest can list: refused before any write when even those overflow
    // it, and checked again as each chunk closes, before it is written.
    let fewest = digests.len() as u64 + (through - builder.from()) / MAX_CHUNK_ROWS as u64 + 1;
    check_manifest_size(scope, through, fewest)?;

    let mut written = 0;
    let mut held_back = None;
    let mut after = floor;

    while after < through {
        let rows = run.store.read_rows(scope, after, MAX_PAGE_ROWS).await?;
        let mut progressed = false;

        for row in rows.into_iter().take_while(|row| row.position <= through) {
            if row.position != after + 1 {
                break;
            }

            if let Some(closed) = builder.push(&row)? {
                digests.push(closed.digest.clone());
                check_manifest_size(scope, through, digests.len() as u64 + 1)?;

                if Some(closed.index) == tail {
                    held_back = Some(closed);
                } else {
                    run.write(vec![run.chunk_write(&closed)]).await?;
                    written += 1;
                }
            }

            after = row.position;
            progressed = true;
        }

        if !progressed {
            return Err(Error::InvalidJournal(format!(
                "{scope} has no row at position {} to compact",
                after + 1
            )));
        }
    }

    let last = builder.build();
    digests.push(last.digest.clone());
    let mut swap = Vec::new();

    if Some(last.index) == tail {
        held_back = Some(last);
    } else {
        run.write(vec![run.chunk_write(&last)]).await?;
        written += 1;
    }

    let manifest = CompactionManifest {
        scope: scope.clone(),
        checkpoint: through,
        chunks: digests,
    };
    let payload = manifest.payload();

    swap.push(CompactionWrite::Put {
        row: compaction_view(scope, MANIFEST_SORT_KEY, payload),
        expect: old_payload.map_or(Expect::Absent, Expect::Payload),
    });

    // The rewritten last chunk of the old manifest, if it changed.
    if let Some(chunk) = held_back.filter(|chunk| {
        old.as_ref()
            .is_none_or(|old| old.chunks[chunk.index as usize] != chunk.digest)
    }) {
        swap.push(run.chunk_write(&chunk));
        written += 1;
    }

    run.write(swap).await?;

    Ok((manifest, written))
}

/// Refuses a manifest of `chunks` chunks through `through` that would exceed [`MAX_CHUNK_BYTES`]:
/// about 5,500 chunks. Compact a scope before it grows past that, or in smaller steps.
fn check_manifest_size(scope: &Scope, through: u64, chunks: u64) -> Result<(), Error> {
    let empty = CompactionManifest {
        scope: scope.clone(),
        checkpoint: through,
        chunks: Vec::new(),
    };
    // Each digest is 64 hex digits in quotes, with a comma between two.
    let size = empty.payload().len() as u64 + chunks * 67 - 1;

    if size > MAX_CHUNK_BYTES as u64 {
        return Err(Error::InvalidJournal(format!(
            "the manifest of {scope} through {through} would list {chunks} chunks in {size} bytes, \
             over {MAX_CHUNK_BYTES}"
        )));
    }

    Ok(())
}

/// Deletes every journal row of the scope at or below `through`, each by its owner, in batches.
async fn delete_rows<S: CompactionStore + ?Sized>(
    run: &mut Run<'_, S>,
    through: u64,
) -> Result<u64, Error> {
    let scope = &run.lease.scope.clone();
    let mut deleted = 0;
    let mut after = 0;

    loop {
        let rows: Vec<_> = run
            .store
            .read_rows(scope, after, DELETE_BATCH)
            .await?
            .into_iter()
            .take_while(|row| row.position <= through)
            .collect();

        let Some(last) = rows.last() else {
            return Ok(deleted);
        };
        after = last.position;

        let writes: Vec<CompactionWrite> = rows
            .iter()
            .map(|row| CompactionWrite::Delete {
                pk: scope.row_pk(),
                sk: row_sk(row.position),
                owner: row.owner.as_str().to_owned(),
            })
            .collect();
        deleted += writes.len() as u64;
        run.write(writes).await?;

        if after >= through {
            return Ok(deleted);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_manifest_over_the_byte_cap_is_refused_by_its_chunk_count() {
        let scope = Scope::new("world:7").unwrap();

        assert!(check_manifest_size(&scope, u64::MAX, 5_000).is_ok());
        assert!(matches!(
            check_manifest_size(&scope, u64::MAX, 6_000),
            Err(Error::InvalidJournal(_))
        ));

        // The estimate is the exact payload size.
        let manifest = CompactionManifest {
            scope: scope.clone(),
            checkpoint: 12,
            chunks: vec!["a".repeat(64); 3],
        };
        let empty = CompactionManifest {
            chunks: Vec::new(),
            ..manifest.clone()
        };
        assert_eq!(empty.payload().len() + 3 * 67 - 1, manifest.payload().len());
    }
}
