//! Journal compaction (ADR 0005, X2): the per-scope lease, the compaction run, and the snapshot
//! reads a client installs after a `410 Gone`.
//!
//! A run compacts a scope's journal through a checkpoint, in an order that keeps every reader
//! correct without stopping writers:
//!
//! 1. it builds the scope's **canonical chunks** of rows `1..=checkpoint` (resuming the previous
//!    manifest's chunks, so only the tail is rebuilt): a preflight streams the whole range and
//!    writes nothing, so a missing row or an oversized manifest is refused before any write, and a
//!    second pass writes the new chunks;
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
    LEASE_SORT_KEY, LeaseRow, MANIFEST_SORT_KEY, MAX_CHUNK_BYTES, MAX_COMPACTION_WRITES, audit_sk,
    check_compaction_row, chunk_sk, compaction_pk, compaction_view, released_payload,
};
use super::read::{JournalCompactionStore, MAX_PAGE_ROWS};
use super::scope::{Scope, row_sk};
use crate::error::Error;
use crate::features::{FeatureManifest, JOURNAL_COMPACTION_FEATURE, JOURNAL_COMPACTION_VERSION};
use crate::projection::ViewRow;
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

/// The compaction row at `sort_key`, if any, checked to be the engine's
/// ([`check_compaction_row`]): a row another writer put there (an application projector, a direct
/// view write) is [`Error::ForeignJournalRow`], never trusted.
async fn read_engine_row<S: JournalCompactionStore + ?Sized>(
    store: &S,
    scope: &Scope,
    sort_key: &str,
) -> Result<Option<ViewRow>, Error> {
    store
        .read_compaction_row(scope, sort_key)
        .await?
        .map(|row| check_compaction_row(scope, sort_key, row))
        .transpose()
}

/// The scope's manifest, if it has been compacted. Strongly consistent.
pub async fn read_manifest<S: JournalCompactionStore + ?Sized>(
    store: &S,
    scope: &Scope,
) -> Result<Option<CompactionManifest>, Error> {
    read_engine_row(store, scope, MANIFEST_SORT_KEY)
        .await?
        .map(|row| CompactionManifest::decode(scope, &row.payload))
        .transpose()
}

/// The scope's floor: the checkpoint of its manifest, `0` when it has none.
pub async fn read_floor<S: JournalCompactionStore + ?Sized>(
    store: &S,
    scope: &Scope,
) -> Result<u64, Error> {
    Ok(read_manifest(store, scope)
        .await?
        .map_or(0, |manifest| manifest.checkpoint))
}

/// Chunk `index` of `manifest`, checked against the manifest's digest for it.
///
/// A chunk that is missing or does not match is [`Error::JournalSnapshotMoved`]: a compaction
/// swapped the manifest since it was read. Read the manifest again and start over; it is
/// transient.
pub async fn read_chunk<S: JournalCompactionStore + ?Sized>(
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
    let row = read_engine_row(store, scope, &chunk_sk(index as u64))
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
pub async fn read_audit<S: JournalCompactionStore + ?Sized>(
    store: &S,
    scope: &Scope,
    checkpoint: u64,
) -> Result<Option<CompactionAudit>, Error> {
    read_engine_row(store, scope, &audit_sk(checkpoint))
        .await?
        .map(|row| CompactionAudit::decode(scope, &row.payload))
        .transpose()
}

/// The scope's lease row, if one was ever written.
pub async fn read_lease<S: JournalCompactionStore + ?Sized>(
    store: &S,
    scope: &Scope,
) -> Result<Option<LeaseRow>, Error> {
    read_engine_row(store, scope, LEASE_SORT_KEY)
        .await?
        .map(|row| LeaseRow::decode(scope, &row.payload))
        .transpose()
}

/// Reads the feature marker and refuses a table declaring a feature this build does not model.
async fn check_marker<S: CompactionStore + ?Sized>(
    store: &S,
) -> Result<Option<FeatureManifest>, Error> {
    let manifest = store.read_manifest().await?;

    if let Some(manifest) = &manifest {
        manifest.validate_supported()?;
    }

    Ok(manifest)
}

/// Declares `journal-compaction` on the feature marker before a run's first write, so a build that
/// does not know compaction (and would read a compacted scope's missing rows as lost, or allocate
/// below its floor) refuses the table. It runs only once the run's checkpoint and its range have
/// been checked, so a refused run leaves the table as it was. The marker is checked first
/// ([`check_marker`]).
async fn ensure_declared<S: CompactionStore + ?Sized>(store: &S) -> Result<(), Error> {
    let manifest = check_marker(store).await?;
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
/// refused while a rebuild holds or left the fence ([`Error::RebuildUnavailable`]), and on a table
/// declaring a feature this build does not model ([`Error::UnsupportedFeature`]). It declares
/// nothing: [`compact`] declares `journal-compaction` on the feature marker before its first
/// write. When its write loses a race it reads the lease again, a few times, and then fails
/// transiently.
pub async fn begin_compaction<S: CompactionStore + ?Sized>(
    store: &S,
    scope: &Scope,
    owner: &str,
    lease: Duration,
) -> Result<CompactionLease, Error> {
    let seconds = rebuild::check_lease(owner, lease)?;
    let epoch = observe_rebuild_epoch(store).await?;

    check_marker(store).await?;

    for _ in 0..MAX_LEASE_ATTEMPTS {
        let stored = read_engine_row(store, scope, LEASE_SORT_KEY).await?;
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

    let Some(row) = read_engine_row(store, scope, LEASE_SORT_KEY).await? else {
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
        self.keep().await?;

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

    /// Stops a worker whose lease lapsed by the repository's clock (it may be taken over), and
    /// renews a lease with less than half of its length left, in place. Called before every write
    /// and between the chunks of the preflight, which reads for as long as the range takes.
    async fn keep(&mut self) -> Result<(), Error> {
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

        Ok(())
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

    let (stored, old) = check_checkpoint(store, scope, through).await?;
    let floor = old.as_ref().map_or(0, |manifest| manifest.checkpoint);

    let (manifest, chunks_written) = if through > floor {
        build(&mut run, old, stored.map(|row| row.payload), through).await?
    } else {
        ensure_declared(store).await?;
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

/// Refuses a checkpoint outside `floor..=high watermark` (or 0) with [`Error::InvalidArgument`],
/// reading nothing but the scope's manifest and counter, and returns the stored manifest row and
/// the manifest it holds.
async fn check_checkpoint<S: CompactionStore + ?Sized>(
    store: &S,
    scope: &Scope,
    through: u64,
) -> Result<(Option<ViewRow>, Option<CompactionManifest>), Error> {
    let stored = read_engine_row(store, scope, MANIFEST_SORT_KEY).await?;
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

    Ok((stored, old))
}

/// [`compact`] holding a lease for the run: checks the rebuild fence and the feature marker, then
/// `through` against the scope's floor and high watermark, acquires the lease as `owner`
/// ([`begin_compaction`]), compacts, and releases it, also when the run fails (the run leaves the
/// scope consistent). A run refused before the lease takes none, and no refused run declares
/// anything.
pub async fn compact_with_lease<S: CompactionStore + ?Sized>(
    store: &S,
    scope: &Scope,
    through: u64,
    owner: &str,
    lease: Duration,
) -> Result<CompactionReport, Error> {
    // The fence and the marker first, so a rebuild in progress stays retriable and a table this
    // build does not model is refused before its compaction rows are read.
    observe_rebuild_epoch(store).await?;
    check_marker(store).await?;
    check_checkpoint(store, scope, through).await?;

    let mut held = begin_compaction(store, scope, owner, lease).await?;
    let result = compact(store, &mut held, through).await;

    match (result, end_compaction(store, &held).await) {
        (Ok(report), Ok(())) => Ok(report),
        (Ok(_), Err(err)) | (Err(err), _) => Err(err),
    }
}

/// The rows `floor + 1..=through` of a scope, chunked: every chunk the canonical rule closes,
/// then the last one. It resumes the previous manifest's last chunk, so closed chunks before it are
/// not rebuilt. It never writes.
struct Chunks<'a, S: ?Sized> {
    store: &'a S,
    scope: Scope,
    builder: Option<ChunkBuilder>,
    after: u64,
    through: u64,
    rows: std::collections::VecDeque<super::row::JournalRow>,
}

impl<'a, S: JournalCompactionStore + ?Sized> Chunks<'a, S> {
    /// Starts after `old`'s floor, from its last chunk (checked against its digest).
    async fn start(
        store: &'a S,
        scope: &Scope,
        old: Option<&CompactionManifest>,
        through: u64,
    ) -> Result<Self, Error> {
        let builder = match old {
            None => ChunkBuilder::new(scope, 0, 1),
            Some(manifest) => {
                let tail = manifest.chunks.len() - 1;
                let row = read_engine_row(store, scope, &chunk_sk(tail as u64))
                    .await?
                    .filter(|row| crate::sha256::sha256_hex(&row.payload) == manifest.chunks[tail])
                    .ok_or_else(|| {
                        Error::InvalidJournal(format!(
                            "chunk {tail} of {scope} does not match its manifest"
                        ))
                    })?;

                ChunkBuilder::resume(scope, &row.payload)?
            }
        };

        Ok(Self {
            store,
            scope: scope.clone(),
            builder: Some(builder),
            after: old.map_or(0, |manifest| manifest.checkpoint),
            through,
            rows: std::collections::VecDeque::new(),
        })
    }

    /// The next chunk, closed or the last, or `None` once the last was returned. A missing row is
    /// [`Error::InvalidJournal`].
    async fn next(&mut self) -> Result<Option<BuiltChunk>, Error> {
        loop {
            if self.builder.is_none() {
                return Ok(None);
            }

            if self.after >= self.through {
                return Ok(self.builder.take().map(ChunkBuilder::build));
            }

            if self.rows.is_empty() {
                let page = self
                    .store
                    .read_rows(&self.scope, self.after, MAX_PAGE_ROWS)
                    .await?;
                self.rows.extend(
                    page.into_iter()
                        .take_while(|row| row.position <= self.through),
                );
            }

            let row = match self.rows.pop_front() {
                Some(row) if row.position == self.after + 1 => row,
                _ => {
                    return Err(Error::InvalidJournal(format!(
                        "{} has no row at position {} to compact",
                        self.scope,
                        self.after + 1
                    )));
                }
            };
            self.after = row.position;

            let builder = self.builder.as_mut().expect("checked above");
            if let Some(closed) = builder.push(&row)? {
                return Ok(Some(closed));
            }
        }
    }
}

/// Plans the snapshot through `through` without writing anything: every chunk's digest, over the
/// full range, and the manifest, refused when a row is missing or the manifest would exceed
/// `limit` bytes. With a run, it keeps the run's lease between chunks ([`Run::keep`]): reading a
/// large range can outlast the lease.
async fn preflight<S: CompactionStore + ?Sized>(
    store: &S,
    scope: &Scope,
    old: Option<&CompactionManifest>,
    through: u64,
    limit: usize,
    mut run: Option<&mut Run<'_, S>>,
) -> Result<CompactionManifest, Error> {
    let mut digests = old.map_or_else(Vec::new, |manifest| {
        manifest.chunks[..manifest.chunks.len() - 1].to_vec()
    });
    let mut chunks = Chunks::start(store, scope, old, through).await?;

    while let Some(chunk) = chunks.next().await? {
        digests.push(chunk.digest);

        if let Some(run) = run.as_mut() {
            run.keep().await?;
        }
    }

    let manifest = CompactionManifest {
        scope: scope.clone(),
        checkpoint: through,
        chunks: digests,
    };
    let size = manifest.payload().len();
    if size > limit {
        return Err(Error::InvalidJournal(format!(
            "the manifest of {scope} through {through} would list {} chunks in {size} bytes, over \
             {limit}; compact in smaller steps",
            manifest.chunks.len()
        )));
    }

    Ok(manifest)
}

/// Builds the snapshot through `through` and swaps the manifest. Returns the new manifest and how
/// many chunks were written.
///
/// A preflight streams the whole range first and writes nothing: a missing row, or a manifest
/// over [`MAX_CHUNK_BYTES`], is refused before any write. A second pass then writes the chunks,
/// each checked against the preflight's digest. Closed chunks never change (the canonical rule
/// closes a chunk on rows before it only), so both passes resume the previous manifest's last
/// chunk. Chunks at new indexes are written first, unreferenced; the rewritten last chunk goes in
/// the same atomic write as the manifest, so a reader never finds a manifest whose chunks are not
/// there.
async fn build<S: CompactionStore + ?Sized>(
    run: &mut Run<'_, S>,
    old: Option<CompactionManifest>,
    old_payload: Option<Vec<u8>>,
    through: u64,
) -> Result<(CompactionManifest, usize), Error> {
    let scope = &run.lease.scope.clone();
    let store = run.store;
    let manifest = preflight(
        store,
        scope,
        old.as_ref(),
        through,
        MAX_CHUNK_BYTES,
        Some(run),
    )
    .await?;
    ensure_declared(store).await?;
    let tail = old.as_ref().map(|old| old.chunks.len() as u64 - 1);

    let mut written = 0;
    let mut held_back = None;
    let mut chunks = Chunks::start(run.store, scope, old.as_ref(), through).await?;

    while let Some(chunk) = chunks.next().await? {
        if manifest.chunks.get(chunk.index as usize) != Some(&chunk.digest) {
            return Err(Error::InvalidJournal(format!(
                "chunk {} of {scope} changed between the preflight and the write",
                chunk.index
            )));
        }

        if Some(chunk.index) == tail {
            held_back = Some(chunk);
        } else {
            run.write(vec![run.chunk_write(&chunk)]).await?;
            written += 1;
        }
    }

    let mut swap = vec![CompactionWrite::Put {
        row: compaction_view(scope, MANIFEST_SORT_KEY, manifest.payload()),
        expect: old_payload.map_or(Expect::Absent, Expect::Payload),
    }];

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

    use crate::MemRepository;
    use crate::journal::row_view;
    use crate::journal::scope::{Allocation, set_allocations};

    fn scope() -> Scope {
        Scope::new("world:7").unwrap()
    }

    /// A store holding the journal rows `positions` of the scope, written directly.
    async fn store_with(positions: &[u64]) -> MemRepository {
        let store = MemRepository::new();
        let epoch = crate::rebuild::observe_rebuild_epoch(&store).await.unwrap();
        let writes: Vec<CompactionWrite> = positions
            .iter()
            .map(|position| {
                let mut event = crate::SerializedEvent {
                    id: format!("acct-1:{position}").into(),
                    entity_id: "acct-1".into(),
                    payload: b"{}".to_vec(),
                    ..Default::default()
                };
                set_allocations(&mut event, &[Allocation::new(&scope(), *position)]);

                CompactionWrite::Put {
                    row: row_view(&scope(), *position, &event).unwrap(),
                    expect: Expect::Any,
                }
            })
            .collect();
        store.write_compaction(Some(epoch), &writes).await.unwrap();

        store
    }

    #[tokio::test]
    async fn a_preflight_plans_every_chunk_and_writes_nothing() {
        let store = store_with(&[1, 2, 3]).await;
        let manifest = preflight(&store, &scope(), None, 3, MAX_CHUNK_BYTES, None)
            .await
            .unwrap();

        assert_eq!((manifest.checkpoint, manifest.chunks.len()), (3, 1));
        assert!(store.views().iter().all(|row| row.pk == scope().row_pk()));
    }

    #[tokio::test]
    async fn a_preflight_keeps_the_run_lease_while_it_reads() {
        let clock = crate::conformance::TestClock::starting_now();
        let store = store_with(&[1, 2, 3]).await.with_clock(clock.reader());
        let mut lease = begin_compaction(&store, &scope(), "c", Duration::from_secs(10))
            .await
            .unwrap();
        let epoch = observe_rebuild_epoch(&store).await.unwrap();

        // Less than half the lease is left: the preflight renews it before any write.
        clock.advance(6);
        let mut run = Run {
            store: &store,
            lease: &mut lease,
            epoch,
        };
        preflight(&store, &scope(), None, 3, MAX_CHUNK_BYTES, Some(&mut run))
            .await
            .unwrap();
        assert_eq!(lease.lease_expires_at, clock.seconds() + 10);

        // A lease that lapsed during the reads stops the run before it writes.
        clock.advance(11);
        let mut run = Run {
            store: &store,
            lease: &mut lease,
            epoch,
        };
        assert!(matches!(
            preflight(&store, &scope(), None, 3, MAX_CHUNK_BYTES, Some(&mut run)).await,
            Err(Error::CompactionLeaseLost { .. })
        ));
    }

    #[tokio::test]
    async fn a_preflight_refuses_a_gap_or_a_manifest_over_its_limit() {
        let gapped = store_with(&[1, 2, 4]).await;
        assert!(matches!(
            preflight(&gapped, &scope(), None, 4, MAX_CHUNK_BYTES, None).await,
            Err(Error::InvalidJournal(_))
        ));

        // Over the limit: the manifest of one chunk is about 120 bytes.
        let store = store_with(&[1, 2]).await;
        assert!(matches!(
            preflight(&store, &scope(), None, 2, 100, None).await,
            Err(Error::InvalidJournal(message)) if message.contains("smaller steps")
        ));
    }
}
