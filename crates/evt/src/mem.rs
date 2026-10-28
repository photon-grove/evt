//! An in-memory repository for tests and local development.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use crate::error::Error;
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::features::{self, CachedMarker, FeatureManifest, FeatureMarker, ValidatedMarker};
use crate::heads::{self, Probe};
use crate::ids::{CommandId, EntityId, EntityType, EventSequence};
use crate::outcome::Outcome;
use crate::projection::{
    ProjectedView, ProjectorOperationError, ViewCondition, ViewOperationKind, ViewRow,
};
use crate::rebuild::{
    self, Clock, Plan, RebuildFence, RebuildGeneration, RebuildLease, RebuildLock, RebuildState,
};
use crate::repository::{Commit, ReadFence, Repository, TransactionCommit};

/// An in-memory [`Repository`]. Clones share the same storage.
///
/// Unlike Go's in-memory test double, it enforces optimistic concurrency the same way the
/// DynamoDB backend does, so tests catch conflicts: every stream has a head (ADR 0007) that a
/// commit raises and a read fence compares, and a failed commit is classified (drift, gap, then
/// conflict) exactly as on the live backends. A clone is an independent writer over the shared
/// storage: it keeps its own feature-marker cache, like a second handle on a live backend, so a
/// declaration one clone writes refuses the other on its next commit.
///
/// It always supports commit-wide view operations (v0.9, C7): rows live in a map keyed by
/// `(pk, sk)` that clones share, read back with [`view`](MemRepository::view) and
/// [`views`](MemRepository::views). A commit evaluates every operation's condition against the
/// rows as they were before the commit, applies nothing unless all hold, and reports the first
/// failed operation as [`Error::ProjectorOperation`].
///
/// It keeps the rebuild fence (v0.9, C10) like the live backends: it implements [`RebuildFence`]
/// over a lock and a generation that clones share, and every commit is refused with
/// [`Error::RebuildInProgress`] while a rebuild holds the lock. Leases read the repository's clock
/// ([`with_clock`](MemRepository::with_clock)).
#[derive(Debug, Default)]
pub struct MemRepository {
    streams: Arc<Mutex<BTreeMap<EntityId, Stream>>>,
    /// The view rows, keyed by `(pk, sk)`. Shared with clones, like the streams.
    views: Arc<Mutex<BTreeMap<(String, String), ViewRow>>>,
    /// The command-outcome rows, keyed by command ID. Shared with clones, like the streams.
    outcomes: Arc<Mutex<BTreeMap<CommandId, Outcome>>>,
    manifest: Arc<Mutex<Option<FeatureManifest>>>,
    /// The rebuild fence's lock row and generation (C10). Shared with clones, like the streams.
    rebuild: Arc<Mutex<RebuildState>>,
    /// The clock rebuild leases are read from.
    clock: Clock,
    /// The feature-marker version this handle validated, if it has written yet. Per handle, not
    /// shared with clones.
    validated: Mutex<CachedMarker>,
}

impl Clone for MemRepository {
    fn clone(&self) -> Self {
        Self {
            streams: self.streams.clone(),
            views: self.views.clone(),
            outcomes: self.outcomes.clone(),
            manifest: self.manifest.clone(),
            rebuild: self.rebuild.clone(),
            clock: self.clock.clone(),
            validated: Mutex::new(*self.validated()),
        }
    }
}

#[derive(Debug, Default)]
struct Stream {
    events: BTreeMap<EventSequence, SerializedEvent>,
    snapshot: Option<SerializedSnapshot>,
    /// The stream's head item: the highest event sequence ever committed to it, 0 while the
    /// stream has none. Compaction never touches it.
    head: EventSequence,
}

impl Stream {
    fn entity_type(&self) -> Option<&EntityType> {
        self.events
            .values()
            .next()
            .map(|event| &event.entity_type)
            .or_else(|| self.snapshot.as_ref().map(|snapshot| &snapshot.entity_type))
    }

    /// The derived head (H4): the larger of the highest event sequence and the snapshot's event
    /// sequence.
    fn derived_head(&self) -> EventSequence {
        let last_event = self.events.keys().next_back().copied().unwrap_or_default();
        let snapshot = self
            .snapshot
            .as_ref()
            .map(|s| s.event_sequence)
            .unwrap_or_default();

        last_event.max(snapshot)
    }

    fn holds_data(&self) -> bool {
        !self.events.is_empty() || self.snapshot.is_some()
    }

    /// Classifies a failed head update or fence the way the failure-path probe does: the stored
    /// head against the derived head. Memory has no concurrent reads, so deriving again changes
    /// nothing and a stored head ahead of the log is always drift.
    fn probe(&self, entity_id: &EntityId, healthy: impl FnOnce(EventSequence) -> Error) -> Error {
        let (stored, derived) = (self.head, self.derived_head());
        let verdict = match Probe::first(stored, derived) {
            Probe::Ahead => Probe::after_rederive(stored, self.derived_head()),
            other => other,
        };

        match verdict {
            Probe::Settled if stored == derived => healthy(stored),
            _ => heads::drift(entity_id, stored, derived),
        }
    }
}

impl MemRepository {
    /// Creates an empty repository.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads `clock` instead of the system time for rebuild leases (see [`RebuildFence`]). For
    /// tests: a lease expires without waiting for it. Clones made afterwards share the clock.
    #[must_use]
    pub fn with_clock(mut self, clock: impl Fn() -> SystemTime + Send + Sync + 'static) -> Self {
        self.clock = Clock::new(clock);
        self
    }

    /// Appends an event to its stream **without** touching the stream's head, as a writer that
    /// predates stream heads (or a corrupted store) would. The stream then drifts (ADR 0007): the
    /// next commit, fence, or snapshot write that probes it fails with
    /// [`Error::StreamHeadDrift`]. For the conformance suite; nothing else should call it.
    #[doc(hidden)]
    #[cfg(any(test, feature = "conformance"))]
    pub fn append_bypassing_head(&self, event: SerializedEvent) {
        self.lock()
            .entry(event.entity_id.clone())
            .or_default()
            .events
            .insert(event.sequence, event);
    }

    /// Deletes the stream's head item, leaving its events and snapshot. For the conformance
    /// suite.
    #[doc(hidden)]
    #[cfg(any(test, feature = "conformance"))]
    pub fn delete_head(&self, entity_id: &EntityId) {
        if let Some(stream) = self.lock().get_mut(entity_id) {
            stream.head = EventSequence::ZERO;
        }
    }

    /// The stream's stored head, if it has a head item. For the conformance suite.
    #[doc(hidden)]
    #[cfg(any(test, feature = "conformance"))]
    pub fn stored_head(&self, entity_id: &EntityId) -> Option<EventSequence> {
        self.lock()
            .get(entity_id)
            .map(|stream| stream.head)
            .filter(|head| *head > EventSequence::ZERO)
    }

    /// Replaces the rebuild fence's lock and generation, as if a writer had stored them, for the
    /// shared fixtures that start an operation from a given state. Nothing else should call it.
    #[doc(hidden)]
    #[cfg(any(test, feature = "conformance"))]
    pub fn set_rebuild_state(&self, state: RebuildState) {
        *self.rebuild_lock() = state;
    }

    /// The view row at `(pk, sk)`, if a commit-wide view operation wrote one.
    pub fn view(&self, pk: &str, sk: &str) -> Option<ViewRow> {
        self.views_lock()
            .get(&(pk.to_owned(), sk.to_owned()))
            .cloned()
    }

    /// Every view row, ordered by `pk` then `sk` (bytewise).
    pub fn views(&self) -> Vec<ViewRow> {
        self.views_lock().values().cloned().collect()
    }

    fn views_lock(&self) -> MutexGuard<'_, BTreeMap<(String, String), ViewRow>> {
        self.views
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<EntityId, Stream>> {
        // A panic while holding the lock cannot leave a stream half-written: commits validate
        // before mutating. Recover the data rather than poisoning every later test.
        self.streams
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn outcomes_lock(&self) -> MutexGuard<'_, BTreeMap<CommandId, Outcome>> {
        self.outcomes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn manifest_lock(&self) -> MutexGuard<'_, Option<FeatureManifest>> {
        self.manifest
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn rebuild_lock(&self) -> MutexGuard<'_, RebuildState> {
        self.rebuild
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn validated(&self) -> MutexGuard<'_, CachedMarker> {
        self.validated
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Re-validates the writer's cached marker state against the stored manifest. Every commit
    /// checks the version, as the live backends' condition does: a manifest that changed under
    /// this writer is re-validated here, refusing an unsupported declared feature, and a writer
    /// requires `stream-heads` (ADR 0007).
    ///
    /// A manifest without the feature is declared by the writer itself, but only when the
    /// repository holds no events and no snapshots; otherwise the write is refused with
    /// [`Error::StreamHeadsNotMigrated`]. The caller holds the manifest and stream locks, so the
    /// emptiness check and the declaration are atomic.
    fn check_manifest(
        &self,
        stored: &mut Option<FeatureManifest>,
        streams: &BTreeMap<EntityId, Stream>,
    ) -> Result<(), Error> {
        let mut validated = self.validated();

        if features::marker_matches(*validated, stored.as_ref()) {
            return Ok(());
        }

        match features::validate_marker_for_write(stored.as_ref()) {
            Ok(marker) => *validated = Some(marker),
            Err(Error::StreamHeadsNotMigrated) if !streams.values().any(Stream::holds_data) => {
                let mut next = stored.clone().unwrap_or_default();
                next.declare_stream_heads();
                next.version = stored.as_ref().map_or(1, |manifest| manifest.version + 1);

                *validated = Some(ValidatedMarker::Version(next.version));
                *stored = Some(next);
            }
            Err(error) => return Err(error),
        }

        Ok(())
    }

    /// Writes the streams, fences, view operations, and outcome of one commit atomically. Streams
    /// are in ascending entity-ID order, as `TransactionCommit::normalize` leaves them.
    fn write(
        &self,
        streams: &[&Commit<Infallible>],
        fences: &[ReadFence],
        views: &[ProjectedView],
        outcome: Option<Outcome>,
        check_retention: impl FnOnce(Option<&FeatureManifest>) -> Result<(), Error>,
    ) -> Result<(), Error> {
        // Both locks are held through the write, so a manifest change cannot slip between the
        // version check and the events, mirroring the live backends' transaction condition.
        let mut stored_manifest = self.manifest_lock();
        let mut stored = self.lock();
        self.check_manifest(&mut stored_manifest, &stored)?;

        // Fences are refused for entity types durably declared with retention.
        check_retention(stored_manifest.as_ref())?;

        // The rebuild-lock check (C10), second only to the marker: an unsettled generation — any
        // lock, an expired one included, or a force-release no rebuild has finished since —
        // refuses the commit before the outcome row and every stream is looked at. The guard is
        // held through the write, so no rebuild can begin between the check and it.
        let rebuild = self.rebuild_lock();
        if rebuild.lock.is_some() || !rebuild.generation.is_settled() {
            return Err(Error::RebuildInProgress);
        }

        let mut outcomes = self.outcomes_lock();

        // A recorded outcome outranks every other failure: the command already ran.
        if let Some(outcome) = &outcome
            && outcomes.contains_key(&outcome.command_id)
        {
            return Err(Error::OutcomeConflict {
                command_id: outcome.command_id.clone(),
            });
        }

        // Validate every stream, snapshot, and fence before writing anything, so a failure leaves
        // every stream unchanged. Reads use get: a failed commit must not create empty streams.
        // A stream with no stored rows validates against an empty one. Every failed item is
        // evaluated before ranking, as on DynamoDB.
        let absent = Stream::default();
        let mut failures = Vec::new();

        for stream in streams.iter().filter(|stream| !stream.events.is_empty()) {
            let held = stored.get(&stream.entity_id).unwrap_or(&absent);
            let first = stream.events[0].sequence;

            // The head update: a create needs no head item, an append needs the head to be the
            // sequence before the commit's first.
            let head_passes = if first <= EventSequence(1) {
                held.head == EventSequence::ZERO
            } else {
                held.head.0 == first.0 - 1
            };
            let events_pass = stream
                .events
                .iter()
                .all(|event| !held.events.contains_key(&event.sequence));

            if !head_passes {
                failures.push(held.probe(&stream.entity_id, |stored| {
                    heads::gap_or_conflict(&stream.entity_id, first, stored)
                }));
            } else if !events_pass {
                // The head update passed and an event put failed: an event exists above the head.
                failures.push(heads::drift(
                    &stream.entity_id,
                    held.head,
                    held.derived_head(),
                ));
            } else if let Some(new) = &stream.snapshot {
                let stored_sequence = held
                    .snapshot
                    .as_ref()
                    .map_or(EventSequence::ZERO, |existing| existing.sequence);

                if stored_sequence.next() != new.sequence {
                    failures.push(Error::Conflict {
                        entity_id: stream.entity_id.clone(),
                        message: format!(
                            "snapshot sequence moved: stored {stored_sequence}, commit writes {}",
                            new.sequence
                        ),
                    });
                }
            }
        }

        for fence in fences {
            if streams
                .iter()
                .any(|stream| !stream.events.is_empty() && stream.entity_id == fence.entity_id)
            {
                // A written stream's head update supersedes its fence.
                continue;
            }

            let held = stored.get(&fence.entity_id).unwrap_or(&absent);
            if held.head != fence.head {
                failures.push(held.probe(&fence.entity_id, |_| heads::fence_conflict(fence)));
            }
        }

        if let Some(error) = heads::rank(failures) {
            return Err(error);
        }

        // View conditions rank last: a projector failure is reported only when nothing ranked
        // above it failed.
        let mut rows = self.views_lock();
        check_view_conditions(&rows, views)?;

        for stream in streams.iter().filter(|stream| !stream.events.is_empty()) {
            let last_sequence = stream.events.last().map(|event| event.sequence);
            let held = stored.entry(stream.entity_id.clone()).or_default();

            for event in &stream.events {
                held.events.insert(event.sequence, event.clone());
            }
            if let Some(last) = last_sequence {
                held.head = held.head.max(last);
            }

            if let (Some(snapshot), Some(event_sequence)) = (&stream.snapshot, last_sequence) {
                held.snapshot = Some(SerializedSnapshot {
                    entity_type: snapshot.entity_type.clone(),
                    entity_id: stream.entity_id.clone(),
                    sequence: snapshot.sequence,
                    event_sequence,
                    payload: snapshot.payload.clone(),
                });
            }
        }

        apply_views(&mut rows, views);

        outcomes.extend(outcome.map(|outcome| (outcome.command_id.clone(), outcome)));

        Ok(())
    }
}

/// Evaluates every view operation's condition against the rows as they were before the write, in
/// order, and reports the first failure. Every write is conditioned on the row's owner, as on
/// DynamoDB: the stored `entity_id` is the owner, so an operation by another owner fails whatever
/// its own condition.
fn check_view_conditions(
    rows: &BTreeMap<(String, String), ViewRow>,
    views: &[ProjectedView],
) -> Result<(), Error> {
    for view in views {
        let stored = rows.get(&(view.op.pk.clone(), view.op.sk.clone()));
        let owned = stored.is_none_or(|row| row.entity_id == view.op.owner.as_str());
        let held = match view.op.condition {
            ViewCondition::None => owned,
            ViewCondition::Absent => stored.is_none(),
            ViewCondition::Present => stored.is_some() && owned,
        } && view
            .op
            .expected_payload
            .as_ref()
            .is_none_or(|expected| stored.is_some_and(|row| owned && row.payload == *expected));

        if !held {
            return Err(Error::ProjectorOperation(Box::new(
                ProjectorOperationError::condition_failed(view),
            )));
        }
    }

    Ok(())
}

/// Applies view operations whose conditions [`check_view_conditions`] accepted.
fn apply_views(rows: &mut BTreeMap<(String, String), ViewRow>, views: &[ProjectedView]) {
    for view in views {
        let key = (view.op.pk.clone(), view.op.sk.clone());

        match view.op.kind {
            ViewOperationKind::Put => {
                rows.insert(key, ViewRow::of(&view.op));
            }
            ViewOperationKind::Delete => {
                rows.remove(&key);
            }
        }
    }
}

impl Repository for MemRepository {
    type Operation = Infallible;

    fn writes_views(&self) -> bool {
        true
    }

    fn applies_expected_payload(&self) -> bool {
        true
    }

    async fn declare_history_dependent(&self, types: &[EntityType]) -> Result<(), Error> {
        crate::features::declare_history(self, types).await
    }

    async fn commit(&self, commit: Commit<Infallible>) -> Result<(), Error> {
        commit.validate()?;
        features::check_not_reserved(&commit.entity_id)?;
        if commit.events.is_empty() && commit.outcome.is_none() {
            return Ok(());
        }

        // Budget parity: an oversized commit fails identically on every backend, before any
        // write. View operations are sized by the shared rule.
        let mut sizes = commit.sizes(|_| None)?;
        sizes.extend(commit.view_sizes());
        crate::repository::check_budget(sizes)?;

        self.write(
            &[&commit],
            &[],
            &commit.views,
            commit.stored_outcome(),
            |_| Ok(()),
        )
    }

    async fn commit_transaction(&self, commit: TransactionCommit<Infallible>) -> Result<(), Error> {
        let mut commit = commit;
        commit.normalize()?;

        if commit.is_empty() {
            return Ok(());
        }

        // Budget parity: an oversized commit fails identically on every backend.
        let mut sizes = commit.sizes(|_| None)?;
        sizes.extend(commit.view_sizes());
        crate::repository::check_budget(sizes)?;

        let streams: Vec<&Commit<Infallible>> = commit.streams.iter().collect();

        self.write(
            &streams,
            &commit.fences,
            &commit.views,
            commit.stored_outcome(),
            |manifest| commit.check_retention(manifest, |_| false),
        )
    }

    async fn outcome(&self, command_id: &CommandId) -> Result<Option<Outcome>, Error> {
        // The same validation as the durable backends: a bad ID is a programmer error, not an
        // absent row.
        crate::outcome::check_command_id(command_id)?;

        Ok(self.outcomes_lock().get(command_id).cloned())
    }

    async fn events(&self, entity_id: &EntityId) -> Result<Vec<SerializedEvent>, Error> {
        self.events_after(entity_id, EventSequence::ZERO).await
    }

    async fn events_after(
        &self,
        entity_id: &EntityId,
        after: EventSequence,
    ) -> Result<Vec<SerializedEvent>, Error> {
        let streams = self.lock();

        Ok(streams
            .get(entity_id)
            .map(|stream| {
                stream
                    .events
                    .range((Bound::Excluded(after), Bound::Unbounded))
                    .map(|(_, e)| e.clone())
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn latest(&self, entity_id: &EntityId, n: usize) -> Result<Vec<SerializedEvent>, Error> {
        if n == 0 {
            return Err(Error::InvalidArgument(
                "latest(n) requires at least one event: got n = 0".into(),
            ));
        }

        let streams = self.lock();

        Ok(streams
            .get(entity_id)
            .map(|stream| stream.events.values().rev().take(n).cloned().collect())
            .unwrap_or_default())
    }

    async fn snapshot(&self, entity_id: &EntityId) -> Result<Option<SerializedSnapshot>, Error> {
        Ok(self
            .lock()
            .get(entity_id)
            .and_then(|stream| stream.snapshot.clone()))
    }

    async fn put_snapshot(&self, snapshot: &SerializedSnapshot) -> Result<bool, Error> {
        features::check_not_reserved(&snapshot.entity_id)?;

        let mut manifest = self.manifest_lock();
        let mut streams = self.lock();
        self.check_manifest(&mut manifest, &streams)?;

        // H3: the stream's head must cover the snapshot. A snapshot of an empty stream (event
        // sequence 0) may have no head item at all.
        let held = streams.get(&snapshot.entity_id);
        let head = held.map_or(EventSequence::ZERO, |stream| stream.head);
        if snapshot.event_sequence > EventSequence::ZERO && head < snapshot.event_sequence {
            let derived = held
                .map_or(EventSequence::ZERO, Stream::derived_head)
                .max(snapshot.event_sequence);

            return Err(heads::drift(&snapshot.entity_id, head, derived));
        }

        if held
            .and_then(|stream| stream.snapshot.as_ref())
            .is_some_and(|existing| existing.event_sequence > snapshot.event_sequence)
        {
            return Ok(false);
        }

        streams
            .entry(snapshot.entity_id.clone())
            .or_default()
            .snapshot = Some(snapshot.clone());

        Ok(true)
    }

    async fn compact_below(
        &self,
        entity_id: &EntityId,
        through: EventSequence,
    ) -> Result<usize, Error> {
        features::check_not_reserved(entity_id)?;
        if through < EventSequence(1) {
            return Ok(0);
        }

        let manifest = self.manifest_lock();
        // Compaction is a write path: a store that does not declare `stream-heads` (or declares a
        // feature this build does not model) is refused before the stream is read, so the refusal
        // wins over `CompactionUncovered`. It never declares the feature itself, and never
        // touches a head.
        features::validate_marker_for_write(manifest.as_ref())?;

        let mut streams = self.lock();
        let Some(stream) = streams.get_mut(entity_id) else {
            return Err(Error::CompactionUncovered {
                entity_id: entity_id.clone(),
                through,
                covered: None,
            });
        };

        let covered = stream
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.event_sequence);
        if covered.is_none_or(|covered| covered < through) {
            return Err(Error::CompactionUncovered {
                entity_id: entity_id.clone(),
                through,
                covered,
            });
        }

        if let Some(manifest) = manifest.as_ref()
            && let Some(entity_type) = stream.entity_type()
            && manifest.is_history_dependent(entity_type)
        {
            return Err(Error::HistoryDependentProjection {
                entity_type: entity_type.clone(),
                entity_id: entity_id.clone(),
            });
        }

        let before = stream.events.len();
        stream.events.retain(|sequence, _| *sequence > through);

        Ok(before - stream.events.len())
    }

    async fn entity_heads(
        &self,
        entity_type: Option<&EntityType>,
    ) -> Result<BTreeMap<EntityId, EventSequence>, Error> {
        let streams = self.lock();

        Ok(streams
            .iter()
            .filter(|(id, _)| !features::is_reserved(id))
            .filter(|(_, stream)| {
                entity_type.is_none_or(|wanted| stream.entity_type() == Some(wanted))
            })
            .map(|(id, stream)| (id.clone(), stream.derived_head()))
            .filter(|(_, head)| *head > EventSequence::ZERO)
            .collect())
    }
}

impl FeatureMarker for MemRepository {
    async fn read_manifest(&self) -> Result<Option<FeatureManifest>, Error> {
        Ok(self.manifest_lock().clone())
    }

    async fn update_manifest(
        &self,
        change: impl Fn(&mut FeatureManifest) + Send,
    ) -> Result<(), Error> {
        let mut stored = self.manifest_lock();
        let mut next = stored.clone().unwrap_or_default();
        change(&mut next);
        next.version = stored.as_ref().map_or(1, |manifest| manifest.version + 1);
        *stored = Some(next);

        // Declaring is not validation: the resulting manifest may be unsupported by this writer.
        *self.validated() = None;

        Ok(())
    }

    /// Declares `stream-heads` when the repository holds no events and no snapshots, as its first
    /// writer would. The in-memory repository always initializes a fresh store on the first write,
    /// so this is only needed by code that treats every backend alike.
    async fn initialize(&self) -> Result<bool, Error> {
        // The manifest lock first, then the streams, as every write path takes them.
        let mut stored = self.manifest_lock();
        let streams = self.lock();

        if let Some(manifest) = stored.as_ref() {
            manifest.validate_supported()?;
            if manifest.declares_stream_heads() {
                return Ok(false);
            }
        }

        if streams.values().any(Stream::holds_data) {
            return Err(Error::StreamHeadsNotMigrated);
        }

        let mut next = stored.clone().unwrap_or_default();
        next.declare_stream_heads();
        next.version = stored.as_ref().map_or(1, |manifest| manifest.version + 1);
        *stored = Some(next);
        *self.validated() = None;

        Ok(true)
    }
}

/// The rebuild fence over the shared lock and generation. Every operation runs under the fence's
/// mutex, so its read and its write are one step, as the live backends' conditional writes are.
impl RebuildFence for MemRepository {
    async fn rebuild_generation(&self) -> Result<RebuildGeneration, Error> {
        Ok(self.rebuild_lock().generation)
    }

    async fn rebuild_state(&self) -> Result<RebuildState, Error> {
        Ok(self.rebuild_lock().clone())
    }

    async fn begin_rebuild(&self, owner: &str, lease: Duration) -> Result<RebuildLease, Error> {
        let seconds = rebuild::check_lease(owner, lease)?;

        let mut state = self.rebuild_lock();
        let now = self.clock.unix_seconds();
        let plan = Plan::decide(&state, now)?;
        let lease_expires_at = now.saturating_add(seconds);

        // The acquire and the takeover leave the force-release flag as it was: only a release
        // clears it. The lock stores the generation they produce as its fencing token.
        state.generation.value += plan.increment;
        state.lock = Some(RebuildLock {
            owner: owner.to_owned(),
            generation: state.generation.value,
            lease_expires_at,
        });

        Ok(RebuildLease {
            owner: owner.to_owned(),
            generation: state.generation.value,
            lease_expires_at,
        })
    }

    async fn renew_rebuild(
        &self,
        lease: &RebuildLease,
        extend: Duration,
    ) -> Result<RebuildLease, Error> {
        let seconds = rebuild::check_lease(&lease.owner, extend)?;

        let mut state = self.rebuild_lock();
        let lease_expires_at = self.clock.unix_seconds().saturating_add(seconds);
        let Some(lock) = state.lock.as_mut().filter(|lock| lease.holds(lock)) else {
            return Err(lease_lost(&lease.owner));
        };

        lock.lease_expires_at = lease_expires_at;

        Ok(RebuildLease {
            lease_expires_at,
            ..lease.clone()
        })
    }

    async fn end_rebuild(&self, lease: &RebuildLease) -> Result<RebuildGeneration, Error> {
        rebuild::check_owner(&lease.owner)?;

        self.release(&lease.owner, |lock| lease.holds(lock), 1, false)
    }

    async fn force_release_rebuild(&self, owner: &str) -> Result<RebuildGeneration, Error> {
        rebuild::check_owner(owner)?;

        self.release(owner, |lock| lock.owner == owner, 2, true)
    }

    async fn write_rebuild_views(
        &self,
        lease: &RebuildLease,
        views: &[ProjectedView],
    ) -> Result<(), Error> {
        rebuild::check_owner(&lease.owner)?;
        rebuild::check_view_batch(views)?;

        // The fence's mutex is held through the write, so a takeover cannot slip between the
        // guard and the rows, like the row lock PostgreSQL holds.
        let state = self.rebuild_lock();
        let now = self.clock.unix_seconds();
        if !state
            .lock
            .as_ref()
            .is_some_and(|lock| lease.holds(lock) && lock.lease_expires_at > now)
        {
            return Err(lease_lost(&lease.owner));
        }

        let mut rows = self.views_lock();
        check_view_conditions(&rows, views)?;
        apply_views(&mut rows, views);

        Ok(())
    }
}

impl MemRepository {
    /// A release (`increment` 1, flag cleared) or a force-release (`increment` 2, flag set) of the
    /// lock `held` accepts: the lease's (owner and fencing token) or, for a force-release, the
    /// named owner's.
    fn release(
        &self,
        owner: &str,
        held: impl FnOnce(&RebuildLock) -> bool,
        increment: i64,
        force_released: bool,
    ) -> Result<RebuildGeneration, Error> {
        let mut state = self.rebuild_lock();
        if !state.lock.as_ref().is_some_and(held) {
            return Err(lease_lost(owner));
        }

        state.lock = None;
        state.generation = RebuildGeneration {
            value: state.generation.value + increment,
            force_released,
        };

        Ok(state.generation)
    }
}

fn lease_lost(owner: &str) -> Error {
    Error::RebuildLeaseLost {
        owner: owner.to_owned(),
    }
}

mod enumerate;
#[cfg(feature = "journal")]
mod journal;
mod views;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::{
        Suite, commit_view_cases, feature_marker_cases, fresh_store_cases, initialization_cases,
        outcome_cases,
    };

    #[tokio::test]
    async fn conforms() {
        let repository = MemRepository::new();

        Suite::new(&repository)
            .with_head_fixtures(repository.clone())
            .run()
            .await;
    }

    #[tokio::test]
    async fn conforms_to_commit_wide_views() {
        let repository = MemRepository::new();

        commit_view_cases(&repository, &repository.clone()).await;
    }

    #[tokio::test]
    async fn conforms_to_the_view_repository() {
        let repository = MemRepository::new();

        crate::conformance::view_repository_cases(&repository).await;
        crate::conformance::committed_view_cases(&repository, &repository.clone()).await;
    }

    #[tokio::test]
    async fn a_fresh_store_declares_stream_heads_on_the_first_write() {
        let first = MemRepository::new();

        fresh_store_cases(&first, &first.clone()).await;
    }

    #[tokio::test]
    async fn an_explicitly_initialized_store_takes_writes() {
        let store = MemRepository::new();

        // The in-memory repository initializes a fresh store on its first write, so it does not
        // refuse an uninitialized one.
        initialization_cases(&store, &store.clone(), &store.clone(), false).await;
    }

    #[tokio::test]
    async fn conforms_to_command_outcomes() {
        let first = MemRepository::new();

        // The clone is a second writer over the same storage.
        outcome_cases(&first, &first.clone()).await;
    }

    #[tokio::test]
    async fn conforms_to_the_rebuild_fence() {
        let clock = crate::conformance::TestClock::starting_now();
        let repository = MemRepository::new().with_clock(clock.reader());

        crate::conformance::RebuildSuite::new(&repository, &clock)
            .with_views(repository.clone())
            .with_head_fixtures(repository.clone())
            .run()
            .await;
        crate::conformance::projection_rebuild_cases(&repository, &clock, &repository).await;
    }

    #[tokio::test]
    async fn conforms_to_the_feature_marker() {
        let first = MemRepository::new();

        // The clone shares the storage but keeps its own marker cache, so the cases see two
        // writers.
        feature_marker_cases(&first, &first.clone()).await;
    }
    // ----- commit-wide view rows (v0.9, C7) -----

    use crate::projection::{CommitProjector, CommitView, ViewOperation};
    use crate::{BoxError, ProjectedView};

    fn event(id: &str, sequence: i64) -> SerializedEvent {
        SerializedEvent {
            id: format!("{id}:{sequence}").into(),
            sequence: EventSequence(sequence),
            event_type: "account:credited".into(),
            version: crate::EventVersion(1),
            entity_id: id.into(),
            entity_type: "account".into(),
            payload: b"{}".to_vec(),
            ..Default::default()
        }
    }

    fn view(operation: usize, op: ViewOperation) -> ProjectedView {
        ProjectedView::new("projector", 0, operation, op)
    }

    #[tokio::test]
    async fn view_rows_are_shared_with_clones_and_ordered_by_key_bytes() {
        let repository = MemRepository::new();
        let clone = repository.clone();

        repository
            .commit(Commit::new("a", vec![event("a", 1)]).with_views([
                view(0, ViewOperation::put("b", "2", "id", "type", "{\"n\":2}")),
                view(1, ViewOperation::put("B", "1", "o", "", "{}")),
                view(2, ViewOperation::put("b", "1", "o", "", "{}")),
            ]))
            .await
            .unwrap();

        // Bytewise: "B" (0x42) sorts before "b" (0x62), then by sort key.
        let keys: Vec<_> = clone
            .views()
            .into_iter()
            .map(|row| (row.pk, row.sk))
            .collect();
        assert_eq!(
            keys,
            [
                ("B".into(), "1".into()),
                ("b".into(), "1".into()),
                ("b".into(), "2".into())
            ]
        );

        let row = clone.view("b", "2").unwrap();
        assert_eq!(
            (
                row.entity_id.as_str(),
                row.entity_type.as_str(),
                row.payload.as_slice()
            ),
            ("id", "type", br#"{"n":2}"#.as_slice())
        );
        assert!(clone.view("b", "3").is_none());
    }

    #[tokio::test]
    async fn a_failed_view_condition_applies_nothing_of_the_commit() {
        let repository = MemRepository::new();
        repository
            .commit(
                Commit::new("a", vec![event("a", 1)])
                    .with_views([view(0, ViewOperation::put("claims", "ada", "o", "", "{}"))]),
            )
            .await
            .unwrap();

        // The first operation would overwrite and the third would write, but the second fails its
        // condition: no row changes, no event is stored, and the second is the one named.
        let result = repository
            .commit_transaction(
                TransactionCommit::new(vec![Commit::new("b", vec![event("b", 1)])]).with_views([
                    view(0, ViewOperation::put("claims", "other", "o", "", "{}")),
                    view(
                        1,
                        ViewOperation::put("claims", "ada", "o", "", "{}").if_absent(),
                    ),
                    view(
                        2,
                        ViewOperation::delete("claims", "missing", "o").if_present(),
                    ),
                ]),
            )
            .await;

        match result {
            Err(Error::ProjectorOperation(failed)) => {
                assert_eq!(
                    (failed.operation, failed.reason.as_str()),
                    (1, "ConditionalCheckFailed")
                );
            }
            other => panic!("expected a projector operation failure, got {other:?}"),
        }
        assert_eq!(repository.views().len(), 1);
        assert!(repository.events(&"b".into()).await.unwrap().is_empty());

        // Conditions see the rows as they were before the commit: an unconditional put and an
        // absent put of the same key cannot both be in one commit (the engine refuses it), so a
        // delete and a put of different keys apply together.
        repository
            .commit(Commit::new("b", vec![event("b", 1)]).with_views([
                view(0, ViewOperation::delete("claims", "ada", "o").if_present()),
                view(
                    1,
                    ViewOperation::put("claims", "grace", "o", "", "{}").if_absent(),
                ),
            ]))
            .await
            .unwrap();
        let keys: Vec<_> = repository.views().into_iter().map(|row| row.sk).collect();
        assert_eq!(keys, ["grace"]);
    }

    #[tokio::test]
    async fn a_stream_conflict_outranks_a_failed_view_condition_and_applies_nothing() {
        let repository = MemRepository::new();
        repository
            .commit(
                Commit::new("a", vec![event("a", 1)])
                    .with_views([view(0, ViewOperation::put("claims", "ada", "o", "", "{}"))]),
            )
            .await
            .unwrap();

        let result = repository
            .commit(Commit::new("a", vec![event("a", 1)]).with_views([
                view(0, ViewOperation::put("claims", "new", "o", "", "{}")),
                view(
                    1,
                    ViewOperation::put("claims", "ada", "o", "", "{}").if_absent(),
                ),
            ]))
            .await;

        assert!(
            matches!(&result, Err(error) if error.is_conflict()),
            "{result:?}"
        );
        assert_eq!(repository.views().len(), 1);
    }

    struct Directory;

    impl CommitProjector for Directory {
        fn name(&self) -> &str {
            "directory"
        }

        fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
            Ok(commit
                .streams
                .iter()
                .map(|stream| {
                    ViewOperation::put(
                        "directory",
                        stream.entity_id.as_str(),
                        stream.entity_id.as_str(),
                        stream.entity_type.as_str(),
                        stream.state.clone(),
                    )
                })
                .collect())
        }
    }

    /// A repository written before v0.9: it commits through the in-memory one but declares neither
    /// view writing (unless `VIEWS`) nor history declaration.
    struct Bare<const VIEWS: bool>(MemRepository);

    impl<const VIEWS: bool> Repository for Bare<VIEWS> {
        type Operation = Infallible;

        fn writes_views(&self) -> bool {
            VIEWS
        }

        async fn commit(&self, commit: Commit<Infallible>) -> Result<(), Error> {
            self.0.commit(commit).await
        }

        async fn events(&self, entity_id: &EntityId) -> Result<Vec<SerializedEvent>, Error> {
            self.0.events(entity_id).await
        }

        async fn events_after(
            &self,
            entity_id: &EntityId,
            after: EventSequence,
        ) -> Result<Vec<SerializedEvent>, Error> {
            self.0.events_after(entity_id, after).await
        }

        async fn latest(
            &self,
            entity_id: &EntityId,
            n: usize,
        ) -> Result<Vec<SerializedEvent>, Error> {
            self.0.latest(entity_id, n).await
        }

        async fn snapshot(
            &self,
            entity_id: &EntityId,
        ) -> Result<Option<SerializedSnapshot>, Error> {
            self.0.snapshot(entity_id).await
        }

        async fn put_snapshot(&self, snapshot: &SerializedSnapshot) -> Result<bool, Error> {
            self.0.put_snapshot(snapshot).await
        }

        async fn compact_below(
            &self,
            entity_id: &EntityId,
            through: EventSequence,
        ) -> Result<usize, Error> {
            self.0.compact_below(entity_id, through).await
        }

        async fn entity_heads(
            &self,
            entity_type: Option<&EntityType>,
        ) -> Result<BTreeMap<EntityId, EventSequence>, Error> {
            self.0.entity_heads(entity_type).await
        }
    }

    #[tokio::test]
    async fn a_repository_that_does_not_write_views_is_refused_before_any_write() {
        use crate::conformance::{Add, Ledger};
        use crate::{Metadata, Store};

        let inner = MemRepository::new();
        let store = Store::<Ledger, _>::new(Bare::<false>(inner.clone()), ())
            .with_commit_projector(std::sync::Arc::new(Directory));

        let error = store
            .execute("one", Add(1), Metadata::default())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                crate::ExecuteError::Store(Error::ViewsUnsupported { .. })
            ),
            "{error:?}"
        );
        assert!(inner.events(&"one".into()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_repository_that_cannot_declare_history_refuses_a_history_dependent_projector() {
        use crate::conformance::{Add, Ledger};
        use crate::{Aggregate, Metadata, Store};

        struct Reads;

        impl CommitProjector for Reads {
            fn name(&self) -> &str {
                "reads"
            }

            fn history(&self) -> &[EntityType] {
                static LEDGER: std::sync::OnceLock<Vec<EntityType>> = std::sync::OnceLock::new();

                LEDGER.get_or_init(|| vec![EntityType::new(Ledger::TYPE)])
            }

            fn project(&self, _: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
                Ok(Vec::new())
            }
        }

        let inner = MemRepository::new();
        let store = Store::<Ledger, _>::new(Bare::<true>(inner.clone()), ())
            .with_commit_projector(std::sync::Arc::new(Reads));

        let error = store
            .execute("one", Add(1), Metadata::default())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                crate::ExecuteError::Store(Error::HistoryDeclarationUnsupported { .. })
            ),
            "{error:?}"
        );
        assert!(inner.events(&"one".into()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_commit_that_writes_no_event_runs_no_projector_and_declares_nothing() {
        use crate::conformance::Ledger;
        use crate::{Metadata, Store};

        let repository = MemRepository::new();
        let store = Store::<Ledger, _>::new(Bare::<false>(repository.clone()), ())
            .with_commit_projector(std::sync::Arc::new(Directory));

        // A repository that cannot write views is not even consulted for a commit with no event.
        let loaded = store.load("one").await.unwrap();
        store
            .commit(loaded, Vec::new(), &Metadata::default())
            .await
            .unwrap();

        assert!(repository.views().is_empty());
    }

    #[tokio::test]
    async fn a_commit_projector_added_after_an_append_fails_the_commit() {
        use crate::conformance::{Added, Ledger};
        use crate::{Error, Metadata, Transaction};

        let repository = MemRepository::new();
        let mut tx = Transaction::new(&repository);
        let mut ledger = tx.read::<Ledger>("late").await.unwrap();
        tx.append(&mut ledger, vec![Added { amount: 1 }.into()])
            .unwrap();

        let tx = tx.with_commit_projector(std::sync::Arc::new(Directory));
        let error = tx.commit(&Metadata::default()).await.unwrap_err();

        assert!(matches!(error, Error::InvalidCommit(_)), "{error}");
        assert!(repository.view("directory", "late").is_none());
    }

    #[tokio::test]
    async fn a_store_and_its_transactions_and_clones_share_their_commit_projectors() {
        use crate::conformance::{Add, Added, Ledger};
        use crate::{Metadata, Store, Transaction};

        let repository = MemRepository::new();
        let store = Store::<Ledger, _>::new(repository.clone(), ())
            .with_snapshot_interval(0)
            .with_commit_projector(std::sync::Arc::new(Directory));

        // A clone of the store projects like the store.
        store
            .clone()
            .execute("one", Add(1), Metadata::default())
            .await
            .unwrap();
        assert_eq!(
            repository.view("directory", "one").unwrap().payload,
            br#"{"total":1,"entries":1}"#
        );

        // The store's transactions inherit the projectors.
        let mut tx = store.transaction();
        let mut ledger = tx.read::<Ledger>("two").await.unwrap();
        tx.append(&mut ledger, vec![Added { amount: 5 }.into()])
            .unwrap();
        tx.commit(&Metadata::default()).await.unwrap();
        assert_eq!(
            repository.view("directory", "two").unwrap().payload,
            br#"{"total":5,"entries":1}"#
        );

        // A bare transaction takes its projectors from `with_commit_projector`, and a read-only
        // stream is not in the commit view.
        let mut tx =
            Transaction::new(&repository).with_commit_projector(std::sync::Arc::new(Directory));
        let mut ledger = tx.read::<Ledger>("three").await.unwrap();
        tx.read::<Ledger>("one").await.unwrap();
        tx.append(&mut ledger, vec![Added { amount: 7 }.into()])
            .unwrap();
        tx.commit(&Metadata::default()).await.unwrap();
        assert_eq!(
            repository.view("directory", "three").unwrap().payload,
            br#"{"total":7,"entries":1}"#
        );
        assert_eq!(
            repository.view("directory", "one").unwrap().payload,
            br#"{"total":1,"entries":1}"#,
            "the fenced stream's row is untouched"
        );
    }

    #[tokio::test]
    async fn view_operations_count_in_the_shared_budget() {
        let repository = MemRepository::new();
        let views = |count: usize, payload: usize| -> Vec<ProjectedView> {
            (0..count)
                .map(|index| {
                    view(
                        index,
                        ViewOperation::put(
                            "p",
                            format!("row-{index:03}"),
                            "o",
                            "",
                            "x".repeat(payload),
                        ),
                    )
                })
                .collect()
        };

        let result = repository
            .commit(Commit::new("a", vec![event("a", 1)]).with_views(views(97, 0)))
            .await;
        assert!(
            matches!(result, Err(Error::CommitTooLarge { actions: 101, .. })),
            "{result:?}"
        );

        let result = repository
            .commit(Commit::new("a", vec![event("a", 1)]).with_views(views(12, 330_000)))
            .await;
        assert!(
            matches!(result, Err(Error::CommitTooLarge { bytes, .. }) if bytes > 3_800_000),
            "{result:?}"
        );
        assert!(
            repository.views().is_empty()
                && repository.events(&"a".into()).await.unwrap().is_empty()
        );

        repository
            .commit(Commit::new("a", vec![event("a", 1)]).with_views(views(96, 0)))
            .await
            .unwrap();
        assert_eq!(repository.views().len(), 96);
    }
    /// A history-dependent projector whose rows depend on the events of the commit: one row per
    /// stream, owned by it, listing the commit's event types and the state after them; and a
    /// summary row per commit, keyed by its ID and owned by its first stream.
    struct Journal;

    impl CommitProjector for Journal {
        fn name(&self) -> &str {
            "journal"
        }

        fn history(&self) -> &[EntityType] {
            static TYPES: std::sync::OnceLock<Vec<EntityType>> = std::sync::OnceLock::new();

            TYPES.get_or_init(|| vec![EntityType::new("ledger"), EntityType::new("claim")])
        }

        fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
            let mut operations: Vec<ViewOperation> = commit
                .streams
                .iter()
                .map(|stream| {
                    let types: Vec<&str> = stream
                        .events
                        .iter()
                        .map(|event| event.event_type.as_str())
                        .collect();

                    ViewOperation::put(
                        "journal",
                        stream.entity_id.as_str(),
                        stream.entity_id.clone(),
                        stream.entity_type.as_str(),
                        format!(
                            "{} {}",
                            types.join(","),
                            String::from_utf8_lossy(&stream.state)
                        ),
                    )
                })
                .collect();
            operations.push(ViewOperation::put(
                "commits",
                commit.commit_id.as_str(),
                commit.streams[0].entity_id.clone(),
                "",
                format!("{} streams", commit.streams.len()),
            ));

            Ok(operations)
        }
    }

    fn fold<A: crate::Aggregate>(events: &[SerializedEvent], through: EventSequence) -> Vec<u8> {
        let mut aggregate = A::default();
        for event in events.iter().filter(|event| event.sequence <= through) {
            aggregate.apply(
                <A::Event as crate::DomainEvent>::from_payload(
                    event.event_type.as_str(),
                    event.version,
                    &event.payload,
                )
                .unwrap(),
            );
        }

        crate::json::to_vec(&aggregate).unwrap()
    }

    /// The end-to-end contract of a history-dependent projector: the rows the live commits leave
    /// are exactly the rows a rebuild reproduces from the stored events, the state each stream had
    /// after each commit, and the commits regrouped by commit ID. Contents are compared, not
    /// counts.
    #[tokio::test]
    async fn a_rebuild_reproduces_the_rows_the_live_commits_left() {
        use crate::conformance::{Add, Added, Claim, Ledger, Take, Taken};
        use crate::{Metadata, Store, group_commits, replay_views};

        let repository = MemRepository::new();
        let projector: std::sync::Arc<dyn CommitProjector> = std::sync::Arc::new(Journal);
        let store = Store::<Ledger, _>::new(repository.clone(), ())
            .with_snapshot_interval(2)
            .with_commit_projector(projector.clone());
        let claims = Store::<Claim, _>::new(repository.clone(), ())
            .with_snapshot_interval(2)
            .with_commit_projector(projector.clone());

        // Single-stream commits, interleaved with a multi-stream one and a snapshot boundary.
        store
            .execute("ledger-1", Add(5), Metadata::default())
            .await
            .unwrap();
        claims
            .execute("claim-1", Take("ada".into()), Metadata::default())
            .await
            .unwrap();
        let mut tx = store.transaction();
        let mut ledger = tx.read::<Ledger>("ledger-1").await.unwrap();
        let mut other = tx.read::<Ledger>("ledger-2").await.unwrap();
        let mut claim = tx.read::<Claim>("claim-2").await.unwrap();
        tx.append(
            &mut ledger,
            vec![Added { amount: 1 }.into(), Added { amount: 2 }.into()],
        )
        .unwrap();
        tx.append(&mut other, vec![Added { amount: 9 }.into()])
            .unwrap();
        tx.append(
            &mut claim,
            vec![
                Taken {
                    holder: "grace".into(),
                }
                .into(),
            ],
        )
        .unwrap();
        tx.commit(&Metadata::default()).await.unwrap();
        store
            .execute("ledger-2", Add(1), Metadata::default())
            .await
            .unwrap();
        store
            .execute("ledger-1", Add(3), Metadata::default())
            .await
            .unwrap();

        let live: Vec<(String, String, String, String)> = repository
            .views()
            .into_iter()
            .map(|row| {
                (
                    row.pk,
                    row.sk,
                    row.entity_id,
                    String::from_utf8(row.payload).unwrap(),
                )
            })
            .collect();
        assert_eq!(
            live.len(),
            4 + 5,
            "four streams' rows and five commits' summaries"
        );

        let mut stored = Vec::new();
        for id in ["ledger-1", "ledger-2", "claim-1", "claim-2"] {
            stored.extend(repository.events(&id.into()).await.unwrap());
        }
        let mut commits = group_commits(stored.clone()).unwrap();
        assert_eq!(commits.len(), 5);
        for commit in &mut commits {
            for stream in &mut commit.streams {
                let events: Vec<SerializedEvent> = stored
                    .iter()
                    .filter(|event| event.entity_id == stream.entity_id)
                    .cloned()
                    .collect();
                let through = stream.events.last().unwrap().sequence;

                stream.state = if stream.entity_type.as_str() == "claim" {
                    fold::<Claim>(&events, through)
                } else {
                    fold::<Ledger>(&events, through)
                };
            }
        }

        let rebuilt: Vec<(String, String, String, String)> = replay_views(&[projector], &commits)
            .unwrap()
            .into_iter()
            .map(|row| {
                (
                    row.pk,
                    row.sk,
                    row.entity_id,
                    String::from_utf8(row.payload).unwrap(),
                )
            })
            .collect();

        assert_eq!(rebuilt, live);
        // The journal row of ledger-1 shows the last commit's event and the final state.
        assert!(
            live.iter().any(|row| row.0 == "journal"
                && row.1 == "ledger-1"
                && row.3 == r#"ledger:added {"total":11,"entries":4}"#),
            "{live:?}"
        );
    }
    #[tokio::test]
    async fn only_a_rows_owner_overwrites_or_deletes_it() {
        let repository = MemRepository::new();
        repository
            .commit(Commit::new("a", vec![event("a", 1)]).with_views([view(
                0,
                ViewOperation::put("rows", "r", "a", "t", "{\"v\":1}"),
            )]))
            .await
            .unwrap();
        assert_eq!(repository.view("rows", "r").unwrap().entity_id, "a");

        // Another stream's put, unconditional or not, and its delete, conditional or not, all
        // fail the condition, and nothing of the failing commit is written.
        let foreign = [
            ViewOperation::put("rows", "r", "b", "t", "{\"v\":2}"),
            ViewOperation::put("rows", "r", "b", "t", "{\"v\":2}").if_absent(),
            ViewOperation::delete("rows", "r", "b"),
            ViewOperation::delete("rows", "r", "b").if_present(),
        ];
        for op in foreign {
            let result = repository
                .commit(Commit::new("b", vec![event("b", 1)]).with_views([view(0, op.clone())]))
                .await;

            match result {
                Err(Error::ProjectorOperation(failed)) => {
                    assert_eq!(failed.reason, "ConditionalCheckFailed", "{op:?}");
                }
                other => panic!("{op:?}: expected a failed condition, got {other:?}"),
            }
            assert!(repository.events(&"b".into()).await.unwrap().is_empty());
            let row = repository.view("rows", "r").unwrap();
            assert_eq!(
                (row.entity_id.as_str(), row.payload.as_slice()),
                ("a", b"{\"v\":1}".as_slice())
            );
        }

        // The owner overwrites its row, and deletes it.
        repository
            .commit(Commit::new("a", vec![event("a", 2)]).with_views([view(
                0,
                ViewOperation::put("rows", "r", "a", "t", "{\"v\":3}"),
            )]))
            .await
            .unwrap();
        assert_eq!(repository.view("rows", "r").unwrap().payload, b"{\"v\":3}");
        repository
            .commit(Commit::new("a", vec![event("a", 3)]).with_views([view(
                0,
                ViewOperation::delete("rows", "r", "a").if_present(),
            )]))
            .await
            .unwrap();
        assert!(repository.view("rows", "r").is_none());

        // A delete of an absent row succeeds for anyone; a present delete does not.
        repository
            .commit(
                Commit::new("b", vec![event("b", 1)])
                    .with_views([view(0, ViewOperation::delete("rows", "r", "b"))]),
            )
            .await
            .unwrap();
        let result = repository
            .commit(Commit::new("b", vec![event("b", 2)]).with_views([view(
                0,
                ViewOperation::delete("rows", "r", "b").if_present(),
            )]))
            .await;
        assert!(
            matches!(result, Err(Error::ProjectorOperation(_))),
            "{result:?}"
        );

        // Once the row is gone any stream may take the key.
        repository
            .commit(Commit::new("b", vec![event("b", 2)]).with_views([view(
                0,
                ViewOperation::put("rows", "r", "b", "t", "{}").if_absent(),
            )]))
            .await
            .unwrap();
        assert_eq!(repository.view("rows", "r").unwrap().entity_id, "b");
    }
}
