//! An in-memory repository for tests and local development.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::Error;
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::features::{self, CachedMarker, FeatureManifest, FeatureMarker, ValidatedMarker};
use crate::heads::{self, Probe};
use crate::ids::{CommandId, EntityId, EntityType, EventSequence};
use crate::outcome::Outcome;
use crate::repository::{Commit, ReadFence, Repository, TransactionCommit};

/// An in-memory [`Repository`]. Clones share the same storage.
///
/// Unlike Go's in-memory test double, it enforces optimistic concurrency the same way the
/// DynamoDB backend does, so tests catch conflicts: every stream has a head (ADR 0007) that a
/// commit raises and a read fence compares, and a failed commit is classified (drift, gap, then
/// conflict) exactly as on the live backends. A clone is an independent writer over the shared
/// storage: it keeps its own feature-marker cache, like a second handle on a live backend, so a
/// declaration one clone writes refuses the other on its next commit.
#[derive(Debug, Default)]
pub struct MemRepository {
    streams: Arc<Mutex<BTreeMap<EntityId, Stream>>>,
    /// The command-outcome rows, keyed by command ID. Shared with clones, like the streams.
    outcomes: Arc<Mutex<BTreeMap<CommandId, Outcome>>>,
    manifest: Arc<Mutex<Option<FeatureManifest>>>,
    /// The feature-marker version this handle validated, if it has written yet. Per handle, not
    /// shared with clones.
    validated: Mutex<CachedMarker>,
}

impl Clone for MemRepository {
    fn clone(&self) -> Self {
        Self {
            streams: self.streams.clone(),
            outcomes: self.outcomes.clone(),
            manifest: self.manifest.clone(),
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

    /// Writes the streams, fences, and outcome of one commit atomically. Streams are in
    /// ascending entity-ID order, as `TransactionCommit::normalize` leaves them.
    fn write(
        &self,
        streams: &[&Commit<Infallible>],
        fences: &[ReadFence],
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

        outcomes.extend(outcome.map(|outcome| (outcome.command_id.clone(), outcome)));

        Ok(())
    }
}

impl Repository for MemRepository {
    type Operation = Infallible;

    async fn commit(&self, commit: Commit<Infallible>) -> Result<(), Error> {
        commit.validate()?;
        features::check_not_reserved(&commit.entity_id)?;
        if commit.events.is_empty() && commit.outcome.is_none() {
            return Ok(());
        }

        // Budget parity: an oversized commit fails identically on every backend, before any
        // write.
        crate::repository::check_budget(commit.sizes(|_| None)?)?;

        self.write(&[&commit], &[], commit.stored_outcome(), |_| Ok(()))
    }

    async fn commit_transaction(&self, commit: TransactionCommit<Infallible>) -> Result<(), Error> {
        let mut commit = commit;
        commit.normalize()?;

        if commit.is_empty() {
            return Ok(());
        }

        // Budget parity: an oversized commit fails identically on every backend.
        crate::repository::check_budget(commit.sizes(|_| None)?)?;

        let streams: Vec<&Commit<Infallible>> = commit.streams.iter().collect();

        self.write(
            &streams,
            &commit.fences,
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
}

mod enumerate;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::{Suite, feature_marker_cases, fresh_store_cases, outcome_cases};

    #[tokio::test]
    async fn conforms() {
        let repository = MemRepository::new();

        Suite::new(&repository)
            .with_head_fixtures(repository.clone())
            .run()
            .await;
    }

    #[tokio::test]
    async fn a_fresh_store_declares_stream_heads_on_the_first_write() {
        let first = MemRepository::new();

        fresh_store_cases(&first, &first.clone()).await;
    }

    #[tokio::test]
    async fn conforms_to_command_outcomes() {
        let first = MemRepository::new();

        // The clone is a second writer over the same storage.
        outcome_cases(&first, &first.clone()).await;
    }

    #[tokio::test]
    async fn conforms_to_the_feature_marker() {
        let first = MemRepository::new();

        // The clone shares the storage but keeps its own marker cache, so the cases see two
        // writers.
        feature_marker_cases(&first, &first.clone()).await;
    }
}
