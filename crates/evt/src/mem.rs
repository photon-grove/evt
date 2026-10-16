//! An in-memory repository for tests and local development.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::Error;
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::features::{self, CachedMarker, FeatureManifest, FeatureMarker};
use crate::ids::{EntityId, EntityType, EventSequence};
use crate::repository::{Commit, Repository, TransactionCommit};

/// An in-memory [`Repository`]. Clones share the same storage.
///
/// Unlike Go's in-memory test double, it enforces optimistic concurrency the same way the
/// DynamoDB backend does, so tests catch conflicts. A clone is an independent writer over the
/// shared storage: it keeps its own feature-marker cache, like a second handle on a live
/// backend, so a declaration one clone writes refuses the other on its next commit.
#[derive(Debug, Default)]
pub struct MemRepository {
    streams: Arc<Mutex<BTreeMap<EntityId, Stream>>>,
    manifest: Arc<Mutex<Option<FeatureManifest>>>,
    /// The feature-marker version this handle validated, if it has written yet. Per handle, not
    /// shared with clones.
    validated: Mutex<CachedMarker>,
}

impl Clone for MemRepository {
    fn clone(&self) -> Self {
        Self {
            streams: self.streams.clone(),
            manifest: self.manifest.clone(),
            validated: Mutex::new(*self.validated()),
        }
    }
}

#[derive(Debug, Default)]
struct Stream {
    events: BTreeMap<EventSequence, SerializedEvent>,
    snapshot: Option<SerializedSnapshot>,
}

impl Stream {
    fn entity_type(&self) -> Option<&EntityType> {
        self.events
            .values()
            .next()
            .map(|event| &event.entity_type)
            .or_else(|| self.snapshot.as_ref().map(|snapshot| &snapshot.entity_type))
    }

    fn head(&self) -> EventSequence {
        let last_event = self.events.keys().next_back().copied().unwrap_or_default();
        let snapshot = self
            .snapshot
            .as_ref()
            .map(|s| s.event_sequence)
            .unwrap_or_default();

        last_event.max(snapshot)
    }
}

/// Refuses a first sequence that skips past the stored head. The head is the stream's highest
/// event or, once compaction deleted every covered event, its snapshot's event sequence. A first
/// sequence at or below the head is left to the event and floor checks, which report it as an
/// ordinary conflict.
fn check_continuation(
    entity_id: &EntityId,
    head: EventSequence,
    first: EventSequence,
) -> Result<(), Error> {
    if first > head.next() {
        return Err(Error::SequenceGap {
            entity_id: entity_id.clone(),
            after: head,
            found: first,
        });
    }

    Ok(())
}

impl MemRepository {
    /// Creates an empty repository.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<EntityId, Stream>> {
        // A panic while holding the lock cannot leave a stream half-written: commits validate
        // before mutating. Recover the data rather than poisoning every later test.
        self.streams
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
    /// this writer is re-validated here, refusing an unsupported declared feature.
    fn check_manifest(&self, stored: Option<&FeatureManifest>) -> Result<(), Error> {
        let mut validated = self.validated();

        if features::marker_matches(*validated, stored) {
            return Ok(());
        }

        *validated = Some(features::validate_marker(stored)?);

        Ok(())
    }
}

impl Repository for MemRepository {
    type Operation = Infallible;

    async fn commit(&self, commit: Commit<Infallible>) -> Result<(), Error> {
        commit.validate()?;
        features::check_not_reserved(&commit.entity_id)?;
        if commit.events.is_empty() {
            return Ok(());
        }

        // Budget parity: an oversized commit fails identically on every backend, before any
        // write.
        crate::repository::check_budget(commit.sizes(|_| None)?)?;

        // Both locks are held through the write, so a manifest change cannot slip between the
        // version check and the events, mirroring the live backends' transaction condition.
        let stored_manifest = self.manifest_lock();
        self.check_manifest(stored_manifest.as_ref())?;

        let mut streams = self.lock();

        // The stream must continue from its stored head; a gap is refused before anything is
        // created or written.
        if let Some(first) = commit.events.first() {
            let head = streams
                .get(&commit.entity_id)
                .map_or(EventSequence::ZERO, Stream::head);
            check_continuation(&commit.entity_id, head, first.sequence)?;
        }

        let stream = streams.entry(commit.entity_id.clone()).or_default();

        for (index, event) in commit.events.iter().enumerate() {
            let reused = commit.events[..index]
                .iter()
                .any(|e| e.sequence == event.sequence);
            if reused || stream.events.contains_key(&event.sequence) {
                return Err(Error::Conflict {
                    entity_id: commit.entity_id,
                    message: format!("event sequence {} already exists", event.sequence),
                });
            }
        }

        if let (Some(first), Some(existing)) = (commit.events.first(), &stream.snapshot)
            && first.sequence <= existing.event_sequence
        {
            return Err(Error::Conflict {
                entity_id: commit.entity_id,
                message: format!(
                    "event sequence {} is at or below the snapshot floor {}",
                    first.sequence, existing.event_sequence
                ),
            });
        }

        if let Some(new) = &commit.snapshot {
            let stored = stream
                .snapshot
                .as_ref()
                .map_or(EventSequence::ZERO, |existing| existing.sequence);

            if stored.next() != new.sequence {
                return Err(Error::Conflict {
                    entity_id: commit.entity_id,
                    message: format!(
                        "snapshot sequence moved: stored {stored}, commit writes {}",
                        new.sequence
                    ),
                });
            }
        }

        let last_sequence = commit.events.last().map(|event| event.sequence);

        for event in commit.events {
            stream.events.insert(event.sequence, event);
        }

        if let (Some(snapshot), Some(event_sequence)) = (commit.snapshot, last_sequence) {
            stream.snapshot = Some(SerializedSnapshot {
                entity_type: snapshot.entity_type,
                entity_id: commit.entity_id,
                sequence: snapshot.sequence,
                event_sequence,
                payload: snapshot.payload,
            });
        }

        Ok(())
    }

    async fn commit_transaction(&self, commit: TransactionCommit<Infallible>) -> Result<(), Error> {
        let mut commit = commit;
        commit.normalize()?;

        if commit.is_empty() {
            return Ok(());
        }

        // Budget parity: an oversized commit fails identically on every backend.
        crate::repository::check_budget(commit.sizes(|_| None)?)?;

        // Both locks are held through the write, so a manifest change cannot slip between the
        // version check and the events, mirroring the live backends' transaction condition.
        let stored_manifest = self.manifest_lock();
        self.check_manifest(stored_manifest.as_ref())?;

        // Fences are refused for entity types durably declared with retention.
        commit.check_retention(stored_manifest.as_ref(), |_| false)?;

        let mut streams = self.lock();

        // Validate every stream, snapshot, and fence before writing anything, so a failure leaves
        // every stream unchanged. Reads use get: a failed commit must not create empty streams.
        // A stream with no stored rows validates against an empty one, so a first snapshot must
        // be sequence 1, as on DynamoDB and PostgreSQL.
        let absent = Stream::default();
        for stream in &commit.streams {
            let stored = streams.get(&stream.entity_id).unwrap_or(&absent);

            if let Some(first) = stream.events.first() {
                check_continuation(&stream.entity_id, stored.head(), first.sequence)?;
            }

            for (index, event) in stream.events.iter().enumerate() {
                let reused = stream.events[..index]
                    .iter()
                    .any(|e| e.sequence == event.sequence);
                if reused || stored.events.contains_key(&event.sequence) {
                    return Err(Error::Conflict {
                        entity_id: stream.entity_id.clone(),
                        message: format!("event sequence {} already exists", event.sequence),
                    });
                }
            }

            if let (Some(first), Some(existing)) = (stream.events.first(), &stored.snapshot)
                && first.sequence <= existing.event_sequence
            {
                return Err(Error::Conflict {
                    entity_id: stream.entity_id.clone(),
                    message: format!(
                        "event sequence {} is at or below the snapshot floor {}",
                        first.sequence, existing.event_sequence
                    ),
                });
            }

            if let Some(new) = &stream.snapshot {
                let stored_sequence = stored
                    .snapshot
                    .as_ref()
                    .map_or(EventSequence::ZERO, |existing| existing.sequence);

                if stored_sequence.next() != new.sequence {
                    return Err(Error::Conflict {
                        entity_id: stream.entity_id.clone(),
                        message: format!(
                            "snapshot sequence moved: stored {stored_sequence}, commit writes {}",
                            new.sequence
                        ),
                    });
                }
            }
        }

        for fence in &commit.fences {
            if commit
                .streams
                .iter()
                .any(|stream| stream.entity_id == fence.entity_id)
            {
                // A written stream's event put supersedes its fence.
                continue;
            }

            let Some(stored) = streams.get(&fence.entity_id) else {
                continue;
            };

            let event_head = stored
                .events
                .keys()
                .next_back()
                .copied()
                .unwrap_or(EventSequence::ZERO);
            let snapshot_head = stored
                .snapshot
                .as_ref()
                .map_or(EventSequence::ZERO, |snapshot| snapshot.event_sequence);

            if event_head > fence.head || snapshot_head > fence.head {
                return Err(Error::FenceConflict {
                    entity_id: fence.entity_id.clone(),
                    head: fence.head,
                    provenance: fence.provenance,
                });
            }
        }

        for stream in commit.streams {
            let last_sequence = stream.events.last().map(|event| event.sequence);
            let stored = streams.entry(stream.entity_id.clone()).or_default();

            for event in stream.events {
                stored.events.insert(event.sequence, event);
            }

            if let (Some(snapshot), Some(event_sequence)) = (stream.snapshot, last_sequence) {
                stored.snapshot = Some(SerializedSnapshot {
                    entity_type: snapshot.entity_type,
                    entity_id: stream.entity_id,
                    sequence: snapshot.sequence,
                    event_sequence,
                    payload: snapshot.payload,
                });
            }
        }

        Ok(())
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

        let manifest = self.manifest_lock();
        self.check_manifest(manifest.as_ref())?;

        let mut streams = self.lock();
        let stream = streams.entry(snapshot.entity_id.clone()).or_default();

        if stream
            .snapshot
            .as_ref()
            .is_some_and(|existing| existing.event_sequence > snapshot.event_sequence)
        {
            return Ok(false);
        }

        stream.snapshot = Some(snapshot.clone());

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
        if let Some(manifest) = manifest.as_ref() {
            manifest.validate_supported()?;
        }

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
            .map(|(id, stream)| (id.clone(), stream.head()))
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
    use crate::conformance::{Suite, feature_marker_cases};

    #[tokio::test]
    async fn conforms() {
        Suite::new(&MemRepository::new()).run().await;
    }

    #[tokio::test]
    async fn conforms_to_the_feature_marker() {
        let first = MemRepository::new();

        // The clone shares the storage but keeps its own marker cache, so the cases see two
        // writers.
        feature_marker_cases(&first, &first.clone()).await;
    }
}
