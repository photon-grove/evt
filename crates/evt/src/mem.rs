//! An in-memory repository for tests and local development.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::Error;
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::ids::{EntityId, EntityType, EventSequence};
use crate::repository::{Commit, Repository};

/// An in-memory [`Repository`]. Clones share the same storage.
///
/// Unlike Go's in-memory test double, it enforces optimistic concurrency the same way the
/// DynamoDB backend does, so tests catch conflicts.
#[derive(Clone, Debug, Default)]
pub struct MemRepository {
    streams: Arc<Mutex<BTreeMap<EntityId, Stream>>>,
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
}

impl Repository for MemRepository {
    type Operation = Infallible;

    async fn commit(&self, commit: Commit<Infallible>) -> Result<(), Error> {
        commit.validate()?;

        let mut streams = self.lock();
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
                        "snapshot sequence moved: stored {stored}, expected {}",
                        new.sequence.0 - 1
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
                    .range(after.next()..)
                    .map(|(_, e)| e.clone())
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn snapshot(&self, entity_id: &EntityId) -> Result<Option<SerializedSnapshot>, Error> {
        Ok(self
            .lock()
            .get(entity_id)
            .and_then(|stream| stream.snapshot.clone()))
    }

    async fn put_snapshot(&self, snapshot: &SerializedSnapshot) -> Result<bool, Error> {
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
        if through < EventSequence(1) {
            return Ok(0);
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
            .filter(|(_, stream)| {
                entity_type.is_none_or(|wanted| stream.entity_type() == Some(wanted))
            })
            .map(|(id, stream)| (id.clone(), stream.head()))
            .filter(|(_, head)| *head > EventSequence::ZERO)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::Suite;

    #[tokio::test]
    async fn conforms() {
        Suite::new(&MemRepository::new()).run().await;
    }
}
