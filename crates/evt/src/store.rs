//! The [`Store`]: load aggregates, handle commands, and commit events with snapshots.

use std::collections::HashSet;

use crate::aggregate::{Aggregate, Projector};
use crate::error::{Error, ExecuteError, ReplayPhase};
use crate::event::{DomainEvent, SerializedEvent};
use crate::ids::{CommandId, EntityId, EntityType, EventId, EventSequence};
use crate::json;
use crate::metadata::Metadata;
use crate::repository::{Commit, NewSnapshot, Repository, snapshot_events_due};
use crate::upcast::upcast_event;

/// The default number of events between snapshots, matching the Go backends' defaults.
pub const DEFAULT_SNAPSHOT_INTERVAL: usize = 5;

/// Loads aggregates of type `A` from a repository, handles commands, and commits the resulting
/// events, writing a snapshot every [`snapshot interval`](Store::with_snapshot_interval) events.
pub struct Store<A: Aggregate, R: Repository> {
    repository: R,
    services: A::Services,
    snapshot_interval: usize,
    projectors: Vec<Box<dyn Projector<A, R::Operation>>>,
}

impl<A: Aggregate, R: Repository> Store<A, R> {
    /// Creates a store with the default snapshot interval.
    pub fn new(repository: R, services: A::Services) -> Self {
        Self {
            repository,
            services,
            snapshot_interval: DEFAULT_SNAPSHOT_INTERVAL,
            projectors: Vec::new(),
        }
    }

    /// Sets the number of events between snapshots. 0 disables snapshots.
    pub fn with_snapshot_interval(mut self, interval: usize) -> Self {
        self.snapshot_interval = interval;
        self
    }

    /// Adds an in-band projector whose operations commit atomically with each command's events.
    pub fn with_projector(mut self, projector: impl Projector<A, R::Operation> + 'static) -> Self {
        self.projectors.push(Box::new(projector));
        self
    }

    /// The underlying repository.
    pub fn repository(&self) -> &R {
        &self.repository
    }

    /// The services passed to command handlers.
    pub fn services(&self) -> &A::Services {
        &self.services
    }

    /// The configured snapshot interval.
    pub fn snapshot_interval(&self) -> usize {
        self.snapshot_interval
    }

    /// Loads an aggregate: from its snapshot plus later events when a snapshot exists, otherwise
    /// from every event. An entity with no events loads as `A::default()` at sequence 0.
    pub async fn load(&self, entity_id: impl Into<EntityId>) -> Result<Loaded<A>, Error> {
        let entity_id = entity_id.into();

        let snapshot = self.repository.snapshot(&entity_id).await?;
        let has_snapshot = snapshot.is_some();

        let mut loaded = match snapshot {
            Some(snapshot) => {
                let aggregate = serde_json::from_slice(&snapshot.payload).map_err(|source| {
                    Error::Snapshot {
                        entity_id: entity_id.clone(),
                        source,
                    }
                })?;

                Loaded {
                    entity_id,
                    aggregate,
                    sequence: snapshot.event_sequence,
                    snapshot_sequence: snapshot.sequence,
                    seen_command_ids: HashSet::new(),
                }
            }
            None => Loaded::new(entity_id),
        };

        let events = if has_snapshot {
            self.repository
                .events_after(&loaded.entity_id, loaded.sequence)
                .await?
        } else {
            self.repository.events(&loaded.entity_id).await?
        };

        for event in events {
            loaded.replay(event)?;
        }

        Ok(loaded)
    }

    /// Loads the aggregate, handles the command, and commits the resulting events.
    ///
    /// When `metadata` carries a command ID already recorded by an event replayed since the
    /// latest snapshot, it returns [`Error::DuplicateCommand`] without calling the handler.
    pub async fn execute(
        &self,
        entity_id: impl Into<EntityId>,
        command: A::Command,
        metadata: Metadata,
    ) -> Result<Executed<A>, ExecuteError<A::Error>> {
        let loaded = self.load(entity_id).await?;

        if let Some(command_id) = &metadata.command_id
            && loaded.has_seen(command_id)
        {
            return Err(Error::DuplicateCommand(command_id.clone()).into());
        }

        let events = loaded
            .aggregate
            .handle(command, &self.services)
            .await
            .map_err(ExecuteError::Rejected)?;

        let (state, events) = self.commit(loaded, events, &metadata).await?;

        Ok(Executed { state, events })
    }

    /// Like [`execute`](Store::execute), but reloads and re-handles the command after an
    /// optimistic-concurrency conflict, up to `max_attempts` attempts in total.
    pub async fn execute_with_retry(
        &self,
        entity_id: impl Into<EntityId>,
        command: A::Command,
        metadata: Metadata,
        max_attempts: usize,
    ) -> Result<Executed<A>, ExecuteError<A::Error>>
    where
        A::Command: Clone,
    {
        let entity_id = entity_id.into();
        let mut attempt = 1;

        loop {
            match self
                .execute(entity_id.clone(), command.clone(), metadata.clone())
                .await
            {
                Err(err) if err.is_conflict() && attempt < max_attempts => attempt += 1,
                result => return result,
            }
        }
    }

    /// Applies events to a loaded aggregate and commits them, snapshotting when the commit
    /// reaches the snapshot interval. Consumes `loaded` so a failed commit cannot leave stale
    /// state in use. On success it returns the updated state and the stored events.
    pub async fn commit(
        &self,
        mut loaded: Loaded<A>,
        events: Vec<A::Event>,
        metadata: &Metadata,
    ) -> Result<(Loaded<A>, Vec<SerializedEvent>), Error> {
        if let Some(command_id) = &metadata.command_id
            && loaded.has_seen(command_id)
        {
            return Err(Error::DuplicateCommand(command_id.clone()));
        }

        if events.is_empty() {
            return Ok((loaded, Vec::new()));
        }

        let serialized =
            serialize_events::<A>(&loaded.entity_id, loaded.sequence, &events, metadata)?;
        let snapshot_due =
            snapshot_events_due(loaded.sequence, events.len(), self.snapshot_interval) > 0;
        let projected_events = (!self.projectors.is_empty()).then(|| events.clone());

        for event in events {
            loaded.aggregate.apply(event);
        }

        let mut operations = Vec::new();
        if let Some(projected_events) = &projected_events {
            for projector in &self.projectors {
                let ops = projector
                    .project(&loaded.entity_id, &loaded.aggregate, projected_events)
                    .map_err(Error::Projector)?;
                operations.extend(ops);
            }
        }

        let snapshot = if snapshot_due {
            Some(NewSnapshot {
                entity_type: EntityType::new(A::TYPE),
                sequence: loaded.snapshot_sequence.next(),
                payload: json::to_vec(&loaded.aggregate)?,
            })
        } else {
            None
        };

        let snapshot_sequence = snapshot.as_ref().map(|snapshot| snapshot.sequence);

        self.repository
            .commit(Commit {
                entity_id: loaded.entity_id.clone(),
                events: serialized.clone(),
                snapshot,
                operations,
            })
            .await?;

        loaded.sequence = serialized
            .last()
            .map_or(loaded.sequence, |event| event.sequence);
        if let Some(sequence) = snapshot_sequence {
            loaded.snapshot_sequence = sequence;
        }
        if let Some(command_id) = &metadata.command_id {
            loaded.seen_command_ids.insert(command_id.clone());
        }

        Ok((loaded, serialized))
    }

    /// Lists the IDs of every stored entity of this aggregate type, for rebuilds. Load each with
    /// [`load`](Store::load), which is snapshot-aware and therefore correct after compaction.
    pub async fn entity_ids(&self) -> Result<Vec<EntityId>, Error> {
        let heads = self
            .repository
            .entity_heads(Some(&EntityType::new(A::TYPE)))
            .await?;

        Ok(heads.into_keys().collect())
    }
}

/// An aggregate loaded from the event log, with the stream position it was loaded at.
#[derive(Debug)]
pub struct Loaded<A> {
    entity_id: EntityId,
    aggregate: A,
    sequence: EventSequence,
    snapshot_sequence: EventSequence,
    seen_command_ids: HashSet<CommandId>,
}

impl<A: Aggregate> Loaded<A> {
    fn new(entity_id: EntityId) -> Self {
        Self {
            entity_id,
            aggregate: A::default(),
            sequence: EventSequence::ZERO,
            snapshot_sequence: EventSequence::ZERO,
            seen_command_ids: HashSet::new(),
        }
    }

    /// Upcasts, decodes, and applies one stored event.
    fn replay(&mut self, stored: SerializedEvent) -> Result<(), Error> {
        let stored = upcast_event(A::upcasters(), stored).map_err(|failure| Error::Replay {
            event_type: failure.event_type,
            version: failure.version,
            phase: ReplayPhase::Upcast,
            source: failure.source,
        })?;

        let event =
            A::Event::from_payload(stored.event_type.as_str(), stored.version, &stored.payload)
                .map_err(|source| Error::Replay {
                    event_type: stored.event_type.clone(),
                    version: stored.version,
                    phase: ReplayPhase::Deserialize,
                    source: source.into(),
                })?;

        self.sequence = stored.sequence;
        if let Some(command_id) = stored.metadata.command_id {
            self.seen_command_ids.insert(command_id);
        }

        self.aggregate.apply(event);

        Ok(())
    }

    /// The entity ID.
    pub fn entity_id(&self) -> &EntityId {
        &self.entity_id
    }

    /// The aggregate state.
    pub fn aggregate(&self) -> &A {
        &self.aggregate
    }

    /// Consumes the loaded state and returns the aggregate.
    pub fn into_aggregate(self) -> A {
        self.aggregate
    }

    /// The sequence of the last event folded into the state (0 for a new entity).
    pub fn sequence(&self) -> EventSequence {
        self.sequence
    }

    /// The sequence of the latest snapshot (0 when the entity has none).
    pub fn snapshot_sequence(&self) -> EventSequence {
        self.snapshot_sequence
    }

    /// Reports whether a command ID was recorded by an event replayed since the latest snapshot.
    pub fn has_seen(&self, command_id: &CommandId) -> bool {
        self.seen_command_ids.contains(command_id)
    }
}

/// The result of a successful [`Store::execute`].
#[derive(Debug)]
pub struct Executed<A> {
    /// The aggregate state after the new events.
    pub state: Loaded<A>,
    /// The events as stored.
    pub events: Vec<SerializedEvent>,
}

/// Serializes typed events into stored events starting after `sequence`.
pub fn serialize_events<A: Aggregate>(
    entity_id: &EntityId,
    sequence: EventSequence,
    events: &[A::Event],
    metadata: &Metadata,
) -> Result<Vec<SerializedEvent>, Error> {
    let mut sequence = sequence;

    events
        .iter()
        .map(|event| {
            sequence = sequence.next();

            Ok(SerializedEvent {
                id: EventId::for_event(entity_id, sequence),
                sequence,
                event_type: event.event_type().into(),
                version: event.event_version(),
                entity_id: entity_id.clone(),
                entity_type: EntityType::new(A::TYPE),
                payload: event.to_payload()?,
                metadata: metadata.clone(),
            })
        })
        .collect()
}
