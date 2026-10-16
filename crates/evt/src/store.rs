//! The [`Store`]: load aggregates, handle commands, and commit events with snapshots.

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::aggregate::{Aggregate, Projector};
use crate::commit_id::{self, CommitIdGenerator};
use crate::error::{Error, ExecuteError, ReplayPhase};
use crate::event::{DomainEvent, SerializedEvent};
use crate::ids::{CommandId, EntityId, EntityType, EventId, EventSequence};
use crate::json;
use crate::metadata::Metadata;
use crate::repository::{Commit, NewSnapshot, Repository, snapshot_events_due};
use crate::retry::RetryPolicy;
use crate::upcast::upcast_event;

/// How many times [`Store::load`] reads the stream when it finds a sequence gap.
const LOAD_ATTEMPTS: usize = 3;

/// The default number of events between snapshots, matching the Go backends' defaults.
pub const DEFAULT_SNAPSHOT_INTERVAL: usize = 5;

/// Loads aggregates of type `A` from a repository, handles commands, and commits the resulting
/// events, writing a snapshot every [`snapshot interval`](Store::with_snapshot_interval) events.
///
/// A store is `Send + Sync` when its repository and services are, so it can be shared across
/// tasks behind an `Arc`. It is `Clone` when the repository and services are `Clone`; projectors
/// are shared between clones.
pub struct Store<A: Aggregate, R: Repository> {
    pub(crate) repository: R,
    services: A::Services,
    pub(crate) snapshot_interval: usize,
    pub(crate) projectors: Vec<Arc<dyn Projector<A, R::Operation>>>,
    pub(crate) commit_ids: CommitIdGenerator,
}

impl<A, R> Clone for Store<A, R>
where
    A: Aggregate,
    A::Services: Clone,
    R: Repository + Clone,
{
    fn clone(&self) -> Self {
        Self {
            repository: self.repository.clone(),
            services: self.services.clone(),
            snapshot_interval: self.snapshot_interval,
            projectors: self.projectors.clone(),
            commit_ids: self.commit_ids.clone(),
        }
    }
}

impl<A: Aggregate, R: Repository + fmt::Debug> fmt::Debug for Store<A, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Store")
            .field("aggregate", &A::TYPE)
            .field("repository", &self.repository)
            .field("snapshot_interval", &self.snapshot_interval)
            .field("projectors", &self.projectors.len())
            .finish_non_exhaustive()
    }
}

impl<A: Aggregate, R: Repository> Store<A, R> {
    /// Creates a store with the default snapshot interval.
    pub fn new(repository: R, services: A::Services) -> Self {
        Self {
            repository,
            services,
            snapshot_interval: DEFAULT_SNAPSHOT_INTERVAL,
            projectors: Vec::new(),
            commit_ids: commit_id::default_generator(),
        }
    }

    /// Replaces the generator that stamps a commit ID on commits whose metadata carries neither
    /// a command ID nor a commit ID. The default produces ULID-format IDs from the system clock;
    /// inject one for deterministic IDs in tests or to follow an existing ID scheme.
    #[must_use]
    pub fn with_commit_id_generator(
        mut self,
        generator: impl Fn() -> String + Send + Sync + 'static,
    ) -> Self {
        self.commit_ids = Arc::new(generator);
        self
    }

    /// Sets the number of events between snapshots. 0 disables snapshots.
    #[must_use]
    pub fn with_snapshot_interval(mut self, interval: usize) -> Self {
        self.snapshot_interval = interval;
        self
    }

    /// Adds an in-band projector whose operations commit atomically with each command's events.
    #[must_use]
    pub fn with_projector(mut self, projector: impl Projector<A, R::Operation> + 'static) -> Self {
        self.projectors.push(Arc::new(projector));
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
    ///
    /// Event sequences must be contiguous. A gap means the stream was compacted between reading
    /// the snapshot and the events, so the load starts over, up to three times. A gap that
    /// persists, for example from expired events, fails with [`Error::SequenceGap`].
    ///
    /// Like any read, a load can be one compaction behind: if another writer snapshots and compacts
    /// during the load, the event read can stop short without a gap (it is empty, or a paginated
    /// query returns only its first page), and the load returns an older state. That state was real
    /// when it was read. A commit from it starts at or below the new
    /// snapshot and fails with [`Error::Conflict`], which [`execute_with_retry`] retries.
    ///
    /// [`execute_with_retry`]: Store::execute_with_retry
    pub async fn load(&self, entity_id: impl Into<EntityId>) -> Result<Loaded<A>, Error> {
        load_aggregate(&self.repository, entity_id.into()).await
    }

    /// Loads the aggregate, handles the command, and commits the resulting events.
    ///
    /// When `metadata` carries a command ID already recorded by an event replayed since the
    /// latest snapshot, it returns [`Error::DuplicateCommand`] without calling the handler.
    ///
    /// # Cancellation
    ///
    /// Not cancellation safe. If the future is dropped while the commit is in flight, the events
    /// may or may not have been stored. The same holds for a commit that fails with
    /// [`Error::Backend`] after a timeout. Reload the entity before deciding what to do. A command
    /// ID in `metadata` catches the repeat only if the lost commit did not write a snapshot and
    /// none has been written since, because IDs folded into a snapshot are no longer checked.
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

    /// Like [`execute`](Store::execute), but reloads and re-handles the command after a
    /// [retryable](Error::is_retryable) error (a conflict or a transient backend failure), waiting
    /// [`RetryPolicy::delay`] between attempts with `sleep`, such as `tokio::time::sleep`.
    pub async fn execute_with_retry<S, F>(
        &self,
        entity_id: impl Into<EntityId>,
        command: A::Command,
        metadata: Metadata,
        policy: &RetryPolicy,
        sleep: S,
    ) -> Result<Executed<A>, ExecuteError<A::Error>>
    where
        A::Command: Clone,
        S: Fn(Duration) -> F + Send,
        F: Future<Output = ()> + Send,
    {
        let entity_id = entity_id.into();
        let max_attempts = policy.max_attempts.max(1);
        let mut retry = 0;

        loop {
            match self
                .execute(entity_id.clone(), command.clone(), metadata.clone())
                .await
            {
                Err(err) if err.is_retryable() && retry + 1 < max_attempts => {
                    sleep(policy.delay(retry)).await;
                    retry += 1;
                }
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

        // The engine stamps the commit ID once per commit; every event of this commit carries it.
        let stamped = stamp_commit_id(metadata, &*self.commit_ids);
        let serialized =
            serialize_events::<A>(&loaded.entity_id, loaded.sequence, &events, &stamped)?;
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
        if let Some(command_id) = metadata
            .command_id
            .as_ref()
            .filter(|id| !id.as_str().is_empty())
        {
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

/// Loads any aggregate type from a repository, retrying when a sequence gap shows a compaction
/// raced the read. [`Store::load`] and [`Transaction::read`](crate::Transaction::read) share it.
pub(crate) async fn load_aggregate<A: Aggregate, R: Repository>(
    repository: &R,
    entity_id: EntityId,
) -> Result<Loaded<A>, Error> {
    let mut attempt = 1;

    // A sequence gap means another writer snapshotted and compacted the stream between the
    // snapshot read and the event read. Loading again starts from the newer snapshot.
    loop {
        match load_once(repository, entity_id.clone()).await {
            Err(err) if err.is_conflict() && attempt < LOAD_ATTEMPTS => attempt += 1,
            result => return result,
        }
    }
}

async fn load_once<A: Aggregate, R: Repository>(
    repository: &R,
    entity_id: EntityId,
) -> Result<Loaded<A>, Error> {
    let snapshot = repository.snapshot(&entity_id).await?;
    let has_snapshot = snapshot.is_some();

    let mut loaded = match snapshot {
        Some(snapshot) => {
            let aggregate =
                serde_json::from_slice(&snapshot.payload).map_err(|source| Error::Snapshot {
                    entity_id: entity_id.clone(),
                    source,
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
        repository
            .events_after(&loaded.entity_id, loaded.sequence)
            .await?
    } else {
        repository.events(&loaded.entity_id).await?
    };

    for event in events {
        loaded.replay(event)?;
    }

    Ok(loaded)
}

/// An aggregate loaded from the event log, with the stream position it was loaded at.
#[derive(Debug)]
pub struct Loaded<A> {
    pub(crate) entity_id: EntityId,
    pub(crate) aggregate: A,
    pub(crate) sequence: EventSequence,
    pub(crate) snapshot_sequence: EventSequence,
    pub(crate) seen_command_ids: HashSet<CommandId>,
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
        if stored.sequence != self.sequence.next() {
            return Err(Error::SequenceGap {
                entity_id: self.entity_id.clone(),
                after: self.sequence,
                found: stored.sequence,
            });
        }

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
        if let Some(command_id) = stored
            .metadata
            .command_id
            .filter(|id| !id.as_str().is_empty())
        {
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
        // An empty command ID is absent, never a duplicate, as when stamping the commit ID.
        !command_id.as_str().is_empty() && self.seen_command_ids.contains(command_id)
    }
}

/// The result of a successful [`Store::execute`].
#[derive(Debug)]
#[non_exhaustive]
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
            sequence = sequence.checked_next().ok_or_else(|| {
                Error::InvalidCommit(format!(
                    "entity {entity_id} is at the maximum event sequence"
                ))
            })?;

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

/// Stamps the engine's commit identity: the command ID when present and non-empty, a
/// caller-supplied commit ID otherwise, else a generated ULID-format ID. An empty command ID is
/// absent, as in Go's `CommitMetadata`, so both languages derive the same commit ID.
pub(crate) fn stamp_commit_id(
    metadata: &Metadata,
    generate: &(dyn Fn() -> String + Send + Sync),
) -> Metadata {
    let mut metadata = metadata.clone();
    if let Some(command_id) = metadata
        .command_id
        .as_ref()
        .filter(|command_id| !command_id.as_str().is_empty())
    {
        metadata.commit_id = command_id.to_string();

        return metadata;
    }

    if metadata.commit_id.is_empty() {
        metadata.commit_id = generate();
    }

    metadata
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::{Added, Ledger};

    #[test]
    fn serializing_past_the_maximum_sequence_fails() {
        let events = [Added { amount: 1 }.into()];
        let id = EntityId::new("ledger-1");

        let result =
            serialize_events::<Ledger>(&id, EventSequence(i64::MAX), &events, &Metadata::default());

        assert!(matches!(result, Err(Error::InvalidCommit(_))), "{result:?}");
    }

    #[test]
    fn an_empty_command_id_is_absent_when_stamping_the_commit_id() {
        let generated = || "GENERATED".to_owned();

        let empty = Metadata::default().with_command_id("");
        assert_eq!(stamp_commit_id(&empty, &generated).commit_id, "GENERATED");

        let mut pinned = Metadata::default().with_command_id("");
        pinned.commit_id = "caller-commit".to_owned();
        assert_eq!(
            stamp_commit_id(&pinned, &generated).commit_id,
            "caller-commit"
        );

        // A non-empty command ID still wins over a caller-supplied commit ID.
        let mut derived = Metadata::default().with_command_id("cmd-1");
        derived.commit_id = "caller-commit".to_owned();
        assert_eq!(stamp_commit_id(&derived, &generated).commit_id, "cmd-1");
    }

    #[tokio::test]
    async fn an_empty_command_id_is_never_a_duplicate() {
        let store = crate::Store::<Ledger, _>::new(crate::MemRepository::new(), ());
        let empty = Metadata::default().with_command_id("");

        // The same empty command ID twice: absent both times, so neither is a duplicate.
        store
            .execute("ledger-1", crate::conformance::Add(1), empty.clone())
            .await
            .unwrap();
        store
            .execute("ledger-1", crate::conformance::Add(1), empty.clone())
            .await
            .unwrap();

        let loaded = store.load("ledger-1").await.unwrap();
        assert_eq!(loaded.aggregate().entries, 2);
        assert!(!loaded.has_seen(&CommandId::new("")));

        // A real command ID is still deduplicated.
        let real = Metadata::default().with_command_id("cmd-1");
        store
            .execute("ledger-1", crate::conformance::Add(1), real.clone())
            .await
            .unwrap();
        let duplicate = store
            .execute("ledger-1", crate::conformance::Add(1), real)
            .await;
        assert!(matches!(
            duplicate.unwrap_err().as_store_error(),
            Some(Error::DuplicateCommand(_))
        ));
    }
}
