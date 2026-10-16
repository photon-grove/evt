//! Multi-stream transactions (ADR 0004, C1/C2) over any mix of aggregate types.
//!
//! A [`Transaction`] holds a type-erased record of every stream it touches: the stream ID, its
//! entity type, the head it was read at, and the serialized events appended since. The aggregate
//! type stays at the call sites through generics: [`Transaction::read`] returns a typed
//! [`Read<A>`] handle that owns the loaded aggregate, and [`Transaction::append`] takes that
//! handle, so each stream's events are serialized by its own `A` while one commit spans streams
//! of several aggregate types, such as a profile and the unique-name claim that guards it.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::aggregate::{Aggregate, Projector};
use crate::commit_id::CommitIdGenerator;
use crate::error::Error;
use crate::event::SerializedEvent;
use crate::ids::{CommandId, EntityId, EntityType, EventSequence};
use crate::json;
use crate::metadata::Metadata;
use crate::repository::{
    Commit, FenceProvenance, NewSnapshot, ReadFence, Repository, TransactionCommit,
    snapshot_events_due,
};
use crate::store::{
    DEFAULT_SNAPSHOT_INTERVAL, Loaded, Store, load_aggregate, serialize_events, stamp_commit_id,
};

static NEXT_TRANSACTION: AtomicU64 = AtomicU64::new(1);

/// One stream of a transaction, independent of its aggregate type.
#[derive(Debug)]
struct StreamState {
    entity_id: EntityId,
    entity_type: EntityType,
    /// The head the stream was read at: its last event sequence, snapshot-aware.
    head: EventSequence,
    /// Command IDs the stream's replay recorded, for duplicate-command detection.
    seen_command_ids: HashSet<CommandId>,
    /// Events appended since the read, serialized by the stream's own aggregate type. Their
    /// metadata is stamped once at commit.
    events: Vec<SerializedEvent>,
    snapshot: Option<NewSnapshot>,
}

/// A stream read into a [`Transaction`]: the loaded aggregate, typed by its own `A`. Pass it to
/// [`Transaction::append`] to stage events for the stream; a read that is never appended to is
/// fenced at the head it saw.
#[derive(Debug)]
pub struct Read<A> {
    transaction: u64,
    index: usize,
    loaded: Loaded<A>,
    /// The sequence the stream was read at; appended events continue from `loaded.sequence`.
    base_sequence: EventSequence,
}

impl<A: Aggregate> Read<A> {
    /// The entity ID.
    pub fn entity_id(&self) -> &EntityId {
        self.loaded.entity_id()
    }

    /// The aggregate state, including events appended through this handle.
    pub fn aggregate(&self) -> &A {
        self.loaded.aggregate()
    }

    /// The sequence of the last event, including events appended through this handle.
    pub fn sequence(&self) -> EventSequence {
        self.loaded.sequence()
    }
}

/// One multi-stream transaction. A caller reads streams with [`read`](Transaction::read) (any
/// aggregate type, mixed freely), stages decided events with [`append`](Transaction::append), adds
/// caller-stamped fences with [`fence`](Transaction::fence), and commits once with
/// [`commit`](Transaction::commit). A stream that was read but never appended to contributes an
/// engine-filled [read fence](ReadFence), whatever its type, so the commit fails when another
/// writer moved it past the head the reader saw.
///
/// The single-aggregate shortcut stays: [`Store::execute`] remains a load, handle, and commit of
/// one entity, over the same repository commit path.
///
/// A transaction commits at most once: [`commit`](Transaction::commit) takes it by value, so any
/// commit attempt, successful or failed, consumes it, and a new transaction must be built to try
/// again. The staging calls validate their input before changing anything, so their errors are
/// recoverable and leave the transaction unchanged: a duplicate [`read`](Transaction::read), or an
/// [`append`](Transaction::append) with a handle from another transaction, returns
/// [`Error::InvalidCommit`], and the caller can carry on and commit.
pub struct Transaction<'a, R: Repository> {
    id: u64,
    repository: &'a R,
    commit_ids: CommitIdGenerator,
    snapshot_interval: usize,
    streams: Vec<StreamState>,
    operations: Vec<R::Operation>,
    fences: Vec<ReadFence>,
}

impl<A: Aggregate, R: Repository> Store<A, R> {
    /// Begins a multi-stream transaction on this store's repository. It reads and appends any
    /// aggregate type; the store's snapshot interval and commit-ID generator apply to it. To
    /// run this store's projectors for its own aggregate type, stage events with
    /// [`Store::append_to`]; [`Transaction::append`] runs none.
    pub fn transaction(&self) -> Transaction<'_, R> {
        Transaction::with_settings(
            &self.repository,
            self.commit_ids.clone(),
            self.snapshot_interval,
        )
    }

    /// Stages events for a stream of this store's aggregate type, running the store's projectors
    /// and its snapshot interval, as [`Store::commit`] does for a single stream. The
    /// transaction should come from [`Store::transaction`] (or share this store's repository).
    pub fn append_to(
        &self,
        transaction: &mut Transaction<'_, R>,
        read: &mut Read<A>,
        events: Vec<A::Event>,
    ) -> Result<(), Error> {
        transaction.stage(read, events, &self.projectors, self.snapshot_interval)
    }
}

impl<'a, R: Repository> Transaction<'a, R> {
    /// Begins a transaction on a repository, with the default snapshot interval and the default
    /// commit-ID generator. [`Store::transaction`] carries a store's settings instead.
    pub fn new(repository: &'a R) -> Self {
        Self::with_settings(
            repository,
            crate::commit_id::default_generator(),
            DEFAULT_SNAPSHOT_INTERVAL,
        )
    }

    fn with_settings(
        repository: &'a R,
        commit_ids: CommitIdGenerator,
        snapshot_interval: usize,
    ) -> Self {
        Self {
            id: NEXT_TRANSACTION.fetch_add(1, Ordering::Relaxed),
            repository,
            commit_ids,
            snapshot_interval,
            streams: Vec::new(),
            operations: Vec::new(),
            fences: Vec::new(),
        }
    }

    /// Sets the number of events between snapshots for streams staged with
    /// [`append`](Transaction::append). 0 disables snapshots.
    #[must_use]
    pub fn with_snapshot_interval(mut self, interval: usize) -> Self {
        self.snapshot_interval = interval;
        self
    }

    /// Loads a stream of aggregate type `A` into the read set and returns its typed handle. The
    /// load is snapshot-aware and retries a compaction race, like [`Store::load`]; the stream may
    /// be of any aggregate type, whatever the others in the transaction are. Reading the same
    /// stream twice returns [`Error::InvalidCommit`].
    pub async fn read<A: Aggregate>(
        &mut self,
        entity_id: impl Into<EntityId>,
    ) -> Result<Read<A>, Error> {
        let entity_id = entity_id.into();
        self.check_unread(&entity_id)?;

        let loaded = load_aggregate::<A, R>(self.repository, entity_id).await?;

        self.read_loaded(loaded)
    }

    /// Adds an already loaded aggregate to the read set. See [`read`](Transaction::read).
    pub fn read_loaded<A: Aggregate>(&mut self, loaded: Loaded<A>) -> Result<Read<A>, Error> {
        self.check_unread(loaded.entity_id())?;

        self.streams.push(StreamState {
            entity_id: loaded.entity_id.clone(),
            entity_type: EntityType::new(A::TYPE),
            head: loaded.sequence,
            seen_command_ids: loaded.seen_command_ids.clone(),
            events: Vec::new(),
            snapshot: None,
        });

        Ok(Read {
            transaction: self.id,
            index: self.streams.len() - 1,
            base_sequence: loaded.sequence,
            loaded,
        })
    }

    fn check_unread(&self, entity_id: &EntityId) -> Result<(), Error> {
        if self
            .streams
            .iter()
            .any(|stream| stream.entity_id == *entity_id)
        {
            return Err(Error::InvalidCommit(format!(
                "duplicate read of {entity_id}"
            )));
        }

        Ok(())
    }

    /// Stages decided events for a stream read by this transaction. The events are applied to the
    /// handle's aggregate, so its state and the snapshot the commit captures reflect them, and
    /// serialized by `A`. Calling it again for the same handle continues the sequence. No
    /// projectors run; use [`append_with`](Transaction::append_with) or [`Store::append_to`] to
    /// add them.
    pub fn append<A: Aggregate>(
        &mut self,
        read: &mut Read<A>,
        events: Vec<A::Event>,
    ) -> Result<&mut Self, Error> {
        self.append_with(read, events, &[])
    }

    /// Like [`append`](Transaction::append), running `projectors` for the staged events. Their
    /// operations commit atomically with the transaction's events. Projectors run on the
    /// post-event state, so when one fails the handle's aggregate already reflects the events:
    /// discard the transaction.
    pub fn append_with<A: Aggregate>(
        &mut self,
        read: &mut Read<A>,
        events: Vec<A::Event>,
        projectors: &[Arc<dyn Projector<A, R::Operation>>],
    ) -> Result<&mut Self, Error> {
        let interval = self.snapshot_interval;
        self.stage(read, events, projectors, interval)?;

        Ok(self)
    }

    fn stage<A: Aggregate>(
        &mut self,
        read: &mut Read<A>,
        events: Vec<A::Event>,
        projectors: &[Arc<dyn Projector<A, R::Operation>>],
        snapshot_interval: usize,
    ) -> Result<(), Error> {
        if read.transaction != self.id {
            return Err(Error::InvalidCommit(format!(
                "stream {} was not read by this transaction",
                read.loaded.entity_id
            )));
        }
        if events.is_empty() {
            return Ok(());
        }

        // Everything fallible runs before the handle or the transaction changes.
        let serialized = serialize_events::<A>(
            &read.loaded.entity_id,
            read.loaded.sequence,
            &events,
            &Metadata::default(),
        )?;

        let projected = (!projectors.is_empty()).then(|| events.clone());
        for event in events {
            read.loaded.aggregate.apply(event);
        }

        let mut operations = Vec::new();
        if let Some(projected) = &projected {
            for projector in projectors {
                operations.extend(
                    projector
                        .project(&read.loaded.entity_id, &read.loaded.aggregate, projected)
                        .map_err(Error::Projector)?,
                );
            }
        }

        let stream = &mut self.streams[read.index];
        let appended = stream.events.len() + serialized.len();
        let snapshot = if snapshot_events_due(read.base_sequence, appended, snapshot_interval) > 0 {
            Some(NewSnapshot {
                entity_type: EntityType::new(A::TYPE),
                sequence: read.loaded.snapshot_sequence.next(),
                payload: json::to_vec(&read.loaded.aggregate)?,
            })
        } else {
            None
        };

        read.loaded.sequence = serialized
            .last()
            .map_or(read.loaded.sequence, |event| event.sequence);
        stream.events.extend(serialized);
        stream.snapshot = snapshot;
        self.operations.extend(operations);

        Ok(())
    }

    /// Adds a caller-stamped read fence for a stream the transaction did not read. The fence fails
    /// the commit when another writer moved the stream past the given head, and a conflict on it
    /// is a stale-fence rejection rather than a retry. A fence for a stream that was read is
    /// dropped, whether `fence` is called before or after `read`: the engine's own fence
    /// provenance supersedes it.
    pub fn fence(&mut self, mut fence: ReadFence) -> Result<&mut Self, Error> {
        if self
            .streams
            .iter()
            .any(|stream| stream.entity_id == fence.entity_id)
        {
            return Ok(self);
        }

        fence.provenance = FenceProvenance::Caller;
        self.fences.push(fence);

        Ok(self)
    }

    /// Stamps the commit ID once, fences every unwritten read at the head it saw, and persists
    /// exactly one atomic repository commit. On failure nothing is written. It returns the
    /// committed events in the canonical order the commit persists them: stream ID bytewise,
    /// then sequence.
    pub async fn commit(mut self, metadata: &Metadata) -> Result<Vec<SerializedEvent>, Error> {
        let metadata = stamp_commit_id(metadata, &*self.commit_ids);

        if let Some(command_id) = &metadata.command_id
            && !command_id.as_str().is_empty()
            && self
                .streams
                .iter()
                .any(|stream| stream.seen_command_ids.contains(command_id))
        {
            return Err(Error::DuplicateCommand(command_id.clone()));
        }

        // A caller-stamped fence for a stream this transaction read is superseded by the
        // engine's own fence (or by the write), whichever order `fence` and `read` were called in.
        let streams = std::mem::take(&mut self.streams);
        self.fences.retain(|fence| {
            !streams
                .iter()
                .any(|stream| stream.entity_id == fence.entity_id)
        });

        let mut written = Vec::new();
        for stream in streams {
            if stream.events.is_empty() {
                self.fences.push(ReadFence {
                    entity_id: stream.entity_id,
                    entity_type: stream.entity_type,
                    head: stream.head,
                    provenance: FenceProvenance::Engine,
                });

                continue;
            }

            written.push(stream);
        }

        // The persisted order, so the returned events match it whatever order streams were read.
        written.sort_by(|a, b| {
            a.entity_id
                .as_str()
                .as_bytes()
                .cmp(b.entity_id.as_str().as_bytes())
        });

        let mut committed = Vec::new();
        let mut commits = Vec::new();
        for stream in written {
            let mut events = stream.events;
            for event in &mut events {
                event.metadata = metadata.clone();
            }

            committed.extend(events.iter().cloned());
            commits.push(Commit {
                entity_id: stream.entity_id,
                events,
                snapshot: stream.snapshot,
                operations: Vec::new(),
            });
        }

        let mut commit = TransactionCommit::new(commits);
        commit.fences = self.fences;
        commit.operations = self.operations;

        self.repository.commit_transaction(commit).await?;

        Ok(committed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::{Add, Added, Claim, ClaimEvent, Ledger, Take, Taken};
    use crate::mem::MemRepository;

    fn store(interval: usize) -> Store<Ledger, MemRepository> {
        Store::new(MemRepository::new(), ()).with_snapshot_interval(interval)
    }

    async fn seed(store: &Store<Ledger, MemRepository>, id: &str, additions: usize) {
        for _ in 0..additions {
            store
                .execute(id, Add(1), Metadata::default())
                .await
                .expect("seed");
        }
    }

    fn added(amount: i64) -> Vec<<Ledger as Aggregate>::Event> {
        vec![Added { amount }.into()]
    }

    fn taken(holder: &str) -> Vec<ClaimEvent> {
        vec![
            Taken {
                holder: holder.to_owned(),
            }
            .into(),
        ]
    }

    async fn stored_events(store: &Store<Ledger, MemRepository>, id: &str) -> usize {
        store.repository().events(&id.into()).await.unwrap().len()
    }

    #[tokio::test]
    async fn one_transaction_commits_two_aggregate_types_atomically() {
        let store = store(0);
        seed(&store, "ledger-1", 2).await;

        let mut tx = store.transaction();
        let mut ledger = tx.read::<Ledger>("ledger-1").await.unwrap();
        let mut claim = tx.read::<Claim>("claim-ada").await.unwrap();
        tx.append(&mut ledger, added(5)).unwrap();
        tx.append(&mut claim, taken("ada")).unwrap();

        assert_eq!(ledger.aggregate().total, 7);
        assert_eq!(claim.aggregate().holder.as_deref(), Some("ada"));

        let committed = tx.commit(&Metadata::default()).await.unwrap();
        assert_eq!(committed.len(), 2);

        // Each stream was serialized by its own aggregate type.
        let ledger_events = store.repository().events(&"ledger-1".into()).await.unwrap();
        assert_eq!(
            ledger_events.last().unwrap().entity_type,
            EntityType::new("ledger")
        );
        assert_eq!(
            ledger_events.last().unwrap().event_type,
            crate::EventType::new("ledger:added")
        );
        let claim_events = store
            .repository()
            .events(&"claim-ada".into())
            .await
            .unwrap();
        assert_eq!(claim_events[0].entity_type, EntityType::new("claim"));
        assert_eq!(
            claim_events[0].event_type,
            crate::EventType::new("claim:taken")
        );
        assert_eq!(
            claim_events[0].metadata.commit_id,
            ledger_events[2].metadata.commit_id
        );
    }

    #[tokio::test]
    async fn a_read_only_stream_of_another_type_gets_an_engine_fence() {
        let store = store(0);
        seed(&store, "ledger-1", 1).await;

        let claim_store = Store::<Claim, _>::new(store.repository().clone(), ());
        claim_store
            .execute("claim-ada", Take("ada".into()), Metadata::default())
            .await
            .unwrap();

        // The ledger is written; the claim, a different aggregate type, is only read.
        let mut tx = store.transaction();
        let mut ledger = tx.read::<Ledger>("ledger-1").await.unwrap();
        let _claim = tx.read::<Claim>("claim-ada").await.unwrap();
        tx.append(&mut ledger, added(1)).unwrap();

        // Another writer moves the claim past the head the transaction read.
        let rival = Store::<Claim, _>::new(store.repository().clone(), ());
        let mut other = rival.load("claim-ada").await.unwrap();
        other = rival
            .commit(other, taken("grace"), &Metadata::default())
            .await
            .unwrap()
            .0;
        assert_eq!(other.aggregate().holder.as_deref(), Some("grace"));

        let error = tx.commit(&Metadata::default()).await.unwrap_err();
        match &error {
            Error::FenceConflict {
                entity_id,
                head,
                provenance,
            } => {
                assert_eq!(entity_id.as_str(), "claim-ada");
                assert_eq!(*head, EventSequence(1));
                assert_eq!(*provenance, FenceProvenance::Engine);
            }
            other => panic!("expected a fence conflict, got {other:?}"),
        }
        assert!(error.is_retryable(), "an engine-filled fence retries");
        assert_eq!(
            stored_events(&store, "ledger-1").await,
            1,
            "nothing written"
        );
    }

    #[tokio::test]
    async fn an_unwritten_read_is_fenced_at_the_head_it_saw_with_engine_provenance() {
        let store = store(0);
        seed(&store, "read-only", 2).await;
        seed(&store, "written", 1).await;

        // Without interference the transaction commits and leaves the fenced stream alone.
        let mut tx = store.transaction();
        tx.read::<Ledger>("read-only").await.unwrap();
        let mut written = tx.read::<Ledger>("written").await.unwrap();
        tx.append(&mut written, added(5)).unwrap();
        let committed = tx.commit(&Metadata::default()).await.unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(stored_events(&store, "read-only").await, 2);
        assert_eq!(stored_events(&store, "written").await, 2);

        // Another writer moves the fenced stream past the head the transaction read.
        let mut tx = store.transaction();
        tx.read::<Ledger>("read-only").await.unwrap();
        let mut written = tx.read::<Ledger>("written").await.unwrap();
        tx.append(&mut written, added(5)).unwrap();
        seed(&store, "read-only", 1).await;

        let error = tx.commit(&Metadata::default()).await.unwrap_err();
        match &error {
            Error::FenceConflict {
                entity_id,
                head,
                provenance,
            } => {
                assert_eq!(entity_id.as_str(), "read-only");
                assert_eq!(*head, EventSequence(2), "the head the transaction read");
                assert_eq!(*provenance, FenceProvenance::Engine);
            }
            other => panic!("expected a fence conflict, got {other:?}"),
        }
        assert!(error.is_conflict());
        assert!(error.is_retryable(), "an engine-filled fence retries");
        assert_eq!(stored_events(&store, "written").await, 2, "nothing written");
    }

    #[tokio::test]
    async fn a_fenced_read_of_a_compacted_stream_keeps_the_snapshots_head() {
        let store = store(2);
        seed(&store, "compacted", 2).await;
        store
            .repository()
            .compact_below(&"compacted".into(), EventSequence(2))
            .await
            .unwrap();
        assert_eq!(stored_events(&store, "compacted").await, 0);

        let mut tx = store.transaction();
        tx.read::<Ledger>("compacted").await.unwrap();
        let mut target = tx.read::<Ledger>("target").await.unwrap();
        tx.append(&mut target, added(1)).unwrap();
        seed(&store, "compacted", 1).await;

        match tx.commit(&Metadata::default()).await.unwrap_err() {
            Error::FenceConflict { head, .. } => assert_eq!(head, EventSequence(2)),
            other => panic!("expected a fence conflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_caller_stamped_fence_is_not_retryable() {
        let store = store(0);
        seed(&store, "external", 2).await;

        let mut tx = store.transaction();
        let mut target = tx.read::<Ledger>("target").await.unwrap();
        tx.append(&mut target, added(1)).unwrap();
        tx.fence(ReadFence::new(
            "external",
            "ledger",
            EventSequence(1),
            FenceProvenance::Engine,
        ))
        .unwrap();

        let error = tx.commit(&Metadata::default()).await.unwrap_err();
        assert!(
            matches!(
                error,
                Error::FenceConflict {
                    provenance: FenceProvenance::Caller,
                    head: EventSequence(1),
                    ..
                }
            ),
            "fence() stamps caller provenance whatever the input says: {error:?}"
        );
        assert!(!error.is_retryable());
        assert_eq!(stored_events(&store, "target").await, 0);
    }

    #[tokio::test]
    async fn a_caller_fence_for_an_already_read_stream_is_dropped_in_either_order() {
        for fence_first in [false, true] {
            let store = store(0);
            seed(&store, "read", 2).await;

            // Head 99 would fail the commit if the caller's fence survived.
            let stale =
                ReadFence::new("read", "ledger", EventSequence(99), FenceProvenance::Caller);

            let mut tx = store.transaction();
            if fence_first {
                tx.fence(stale.clone()).unwrap();
            }
            tx.read::<Ledger>("read").await.unwrap();
            if !fence_first {
                tx.fence(stale).unwrap();
            }
            let mut target = tx.read::<Ledger>("target").await.unwrap();
            tx.append(&mut target, added(1)).unwrap();

            tx.commit(&Metadata::default())
                .await
                .unwrap_or_else(|error| panic!("fence_first={fence_first}: {error:?}"));
            assert_eq!(stored_events(&store, "target").await, 1);
        }
    }

    #[tokio::test]
    async fn a_duplicate_command_in_any_stream_fails_the_whole_transaction() {
        let store = store(0);
        let metadata = Metadata::default().with_command_id("cmd-1");

        // The command already ran against the first stream of the read set.
        store
            .execute("earlier", Add(1), metadata.clone())
            .await
            .unwrap();

        let mut tx = store.transaction();
        tx.read::<Ledger>("earlier").await.unwrap();
        let mut later = tx.read::<Claim>("later").await.unwrap();
        tx.append(&mut later, taken("ada")).unwrap();

        let error = tx.commit(&metadata).await.unwrap_err();
        assert!(error.is_duplicate_command(), "{error:?}");
        assert_eq!(
            store
                .repository()
                .events(&"later".into())
                .await
                .unwrap()
                .len(),
            0,
            "nothing written"
        );
    }

    #[tokio::test]
    async fn staging_errors_are_recoverable() {
        let store = store(0);
        seed(&store, "known", 1).await;

        let mut tx = store.transaction();
        let mut known = tx.read::<Ledger>("known").await.unwrap();

        assert!(matches!(
            tx.read::<Ledger>("known").await,
            Err(Error::InvalidCommit(_))
        ));
        // The same stream is a duplicate whatever aggregate type reads it.
        assert!(matches!(
            tx.read::<Claim>("known").await,
            Err(Error::InvalidCommit(_))
        ));

        // A handle from another transaction is refused.
        let other = store.transaction();
        let mut elsewhere = {
            let mut other = other;
            other.read::<Ledger>("elsewhere").await.unwrap()
        };
        assert!(matches!(
            tx.append(&mut elsewhere, added(1)),
            Err(Error::InvalidCommit(_))
        ));

        // Both errors were recoverable: the transaction is unchanged and still commits. Only
        // commit() consumes it, by taking it by value.
        tx.append(&mut known, added(1)).unwrap();
        tx.commit(&Metadata::default()).await.unwrap();
        assert_eq!(stored_events(&store, "known").await, 2);
    }

    #[tokio::test]
    async fn one_commit_id_is_stamped_across_every_stream() {
        let store = store(0);

        let mut tx = store.transaction();
        let mut first = tx.read::<Ledger>("first").await.unwrap();
        let mut second = tx.read::<Claim>("second").await.unwrap();
        tx.append(&mut second, taken("ada")).unwrap();
        tx.append(&mut first, added(2)).unwrap();
        let committed = tx.commit(&Metadata::default()).await.unwrap();

        assert_eq!(committed.len(), 2);
        let commit_id = committed[0].metadata.commit_id.clone();
        assert!(!commit_id.is_empty(), "the engine generates a commit ID");
        assert!(committed.iter().all(|e| e.metadata.commit_id == commit_id));

        // The stored copies carry it too.
        for id in ["first", "second"] {
            let stored = store.repository().events(&id.into()).await.unwrap();
            assert_eq!(stored[0].metadata.commit_id, commit_id);
        }

        // With a command ID the commit ID derives from it.
        let metadata = Metadata::default().with_command_id("cmd-9");
        let mut tx = store.transaction();
        let mut first = tx.read::<Ledger>("first").await.unwrap();
        let mut second = tx.read::<Claim>("second").await.unwrap();
        tx.append(&mut first, added(3)).unwrap();
        tx.append(&mut second, taken("grace")).unwrap();
        let committed = tx.commit(&metadata).await.unwrap();
        assert!(committed.iter().all(|e| e.metadata.commit_id == "cmd-9"));
    }

    #[tokio::test]
    async fn returned_events_are_in_the_persisted_stream_then_sequence_order() {
        let store = store(0);

        // Streams are read and appended in reverse bytewise order, with two events on one.
        let mut tx = store.transaction();
        let mut b = tx.read::<Ledger>("b").await.unwrap();
        let mut a = tx.read::<Claim>("a").await.unwrap();
        tx.append(&mut b, added(1)).unwrap();
        tx.append(&mut b, added(2)).unwrap();
        tx.append(&mut a, taken("ada")).unwrap();
        let committed = tx.commit(&Metadata::default()).await.unwrap();

        let order: Vec<(&str, i64)> = committed
            .iter()
            .map(|event| (event.entity_id.as_str(), event.sequence.0))
            .collect();
        assert_eq!(order, vec![("a", 1), ("b", 1), ("b", 2)]);

        // And they are exactly what was persisted.
        let mut persisted = store.repository().events(&"a".into()).await.unwrap();
        persisted.extend(store.repository().events(&"b".into()).await.unwrap());
        assert_eq!(committed.len(), persisted.len());
        assert!(
            committed
                .iter()
                .zip(&persisted)
                .all(|(c, p)| c.id == p.id && c.sequence == p.sequence)
        );
    }

    #[tokio::test]
    async fn repeated_appends_continue_the_sequence_and_snapshot_once() {
        let store = store(3);

        let mut tx = store.transaction();
        let mut ledger = tx.read::<Ledger>("snap").await.unwrap();
        tx.append(&mut ledger, added(1)).unwrap();
        tx.append(&mut ledger, added(2)).unwrap();
        tx.append(&mut ledger, added(3)).unwrap();
        assert_eq!(ledger.sequence(), EventSequence(3));
        tx.commit(&Metadata::default()).await.unwrap();

        assert_eq!(stored_events(&store, "snap").await, 3);
        let snapshot = store.repository().snapshot(&"snap".into()).await.unwrap();
        let snapshot = snapshot.expect("the third event reaches the interval");
        assert_eq!(snapshot.event_sequence, EventSequence(3));
        assert_eq!(snapshot.sequence, EventSequence(1));
    }

    #[tokio::test]
    async fn an_injected_generator_stamps_every_stream_of_a_commit() {
        let store = Store::<Ledger, _>::new(MemRepository::new(), ())
            .with_snapshot_interval(0)
            .with_commit_id_generator(|| "injected-commit".to_owned());

        let mut tx = store.transaction();
        let mut first = tx.read::<Ledger>("first").await.unwrap();
        let mut second = tx.read::<Claim>("second").await.unwrap();
        tx.append(&mut first, added(1)).unwrap();
        tx.append(&mut second, taken("ada")).unwrap();
        // An empty command ID must not blank the commit ID.
        let committed = tx
            .commit(&Metadata::default().with_command_id(""))
            .await
            .unwrap();
        assert!(
            committed
                .iter()
                .all(|event| event.metadata.commit_id == "injected-commit")
        );
    }

    #[tokio::test]
    async fn an_empty_command_id_never_matches_a_stream_that_recorded_one() {
        let store = store(0);
        let empty = Metadata::default().with_command_id("");
        store
            .execute("earlier", Add(1), empty.clone())
            .await
            .unwrap();

        let mut tx = store.transaction();
        tx.read::<Ledger>("earlier").await.unwrap();
        let mut later = tx.read::<Ledger>("later").await.unwrap();
        tx.append(&mut later, added(1)).unwrap();

        tx.commit(&empty)
            .await
            .expect("an empty command ID is absent");
        assert_eq!(stored_events(&store, "later").await, 1);
    }
}
