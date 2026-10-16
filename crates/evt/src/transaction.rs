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
struct StreamState<Op> {
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
    /// The projector operations for the stream's cumulative appended events, recomputed on every
    /// append from the final aggregate state, never accumulated across appends.
    operations: Vec<Op>,
}

/// A stream read into a [`Transaction`]: the loaded aggregate, typed by its own `A`. Pass it to
/// [`Transaction::append`] to stage events for the stream; a read that is never appended to is
/// fenced at the head it saw.
pub struct Read<A: Aggregate, Op> {
    transaction: u64,
    index: usize,
    loaded: Loaded<A>,
    /// The sequence the stream was read at; appended events continue from `loaded.sequence`.
    base_sequence: EventSequence,
    /// Every event appended through this handle so far, so projectors see the cumulative set.
    appended: Vec<A::Event>,
    /// The stream's projector set, fixed by its first projector-bearing append and reused by later
    /// appends that name none.
    projectors: Option<Vec<Arc<dyn Projector<A, Op>>>>,
}

impl<A: Aggregate, Op> std::fmt::Debug for Read<A, Op> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Read")
            .field("entity_id", &self.loaded.entity_id)
            .field("sequence", &self.loaded.sequence)
            .finish_non_exhaustive()
    }
}

impl<A: Aggregate, Op> Read<A, Op> {
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
///
/// The one exception is a failure after an append has already changed a handle's aggregate: a
/// projector error or a snapshot that cannot be serialized. That handle is then ahead of the
/// staged events, so the transaction is poisoned and every later `append` and the `commit` return
/// [`Error::TransactionPoisoned`]. Discard it and build a new transaction.
pub struct Transaction<'a, R: Repository> {
    id: u64,
    repository: &'a R,
    commit_ids: CommitIdGenerator,
    snapshot_interval: usize,
    streams: Vec<StreamState<R::Operation>>,
    fences: Vec<ReadFence>,
    /// Set when a step failed after an append changed a handle's aggregate, leaving the handle
    /// ahead of what the transaction staged. Further appends and the commit are refused.
    poisoned: bool,
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
        read: &mut Read<A, R::Operation>,
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
            fences: Vec::new(),
            poisoned: false,
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
    ) -> Result<Read<A, R::Operation>, Error> {
        let entity_id = entity_id.into();
        self.check_unread(&entity_id)?;

        let loaded = load_aggregate::<A, R>(self.repository, entity_id).await?;

        self.read_loaded(loaded)
    }

    /// Adds an already loaded aggregate to the read set. See [`read`](Transaction::read).
    pub fn read_loaded<A: Aggregate>(
        &mut self,
        loaded: Loaded<A>,
    ) -> Result<Read<A, R::Operation>, Error> {
        self.check_unread(loaded.entity_id())?;

        self.streams.push(StreamState {
            entity_id: loaded.entity_id.clone(),
            entity_type: EntityType::new(A::TYPE),
            head: loaded.sequence,
            seen_command_ids: loaded.seen_command_ids.clone(),
            events: Vec::new(),
            snapshot: None,
            operations: Vec::new(),
        });

        Ok(Read {
            transaction: self.id,
            index: self.streams.len() - 1,
            base_sequence: loaded.sequence,
            appended: Vec::new(),
            projectors: None,
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
    /// serialized by `A`. Calling it again for the same handle continues the sequence. It runs the
    /// stream's projectors only if an earlier [`append_with`](Transaction::append_with) or
    /// [`Store::append_to`] fixed them; otherwise none run.
    pub fn append<A: Aggregate>(
        &mut self,
        read: &mut Read<A, R::Operation>,
        events: Vec<A::Event>,
    ) -> Result<&mut Self, Error> {
        self.append_with(read, events, &[])
    }

    /// Like [`append`](Transaction::append), running `projectors` for the staged events. The first
    /// call that names projectors fixes the stream's projector set; later calls, with or without
    /// projectors, derive the stream's operations with that set, and naming a different set returns
    /// [`Error::ProjectorSetConflict`] without changing anything. Sets are compared by projector
    /// identity (the `Arc` allocation), so pass clones of the same `Arc`s. Their
    /// operations commit atomically with the transaction's events. Projectors run on the
    /// post-event state, so when one fails the handle's aggregate already reflects the events:
    /// discard the transaction.
    pub fn append_with<A: Aggregate>(
        &mut self,
        read: &mut Read<A, R::Operation>,
        events: Vec<A::Event>,
        projectors: &[Arc<dyn Projector<A, R::Operation>>],
    ) -> Result<&mut Self, Error> {
        let interval = self.snapshot_interval;
        self.stage(read, events, projectors, interval)?;

        Ok(self)
    }

    fn stage<A: Aggregate>(
        &mut self,
        read: &mut Read<A, R::Operation>,
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
        if self.poisoned {
            return Err(Error::TransactionPoisoned);
        }

        // The stream's projector set is fixed by its first projector-bearing append. An append
        // that names none reuses it, so a stream first staged with projectors keeps its view
        // update.
        let remembered;
        let named = projectors;
        let projectors = if projectors.is_empty() {
            remembered = read.projectors.clone().unwrap_or_default();
            &remembered[..]
        } else {
            projectors
        };
        let identity = |set: &[Arc<dyn Projector<A, R::Operation>>]| -> Vec<usize> {
            set.iter()
                .map(|projector| Arc::as_ptr(projector) as *const () as usize)
                .collect()
        };
        if let Some(existing) = &read.projectors
            && !named.is_empty()
            && identity(existing) != identity(projectors)
        {
            return Err(Error::ProjectorSetConflict {
                entity_id: read.loaded.entity_id.clone(),
            });
        }

        if events.is_empty() {
            return Ok(());
        }

        // Serialization changes nothing, so its failure leaves the transaction usable.
        let serialized = serialize_events::<A>(
            &read.loaded.entity_id,
            read.loaded.sequence,
            &events,
            &Metadata::default(),
        )?;

        // From here the handle's aggregate changes. A failure in any later step leaves it ahead
        // of the staged events, so the transaction is poisoned: a half-applied handle must never
        // reach a commit, where a snapshot could capture state its persisted events lack.
        let projected = (!projectors.is_empty()).then(|| events.clone());
        for event in &events {
            read.loaded.aggregate.apply(event.clone());
        }

        let staged =
            self.stage_derived(read, &serialized, projected, projectors, snapshot_interval);
        let (operations, snapshot) = match staged {
            Ok(derived) => derived,
            Err(error) => {
                self.poisoned = true;

                return Err(error);
            }
        };

        if read.projectors.is_none() && !named.is_empty() {
            read.projectors = Some(projectors.to_vec());
        }
        read.appended.extend(events);
        read.loaded.sequence = serialized
            .last()
            .map_or(read.loaded.sequence, |event| event.sequence);

        let stream = &mut self.streams[read.index];
        stream.events.extend(serialized);
        stream.snapshot = snapshot;
        stream.operations = operations;

        Ok(())
    }

    /// Derives the projector operations (from the stream's cumulative appended events and final
    /// aggregate state) and the snapshot for the state after the new events.
    #[allow(clippy::type_complexity)]
    fn stage_derived<A: Aggregate>(
        &self,
        read: &Read<A, R::Operation>,
        serialized: &[SerializedEvent],
        projected: Option<Vec<A::Event>>,
        projectors: &[Arc<dyn Projector<A, R::Operation>>],
        snapshot_interval: usize,
    ) -> Result<(Vec<R::Operation>, Option<NewSnapshot>), Error> {
        let mut operations = Vec::new();
        if let Some(new_events) = projected {
            let mut cumulative = read.appended.clone();
            cumulative.extend(new_events);

            for projector in projectors {
                operations.extend(
                    projector
                        .project(&read.loaded.entity_id, &read.loaded.aggregate, &cumulative)
                        .map_err(Error::Projector)?,
                );
            }
        }

        let appended = self.streams[read.index].events.len() + serialized.len();
        let snapshot = if snapshot_events_due(read.base_sequence, appended, snapshot_interval) > 0 {
            Some(NewSnapshot {
                entity_type: EntityType::new(A::TYPE),
                sequence: read.loaded.snapshot_sequence.next(),
                payload: json::to_vec(&read.loaded.aggregate)?,
            })
        } else {
            None
        };

        Ok((operations, snapshot))
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
        if self.poisoned {
            return Err(Error::TransactionPoisoned);
        }

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

        // Projector operations follow the streams in read order, each stream's own in emission
        // order, as recomputed from its final appended state.
        let mut operations = Vec::new();
        let mut written = Vec::new();
        for mut stream in streams {
            if stream.events.is_empty() {
                self.fences.push(ReadFence {
                    entity_id: stream.entity_id,
                    entity_type: stream.entity_type,
                    head: stream.head,
                    provenance: FenceProvenance::Engine,
                });

                continue;
            }

            operations.append(&mut stream.operations);
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
        commit.operations = operations;

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

    /// A repository that records the transaction it is asked to commit, with string operations.
    #[derive(Default)]
    struct Recording {
        committed: std::sync::Mutex<Vec<TransactionCommit<String>>>,
    }

    impl Repository for Recording {
        type Operation = String;

        async fn commit(&self, _: Commit<String>) -> Result<(), Error> {
            unimplemented!("transactions only")
        }

        async fn commit_transaction(&self, commit: TransactionCommit<String>) -> Result<(), Error> {
            self.committed.lock().unwrap().push(commit);

            Ok(())
        }

        async fn events(&self, _: &EntityId) -> Result<Vec<SerializedEvent>, Error> {
            Ok(Vec::new())
        }

        async fn events_after(
            &self,
            _: &EntityId,
            _: EventSequence,
        ) -> Result<Vec<SerializedEvent>, Error> {
            Ok(Vec::new())
        }

        async fn latest(&self, _: &EntityId, _: usize) -> Result<Vec<SerializedEvent>, Error> {
            Ok(Vec::new())
        }

        async fn snapshot(&self, _: &EntityId) -> Result<Option<crate::SerializedSnapshot>, Error> {
            Ok(None)
        }

        async fn put_snapshot(&self, _: &crate::SerializedSnapshot) -> Result<bool, Error> {
            Ok(true)
        }

        async fn compact_below(&self, _: &EntityId, _: EventSequence) -> Result<usize, Error> {
            Ok(0)
        }

        async fn entity_heads(
            &self,
            _: Option<&EntityType>,
        ) -> Result<std::collections::BTreeMap<EntityId, EventSequence>, Error> {
            Ok(Default::default())
        }
    }

    /// Writes one view operation per call, describing the final state and every event it saw,
    /// the way a state-keyed view projector writes one row per stream.
    struct ViewRow;

    impl Projector<Ledger, String> for ViewRow {
        fn project(
            &self,
            entity_id: &EntityId,
            ledger: &Ledger,
            events: &[crate::conformance::LedgerEvent],
        ) -> Result<Vec<String>, crate::BoxError> {
            Ok(vec![format!(
                "put {entity_id} total={} events={}",
                ledger.total,
                events.len()
            )])
        }
    }

    #[tokio::test]
    async fn repeated_appends_project_once_from_the_final_state() {
        let repository = Recording::default();
        let projectors: Vec<Arc<dyn Projector<Ledger, String>>> = vec![Arc::new(ViewRow)];

        let mut tx = Transaction::new(&repository).with_snapshot_interval(0);
        let mut ledger = tx.read::<Ledger>("view-1").await.unwrap();
        tx.append_with(&mut ledger, added(1), &projectors).unwrap();
        tx.append_with(&mut ledger, added(2), &projectors).unwrap();
        tx.commit(&Metadata::default()).await.unwrap();

        let committed = repository.committed.lock().unwrap();
        assert_eq!(
            committed[0].operations,
            vec!["put view-1 total=3 events=2".to_owned()],
            "one operation for the stream, from the final state and both batches' events"
        );
    }

    #[tokio::test]
    async fn operations_follow_each_streams_final_state_across_streams() {
        let repository = Recording::default();
        let projectors: Vec<Arc<dyn Projector<Ledger, String>>> = vec![Arc::new(ViewRow)];

        let mut tx = Transaction::new(&repository).with_snapshot_interval(0);
        let mut first = tx.read::<Ledger>("a").await.unwrap();
        let mut second = tx.read::<Ledger>("b").await.unwrap();
        tx.append_with(&mut first, added(1), &projectors).unwrap();
        tx.append_with(&mut second, added(5), &projectors).unwrap();
        tx.append_with(&mut first, added(1), &projectors).unwrap();
        tx.commit(&Metadata::default()).await.unwrap();

        let committed = repository.committed.lock().unwrap();
        assert_eq!(
            committed[0].operations,
            vec![
                "put a total=2 events=2".to_owned(),
                "put b total=5 events=1".to_owned()
            ]
        );
    }

    /// An aggregate whose snapshot encoding fails once its count reaches two.
    #[derive(Debug, Default, serde::Deserialize)]
    struct Fragile {
        count: i64,
    }

    impl serde::Serialize for Fragile {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            if self.count >= 2 {
                return Err(serde::ser::Error::custom("too fragile to snapshot"));
            }

            serializer.collect_str(&self.count)
        }
    }

    crate::domain_events! {
        #[derive(Clone, Debug, PartialEq)]
        enum FragileEvent {
            Bumped(Bumped) => ("fragile:bumped", 1),
        }
    }

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Bumped {}

    #[derive(Debug, thiserror::Error)]
    #[error("never")]
    struct Never;

    impl Aggregate for Fragile {
        const TYPE: &'static str = "fragile";

        type Command = ();
        type Event = FragileEvent;
        type Error = Never;
        type Services = ();

        async fn handle(&self, _: (), _: &()) -> Result<Vec<FragileEvent>, Never> {
            Ok(vec![Bumped {}.into()])
        }

        fn apply(&mut self, _: FragileEvent) {
            self.count += 1;
        }
    }

    #[tokio::test]
    async fn a_failed_append_after_the_aggregate_changed_poisons_the_transaction() {
        let repository = MemRepository::new();
        let bump = || -> Vec<FragileEvent> { vec![Bumped {}.into()] };

        // Every append crosses the snapshot interval, so the second one's snapshot cannot encode.
        let mut tx = Transaction::new(&repository).with_snapshot_interval(1);
        let mut fragile = tx.read::<Fragile>("fragile-1").await.unwrap();
        tx.append(&mut fragile, bump()).unwrap();

        let failed = tx.append(&mut fragile, bump()).map(|_| ()).unwrap_err();
        assert!(matches!(failed, Error::Json(_)), "{failed:?}");

        // The handle is ahead of the staged events now: nothing more can be staged or committed.
        assert!(matches!(
            tx.append(&mut fragile, bump()).map(|_| ()),
            Err(Error::TransactionPoisoned)
        ));
        assert!(matches!(
            tx.commit(&Metadata::default()).await,
            Err(Error::TransactionPoisoned)
        ));
        assert!(
            repository
                .events(&"fragile-1".into())
                .await
                .unwrap()
                .is_empty(),
            "a poisoned transaction writes nothing"
        );
    }

    #[tokio::test]
    async fn a_projector_failure_poisons_the_transaction_too() {
        struct Failing;
        impl Projector<Ledger, String> for Failing {
            fn project(
                &self,
                _: &EntityId,
                _: &Ledger,
                _: &[crate::conformance::LedgerEvent],
            ) -> Result<Vec<String>, crate::BoxError> {
                Err("projection failed".into())
            }
        }

        let repository = Recording::default();
        let projectors: Vec<Arc<dyn Projector<Ledger, String>>> = vec![Arc::new(Failing)];

        let mut tx = Transaction::new(&repository).with_snapshot_interval(0);
        let mut ledger = tx.read::<Ledger>("view-2").await.unwrap();
        assert!(matches!(
            tx.append_with(&mut ledger, added(1), &projectors)
                .map(|_| ()),
            Err(Error::Projector(_))
        ));
        assert!(matches!(
            tx.commit(&Metadata::default()).await,
            Err(Error::TransactionPoisoned)
        ));
        assert!(repository.committed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_serialization_error_before_any_change_leaves_the_transaction_usable() {
        let store = store(0);
        let mut tx = store.transaction();
        let mut ledger = tx.read::<Ledger>("usable").await.unwrap();

        // Appending nothing stages nothing and poisons nothing.
        tx.append(&mut ledger, Vec::new()).unwrap();
        tx.append(&mut ledger, added(1)).unwrap();
        tx.commit(&Metadata::default()).await.unwrap();
        assert_eq!(stored_events(&store, "usable").await, 1);
    }

    #[tokio::test]
    async fn a_plain_append_keeps_the_streams_remembered_projectors() {
        let repository = Recording::default();
        let projectors: Vec<Arc<dyn Projector<Ledger, String>>> = vec![Arc::new(ViewRow)];

        let mut tx = Transaction::new(&repository).with_snapshot_interval(0);
        let mut ledger = tx.read::<Ledger>("remembered").await.unwrap();
        tx.append_with(&mut ledger, added(1), &projectors).unwrap();
        // A plain append, and an append_with that names none, both reuse the fixed set.
        tx.append(&mut ledger, added(2)).unwrap();
        tx.append_with(&mut ledger, added(3), &[]).unwrap();
        tx.commit(&Metadata::default()).await.unwrap();

        let committed = repository.committed.lock().unwrap();
        assert_eq!(
            committed[0].operations,
            vec!["put remembered total=6 events=3".to_owned()],
            "the view update survives the later plain appends"
        );
    }

    #[tokio::test]
    async fn a_stream_first_staged_plain_adopts_the_first_named_set() {
        let repository = Recording::default();
        let projectors: Vec<Arc<dyn Projector<Ledger, String>>> = vec![Arc::new(ViewRow)];

        let mut tx = Transaction::new(&repository).with_snapshot_interval(0);
        let mut ledger = tx.read::<Ledger>("late").await.unwrap();
        tx.append(&mut ledger, added(1)).unwrap();
        tx.append_with(&mut ledger, added(2), &projectors).unwrap();
        tx.commit(&Metadata::default()).await.unwrap();

        let committed = repository.committed.lock().unwrap();
        assert_eq!(
            committed[0].operations,
            vec!["put late total=3 events=2".to_owned()],
            "the set named later covers the cumulative events"
        );
    }

    #[tokio::test]
    async fn naming_a_different_projector_set_for_a_stream_is_a_typed_error() {
        struct Other;
        impl Projector<Ledger, String> for Other {
            fn project(
                &self,
                _: &EntityId,
                _: &Ledger,
                _: &[crate::conformance::LedgerEvent],
            ) -> Result<Vec<String>, crate::BoxError> {
                Ok(vec!["other".to_owned()])
            }
        }

        let repository = Recording::default();
        let first: Vec<Arc<dyn Projector<Ledger, String>>> = vec![Arc::new(ViewRow)];
        let different: Vec<Arc<dyn Projector<Ledger, String>>> = vec![Arc::new(Other)];
        let same_again: Vec<Arc<dyn Projector<Ledger, String>>> = first.clone();

        let mut tx = Transaction::new(&repository).with_snapshot_interval(0);
        let mut ledger = tx.read::<Ledger>("fixed").await.unwrap();
        tx.append_with(&mut ledger, added(1), &first).unwrap();

        let error = tx
            .append_with(&mut ledger, added(2), &different)
            .map(|_| ())
            .unwrap_err();
        assert!(
            matches!(&error, Error::ProjectorSetConflict { entity_id } if entity_id.as_str() == "fixed"),
            "{error:?}"
        );
        assert_eq!(
            ledger.sequence(),
            EventSequence(1),
            "the refused append changed nothing"
        );

        // The same projectors again are accepted, and the transaction is not poisoned.
        tx.append_with(&mut ledger, added(2), &same_again).unwrap();
        tx.commit(&Metadata::default()).await.unwrap();

        let committed = repository.committed.lock().unwrap();
        assert_eq!(
            committed[0].operations,
            vec!["put fixed total=3 events=2".to_owned()]
        );
    }
}
