//! The storage contract every backend implements.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::error::Error;
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::ids::{CommandId, EntityId, EntityType, EventId, EventSequence};
use crate::outcome::{Outcome, check_outcome};
use crate::projection::{ProjectedView, check_views, view_operation_size};

/// A snapshot to write with a commit. Its event sequence is the last event in the commit.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct NewSnapshot {
    /// The aggregate type.
    pub entity_type: EntityType,
    /// The new snapshot sequence: the previous snapshot's sequence plus one.
    pub sequence: EventSequence,
    /// The JSON encoding of the aggregate state after the commit's events.
    pub payload: Vec<u8>,
}

impl NewSnapshot {
    /// Creates a snapshot to write with a commit.
    pub fn new(
        entity_type: impl Into<EntityType>,
        sequence: EventSequence,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            entity_type: entity_type.into(),
            sequence,
            payload: payload.into(),
        }
    }
}

/// Everything a repository writes atomically for one command. A commit with no events and no
/// [outcome](Commit::with_outcome) is a no-op: repositories ignore its snapshot. Operations
/// without an event are refused.
#[derive(Debug)]
#[non_exhaustive]
pub struct Commit<Op> {
    /// The entity whose stream is appended to.
    pub entity_id: EntityId,
    /// The new events, in ascending sequence order.
    pub events: Vec<SerializedEvent>,
    /// The snapshot to write, if the commit crosses a snapshot boundary.
    pub snapshot: Option<NewSnapshot>,
    /// Extra backend operations from in-band projectors, written in order in the same
    /// transaction. DynamoDB puts them before the events. PostgreSQL runs them after the head
    /// statement and the event inserts, so a racing writer fails on its stream's head first.
    pub operations: Vec<Op>,
    /// Backend-neutral view operations from commit-wide projectors (v0.9, C7), with their
    /// provenance, written in order in the same transaction after the `operations`. Each
    /// repository owns the semantics: DynamoDB writes them to its views table, PostgreSQL through
    /// its view writer, and the in-memory repository to its view rows. A repository with nowhere
    /// to write them refuses the commit with [`Error::ViewsNotConfigured`].
    pub views: Vec<ProjectedView>,
    /// The command outcome to record in the same transaction (C3), after every other write. With
    /// no events it is an outcome-only commit: a rejection or an accepted no-op.
    pub outcome: Option<Outcome>,
}

impl<Op> Commit<Op> {
    /// Creates a commit of events with no snapshot, no operations, and no outcome.
    pub fn new(entity_id: impl Into<EntityId>, events: Vec<SerializedEvent>) -> Self {
        Self {
            entity_id: entity_id.into(),
            events,
            snapshot: None,
            operations: Vec::new(),
            views: Vec::new(),
            outcome: None,
        }
    }

    /// Records a command outcome in the same transaction, as the last write. The outcome's event
    /// IDs are derived from the commit's events and need not be set.
    #[must_use]
    pub fn with_outcome(mut self, outcome: Outcome) -> Self {
        self.outcome = Some(outcome);
        self
    }

    /// The outcome as the commit stores it: event IDs derived from the commit's events.
    pub fn stored_outcome(&self) -> Option<Outcome> {
        self.outcome.as_ref().map(|outcome| {
            outcome.stored(self.events.iter().map(|event| event.id.clone()).collect())
        })
    }

    /// Writes a snapshot with the events.
    #[must_use]
    pub fn with_snapshot(mut self, snapshot: NewSnapshot) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    /// Adds backend operations, written in the same transaction as the events.
    #[must_use]
    pub fn with_operations(mut self, operations: impl IntoIterator<Item = Op>) -> Self {
        self.operations.extend(operations);
        self
    }

    /// Adds view operations from commit-wide projectors, written in the same transaction after the
    /// backend operations.
    #[must_use]
    pub fn with_views(mut self, views: impl IntoIterator<Item = ProjectedView>) -> Self {
        self.views.extend(views);
        self
    }

    /// The budgeted sizes of the view operations by the backend-neutral rule
    /// ([`view_operation_size`]), for repositories that size them analytically.
    pub(crate) fn view_sizes(&self) -> impl Iterator<Item = usize> + '_ {
        self.views.iter().map(|view| view_operation_size(&view.op))
    }

    /// Exact backend-neutral event/snapshot/action representation, matching Dynamo item sizes,
    /// for the shared commit budget: the feature-marker and rebuild-lock checks, then the written
    /// stream's head update, event puts, and snapshot put (see [`stream_sizes`]), then the outcome
    /// put. Backend projector operations and view operations are opaque here and sized by the
    /// backend.
    pub(crate) fn sizes(
        &self,
        ttl: impl Fn(&EntityType) -> Option<i64>,
    ) -> Result<Vec<usize>, Error> {
        // The two control checks every commit emits, each sized by its two key attributes.
        let mut sizes = control_check_sizes();
        stream_sizes(self, &ttl, &mut sizes)?;
        // The outcome row comes last, and an outcome-only commit carries nothing else.
        if let Some(outcome) = self.stored_outcome() {
            sizes.push(item_size(&crate::wire::dynamo::outcome_item(&outcome)?));
        }

        Ok(sizes)
    }

    /// Checks that the events can be stored as written: each belongs to the commit's entity and
    /// has the ID `{entity_id}:{sequence}`, they share one entity type, the first sequence is at
    /// least 1, and sequences are contiguous. A snapshot must have the events' entity type and a
    /// sequence of at least 1. Repositories call it before writing, because the head update
    /// conditions only look at `entity_id` and the first and last events' sequences, loads reject
    /// streams with gaps, typed scans read each row's entity type, and DynamoDB does not store the
    /// event ID.
    ///
    /// A commit with an [outcome](Commit::with_outcome) is validated first: the command ID and the
    /// response and rejection bounds. A rejected outcome may carry events, under the rules of an
    /// accepted commit. A commit with no events and no operations is a valid no-op (or an
    /// outcome-only commit when it carries an outcome), and repositories ignore its snapshot.
    /// Operations and views without an event are refused with [`Error::ProjectorWithoutEvents`]:
    /// views are projections of committed events, so a projector operation with no backing event
    /// could never be rebuilt by replay (ADR-0012). The view operations are then checked with
    /// [`check_views`].
    pub fn validate(&self) -> Result<(), Error> {
        let invalid = |message: String| Err(Error::InvalidCommit(message));

        if let Some(outcome) = &self.outcome {
            check_outcome(outcome)?;
            crate::outcome::check_commit_text(
                &self.events,
                self.snapshot.iter().map(|snapshot| &snapshot.entity_type),
                [&self.entity_id],
            )?;
        }

        let Some(first) = self.events.first() else {
            if !self.operations.is_empty() || !self.views.is_empty() {
                return Err(Error::ProjectorWithoutEvents {
                    operations: self.operations.len() + self.views.len(),
                });
            }

            return Ok(());
        };

        if let Some(event) = self
            .events
            .iter()
            .find(|event| event.entity_type != first.entity_type)
        {
            return invalid(format!(
                "event {} has entity type {}, but the commit's first event has {}",
                event.id, event.entity_type, first.entity_type
            ));
        }

        if let Some(snapshot) = &self.snapshot
            && snapshot.entity_type != first.entity_type
        {
            return invalid(format!(
                "the snapshot has entity type {}, but the events have {}",
                snapshot.entity_type, first.entity_type
            ));
        }

        if let Some(snapshot) = &self.snapshot
            && snapshot.sequence < EventSequence(1)
        {
            return invalid(format!(
                "snapshot sequence {} is below 1; snapshot sequences start at 1",
                snapshot.sequence
            ));
        }

        for event in &self.events {
            if event.entity_id != self.entity_id {
                return invalid(format!(
                    "event {} belongs to entity {}, not {}",
                    event.id, event.entity_id, self.entity_id
                ));
            }

            let id = EventId::for_event(&event.entity_id, event.sequence);
            if event.id != id {
                return invalid(format!("event {} should have the ID {id}", event.id));
            }
        }

        if first.sequence < EventSequence(1) {
            return invalid(format!(
                "event sequence {} is below 1; sequences start at 1",
                first.sequence
            ));
        }

        if let Some(pair) = self
            .events
            .windows(2)
            .find(|pair| pair[0].sequence.checked_next() != Some(pair[1].sequence))
        {
            return invalid(format!(
                "event sequence {} follows {}; sequences must be contiguous",
                pair[1].sequence, pair[0].sequence
            ));
        }

        check_views(&self.views)
    }
}

/// Where a fenced head came from. Caller decisions must not be automatically re-prepared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FenceProvenance {
    /// Loaded by the transaction engine.
    Engine,
    /// Supplied by an external caller.
    Caller,
}

/// A stream whose observed head must not advance before the transaction commits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadFence {
    /// Stream ID.
    pub entity_id: EntityId,
    /// Aggregate type (used for durable retention checks).
    pub entity_type: EntityType,
    /// Observed head: the stream's derived head, the larger of its highest event sequence and its
    /// snapshot's event sequence. The fence passes while the stream's head item still equals it.
    pub head: EventSequence,
    /// Origin of the observation.
    pub provenance: FenceProvenance,
}

impl ReadFence {
    /// Constructs a fence.
    pub fn new(
        entity_id: impl Into<EntityId>,
        entity_type: impl Into<EntityType>,
        head: EventSequence,
        provenance: FenceProvenance,
    ) -> Self {
        Self {
            entity_id: entity_id.into(),
            entity_type: entity_type.into(),
            head,
            provenance,
        }
    }
}

/// One atomic transaction. Stream-local operations flatten in declaration order; streams and
/// fences normalize by stream ID. Empty stream commits retain the legacy no-op semantics.
#[derive(Debug)]
pub struct TransactionCommit<Op> {
    /// Stream appends, in projector declaration order.
    pub streams: Vec<Commit<Op>>,
    /// Read set, including any streams also written.
    pub fences: Vec<ReadFence>,
    /// Additional transaction operations, after stream-local operations.
    pub operations: Vec<Op>,
    /// Commit-wide view operations (v0.9, C7), after every backend operation.
    /// [`normalize`](Self::normalize) moves a stream's own views here, ahead of these, keeping
    /// declaration and emission order.
    pub views: Vec<ProjectedView>,
    /// The command outcome to record in the same transaction (C3), after the fence checks. With
    /// no events it is an outcome-only commit: a rejection or an accepted no-op, still fenced.
    pub outcome: Option<Outcome>,
}

impl<Op> TransactionCommit<Op> {
    /// Creates an envelope from stream commits.
    pub fn new(streams: Vec<Commit<Op>>) -> Self {
        Self {
            streams,
            fences: Vec::new(),
            operations: Vec::new(),
            views: Vec::new(),
            outcome: None,
        }
    }

    /// Records a command outcome in the same transaction, as the last write. The outcome's event
    /// IDs are derived from the commit's events and need not be set.
    #[must_use]
    pub fn with_outcome(mut self, outcome: Outcome) -> Self {
        self.outcome = Some(outcome);
        self
    }

    /// The outcome as the commit stores it: event IDs derived from the written streams, ordered
    /// by stream ID bytewise, then sequence. Call after [`normalize`](Self::normalize), which
    /// sorts the streams.
    pub fn stored_outcome(&self) -> Option<Outcome> {
        self.outcome.as_ref().map(|outcome| {
            outcome.stored(
                self.streams
                    .iter()
                    .flat_map(|stream| stream.events.iter().map(|event| event.id.clone()))
                    .collect(),
            )
        })
    }

    /// Adds read fences.
    #[must_use]
    pub fn with_fences(mut self, fences: impl IntoIterator<Item = ReadFence>) -> Self {
        self.fences.extend(fences);
        self
    }

    /// Adds projector operations.
    #[must_use]
    pub fn with_operations(mut self, operations: impl IntoIterator<Item = Op>) -> Self {
        self.operations.extend(operations);
        self
    }

    /// Adds commit-wide view operations, after the backend operations.
    #[must_use]
    pub fn with_views(mut self, views: impl IntoIterator<Item = ProjectedView>) -> Self {
        self.views.extend(views);
        self
    }

    /// The budgeted sizes of the view operations by the backend-neutral rule
    /// ([`view_operation_size`]), for repositories that size them analytically.
    pub(crate) fn view_sizes(&self) -> impl Iterator<Item = usize> + '_ {
        self.views.iter().map(|view| view_operation_size(&view.op))
    }

    /// Validates every stream and normalizes the pinned event/fence ordering.
    pub fn normalize(&mut self) -> Result<(), Error> {
        if let Some(outcome) = &self.outcome {
            check_outcome(outcome)?;
            crate::outcome::check_commit_text(
                self.streams.iter().flat_map(|stream| &stream.events),
                self.fences.iter().map(|fence| &fence.entity_type).chain(
                    self.streams
                        .iter()
                        .filter_map(|stream| stream.snapshot.as_ref())
                        .map(|snapshot| &snapshot.entity_type),
                ),
                self.fences
                    .iter()
                    .map(|fence| &fence.entity_id)
                    .chain(self.streams.iter().map(|stream| &stream.entity_id)),
            )?;
        }

        let mut ids = std::collections::BTreeSet::new();
        for stream in &self.streams {
            stream.validate()?;
            crate::features::check_not_reserved(&stream.entity_id)?;
            if !ids.insert(stream.entity_id.clone()) {
                return Err(Error::InvalidCommit(format!(
                    "duplicate stream {}",
                    stream.entity_id
                )));
            }
        }
        let mut fences = std::collections::BTreeSet::new();
        for fence in &self.fences {
            crate::features::check_not_reserved(&fence.entity_id)?;
            if fence.head.0 < 0
                || fence.head.checked_next().is_none()
                || !fences.insert(fence.entity_id.clone())
            {
                return Err(Error::InvalidCommit(format!(
                    "invalid or duplicate fence {}",
                    fence.entity_id
                )));
            }
        }
        // Snapshots belong to written streams: a snapshot must cover its commit's last event.
        for stream in &self.streams {
            if stream.snapshot.is_some() && stream.events.is_empty() {
                return Err(Error::InvalidCommit(format!(
                    "snapshot for {} must cover this commit's last event",
                    stream.entity_id
                )));
            }
        }
        // Views are derived state: projector operations need a backing event in the same
        // commit, or no replay could rebuild what they wrote (ADR-0012).
        let carried = self.operations.len()
            + self.views.len()
            + self
                .streams
                .iter()
                .map(|stream| stream.operations.len() + stream.views.len())
                .sum::<usize>();
        if carried > 0 && self.streams.iter().all(|stream| stream.events.is_empty()) {
            return Err(Error::ProjectorWithoutEvents {
                operations: carried,
            });
        }
        // Preserve declaration/emission order before sorting the write set.
        let mut operations = Vec::new();
        let mut views = Vec::new();
        for stream in &mut self.streams {
            if !stream.events.is_empty() {
                operations.append(&mut stream.operations);
                views.append(&mut stream.views);
            }
        }
        operations.append(&mut self.operations);
        views.append(&mut self.views);
        self.operations = operations;
        self.views = views;
        check_views(&self.views)?;
        self.streams.retain(|stream| !stream.events.is_empty());
        self.streams.sort_by(|a, b| {
            a.entity_id
                .as_str()
                .as_bytes()
                .cmp(b.entity_id.as_str().as_bytes())
        });
        self.fences.sort_by(|a, b| {
            a.entity_id
                .as_str()
                .as_bytes()
                .cmp(b.entity_id.as_str().as_bytes())
        });
        // A written stream's head update supersedes its fence.
        self.fences.retain(|fence| {
            !self
                .streams
                .iter()
                .any(|stream| stream.entity_id == fence.entity_id)
        });
        Ok(())
    }

    pub(crate) fn read_only_fences(&self) -> impl Iterator<Item = &ReadFence> {
        self.fences
            .iter()
            .filter(|f| !self.streams.iter().any(|s| s.entity_id == f.entity_id))
    }

    /// Refuses fences for entity types in the durable manifest's retention set or retained by the
    /// repository's own configuration (`locally_retained`): the union of the two.
    pub(crate) fn check_retention(
        &self,
        manifest: Option<&crate::FeatureManifest>,
        locally_retained: impl Fn(&EntityType) -> bool,
    ) -> Result<(), Error> {
        for fence in &self.fences {
            let durable =
                manifest.is_some_and(|m| m.retention.contains(fence.entity_type.as_str()));
            if durable || locally_retained(&fence.entity_type) {
                return Err(Error::RetentionConflict {
                    entity_type: fence.entity_type.clone(),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.streams.is_empty()
            && self.fences.is_empty()
            && self.operations.is_empty()
            && self.views.is_empty()
            && self.outcome.is_none()
    }

    /// Exact backend-neutral event/snapshot/action representation, matching Dynamo item sizes.
    ///
    /// ```text
    /// items = Σ over written streams (events + 1 head update + [snapshot put])
    ///       + fenced read-only streams × 1
    ///       + 1 outcome put, when the commit records one
    ///       + 1 feature-marker check
    ///       + 1 rebuild-lock check
    ///       + projector operations (sized by the backend)
    /// ```
    pub(crate) fn sizes(
        &self,
        ttl: impl Fn(&EntityType) -> Option<i64>,
    ) -> Result<Vec<usize>, Error> {
        use crate::wire::dynamo::{self as wire, HeadAction};

        // The two control checks every commit emits, each sized by its two key attributes.
        let mut sizes = control_check_sizes();
        for stream in &self.streams {
            stream_sizes(stream, &ttl, &mut sizes)?;
        }
        for fence in self.read_only_fences() {
            sizes.push(HeadAction::fence(&fence.entity_id, fence.head).size());
        }
        // The outcome row comes last, and an outcome-only commit carries nothing else.
        if let Some(outcome) = self.stored_outcome() {
            sizes.push(item_size(&wire::outcome_item(&outcome)?));
        }

        Ok(sizes)
    }
}

/// The budgeted sizes of the two control checks every commit carries, in transaction order: the
/// feature-marker check and the rebuild-lock check (C10), each sized by its key.
fn control_check_sizes() -> Vec<usize> {
    vec![marker_check_size(), crate::rebuild::lock_check_size()]
}

/// The feature-marker check's budgeted size: its key.
fn marker_check_size() -> usize {
    use crate::wire::dynamo::{AttributeValue as V, Item};

    item_size(&Item::from([
        ("pk".into(), V::S(crate::features::FEATURES_ID.into())),
        (
            "sk".into(),
            V::N(crate::features::FEATURES_SORT_KEY.to_string()),
        ),
    ]))
}

/// Appends the budgeted sizes of one written stream's group: the head update (budgeted as its
/// action is built: key, expression strings, name placeholders, and attribute values), the event
/// puts, and the snapshot put when the commit writes one. A stream with no events contributes
/// nothing. The head update's `:t` is the commit's own `ttl` for a stream with retention.
fn stream_sizes<Op>(
    stream: &Commit<Op>,
    ttl: &impl Fn(&EntityType) -> Option<i64>,
    sizes: &mut Vec<usize>,
) -> Result<(), Error> {
    use crate::wire::dynamo::{self as wire, HeadAction};

    let (Some(first), Some(last)) = (stream.events.first(), stream.events.last()) else {
        return Ok(());
    };

    sizes.push(
        HeadAction::update(
            &stream.entity_id,
            first.sequence,
            last.sequence,
            ttl(&first.entity_type),
        )
        .size(),
    );
    for event in &stream.events {
        sizes.push(item_size(&wire::event_item(
            event,
            ttl(&event.entity_type),
        )?));
    }
    if let Some(snapshot) = &stream.snapshot {
        sizes.push(item_size(&wire::snapshot_item(
            &SerializedSnapshot {
                entity_type: snapshot.entity_type.clone(),
                entity_id: stream.entity_id.clone(),
                sequence: snapshot.sequence,
                event_sequence: last.sequence,
                payload: snapshot.payload.clone(),
            },
            ttl(&snapshot.entity_type),
        )?));
    }

    Ok(())
}

pub(crate) fn check_budget(sizes: impl IntoIterator<Item = usize>) -> Result<(), Error> {
    let mut actions = 0usize;
    let mut bytes = 0usize;
    let mut largest_item = 0usize;
    for size in sizes {
        actions += 1;
        bytes = bytes.saturating_add(size);
        largest_item = largest_item.max(size);
    }
    if actions > 100 || bytes > 3_800_000 || largest_item > 380_000 {
        return Err(Error::CommitTooLarge {
            actions,
            bytes,
            largest_item,
        });
    }
    Ok(())
}

/// DynamoDB item size, including attribute names and recursively encoded values.
pub(crate) fn item_size(item: &crate::wire::dynamo::Item) -> usize {
    use crate::wire::dynamo::AttributeValue as V;
    fn value_size(value: &V) -> usize {
        match value {
            V::S(s) | V::N(s) => s.len(),
            V::B(b) => b.len(),
            V::Bool(_) | V::Null(_) => 1,
            V::M(m) => 3 + item_size(m) + m.len(),
            V::L(l) => 3 + l.iter().map(|v| 1 + value_size(v)).sum::<usize>(),
            V::Ss(s) | V::Ns(s) => s.iter().map(String::len).sum(),
            V::Bs(b) => b.iter().map(|v| v.len()).sum(),
        }
    }
    item.iter()
        .map(|(name, value)| name.len() + value_size(value))
        .sum()
}

/// An append-only event log with one optional snapshot per entity and one head per stream (ADR
/// 0007).
///
/// Implementations must:
///
/// - reject a commit whose events belong to another entity, have non-canonical IDs, mix entity
///   types, start below sequence 1, or are not contiguous, or whose snapshot has another entity
///   type or a sequence below 1, with [`Error::InvalidCommit`] (see [`Commit::validate`]);
/// - keep a head per stream, the highest event sequence ever committed to it, which a commit
///   raises to its last event and compaction never touches (H1 to H4);
/// - reject a commit whose first event does not follow the stored head (a stale writer, or an
///   event sequence that already exists) with [`Error::Conflict`], and one that skips past it with
///   [`Error::SequenceGap`], which is a conflict;
/// - report a stream whose head disagrees with its events or snapshot as
///   [`Error::StreamHeadDrift`], never retried, ranked above a gap and any other conflict;
/// - reject a snapshot whose sequence is not the stored snapshot's sequence plus one (or 1 when
///   there is no stored snapshot) with [`Error::Conflict`] (DynamoDB), or keep the stored snapshot's event sequence monotonic
///   (PostgreSQL, where the head conflict already covers racing writers);
/// - write events, head, snapshot, and operations atomically;
/// - refuse an out-of-band snapshot that the stream's head does not cover with
///   [`Error::StreamHeadDrift`] (a snapshot of event sequence 0 needs no head);
/// - refuse every write path that validates the feature marker (commits, `put_snapshot`,
///   `compact_below`) with [`Error::StreamHeadsNotMigrated`] when the table does not declare
///   `stream-heads`, after declaring it themselves on a table that holds no data;
/// - return events in ascending sequence order, and `latest` in descending order;
/// - refuse an entity ID in a reserved namespace (`SYSTEM#`, `command-outcome#`) on every
///   entity-addressed write path (commit, `put_snapshot`, `compact_below`) with
///   [`Error::ReservedEntityId`], before any request, and skip those partitions in `entity_heads`
///   (v0.9, C8). Explicit event and snapshot reads by ID retain legacy reserved-prefix streams
///   so migration tooling can read them;
/// - refuse every commit (outcome-only and rejection commits included) with
///   [`Error::RebuildInProgress`] while a rebuild holds the fence's lock row, an expired one
///   included, ranked below the feature-marker check and above every other failure; the lock
///   rows are in the reserved namespace, and snapshot writes and compaction do not check them
///   (v0.9, C10, see [`RebuildFence`](crate::RebuildFence)).
pub trait Repository: Send + Sync {
    /// Backend-specific operations that in-band projectors can add to a commit. Backends without
    /// projector support use [`std::convert::Infallible`].
    type Operation: Send + 'static;

    /// Appends events (and optionally a snapshot and projector operations) atomically.
    fn commit(
        &self,
        commit: Commit<Self::Operation>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Atomically appends all streams and validates read-only fences. Third-party repositories
    /// may opt in independently; the default never silently degrades to sequential commits.
    fn commit_transaction(
        &self,
        _commit: TransactionCommit<Self::Operation>,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        async {
            Err(Error::InvalidConfig(
                "repository does not support transactions".into(),
            ))
        }
    }

    /// Whether the repository writes the commit-wide view operations of [`Commit::views`] and
    /// [`TransactionCommit::views`] atomically with the events (or refuses them with
    /// [`Error::ViewsNotConfigured`] when it has nowhere to write them). The default is `false`: a
    /// repository written before v0.9 would silently drop them, so a [`Store`](crate::Store) or
    /// [`Transaction`](crate::Transaction) refuses to run a commit-wide projector over it with
    /// [`Error::ViewsUnsupported`]. The in-memory, DynamoDB, and PostgreSQL repositories return
    /// `true`. A wrapper must forward it.
    fn writes_views(&self) -> bool {
        false
    }

    /// Reports whether the repository applies a view put's engine-only expected-payload condition
    /// (the journal's counter compare-and-set): the row must exist, be the owner's, and hold
    /// exactly those bytes, or the commit fails with [`Error::ProjectorOperation`]. The default is
    /// `false`, and the journal refuses to commit through a repository that does not opt in
    /// ([`Error::ExpectedPayloadUnsupported`]), so a backend that ignored the condition cannot
    /// write an unconditional counter. The in-memory and DynamoDB repositories return `true`;
    /// PostgreSQL does not. A wrapper must forward it.
    fn applies_expected_payload(&self) -> bool {
        false
    }

    /// Durably adds the entity types to the feature manifest's `historyDependent` set, so
    /// compaction refuses them in every process; types already declared are left alone and nothing
    /// is written. The default refuses with [`Error::HistoryDeclarationUnsupported`]. The
    /// in-memory, DynamoDB, and PostgreSQL repositories implement it. A wrapper must forward it.
    fn declare_history_dependent(
        &self,
        types: &[EntityType],
    ) -> impl Future<Output = Result<(), Error>> + Send {
        let _ = types;

        async {
            Err(Error::HistoryDeclarationUnsupported {
                repository: std::any::type_name::<Self>().to_owned(),
            })
        }
    }

    /// Waits `duration` on the repository's runtime timer, between the polls of ambiguous-write
    /// recovery. The core is runtime-agnostic, so a backend that has a timer (the DynamoDB SDK's,
    /// Tokio's) overrides it; the default returns at once, which keeps in-memory and test
    /// repositories fast (the polls then run back to back).
    fn backoff(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        let _ = duration;

        async {}
    }

    /// Resolves after `duration` on the repository's runtime timer, to bound a read: ambiguous-write
    /// recovery races each outcome read against it, so a read that never resolves ends recovery at
    /// its window instead of hanging it. The default never resolves, so a backend with no timer
    /// must bound its own reads (the SDK and database clients here do, and override this).
    fn deadline(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        let _ = duration;

        std::future::pending()
    }

    /// Reads the command outcome row for a command ID, or `None` when none exists. The read must
    /// be strongly consistent regardless of the repository's read-consistency setting: an
    /// eventually consistent miss would re-execute a committed command. The default refuses with
    /// [`Error::InvalidConfig`], so a repository that has not declared outcome support cannot be
    /// used with a store that records them.
    ///
    /// A repository that supports outcomes also writes [`Commit::outcome`] and
    /// [`TransactionCommit::outcome`] as the commit transaction's last write, conditioned on the
    /// row not existing, and reports a lost condition as [`Error::OutcomeConflict`] (never a
    /// generic conflict). It counts the outcome item in the commit budget.
    ///
    /// **Contract for backend authors.** A commit that carries an outcome and whose result is
    /// genuinely unknown (a timeout, a lost connection or response, a dispatch failure, a server
    /// fault, a connection dropped during `COMMIT`) **must** fail with [`Error::Indeterminate`]:
    /// it is the only error that makes the store re-read the outcome row (ambiguous-write
    /// recovery), so reporting such a failure as [`Error::Backend`] would skip the recovery and
    /// leave a committed command reported as failed. The converse holds too: a definitive failure
    /// (validation, a missing table or relation, authorization, a constraint error other than the
    /// outcome-row conflict) **must** stay [`Error::Backend`] (or another definitive error),
    /// because recovering it would turn a request that can never succeed into a retryable "did not
    /// apply". The store polls the row for its recovery window (using [`backoff`](Self::backoff))
    /// and never concludes "not applied" from an absent row. Rows decoded from storage must pass
    /// [`Outcome::validate_stored`] (a malformed row is [`Error::InvalidRecord`]). The store's
    /// recovery behavior for both sides is pinned by [`conformance::outcome_cases`].
    fn outcome(
        &self,
        _command_id: &CommandId,
    ) -> impl Future<Output = Result<Option<Outcome>, Error>> + Send {
        async {
            Err(Error::InvalidConfig(
                "repository does not support command outcomes".into(),
            ))
        }
    }

    /// Returns every stored event for the entity, ascending.
    fn events(
        &self,
        entity_id: &EntityId,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, Error>> + Send;

    /// Returns the entity's events with a sequence greater than `after`, ascending.
    fn events_after(
        &self,
        entity_id: &EntityId,
        after: EventSequence,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, Error>> + Send;

    /// Returns the entity's last `n` events, newest first (descending sequence), excluding the
    /// snapshot item. At most `n` events: a backend may close a page early, so the read keeps
    /// paging until it has `n` events or the stream runs out. `n = 0` is a programmer error
    /// returned before any request, with [`Error::InvalidArgument`].
    fn latest(
        &self,
        entity_id: &EntityId,
        n: usize,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, Error>> + Send;

    /// Returns the entity's snapshot, if one exists.
    fn snapshot(
        &self,
        entity_id: &EntityId,
    ) -> impl Future<Output = Result<Option<SerializedSnapshot>, Error>> + Send;

    /// Writes a snapshot outside a commit (for background catch-up). The write never lowers the
    /// stored snapshot's event sequence: it replaces a snapshot whose event sequence is lower or
    /// equal and returns `true`, or leaves a newer snapshot in place and returns `false`.
    ///
    /// The stream's head must cover the snapshot (H3): a snapshot whose event sequence is above
    /// the stored head, or above 0 on a stream with no head, is [`Error::StreamHeadDrift`], not
    /// the `false` of a newer snapshot already in place. A stream with retention also refreshes
    /// the head's `ttl` to the snapshot's own in the same transaction.
    fn put_snapshot(
        &self,
        snapshot: &SerializedSnapshot,
    ) -> impl Future<Output = Result<bool, Error>> + Send;

    /// Deletes the entity's events with sequence in `[1, through]`, but only when the stored
    /// snapshot covers them (`snapshot.event_sequence >= through`). The snapshot itself is never
    /// deleted. Returns the number of events deleted. `through < 1` is a no-op.
    fn compact_below(
        &self,
        entity_id: &EntityId,
        through: EventSequence,
    ) -> impl Future<Output = Result<usize, Error>> + Send;

    /// Returns each entity's head: the larger of its highest event sequence and its snapshot's
    /// event sequence. Optionally narrowed to one entity type. This reads the whole log on some
    /// backends (a DynamoDB Scan; on PostgreSQL the heads table once the table declares
    /// `stream-heads`, and an aggregate over events and snapshots until then). Use it for
    /// rebuilds and diagnostics, not request paths.
    fn entity_heads(
        &self,
        entity_type: Option<&EntityType>,
    ) -> impl Future<Output = Result<BTreeMap<EntityId, EventSequence>, Error>> + Send;
}

macro_rules! forward_repository {
    ($($target:ty),+) => {$(
        impl<R: Repository + ?Sized> Repository for $target {
            type Operation = R::Operation;

            fn commit(&self, commit: Commit<Self::Operation>) -> impl Future<Output = Result<(), Error>> + Send {
                (**self).commit(commit)
            }

            fn commit_transaction(&self, commit: TransactionCommit<Self::Operation>) -> impl Future<Output = Result<(), Error>> + Send {
                (**self).commit_transaction(commit)
            }

            fn writes_views(&self) -> bool {
                (**self).writes_views()
            }

            fn applies_expected_payload(&self) -> bool {
                (**self).applies_expected_payload()
            }

            fn declare_history_dependent(
                &self,
                types: &[EntityType],
            ) -> impl Future<Output = Result<(), Error>> + Send {
                (**self).declare_history_dependent(types)
            }

            fn backoff(&self, duration: Duration) -> impl Future<Output = ()> + Send {
                (**self).backoff(duration)
            }

            fn deadline(&self, duration: Duration) -> impl Future<Output = ()> + Send {
                (**self).deadline(duration)
            }

            fn outcome(
                &self,
                command_id: &CommandId,
            ) -> impl Future<Output = Result<Option<Outcome>, Error>> + Send {
                (**self).outcome(command_id)
            }

            fn events(
                &self,
                entity_id: &EntityId,
            ) -> impl Future<Output = Result<Vec<SerializedEvent>, Error>> + Send {
                (**self).events(entity_id)
            }

            fn events_after(
                &self,
                entity_id: &EntityId,
                after: EventSequence,
            ) -> impl Future<Output = Result<Vec<SerializedEvent>, Error>> + Send {
                (**self).events_after(entity_id, after)
            }

            fn latest(
                &self,
                entity_id: &EntityId,
                n: usize,
            ) -> impl Future<Output = Result<Vec<SerializedEvent>, Error>> + Send {
                (**self).latest(entity_id, n)
            }

            fn snapshot(
                &self,
                entity_id: &EntityId,
            ) -> impl Future<Output = Result<Option<SerializedSnapshot>, Error>> + Send {
                (**self).snapshot(entity_id)
            }

            fn put_snapshot(
                &self,
                snapshot: &SerializedSnapshot,
            ) -> impl Future<Output = Result<bool, Error>> + Send {
                (**self).put_snapshot(snapshot)
            }

            fn compact_below(
                &self,
                entity_id: &EntityId,
                through: EventSequence,
            ) -> impl Future<Output = Result<usize, Error>> + Send {
                (**self).compact_below(entity_id, through)
            }

            fn entity_heads(
                &self,
                entity_type: Option<&EntityType>,
            ) -> impl Future<Output = Result<BTreeMap<EntityId, EventSequence>, Error>> + Send {
                (**self).entity_heads(entity_type)
            }
        }
    )+};
}

forward_repository!(&R, Arc<R>);

/// Decides whether a commit takes a snapshot, given the entity's current sequence, the number of
/// events in the commit, and the snapshot interval. Returns 0 for no snapshot, otherwise the
/// number of events folded into it (always the whole commit). An interval of 0 disables
/// snapshots.
///
/// Identical to Go's `evt.CalculateAdditionalEvents`: a commit snapshots when it reaches or
/// crosses the next multiple of the interval.
pub fn snapshot_events_due(current: EventSequence, events: usize, interval: usize) -> usize {
    if interval == 0 {
        return 0;
    }

    let next_snapshot_at = interval - (current.0.rem_euclid(interval as i64) as usize);
    if events < next_snapshot_at {
        return 0;
    }

    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::SerializedEvent;

    fn event(entity: &str, sequence: i64) -> SerializedEvent {
        SerializedEvent {
            id: format!("{entity}:{sequence}").into(),
            sequence: EventSequence(sequence),
            event_type: "t:created".into(),
            version: crate::EventVersion(1),
            entity_id: entity.into(),
            entity_type: "t".into(),
            payload: b"{}".to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn projector_operations_without_a_written_event_are_refused() {
        // Top-level operations and nothing written.
        let mut commit = TransactionCommit::<u8>::new(Vec::new());
        commit.operations = vec![1, 2];
        assert!(matches!(
            commit.normalize(),
            Err(Error::ProjectorWithoutEvents { operations: 2 })
        ));

        // Top-level operations beside a fence, still nothing written.
        let mut commit = TransactionCommit::<u8>::new(Vec::new());
        commit.operations = vec![1];
        commit.fences = vec![ReadFence::new(
            "a",
            "t",
            EventSequence(0),
            FenceProvenance::Engine,
        )];
        assert!(matches!(
            commit.normalize(),
            Err(Error::ProjectorWithoutEvents { operations: 1 })
        ));

        // Operations attached to a stream that wrote no event.
        let eventless = Commit::new("a", Vec::new()).with_operations([3u8]);
        let mut commit = TransactionCommit::new(vec![eventless]);
        assert!(matches!(
            commit.normalize(),
            Err(Error::ProjectorWithoutEvents { operations: 1 })
        ));
    }

    #[test]
    fn projector_operations_with_a_written_event_are_kept_in_order() {
        let written = Commit::new("a", vec![event("a", 1)]).with_operations([1u8]);
        let mut commit = TransactionCommit::new(vec![written]);
        commit.operations = vec![2];

        commit.normalize().unwrap();

        assert_eq!(commit.operations, vec![1, 2]);
    }

    #[test]
    fn a_single_stream_commit_with_operations_but_no_events_is_refused() {
        let commit = Commit::new("a", Vec::new()).with_operations([1u8, 2]);
        assert!(matches!(
            commit.validate(),
            Err(Error::ProjectorWithoutEvents { operations: 2 })
        ));

        // Without operations an eventless commit stays a no-op, snapshot or not.
        assert!(Commit::<u8>::new("a", Vec::new()).validate().is_ok());
        assert!(
            Commit::<u8>::new("a", Vec::new())
                .with_snapshot(NewSnapshot::new("t", EventSequence(1), "{}"))
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn the_budget_counts_only_emitted_actions() {
        // marker check + rebuild-lock check + head update + events: 97 events fill the 100
        // actions exactly, whether the commit creates the stream or appends to it. There is no
        // continuation or floor check.
        let fits = Commit::<u8>::new("a", (1..=97).map(|n| event("a", n)).collect());
        assert_eq!(fits.sizes(|_| None).unwrap().len(), 100);
        check_budget(fits.sizes(|_| None).unwrap()).unwrap();

        let over = Commit::<u8>::new("a", (1..=98).map(|n| event("a", n)).collect());
        assert!(matches!(
            check_budget(over.sizes(|_| None).unwrap()),
            Err(Error::CommitTooLarge { actions: 101, .. })
        ));

        // A snapshot put is one more action beside the head update, so 96 events fill the
        // budget with a snapshot, for a transaction over one stream as for a single commit.
        let snapshotted = Commit::<u8>::new("a", (1..=96).map(|n| event("a", n)).collect())
            .with_snapshot(NewSnapshot::new("t", EventSequence(1), "{}"));
        assert_eq!(snapshotted.sizes(|_| None).unwrap().len(), 100);
        let transaction = TransactionCommit::new(vec![snapshotted]);
        assert_eq!(transaction.sizes(|_| None).unwrap().len(), 100);

        let over = Commit::<u8>::new("a", (1..=97).map(|n| event("a", n)).collect())
            .with_snapshot(NewSnapshot::new("t", EventSequence(1), "{}"));
        assert_eq!(over.sizes(|_| None).unwrap().len(), 101);
    }

    #[test]
    fn appending_to_an_existing_stream_budgets_no_continuation_check() {
        // An append costs the same as a create: marker + lock + head update + 97 events fill 100.
        let fits = Commit::<u8>::new("a", (2..=98).map(|n| event("a", n)).collect());
        assert_eq!(fits.sizes(|_| None).unwrap().len(), 100);
        check_budget(fits.sizes(|_| None).unwrap()).unwrap();

        let over = Commit::<u8>::new("a", (2..=99).map(|n| event("a", n)).collect());
        assert!(matches!(
            check_budget(over.sizes(|_| None).unwrap()),
            Err(Error::CommitTooLarge { actions: 101, .. })
        ));

        // The transaction estimator agrees, per stream: a stream without a snapshot pays
        // exactly one action beyond its events.
        let transaction = TransactionCommit::new(vec![
            Commit::<u8>::new("a", (2..=3).map(|n| event("a", n)).collect()),
            Commit::<u8>::new("b", vec![event("b", 1)]),
        ]);
        // marker + lock + a: head + 2 events, b: head + 1 event
        assert_eq!(transaction.sizes(|_| None).unwrap().len(), 2 + 3 + 2);
    }

    /// The ADR's worked examples ("Action counts"), each with the two control checks (the
    /// feature-marker check and the rebuild-lock check) and the outcome put, as the shared
    /// estimator counts them.
    #[test]
    fn the_adr_action_counts_hold() {
        let append = |id: &str| Commit::<u8>::new(id, vec![event(id, 2)]);
        let fence = |id: &str| ReadFence::new(id, "t", EventSequence(3), FenceProvenance::Engine);
        let counted = |streams: Vec<Commit<u8>>, fences: Vec<ReadFence>| {
            let mut commit = TransactionCommit::new(streams)
                .with_fences(fences)
                .with_outcome(outcome("c"));
            commit.normalize().unwrap();

            commit.sizes(|_| None).unwrap().len()
        };
        let snapshot = || NewSnapshot::new("t", EventSequence(2), "{}");

        // create one stream, one event, no snapshot
        let created = Commit::<u8>::new("a", vec![event("a", 1)]).with_outcome(outcome("c"));
        assert_eq!(created.sizes(|_| None).unwrap().len(), 5);
        assert_eq!(
            counted(vec![Commit::new("a", vec![event("a", 1)])], vec![]),
            5
        );

        // append one event, no snapshot
        let appended = append("a").with_outcome(outcome("c"));
        assert_eq!(appended.sizes(|_| None).unwrap().len(), 5);
        assert_eq!(counted(vec![append("a")], vec![]), 5);

        // append one event that writes a snapshot
        assert_eq!(
            counted(vec![append("a").with_snapshot(snapshot())], vec![]),
            6
        );

        // create with five events and a snapshot
        let five = Commit::<u8>::new("a", (1..=5).map(|n| event("a", n)).collect())
            .with_snapshot(NewSnapshot::new("t", EventSequence(1), "{}"));
        assert_eq!(counted(vec![five], vec![]), 10);

        // ten single-event appends and five fenced reads
        let appends = |count: usize| {
            (0..count)
                .map(|n| append(&format!("w{n:02}")))
                .collect::<Vec<_>>()
        };
        let fences = |count: usize| {
            (0..count)
                .map(|n| fence(&format!("r{n:02}")))
                .collect::<Vec<_>>()
        };
        assert_eq!(counted(appends(10), fences(5)), 28);
        assert_eq!(counted(appends(12), fences(9)), 36);
        assert_eq!(counted(appends(1), fences(30)), 35);
    }

    #[test]
    fn an_event_id_that_is_not_canonical_is_refused_before_an_outcome_is_recorded() {
        let mut bad = event("a", 1);
        bad.id = "elsewhere:9".into();

        let outcome = crate::Outcome::accepted("cmd", "digest");

        let mut transaction =
            TransactionCommit::new(vec![Commit::<u8>::new("a", vec![bad.clone()])])
                .with_outcome(outcome.clone());
        assert!(matches!(
            transaction.normalize(),
            Err(Error::InvalidCommit(_))
        ));

        let single = Commit::<u8>::new("a", vec![bad]).with_outcome(outcome);
        assert!(matches!(single.validate(), Err(Error::InvalidCommit(_))));
    }

    #[test]
    fn an_empty_commit_is_still_a_no_op() {
        let mut commit = TransactionCommit::<u8>::new(Vec::new());

        commit.normalize().unwrap();

        assert!(commit.is_empty());
    }

    fn outcome(command_id: &str) -> Outcome {
        Outcome::accepted(command_id, "digest")
    }

    #[test]
    fn the_outcome_item_counts_against_the_commit_budget() {
        // marker + lock + head update + 96 events + outcome = 100 actions; 97 events are one too
        // many.
        let fits = Commit::<u8>::new("a", (1..=96).map(|n| event("a", n)).collect())
            .with_outcome(outcome("c"));
        assert_eq!(fits.sizes(|_| None).unwrap().len(), 100);
        check_budget(fits.sizes(|_| None).unwrap()).unwrap();

        let over = Commit::<u8>::new("a", (1..=97).map(|n| event("a", n)).collect())
            .with_outcome(outcome("c"));
        assert!(matches!(
            check_budget(over.sizes(|_| None).unwrap()),
            Err(Error::CommitTooLarge { actions: 101, .. })
        ));

        // Appending costs the same: there is no continuation check. 96 events fit, 97 do not.
        let appends = |last| {
            Commit::<u8>::new("a", (2..=last).map(|n| event("a", n)).collect())
                .with_outcome(outcome("c"))
        };
        assert_eq!(appends(97).sizes(|_| None).unwrap().len(), 100);
        assert_eq!(appends(98).sizes(|_| None).unwrap().len(), 101);

        // The same arithmetic for a transaction.
        let transaction = |last| {
            TransactionCommit::new(vec![Commit::<u8>::new(
                "a",
                (1..=last).map(|n| event("a", n)).collect(),
            )])
            .with_outcome(outcome("c"))
        };
        assert_eq!(transaction(96).sizes(|_| None).unwrap().len(), 100);
        assert_eq!(transaction(97).sizes(|_| None).unwrap().len(), 101);
    }

    #[test]
    fn an_outcome_only_commit_budgets_the_control_checks_and_the_outcome() {
        let single = Commit::<u8>::new("a", Vec::new()).with_outcome(outcome("c"));
        let sizes = single.sizes(|_| None).unwrap();
        assert_eq!(sizes.len(), 3, "no head update");
        // Each control check is sized by its key: pk + SYSTEM#FEATURES + sk + 0, and
        // pk + SYSTEM#REBUILD + sk + 0.
        assert_eq!(sizes[..2], [2 + 15 + 2 + 1, 2 + 14 + 2 + 1]);

        let transaction = TransactionCommit::<u8>::new(Vec::new()).with_outcome(outcome("c"));
        assert_eq!(transaction.sizes(|_| None).unwrap().len(), 3);

        // A fence is one check, budgeted between the control checks and the outcome.
        let mut fenced = TransactionCommit::<u8>::new(Vec::new()).with_outcome(outcome("c"));
        fenced.fences = vec![ReadFence::new(
            "f",
            "t",
            EventSequence(1),
            FenceProvenance::Engine,
        )];
        assert_eq!(fenced.sizes(|_| None).unwrap().len(), 4);

        // Without an outcome it is still a no-op.
        assert!(TransactionCommit::<u8>::new(Vec::new()).is_empty());
        assert!(!transaction.is_empty());
    }

    #[test]
    fn the_head_update_and_the_fence_are_budgeted_by_their_expressions() {
        use crate::wire::dynamo::HeadAction;

        // pk + "acct" + sk + "-1"
        let key = 2 + 4 + 2 + 2;
        let id = EntityId::new("acct");
        let at = EventSequence;

        // A create: "SET head = :last" + "attribute_not_exists(head)" + ":last" + "2"
        let create = HeadAction::update(&id, at(1), at(2), None);
        assert_eq!(
            create.size(),
            key + "SET head = :last".len() + "attribute_not_exists(head)".len() + ":last".len() + 1
        );

        // An append adds `:prev` and its condition.
        let append = HeadAction::update(&id, at(8), at(9), None);
        assert_eq!(
            append.size(),
            key + "SET head = :last".len()
                + "head = :prev".len()
                + ":last".len()
                + 1
                + ":prev".len()
                + 1
        );

        // A retained append adds the `ttl` name placeholder and `:t`.
        let retained = HeadAction::update(&id, at(8), at(9), Some(1_760_086_400));
        assert_eq!(
            retained.size(),
            key + "SET head = :last, #ttl = :t".len()
                + "head = :prev AND (attribute_not_exists(#ttl) OR #ttl <= :t)".len()
                + "#ttl".len()
                + "ttl".len()
                + ":last".len()
                + 1
                + ":prev".len()
                + 1
                + ":t".len()
                + 10
        );

        // A fence is its key, one expression, and (from head 1) one value.
        assert_eq!(
            HeadAction::fence(&id, at(0)).size(),
            key + "attribute_not_exists(head)".len()
        );
        assert_eq!(
            HeadAction::fence(&id, at(12)).size(),
            key + "head = :head".len() + ":head".len() + 2
        );

        // The estimator uses the commit's own ttl for `:t` and `:prev` only above sequence 1.
        let commit = Commit::<u8>::new("acct", vec![event("acct", 8), event("acct", 9)]);
        let sizes = commit.sizes(|_| Some(1_760_086_400)).unwrap();
        assert_eq!(sizes[2], retained.size());
    }

    #[test]
    fn the_outcome_event_ids_are_derived_in_canonical_order() {
        let mut commit = TransactionCommit::new(vec![
            Commit::<u8>::new("profile#u-1", vec![event("profile#u-1", 1)]),
            Commit::<u8>::new(
                "claim#ada",
                vec![event("claim#ada", 1), event("claim#ada", 2)],
            ),
        ])
        .with_outcome(outcome("c"));
        commit.normalize().unwrap();

        let ids: Vec<String> = commit
            .stored_outcome()
            .unwrap()
            .event_ids
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(ids, ["claim#ada:1", "claim#ada:2", "profile#u-1:1"]);

        // A caller-supplied list is replaced, and an empty response is no response.
        let mut supplied = outcome("c").with_response(Vec::new());
        supplied.event_ids = vec![EventId::new("bogus:9")];
        let single = Commit::<u8>::new("a", vec![event("a", 1)]).with_outcome(supplied);
        let stored = single.stored_outcome().unwrap();
        assert_eq!(stored.event_ids, vec![EventId::new("a:1")]);
        assert_eq!(stored.response, None);
    }

    #[test]
    fn commits_validate_their_outcome_before_anything_else() {
        let rejected = Outcome::rejected("c", "digest", crate::outcome::Rejection::new("x", "y"));

        // A rejection may carry events, in either commit shape, and lists them.
        let with_events =
            Commit::<u8>::new("a", vec![event("a", 1)]).with_outcome(rejected.clone());
        with_events.validate().unwrap();
        assert_eq!(
            with_events.stored_outcome().unwrap().event_ids,
            vec![EventId::new("a:1")]
        );
        let mut transaction =
            TransactionCommit::new(vec![Commit::<u8>::new("a", vec![event("a", 1)])])
                .with_outcome(rejected.clone());
        transaction.normalize().unwrap();
        assert_eq!(
            transaction.stored_outcome().unwrap().event_ids,
            vec![EventId::new("a:1")]
        );

        // Operations still need an event behind them, rejected or not.
        let mut operations = TransactionCommit::<u8>::new(Vec::new())
            .with_operations([1])
            .with_outcome(rejected.clone());
        assert!(matches!(
            operations.normalize(),
            Err(Error::ProjectorWithoutEvents { operations: 1 })
        ));

        // Without events it is a valid outcome-only commit.
        Commit::<u8>::new("a", Vec::new())
            .with_outcome(rejected)
            .validate()
            .unwrap();

        // The command ID is checked first, before the response bound or the events.
        let mut bad = Outcome::accepted("", "digest");
        bad.response = Some(vec![0; crate::MAX_OUTCOME_BYTES + 1]);
        let commit = Commit::<u8>::new("a", vec![event("b", 1)]).with_outcome(bad);
        assert!(matches!(commit.validate(), Err(Error::InvalidArgument(_))));

        let oversized = outcome("c").with_response(vec![0; crate::MAX_OUTCOME_BYTES + 1]);
        assert!(matches!(
            Commit::<u8>::new("a", Vec::new())
                .with_outcome(oversized)
                .validate(),
            Err(Error::OutcomeTooLarge {
                what: "response",
                ..
            })
        ));
    }
    fn view(projector: &str, operation: usize, sk: &str) -> ProjectedView {
        ProjectedView::new(
            projector,
            0,
            operation,
            crate::ViewOperation::put("p", sk, "o", "", "{}"),
        )
    }

    #[test]
    fn normalize_moves_stream_views_to_the_commit_in_stream_order_then_commit_level() {
        let mut commit = TransactionCommit::<u8>::new(vec![
            Commit::new("b", vec![event("b", 1)])
                .with_views([view("one", 0, "b0"), view("one", 1, "b1")]),
            Commit::new("a", vec![event("a", 1)]).with_views([view("two", 0, "a0")]),
        ])
        .with_views([view("three", 0, "c0")]);

        commit.normalize().unwrap();

        // Streams were given as b then a; their views keep that order, ahead of the commit's own.
        let order: Vec<_> = commit
            .views
            .iter()
            .map(|view| view.op.sk.as_str())
            .collect();
        assert_eq!(order, ["b0", "b1", "a0", "c0"]);
        assert!(commit.streams.iter().all(|stream| stream.views.is_empty()));
        assert_eq!(
            commit
                .streams
                .iter()
                .map(|s| s.entity_id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"],
            "the streams themselves are sorted by ID bytes"
        );
        assert!(!commit.is_empty());

        // Normalizing again changes nothing.
        commit.normalize().unwrap();
        assert_eq!(commit.views.len(), 4);
    }

    #[test]
    fn views_count_in_the_projector_only_refusal_and_in_emptiness() {
        let mut commit = TransactionCommit::<u8>::new(Vec::new()).with_views([view("p", 0, "s")]);
        assert!(!commit.is_empty());
        assert!(matches!(
            commit.normalize(),
            Err(Error::ProjectorWithoutEvents { operations: 1 })
        ));

        // Legacy operations and views add up.
        let mut commit = TransactionCommit::<u8>::new(vec![
            Commit::new("a", Vec::new())
                .with_operations([1, 2])
                .with_views([view("p", 0, "s")]),
        ]);
        assert!(matches!(
            commit.normalize(),
            Err(Error::ProjectorWithoutEvents { operations: 3 })
        ));

        let single = Commit::<u8>::new("a", Vec::new())
            .with_operations([1])
            .with_views([view("p", 0, "s")]);
        assert!(matches!(
            single.validate(),
            Err(Error::ProjectorWithoutEvents { operations: 2 })
        ));
    }

    #[test]
    fn normalize_and_validate_check_the_views() {
        let repeated = [view("p", 0, "s"), view("q", 0, "s")];

        let mut commit = TransactionCommit::<u8>::new(vec![
            Commit::new("a", vec![event("a", 1)]).with_views([repeated[0].clone()]),
            Commit::new("b", vec![event("b", 1)]).with_views([repeated[1].clone()]),
        ]);
        assert!(
            matches!(commit.normalize(), Err(Error::DuplicateViewKey { .. })),
            "views of different streams are checked together"
        );

        let single = Commit::<u8>::new("a", vec![event("a", 1)]).with_views(repeated);
        assert!(matches!(
            single.validate(),
            Err(Error::DuplicateViewKey { .. })
        ));

        let invalid = Commit::<u8>::new("a", vec![event("a", 1)]).with_views([ProjectedView::new(
            "p",
            0,
            0,
            crate::ViewOperation::put("", "s", "o", "", "{}"),
        )]);
        assert!(matches!(
            invalid.validate(),
            Err(Error::InvalidViewOperation(_))
        ));
    }
}
