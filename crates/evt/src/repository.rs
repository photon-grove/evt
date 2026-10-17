//! The storage contract every backend implements.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::error::Error;
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::ids::{CommandId, EntityId, EntityType, EventId, EventSequence};
use crate::outcome::{Outcome, check_outcome};

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
    /// transaction. DynamoDB puts them before the events. PostgreSQL runs them after the event
    /// inserts and the floor check, so a racing writer fails on the event key first.
    pub operations: Vec<Op>,
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

    /// Exact backend-neutral event/snapshot/check representation, matching Dynamo item sizes,
    /// for the shared commit budget: the feature-marker check, the event puts, then the snapshot put
    /// or floor check. Backend projector operations are opaque here and sized by the backend.
    pub(crate) fn sizes(
        &self,
        ttl: impl Fn(&EntityType) -> Option<i64>,
    ) -> Result<Vec<usize>, Error> {
        use crate::wire::dynamo::{self as wire, AttributeValue as V};
        let key = |id: &str, seq: i64| {
            wire::Item::from([
                ("pk".into(), V::S(id.into())),
                ("sk".into(), V::N(seq.to_string())),
            ])
        };
        // Only actions a commit actually emits are budgeted: the feature-marker check today. The
        // rebuild-lock check (C10) joins this list in the change that starts emitting it.
        let mut sizes = vec![item_size(&key(crate::features::FEATURES_ID, 0))];
        for event in &self.events {
            sizes.push(item_size(&wire::event_item(
                event,
                ttl(&event.entity_type),
            )?));
        }
        // A stream continuing from an existing head carries a continuation check on the event
        // just before its first. It is budgeted for every such stream, also on the compacted-head
        // path that pins the snapshot instead, so CommitTooLarge fires identically everywhere.
        if let Some(first) = self
            .events
            .first()
            .filter(|event| event.sequence > EventSequence(1))
        {
            sizes.push(item_size(&key(
                self.entity_id.as_str(),
                first.sequence.0 - 1,
            )));
        }
        if let Some(snapshot) = &self.snapshot
            && let Some(last) = self.events.last()
        {
            sizes.push(item_size(&wire::snapshot_item(
                &SerializedSnapshot {
                    entity_type: snapshot.entity_type.clone(),
                    entity_id: self.entity_id.clone(),
                    sequence: snapshot.sequence,
                    event_sequence: last.sequence,
                    payload: snapshot.payload.clone(),
                },
                ttl(&snapshot.entity_type),
            )?));
        } else if !self.events.is_empty() {
            sizes.push(item_size(&key(self.entity_id.as_str(), 0)));
        }
        // The outcome row comes last, and an outcome-only commit carries nothing else.
        if let Some(outcome) = self.stored_outcome() {
            sizes.push(item_size(&wire::outcome_item(&outcome)?));
        }
        Ok(sizes)
    }

    /// Checks that the events can be stored as written: each belongs to the commit's entity and
    /// has the ID `{entity_id}:{sequence}`, they share one entity type, the first sequence is at
    /// least 1, and sequences are contiguous. A snapshot must have the events' entity type and a
    /// sequence of at least 1. Repositories call it before writing, because the floor and snapshot
    /// checks only look at `entity_id` and the first event's sequence, loads reject streams with
    /// gaps, typed scans read each row's entity type, and DynamoDB does not store the event ID.
    ///
    /// A commit with an [outcome](Commit::with_outcome) is validated first: the command ID, the
    /// response and rejection bounds, and a rejected outcome with events is invalid. A commit
    /// with no events and no operations is a valid no-op (or an outcome-only commit when it
    /// carries an outcome), and repositories ignore its snapshot. Operations without an event are refused with
    /// [`Error::ProjectorWithoutEvents`]: views are projections of committed events, so a
    /// projector operation with no backing event could never be rebuilt by replay (ADR-0012).
    pub fn validate(&self) -> Result<(), Error> {
        let invalid = |message: String| Err(Error::InvalidCommit(message));

        if let Some(outcome) = &self.outcome {
            check_outcome(outcome)?;
            crate::outcome::check_commit_text(
                &self.events,
                self.snapshot.iter().map(|snapshot| &snapshot.entity_type),
                [&self.entity_id],
            )?;

            if outcome.is_rejected() && !self.events.is_empty() {
                return invalid(format!(
                    "a rejected outcome for command {} cannot carry events",
                    outcome.command_id
                ));
            }
        }

        let Some(first) = self.events.first() else {
            if !self.operations.is_empty() {
                return Err(Error::ProjectorWithoutEvents {
                    operations: self.operations.len(),
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

        Ok(())
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
    /// Observed head, including the snapshot floor.
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

/// One atomic transaction. Stream-local operations flatten in declaration order; events and
/// floors normalize by stream ID. Empty stream commits retain the legacy no-op semantics.
#[derive(Debug)]
pub struct TransactionCommit<Op> {
    /// Stream appends, in projector declaration order.
    pub streams: Vec<Commit<Op>>,
    /// Read set, including any streams also written.
    pub fences: Vec<ReadFence>,
    /// Additional transaction operations, after stream-local operations.
    pub operations: Vec<Op>,
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
            + self
                .streams
                .iter()
                .map(|stream| stream.operations.len())
                .sum::<usize>();
        if carried > 0 && self.streams.iter().all(|stream| stream.events.is_empty()) {
            return Err(Error::ProjectorWithoutEvents {
                operations: carried,
            });
        }
        // Preserve declaration/emission order before sorting the write set.
        let mut operations = Vec::new();
        for stream in &mut self.streams {
            if !stream.events.is_empty() {
                operations.append(&mut stream.operations);
            }
        }
        operations.append(&mut self.operations);
        self.operations = operations;
        self.streams.retain(|stream| !stream.events.is_empty());
        if let Some(outcome) = &self.outcome
            && outcome.is_rejected()
            && !self.streams.is_empty()
        {
            return Err(Error::InvalidCommit(format!(
                "a rejected outcome for command {} cannot carry events",
                outcome.command_id
            )));
        }
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
        // A written stream's event put at head + 1 supersedes its fence.
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
            && self.outcome.is_none()
    }

    /// Exact backend-neutral event/snapshot/check representation, matching Dynamo item sizes.
    pub(crate) fn sizes(
        &self,
        ttl: impl Fn(&EntityType) -> Option<i64>,
    ) -> Result<Vec<usize>, Error> {
        use crate::wire::dynamo::{self as wire, AttributeValue as V};
        let key = |id: &str, seq: i64| {
            wire::Item::from([
                ("pk".into(), V::S(id.into())),
                ("sk".into(), V::N(seq.to_string())),
            ])
        };
        // Only actions a commit actually emits are budgeted: the feature-marker check today. The
        // rebuild-lock check (C10) joins this list in the change that starts emitting it.
        let mut sizes = vec![item_size(&key(crate::features::FEATURES_ID, 0))];
        for stream in &self.streams {
            for event in &stream.events {
                sizes.push(item_size(&wire::event_item(
                    event,
                    ttl(&event.entity_type),
                )?));
            }
            // The continuation check of a stream continuing from an existing head (see
            // `Commit::sizes`).
            if let Some(first) = stream
                .events
                .first()
                .filter(|event| event.sequence > EventSequence(1))
            {
                sizes.push(item_size(&key(
                    stream.entity_id.as_str(),
                    first.sequence.0 - 1,
                )));
            }
        }
        for stream in &self.streams {
            if let Some(snapshot) = &stream.snapshot {
                sizes.push(item_size(&wire::snapshot_item(
                    &SerializedSnapshot {
                        entity_type: snapshot.entity_type.clone(),
                        entity_id: stream.entity_id.clone(),
                        sequence: snapshot.sequence,
                        event_sequence: stream.events.last().expect("normalized stream").sequence,
                        payload: snapshot.payload.clone(),
                    },
                    ttl(&snapshot.entity_type),
                )?));
            } else {
                sizes.push(item_size(&key(stream.entity_id.as_str(), 0)));
            }
        }
        for fence in self.read_only_fences() {
            sizes.push(item_size(&key(
                fence.entity_id.as_str(),
                fence.head.next().0,
            )));
            sizes.push(item_size(&key(fence.entity_id.as_str(), 0)));
        }
        // The outcome row comes last, and an outcome-only commit carries nothing else.
        if let Some(outcome) = self.stored_outcome() {
            sizes.push(item_size(&wire::outcome_item(&outcome)?));
        }
        Ok(sizes)
    }
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

/// An append-only event log with one optional snapshot per entity.
///
/// Implementations must:
///
/// - reject a commit whose events belong to another entity, have non-canonical IDs, mix entity
///   types, start below sequence 1, or are not contiguous, or whose snapshot has another entity
///   type or a sequence below 1, with [`Error::InvalidCommit`] (see [`Commit::validate`]);
/// - reject a commit that reuses an existing `(entity_id, sequence)` with [`Error::Conflict`];
/// - reject a commit whose first event is at or below the stored snapshot's event sequence with
///   [`Error::Conflict`], because compaction may have deleted the events that sequence would
///   collide with;
/// - reject a snapshot whose sequence is not the stored snapshot's sequence plus one (or 1 when
///   there is no stored snapshot) with [`Error::Conflict`] (DynamoDB), or keep the stored snapshot's event sequence monotonic
///   (PostgreSQL, where the event-sequence conflict already covers racing writers);
/// - write events, snapshot, and operations atomically;
/// - return events in ascending sequence order, and `latest` in descending order;
/// - refuse an entity ID in a reserved namespace (`SYSTEM#`, `command-outcome#`) on every
///   entity-addressed write path (commit, `put_snapshot`, `compact_below`) with
///   [`Error::ReservedEntityId`], before any request, and skip those partitions in `entity_heads`
///   (v0.9, C8). Explicit event and snapshot reads by ID retain legacy reserved-prefix streams
///   so migration tooling can read them.
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
    /// backends. Use it for rebuilds and diagnostics, not request paths.
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
        // marker check + events + floor check: 98 events fill the 100 actions exactly.
        let fits = Commit::<u8>::new("a", (1..=98).map(|n| event("a", n)).collect());
        assert_eq!(fits.sizes(|_| None).unwrap().len(), 100);
        check_budget(fits.sizes(|_| None).unwrap()).unwrap();

        let over = Commit::<u8>::new("a", (1..=99).map(|n| event("a", n)).collect());
        assert!(matches!(
            check_budget(over.sizes(|_| None).unwrap()),
            Err(Error::CommitTooLarge { actions: 101, .. })
        ));

        // The same holds for a transaction over one stream, and a snapshot put replaces the floor.
        let snapshotted = Commit::<u8>::new("a", (1..=98).map(|n| event("a", n)).collect())
            .with_snapshot(NewSnapshot::new("t", EventSequence(1), "{}"));
        let transaction = TransactionCommit::new(vec![snapshotted]);
        assert_eq!(transaction.sizes(|_| None).unwrap().len(), 100);
    }

    #[test]
    fn appending_to_an_existing_stream_budgets_a_continuation_check() {
        // marker + continuation check + 97 events + floor check: 97 appended events fill 100.
        let fits = Commit::<u8>::new("a", (2..=98).map(|n| event("a", n)).collect());
        assert_eq!(fits.sizes(|_| None).unwrap().len(), 100);
        check_budget(fits.sizes(|_| None).unwrap()).unwrap();

        let over = Commit::<u8>::new("a", (2..=99).map(|n| event("a", n)).collect());
        assert!(matches!(
            check_budget(over.sizes(|_| None).unwrap()),
            Err(Error::CommitTooLarge { actions: 101, .. })
        ));

        // The transaction estimator agrees, per stream.
        let transaction = TransactionCommit::new(vec![
            Commit::<u8>::new("a", (2..=3).map(|n| event("a", n)).collect()),
            Commit::<u8>::new("b", vec![event("b", 1)]),
        ]);
        // marker + a: check + 2 events + floor, b: 1 event + floor
        assert_eq!(transaction.sizes(|_| None).unwrap().len(), 1 + 4 + 2);
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
        // marker + 97 events + floor + outcome = 100 actions; 98 events are one too many.
        let fits = Commit::<u8>::new("a", (1..=97).map(|n| event("a", n)).collect())
            .with_outcome(outcome("c"));
        assert_eq!(fits.sizes(|_| None).unwrap().len(), 100);
        check_budget(fits.sizes(|_| None).unwrap()).unwrap();

        let over = Commit::<u8>::new("a", (1..=98).map(|n| event("a", n)).collect())
            .with_outcome(outcome("c"));
        assert!(matches!(
            check_budget(over.sizes(|_| None).unwrap()),
            Err(Error::CommitTooLarge { actions: 101, .. })
        ));

        // Appending adds the continuation check: 96 events fit, 97 do not.
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
        assert_eq!(transaction(97).sizes(|_| None).unwrap().len(), 100);
        assert_eq!(transaction(98).sizes(|_| None).unwrap().len(), 101);
    }

    #[test]
    fn an_outcome_only_commit_budgets_the_marker_check_and_the_outcome() {
        let single = Commit::<u8>::new("a", Vec::new()).with_outcome(outcome("c"));
        assert_eq!(single.sizes(|_| None).unwrap().len(), 2, "no floor check");

        let transaction = TransactionCommit::<u8>::new(Vec::new()).with_outcome(outcome("c"));
        assert_eq!(transaction.sizes(|_| None).unwrap().len(), 2);

        // Fences are budgeted between the marker and the outcome.
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

        // A rejection cannot carry events, in either commit shape.
        let with_events =
            Commit::<u8>::new("a", vec![event("a", 1)]).with_outcome(rejected.clone());
        assert!(matches!(
            with_events.validate(),
            Err(Error::InvalidCommit(_))
        ));
        let mut transaction =
            TransactionCommit::new(vec![Commit::<u8>::new("a", vec![event("a", 1)])])
                .with_outcome(rejected.clone());
        assert!(matches!(
            transaction.normalize(),
            Err(Error::InvalidCommit(_))
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
}
