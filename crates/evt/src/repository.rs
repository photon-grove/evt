//! The storage contract every backend implements.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use crate::error::Error;
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::ids::{EntityId, EntityType, EventId, EventSequence};

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

/// Everything a repository writes atomically for one command. A commit with no events is a
/// no-op: repositories ignore its snapshot. Operations without an event are refused.
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
}

impl<Op> Commit<Op> {
    /// Creates a commit of events with no snapshot and no operations.
    pub fn new(entity_id: impl Into<EntityId>, events: Vec<SerializedEvent>) -> Self {
        Self {
            entity_id: entity_id.into(),
            events,
            snapshot: None,
            operations: Vec::new(),
        }
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
        if let Some(snapshot) = &self.snapshot {
            sizes.push(item_size(&wire::snapshot_item(
                &SerializedSnapshot {
                    entity_type: snapshot.entity_type.clone(),
                    entity_id: self.entity_id.clone(),
                    sequence: snapshot.sequence,
                    event_sequence: self
                        .events
                        .last()
                        .expect("validated commit with a snapshot has events")
                        .sequence,
                    payload: snapshot.payload.clone(),
                },
                ttl(&snapshot.entity_type),
            )?));
        } else {
            sizes.push(item_size(&key(self.entity_id.as_str(), 0)));
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
    /// A commit with no events and no operations is a valid no-op, and repositories ignore its
    /// snapshot. Operations without an event are refused with
    /// [`Error::ProjectorWithoutEvents`]: views are projections of committed events, so a
    /// projector operation with no backing event could never be rebuilt by replay (ADR-0012).
    pub fn validate(&self) -> Result<(), Error> {
        let invalid = |message: String| Err(Error::InvalidCommit(message));

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
}

impl<Op> TransactionCommit<Op> {
    /// Creates an envelope from stream commits.
    pub fn new(streams: Vec<Commit<Op>>) -> Self {
        Self {
            streams,
            fences: Vec::new(),
            operations: Vec::new(),
        }
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

    pub(crate) fn check_retention(
        &self,
        manifest: Option<&crate::FeatureManifest>,
    ) -> Result<(), Error> {
        for fence in &self.fences {
            if manifest.is_some_and(|m| m.retention.contains(fence.entity_type.as_str())) {
                return Err(Error::RetentionConflict {
                    entity_type: fence.entity_type.clone(),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.streams.is_empty() && self.fences.is_empty() && self.operations.is_empty()
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
    fn an_empty_commit_is_still_a_no_op() {
        let mut commit = TransactionCommit::<u8>::new(Vec::new());

        commit.normalize().unwrap();

        assert!(commit.is_empty());
    }
}
