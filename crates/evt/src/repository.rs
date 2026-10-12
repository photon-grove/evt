//! The storage contract every backend implements.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use crate::error::Error;
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::ids::{EntityId, EntityType, EventSequence};

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
/// no-op: repositories ignore its snapshot and operations.
#[derive(Debug)]
#[non_exhaustive]
pub struct Commit<Op> {
    /// The entity whose stream is appended to.
    pub entity_id: EntityId,
    /// The new events, in ascending sequence order.
    pub events: Vec<SerializedEvent>,
    /// The snapshot to write, if the commit crosses a snapshot boundary.
    pub snapshot: Option<NewSnapshot>,
    /// Extra backend operations from in-band projectors. Written first, in order.
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

    /// Adds backend operations, written before the events in the same transaction.
    #[must_use]
    pub fn with_operations(mut self, operations: impl IntoIterator<Item = Op>) -> Self {
        self.operations.extend(operations);
        self
    }

    /// Checks that every event belongs to the commit's entity. Repositories call it before writing,
    /// because the floor and snapshot checks only look at `entity_id`.
    pub fn validate(&self) -> Result<(), Error> {
        match self
            .events
            .iter()
            .find(|event| event.entity_id != self.entity_id)
        {
            Some(event) => Err(Error::InvalidCommit(format!(
                "event {} belongs to entity {}, not {}",
                event.id, event.entity_id, self.entity_id
            ))),
            None => Ok(()),
        }
    }
}

/// An append-only event log with one optional snapshot per entity.
///
/// Implementations must:
///
/// - reject a commit with an event for another entity with [`Error::InvalidCommit`] (see
///   [`Commit::validate`]);
/// - reject a commit that reuses an existing `(entity_id, sequence)` with [`Error::Conflict`];
/// - reject a commit whose first event is at or below the stored snapshot's event sequence with
///   [`Error::Conflict`], because compaction may have deleted the events that sequence would
///   collide with;
/// - reject a snapshot whose sequence is not the stored snapshot's sequence plus one (or 1 when
///   there is no stored snapshot) with [`Error::Conflict`] (DynamoDB), or keep the stored snapshot's event sequence monotonic
///   (PostgreSQL, where the event-sequence conflict already covers racing writers);
/// - write events, snapshot, and operations atomically;
/// - return events in ascending sequence order.
pub trait Repository: Send + Sync {
    /// Backend-specific operations that in-band projectors can add to a commit. Backends without
    /// projector support use [`std::convert::Infallible`].
    type Operation: Send + 'static;

    /// Appends events (and optionally a snapshot and projector operations) atomically.
    fn commit(
        &self,
        commit: Commit<Self::Operation>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

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
