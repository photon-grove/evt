//! Rebuilding the journal from stored events.
//!
//! Every journal row and counter is a function of stored events alone, because an event's
//! allocations are in its metadata. Nothing here needs the order the commits happened in, or any
//! commit grouping: feed the events in any order, from any number of streams, and the same rows
//! and counters come out.

use std::collections::BTreeMap;
use std::time::Duration;

use super::projector::PROJECTOR_NAME;
use super::row::{counter_view, row_view};
use super::scope::{Scope, allocations};
use crate::error::Error;
use crate::event::SerializedEvent;
use crate::projection::{ProjectedView, ViewOperation, ViewRow};
use crate::rebuild::{RebuildFence, RebuildLease, rebuild_batches};

/// The journal rows and counters a set of stored events imply.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JournalState {
    /// Every journal row, by scope and position.
    pub rows: BTreeMap<(Scope, u64), ViewRow>,
    /// Every counter: the highest position any event holds in the scope.
    pub counters: BTreeMap<Scope, u64>,
}

impl JournalState {
    /// The view rows to write: the journal rows ascending by scope and position, then the
    /// counters ascending by scope.
    pub fn views(&self) -> Vec<ViewRow> {
        let mut views: Vec<ViewRow> = self.rows.values().cloned().collect();
        views.extend(
            self.counters
                .iter()
                .map(|(scope, position)| counter_view(scope, *position)),
        );

        views
    }
}

/// Derives the journal's rows and counters from stored events, in any order.
///
/// Each event contributes one row per allocation in its metadata and raises the counter of the
/// scope to its position if higher. An event with no allocation contributes nothing. Two
/// *different* records holding one position of a scope can never come from this journal (the
/// position is claimed once, in a transaction) and fail with [`Error::InvalidJournal`], whatever
/// order they are given in; only an identical record given twice is the same row.
pub fn replay<'a>(
    events: impl IntoIterator<Item = &'a SerializedEvent>,
) -> Result<JournalState, Error> {
    let mut state = JournalState::default();
    let mut claimed: BTreeMap<(Scope, u64), &SerializedEvent> = BTreeMap::new();

    for event in events {
        for allocation in allocations(&event.metadata)? {
            let scope = allocation.scope()?;
            let key = (scope.clone(), allocation.position);

            if let Some(holder) = claimed.get(&key) {
                // The same stored event given twice is one row. Anything else claiming the
                // position, even a record with the same ID and allocation but another payload or
                // metadata, is a different event, and keeping the first would make the result
                // depend on the order the events were given in.
                if *holder != event {
                    return Err(Error::InvalidJournal(format!(
                        "position {} of scope {scope} is held by two different records, {} and {}",
                        allocation.position, holder.id, event.id
                    )));
                }

                continue;
            }

            state
                .rows
                .insert(key.clone(), row_view(&scope, allocation.position, event)?);
            claimed.insert(key, event);

            let counter = state.counters.entry(scope).or_insert(0);
            *counter = (*counter).max(allocation.position);
        }
    }

    Ok(state)
}

/// The owner token [`rebuild_with_lock`] takes the rebuild lock under.
pub const REBUILD_OWNER: &str = "journal-rebuild";

/// Rebuilds the journal under a rebuild lease (C10): derives the state from `events` and writes
/// every row and counter through [`RebuildFence::write_rebuild_views`], in batches of at most
/// [`MAX_REBUILD_VIEW_BATCH`](crate::MAX_REBUILD_VIEW_BATCH) operations that fit the commit byte budget.
///
/// `lease` is the one [`begin_rebuild`](RebuildFence::begin_rebuild) returned; the caller renews and
/// ends it. While it is held every live commit to the table fails with
/// [`Error::RebuildInProgress`], so the unconditional rewrite of rows and counters cannot race a
/// writer (a lowered counter under a live writer is exactly what the lock prevents), and every
/// batch asserts the lease is still the lock's, owner and fencing token, so a worker that lost its
/// lock writes nothing. Readers of derived state use the epoch check
/// ([`page_at_epoch`](super::page_at_epoch)).
///
/// Rows are rewritten as they were: owned by the event's stream, counters by the engine, with no
/// `absent` condition. It is idempotent, and it does not delete: a scope's rows that no event holds
/// stay, which cannot happen to a journal written through commits. Returns the state it wrote.
///
/// It **restores compacted rows** (X2): every row is derived from the events, those below a scope's
/// compaction floor included, and the compaction partition (manifest, chunks, lease, audit) is not
/// read or written. The floor and its snapshot stay valid, readers below the floor are served the
/// restored rows, and [`compact`](super::compact) at the manifest's checkpoint prunes them again.
pub async fn rebuild<'a, F: RebuildFence>(
    fence: &F,
    events: impl IntoIterator<Item = &'a SerializedEvent>,
    lease: &RebuildLease,
) -> Result<JournalState, Error> {
    let state = replay(events)?;

    let mut operations = Vec::with_capacity(state.rows.len() + state.counters.len());
    for row in state.views() {
        operations.push(ProjectedView::new(
            PROJECTOR_NAME,
            0,
            operations.len(),
            ViewOperation::put(row.pk, row.sk, row.entity_id, row.entity_type, row.payload),
        ));
    }

    for batch in rebuild_batches(&operations) {
        fence.write_rebuild_views(lease, batch).await?;
    }

    Ok(state)
}

/// [`rebuild`] holding the lock for its whole run: takes it as [`REBUILD_OWNER`] for `lease`
/// ([`begin_rebuild`](RebuildFence::begin_rebuild)), rebuilds, and releases it, which settles the
/// generation for readers. A lock another rebuild holds is [`Error::RebuildLockHeld`].
///
/// If the rebuild fails the lock is **kept**: the derived rows may be half-written, so commits stay
/// refused and readers stay fenced until the lease lapses and a retry takes it over. An operator
/// may force-release it instead, but that does not let commits through: the generation stays
/// unsettled, and every commit (a journaled one would allocate positions from counters the failed
/// rebuild may have cleared) is refused with [`Error::RebuildInProgress`] until a later rebuild,
/// this function run again, ends. The lease is not renewed; use [`rebuild`] and
/// [`renew_rebuild`](RebuildFence::renew_rebuild) for a long run.
pub async fn rebuild_with_lock<'a, F: RebuildFence>(
    fence: &F,
    events: impl IntoIterator<Item = &'a SerializedEvent>,
    lease: Duration,
) -> Result<JournalState, Error> {
    let held = fence.begin_rebuild(REBUILD_OWNER, lease).await?;
    let state = rebuild(fence, events, &held).await?;
    fence.end_rebuild(&held).await?;

    Ok(state)
}
