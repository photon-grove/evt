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
use crate::rebuild::{
    Held, RebuildFailure, RebuildFence, RebuildLease, Renewing, check_renew, rebuild_batches,
};

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
/// lock writes nothing. Read `events` after the lease was taken: a commit that landed before the
/// lock but after the read would be missing. Readers of derived state use the epoch check
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
///
/// It does not renew the lease; [`rebuild_with_lease`] does.
pub async fn rebuild<'a, F: RebuildFence>(
    fence: &F,
    events: impl IntoIterator<Item = &'a SerializedEvent>,
    lease: &RebuildLease,
) -> Result<JournalState, Error> {
    let state = replay(events)?;

    for batch in rebuild_batches(&operations(&state)) {
        fence.write_rebuild_views(lease, batch).await?;
    }

    Ok(state)
}

/// [`rebuild`] under a lease the caller took and will end, renewing it as it goes, so one lease can
/// cover the journal and other derived state and a long rebuild does not lapse. The lease is renewed
/// for `renew` (at least one second) before the first batch, and again before each later batch
/// whenever half of `renew` has passed; `lease` is updated in place, so the caller ends the renewed
/// lease. A renewal that finds the lock gone (taken over, force-released) stops the run with
/// [`Error::RebuildLeaseLost`] before another write.
///
/// A failure says how many batches were attempted ([`RebuildFailure::attempted`]). Keep the lock
/// unless no write was attempted under the lease (this run's or your own) **and** your begin
/// acquired it from a settled generation ([`RebuildLease::acquired_from`]); otherwise the rows and
/// counters may be half-written, and only a rebuild that runs to its end may settle the generation.
pub async fn rebuild_with_lease<'a, F: RebuildFence>(
    fence: &F,
    events: impl IntoIterator<Item = &'a SerializedEvent>,
    lease: &mut RebuildLease,
    renew: Duration,
) -> Result<JournalState, RebuildFailure> {
    let unattempted = |error| RebuildFailure {
        error: Box::new(error),
        attempted: 0,
        batches: 0,
    };
    check_renew(renew).map_err(unattempted)?;
    let state = replay(events).map_err(unattempted)?;

    // Renew (and so check) the lease even when there is nothing to write.
    let mut fence = Renewing::new(fence, lease, renew, None);
    fence.keep_alive().await.map_err(unattempted)?;
    write(&mut fence, &operations(&state)).await?;

    Ok(state)
}

/// [`rebuild`] holding the lock for its whole run: derives the state from `events`, takes the lock
/// as [`REBUILD_OWNER`] for `lease` ([`begin_rebuild`](RebuildFence::begin_rebuild)), writes it,
/// renewing the lease for another `lease` before a batch whenever half of it has passed, and
/// releases the lock, which settles the generation for readers. A lock another rebuild holds is
/// [`Error::RebuildLockHeld`]; events that cannot be replayed ([`Error::InvalidJournal`]) fail
/// before the lock is taken. `events` are read before the lock, so a commit that lands in between
/// is missing from the rebuild: rebuild a table nothing writes to, or read the events under a lease
/// and use [`rebuild_with_lease`].
///
/// If a write was attempted and the rebuild fails, the lock is **kept**: the derived rows may be
/// half-written, so commits stay refused and readers stay fenced until the lease lapses and a retry
/// takes it over. So does a run that took over a lapsed lease or acquired after a force-release,
/// whatever failed, because it started from rows an earlier rebuild may have half-written. Only a
/// run that acquired from a settled generation and failed before its first write releases the lock.
/// An operator may force-release a kept lock, but that does not let commits through: the
/// generation stays unsettled, and every commit (a journaled one would allocate positions from
/// counters the failed rebuild may have cleared) is refused with [`Error::RebuildInProgress`] until
/// a later rebuild, this function run again, ends.
pub async fn rebuild_with_lock<'a, F: RebuildFence>(
    fence: &F,
    events: impl IntoIterator<Item = &'a SerializedEvent>,
    lease: Duration,
) -> Result<JournalState, Error> {
    let state = replay(events)?;
    let operations = operations(&state);

    let mut held = Held::take(fence, REBUILD_OWNER, lease).await?;
    let taken = held.taken;
    let result = write(
        &mut Renewing::new(fence, &mut held.lease, lease, Some(taken)),
        &operations,
    )
    .await;

    held.finish(fence, result.map(|()| state)).await
}

/// The journal's rows and then its counters as rebuild puts, owned as the live commits own them.
pub(crate) fn operations(state: &JournalState) -> Vec<ProjectedView> {
    state
        .views()
        .into_iter()
        .enumerate()
        .map(|(index, row)| {
            ProjectedView::new(
                PROJECTOR_NAME,
                0,
                index,
                ViewOperation::put(row.pk, row.sk, row.entity_id, row.entity_type, row.payload),
            )
        })
        .collect()
}

/// Writes `operations` in guarded batches under a renewing lease.
async fn write<F: RebuildFence>(
    fence: &mut Renewing<'_, F>,
    operations: &[ProjectedView],
) -> Result<(), RebuildFailure> {
    let batches: Vec<&[ProjectedView]> = rebuild_batches(operations).collect();

    for batch in &batches {
        if let Err(error) = fence.write(batch).await {
            return Err(RebuildFailure {
                error: Box::new(error),
                attempted: fence.attempted,
                batches: batches.len(),
            });
        }
    }

    Ok(())
}
