//! Rebuilding the journal from stored events.
//!
//! Every journal row and counter is a function of stored events alone, because an event's
//! allocations are in its metadata. Nothing here needs the order the commits happened in, or any
//! commit grouping: feed the events in any order, from any number of streams, and the same rows
//! and counters come out.

use std::collections::BTreeMap;

use super::row::{counter_view, row_view};
use super::scope::{Scope, allocations};
use crate::error::Error;
use crate::event::SerializedEvent;
use crate::projection::ViewRow;
use crate::views::ViewRepository;

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
/// *different* events holding one position of a scope can never come from this journal (the
/// position is claimed once, in a transaction) and fail with [`Error::InvalidJournal`]; the same
/// event given twice is the same row.
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
                if holder.id != event.id {
                    return Err(Error::InvalidJournal(format!(
                        "position {} of scope {scope} is held by both {} and {}",
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

/// Acknowledges that no writer commits to the journal while a [`rebuild`] runs.
///
/// A rebuild rewrites rows and counters unconditionally, so a writer committing beside it can have
/// its counter lowered (the next writer repairs a counter that is behind its rows, but a lowered
/// counter is not a state to run live). The rebuild fence (C10) is the durable way to hold writers
/// off; until it is wired in, the caller promises, and this token is where the lock will go.
#[derive(Clone, Copy, Debug)]
pub struct WritersQuiesced(());

impl WritersQuiesced {
    /// The caller guarantees no `JournalRepository` commits to this table until the rebuild ends.
    pub fn acknowledge() -> Self {
        Self(())
    }
}

/// Rebuilds the journal into a view table: derives the state from `events` and writes every row
/// and counter, unconditionally, with [`ViewRepository::put_views`] (the rebuild path; the commit
/// path's `absent` condition is not wanted here, since a rebuild rewrites rows that exist). It is
/// idempotent. It does not delete rows: wipe the journal's partitions first when rebuilding from
/// scratch. Writers must be held off ([`WritersQuiesced`]). Returns the state it wrote.
pub async fn rebuild<'a, V: ViewRepository>(
    views: &V,
    events: impl IntoIterator<Item = &'a SerializedEvent>,
    _quiesced: WritersQuiesced,
) -> Result<JournalState, Error> {
    let state = replay(events)?;

    views.put_views(&state.views()).await?;

    Ok(state)
}
