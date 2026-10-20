//! Reading the journal: `high_watermark` and `page`, with gap detection.

use std::future::Future;
use std::sync::Arc;

use super::compact::read_manifest;
use super::row::JournalRow;
use super::scope::Scope;
use crate::error::Error;
use crate::projection::ViewRow;
use crate::rebuild::{RebuildFence, read_at_rebuild_epoch};

/// The most rows [`page`] returns at once.
pub const MAX_PAGE_ROWS: usize = 1000;

/// A backend that holds a journal: it reads a scope's counter and its rows, strongly consistently,
/// from the views table the commit path writes to.
///
/// [`MemRepository`](crate::MemRepository) and the DynamoDB repository
/// ([`DynamoRepository`](crate::dynamodb::DynamoRepository), with a views table) implement it.
/// Application code calls [`high_watermark`] and [`page`] rather than these two reads.
pub trait JournalStore: Send + Sync {
    /// The scope's counter: the highest position allocated, `0` when the scope has none. Strongly
    /// consistent.
    fn read_counter(&self, scope: &Scope) -> impl Future<Output = Result<u64, Error>> + Send;

    /// The counters of several scopes, in the order given. The default reads them one at a time;
    /// DynamoDB reads them in one batch.
    fn read_counters(
        &self,
        scopes: &[Scope],
    ) -> impl Future<Output = Result<Vec<u64>, Error>> + Send {
        async move {
            let mut counters = Vec::with_capacity(scopes.len());

            for scope in scopes {
                counters.push(self.read_counter(scope).await?);
            }

            Ok(counters)
        }
    }

    /// The highest position that has a row in the scope, `0` when it has none. Strongly
    /// consistent. It is the truth the counter summarizes: a writer that finds its counter behind
    /// the rows allocates from here, which repairs the counter.
    fn read_last_position(&self, scope: &Scope) -> impl Future<Output = Result<u64, Error>> + Send;

    /// Up to `limit` rows of the scope with a position above `after`, ascending. Strongly
    /// consistent, and not stopped by a missing position: a gap shows as a jump in the rows.
    fn read_rows(
        &self,
        scope: &Scope,
        after: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<JournalRow>, Error>> + Send;

    /// The row at `sort_key` in the scope's compaction partition (`journal-compaction#<scope>`, X2):
    /// its lease, manifest, a chunk, or an audit row. Strongly consistent.
    fn read_compaction_row(
        &self,
        scope: &Scope,
        sort_key: &str,
    ) -> impl Future<Output = Result<Option<ViewRow>, Error>> + Send;
}

macro_rules! forward_journal_store {
    ($($pointer:ty),+) => {$(
        impl<S: JournalStore + ?Sized> JournalStore for $pointer {
            fn read_counter(&self, scope: &Scope) -> impl Future<Output = Result<u64, Error>> + Send {
                (**self).read_counter(scope)
            }

            fn read_counters(
                &self,
                scopes: &[Scope],
            ) -> impl Future<Output = Result<Vec<u64>, Error>> + Send {
                (**self).read_counters(scopes)
            }

            fn read_last_position(
                &self,
                scope: &Scope,
            ) -> impl Future<Output = Result<u64, Error>> + Send {
                (**self).read_last_position(scope)
            }

            fn read_rows(
                &self,
                scope: &Scope,
                after: u64,
                limit: usize,
            ) -> impl Future<Output = Result<Vec<JournalRow>, Error>> + Send {
                (**self).read_rows(scope, after, limit)
            }

            fn read_compaction_row(
                &self,
                scope: &Scope,
                sort_key: &str,
            ) -> impl Future<Output = Result<Option<ViewRow>, Error>> + Send {
                (**self).read_compaction_row(scope, sort_key)
            }
        }
    )+};
}

forward_journal_store!(&S, Arc<S>);

/// Positions the journal should hold but a read did not find: `from..=to`, both included.
///
/// A gap is not an error the reader can fix. The rows after it may be real and the missing ones
/// still in flight (a reader that raced a writer, or a replica that lags), or the rows may be lost
/// (a deleted row, a rebuild that stopped halfway). A consumer must not move past a gap: it retries
/// the page, and escalates when the gap persists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Gap {
    /// The first missing position.
    pub from: u64,
    /// The last missing position.
    pub to: u64,
}

/// One page of a scope's journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalPage {
    /// The scope.
    pub scope: Scope,
    /// The position the page was read after.
    pub after: u64,
    /// The rows from `after + 1`, ascending and **contiguous**: the page stops before a gap.
    pub rows: Vec<JournalRow>,
    /// The scope's high watermark when the page was read: the highest position allocated,
    /// at least the last row returned.
    pub high_watermark: u64,
    /// The first gap met, if any: the rows stop just before it.
    pub gap: Option<Gap>,
    /// The reader's position is **ahead of the journal**: `after` is above the high watermark. A
    /// journal restored from a backup, or rebuilt shorter, looks like this to a consumer that
    /// kept its cursor. The page is empty and the consumer is not caught up: it must resync, not
    /// wait.
    pub cursor_ahead: bool,
}

impl JournalPage {
    /// The position to pass as `after` to read on: the last row's position, or `after` for an
    /// empty page. It never passes a gap.
    pub fn next_after(&self) -> u64 {
        self.rows.last().map_or(self.after, |row| row.position)
    }

    /// Reports whether more rows can be read now: the page is contiguous and the watermark is
    /// ahead of it.
    pub fn has_more(&self) -> bool {
        self.gap.is_none() && self.next_after() < self.high_watermark
    }

    /// Reports whether the page reached the watermark with no gap: everything allocated when it
    /// was read has been read. A cursor ahead of the journal is not caught up.
    pub fn is_caught_up(&self) -> bool {
        self.gap.is_none() && !self.cursor_ahead && self.next_after() >= self.high_watermark
    }
}

/// The scope's high watermark: the highest position allocated in it, `0` for a scope nothing has
/// been journaled into. It is the scope's counter row, read strongly consistently, so a position
/// at or below it was allocated by a committed transaction.
pub async fn high_watermark<S: JournalStore>(store: &S, scope: &Scope) -> Result<u64, Error> {
    store.read_counter(scope).await
}

/// Reads up to `limit` rows (1 to [`MAX_PAGE_ROWS`]) of the scope after position `after`.
///
/// The counter is read first, then the rows. Walking the rows from `after + 1`, the page keeps the
/// contiguous prefix and records the first [`Gap`] it meets: a jump in positions inside the rows,
/// or, when the rows end short of the counter, the missing tail up to the high watermark. A row
/// that appears between the two reads raises the reported watermark to itself.
///
/// A fresh page is `after = 0`. Page on with [`JournalPage::next_after`] until
/// [`is_caught_up`](JournalPage::is_caught_up); poll again from the same `after` to wait for new
/// rows.
///
/// When the page meets a gap and the scope's compaction floor ([`read_floor`](super::read_floor))
/// is above the last contiguous position, the rows were deleted by compaction: the read fails with
/// [`Error::JournalGone`], which names the snapshot to install instead (the `410 Gone` handoff).
/// Rows below the floor that are still there (a run that has not deleted them yet, or a rebuild
/// that restored them) are served as usual.
pub async fn page<S: JournalStore>(
    store: &S,
    scope: &Scope,
    after: u64,
    limit: usize,
) -> Result<JournalPage, Error> {
    if limit == 0 || limit > MAX_PAGE_ROWS {
        return Err(Error::InvalidArgument(format!(
            "a journal page holds 1 to {MAX_PAGE_ROWS} rows; got {limit}"
        )));
    }

    let high_watermark = store.read_counter(scope).await?;
    // One row more than the page holds: a full page can then tell that rows follow it, even when
    // the counter is missing or behind.
    let found = store.read_rows(scope, after, limit + 1).await?;
    let page = assemble(scope, after, limit, high_watermark, found);

    // A gap may be rows a compaction deleted (X2). The manifest is read after the rows: a
    // compaction swaps it before it deletes anything, so a deletion the rows show is always
    // explained by the floor read here. A page that reaches the floor is served as it is.
    if page.gap.is_some()
        && let Some(manifest) = read_manifest(store, scope).await?
        && page.next_after() < manifest.checkpoint
    {
        return Err(Error::JournalGone(Box::new(manifest.gone(after))));
    }

    Ok(page)
}

/// [`high_watermark`] under the rebuild epoch check (C10): fails with
/// [`Error::RebuildUnavailable`] while a rebuild holds the fence (or after a force-release) and with
/// [`Error::RebuildEpochChanged`] if one started, finished, or was taken over during the read.
/// Both are transient: read again once the rebuild is over.
pub async fn high_watermark_at_epoch<S: JournalStore + RebuildFence>(
    store: &S,
    scope: &Scope,
) -> Result<u64, Error> {
    read_at_rebuild_epoch(store, |_| high_watermark(store, scope)).await
}

/// [`page`] under the rebuild epoch check (C10), as [`high_watermark_at_epoch`]. The recommended
/// consumer read for a table whose journal can be rebuilt: a page read while a rebuild rewrites
/// rows and counters would show rows that are missing or counters that are behind, and the epoch
/// check turns that into a retryable error instead of a gap to interpret. On a failure, keep the
/// cursor and retry the same page after a backoff.
pub async fn page_at_epoch<S: JournalStore + RebuildFence>(
    store: &S,
    scope: &Scope,
    after: u64,
    limit: usize,
) -> Result<JournalPage, Error> {
    read_at_rebuild_epoch(store, |_| page(store, scope, after, limit)).await
}

/// Builds a page from the rows a read returned: contiguous prefix, first gap, watermark.
fn assemble(
    scope: &Scope,
    after: u64,
    limit: usize,
    high_watermark: u64,
    found: Vec<JournalRow>,
) -> JournalPage {
    let mut rows = Vec::with_capacity(found.len().min(limit));
    let mut gap = None;
    let mut expected = after.saturating_add(1);
    let watermark = high_watermark.max(found.iter().map(|row| row.position).max().unwrap_or(0));

    for row in found.into_iter().take(limit) {
        if row.position != expected {
            gap = Some(Gap {
                from: expected,
                to: row.position - 1,
            });

            break;
        }

        expected = expected.saturating_add(1);
        rows.push(row);
    }

    // The rows ended before the counter says they should have: the missing tail is a gap, unless
    // the page is simply full.
    if gap.is_none() && rows.len() < limit && expected <= watermark {
        gap = Some(Gap {
            from: expected,
            to: watermark,
        });
    }

    let cursor_ahead = after > watermark;

    JournalPage {
        scope: scope.clone(),
        after,
        rows,
        high_watermark: watermark,
        gap,
        cursor_ahead,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::SerializedEvent;
    use crate::journal::scope::{Allocation, set_allocations};

    fn scope() -> Scope {
        Scope::new("world:1").unwrap()
    }

    fn row(position: u64) -> JournalRow {
        let mut event = SerializedEvent {
            id: format!("e:{position}").into(),
            entity_id: "e".into(),
            ..Default::default()
        };
        set_allocations(&mut event, &[Allocation::new(&scope(), position)]);

        JournalRow {
            scope: scope(),
            position,
            owner: "e".into(),
            event,
        }
    }

    fn rows(positions: &[u64]) -> Vec<JournalRow> {
        positions.iter().map(|p| row(*p)).collect()
    }

    fn positions(page: &JournalPage) -> Vec<u64> {
        page.rows.iter().map(|row| row.position).collect()
    }

    #[test]
    fn a_contiguous_page_has_no_gap() {
        let page = assemble(&scope(), 0, 10, 3, rows(&[1, 2, 3]));

        assert_eq!(positions(&page), [1, 2, 3]);
        assert_eq!(page.gap, None);
        assert_eq!((page.next_after(), page.high_watermark), (3, 3));
        assert!(page.is_caught_up());
        assert!(!page.has_more());
    }

    #[test]
    fn a_full_page_short_of_the_watermark_has_more_and_no_gap() {
        let page = assemble(&scope(), 0, 2, 5, rows(&[1, 2]));

        assert_eq!(positions(&page), [1, 2]);
        assert_eq!(page.gap, None);
        assert!(page.has_more());
        assert!(!page.is_caught_up());
        assert_eq!(page.next_after(), 2);
    }

    #[test]
    fn a_jump_in_the_rows_is_a_gap_and_the_page_stops_before_it() {
        let page = assemble(&scope(), 0, 10, 6, rows(&[1, 2, 5, 6]));

        assert_eq!(positions(&page), [1, 2]);
        assert_eq!(page.gap, Some(Gap { from: 3, to: 4 }));
        assert_eq!(
            page.next_after(),
            2,
            "a consumer must not move past the gap"
        );
        assert!(!page.has_more() && !page.is_caught_up());
    }

    #[test]
    fn a_gap_can_start_the_page() {
        let page = assemble(&scope(), 4, 10, 8, rows(&[7, 8]));

        assert!(page.rows.is_empty());
        assert_eq!(page.gap, Some(Gap { from: 5, to: 6 }));
        assert_eq!(page.next_after(), 4);
    }

    #[test]
    fn rows_that_end_short_of_the_counter_are_a_gap_at_the_tail() {
        let page = assemble(&scope(), 0, 10, 5, rows(&[1, 2, 3]));

        assert_eq!(positions(&page), [1, 2, 3]);
        assert_eq!(page.gap, Some(Gap { from: 4, to: 5 }));

        // No rows at all, counter ahead: the whole range is missing.
        let page = assemble(&scope(), 0, 10, 2, Vec::new());
        assert_eq!(page.gap, Some(Gap { from: 1, to: 2 }));
    }

    #[test]
    fn a_row_past_the_counter_raises_the_watermark_without_a_gap() {
        // The counter was read before a commit landed; its rows are in the page.
        let page = assemble(&scope(), 0, 10, 2, rows(&[1, 2, 3]));

        assert_eq!(page.gap, None);
        assert_eq!(page.high_watermark, 3);
        assert!(page.is_caught_up());
    }

    #[test]
    fn an_empty_scope_and_a_caught_up_reader_are_not_gaps() {
        let empty = assemble(&scope(), 0, 10, 0, Vec::new());
        assert_eq!((empty.gap, empty.high_watermark), (None, 0));
        assert!(empty.is_caught_up());

        let caught_up = assemble(&scope(), 7, 10, 7, Vec::new());
        assert_eq!(caught_up.gap, None);
        assert!(caught_up.is_caught_up());
    }

    #[test]
    fn the_row_after_a_full_page_shows_more_when_the_counter_is_behind() {
        // The read asks for limit + 1 rows: with the counter at 1, rows 1..=3 are found for a
        // page of 2. The extra row raises the watermark, so the page is not caught up.
        let page = assemble(&scope(), 0, 2, 1, rows(&[1, 2, 3]));

        assert_eq!(positions(&page), [1, 2]);
        assert_eq!(page.high_watermark, 3);
        assert!(page.gap.is_none() && page.has_more() && !page.is_caught_up());
    }

    #[test]
    fn a_cursor_ahead_of_the_journal_is_flagged_not_caught_up() {
        let page = assemble(&scope(), 9, 10, 4, Vec::new());

        assert!(page.cursor_ahead);
        assert!(page.rows.is_empty() && page.gap.is_none());
        assert!(!page.is_caught_up() && !page.has_more());

        // At the watermark exactly is caught up; one above it is not.
        assert!(assemble(&scope(), 4, 10, 4, Vec::new()).is_caught_up());
        assert!(assemble(&scope(), 5, 10, 4, Vec::new()).cursor_ahead);
    }

    #[test]
    fn a_full_page_is_not_checked_against_the_watermark() {
        // limit rows, all contiguous: the counter being far ahead is "more", not a gap.
        let page = assemble(&scope(), 0, 3, 100, rows(&[1, 2, 3]));

        assert_eq!(page.gap, None);
        assert!(page.has_more());
    }
}
