//! Reading and writing view rows outside the commit path (P1, P10): the Rust counterpart of Go's
//! `evt.ViewRepository`, its `dynamo.ViewRepository.PutViews` batch writer, and the views-table
//! reads.
//!
//! A [`ViewRow`] is the row a commit-wide view operation writes (v0.9, C7) plus an optional `ttl`:
//! the owner stream is the row's `entityID`, and a repository reads and writes the same table and
//! attributes the commit path does. [`MemRepository`](crate::MemRepository),
//! [`DynamoViewRepository`](crate::dynamodb::DynamoViewRepository) and
//! [`PgViewRepository`](crate::postgres::PgViewRepository) implement [`ViewRepository`];
//! [`ViewCodec`](crate::ViewCodec) layers typed JSON values on top of it.
//!
//! **Views are projections.** Every row must be reproducible by replaying events. The writes here
//! take no owner check and no condition, like Go's: [`put_view`](ViewRepository::put_view),
//! [`put_views`](ViewRepository::put_views) and [`delete_view`](ViewRepository::delete_view) are
//! for the rebuild path and for rows no commit-wide projector owns. Writing a row a commit-wide
//! projector owns changes its owner behind the projector's back, which a replay refuses
//! ([`Error::SharedViewKey`]); keep such rows on the commit path.

use std::future::Future;

use crate::error::Error;
use crate::projection::ViewRow;
use crate::wire::dynamo::DEFAULT_VIEW_SORT_KEY;

/// The sort key of a single-item view row: [`get_view`](ViewRepository::get_view) reads it, and
/// the writers use it for a row whose `sk` is empty.
pub const DEFAULT_VIEW_SK: &str = DEFAULT_VIEW_SORT_KEY;

/// One page of view rows and the cursor of the next, if there may be more.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ViewPage {
    /// The rows of the page, in the order the backend lists them.
    pub views: Vec<ViewRow>,
    /// An opaque cursor for the next page, or `None` when the list is exhausted. DynamoDB returns
    /// a cursor whenever a bounded page was full, even when no row follows, so the next page can
    /// be empty, and so does PostgreSQL; [`MemRepository`](crate::MemRepository) returns one only
    /// when a row follows.
    /// Either way, call until the cursor is `None`.
    pub next_cursor: Option<String>,
}

impl ViewPage {
    /// A page.
    pub fn new(views: Vec<ViewRow>, next_cursor: Option<String>) -> Self {
        Self { views, next_cursor }
    }
}

/// Reads and writes view rows by key, partition, and entity type.
///
/// **Consistency.** A point read ([`get_view_at`](Self::get_view_at),
/// [`batch_get_views_at`](Self::batch_get_views_at) and their defaults) and a partition listing
/// ([`list_views_by_pk_paged`](Self::list_views_by_pk_paged),
/// [`list_views_by_pk`](Self::list_views_by_pk)) see every row a commit wrote once the commit has
/// returned, so a request handler can commit and then read its own write. The in-memory repository
/// is trivially consistent; [`DynamoViewRepository`](crate::dynamodb::DynamoViewRepository) reads
/// its base table strongly consistently (its `with_consistent_read(false)` trades that for half
/// the read cost on partition listings), and `PgViewRepository` sees every committed transaction,
/// entity-type listings included. An entity-type listing may lag behind a commit on DynamoDB, which
/// serves it from a global secondary index that can only be read eventually consistently.
///
/// A `limit` of `None` or `Some(0)` leaves the page size to the backend (DynamoDB: its 1 MB
/// response cap). A `cursor` is the opaque [`ViewPage::next_cursor`] of an earlier page of the
/// same listing: base64url JSON of the last key, byte-identical to Go's, so a cursor can move
/// between a Go and a Rust process. A malformed cursor fails with [`Error::InvalidArgument`].
pub trait ViewRepository: Send + Sync {
    /// Reads the row at `pk` and `sk` (an empty `sk` means `"VIEW"`, an empty `pk` is refused with
    /// [`Error::InvalidArgument`]), or `None` when there is none. DynamoDB reads are strongly
    /// consistent.
    fn get_view_at(
        &self,
        pk: &str,
        sk: &str,
    ) -> impl Future<Output = Result<Option<ViewRow>, Error>> + Send;

    /// Writes or replaces the row, with no owner check and no condition. An empty `sk` is stored
    /// as `"VIEW"`.
    fn put_view(&self, view: &ViewRow) -> impl Future<Output = Result<(), Error>> + Send;

    /// Deletes the row at `pk` and `sk` (an empty `sk` means `"VIEW"`, an empty `pk` is refused).
    /// Deleting a row that is not there succeeds.
    fn delete_view(&self, pk: &str, sk: &str) -> impl Future<Output = Result<(), Error>> + Send;

    /// One page of the rows of a partition, ascending by `sk` bytes. It reflects every commit
    /// that has returned (DynamoDB: a strongly consistent query of the base table, unless the
    /// repository opted out).
    fn list_views_by_pk_paged(
        &self,
        pk: &str,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> impl Future<Output = Result<ViewPage, Error>> + Send;

    /// One page of the rows with an entity type. DynamoDB lists them through the
    /// `entityType-index` index, so they are eventually consistent and in no particular order.
    fn list_views_by_entity_type_paged(
        &self,
        entity_type: &str,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> impl Future<Output = Result<ViewPage, Error>> + Send;

    /// Reads the single-item view at `pk` (its `sk` is `"VIEW"`), or `None`. For rows at another
    /// sort key use [`get_view_at`](Self::get_view_at) or a list.
    fn get_view(&self, pk: &str) -> impl Future<Output = Result<Option<ViewRow>, Error>> + Send {
        self.get_view_at(pk, DEFAULT_VIEW_SK)
    }

    /// Writes or replaces many rows (Go `PutViews`, the writer behind a rebuild). The default
    /// checks every row, then writes them one at a time; DynamoDB coalesces them into
    /// `BatchWriteItem` calls of 25 and retries what it leaves unprocessed. The batch is not
    /// atomic: a failure can leave a prefix written. A repeated `(pk, sk)` keeps its last row.
    fn put_views(&self, views: &[ViewRow]) -> impl Future<Output = Result<(), Error>> + Send {
        async move {
            // A bad row refuses the batch before any write, as DynamoDB's batch writer does.
            for view in views {
                check_view_row(view)?;
            }

            for view in views {
                self.put_view(view).await?;
            }

            Ok(())
        }
    }

    /// Reads the rows at the given partition keys and sort key `sk`. Keys are de-duplicated and an
    /// empty key is skipped; only the rows that exist are returned, in no guaranteed order, so
    /// index the result by `pk`. DynamoDB reads are strongly consistent and chunked to 100 keys.
    fn batch_get_views_at(
        &self,
        pks: &[&str],
        sk: &str,
    ) -> impl Future<Output = Result<Vec<ViewRow>, Error>> + Send {
        async move {
            let mut seen = std::collections::BTreeSet::new();
            let mut views = Vec::new();

            for pk in pks {
                if pk.is_empty() || !seen.insert(*pk) {
                    continue;
                }

                views.extend(self.get_view_at(pk, sk).await?);
            }

            Ok(views)
        }
    }

    /// [`batch_get_views_at`](Self::batch_get_views_at) at the single-item sort key `"VIEW"`.
    fn batch_get_views(
        &self,
        pks: &[&str],
    ) -> impl Future<Output = Result<Vec<ViewRow>, Error>> + Send {
        self.batch_get_views_at(pks, DEFAULT_VIEW_SK)
    }

    /// Every row of a partition, ascending by `sk` bytes. It buffers the whole result; page with
    /// [`list_views_by_pk_paged`](Self::list_views_by_pk_paged) for a large partition.
    fn list_views_by_pk(
        &self,
        pk: &str,
    ) -> impl Future<Output = Result<Vec<ViewRow>, Error>> + Send {
        async move {
            let mut views = Vec::new();
            let mut cursor: Option<String> = None;

            loop {
                let page = self
                    .list_views_by_pk_paged(pk, None, cursor.as_deref())
                    .await?;
                views.extend(page.views);

                match page.next_cursor {
                    Some(next) => cursor = Some(next),
                    None => return Ok(views),
                }
            }
        }
    }

    /// Every row with an entity type. It buffers the whole result; page with
    /// [`list_views_by_entity_type_paged`](Self::list_views_by_entity_type_paged) for a large type.
    fn list_views_by_entity_type(
        &self,
        entity_type: &str,
    ) -> impl Future<Output = Result<Vec<ViewRow>, Error>> + Send {
        async move {
            let mut views = Vec::new();
            let mut cursor: Option<String> = None;

            loop {
                let page = self
                    .list_views_by_entity_type_paged(entity_type, None, cursor.as_deref())
                    .await?;
                views.extend(page.views);

                match page.next_cursor {
                    Some(next) => cursor = Some(next),
                    None => return Ok(views),
                }
            }
        }
    }
}

/// Checks the parts of a row every backend refuses before writing: an empty partition key, and a
/// payload that is not valid UTF-8 (DynamoDB stores it as a string).
pub(crate) fn check_view_row(view: &ViewRow) -> Result<(), Error> {
    if view.pk.is_empty() {
        return Err(Error::InvalidArgument(
            "a view row needs a non-empty pk".into(),
        ));
    }

    if std::str::from_utf8(&view.payload).is_err() {
        return Err(Error::InvalidRecord(
            "view payload is not valid UTF-8 and cannot be stored as a string".into(),
        ));
    }

    Ok(())
}

/// The effective page size: `None` for no bound.
pub(crate) fn page_limit(limit: Option<usize>) -> Option<usize> {
    limit.filter(|limit| *limit > 0)
}

/// Forwards every method, the defaulted ones too, so a backend's batching overrides are kept.
macro_rules! forward_view_repository {
    ($($pointer:ty),+) => {$(
        impl<T: ViewRepository + ?Sized> ViewRepository for $pointer {
            fn get_view_at(
                &self,
                pk: &str,
                sk: &str,
            ) -> impl Future<Output = Result<Option<ViewRow>, Error>> + Send {
                (**self).get_view_at(pk, sk)
            }

            fn put_view(&self, view: &ViewRow) -> impl Future<Output = Result<(), Error>> + Send {
                (**self).put_view(view)
            }

            fn delete_view(
                &self,
                pk: &str,
                sk: &str,
            ) -> impl Future<Output = Result<(), Error>> + Send {
                (**self).delete_view(pk, sk)
            }

            fn list_views_by_pk_paged(
                &self,
                pk: &str,
                limit: Option<usize>,
                cursor: Option<&str>,
            ) -> impl Future<Output = Result<ViewPage, Error>> + Send {
                (**self).list_views_by_pk_paged(pk, limit, cursor)
            }

            fn list_views_by_entity_type_paged(
                &self,
                entity_type: &str,
                limit: Option<usize>,
                cursor: Option<&str>,
            ) -> impl Future<Output = Result<ViewPage, Error>> + Send {
                (**self).list_views_by_entity_type_paged(entity_type, limit, cursor)
            }

            fn put_views(
                &self,
                views: &[ViewRow],
            ) -> impl Future<Output = Result<(), Error>> + Send {
                (**self).put_views(views)
            }

            fn batch_get_views_at(
                &self,
                pks: &[&str],
                sk: &str,
            ) -> impl Future<Output = Result<Vec<ViewRow>, Error>> + Send {
                (**self).batch_get_views_at(pks, sk)
            }

            fn list_views_by_pk(
                &self,
                pk: &str,
            ) -> impl Future<Output = Result<Vec<ViewRow>, Error>> + Send {
                (**self).list_views_by_pk(pk)
            }

            fn list_views_by_entity_type(
                &self,
                entity_type: &str,
            ) -> impl Future<Output = Result<Vec<ViewRow>, Error>> + Send {
                (**self).list_views_by_entity_type(entity_type)
            }
        }
    )+};
}

forward_view_repository!(&T, std::sync::Arc<T>);

/// Resolves a point key: an empty `pk` is refused, and an empty `sk` means `"VIEW"`, as it does for
/// a write, so every operation reads the key a write stored.
pub(crate) fn resolve_key<'a>(pk: &str, sk: &'a str) -> Result<&'a str, Error> {
    if pk.is_empty() {
        return Err(Error::InvalidArgument(
            "a view key needs a non-empty pk".into(),
        ));
    }

    Ok(if sk.is_empty() { DEFAULT_VIEW_SK } else { sk })
}
