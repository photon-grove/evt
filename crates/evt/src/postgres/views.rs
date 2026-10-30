//! The evt-owned PostgreSQL views table (ADR 0008): [`PgViewRepository`] reads and writes
//! `{events}__evt_views` outside the commit path, with the DynamoDB view repository's semantics
//! (storage format: PostgreSQL views table).
//!
//! The keys collate as `"C"`, so a partition lists in sort-key byte order, as on DynamoDB, and the
//! payload is `text`, kept byte for byte. Cursors are the DynamoDB repository's encoding of the
//! last row's keys. Every read sees every committed transaction, entity-type lists included.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use deadpool_postgres::Pool;
use tokio_postgres::Row;

use super::{pool_error, read_committed, statement_error};
use crate::error::Error;
use crate::projection::ViewRow;
use crate::views::{ViewPage, ViewRepository, check_view_row, page_limit, resolve_key};
use crate::wire::dynamo::{AttributeValue, Item, decode_view_cursor, encode_view_cursor};
use crate::wire::postgres::{MAX_VIEW_INDEX_KEY_BYTES, MAX_VIEW_PAGE_ROWS, views_table};

/// The columns every read selects, in [`row`]'s order.
const COLUMNS: &str = "pk, sk, entity_id, entity_type, payload, ttl";

/// How many expired rows [`PgViewRepository::purge_expired`] deletes per statement.
const PURGE_BATCH: i64 = 1_000;

/// The view repository over a PostgreSQL store's views table. Obtain it with
/// [`PostgresRepository::view_repository`](super::PostgresRepository::view_repository); the table
/// is created by [`ensure_schema`](super::PostgresRepository::ensure_schema).
///
/// Writes take no owner check and no condition, like the other backends' view repositories: they
/// are for the rebuild path and for rows no commit-wide projector owns.
#[derive(Clone)]
pub struct PgViewRepository {
    pool: Pool,
    table: String,
    /// Set once the database's encoding has been checked: the views table needs UTF-8 text.
    encoding_checked: Arc<AtomicBool>,
}

impl std::fmt::Debug for PgViewRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgViewRepository")
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

impl PgViewRepository {
    /// The view repository of the event log `events_table`, over `pool`.
    pub(super) fn new(pool: Pool, events_table: &str, encoding_checked: Arc<AtomicBool>) -> Self {
        Self {
            pool,
            table: views_table(events_table),
            encoding_checked,
        }
    }

    /// The views table's name.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Deletes the rows whose `ttl` has passed by `now` (whole Unix seconds), a batch at a time
    /// through the partial `ttl` index, and returns how many it deleted. Readers return expired
    /// rows until it runs, as DynamoDB's do until its sweep: an operator or a scheduled job runs
    /// it. Rows without a `ttl` (every journal, counter and compaction row) are never touched.
    ///
    /// The outer condition repeats the expiry because the subquery picks keys from its snapshot:
    /// a row a concurrent write renewed while the delete waited on it is checked again against the
    /// committed row and kept.
    pub async fn purge_expired(&self, now: i64) -> Result<u64, Error> {
        let client = self.client().await?;
        let sql = format!(
            "DELETE FROM {table} WHERE (pk, sk) IN (SELECT pk, sk FROM {table} \
             WHERE ttl > 0 AND ttl <= $1 ORDER BY ttl LIMIT $2) AND ttl > 0 AND ttl <= $1",
            table = self.table
        );
        let mut deleted = 0;

        loop {
            let batch = client
                .execute(&sql, &[&now, &PURGE_BATCH])
                .await
                .map_err(statement_error)?;
            deleted += batch;

            if batch == 0 {
                return Ok(deleted);
            }
        }
    }

    /// Checks, once per repository handle, that the database is `UTF8`.
    pub(super) async fn check_encoding(&self) -> Result<(), Error> {
        self.client().await.map(drop)
    }

    /// A pooled connection, after the one-time encoding check.
    async fn client(&self) -> Result<deadpool_postgres::Client, Error> {
        let client = self.pool.get().await.map_err(pool_error)?;

        if !self.encoding_checked.load(Ordering::Acquire) {
            let encoding: String = client
                .query_one("SELECT current_setting('server_encoding')", &[])
                .await
                .map_err(statement_error)?
                .get(0);

            if encoding != "UTF8" {
                return Err(Error::InvalidConfig(format!(
                    "the PostgreSQL views table needs a UTF8 database, and this one is {encoding}"
                )));
            }

            self.encoding_checked.store(true, Ordering::Release);
        }

        Ok(client)
    }

    /// One page of `query`'s rows; `filter` is the leading parameter (a `pk` or an entity type),
    /// and `after` the cursor's keys to continue after.
    async fn page(
        &self,
        query: &PageQuery,
        filter: &str,
        limit: Option<usize>,
        after: Option<Item>,
    ) -> Result<ViewPage, Error> {
        let client = self.client().await?;
        let size = page_limit(limit).unwrap_or(MAX_VIEW_PAGE_ROWS);
        let bound = i64::try_from(size).unwrap_or(i64::MAX);
        let rows = match after {
            None => client
                .query(&query.first(&self.table), &[&filter, &bound])
                .await
                .map_err(statement_error)?,
            Some(key) => {
                let (pk, sk) = (string(&key, "pk"), string(&key, "sk"));

                // A cursor continues only the listing that made it.
                let continues = if query.entity_type {
                    matches!(key.get("entityType"), Some(AttributeValue::S(kind)) if kind == filter)
                } else {
                    pk == filter
                };

                if !continues {
                    return Err(Error::InvalidArgument(
                        "invalid pagination cursor: it continues another listing".into(),
                    ));
                }

                if query.entity_type {
                    client
                        .query(&query.next(&self.table), &[&filter, &pk, &sk, &bound])
                        .await
                        .map_err(statement_error)?
                } else {
                    client
                        .query(&query.next(&self.table), &[&filter, &sk, &bound])
                        .await
                        .map_err(statement_error)?
                }
            }
        };
        let views = rows.iter().map(row).collect::<Result<Vec<_>, _>>()?;
        let next_cursor = match views.last() {
            Some(last) if views.len() == size => {
                Some(encode_view_cursor(&cursor_key(last, query.entity_type))?)
            }
            _ => None,
        };

        Ok(ViewPage::new(views, next_cursor))
    }
}

/// A listing: by partition (`pk`, ordered by `sk`) or by entity type (ordered by `pk`, `sk`).
struct PageQuery {
    entity_type: bool,
}

impl PageQuery {
    fn first(&self, table: &str) -> String {
        if self.entity_type {
            format!("SELECT {COLUMNS} FROM {table} WHERE entity_type = $1 ORDER BY pk, sk LIMIT $2")
        } else {
            format!("SELECT {COLUMNS} FROM {table} WHERE pk = $1 ORDER BY sk LIMIT $2")
        }
    }

    fn next(&self, table: &str) -> String {
        if self.entity_type {
            format!(
                "SELECT {COLUMNS} FROM {table} WHERE entity_type = $1 AND (pk, sk) > ($2, $3) \
                 ORDER BY pk, sk LIMIT $4"
            )
        } else {
            format!("SELECT {COLUMNS} FROM {table} WHERE pk = $1 AND sk > $2 ORDER BY sk LIMIT $3")
        }
    }
}

/// A cursor's string attribute, empty when absent.
fn string(key: &Item, name: &str) -> String {
    match key.get(name) {
        Some(AttributeValue::S(text)) => text.clone(),
        _ => String::new(),
    }
}

/// The cursor of a page ending at `last`: its `pk` and `sk`, and for an entity-type list its
/// `entityType`, the keys DynamoDB's `LastEvaluatedKey` carries.
fn cursor_key(last: &ViewRow, entity_type: bool) -> Item {
    let mut key = Item::new();
    key.insert("pk".into(), AttributeValue::S(last.pk.clone()));
    key.insert("sk".into(), AttributeValue::S(last.sk.clone()));

    if entity_type {
        key.insert(
            "entityType".into(),
            AttributeValue::S(last.entity_type.clone()),
        );
    }

    key
}

/// Decodes one row of [`COLUMNS`].
pub(super) fn row(row: &Row) -> Result<ViewRow, Error> {
    let payload: String = row.try_get(4).map_err(Error::backend)?;

    Ok(ViewRow {
        pk: row.try_get(0).map_err(Error::backend)?,
        sk: row.try_get(1).map_err(Error::backend)?,
        entity_id: row.try_get(2).map_err(Error::backend)?,
        entity_type: row.try_get(3).map_err(Error::backend)?,
        payload: payload.into_bytes(),
        ttl: row.try_get(5).map_err(Error::backend)?,
    })
}

/// Refuses a string PostgreSQL `text` cannot hold: one with U+0000. DynamoDB and the in-memory
/// repository store it, so this is a typed refusal rather than the database's own error.
fn nul_free(value: &str, field: &str) -> Result<(), Error> {
    if value.contains('\0') {
        return Err(Error::InvalidArgument(format!(
            "a PostgreSQL view {field} cannot contain U+0000"
        )));
    }

    Ok(())
}

/// A point key, resolved as every backend resolves it, then checked for U+0000.
fn key<'a>(pk: &str, sk: &'a str) -> Result<&'a str, Error> {
    let sk = resolve_key(pk, sk)?;
    nul_free(pk, "pk")?;
    nul_free(sk, "sk")?;

    Ok(sk)
}

/// Checks a row before writing it: the checks every backend makes, U+0000, then the index key
/// bound. Returns the sort key it is stored under (`VIEW` for an empty one).
fn checked(view: &ViewRow) -> Result<&str, Error> {
    check_view_row(view)?;
    let sk = key(&view.pk, &view.sk)?;
    nul_free(&view.entity_id, "owner")?;
    nul_free(&view.entity_type, "entity type")?;
    nul_free(
        std::str::from_utf8(&view.payload).unwrap_or_default(),
        "payload",
    )?;
    let key_bytes = view.entity_type.len() + view.pk.len() + sk.len();

    if key_bytes > MAX_VIEW_INDEX_KEY_BYTES {
        return Err(Error::InvalidArgument(format!(
            "view key too long for PostgreSQL: entity type, pk and sk are {key_bytes} bytes \
             together, the limit is {MAX_VIEW_INDEX_KEY_BYTES}"
        )));
    }

    Ok(sk)
}

/// The upsert of one row: every column replaced, as DynamoDB's `PutItem` replaces the item.
fn upsert_sql(table: &str) -> String {
    format!(
        "INSERT INTO {table} ({COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (pk, sk) DO UPDATE SET entity_id = EXCLUDED.entity_id, \
         entity_type = EXCLUDED.entity_type, payload = EXCLUDED.payload, ttl = EXCLUDED.ttl"
    )
}

impl ViewRepository for PgViewRepository {
    async fn get_view_at(&self, pk: &str, sk: &str) -> Result<Option<ViewRow>, Error> {
        let sk = key(pk, sk)?;
        let client = self.client().await?;
        let found = client
            .query_opt(
                &format!(
                    "SELECT {COLUMNS} FROM {} WHERE pk = $1 AND sk = $2",
                    self.table
                ),
                &[&pk, &sk],
            )
            .await
            .map_err(statement_error)?;

        found.as_ref().map(row).transpose()
    }

    async fn put_view(&self, view: &ViewRow) -> Result<(), Error> {
        self.put_views(std::slice::from_ref(view)).await
    }

    /// Writes every row in one transaction. Every row is checked before anything is written, a
    /// repeated `(pk, sk)` keeps its last row, and the rows are written in `(pk, sk)` byte order,
    /// so two batches over the same keys lock them in the same order and cannot deadlock. Unlike
    /// DynamoDB's batches, the write is atomic.
    async fn put_views(&self, views: &[ViewRow]) -> Result<(), Error> {
        let mut rows = std::collections::BTreeMap::new();

        for view in views {
            let sk = checked(view)?;
            rows.insert((view.pk.as_str(), sk), view);
        }

        if rows.is_empty() {
            return Ok(());
        }

        let mut client = self.client().await?;
        let tx = read_committed(&mut client).await?;
        let statement = tx
            .prepare(&upsert_sql(&self.table))
            .await
            .map_err(statement_error)?;

        for ((_, sk), view) in rows {
            let payload = std::str::from_utf8(&view.payload)
                .map_err(|_| Error::InvalidRecord("view payload is not valid UTF-8".into()))?;
            tx.execute(
                &statement,
                &[
                    &view.pk,
                    &sk,
                    &view.entity_id,
                    &view.entity_type,
                    &payload,
                    &view.ttl,
                ],
            )
            .await
            .map_err(statement_error)?;
        }

        tx.commit().await.map_err(statement_error)
    }

    async fn delete_view(&self, pk: &str, sk: &str) -> Result<(), Error> {
        let sk = key(pk, sk)?;
        let client = self.client().await?;
        client
            .execute(
                &format!("DELETE FROM {} WHERE pk = $1 AND sk = $2", self.table),
                &[&pk, &sk],
            )
            .await
            .map_err(statement_error)?;

        Ok(())
    }

    async fn list_views_by_pk_paged(
        &self,
        pk: &str,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<ViewPage, Error> {
        if pk.is_empty() {
            return Err(Error::InvalidArgument(
                "a view listing needs a non-empty pk".into(),
            ));
        }

        nul_free(pk, "pk")?;
        let after = cursor
            .filter(|cursor| !cursor.is_empty())
            .map(decode_view_cursor)
            .transpose()?;

        self.page(&PageQuery { entity_type: false }, pk, limit, after)
            .await
    }

    async fn list_views_by_entity_type_paged(
        &self,
        entity_type: &str,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<ViewPage, Error> {
        nul_free(entity_type, "entity type")?;
        let after = cursor
            .filter(|cursor| !cursor.is_empty())
            .map(decode_view_cursor)
            .transpose()?;

        self.page(&PageQuery { entity_type: true }, entity_type, limit, after)
            .await
    }
}
