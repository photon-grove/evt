//! [`JournalStore`] and [`CompactionStore`] for the PostgreSQL repository, over the evt-owned views
//! table the commit path writes to with [`with_views_table`](super::PostgresRepository::with_views_table)
//! (ADR 0008, storage format: Journal on PostgreSQL). Every read sees every committed transaction, so
//! gap detection never reports rows still in flight as lost.

use std::collections::BTreeMap;

use super::{PostgresRepository, read_committed, statement_error};
use crate::error::Error;
use crate::journal::{
    COUNTER_SORT_KEY, CompactionStore, CompactionWrite, CompactionWritten, Expect,
    JournalCompactionStore, JournalRow, JournalStore, LEASE_SORT_KEY, Scope,
    check_compaction_writes, compaction_pk, meets, parse_row_sk, read_counter, row_sk,
};
use crate::projection::ViewRow;
use crate::rebuild::RebuildEpoch;
use crate::views::ViewRepository;
use crate::wire::postgres::{FEATURES_LOCK_SHARED, rebuild_lock_key, views_table};

/// The columns every row read selects, in [`super::views::row`]'s order.
const COLUMNS: &str = "pk, sk, entity_id, entity_type, payload, ttl";

impl PostgresRepository {
    /// The views table the journal lives in, or [`Error::ViewsNotConfigured`] for a repository
    /// whose commit-wide views go elsewhere: the journal is written only through the views table.
    fn journal_table(&self) -> Result<String, Error> {
        if !self.views_table_target {
            return Err(Error::ViewsNotConfigured {
                backend: "postgres",
            });
        }

        Ok(views_table(&self.events_table))
    }
}

impl JournalStore for PostgresRepository {
    async fn read_counter(&self, scope: &Scope) -> Result<u64, Error> {
        self.journal_table()?;

        match self
            .view_repository()
            .get_view_at(&scope.counter_pk(), COUNTER_SORT_KEY)
            .await?
        {
            Some(row) => read_counter(scope, &row),
            None => Ok(0),
        }
    }

    /// One statement for every scope's counter.
    async fn read_counters(&self, scopes: &[Scope]) -> Result<Vec<u64>, Error> {
        let table = self.journal_table()?;

        if scopes.is_empty() {
            return Ok(Vec::new());
        }

        let pks: Vec<String> = scopes.iter().map(Scope::counter_pk).collect();
        let client = self.pool.get().await.map_err(super::pool_error)?;
        let rows = client
            .query(
                &format!("SELECT {COLUMNS} FROM {table} WHERE pk = ANY($1) AND sk = $2"),
                &[&pks, &COUNTER_SORT_KEY],
            )
            .await
            .map_err(statement_error)?;
        let mut found = BTreeMap::new();

        for row in &rows {
            let row = super::views::row(row)?;
            found.insert(row.pk.clone(), row);
        }

        scopes
            .iter()
            .map(|scope| {
                found
                    .get(&scope.counter_pk())
                    .map_or(Ok(0), |row| read_counter(scope, row))
            })
            .collect()
    }

    async fn read_last_position(&self, scope: &Scope) -> Result<u64, Error> {
        let table = self.journal_table()?;
        let pk = scope.row_pk();
        let client = self.pool.get().await.map_err(super::pool_error)?;
        let last = client
            .query_opt(
                &format!("SELECT sk FROM {table} WHERE pk = $1 ORDER BY sk DESC LIMIT 1"),
                &[&pk],
            )
            .await
            .map_err(statement_error)?;

        match last {
            None => Ok(0),
            Some(row) => {
                let sk: String = row.get(0);

                parse_row_sk(&sk).ok_or_else(|| {
                    Error::InvalidRecord(format!("journal row {pk:?}/{sk:?} has no position"))
                })
            }
        }
    }

    async fn read_rows(
        &self,
        scope: &Scope,
        after: u64,
        limit: usize,
    ) -> Result<Vec<JournalRow>, Error> {
        let table = self.journal_table()?;
        let client = self.pool.get().await.map_err(super::pool_error)?;
        let bound = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = client
            .query(
                &format!(
                    "SELECT {COLUMNS} FROM {table} WHERE pk = $1 AND sk > $2 ORDER BY sk LIMIT $3"
                ),
                &[&scope.row_pk(), &row_sk(after), &bound],
            )
            .await
            .map_err(statement_error)?;

        rows.iter()
            .map(|row| JournalRow::from_view(scope, &super::views::row(row)?))
            .collect()
    }
}

impl JournalCompactionStore for PostgresRepository {
    async fn read_compaction_row(
        &self,
        scope: &Scope,
        sort_key: &str,
    ) -> Result<Option<ViewRow>, Error> {
        self.journal_table()?;

        self.view_repository()
            .get_view_at(&compaction_pk(scope), sort_key)
            .await
    }
}

/// One `READ COMMITTED` transaction per call (storage format: Journal on PostgreSQL):
///
/// 1. with an epoch, the shared rebuild advisory lock and the generation check;
/// 2. the named rows locked `FOR UPDATE` in one order every batch uses: the scope's lease row first,
///    then the rest by `(pk, sk)` bytes;
/// 3. every condition checked against what step 2 read, the first failing write in request order
///    reported as [`CompactionWritten::Refused`];
/// 4. the writes in step 2's order, each guarded again in its own statement: a put that expects no
///    row is a plain `INSERT`, whose key violation (a row that appeared since step 2) refuses the
///    batch, and a delete removes the row only if it is the owner's (an absent row passes).
///
/// `FOR UPDATE` cannot lock an absent row, so an absent key is safe to check only because every
/// batch of a scope puts or checks the scope's lease row first, which serializes the batches before
/// they read any other key.
impl CompactionStore for PostgresRepository {
    async fn write_compaction(
        &self,
        epoch: Option<RebuildEpoch>,
        writes: &[CompactionWrite],
    ) -> Result<CompactionWritten, Error> {
        check_compaction_writes(writes)?;
        let table = self.journal_table()?;

        // Refuse what the table cannot store before any request, as DynamoDB does.
        for write in writes {
            let texts = match write {
                CompactionWrite::Put { row, expect } => {
                    vec![row.payload.as_slice(), expected(expect)]
                }
                CompactionWrite::Check { expect, .. } => vec![expected(expect)],
                CompactionWrite::Delete { .. } => Vec::new(),
            };

            if texts.iter().any(|text| std::str::from_utf8(text).is_err()) {
                return Err(Error::InvalidArgument(
                    "a compaction payload must be UTF-8".into(),
                ));
            }
        }

        // A store compacted before its first commit still records where its views live.
        self.ensure_stream_heads().await?;
        self.ensure_views_declared().await?;

        let mut client = self.pool.get().await.map_err(super::pool_error)?;
        let tx = read_committed(&mut client).await?;

        if let Some(epoch) = epoch {
            tx.execute(
                FEATURES_LOCK_SHARED,
                &[&rebuild_lock_key(&self.events_table)],
            )
            .await
            .map_err(statement_error)?;

            if self.read_generation(&tx, false).await?.value != epoch.generation() {
                return Ok(CompactionWritten::EpochChanged);
            }
        }

        // One lock order for every batch, so two batches over the same keys cannot deadlock: the
        // lease row first (it serializes a scope's batches), then the rest by bytes.
        let mut order: Vec<usize> = (0..writes.len()).collect();
        order.sort_by_key(|&at| {
            let (pk, sk) = writes[at].key();

            (sk != LEASE_SORT_KEY, pk, sk)
        });
        let lock = format!("SELECT {COLUMNS} FROM {table} WHERE pk = $1 AND sk = $2 FOR UPDATE");
        let mut stored = BTreeMap::new();

        for &at in &order {
            let (pk, sk) = writes[at].key();

            if let Some(row) = tx
                .query_opt(&lock, &[&pk, &sk])
                .await
                .map_err(statement_error)?
            {
                stored.insert((pk, sk), super::views::row(&row)?);
            }
        }

        for (at, write) in writes.iter().enumerate() {
            let row = stored.get(&write.key());
            let met = match write {
                CompactionWrite::Put { expect, .. } | CompactionWrite::Check { expect, .. } => {
                    meets(expect, row)
                }
                CompactionWrite::Delete { owner, .. } => {
                    row.is_none_or(|row| row.entity_id == *owner)
                }
            };

            if !met {
                return Ok(CompactionWritten::Refused(at));
            }
        }

        let key_constraint = format!("{table}_pkey");

        for &at in &order {
            let written = match &writes[at] {
                CompactionWrite::Put { row, expect } => {
                    let payload = std::str::from_utf8(&row.payload).unwrap_or_default();
                    let sql = if matches!(expect, Expect::Absent) {
                        format!("INSERT INTO {table} ({COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6)")
                    } else {
                        format!(
                            "INSERT INTO {table} ({COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6) \
                             ON CONFLICT (pk, sk) DO UPDATE SET entity_id = EXCLUDED.entity_id, \
                             entity_type = EXCLUDED.entity_type, payload = EXCLUDED.payload, \
                             ttl = EXCLUDED.ttl"
                        )
                    };

                    tx.execute(
                        &sql,
                        &[
                            &row.pk,
                            &row.sk,
                            &row.entity_id,
                            &row.entity_type,
                            &payload,
                            &row.ttl,
                        ],
                    )
                    .await
                }

                CompactionWrite::Delete { pk, sk, owner } => {
                    tx.execute(
                        &format!(
                            "DELETE FROM {table} WHERE pk = $1 AND sk = $2 AND entity_id = $3"
                        ),
                        &[pk, sk, owner],
                    )
                    .await
                }

                CompactionWrite::Check { .. } => continue,
            };

            match written {
                Ok(_) => {}

                // A row that appeared at an absent key since step 2.
                Err(err)
                    if err
                        .as_db_error()
                        .and_then(|db| db.constraint())
                        .is_some_and(|violated| violated.eq_ignore_ascii_case(&key_constraint)) =>
                {
                    return Ok(CompactionWritten::Refused(at));
                }

                Err(err) => return Err(statement_error(err)),
            }
        }

        tx.commit().await.map_err(statement_error)?;

        Ok(CompactionWritten::Written)
    }

    fn compaction_now(&self) -> i64 {
        self.clock.unix_seconds()
    }
}

/// The payload bytes a condition compares, empty for none.
fn expected(expect: &Expect) -> &[u8] {
    match expect {
        Expect::Payload(payload) => payload,
        Expect::Any | Expect::Absent => &[],
    }
}
