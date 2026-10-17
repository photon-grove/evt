//! A PostgreSQL event log, row-for-row compatible with Go's `postgres.Repository`.
//!
//! It uses a [`deadpool_postgres::Pool`]. Call [`PostgresRepository::ensure_schema`] at startup.
//! It runs the same idempotent DDL as Go's `EnsureSchema`. See [`crate::wire::postgres`] for the
//! table layout.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;

use deadpool_postgres::{GenericClient, Pool, PoolError};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::ToSql;

use crate::error::Error;
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::features::{
    self, COMMAND_OUTCOME_PREFIX, CachedMarker, FeatureManifest, FeatureMarker, SYSTEM_PREFIX,
};
use crate::ids::{CommandId, EntityId, EntityType, EventSequence};
use crate::outcome::Outcome;
use crate::repository::{Commit, Repository, TransactionCommit};
use crate::wire::postgres::{
    DEFAULT_EVENTS_TABLE, DEFAULT_SNAPSHOTS_TABLE, ENTITY_LOCK, EventRow, FEATURES_LOCK_EXCLUSIVE,
    FEATURES_LOCK_SHARED, OutcomeRow, SnapshotRow, check_table_names, features_lock_key,
    features_table, insert_outcome_sql, schema_ddl, select_outcome_sql,
};

/// A SQL statement that an in-band projector adds to a commit. It runs in the same transaction
/// as the events, after the event inserts and the floor check and before the snapshot upsert, so a
/// writer that lost a race fails on the event key before it reaches a view row.
pub struct PgStatement {
    /// The SQL text, with `$1`-style placeholders.
    pub sql: String,
    /// The parameters.
    pub params: Vec<Box<dyn ToSql + Sync + Send>>,
}

impl PgStatement {
    /// Creates a statement.
    pub fn new(sql: impl Into<String>, params: Vec<Box<dyn ToSql + Sync + Send>>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }

    /// The statement's size in the shared commit budget: one action, sized by its SQL text. The
    /// bound parameters are opaque, so the estimate is a lower bound; the action count, which
    /// the budget checks exactly, is what makes `CommitTooLarge` fire at the same point as on
    /// DynamoDB.
    fn budget_size(&self) -> usize {
        self.sql.len()
    }
}

impl fmt::Debug for PgStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgStatement")
            .field("sql", &self.sql)
            .field("params", &self.params.len())
            .finish()
    }
}

/// A PostgreSQL-backed [`Repository`]. Clones share the pool.
pub struct PostgresRepository {
    pool: Pool,
    events_table: String,
    snapshots_table: String,
    /// The feature-marker version this handle validated, if it has written yet. Each handle is
    /// its own writer: clones validate independently, like separate processes.
    validated_marker: Mutex<CachedMarker>,
}

impl Clone for PostgresRepository {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            events_table: self.events_table.clone(),
            snapshots_table: self.snapshots_table.clone(),
            validated_marker: Mutex::new(*self.validated_marker()),
        }
    }
}

impl fmt::Debug for PostgresRepository {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresRepository")
            .field("events_table", &self.events_table)
            .field("snapshots_table", &self.snapshots_table)
            .finish_non_exhaustive()
    }
}

const EVENT_COLUMNS: &str =
    "event_id, sequence, event_type, version, entity_id, entity_type, payload, metadata::text";

impl PostgresRepository {
    /// Creates a repository using the default tables (`evt_events`, `evt_snapshots`).
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            events_table: DEFAULT_EVENTS_TABLE.to_owned(),
            snapshots_table: DEFAULT_SNAPSHOTS_TABLE.to_owned(),
            validated_marker: Mutex::new(None),
        }
    }

    /// Uses other table names, so several logical event logs can share a database. Names must be
    /// plain SQL identifiers. This rebinds both events and the repository-owned feature marker,
    /// discarding cached validation from the old log.
    #[must_use = "with_tables returns the reconfigured repository"]
    pub fn with_tables(mut self, events_table: &str, snapshots_table: &str) -> Result<Self, Error> {
        check_table_names(events_table, snapshots_table)
            .map_err(|message| Error::InvalidConfig(format!("postgres: {message}")))?;

        self.events_table = events_table.to_ascii_lowercase();
        self.snapshots_table = snapshots_table.to_ascii_lowercase();
        *self.validated_marker() = None;

        Ok(self)
    }

    /// The connection pool.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Creates the event and snapshot tables if they do not exist.
    pub async fn ensure_schema(&self) -> Result<(), Error> {
        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = client.transaction().await.map_err(Error::backend)?;
        // IF NOT EXISTS can still race in PostgreSQL's catalogs. Go takes this same lock.
        tx.execute(
            "SELECT pg_advisory_xact_lock(1702261762, hashtext($1))",
            &[&self.events_table.to_lowercase()],
        )
        .await
        .map_err(Error::backend)?;
        tx.batch_execute(&schema_ddl(&self.events_table, &self.snapshots_table))
            .await
            .map_err(Error::backend)?;
        tx.commit().await.map_err(Error::backend)
    }

    fn validated_marker(&self) -> std::sync::MutexGuard<'_, CachedMarker> {
        self.validated_marker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Reads the feature manifest row, if one exists.
    pub async fn read_manifest(&self) -> Result<Option<FeatureManifest>, Error> {
        let client = self.pool.get().await.map_err(pool_error)?;

        self.read_manifest_in(&client).await
    }

    /// Reads the feature manifest row on an open transaction or connection.
    async fn read_manifest_in(
        &self,
        client: &impl GenericClient,
    ) -> Result<Option<FeatureManifest>, Error> {
        let table = features_table(&self.events_table);
        // Older logs have only events and snapshots. Catching 42P01 after SELECT would still
        // abort an enclosing commit transaction, so probe without referencing the relation.
        // Callers retain their shared advisory lock through this probe and the eventual write.
        let exists: bool = client
            .query_one("SELECT to_regclass($1) IS NOT NULL", &[&table])
            .await
            .map_err(Error::backend)?
            .get(0);
        if !exists {
            return Ok(None);
        }

        let sql = format!(
            "SELECT version, features, history_dependent, retention FROM {table} WHERE id = 0"
        );
        let row = client.query_opt(&sql, &[]).await.map_err(Error::backend)?;

        row.map(|row| {
            FeatureManifest::from_attributes(
                row.get(0),
                row.get(1),
                Some(row.get(2)),
                Some(row.get(3)),
            )
        })
        .transpose()
    }

    /// Applies the change to the stored feature manifest in one transaction that bumps `version`
    /// by exactly one, under the manifest's exclusive advisory lock. Declaring does not require
    /// support — commits do — so a table can be prepared for, or tested against, a feature this
    /// build does not model.
    pub async fn update_manifest(
        &self,
        change: impl Fn(&mut FeatureManifest),
    ) -> Result<(), Error> {
        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = client.transaction().await.map_err(Error::backend)?;
        let lock_key = features_lock_key(&self.events_table);
        tx.execute(FEATURES_LOCK_EXCLUSIVE, &[&lock_key])
            .await
            .map_err(statement_error)?;

        let observed = self.read_manifest_in(&tx).await?;
        let mut next = observed.clone().unwrap_or_default();
        change(&mut next);
        next.version = observed.as_ref().map_or(1, |manifest| manifest.version + 1);

        let history_dependent = next.history_dependent_json().unwrap_or_else(|| "[]".into());
        let retention = next.retention_json().unwrap_or_else(|| "[]".into());
        let table = features_table(&self.events_table);

        match &observed {
            None => {
                let sql = format!(
                    "INSERT INTO {table} (id, version, features, history_dependent, retention) \
                     VALUES (0, $1, $2, $3, $4)"
                );
                tx.execute(
                    &sql,
                    &[
                        &next.version,
                        &next.features_json(),
                        &history_dependent,
                        &retention,
                    ],
                )
                .await
                .map_err(Error::backend)
            }
            Some(manifest) => {
                let sql = format!(
                    "UPDATE {table} SET version = $1, features = $2, history_dependent = $3, \
                     retention = $4 WHERE id = 0 AND version = $5"
                );
                let written = tx
                    .execute(
                        &sql,
                        &[
                            &next.version,
                            &next.features_json(),
                            &history_dependent,
                            &retention,
                            &manifest.version,
                        ],
                    )
                    .await
                    .map_err(Error::backend)?;
                if written == 0 {
                    // Impossible while the exclusive lock is held; refuse rather than guess.
                    return Err(Error::Backend(
                        "the feature manifest moved during its \
                        declaration despite the exclusive lock"
                            .into(),
                    ));
                }

                Ok(written)
            }
        }?;

        tx.commit().await.map_err(Error::backend)?;
        *self.validated_marker() = None;

        Ok(())
    }

    /// Compares the commit's manifest row against the writer's cached validation, re-validating
    /// when the row changed under it. Runs inside the commit transaction under the shared
    /// advisory lock, so a declaration cannot slip between this check and the `COMMIT`.
    fn check_manifest(&self, observed: Option<&FeatureManifest>) -> Result<(), Error> {
        let mut cached = self.validated_marker();

        if !features::marker_matches(*cached, observed) {
            *cached = Some(features::validate_marker(observed)?);
        }

        Ok(())
    }

    /// Returns the raw event rows for an entity, ascending. Intended for diagnostics and
    /// compatibility checks.
    pub async fn event_rows(&self, entity_id: &EntityId) -> Result<Vec<EventRow>, Error> {
        self.select_events(entity_id, None).await
    }

    /// Returns the raw command-outcome row for a command ID. Intended for diagnostics and
    /// compatibility checks; [`Repository::outcome`] decodes it.
    pub async fn outcome_row(&self, command_id: &CommandId) -> Result<Option<OutcomeRow>, Error> {
        crate::outcome::check_command_id(command_id)?;

        let client = self.pool.get().await.map_err(pool_error)?;
        let row = client
            .query_opt(
                &select_outcome_sql(&self.events_table),
                &[&command_id.as_str()],
            )
            .await
            .map_err(statement_error)?;

        Ok(row.map(|row| OutcomeRow {
            command_id: command_id.to_string(),
            status: row.get(0),
            digest: row.get(1),
            response: row.get(2),
            events: row.get(3),
            rejection: row.get(4),
            code: row.get(5),
        }))
    }

    /// Returns the raw snapshot row for an entity.
    pub async fn snapshot_row(&self, entity_id: &EntityId) -> Result<Option<SnapshotRow>, Error> {
        let client = self.pool.get().await.map_err(pool_error)?;
        let sql = format!(
            "SELECT entity_type, entity_id, sequence, event_sequence, payload FROM {} WHERE entity_id = $1",
            self.snapshots_table
        );

        let row = client
            .query_opt(&sql, &[&entity_id.as_str()])
            .await
            .map_err(Error::backend)?;

        Ok(row.map(|row| SnapshotRow {
            entity_type: row.get(0),
            entity_id: row.get(1),
            sequence: row.get(2),
            event_sequence: row.get(3),
            payload: row.get(4),
        }))
    }

    async fn select_events(
        &self,
        entity_id: &EntityId,
        after: Option<EventSequence>,
    ) -> Result<Vec<EventRow>, Error> {
        let client = self.pool.get().await.map_err(pool_error)?;

        let rows = match after {
            None => {
                let sql = format!(
                    "SELECT {EVENT_COLUMNS} FROM {} WHERE entity_id = $1 ORDER BY sequence ASC",
                    self.events_table
                );
                client.query(&sql, &[&entity_id.as_str()]).await
            }
            Some(after) => {
                let sql = format!(
                    "SELECT {EVENT_COLUMNS} FROM {} WHERE entity_id = $1 AND sequence > $2 ORDER BY sequence ASC",
                    self.events_table
                );
                client.query(&sql, &[&entity_id.as_str(), &after.0]).await
            }
        }
        .map_err(Error::backend)?;

        Ok(rows
            .into_iter()
            .map(|row| EventRow {
                event_id: row.get(0),
                sequence: row.get(1),
                event_type: row.get(2),
                version: row.get(3),
                entity_id: row.get(4),
                entity_type: row.get(5),
                payload: row.get(6),
                metadata: row.get(7),
            })
            .collect())
    }

    /// Checks, under the stream's advisory lock, the conditions the DynamoDB snapshot put
    /// expresses: the stored snapshot is this one's predecessor, and a first snapshot has
    /// sequence 1.
    async fn check_snapshot_predecessor(
        &self,
        client: &impl GenericClient,
        entity_id: &EntityId,
        sequence: i64,
    ) -> Result<(), Error> {
        let sql = format!(
            "SELECT sequence FROM {} WHERE entity_id = $1",
            self.snapshots_table
        );
        let stored: Option<i64> = client
            .query_opt(&sql, &[&entity_id.as_str()])
            .await
            .map_err(statement_error)?
            .map(|row| row.get(0));

        match stored {
            None if sequence != 1 => Err(Error::Conflict {
                entity_id: entity_id.clone(),
                message: format!("snapshot sequence {sequence} has no stored predecessor"),
            }),
            None => Ok(()),
            Some(stored) if stored != sequence - 1 => Err(Error::Conflict {
                entity_id: entity_id.clone(),
                message: format!(
                    "snapshot sequence moved: stored {stored}, commit writes {sequence}"
                ),
            }),
            Some(_) => Ok(()),
        }
    }

    /// Inserts the command-outcome row, the last write of the commit transaction. A unique
    /// violation means another writer recorded the outcome first.
    async fn insert_outcome(
        &self,
        client: &impl GenericClient,
        outcome: &Outcome,
    ) -> Result<(), Error> {
        let row = OutcomeRow::from_outcome(outcome)?;

        client
            .execute(
                &insert_outcome_sql(&self.events_table),
                &[
                    &row.command_id,
                    &row.status,
                    &row.digest,
                    &row.response,
                    &row.events,
                    &row.rejection,
                    &row.code,
                ],
            )
            .await
            .map_err(|err| outcome_error(&outcome.command_id, err))?;

        Ok(())
    }

    async fn upsert_snapshot(
        &self,
        client: &impl GenericClient,
        snapshot: &SnapshotRow,
    ) -> Result<u64, Error> {
        let sql = format!(
            "INSERT INTO {table} (entity_id, entity_type, sequence, event_sequence, payload, updated_at)
             VALUES ($1, $2, $3, $4, $5, now())
             ON CONFLICT (entity_id) DO UPDATE SET
                 entity_type = EXCLUDED.entity_type,
                 sequence = EXCLUDED.sequence,
                 event_sequence = EXCLUDED.event_sequence,
                 payload = EXCLUDED.payload,
                 updated_at = now()
             WHERE EXCLUDED.event_sequence >= {table}.event_sequence",
            table = self.snapshots_table
        );

        client
            .execute(
                &sql,
                &[
                    &snapshot.entity_id,
                    &snapshot.entity_type,
                    &snapshot.sequence,
                    &snapshot.event_sequence,
                    &snapshot.payload,
                ],
            )
            .await
            .map_err(statement_error)
    }
}

impl PostgresRepository {
    /// Asserts, under the stream's advisory lock, that a commit whose first sequence is above 1
    /// continues from the stored head. The head is the larger of the stream's highest event and
    /// its snapshot's event sequence, because compaction can delete every covered event. A first
    /// sequence at or below the head is left to the primary key and the floor check, which report
    /// it as an ordinary conflict.
    async fn check_continuation(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
        entity_id: &EntityId,
        first: EventSequence,
    ) -> Result<(), Error> {
        if first <= EventSequence(1) {
            return Ok(());
        }

        let sql = format!(
            "SELECT GREATEST(
                 COALESCE((SELECT MAX(sequence) FROM {} WHERE entity_id = $1), 0),
                 COALESCE((SELECT MAX(event_sequence) FROM {} WHERE entity_id = $1), 0))",
            self.events_table, self.snapshots_table
        );
        let row = tx
            .query_one(&sql, &[&entity_id.as_str()])
            .await
            .map_err(statement_error)?;
        let head: i64 = row.get(0);

        if first.0 > head + 1 {
            return Err(Error::SequenceGap {
                entity_id: entity_id.clone(),
                after: EventSequence(head),
                found: first,
            });
        }

        Ok(())
    }
}

impl Repository for PostgresRepository {
    type Operation = PgStatement;

    async fn commit(&self, commit: Commit<PgStatement>) -> Result<(), Error> {
        commit.validate()?;
        features::check_not_reserved(&commit.entity_id)?;

        if commit.events.is_empty() && commit.outcome.is_none() {
            return Ok(());
        }

        let last_sequence = commit.events.last().map(|event| event.sequence);
        let outcome = commit.stored_outcome();

        // The budget is backend-neutral, so an oversized commit is refused before any write,
        // identically to the other backends. Each projector statement counts as one action.
        let mut sizes = commit.sizes(|_| None)?;
        sizes.extend(commit.operations.iter().map(PgStatement::budget_size));
        crate::repository::check_budget(sizes)?;

        let rows = commit
            .events
            .iter()
            .map(EventRow::from_event)
            .collect::<Result<Vec<_>, _>>()?;

        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = client.transaction().await.map_err(Error::backend)?;
        // An outcome-only commit writes no stream, so it takes no stream lock.
        if !commit.events.is_empty() {
            tx.execute(ENTITY_LOCK, &[&commit.entity_id.as_str()])
                .await
                .map_err(statement_error)?;
        }

        // The feature-marker check is the first action of every v0.9 commit (spec Writes step 1).
        // The shared advisory lock is held to COMMIT, so a declaration cannot slip between the
        // version check and the commit.
        let lock_key = features_lock_key(&self.events_table);
        tx.execute(FEATURES_LOCK_SHARED, &[&lock_key])
            .await
            .map_err(statement_error)?;
        let manifest = self.read_manifest_in(&tx).await?;
        self.check_manifest(manifest.as_ref())?;

        // Under the stream's advisory lock the commit must continue exactly from the stored head.
        if let Some(first) = commit.events.first() {
            self.check_continuation(&tx, &commit.entity_id, first.sequence)
                .await?;
        }

        let insert = format!(
            "INSERT INTO {} (entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text::jsonb)",
            self.events_table
        );

        for row in &rows {
            tx.execute(
                &insert,
                &[
                    &row.entity_id,
                    &row.sequence,
                    &row.event_id,
                    &row.event_type,
                    &row.version,
                    &row.entity_type,
                    &row.payload,
                    &row.metadata,
                ],
            )
            .await
            .map_err(|err| classify(&commit.entity_id, err))?;
        }

        // Compaction deletes the events a snapshot covers, so the primary key alone would let a
        // writer that loaded before the snapshot recreate a compacted sequence. Checking after the
        // inserts is race-free: a concurrent writer can only raise the floor past our first
        // sequence by inserting that sequence, which our uncommitted insert already holds.
        let first = rows.first().map_or(0, |row| row.sequence);
        let floor = format!(
            "SELECT event_sequence FROM {} WHERE entity_id = $1 AND event_sequence >= $2",
            self.snapshots_table
        );
        if !rows.is_empty()
            && let Some(row) = tx
                .query_opt(&floor, &[&commit.entity_id.as_str(), &first])
                .await
                .map_err(statement_error)?
        {
            return Err(Error::Conflict {
                entity_id: commit.entity_id,
                message: format!(
                    "event sequence {first} is at or below the snapshot floor {}",
                    row.get::<_, i64>(0)
                ),
            });
        }

        // Projector statements run after the events, so a writer that lost a race fails on the
        // event key (a conflict) rather than on a view row (a projector error).
        for statement in &commit.operations {
            let params: Vec<&(dyn ToSql + Sync)> = statement
                .params
                .iter()
                .map(|param| param.as_ref() as &(dyn ToSql + Sync))
                .collect();
            tx.execute(&statement.sql, &params)
                .await
                .map_err(projector_error)?;
        }

        if let (Some(snapshot), Some(last_sequence)) = (commit.snapshot, last_sequence) {
            self.check_snapshot_predecessor(&tx, &commit.entity_id, snapshot.sequence.0)
                .await?;
            let row = SnapshotRow {
                entity_id: commit.entity_id.to_string(),
                entity_type: snapshot.entity_type.to_string(),
                sequence: snapshot.sequence.0,
                event_sequence: last_sequence.0,
                payload: snapshot.payload,
            };
            self.upsert_snapshot(&tx, &row).await?;
        }

        // The outcome row is the last write of the transaction.
        if let Some(outcome) = &outcome {
            self.insert_outcome(&tx, outcome).await?;
        }

        tx.commit()
            .await
            .map_err(|err| commit_error(err, outcome.is_some()))
    }

    async fn commit_transaction(
        &self,
        commit: TransactionCommit<PgStatement>,
    ) -> Result<(), Error> {
        let mut commit = commit;
        commit.normalize()?;

        if commit.is_empty() {
            return Ok(());
        }

        let outcome = commit.stored_outcome();

        // Budget parity: an oversized commit fails identically on every backend, before any write.
        // Each projector statement counts as one action.
        let mut sizes = commit.sizes(|_| None)?;
        sizes.extend(commit.operations.iter().map(PgStatement::budget_size));
        crate::repository::check_budget(sizes)?;

        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = client.transaction().await.map_err(Error::backend)?;

        // Every touched stream — written and fenced — takes its advisory lock in canonical
        // (sorted) order, so concurrent multi-stream commits cannot deadlock.
        let mut ids: Vec<&str> = commit
            .streams
            .iter()
            .map(|stream| stream.entity_id.as_str())
            .chain(commit.fences.iter().map(|fence| fence.entity_id.as_str()))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        for id in &ids {
            tx.execute(ENTITY_LOCK, &[id])
                .await
                .map_err(statement_error)?;
        }

        // The feature-marker check is the first action of every v0.9 commit (spec Writes step 1).
        // The shared advisory lock is held to COMMIT, so a declaration cannot slip between the
        // version check and the commit.
        let lock_key = features_lock_key(&self.events_table);
        tx.execute(FEATURES_LOCK_SHARED, &[&lock_key])
            .await
            .map_err(statement_error)?;
        let manifest = self.read_manifest_in(&tx).await?;
        self.check_manifest(manifest.as_ref())?;

        // Fences are refused for entity types durably declared with retention: TTL deletes reopen
        // fenced slots without leaving a successor snapshot. The refusal consults the manifest in
        // this transaction, not this instance's configuration.
        commit.check_retention(manifest.as_ref(), |_| false)?;

        // Under the streams' advisory locks every written stream must continue exactly from its
        // stored head; a gap is refused before anything is inserted.
        for stream in &commit.streams {
            if let Some(first) = stream.events.first() {
                self.check_continuation(&tx, &stream.entity_id, first.sequence)
                    .await?;
            }
        }

        let insert = format!(
            "INSERT INTO {} (entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text::jsonb)",
            self.events_table
        );

        for stream in &commit.streams {
            let rows = stream
                .events
                .iter()
                .map(EventRow::from_event)
                .collect::<Result<Vec<_>, _>>()?;

            for row in &rows {
                tx.execute(
                    &insert,
                    &[
                        &row.entity_id,
                        &row.sequence,
                        &row.event_id,
                        &row.event_type,
                        &row.version,
                        &row.entity_type,
                        &row.payload,
                        &row.metadata,
                    ],
                )
                .await
                .map_err(|err| classify(&stream.entity_id, err))?;
            }

            // Per-stream floor check, as after the inserts: race-free under the stream's lock.
            let first = rows.first().map_or(0, |row| row.sequence);
            let floor = format!(
                "SELECT event_sequence FROM {} WHERE entity_id = $1 AND event_sequence >= $2",
                self.snapshots_table
            );
            if let Some(row) = tx
                .query_opt(&floor, &[&stream.entity_id.as_str(), &first])
                .await
                .map_err(statement_error)?
            {
                return Err(Error::Conflict {
                    entity_id: stream.entity_id.clone(),
                    message: format!(
                        "event sequence {first} is at or below the snapshot floor {}",
                        row.get::<_, i64>(0)
                    ),
                });
            }
        }

        // Projector statements run after the events, so a writer that lost a race fails on the
        // event key (a conflict) rather than on a view row (a projector error).
        for statement in &commit.operations {
            let params: Vec<&(dyn ToSql + Sync)> = statement
                .params
                .iter()
                .map(|param| param.as_ref() as &(dyn ToSql + Sync))
                .collect();
            tx.execute(&statement.sql, &params)
                .await
                .map_err(projector_error)?;
        }

        // Commit snapshots are written in the same transaction as their events, under the same
        // conditions the DynamoDB snapshot put expresses.
        for stream in &commit.streams {
            if let Some(snapshot) = &stream.snapshot {
                self.check_snapshot_predecessor(&tx, &stream.entity_id, snapshot.sequence.0)
                    .await?;

                let last_sequence = stream
                    .events
                    .last()
                    .expect("normalized stream has events")
                    .sequence;
                let row = SnapshotRow {
                    entity_id: stream.entity_id.to_string(),
                    entity_type: snapshot.entity_type.to_string(),
                    sequence: snapshot.sequence.0,
                    event_sequence: last_sequence.0,
                    payload: snapshot.payload.clone(),
                };
                self.upsert_snapshot(&tx, &row).await?;
            }
        }

        // Fenced read-only streams: assert that no stored event and no snapshot covers past the
        // fenced head — the SQL form of the slot check plus the snapshot companion (ADR 0004, C2).
        // The locks above hold the streams steady through the assertions.
        for fence in &commit.fences {
            let sql = format!(
                "SELECT COALESCE((SELECT MAX(sequence) FROM {} WHERE entity_id = $1), 0),
                        COALESCE((SELECT MAX(event_sequence) FROM {} WHERE entity_id = $1), 0)",
                self.events_table, self.snapshots_table
            );
            let row = tx
                .query_one(&sql, &[&fence.entity_id.as_str()])
                .await
                .map_err(statement_error)?;
            let event_head: i64 = row.get(0);
            let snapshot_head: i64 = row.get(1);

            if event_head > fence.head.0 || snapshot_head > fence.head.0 {
                return Err(Error::FenceConflict {
                    entity_id: fence.entity_id.clone(),
                    head: fence.head,
                    provenance: fence.provenance,
                });
            }
        }

        // The outcome row is the last write of the transaction, after the fence assertions.
        if let Some(outcome) = &outcome {
            self.insert_outcome(&tx, outcome).await?;
        }

        tx.commit()
            .await
            .map_err(|err| commit_error(err, outcome.is_some()))
    }

    /// Reads the outcome row. PostgreSQL reads are transactionally consistent, so no flag is
    /// needed: a committed outcome is always visible.
    fn backoff(&self, duration: std::time::Duration) -> impl Future<Output = ()> + Send {
        tokio::time::sleep(duration)
    }

    fn deadline(&self, duration: std::time::Duration) -> impl Future<Output = ()> + Send {
        tokio::time::sleep(duration)
    }

    async fn outcome(&self, command_id: &CommandId) -> Result<Option<Outcome>, Error> {
        crate::outcome::check_command_id(command_id)?;

        self.outcome_row(command_id)
            .await?
            .map(OutcomeRow::into_outcome)
            .transpose()
    }

    async fn events(&self, entity_id: &EntityId) -> Result<Vec<SerializedEvent>, Error> {
        self.select_events(entity_id, None)
            .await?
            .into_iter()
            .map(EventRow::into_event)
            .collect()
    }

    async fn events_after(
        &self,
        entity_id: &EntityId,
        after: EventSequence,
    ) -> Result<Vec<SerializedEvent>, Error> {
        self.select_events(entity_id, Some(after))
            .await?
            .into_iter()
            .map(EventRow::into_event)
            .collect()
    }

    async fn latest(
        &self,
        entity_id: &EntityId,
        count: usize,
    ) -> Result<Vec<SerializedEvent>, Error> {
        if count == 0 {
            return Err(Error::InvalidArgument(
                "latest(n) requires at least one event: got n = 0".into(),
            ));
        }

        let client = self.pool.get().await.map_err(pool_error)?;
        let sql = format!(
            "SELECT {EVENT_COLUMNS} FROM {} WHERE entity_id = $1 ORDER BY sequence DESC LIMIT $2",
            self.events_table
        );

        let rows = client
            .query(
                &sql,
                &[
                    &entity_id.as_str(),
                    &i64::try_from(count).unwrap_or(i64::MAX),
                ],
            )
            .await
            .map_err(Error::backend)?;

        rows.into_iter()
            .map(|row| EventRow {
                event_id: row.get(0),
                sequence: row.get(1),
                event_type: row.get(2),
                version: row.get(3),
                entity_id: row.get(4),
                entity_type: row.get(5),
                payload: row.get(6),
                metadata: row.get(7),
            })
            .map(EventRow::into_event)
            .collect()
    }

    async fn snapshot(&self, entity_id: &EntityId) -> Result<Option<SerializedSnapshot>, Error> {
        Ok(self
            .snapshot_row(entity_id)
            .await?
            .map(SnapshotRow::into_snapshot))
    }

    async fn put_snapshot(&self, snapshot: &SerializedSnapshot) -> Result<bool, Error> {
        features::check_not_reserved(&snapshot.entity_id)?;

        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = client.transaction().await.map_err(Error::backend)?;
        tx.execute(ENTITY_LOCK, &[&snapshot.entity_id.as_str()])
            .await
            .map_err(statement_error)?;

        let lock_key = features_lock_key(&self.events_table);
        tx.execute(FEATURES_LOCK_SHARED, &[&lock_key])
            .await
            .map_err(statement_error)?;
        let manifest = self.read_manifest_in(&tx).await?;
        self.check_manifest(manifest.as_ref())?;

        let written = self
            .upsert_snapshot(&tx, &SnapshotRow::from_snapshot(snapshot))
            .await?;
        tx.commit().await.map_err(statement_error)?;

        Ok(written > 0)
    }

    async fn compact_below(
        &self,
        entity_id: &EntityId,
        through: EventSequence,
    ) -> Result<usize, Error> {
        features::check_not_reserved(entity_id)?;

        if through < EventSequence(1) {
            return Ok(0);
        }

        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = client.transaction().await.map_err(Error::backend)?;
        let lock_key = features_lock_key(&self.events_table);
        tx.execute(FEATURES_LOCK_SHARED, &[&lock_key])
            .await
            .map_err(statement_error)?;
        let manifest = self.read_manifest_in(&tx).await?;
        if let Some(manifest) = &manifest {
            manifest.validate_supported()?;
        }

        let sql = format!(
            "SELECT entity_type, event_sequence FROM {} WHERE entity_id = $1",
            self.snapshots_table
        );
        let snapshot = tx
            .query_opt(&sql, &[&entity_id.as_str()])
            .await
            .map_err(Error::backend)?;
        let covered = snapshot.as_ref().map(|row| EventSequence(row.get(1)));
        if covered.is_none_or(|covered| covered < through) {
            return Err(Error::CompactionUncovered {
                entity_id: entity_id.clone(),
                through,
                covered,
            });
        }

        // Keep the shared lock through the delete and COMMIT: a completed declaration must
        // never be followed by deletion authorized against the previous manifest.
        if let (Some(manifest), Some(snapshot)) = (&manifest, &snapshot) {
            let entity_type = EntityType::new(snapshot.get::<_, String>(0));
            if manifest.is_history_dependent(&entity_type) {
                return Err(Error::HistoryDependentProjection {
                    entity_type,
                    entity_id: entity_id.clone(),
                });
            }
        }
        let sql = format!(
            "DELETE FROM {} WHERE entity_id = $1 AND sequence >= 1 AND sequence <= $2",
            self.events_table
        );
        let deleted = tx
            .execute(&sql, &[&entity_id.as_str(), &through.0])
            .await
            .map_err(Error::backend)?;
        tx.commit().await.map_err(Error::backend)?;

        Ok(deleted as usize)
    }

    async fn entity_heads(
        &self,
        entity_type: Option<&EntityType>,
    ) -> Result<BTreeMap<EntityId, EventSequence>, Error> {
        let client = self.pool.get().await.map_err(pool_error)?;
        let sql = format!(
            "SELECT entity_id, MAX(head) AS head FROM (
                 SELECT entity_id, sequence AS head, entity_type FROM {}
                 UNION ALL
                 SELECT entity_id, event_sequence AS head, entity_type FROM {}
             ) sources
             WHERE ($1::text IS NULL OR entity_type = $1)
                 AND entity_id NOT LIKE '{}%'
                 AND entity_id NOT LIKE '{}%'
             GROUP BY entity_id",
            self.events_table, self.snapshots_table, SYSTEM_PREFIX, COMMAND_OUTCOME_PREFIX
        );
        let filter = entity_type.map(EntityType::as_str);

        let rows = client
            .query(&sql, &[&filter])
            .await
            .map_err(Error::backend)?;

        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    EntityId::new(row.get::<_, String>(0)),
                    EventSequence(row.get(1)),
                )
            })
            .collect())
    }
}

impl FeatureMarker for PostgresRepository {
    async fn read_manifest(&self) -> Result<Option<FeatureManifest>, Error> {
        PostgresRepository::read_manifest(self).await
    }

    async fn update_manifest(
        &self,
        change: impl Fn(&mut FeatureManifest) + Send,
    ) -> Result<(), Error> {
        PostgresRepository::update_manifest(self, change).await
    }
}

fn classify(entity_id: &EntityId, err: tokio_postgres::Error) -> Error {
    match err.code() {
        Some(code) if *code == SqlState::UNIQUE_VIOLATION => Error::Conflict {
            entity_id: entity_id.clone(),
            message: err.to_string(),
        },
        _ if transient(&err) => Error::Transient(Box::new(err)),
        _ => Error::backend(err),
    }
}

/// Maps a failed outcome insert. A unique violation (`23505`) means another writer recorded the
/// outcome first: an [`Error::OutcomeConflict`], never a generic conflict. Any other failure is
/// ambiguous exactly like the rest of the commit: [`Error::Backend`] unless PostgreSQL rolled the
/// transaction back for a retryable reason.
fn outcome_error(command_id: &CommandId, err: tokio_postgres::Error) -> Error {
    match err.code() {
        Some(code) if *code == SqlState::UNIQUE_VIOLATION => Error::OutcomeConflict {
            command_id: command_id.clone(),
        },
        _ => statement_error(err),
    }
}

/// Maps a failed connection checkout. A timeout means the pool was busy and no statement ran, so it
/// is transient. A closed or misconfigured pool, or a failure to connect, is a backend error.
fn pool_error(err: PoolError) -> Error {
    if matches!(err, PoolError::Timeout(_)) {
        Error::Transient(Box::new(err))
    } else {
        Error::backend(err)
    }
}

/// Maps a failed projector statement. Only an error the database raised is the projector's
/// fault. A lost connection or a pool timeout is a backend failure.
fn projector_error(err: tokio_postgres::Error) -> Error {
    if transient(&err) {
        Error::Transient(Box::new(err))
    } else if err.as_db_error().is_some() {
        Error::Projector(Box::new(err))
    } else {
        Error::backend(err)
    }
}

/// Maps a failed `COMMIT`. The event inserts and the snapshot upsert check their own keys, so an
/// integrity violation raised at commit comes from a deferred constraint on a projector's table.
/// An error the database did not raise (the connection dropped while `COMMIT` was in flight) leaves
/// it unknown whether the transaction applied: for a commit that carries an outcome that is
/// [`Error::Indeterminate`], which the store settles by re-reading the outcome row. A database
/// error is a definitive refusal.
fn commit_error(err: tokio_postgres::Error, carries_outcome: bool) -> Error {
    if transient(&err) {
        Error::Transient(Box::new(err))
    } else if err.code().is_some_and(|code| code.code().starts_with("23")) {
        Error::Projector(Box::new(err))
    } else if carries_outcome && err.as_db_error().is_none() {
        Error::Indeterminate(Box::new(err))
    } else {
        Error::backend(err)
    }
}

/// Maps a failed statement inside a commit: [`Error::Transient`] when PostgreSQL rolled the
/// transaction back for a retryable reason, otherwise [`Error::Backend`].
fn statement_error(err: tokio_postgres::Error) -> Error {
    if transient(&err) {
        Error::Transient(Box::new(err))
    } else {
        Error::backend(err)
    }
}

/// Reports whether PostgreSQL rolled the transaction back for a reason a retry may not hit again:
/// a serialization failure or a deadlock.
fn transient(err: &tokio_postgres::Error) -> bool {
    err.code().is_some_and(|code| {
        *code == SqlState::T_R_SERIALIZATION_FAILURE || *code == SqlState::T_R_DEADLOCK_DETECTED
    })
}

mod enumerate;

#[cfg(test)]
mod tests {
    use deadpool_postgres::Manager;
    use tokio_postgres::NoTls;

    use crate::wire::postgres::MAX_EVENTS_TABLE_NAME_LEN;

    use super::*;

    fn repository() -> PostgresRepository {
        let manager = Manager::new(tokio_postgres::Config::new(), NoTls);

        PostgresRepository::new(Pool::builder(manager).build().unwrap())
    }

    #[test]
    fn a_pool_timeout_is_transient() {
        use deadpool_postgres::TimeoutType;

        assert!(pool_error(PoolError::Timeout(TimeoutType::Wait)).is_retryable());
        assert!(!pool_error(PoolError::Closed).is_retryable());
    }

    #[test]
    fn rejects_unsafe_or_shared_table_names() {
        let too_long = "e".repeat(48);
        let events_too_long = "e".repeat(MAX_EVENTS_TABLE_NAME_LEN + 1);
        let events_max = "e".repeat(MAX_EVENTS_TABLE_NAME_LEN);

        for (events, snapshots) in [
            ("evt_events", "evt_events"),
            ("Evt_Events", "evt_events"),
            ("events", "events__evt_features"),
            ("events_pkey", "events"),
            ("orders__evt_features", "evt_snapshots"),
            ("evt_events; drop", "evt_snapshots"),
            ("Select", "evt_snapshots"),
            (too_long.as_str(), "evt_snapshots"),
            (events_too_long.as_str(), "evt_snapshots"),
        ] {
            assert!(
                matches!(
                    repository().with_tables(events, snapshots),
                    Err(Error::InvalidConfig(_))
                ),
                "{events:?}, {snapshots:?}"
            );
        }

        assert!(repository().with_tables("a_events", "a_snapshots").is_ok());
        assert!(repository().with_tables(&events_max, "a_snapshots").is_ok());
    }
}
