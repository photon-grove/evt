//! A PostgreSQL event log, row-for-row compatible with Go's `postgres.Repository`.
//!
//! It uses a [`deadpool_postgres::Pool`]. Call [`PostgresRepository::ensure_schema`] at startup.
//! It runs the same idempotent DDL as Go's `EnsureSchema`, plus the Rust-only views table
//! ([`PgViewRepository`], ADR 0008). See [`crate::wire::postgres`] for the table layout.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use deadpool_postgres::{GenericClient, Pool, PoolError};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::ToSql;

use crate::error::Error;
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::features::{
    self, COMMAND_OUTCOME_PREFIX, CachedMarker, FeatureManifest, FeatureMarker,
    POSTGRES_VIEWS_FEATURE, POSTGRES_VIEWS_VERSION, SYSTEM_PREFIX,
};
use crate::heads::{self, Probe};
use crate::ids::{CommandId, EntityId, EntityType, EventSequence};
use crate::outcome::Outcome;
use crate::projection::{
    ProjectedView, ProjectorOperationError, REASON_CONDITION_FAILED, ViewCondition, ViewOperation,
    ViewOperationKind,
};
use crate::rebuild::Clock;
use crate::repository::{Commit, ReadFence, Repository, TransactionCommit};
use crate::wire::postgres::{
    DEFAULT_EVENTS_TABLE, DEFAULT_SNAPSHOTS_TABLE, ENTITY_LOCK, EventRow, FEATURES_LOCK_EXCLUSIVE,
    FEATURES_LOCK_SHARED, OutcomeRow, SnapshotRow, check_table_names, features_lock_key,
    features_table, insert_outcome_sql, outcomes_table, rebuild_generation_table, rebuild_lock_key,
    rebuild_lock_table, schema_ddl, select_outcome_sql, stream_heads_table, views_table,
};

/// Turns one commit-wide view operation (v0.9, C7) into the statement that writes it. evt defines
/// no PostgreSQL view schema: the application owns its view tables and their DDL, so a repository
/// executes whatever statement its writer returns, inside the commit's transaction, after the
/// events and before the snapshots (see [`PostgresRepository::with_view_writer`]).
pub type ViewWriter = Arc<dyn Fn(&ViewOperation) -> Result<PgStatement, Error> + Send + Sync>;

/// A SQL statement that an in-band projector adds to a commit. It runs in the same transaction
/// as the events, after the head statements and the event inserts and before the snapshot upsert,
/// so a writer that lost a race fails on its head before it reaches a view row.
///
/// It is cheap to clone (the parameters are shared), which a journaled PostgreSQL repository
/// (`JournalRepository`, the `journal` cargo feature) needs: it stages a commit again when the
/// journal's positions are taken.
#[derive(Clone)]
pub struct PgStatement {
    /// The SQL text, with `$1`-style placeholders.
    pub sql: String,
    /// The parameters, shared so a statement can be cloned.
    pub params: Vec<Arc<dyn ToSql + Sync + Send>>,
    /// For a [view writer](PostgresRepository::with_view_writer)'s statement: the name of the
    /// table's unique or primary-key constraint on `(pk, sk)`. It is required for a put with
    /// [`ViewCondition::Absent`]: only a violation of this constraint means the row already
    /// exists. A unique violation of any other constraint says nothing about the requested row, so
    /// it is an ordinary failure of the statement, with the SQLSTATE as its reason.
    pub key_constraint: Option<String>,
}

impl PgStatement {
    /// Creates a statement.
    pub fn new(sql: impl Into<String>, params: Vec<Box<dyn ToSql + Sync + Send>>) -> Self {
        Self {
            sql: sql.into(),
            params: params.into_iter().map(Arc::from).collect(),
            key_constraint: None,
        }
    }

    /// Names the unique or primary-key constraint on `(pk, sk)` of a view statement; see
    /// [`key_constraint`](PgStatement::key_constraint).
    #[must_use]
    pub fn with_key_constraint(mut self, constraint: impl Into<String>) -> Self {
        self.key_constraint = Some(constraint.into());
        self
    }

    /// The statement's size in the shared commit budget: one action, sized by its SQL text. The
    /// bound parameters are opaque, so the estimate is a lower bound; the action count, which
    /// the budget checks exactly, is what makes `CommitTooLarge` fire at the same point as on
    /// DynamoDB. A view operation on the views table is sized as its DynamoDB action instead
    /// ([`PostgresRepository::view_sizes`]).
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
    /// Turns a commit-wide view operation into its statement, when configured.
    view_writer: Option<ViewWriter>,
    /// The feature-marker version this handle validated, if it has written yet. Each handle is
    /// its own writer: clones validate independently, like separate processes.
    validated_marker: Mutex<CachedMarker>,
    /// The clock rebuild leases are read from.
    clock: Clock,
    /// Records that the rebuild-lock relation exists, so a commit's lock assertion probes the
    /// catalog only until it first finds it. Shared by clones, like the database it describes.
    rebuild_ready: Arc<AtomicBool>,
    /// Records that the database's encoding was found to be UTF-8, so the views table's check runs
    /// once per handle and its clones rather than once per view repository.
    views_encoding_checked: Arc<AtomicBool>,
    /// Whether the commit's view operations go to the evt-owned views table
    /// ([`with_views_table`](Self::with_views_table)) instead of a view writer.
    views_table_target: bool,
    /// Records that the marker declares `postgres-views`, so the declaration runs once per
    /// handle and its clones.
    views_declared: Arc<AtomicBool>,
    /// Whether a journaled commit notifies listeners ([`with_wakeups`](Self::with_wakeups)).
    wakeups: bool,
}

impl Clone for PostgresRepository {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            events_table: self.events_table.clone(),
            snapshots_table: self.snapshots_table.clone(),
            view_writer: self.view_writer.clone(),
            validated_marker: Mutex::new(*self.validated_marker()),
            clock: self.clock.clone(),
            rebuild_ready: self.rebuild_ready.clone(),
            views_encoding_checked: self.views_encoding_checked.clone(),
            views_table_target: self.views_table_target,
            views_declared: self.views_declared.clone(),
            wakeups: self.wakeups,
        }
    }
}

impl fmt::Debug for PostgresRepository {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresRepository")
            .field("events_table", &self.events_table)
            .field("snapshots_table", &self.snapshots_table)
            .field("view_writer", &self.view_writer.is_some())
            .field("views_table_target", &self.views_table_target)
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
            view_writer: None,
            validated_marker: Mutex::new(None),
            clock: Clock::default(),
            rebuild_ready: Arc::new(AtomicBool::new(false)),
            views_encoding_checked: Arc::new(AtomicBool::new(false)),
            views_table_target: false,
            views_declared: Arc::new(AtomicBool::new(false)),
            wakeups: true,
        }
    }

    /// Reads `clock` instead of the system time for rebuild leases (see
    /// [`RebuildFence`](crate::RebuildFence)). For tests: a lease expires without waiting for it.
    #[must_use]
    pub fn with_clock(mut self, clock: impl Fn() -> SystemTime + Send + Sync + 'static) -> Self {
        self.clock = Clock::new(clock);
        self
    }

    /// Writes commit-wide view operations (v0.9, C7) with `writer`. Without one, a commit that
    /// carries view operations is refused with [`Error::ViewsNotConfigured`] before any write.
    ///
    /// **The writer must enforce ownership in SQL.** Every operation names an
    /// [owner](crate::ViewOperation::owner), the stream that owns the row and the value of its
    /// `entity_id` column; no operation may overwrite or delete a row another stream owns, and the
    /// engine cannot check that for you. The repository decides whether an operation's condition
    /// held from its statement's outcome, so the writer's statements must have these semantics:
    ///
    /// - a put with [`ViewCondition::Absent`] must raise a unique violation (`23505`) of the
    ///   `(pk, sk)` key constraint when the row exists: write a plain `INSERT`, with no
    ///   `ON CONFLICT` clause, and name the constraint with [`PgStatement::with_key_constraint`];
    /// - any other put is an upsert that only the owner may overwrite:
    ///   `INSERT ... ON CONFLICT (pk, sk) DO UPDATE SET ... WHERE <table>.entity_id =
    ///   EXCLUDED.entity_id`. A put that affects no row (the row belongs to another stream) fails
    ///   its condition;
    /// - a delete with [`ViewCondition::Present`] must delete the owner's row:
    ///   `DELETE ... WHERE pk = $1 AND sk = $2 AND entity_id = $3`. A delete that affects no row
    ///   (the row is absent, or another stream's) fails the condition;
    /// - a delete without a condition succeeds whether or not it deleted a row, and refuses
    ///   another stream's row. Its statement reports the foreign rows it found instead of the rows
    ///   it deleted: `WITH gone AS (DELETE ... WHERE pk = $1 AND sk = $2 AND entity_id = $3)
    ///   SELECT 1 FROM <table> WHERE pk = $1 AND sk = $2 AND entity_id <> $3`. An affected-row
    ///   count above zero fails the condition. A plain `DELETE` is wrong here: it would silently
    ///   do nothing to another stream's row, and the engine could not tell.
    ///
    /// **A row's owner must never change.** Never delete a row to hand its key to another stream;
    /// keep the row and rewrite it, as its owner, with a tombstone payload.
    ///
    /// A failed condition is an [`Error::ProjectorOperation`] with the reason
    /// [`REASON_CONDITION_FAILED`]; any other error the database raises from a view statement is
    /// one too, with its SQLSTATE as the reason. A serialization failure or deadlock (`40xxx`)
    /// passes through as [`Error::Transient`], as for every other statement of the commit.
    #[must_use]
    pub fn with_view_writer(mut self, writer: ViewWriter) -> Self {
        self.view_writer = Some(writer);
        self.views_table_target = false;
        self
    }

    /// Whether a commit that writes journal rows (through the views table) notifies listeners
    /// with `pg_notify` on the journal channel, one notification per written scope, delivered at
    /// `COMMIT` (read them with `JournalListener`, feature `journal`). On by
    /// default; `false` is for a deployment that only polls (`NOTIFY` serializes notifying commits
    /// at `COMMIT`).
    #[must_use]
    pub fn with_wakeups(mut self, wakeups: bool) -> Self {
        self.wakeups = wakeups;
        self
    }

    /// Makes the evt-owned views table (`{events}__evt_views`, see [`PgViewRepository`]) the
    /// target of a commit's view operations, in place of a [view writer](Self::with_view_writer):
    /// a repository has one or the other. evt builds each statement with the DynamoDB repository's
    /// owner rules, applies the journal's expected-payload condition
    /// ([`applies_expected_payload`](Repository::applies_expected_payload) is true), sizes it in
    /// the commit budget as its DynamoDB item, and declares the `postgres-views` marker feature
    /// before the first commit, so a writer that would put the views elsewhere refuses the store.
    /// The database must be `UTF8`.
    #[must_use]
    pub fn with_views_table(mut self) -> Self {
        self.view_writer = None;
        self.views_table_target = true;
        self
    }

    /// Builds the statements of a commit's view operations, in execution order. It runs before the
    /// budget is checked and before any write, so a repository with no writer, or a writer that
    /// refuses an operation, fails the whole commit with nothing written.
    fn view_statements(&self, views: &[ProjectedView]) -> Result<Vec<PgStatement>, Error> {
        if views.is_empty() {
            return Ok(Vec::new());
        }

        if self.views_table_target {
            let table = views_table(&self.events_table);

            return views
                .iter()
                .map(|view| views_table_statement(&table, view))
                .collect();
        }

        let Some(writer) = &self.view_writer else {
            return Err(Error::ViewsNotConfigured {
                backend: "postgres",
            });
        };

        views
            .iter()
            .map(|view| {
                if view.op.expected_payload.is_some() {
                    return Err(Error::InvalidViewOperation(format!(
                        "projector {:?} (#{}) operation {}: PostgreSQL does not apply an expected \
                         payload",
                        view.projector, view.projector_index, view.operation
                    )));
                }

                let statement = writer(&view.op)?;
                if statement.sql.trim().is_empty() {
                    return Err(Error::InvalidViewOperation(format!(
                        "projector {:?} (#{}) operation {}: the view writer returned an empty \
                         statement",
                        view.projector, view.projector_index, view.operation
                    )));
                }

                if view.op.kind == ViewOperationKind::Put
                    && view.op.condition == ViewCondition::Absent
                    && statement.key_constraint.is_none()
                {
                    return Err(Error::InvalidViewOperation(format!(
                        "projector {:?} (#{}) operation {}: the view writer must name the key \
                         constraint of a put that requires the row to be absent",
                        view.projector, view.projector_index, view.operation
                    )));
                }

                Ok(statement)
            })
            .collect()
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
        self.rebuild_ready = Arc::new(AtomicBool::new(false));
        self.views_declared = Arc::new(AtomicBool::new(false));

        Ok(self)
    }

    /// The connection pool.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Creates the event and snapshot tables if they do not exist, and the views table
    /// ([`PgViewRepository`], ADR 0008), which Go's `EnsureSchema` does not create.
    pub async fn ensure_schema(&self) -> Result<(), Error> {
        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = read_committed(&mut client).await?;
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
        tx.batch_execute(&crate::wire::postgres::views_ddl(&self.events_table))
            .await
            .map_err(Error::backend)?;
        tx.commit().await.map_err(Error::backend)
    }

    /// The view repository over this event log's views table (`{events}__evt_views`), which
    /// [`ensure_schema`](Self::ensure_schema) creates.
    pub fn view_repository(&self) -> PgViewRepository {
        PgViewRepository::new(
            self.pool.clone(),
            &self.events_table,
            self.views_encoding_checked.clone(),
        )
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
        self.declare(change, false).await.map(|_| ())
    }

    /// Declares `stream-heads: 1` in the feature manifest through the ordinary declaration
    /// transaction (one version bump), **without** checking that the tables are empty or that
    /// every stream has a head row.
    ///
    /// This is the hook for a migration tool: after it has backfilled and verified a head row
    /// for every stream, with every writer that predates stream heads stopped, it declares the
    /// feature with this call. A fresh database is declared with [`initialize`](Self::initialize)
    /// (or by its first writer) instead, which verifies it first. Declaring on a populated
    /// database whose streams have no head rows makes every later commit to those streams fail
    /// with [`Error::StreamHeadDrift`].
    pub async fn declare_stream_heads(&self) -> Result<(), Error> {
        self.update_manifest(FeatureManifest::declare_stream_heads)
            .await
    }

    /// Initializes the database: declares `stream-heads` when the events and snapshots tables are
    /// empty, in one transaction under the exclusive feature lock, and returns whether this call
    /// declared it (see [`FeatureMarker::initialize`]). A database that already declares the
    /// feature is left alone; a populated one that does not is refused with
    /// [`Error::StreamHeadsNotMigrated`]. Run [`ensure_schema`](Self::ensure_schema) first.
    ///
    /// Unlike DynamoDB, a PostgreSQL writer still initializes a fresh database itself on its first
    /// write: the check is two `EXISTS` probes under the lock it already takes, and needs no
    /// privilege a writer lacks. This call is for a deploy step that treats both backends alike.
    pub async fn initialize(&self) -> Result<bool, Error> {
        self.declare(FeatureManifest::declare_stream_heads, true)
            .await
    }

    /// One declaration transaction, returning whether it wrote. With `only_if_empty`, it is the
    /// initialization of a fresh database: under the exclusive lock it declares `stream-heads`
    /// unless it is already declared, and refuses with [`Error::StreamHeadsNotMigrated`] when the
    /// events or snapshots table holds a row.
    async fn declare(
        &self,
        change: impl Fn(&mut FeatureManifest),
        only_if_empty: bool,
    ) -> Result<bool, Error> {
        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = read_committed(&mut client).await?;
        let lock_key = features_lock_key(&self.events_table);
        tx.execute(FEATURES_LOCK_EXCLUSIVE, &[&lock_key])
            .await
            .map_err(statement_error)?;

        let observed = self.read_manifest_in(&tx).await?;
        if only_if_empty {
            if let Some(manifest) = &observed {
                manifest.validate_supported()?;
                if manifest.declares_stream_heads() {
                    // Already initialized (or another writer declared it first).
                    return Ok(false);
                }
            }

            let populated: bool = tx
                .query_one(
                    &format!(
                        "SELECT EXISTS (SELECT 1 FROM {}) OR EXISTS (SELECT 1 FROM {})",
                        self.events_table, self.snapshots_table
                    ),
                    &[],
                )
                .await
                .map_err(Error::backend)?
                .get(0);
            if populated {
                return Err(Error::StreamHeadsNotMigrated);
            }
        }

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

        Ok(true)
    }

    /// Makes sure the database is ready for this writer's first write: the feature manifest
    /// declares `stream-heads` (ADR 0007). A database whose manifest lacks it is initialized by
    /// the writer itself when the events and snapshots tables are empty, and otherwise refused with
    /// [`Error::StreamHeadsNotMigrated`]. It runs before the commit transaction, only while the
    /// handle has not validated a manifest yet: a declaration needs the exclusive lock, which a
    /// commit holding the shared lock must not wait for. The commit then re-validates inside its
    /// own transaction.
    async fn ensure_stream_heads(&self) -> Result<(), Error> {
        if self.validated_marker().is_some() {
            return Ok(());
        }

        let observed = self.read_manifest().await?;

        match self.validate_for_write(observed.as_ref(), false) {
            Ok(_) => Ok(()),
            Err(Error::StreamHeadsNotMigrated) => self.initialize().await.map(|_| ()),
            Err(error) => Err(error),
        }
    }

    /// The commit-budget sizes of a commit's view operations: with the views table, each as the
    /// DynamoDB action it mirrors ([`crate::view_operation_size`], which equals DynamoDB's own
    /// sizing), so `CommitTooLarge` fires at the same commit on both backends; with a view writer,
    /// each statement by its SQL text.
    fn view_sizes(&self, views: &[ProjectedView], statements: &[PgStatement]) -> Vec<usize> {
        if self.views_table_target {
            views
                .iter()
                .map(|view| crate::view_operation_size(&view.op))
                .collect()
        } else {
            statements.iter().map(PgStatement::budget_size).collect()
        }
    }

    /// [`features::validate_marker_for_write`], with the PostgreSQL repository's own rule for
    /// `postgres-views`: a repository without the views table refuses a store that declares it
    /// ([`Error::UnsupportedFeature`]), so commit-wide views never split between the views table and
    /// an application's tables. With `views` (a commit through the views table, which
    /// [`ensure_views_declared`](Self::ensure_views_declared) declared the feature for), a store
    /// that no longer declares it fails as [`Error::Transient`], and the retry declares it again.
    fn validate_for_write(
        &self,
        observed: Option<&FeatureManifest>,
        views: bool,
    ) -> Result<features::ValidatedMarker, Error> {
        self.refuse_foreign_views(observed)?;

        let declared =
            observed.is_some_and(|manifest| manifest.features.contains_key(POSTGRES_VIEWS_FEATURE));

        if views && self.views_table_target && !declared {
            self.views_declared.store(false, Ordering::Release);

            return Err(Error::Transient(
                "the store no longer declares postgres-views; declaring it again".into(),
            ));
        }

        features::validate_marker_for_write(observed)
    }

    /// Refuses a store that declares `postgres-views` unless this repository writes its views to
    /// the views table.
    fn refuse_foreign_views(&self, observed: Option<&FeatureManifest>) -> Result<(), Error> {
        match observed.and_then(|manifest| manifest.features.get(POSTGRES_VIEWS_FEATURE)) {
            Some(&version) if !self.views_table_target => Err(Error::UnsupportedFeature {
                feature: POSTGRES_VIEWS_FEATURE.to_owned(),
                version,
            }),

            _ => Ok(()),
        }
    }

    /// Declares `postgres-views` on a store whose views live in the views table, once per handle
    /// (clones share the flag), after checking the database is `UTF8`. Runs before a commit or a
    /// rebuild write, outside its transaction: a declaration takes the exclusive marker lock,
    /// which a commit holding the shared lock must not wait for.
    async fn ensure_views_declared(&self) -> Result<(), Error> {
        if !self.views_table_target || self.views_declared.load(Ordering::Acquire) {
            return Ok(());
        }

        self.view_repository().check_encoding().await?;

        let manifest = self.read_manifest().await?;
        match manifest
            .as_ref()
            .and_then(|manifest| manifest.features.get(POSTGRES_VIEWS_FEATURE))
        {
            Some(&POSTGRES_VIEWS_VERSION) => {}
            Some(&version) => {
                return Err(Error::UnsupportedFeature {
                    feature: POSTGRES_VIEWS_FEATURE.to_owned(),
                    version,
                });
            }
            None => {
                self.update_manifest(|manifest| {
                    manifest
                        .features
                        .insert(POSTGRES_VIEWS_FEATURE.to_owned(), POSTGRES_VIEWS_VERSION);
                })
                .await?;
            }
        }

        self.views_declared.store(true, Ordering::Release);

        Ok(())
    }

    /// Compares the commit's manifest row against the writer's cached validation, re-validating
    /// when the row changed under it. Runs inside the commit transaction under the shared
    /// advisory lock, so a declaration cannot slip between this check and the `COMMIT`.
    fn check_manifest(&self, observed: Option<&FeatureManifest>, views: bool) -> Result<(), Error> {
        let mut cached = self.validated_marker();

        if !features::marker_matches(*cached, observed) {
            *cached = Some(self.validate_for_write(observed, views)?);
        } else if views {
            // The cache says nothing about postgres-views: check it on every commit.
            self.validate_for_write(observed, true)?;
        } else {
            // Nor does it know this handle's view target, which a clone can change
            // (`with_view_writer` on a views-table repository): refuse a foreign store every time.
            self.refuse_foreign_views(observed)?;
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
    /// The commit-time rebuild-lock check (C10): after taking the rebuild fence's shared advisory
    /// lock, held to `COMMIT`, it asserts the generation is settled: the lock row is absent, an
    /// expired row included, and the generation is even, so a force-release no rebuild has
    /// finished since refuses the commit too. A schema that predates the fence has no lock
    /// relation, and nobody can have taken the lock: the assertion passes, and the catalog probe
    /// stops once the relation has been seen.
    async fn assert_no_rebuild_lock(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
    ) -> Result<(), Error> {
        tx.execute(
            FEATURES_LOCK_SHARED,
            &[&rebuild_lock_key(&self.events_table)],
        )
        .await
        .map_err(statement_error)?;

        let table = rebuild_lock_table(&self.events_table);
        if !self.rebuild_ready.load(Ordering::Acquire) {
            let exists: bool = tx
                .query_one("SELECT to_regclass($1) IS NOT NULL", &[&table])
                .await
                .map_err(statement_error)?
                .get(0);
            if !exists {
                return Ok(());
            }

            self.rebuild_ready.store(true, Ordering::Release);
        }

        // A lock row, an expired one included, or an odd generation (a force-release no rebuild
        // has finished since) refuses the commit. The schema DDL creates the generation relation
        // with the lock relation, so seeing one is seeing both.
        let generation = rebuild_generation_table(&self.events_table);
        let held: bool = tx
            .query_one(
                &format!(
                    "SELECT EXISTS (SELECT 1 FROM {table} WHERE id = 0) \
                     OR EXISTS (SELECT 1 FROM {generation} WHERE id = 0 AND value % 2 <> 0)"
                ),
                &[],
            )
            .await
            .map_err(statement_error)?
            .get(0);
        if held {
            return Err(Error::RebuildInProgress);
        }

        Ok(())
    }
}

/// A written stream's head statement and how it fared (ADR 0007).
struct HeadStatement<'a> {
    stream: &'a Commit<PgStatement>,
    first: EventSequence,
    passed: bool,
}

impl PostgresRepository {
    /// The stream-heads table (`{events}__evt_stream_heads`).
    fn heads_table(&self) -> String {
        stream_heads_table(&self.events_table)
    }

    /// Reads a stream's stored head under its advisory lock; absent is 0.
    async fn stored_head(
        &self,
        client: &impl GenericClient,
        entity_id: &EntityId,
    ) -> Result<EventSequence, Error> {
        let sql = format!(
            "SELECT head FROM {} WHERE entity_id = $1",
            self.heads_table()
        );
        let row = client
            .query_opt(&sql, &[&entity_id.as_str()])
            .await
            .map_err(statement_error)?;

        Ok(EventSequence(row.map_or(0, |row| row.get(0))))
    }

    /// Derives a stream's head (H4) from the events and snapshots tables: the larger of its
    /// highest event sequence and its snapshot's event sequence.
    async fn derived_head(
        &self,
        client: &impl GenericClient,
        entity_id: &EntityId,
    ) -> Result<EventSequence, Error> {
        let sql = format!(
            "SELECT GREATEST(
                 COALESCE((SELECT MAX(sequence) FROM {} WHERE entity_id = $1), 0),
                 COALESCE((SELECT MAX(event_sequence) FROM {} WHERE entity_id = $1), 0))",
            self.events_table, self.snapshots_table
        );
        let row = client
            .query_one(&sql, &[&entity_id.as_str()])
            .await
            .map_err(statement_error)?;

        Ok(EventSequence(row.get(0)))
    }

    /// The failure-path probe, run inside the commit transaction under the stream's lock (ADR
    /// 0007): derive the head `D1`, read the stored head `S`, and compare. `S < D1` is drift,
    /// `S = D1` is no drift, and `S > D1` derives again: `D2 < S` is drift, otherwise a commit
    /// landed between the reads, an ordinary conflict. Returns `Ok(Err(drift))` for drift,
    /// `Ok(Ok(Some(stored)))` when the head matches the log, and `Ok(Ok(None))` for the landed
    /// commit.
    async fn probe(
        &self,
        client: &impl GenericClient,
        entity_id: &EntityId,
    ) -> Result<Result<Option<EventSequence>, Error>, Error> {
        let derived = self.derived_head(client, entity_id).await?;
        let stored = self.stored_head(client, entity_id).await?;

        Ok(match Probe::first(stored, derived) {
            Probe::Settled => Ok(Some(stored)),
            Probe::Drift => Err(heads::drift(entity_id, stored, derived)),
            Probe::Ahead => {
                let rederived = self.derived_head(client, entity_id).await?;

                match Probe::after_rederive(stored, rederived) {
                    Probe::Settled => Ok(None),
                    _ => Err(heads::drift(entity_id, stored, rederived)),
                }
            }
        })
    }

    /// Runs one written stream's head statement: a create inserts the row, an append moves it
    /// from the sequence before the commit's first. It passes when exactly one row is affected.
    async fn run_head_statement(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
        stream: &Commit<PgStatement>,
    ) -> Result<bool, Error> {
        let (Some(first), Some(last)) = (stream.events.first(), stream.events.last()) else {
            return Ok(true);
        };

        let table = self.heads_table();
        let affected = if first.sequence <= EventSequence(1) {
            let sql = format!(
                "INSERT INTO {table} (entity_id, entity_type, head) VALUES ($1, $2, $3)
                 ON CONFLICT (entity_id) DO NOTHING"
            );
            tx.execute(
                &sql,
                &[
                    &stream.entity_id.as_str(),
                    &first.entity_type.as_str(),
                    &last.sequence.0,
                ],
            )
            .await
        } else {
            let sql = format!("UPDATE {table} SET head = $3 WHERE entity_id = $1 AND head = $2");
            tx.execute(
                &sql,
                &[
                    &stream.entity_id.as_str(),
                    &(first.sequence.0 - 1),
                    &last.sequence.0,
                ],
            )
            .await
        }
        .map_err(statement_error)?;

        Ok(affected == 1)
    }

    /// Writes the streams, fences, and outcome of one commit in one transaction, in the pinned
    /// statement order (spec, "PostgreSQL"):
    ///
    /// 1. the per-entity advisory locks, written and fenced streams in ascending entity-ID byte
    ///    order;
    /// 2. the feature-marker `SELECT`, the retention refusal of fences, then the rebuild-lock
    ///    assertion (C10) under the shared rebuild advisory lock;
    /// 3. a probe for an existing outcome row for the command, which is `OutcomeConflict` and
    ///    outranks every stream failure, as on DynamoDB;
    /// 4. every head statement and every fence read, each evaluated without stopping at the
    ///    first failure;
    /// 5. the ranking of any failures (drift, then gap, then conflicts), by the same probe as
    ///    DynamoDB;
    /// 6. the event inserts (an event that already exists after the head statement passed is
    ///    drift, because the locks make it certain), the legacy projector statements, the
    ///    commit-wide view statements, the snapshot upserts, and the outcome insert.
    async fn write_commit(
        &self,
        streams: &[&Commit<PgStatement>],
        fences: &[ReadFence],
        operations: &[&PgStatement],
        views: &[(&ProjectedView, &PgStatement)],
        outcome: Option<&Outcome>,
        check_retention: impl FnOnce(Option<&FeatureManifest>) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let written: Vec<&Commit<PgStatement>> = streams
            .iter()
            .copied()
            .filter(|stream| !stream.events.is_empty())
            .collect();

        self.ensure_stream_heads().await?;
        self.ensure_views_declared().await?;

        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = read_committed(&mut client).await?;

        // Every touched stream, written and fenced, takes its advisory lock in canonical
        // (sorted) order, so concurrent multi-stream commits cannot deadlock. An outcome-only
        // commit with no fences writes no stream, so it takes no stream lock.
        let mut ids: Vec<&str> = written
            .iter()
            .map(|stream| stream.entity_id.as_str())
            .chain(fences.iter().map(|fence| fence.entity_id.as_str()))
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
        self.check_manifest(manifest.as_ref(), self.views_table_target)?;

        // Fences are refused for entity types durably declared with retention: TTL deletes reopen
        // fenced slots without leaving a successor snapshot. The refusal consults the manifest in
        // this transaction, not this instance's configuration.
        check_retention(manifest.as_ref())?;

        // The rebuild-lock assertion (C10): an unsettled generation — any lock row, an expired one
        // included, or a force-release no rebuild has finished since — refuses the commit before
        // the outcome row and every stream is looked at.
        self.assert_no_rebuild_lock(&tx).await?;

        // A recorded outcome outranks every stream failure, as the outcome put does on DynamoDB.
        // The final insert's `23505` still resolves a real race.
        if let Some(outcome) = outcome {
            let sql = format!(
                "SELECT 1 FROM {} WHERE command_id = $1",
                outcomes_table(&self.events_table)
            );
            if tx
                .query_opt(&sql, &[&outcome.command_id.as_str()])
                .await
                .map_err(statement_error)?
                .is_some()
            {
                return Err(Error::OutcomeConflict {
                    command_id: outcome.command_id.clone(),
                });
            }
        }

        // Every head statement and fence read runs, without stopping at the first failure.
        let mut statements = Vec::with_capacity(written.len());
        for stream in &written {
            statements.push(HeadStatement {
                stream,
                first: stream.events[0].sequence,
                passed: self.run_head_statement(&tx, stream).await?,
            });
        }

        let mut failed_fences = Vec::new();
        for fence in fences {
            if written
                .iter()
                .any(|stream| stream.entity_id == fence.entity_id)
            {
                // A written stream's head update supersedes its fence.
                continue;
            }

            if self.stored_head(&tx, &fence.entity_id).await? != fence.head {
                failed_fences.push(fence);
            }
        }

        // Rank before reporting: drift, then gap, then conflicts, in transaction order.
        let mut verdicts = Vec::new();
        for statement in statements.iter().filter(|statement| !statement.passed) {
            let entity_id = &statement.stream.entity_id;

            verdicts.push(match self.probe(&tx, entity_id).await? {
                Err(drift) => drift,
                Ok(Some(stored)) => heads::gap_or_conflict(entity_id, statement.first, stored),
                Ok(None) => heads::conflict(entity_id),
            });
        }
        for fence in failed_fences {
            verdicts.push(match self.probe(&tx, &fence.entity_id).await? {
                Err(drift) => drift,
                Ok(_) => heads::fence_conflict(fence),
            });
        }
        if let Some(error) = heads::rank(verdicts) {
            return Err(error);
        }

        // The event inserts. A conflicting row after the head statement passed is certain drift:
        // an event sits above the head, and the locks rule out a racing writer.
        let insert = format!(
            "INSERT INTO {} (entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text::jsonb)
             ON CONFLICT (entity_id, sequence) DO NOTHING",
            self.events_table
        );
        for statement in &statements {
            let stream = statement.stream;

            for event in &stream.events {
                let row = EventRow::from_event(event)?;
                let inserted = tx
                    .execute(
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

                if inserted == 0 {
                    let derived = self.derived_head(&tx, &stream.entity_id).await?;
                    // The head the stream had before this commit's head statement.
                    let stored = EventSequence(statement.first.0 - 1);

                    return Err(heads::drift(&stream.entity_id, stored, derived));
                }
            }
        }

        // Projector statements run after the events, so a writer that lost a race fails on its
        // head (a conflict) rather than on a view row (a projector error).
        for statement in operations {
            let params: Vec<&(dyn ToSql + Sync)> = statement
                .params
                .iter()
                .map(|param| param.as_ref() as &(dyn ToSql + Sync))
                .collect();
            tx.execute(&statement.sql, &params)
                .await
                .map_err(projector_error)?;
        }

        // The view statements follow the legacy ones, in declaration then emission order.
        run_view_statements(&tx, views).await?;

        // Wake listeners of every scope the commit journaled: delivered at COMMIT, in commit
        // order, and dropped with a rollback.
        #[cfg(feature = "journal")]
        if self.wakeups && self.views_table_target {
            let scopes = journaled_scopes(views);

            if !scopes.is_empty() {
                let channel = crate::wire::postgres::journal_channel(&self.events_table);

                // One statement for every scope, in the array's (ascending) order.
                tx.execute(
                    "SELECT pg_notify($1, scope) FROM unnest($2::text[]) WITH ORDINALITY AS s(scope, n) ORDER BY n",
                    &[&channel, &scopes],
                )
                .await
                .map_err(statement_error)?;
            }
        }

        // Commit snapshots are written in the same transaction as their events, under the same
        // conditions the DynamoDB snapshot put expresses. The commit raised the head to the
        // snapshot's event sequence, so the snapshot is covered (H3).
        for statement in &statements {
            let stream = statement.stream;
            if let Some(snapshot) = &stream.snapshot {
                self.check_snapshot_predecessor(&tx, &stream.entity_id, snapshot.sequence.0)
                    .await?;

                let last_sequence = stream
                    .events
                    .last()
                    .expect("a written stream has events")
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

        // The outcome row is the last write of the transaction, after the fence reads.
        if let Some(outcome) = outcome {
            self.insert_outcome(&tx, outcome).await?;
        }

        tx.commit()
            .await
            .map_err(|err| commit_error(err, outcome.is_some()))
    }
}

impl Repository for PostgresRepository {
    type Operation = PgStatement;

    fn writes_views(&self) -> bool {
        true
    }

    /// True with the views table ([`with_views_table`](PostgresRepository::with_views_table)),
    /// whose statements evt builds; an application view writer cannot express the condition.
    fn applies_expected_payload(&self) -> bool {
        self.views_table_target
    }

    async fn declare_history_dependent(&self, types: &[EntityType]) -> Result<(), Error> {
        crate::features::declare_history(self, types).await
    }

    async fn commit(&self, commit: Commit<PgStatement>) -> Result<(), Error> {
        commit.validate()?;
        features::check_not_reserved(&commit.entity_id)?;

        if commit.events.is_empty() && commit.outcome.is_none() {
            return Ok(());
        }

        let outcome = commit.stored_outcome();

        // The budget is backend-neutral, so an oversized commit is refused before any write,
        // identically to the other backends. Each projector statement counts as one action.
        // The view statements are built first, so a missing writer fails before the budget and
        // before any write, and each counts as one action sized by its SQL text.
        let view_statements = self.view_statements(&commit.views)?;
        let mut sizes = commit.sizes(|_| None)?;
        sizes.extend(commit.operations.iter().map(PgStatement::budget_size));
        sizes.extend(self.view_sizes(&commit.views, &view_statements));
        crate::repository::check_budget(sizes)?;

        let operations: Vec<&PgStatement> = commit.operations.iter().collect();

        self.write_commit(
            &[&commit],
            &[],
            &operations,
            &view_writes(&commit.views, &view_statements),
            outcome.as_ref(),
            |_| Ok(()),
        )
        .await
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
        // The view statements are built first, as for a single-stream commit.
        let view_statements = self.view_statements(&commit.views)?;
        let mut sizes = commit.sizes(|_| None)?;
        sizes.extend(commit.operations.iter().map(PgStatement::budget_size));
        sizes.extend(self.view_sizes(&commit.views, &view_statements));
        crate::repository::check_budget(sizes)?;

        let streams: Vec<&Commit<PgStatement>> = commit.streams.iter().collect();
        let operations: Vec<&PgStatement> = commit.operations.iter().collect();

        self.write_commit(
            &streams,
            &commit.fences,
            &operations,
            &view_writes(&commit.views, &view_statements),
            outcome.as_ref(),
            |manifest| commit.check_retention(manifest, |_| false),
        )
        .await
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

        self.ensure_stream_heads().await?;

        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = read_committed(&mut client).await?;
        tx.execute(ENTITY_LOCK, &[&snapshot.entity_id.as_str()])
            .await
            .map_err(statement_error)?;

        let lock_key = features_lock_key(&self.events_table);
        tx.execute(FEATURES_LOCK_SHARED, &[&lock_key])
            .await
            .map_err(statement_error)?;
        let manifest = self.read_manifest_in(&tx).await?;
        self.check_manifest(manifest.as_ref(), false)?;

        // H3: the stream's head must cover the snapshot. A snapshot of an empty stream (event
        // sequence 0) may have no head row at all. The entity lock holds the head steady.
        let head = self.stored_head(&tx, &snapshot.entity_id).await?;
        if snapshot.event_sequence > EventSequence::ZERO && head < snapshot.event_sequence {
            let derived = self
                .derived_head(&tx, &snapshot.entity_id)
                .await?
                .max(snapshot.event_sequence);

            return Err(heads::drift(&snapshot.entity_id, head, derived));
        }

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
        let tx = read_committed(&mut client).await?;
        let lock_key = features_lock_key(&self.events_table);
        tx.execute(FEATURES_LOCK_SHARED, &[&lock_key])
            .await
            .map_err(statement_error)?;
        let manifest = self.read_manifest_in(&tx).await?;
        // Compaction is a write path: a database that does not declare `stream-heads` (or
        // declares a feature this build does not model) is refused before the snapshot is read,
        // so the refusal wins over `CompactionUncovered`. It never declares the feature itself,
        // and never touches a head row.
        self.validate_for_write(manifest.as_ref(), false)?;

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
        // Until `stream-heads` is declared, the heads table may be empty on a populated database
        // (`ensure_schema` creates it empty), and a head-aware process reading it would report
        // no entities, so an incremental rebuild would skip them all. The aggregate over the
        // events and snapshots is the answer until then.
        let declared = self
            .read_manifest()
            .await?
            .is_some_and(|manifest| manifest.declares_stream_heads());

        let client = self.pool.get().await.map_err(pool_error)?;
        let sql = if declared {
            format!(
                "SELECT entity_id, head FROM {}
                 WHERE ($1::text IS NULL OR entity_type = $1)
                     AND entity_id NOT LIKE '{}%'
                     AND entity_id NOT LIKE '{}%'",
                self.heads_table(),
                SYSTEM_PREFIX,
                COMMAND_OUTCOME_PREFIX
            )
        } else {
            format!(
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
            )
        };
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

    async fn initialize(&self) -> Result<bool, Error> {
        PostgresRepository::initialize(self).await
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

/// The scopes a commit's journal rows belong to, each once, in ascending byte order.
#[cfg(feature = "journal")]
fn journaled_scopes<'a>(views: &[(&'a ProjectedView, &PgStatement)]) -> Vec<&'a str> {
    use crate::journal::{PROJECTOR_NAME, ROW_PARTITION_PREFIX};

    let scopes: std::collections::BTreeSet<&str> = views
        .iter()
        .filter(|(view, _)| view.projector == PROJECTOR_NAME)
        .filter_map(|(view, _)| view.op.pk.strip_prefix(ROW_PARTITION_PREFIX))
        .collect();

    scopes.into_iter().collect()
}

/// The statement of one commit-wide view operation on the evt-owned views table `table`, with the
/// DynamoDB repository's owner rules (storage format: PostgreSQL views table, Commit target). It
/// refuses, before any write, what the table cannot store: U+0000 anywhere, a payload that is not
/// UTF-8, and a key too long for the btree indexes ([`Error::ViewKeyTooLong`]).
fn views_table_statement(table: &str, view: &ProjectedView) -> Result<PgStatement, Error> {
    use crate::wire::postgres::MAX_VIEW_INDEX_KEY_BYTES;

    let op = &view.op;
    let refuse = |reason: &str| {
        Error::InvalidViewOperation(format!(
            "projector {:?} (#{}) operation {}: {reason}",
            view.projector, view.projector_index, view.operation
        ))
    };
    let payload = std::str::from_utf8(&op.payload)
        .map_err(|_| refuse("the views table stores a UTF-8 payload"))?
        .to_owned();

    for (field, value) in [
        ("pk", op.pk.as_str()),
        ("sk", op.sk.as_str()),
        ("owner", op.owner.as_str()),
        ("entity type", op.entity_type.as_str()),
        ("payload", payload.as_str()),
    ] {
        if value.contains('\0') {
            return Err(refuse(&format!(
                "a PostgreSQL view {field} cannot contain U+0000"
            )));
        }
    }

    let key_bytes = op.entity_type.len() + op.pk.len() + op.sk.len();
    if key_bytes > MAX_VIEW_INDEX_KEY_BYTES {
        return Err(Error::ViewKeyTooLong {
            field: "entity type, pk and sk",
            length: key_bytes,
            max: MAX_VIEW_INDEX_KEY_BYTES,
            projector: view.projector.clone(),
            operation: view.operation,
        });
    }

    let (pk, sk, owner) = (op.pk.clone(), op.sk.clone(), op.owner.as_str().to_owned());
    let columns = "(pk, sk, entity_id, entity_type, payload, ttl)";
    let statement = match (op.kind, op.condition, &op.expected_payload) {
        // The journal's counter: replace exactly the row the commit read.
        (ViewOperationKind::Put, _, Some(expected)) => {
            let expected = String::from_utf8(expected.clone())
                .map_err(|_| refuse("an expected view payload must be UTF-8"))?;

            PgStatement::new(
                format!(
                    "UPDATE {table} SET entity_type = $4, payload = $5, ttl = 0 \
                     WHERE pk = $1 AND sk = $2 AND entity_id = $3 AND payload = $6"
                ),
                vec![
                    Box::new(pk),
                    Box::new(sk),
                    Box::new(owner),
                    Box::new(op.entity_type.clone()),
                    Box::new(payload),
                    Box::new(expected),
                ],
            )
        }

        // A plain insert: a violation of the key is the failed condition.
        (ViewOperationKind::Put, ViewCondition::Absent, None) => PgStatement::new(
            format!("INSERT INTO {table} {columns} VALUES ($1, $2, $3, $4, $5, 0)"),
            vec![
                Box::new(pk),
                Box::new(sk),
                Box::new(owner),
                Box::new(op.entity_type.clone()),
                Box::new(payload),
            ],
        )
        .with_key_constraint(format!("{table}_pkey")),

        // The owner's row must exist: an update of it, zero rows otherwise.
        (ViewOperationKind::Put, ViewCondition::Present, None) => PgStatement::new(
            format!(
                "UPDATE {table} SET entity_type = $4, payload = $5, ttl = 0 \
                 WHERE pk = $1 AND sk = $2 AND entity_id = $3"
            ),
            vec![
                Box::new(pk),
                Box::new(sk),
                Box::new(owner),
                Box::new(op.entity_type.clone()),
                Box::new(payload),
            ],
        ),

        // An upsert that replaces only the owner's row: zero rows when another stream owns it.
        (ViewOperationKind::Put, ViewCondition::None, None) => PgStatement::new(
            format!(
                "INSERT INTO {table} {columns} VALUES ($1, $2, $3, $4, $5, 0) \
                 ON CONFLICT (pk, sk) DO UPDATE SET entity_type = EXCLUDED.entity_type, \
                 payload = EXCLUDED.payload, ttl = 0 WHERE {table}.entity_id = EXCLUDED.entity_id"
            ),
            vec![
                Box::new(pk),
                Box::new(sk),
                Box::new(owner),
                Box::new(op.entity_type.clone()),
                Box::new(payload),
            ],
        ),

        // Deletes the owner's row; zero rows (absent, or another owner's) fails the condition.
        (ViewOperationKind::Delete, ViewCondition::Present, _) => PgStatement::new(
            format!("DELETE FROM {table} WHERE pk = $1 AND sk = $2 AND entity_id = $3"),
            vec![Box::new(pk), Box::new(sk), Box::new(owner)],
        ),

        // Deletes the owner's row if there is one, and reports another owner's row instead of the
        // rows it deleted: any row it reports fails the condition.
        (ViewOperationKind::Delete, _, _) => PgStatement::new(
            format!(
                "WITH gone AS (DELETE FROM {table} WHERE pk = $1 AND sk = $2 AND entity_id = $3) \
                 SELECT 1 FROM {table} WHERE pk = $1 AND sk = $2 AND entity_id <> $3"
            ),
            vec![Box::new(pk), Box::new(sk), Box::new(owner)],
        ),
    };
    Ok(statement)
}

/// Reads a view statement's affected-row count (see
/// [`with_view_writer`](PostgresRepository::with_view_writer)): a put without a presence condition
/// and a delete that requires the row affect no row when the owner's condition fails; a delete
/// without a condition reports (affects) a row only when another stream's holds the key; a put that
/// requires the row to be absent fails with a unique violation instead, mapped by [`view_error`].
fn view_condition_failed(op: &ViewOperation, affected: u64) -> bool {
    match (op.kind, op.condition) {
        (ViewOperationKind::Put, ViewCondition::Absent) => false,
        (ViewOperationKind::Put, _) | (ViewOperationKind::Delete, ViewCondition::Present) => {
            affected == 0
        }
        (ViewOperationKind::Delete, _) => affected > 0,
    }
}

/// Runs view statements in order. Each statement's outcome decides whether its operation's
/// condition held; any failure maps to the operation, and the transaction rolls back when it drops.
async fn run_view_statements(
    tx: &deadpool_postgres::Transaction<'_>,
    views: &[(&ProjectedView, &PgStatement)],
) -> Result<(), Error> {
    for (view, statement) in views {
        let params: Vec<&(dyn ToSql + Sync)> = statement
            .params
            .iter()
            .map(|param| param.as_ref() as &(dyn ToSql + Sync))
            .collect();
        let affected = tx
            .execute(&statement.sql, &params)
            .await
            .map_err(|err| view_error(view, statement.key_constraint.as_deref(), err))?;

        if view_condition_failed(&view.op, affected) {
            return Err(Error::ProjectorOperation(Box::new(
                ProjectorOperationError::condition_failed(view),
            )));
        }
    }

    Ok(())
}

/// Pairs each view operation with the statement its writer built.
fn view_writes<'a>(
    views: &'a [ProjectedView],
    statements: &'a [PgStatement],
) -> Vec<(&'a ProjectedView, &'a PgStatement)> {
    views.iter().zip(statements).collect()
}

/// Maps an error a view statement raised. A violation of the `(pk, sk)` key constraint on a put
/// that requires the row to be absent is its failed condition; a unique violation of any other
/// constraint is an ordinary failure of the statement. Any other error the database raised belongs
/// to the statement, so it is the operation's failure with the SQLSTATE as its reason, except a
/// transaction rollback (serialization failure, deadlock), which is not the projector's doing and
/// passes through as transient like for every other statement of the commit. A lost connection or
/// pool timeout is a backend failure.
fn view_error(
    view: &ProjectedView,
    key_constraint: Option<&str>,
    err: tokio_postgres::Error,
) -> Error {
    if transient(&err) {
        return Error::Transient(Box::new(err));
    }
    let Some(code) = err.code().map(|code| code.code().to_owned()) else {
        return Error::backend(err);
    };

    let condition_failed = err.code() == Some(&SqlState::UNIQUE_VIOLATION)
        && view.op.kind == ViewOperationKind::Put
        && view.op.condition == ViewCondition::Absent
        && key_constraint.is_some_and(|key| {
            err.as_db_error()
                .and_then(|db| db.constraint())
                .is_some_and(|violated| violated.eq_ignore_ascii_case(key))
        });
    let reason = if condition_failed {
        REASON_CONDITION_FAILED.to_owned()
    } else {
        code
    };

    Error::ProjectorOperation(Box::new(ProjectorOperationError::for_view(
        view,
        reason,
        Some(Box::new(err)),
    )))
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

/// Starts a transaction at `READ COMMITTED`, whatever the server's or role's
/// `default_transaction_isolation`. The engine's transactions read fence rows (the rebuild lock and
/// generation, stream heads, the feature manifest) after waiting for advisory locks, and only
/// `READ COMMITTED` takes each statement's snapshot after the wait: a `REPEATABLE READ` snapshot,
/// taken at the first statement, would miss a rebuild or a commit that landed while it waited.
async fn read_committed(
    client: &mut deadpool_postgres::Client,
) -> Result<deadpool_postgres::Transaction<'_>, Error> {
    client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
        .start()
        .await
        .map_err(Error::backend)
}

mod enumerate;
#[cfg(feature = "journal")]
mod journal;
#[cfg(feature = "journal")]
mod listener;
mod rebuild;
mod views;

#[cfg(feature = "journal")]
pub use listener::{DEFAULT_CONNECT_TIMEOUT, DEFAULT_POLL_INTERVAL, JournalListener, Wakeup};
pub use views::PgViewRepository;

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
    // ----- commit-wide view operations (v0.9, C7) -----

    fn event(id: &str, sequence: i64) -> SerializedEvent {
        SerializedEvent {
            id: format!("{id}:{sequence}").into(),
            sequence: EventSequence(sequence),
            event_type: "account:credited".into(),
            version: crate::EventVersion(1),
            entity_id: id.into(),
            entity_type: "account".into(),
            payload: b"{}".to_vec(),
            ..Default::default()
        }
    }

    fn view(operation: usize, op: ViewOperation) -> ProjectedView {
        ProjectedView::new("projector", 0, operation, op)
    }

    /// A writer that records the operations it was asked for, in order, as one `SELECT` each.
    fn recording_writer(seen: Arc<Mutex<Vec<String>>>) -> ViewWriter {
        Arc::new(move |operation: &ViewOperation| {
            seen.lock()
                .unwrap()
                .push(format!("{} {}", operation.pk, operation.sk));

            Ok(PgStatement::new("SELECT 1", Vec::new()))
        })
    }

    fn views(count: usize) -> Vec<ProjectedView> {
        (0..count)
            .map(|index| {
                view(
                    index,
                    ViewOperation::put("p", format!("row-{index:03}"), "o", "", "{}"),
                )
            })
            .collect()
    }

    /// Views are refused without a writer, built before the budget, and budgeted by their SQL
    /// text, all before the first request: the pool of this repository could not serve one.
    #[tokio::test]
    async fn views_are_built_and_budgeted_before_any_request() {
        let commit = |views| Commit::<PgStatement>::new("a", vec![event("a", 1)]).with_views(views);
        let transaction = |views| {
            TransactionCommit::<PgStatement>::new(vec![Commit::new("a", vec![event("a", 1)])])
                .with_views(views)
        };

        // No writer.
        let bare = repository();
        for result in [
            bare.commit(commit(views(1))).await,
            bare.commit_transaction(transaction(views(1))).await,
        ] {
            assert!(
                matches!(
                    result,
                    Err(Error::ViewsNotConfigured {
                        backend: "postgres"
                    })
                ),
                "{result:?}"
            );
        }

        // The writer is asked for every operation, in declaration then emission order, before the
        // budget is checked: 97 statements are one action too many beside the two control checks,
        // the head update, and the event.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let repository = repository().with_view_writer(recording_writer(seen.clone()));
        let result = repository.commit(commit(views(97))).await;
        assert!(
            matches!(result, Err(Error::CommitTooLarge { actions: 101, .. })),
            "{result:?}"
        );
        assert_eq!(seen.lock().unwrap().len(), 97);
        assert_eq!(seen.lock().unwrap()[0], "p row-000");
        assert_eq!(seen.lock().unwrap()[96], "p row-096");

        let result = repository.commit_transaction(transaction(views(97))).await;
        assert!(
            matches!(result, Err(Error::CommitTooLarge { actions: 101, .. })),
            "{result:?}"
        );

        // 96 fit the budget (the commit then needs a database, which this pool lacks).
        let result = repository.commit(commit(views(96))).await;
        assert!(
            !matches!(result, Err(Error::CommitTooLarge { .. })),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn a_view_statement_is_sized_by_its_sql_text() {
        let statement_of = |size: usize| -> ViewWriter {
            Arc::new(move |_: &ViewOperation| {
                Ok(PgStatement::new(
                    format!("SELECT '{}'", "x".repeat(size)),
                    Vec::new(),
                ))
            })
        };

        // The bound parameters are opaque: only the SQL text counts toward the bytes.
        let commit = Commit::<PgStatement>::new("a", vec![event("a", 1)]).with_views(views(12));
        let result = repository()
            .with_view_writer(statement_of(330_000))
            .commit(commit)
            .await;
        match result {
            Err(Error::CommitTooLarge {
                actions,
                bytes,
                largest_item,
            }) => {
                assert_eq!(actions, 16);
                assert!(
                    bytes > 3_800_000 && largest_item < 380_000,
                    "{bytes} {largest_item}"
                );
            }
            other => panic!("expected CommitTooLarge, got {other:?}"),
        }

        let commit = Commit::<PgStatement>::new("a", vec![event("a", 1)]).with_views(views(1));
        let result = repository()
            .with_view_writer(statement_of(400_000))
            .commit(commit)
            .await;
        assert!(
            matches!(&result, Err(Error::CommitTooLarge { largest_item, .. }) if *largest_item > 380_000),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn a_writer_that_refuses_or_returns_nothing_fails_the_commit_before_any_write() {
        let commit = || Commit::<PgStatement>::new("a", vec![event("a", 1)]).with_views(views(2));

        let empty: ViewWriter =
            Arc::new(|_: &ViewOperation| Ok(PgStatement::new(" \n", Vec::new())));
        let result = repository().with_view_writer(empty).commit(commit()).await;
        assert!(
            matches!(&result, Err(Error::InvalidViewOperation(message)) if message.contains("operation 0")),
            "{result:?}"
        );

        let refusing: ViewWriter = Arc::new(|operation: &ViewOperation| {
            if operation.sk == "row-001" {
                return Err(Error::InvalidArgument("no table for that row".into()));
            }

            Ok(PgStatement::new("SELECT 1", Vec::new()))
        });
        let result = repository()
            .with_view_writer(refusing)
            .commit(commit())
            .await;
        assert!(
            matches!(result, Err(Error::InvalidArgument(_))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn views_are_checked_before_the_writer_runs() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let repository = repository().with_view_writer(recording_writer(seen.clone()));

        let long = vec![view(
            0,
            ViewOperation::put("p".repeat(2049), "s", "o", "", "{}"),
        )];
        let result = repository
            .commit(Commit::<PgStatement>::new("a", vec![event("a", 1)]).with_views(long))
            .await;
        assert!(
            matches!(result, Err(Error::ViewKeyTooLong { .. })),
            "{result:?}"
        );

        let repeated = vec![
            view(0, ViewOperation::put("p", "s", "o", "", "{}")),
            view(1, ViewOperation::delete("p", "s", "o")),
        ];
        let result = repository
            .commit_transaction(
                TransactionCommit::<PgStatement>::new(vec![Commit::new("a", vec![event("a", 1)])])
                    .with_views(repeated),
            )
            .await;
        assert!(
            matches!(result, Err(Error::DuplicateViewKey { .. })),
            "{result:?}"
        );

        let orphan = repository
            .commit(Commit::<PgStatement>::new("a", Vec::new()).with_views(views(1)))
            .await;
        assert!(
            matches!(orphan, Err(Error::ProjectorWithoutEvents { operations: 1 })),
            "{orphan:?}"
        );
        let orphan = repository
            .commit_transaction(
                TransactionCommit::<PgStatement>::new(Vec::new()).with_views(views(1)),
            )
            .await;
        assert!(
            matches!(orphan, Err(Error::ProjectorWithoutEvents { operations: 1 })),
            "{orphan:?}"
        );

        assert!(seen.lock().unwrap().is_empty(), "the writer never ran");
    }
}
