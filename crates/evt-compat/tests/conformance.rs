//! Runs the `photon-evt` conformance suite against the local DynamoDB emulator and PostgreSQL.
//! Needs the `drivers` feature and the `infra/local` and `infra/local-postgres` stacks:
//!
//! ```sh
//! cargo test -p evt-compat --features drivers --test conformance
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem};
use deadpool_postgres::Pool;
use evt_compat::drivers;
use photon_evt::conformance::{Add, Ledger, LedgerEvent, Suite, ViewTarget};
use photon_evt::dynamodb::{DynamoRepository, ViewProjector};
use photon_evt::postgres::{PgStatement, PostgresRepository};
use photon_evt::wire::dynamo::DEFAULT_VIEW_SORT_KEY;
use photon_evt::wire::postgres::ENTITY_LOCK;
use photon_evt::{
    Aggregate, BoxError, Commit, EntityId, Error, EventId, EventSequence, EventType, EventVersion,
    Metadata, Projector, Repository, RetryPolicy, SerializedEvent, SerializedSnapshot, Store, json,
};

/// Reads the single-item views `ViewProjector` writes.
struct DynamoViews {
    client: aws_sdk_dynamodb::Client,
    table: String,
}

impl ViewTarget<DynamoRepository> for DynamoViews {
    type Projector = ViewProjector;

    fn projector(&self) -> ViewProjector {
        ViewProjector::new(&self.table)
    }

    async fn view(&self, entity_id: &EntityId) -> Result<Option<Ledger>, BoxError> {
        let output = self
            .client
            .get_item()
            .table_name(&self.table)
            .consistent_read(true)
            .key(
                "pk",
                AttributeValue::S(format!("{}#{entity_id}", Ledger::TYPE)),
            )
            .key("sk", AttributeValue::S(DEFAULT_VIEW_SORT_KEY.into()))
            .send()
            .await?;

        let Some(item) = output.item else {
            return Ok(None);
        };
        let payload = item
            .get("payload")
            .and_then(|value| value.as_s().ok())
            .ok_or("view item has no string payload")?;

        Ok(Some(serde_json::from_str(payload)?))
    }
}

const PG_VIEWS_TABLE: &str = "evt_conformance_views";

/// Upserts the ledger JSON into a test view table in the commit's transaction.
struct PgViewProjector;

impl Projector<Ledger, PgStatement> for PgViewProjector {
    fn project(
        &self,
        entity_id: &EntityId,
        ledger: &Ledger,
        _: &[LedgerEvent],
    ) -> Result<Vec<PgStatement>, BoxError> {
        Ok(vec![PgStatement::new(
            format!(
                "INSERT INTO {PG_VIEWS_TABLE} (entity_id, payload) VALUES ($1, $2)
                 ON CONFLICT (entity_id) DO UPDATE SET payload = EXCLUDED.payload"
            ),
            vec![
                Box::new(entity_id.to_string()),
                Box::new(json::to_string(ledger)?),
            ],
        )])
    }
}

struct PostgresViews {
    pool: Pool,
}

impl PostgresViews {
    async fn new(pool: Pool) -> Result<Self, BoxError> {
        pool.get()
            .await?
            .batch_execute(&format!(
                "CREATE TABLE IF NOT EXISTS {PG_VIEWS_TABLE} (entity_id text PRIMARY KEY, payload text NOT NULL)"
            ))
            .await?;

        Ok(Self { pool })
    }
}

impl ViewTarget<PostgresRepository> for PostgresViews {
    type Projector = PgViewProjector;

    fn projector(&self) -> PgViewProjector {
        PgViewProjector
    }

    async fn view(&self, entity_id: &EntityId) -> Result<Option<Ledger>, BoxError> {
        let row = self
            .pool
            .get()
            .await?
            .query_opt(
                &format!("SELECT payload FROM {PG_VIEWS_TABLE} WHERE entity_id = $1"),
                &[&entity_id.as_str()],
            )
            .await?;

        row.map(|row| serde_json::from_str(row.get::<_, &str>(0)))
            .transpose()
            .map_err(Into::into)
    }
}

// One test, run in sequence: Moto rolls back a failed transaction by restoring a copy of every
// table it touched, which drops writes that a concurrent test made in the meantime. Real DynamoDB
// does not, but the projector-condition case deliberately fails a transaction.
#[tokio::test]
async fn dynamo_conforms() {
    let repository = drivers::dynamo().await;
    let views = DynamoViews {
        client: repository.client().clone(),
        table: drivers::views_table(),
    };

    Suite::new(&repository).with_views(views).run().await;
    projector_condition_failure_is_not_a_conflict(&repository).await;
}

/// Writes a view only if it already exists, so its condition fails for a new entity.
struct RequireExistingView {
    table: String,
}

impl Projector<Ledger, TransactWriteItem> for RequireExistingView {
    fn project(
        &self,
        entity_id: &EntityId,
        _: &Ledger,
        _: &[LedgerEvent],
    ) -> Result<Vec<TransactWriteItem>, BoxError> {
        let put = Put::builder()
            .table_name(&self.table)
            .item("pk", AttributeValue::S(format!("missing#{entity_id}")))
            .item("sk", AttributeValue::S(DEFAULT_VIEW_SORT_KEY.into()))
            .condition_expression("attribute_exists(pk)")
            .build()?;

        Ok(vec![TransactWriteItem::builder().put(put).build()])
    }
}

async fn projector_condition_failure_is_not_a_conflict(repository: &DynamoRepository) {
    let store = Store::<Ledger, _>::new(repository, ()).with_projector(RequireExistingView {
        table: drivers::views_table(),
    });
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = EntityId::new(format!("conf-{nanos}-projector-condition"));

    let err = store
        .execute_with_retry(
            &id,
            Add(1),
            Metadata::new("local"),
            &RetryPolicy::immediate(3),
            |_| async {},
        )
        .await
        .expect_err("the projector condition fails");

    assert!(
        matches!(err.as_store_error(), Some(Error::Projector(_))),
        "want a projector error, got {err:?}"
    );
    assert!(repository.events(&id).await.unwrap().is_empty());
}

/// Fails the first commit with a deadlock (`40P01`), the way PostgreSQL aborts a deadlock victim.
struct DeadlockOnce {
    raised: AtomicBool,
}

impl Projector<Ledger, PgStatement> for DeadlockOnce {
    fn project(
        &self,
        _: &EntityId,
        _: &Ledger,
        _: &[LedgerEvent],
    ) -> Result<Vec<PgStatement>, BoxError> {
        if self.raised.swap(true, Ordering::SeqCst) {
            return Ok(Vec::new());
        }

        Ok(vec![PgStatement::new(
            "DO $$ BEGIN RAISE EXCEPTION 'deadlock' USING ERRCODE = '40P01'; END $$",
            Vec::new(),
        )])
    }
}

#[tokio::test]
async fn postgres_projector_deadlock_is_retried() {
    let repository = drivers::postgres().await.expect("connect to PostgreSQL");
    let store = Store::<Ledger, _>::new(&repository, ()).with_projector(DeadlockOnce {
        raised: AtomicBool::new(false),
    });
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = EntityId::new(format!("conf-{nanos}-projector-deadlock"));

    let executed = store
        .execute_with_retry(
            &id,
            Add(1),
            Metadata::new("local"),
            &RetryPolicy::immediate(2),
            |_| async {},
        )
        .await
        .expect("the retry succeeds after the deadlock");

    assert_eq!(executed.state.sequence().get(), 1);
    assert_eq!(repository.events(&id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn postgres_snapshot_deadlock_is_retried() {
    // Dedicated tables, so the trigger never touches the shared ones. The trigger uses a sequence
    // rather than a flag row, because a flag row would roll back with the failed transaction.
    let repository = PostgresRepository::new(drivers::postgres_pool().expect("pool"))
        .with_tables("evt_deadlock_events", "evt_deadlock_snapshots")
        .expect("valid table names");
    repository.ensure_schema().await.expect("create the tables");
    repository
        .pool()
        .get()
        .await
        .expect("connect to PostgreSQL")
        .batch_execute(
            "DROP SEQUENCE IF EXISTS evt_deadlock_attempts;
             CREATE SEQUENCE evt_deadlock_attempts;
             CREATE OR REPLACE FUNCTION evt_deadlock_once() RETURNS trigger AS $$
             BEGIN
                 IF nextval('evt_deadlock_attempts') = 1 THEN
                     RAISE EXCEPTION 'deadlock' USING ERRCODE = '40P01';
                 END IF;
                 RETURN NEW;
             END $$ LANGUAGE plpgsql;
             CREATE OR REPLACE TRIGGER evt_deadlock_once BEFORE INSERT OR UPDATE
                 ON evt_deadlock_snapshots FOR EACH ROW EXECUTE FUNCTION evt_deadlock_once();",
        )
        .await
        .expect("install the trigger");

    let store = Store::<Ledger, _>::new(&repository, ()).with_snapshot_interval(1);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = EntityId::new(format!("conf-{nanos}-snapshot-deadlock"));

    let executed = store
        .execute_with_retry(
            &id,
            Add(1),
            Metadata::new("local"),
            &RetryPolicy::immediate(2),
            |_| async {},
        )
        .await
        .expect("the retry succeeds after the deadlock");

    assert_eq!(executed.state.snapshot_sequence().get(), 1);
    assert!(repository.snapshot(&id).await.unwrap().is_some());
}

/// Inserts a row that already exists into a table whose unique constraint is checked at `COMMIT`.
struct DeferredDuplicate;

impl Projector<Ledger, PgStatement> for DeferredDuplicate {
    fn project(
        &self,
        _: &EntityId,
        _: &Ledger,
        _: &[LedgerEvent],
    ) -> Result<Vec<PgStatement>, BoxError> {
        Ok(vec![PgStatement::new(
            "INSERT INTO evt_deferred_views (key) VALUES ('taken')",
            Vec::new(),
        )])
    }
}

#[tokio::test]
async fn postgres_deferred_projector_violation_is_not_a_conflict() {
    let repository = drivers::postgres().await.expect("connect to PostgreSQL");
    repository
        .pool()
        .get()
        .await
        .expect("connect to PostgreSQL")
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS evt_deferred_views (
                 key text,
                 CONSTRAINT evt_deferred_views_key UNIQUE (key) DEFERRABLE INITIALLY DEFERRED
             );
             INSERT INTO evt_deferred_views (key)
                 SELECT 'taken' WHERE NOT EXISTS (SELECT 1 FROM evt_deferred_views);",
        )
        .await
        .expect("create the view table");

    let store = Store::<Ledger, _>::new(&repository, ()).with_projector(DeferredDuplicate);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = EntityId::new(format!("conf-{nanos}-deferred-projector"));

    let err = store
        .execute_with_retry(
            &id,
            Add(1),
            Metadata::new("local"),
            &RetryPolicy::immediate(3),
            |_| async {},
        )
        .await
        .expect_err("the deferred constraint fails at commit");

    assert!(
        matches!(err.as_store_error(), Some(Error::Projector(_))),
        "want a projector error, got {err:?}"
    );
    assert!(repository.events(&id).await.unwrap().is_empty());
}

/// Commits and snapshot writes for one entity take the same advisory lock, so a snapshot cannot
/// raise the floor under a commit that already passed its floor check. The other language runs the
/// same statement, so holding it here must block both kinds of write.
#[tokio::test]
async fn postgres_writes_wait_for_the_entity_lock() {
    let repository = drivers::postgres().await.expect("connect to PostgreSQL");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = EntityId::new(format!("conf-{nanos}-entity-lock"));

    let mut holder = repository
        .pool()
        .get()
        .await
        .expect("connect to PostgreSQL");
    let lock = holder.transaction().await.expect("begin");
    lock.execute(ENTITY_LOCK, &[&id.as_str()])
        .await
        .expect("take the lock");

    let snapshot = SerializedSnapshot {
        entity_type: Ledger::TYPE.into(),
        entity_id: id.clone(),
        sequence: EventSequence(1),
        event_sequence: EventSequence(10),
        payload: b"{}".to_vec(),
    };
    let event = SerializedEvent {
        id: EventId::for_event(&id, EventSequence(1)),
        sequence: EventSequence(1),
        event_type: EventType::new("ledger:added"),
        version: EventVersion(1),
        entity_id: id.clone(),
        entity_type: Ledger::TYPE.into(),
        payload: br#"{"amount":1}"#.to_vec(),
        metadata: Metadata::new("local"),
    };

    let put = tokio::spawn({
        let repository = repository.clone();
        async move { repository.put_snapshot(&snapshot).await }
    });
    let commit = tokio::spawn({
        let repository = repository.clone();
        async move { repository.commit(Commit::new(id, vec![event])).await }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!put.is_finished(), "put_snapshot waits for the entity lock");
    assert!(!commit.is_finished(), "commit waits for the entity lock");

    lock.rollback().await.expect("release the lock");

    assert!(put.await.unwrap().expect("the snapshot is written"));
    // Either order is consistent: the commit lands first, or the floor check rejects it.
    let committed = commit.await.unwrap();
    assert!(
        matches!(committed, Ok(()) | Err(Error::Conflict { .. })),
        "{committed:?}"
    );
}

#[tokio::test]
async fn postgres_conforms() {
    let repository = drivers::postgres().await.expect("connect to PostgreSQL");
    let views = PostgresViews::new(repository.pool().clone())
        .await
        .expect("create the view table");

    // PostgreSQL does not check snapshot sequences: the event-sequence conflict already rejects
    // racing writers, and the snapshot upsert keeps the event sequence monotonic.
    Suite::new(&repository)
        .snapshot_sequence_conflicts(false)
        .with_views(views)
        .run()
        .await;
}
