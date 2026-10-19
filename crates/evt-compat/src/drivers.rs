//! Connections to the local DynamoDB emulator and PostgreSQL, configured from the same environment
//! variables as the Go integration tests.

use crate::scenario::{HeadAccess, HeadFuture, ViewAccess};
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_dynamodb::types::AttributeValue;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use photon_evt::conformance::HeadFixtures;
use photon_evt::dynamodb::DynamoRepository;
use photon_evt::postgres::{PgStatement, PostgresRepository, ViewWriter};
use photon_evt::wire::postgres::{DEFAULT_EVENTS_TABLE, EventRow, stream_heads_table};
use photon_evt::{
    BoxError, EntityId, Error, EventSequence, Repository, SerializedEvent, ViewCondition,
    ViewOperation, ViewOperationKind, ViewRow,
};
use std::time::Duration;

/// The local event-log table (`EVT_COMPAT_DYNAMO_TABLE`).
pub fn dynamo_table() -> String {
    std::env::var("EVT_COMPAT_DYNAMO_TABLE").unwrap_or_else(|_| "evt-local-event-log".into())
}

/// The local views table (`EVT_COMPAT_DYNAMO_VIEWS_TABLE`, as the Go driver reads it; the older
/// `EVT_COMPAT_VIEWS_TABLE` is still honored). The commit-wide view operations of the scenarios
/// write to it, and it needs a string partition key `pk` and a string sort key `sk`.
pub fn views_table() -> String {
    std::env::var("EVT_COMPAT_DYNAMO_VIEWS_TABLE")
        .or_else(|_| std::env::var("EVT_COMPAT_VIEWS_TABLE"))
        .unwrap_or_else(|_| "evt-local-entity-views".into())
}

/// The longest one DynamoDB call may take, retries included. The SDK sets no deadline by default,
/// so an emulator that stops answering would otherwise hold a test (and the CI job) until its
/// timeout.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// A DynamoDB client for `AWS_ENDPOINT_URL` (default `http://localhost:4566`) in `us-west-2`.
///
/// Calls are bounded ([`CALL_TIMEOUT`]) and never retried, like the Go integration client: an
/// emulator error fails the test where it happened instead of being retried into a hang.
pub async fn dynamo_client() -> aws_sdk_dynamodb::Client {
    let endpoint =
        std::env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| "http://localhost:4566".into());
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-west-2"))
        .endpoint_url(endpoint)
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(5))
                .operation_timeout(CALL_TIMEOUT)
                .build(),
        )
        .load()
        .await;

    aws_sdk_dynamodb::Client::new(&config)
}

/// A repository over the local event-log table, writing commit-wide view operations to the local
/// views table.
pub async fn dynamo() -> DynamoRepository {
    DynamoRepository::new(dynamo_client().await, dynamo_table()).with_views_table(views_table())
}

/// A connection pool for `DATABASE_URL` (default: the `infra/local-postgres` database).
pub fn postgres_pool() -> Result<Pool, BoxError> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://evt:evt@localhost:5432/evt_local?sslmode=disable".into());
    let config: tokio_postgres::Config = url.parse()?;
    let manager = Manager::from_config(
        config,
        tokio_postgres::NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );

    Ok(Pool::builder(manager).max_size(4).build()?)
}

/// The application-owned table the live PostgreSQL runs write commit-wide view rows to. evt
/// defines no view schema; the drivers of both languages create this one.
pub const POSTGRES_VIEW_TABLE: &str = "evt_compat_views";

/// The name of [`POSTGRES_VIEW_TABLE`]'s primary key, the constraint whose violation means a row
/// already exists.
pub const POSTGRES_VIEW_KEY_CONSTRAINT: &str = "evt_compat_views_pkey";

/// Creates [`POSTGRES_VIEW_TABLE`]; the twin of Go's `compat.PostgresViewDDL`.
pub const POSTGRES_VIEW_DDL: &str = "CREATE TABLE IF NOT EXISTS evt_compat_views (
    pk          text NOT NULL,
    sk          text NOT NULL,
    entity_id   text NOT NULL,
    entity_type text NOT NULL,
    payload     text NOT NULL,
    PRIMARY KEY (pk, sk)
)";

/// Writes a view operation to [`POSTGRES_VIEW_TABLE`] with the statement semantics the repository
/// relies on, enforcing the row's owner (its `entity_id`) in SQL: a put that requires the row to be
/// absent is a plain `INSERT` (the key-constraint violation is its failed condition), any other put
/// is an upsert that only the owner may overwrite (it affects no row for another stream's row), a
/// delete that requires the row removes the owner's row (affecting nothing fails it), and any other
/// delete removes the owner's row if there is one and reports the rows of another owner it found,
/// which fail it. The twin of Go's `compat.PostgresViewWriter`.
pub fn postgres_view_writer() -> ViewWriter {
    std::sync::Arc::new(|operation: &ViewOperation| {
        if operation.kind == ViewOperationKind::Delete {
            let sql = if operation.condition == ViewCondition::Present {
                "DELETE FROM evt_compat_views WHERE pk = $1 AND sk = $2 AND entity_id = $3"
            } else {
                "WITH gone AS (DELETE FROM evt_compat_views \
                 WHERE pk = $1 AND sk = $2 AND entity_id = $3) \
                 SELECT 1 FROM evt_compat_views WHERE pk = $1 AND sk = $2 AND entity_id <> $3"
            };

            return Ok(PgStatement::new(
                sql,
                vec![
                    Box::new(operation.pk.clone()),
                    Box::new(operation.sk.clone()),
                    Box::new(operation.owner.to_string()),
                ],
            ));
        }

        let payload = String::from_utf8(operation.payload.clone())
            .map_err(|err| Error::InvalidViewOperation(format!("payload: {err}")))?;
        let insert = "INSERT INTO evt_compat_views (pk, sk, entity_id, entity_type, payload) \
                      VALUES ($1, $2, $3, $4, $5)";
        let absent = operation.condition == ViewCondition::Absent;
        let sql = if absent {
            insert.to_owned()
        } else {
            format!(
                "{insert} ON CONFLICT (pk, sk) DO UPDATE SET entity_type = EXCLUDED.entity_type, \
                 payload = EXCLUDED.payload WHERE evt_compat_views.entity_id = EXCLUDED.entity_id"
            )
        };

        Ok(PgStatement::new(
            sql,
            vec![
                Box::new(operation.pk.clone()),
                Box::new(operation.sk.clone()),
                Box::new(operation.owner.to_string()),
                Box::new(operation.entity_type.clone()),
                Box::new(payload),
            ],
        )
        .with_key_constraint(POSTGRES_VIEW_KEY_CONSTRAINT))
    })
}

/// A repository over the default tables, creating the schema once per process to avoid redundant
/// DDL. The repository also serializes schema setup across processes with an advisory lock. It
/// writes commit-wide view operations to [`POSTGRES_VIEW_TABLE`], created if absent.
pub async fn postgres() -> Result<PostgresRepository, BoxError> {
    static SCHEMA: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

    let repository =
        PostgresRepository::new(postgres_pool()?).with_view_writer(postgres_view_writer());
    SCHEMA
        .get_or_init(|| async {
            repository.ensure_schema().await.expect("create the schema");
            repository
                .pool()
                .get()
                .await
                .expect("connect")
                .batch_execute(POSTGRES_VIEW_DDL)
                .await
                .expect("create the view table");
        })
        .await;

    Ok(repository)
}

/// Read access to the commit-wide view rows in a DynamoDB views table.
#[derive(Clone, Debug)]
pub struct DynamoViews {
    client: aws_sdk_dynamodb::Client,
    table: String,
}

impl DynamoViews {
    /// Access to a views table through a client.
    pub fn new(client: aws_sdk_dynamodb::Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
        }
    }

    /// Access to a repository's views table.
    pub fn of(repository: &DynamoRepository) -> Self {
        Self::new(
            repository.client().clone(),
            repository.views_table().expect("a views table"),
        )
    }
}

impl ViewAccess for DynamoViews {
    /// A consistent `Query` of the partition. Each item must be exactly the five string
    /// attributes of a view row, so a stray or mistyped attribute fails the read.
    fn rows<'a>(&'a self, pk: &'a str) -> HeadFuture<'a, Vec<ViewRow>> {
        Box::pin(async move {
            let mut rows = Vec::new();
            let mut start = None;

            loop {
                let page = self
                    .client
                    .query()
                    .table_name(&self.table)
                    .consistent_read(true)
                    .key_condition_expression("pk = :pk")
                    .expression_attribute_values(":pk", AttributeValue::S(pk.into()))
                    .set_exclusive_start_key(start)
                    .send()
                    .await
                    .map_err(Error::backend)?;

                for item in page.items() {
                    let text = |name: &str| -> Result<String, Error> {
                        match item.get(name) {
                            Some(AttributeValue::S(value)) => Ok(value.clone()),
                            other => Err(Error::InvalidRecord(format!(
                                "view item attribute {name} is {other:?}, not a string"
                            ))),
                        }
                    };
                    if item.len() != 5 {
                        return Err(Error::InvalidRecord(format!(
                            "view item has {} attributes, want the five of a view row",
                            item.len()
                        )));
                    }

                    rows.push(ViewRow::new(
                        text("pk")?,
                        text("sk")?,
                        text("entityID")?,
                        text("entityType")?,
                        text("payload")?.into_bytes(),
                    ));
                }

                match page.last_evaluated_key {
                    Some(key) if !key.is_empty() => start = Some(key),
                    _ => return Ok(rows),
                }
            }
        })
    }
}

/// Read access to the commit-wide view rows in [`POSTGRES_VIEW_TABLE`].
#[derive(Clone)]
pub struct PostgresViews {
    pool: Pool,
}

impl PostgresViews {
    /// Access to the view table through a pool.
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl ViewAccess for PostgresViews {
    fn rows<'a>(&'a self, pk: &'a str) -> HeadFuture<'a, Vec<ViewRow>> {
        Box::pin(async move {
            let rows = self
                .pool
                .get()
                .await
                .map_err(Error::backend)?
                .query(
                    "SELECT pk, sk, entity_id, entity_type, payload FROM evt_compat_views \
                     WHERE pk = $1 ORDER BY sk COLLATE \"C\"",
                    &[&pk],
                )
                .await
                .map_err(Error::backend)?;

            Ok(rows
                .into_iter()
                .map(|row| {
                    ViewRow::new(
                        row.get::<_, String>(0),
                        row.get::<_, String>(1),
                        row.get::<_, String>(2),
                        row.get::<_, String>(3),
                        row.get::<_, String>(4).into_bytes(),
                    )
                })
                .collect())
        })
    }
}

/// Raw access to the DynamoDB head items and event items, for the steps and cases that put a
/// stream in a state no conforming writer can (ADR 0007): an event stored without its head update,
/// a head item deleted, a stored head read back.
#[derive(Clone, Debug)]
pub struct DynamoHeads {
    client: aws_sdk_dynamodb::Client,
    table: String,
}

impl DynamoHeads {
    /// Raw access to a table through a client.
    pub fn new(client: aws_sdk_dynamodb::Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
        }
    }

    /// Raw access to a repository's table.
    pub fn of(repository: &DynamoRepository) -> Self {
        Self::new(repository.client().clone(), repository.table())
    }
}

impl<R: Repository> HeadFixtures<R> for DynamoHeads {
    /// A `PutItem` of the event item with `attribute_not_exists(sk)`, the item Go's
    /// `DynamoBypassHead` writes: `pk`, `sk`, `type`, `version`, `entityType`, `payload`, and
    /// `metadata`.
    async fn append_bypassing_head(&self, event: &SerializedEvent) -> Result<(), Error> {
        let item = photon_evt::wire::dynamo::event_item(event, None)?;

        self.client
            .put_item()
            .table_name(&self.table)
            .set_item(Some(photon_evt::dynamodb::to_sdk_item(&item)))
            .condition_expression("attribute_not_exists(sk)")
            .send()
            .await
            .map_err(Error::backend)?;

        Ok(())
    }

    async fn delete_head(&self, entity_id: &EntityId) -> Result<(), Error> {
        self.client
            .delete_item()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(entity_id.to_string()))
            .key("sk", AttributeValue::N("-1".into()))
            .send()
            .await
            .map_err(Error::backend)?;

        Ok(())
    }

    async fn stored_head(&self, entity_id: &EntityId) -> Result<Option<EventSequence>, Error> {
        let output = self
            .client
            .get_item()
            .table_name(&self.table)
            .consistent_read(true)
            .key("pk", AttributeValue::S(entity_id.to_string()))
            .key("sk", AttributeValue::N("-1".into()))
            .send()
            .await
            .map_err(Error::backend)?;

        output
            .item
            .map(|item| {
                photon_evt::wire::dynamo::head_from_item(&photon_evt::dynamodb::from_sdk_item(
                    &item,
                ))
                .map(|(head, _)| head)
            })
            .transpose()
    }
}

/// Raw access to the PostgreSQL events and heads tables, for the same purpose as [`DynamoHeads`].
#[derive(Clone)]
pub struct PostgresHeads {
    pool: Pool,
    events: String,
}

impl PostgresHeads {
    /// Raw access to the tables of an event log named by its events table.
    pub fn new(pool: Pool, events: impl Into<String>) -> Self {
        Self {
            pool,
            events: events.into(),
        }
    }

    /// Raw access to the default tables.
    pub fn default_tables(pool: Pool) -> Self {
        Self::new(pool, DEFAULT_EVENTS_TABLE)
    }
}

impl<R: Repository> HeadFixtures<R> for PostgresHeads {
    /// A direct `INSERT` of the event row, leaving the heads table untouched.
    async fn append_bypassing_head(&self, event: &SerializedEvent) -> Result<(), Error> {
        let row = EventRow::from_event(event)?;

        self.pool
            .get()
            .await
            .map_err(Error::backend)?
            .execute(
                &format!(
                    "INSERT INTO {} \
                     (entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text::jsonb)",
                    self.events
                ),
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
            .map_err(Error::backend)?;

        Ok(())
    }

    async fn delete_head(&self, entity_id: &EntityId) -> Result<(), Error> {
        self.pool
            .get()
            .await
            .map_err(Error::backend)?
            .execute(
                &format!(
                    "DELETE FROM {} WHERE entity_id = $1",
                    stream_heads_table(&self.events)
                ),
                &[&entity_id.as_str()],
            )
            .await
            .map_err(Error::backend)?;

        Ok(())
    }

    async fn stored_head(&self, entity_id: &EntityId) -> Result<Option<EventSequence>, Error> {
        let row = self
            .pool
            .get()
            .await
            .map_err(Error::backend)?
            .query_opt(
                &format!(
                    "SELECT head FROM {} WHERE entity_id = $1",
                    stream_heads_table(&self.events)
                ),
                &[&entity_id.as_str()],
            )
            .await
            .map_err(Error::backend)?;

        Ok(row.map(|row| EventSequence(row.get(0))))
    }
}

impl HeadAccess for DynamoHeads {
    fn append_bypassing_head<'a>(&'a self, event: &'a SerializedEvent) -> HeadFuture<'a, ()> {
        Box::pin(HeadFixtures::<DynamoRepository>::append_bypassing_head(
            self, event,
        ))
    }

    fn stored_head<'a>(&'a self, entity_id: &'a EntityId) -> HeadFuture<'a, Option<EventSequence>> {
        Box::pin(HeadFixtures::<DynamoRepository>::stored_head(
            self, entity_id,
        ))
    }
}

impl HeadAccess for PostgresHeads {
    fn append_bypassing_head<'a>(&'a self, event: &'a SerializedEvent) -> HeadFuture<'a, ()> {
        Box::pin(HeadFixtures::<PostgresRepository>::append_bypassing_head(
            self, event,
        ))
    }

    fn stored_head<'a>(&'a self, entity_id: &'a EntityId) -> HeadFuture<'a, Option<EventSequence>> {
        Box::pin(HeadFixtures::<PostgresRepository>::stored_head(
            self, entity_id,
        ))
    }
}
