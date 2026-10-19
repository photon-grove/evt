//! Connections to the local DynamoDB emulator and PostgreSQL, configured from the same environment
//! variables as the Go integration tests.

use crate::scenario::{HeadAccess, HeadFuture};
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_dynamodb::types::AttributeValue;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use photon_evt::conformance::HeadFixtures;
use photon_evt::dynamodb::DynamoRepository;
use photon_evt::postgres::PostgresRepository;
use photon_evt::wire::postgres::{DEFAULT_EVENTS_TABLE, EventRow, stream_heads_table};
use photon_evt::{BoxError, EntityId, Error, EventSequence, Repository, SerializedEvent};
use std::time::Duration;

/// The local event-log table (`EVT_COMPAT_DYNAMO_TABLE`).
pub fn dynamo_table() -> String {
    std::env::var("EVT_COMPAT_DYNAMO_TABLE").unwrap_or_else(|_| "evt-local-event-log".into())
}

/// The local views table (`EVT_COMPAT_VIEWS_TABLE`).
pub fn views_table() -> String {
    std::env::var("EVT_COMPAT_VIEWS_TABLE").unwrap_or_else(|_| "evt-local-entity-views".into())
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

/// A repository over the local event-log table.
pub async fn dynamo() -> DynamoRepository {
    DynamoRepository::new(dynamo_client().await, dynamo_table())
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

/// A repository over the default tables, creating the schema once per process to avoid redundant
/// DDL. The repository also serializes schema setup across processes with an advisory lock.
pub async fn postgres() -> Result<PostgresRepository, BoxError> {
    static SCHEMA: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

    let repository = PostgresRepository::new(postgres_pool()?);
    SCHEMA
        .get_or_init(|| async { repository.ensure_schema().await.expect("create the schema") })
        .await;

    Ok(repository)
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
