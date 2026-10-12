//! Runs the `photon-evt` conformance suite against the local DynamoDB emulator and PostgreSQL.
//! Needs the `drivers` feature and the `infra/local` and `infra/local-postgres` stacks:
//!
//! ```sh
//! cargo test -p evt-compat --features drivers --test conformance
//! ```

use deadpool_postgres::Pool;
use evt_compat::drivers;
use photon_evt::conformance::{Ledger, LedgerEvent, Suite, ViewTarget};
use photon_evt::dynamodb::{DynamoRepository, ViewProjector};
use photon_evt::postgres::{PgStatement, PostgresRepository};
use photon_evt::wire::dynamo::DEFAULT_VIEW_SORT_KEY;
use photon_evt::{Aggregate, BoxError, EntityId, Projector, json};

use aws_sdk_dynamodb::types::AttributeValue;

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

#[tokio::test]
async fn dynamo_conforms() {
    let repository = drivers::dynamo().await;
    let views = DynamoViews {
        client: repository.client().clone(),
        table: drivers::views_table(),
    };

    Suite::new(&repository).with_views(views).run().await;
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
