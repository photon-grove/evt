//! Connections to the local DynamoDB emulator and PostgreSQL, configured from the same environment
//! variables as the Go integration tests.

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use photon_evt::BoxError;
use photon_evt::dynamodb::DynamoRepository;
use photon_evt::postgres::PostgresRepository;

/// The local event-log table (`EVT_COMPAT_DYNAMO_TABLE`).
pub fn dynamo_table() -> String {
    std::env::var("EVT_COMPAT_DYNAMO_TABLE").unwrap_or_else(|_| "evt-local-event-log".into())
}

/// The local views table (`EVT_COMPAT_VIEWS_TABLE`).
pub fn views_table() -> String {
    std::env::var("EVT_COMPAT_VIEWS_TABLE").unwrap_or_else(|_| "evt-local-entity-views".into())
}

/// A DynamoDB client for `AWS_ENDPOINT_URL` (default `http://localhost:4566`) in `us-west-2`.
pub async fn dynamo_client() -> aws_sdk_dynamodb::Client {
    let endpoint =
        std::env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| "http://localhost:4566".into());
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-west-2"))
        .endpoint_url(endpoint)
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
