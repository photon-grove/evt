//! Runs the `photon-evt` conformance suite against the local DynamoDB emulator and PostgreSQL.
//! Needs the `drivers` feature and the `infra/local` and `infra/local-postgres` stacks:
//!
//! ```sh
//! cargo test -p evt-compat --features drivers --test conformance
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem};
use deadpool_postgres::Pool;
use evt_compat::drivers::{self, DynamoHeads, PostgresHeads};
use futures_core::Stream as _;
use photon_evt::conformance::{
    Add, CommitViewTarget, HeadFixtures, Ledger, LedgerEvent, RebuildSuite, Suite, TestClock,
    ViewTarget, commit_view_cases, committed_view_cases, enumeration_cases, expected_payload_cases,
    feature_marker_cases, fresh_store_cases, initialization_cases, outcome_cases,
    projection_diff_cases, projection_rebuild_cases, view_repository_cases,
};
use photon_evt::dynamodb::{DynamoRepository, DynamoViewRepository, ViewProjector};
use photon_evt::postgres::{PgStatement, PostgresRepository, ViewWriter};
use photon_evt::wire::dynamo::DEFAULT_VIEW_SORT_KEY;
use photon_evt::wire::postgres::ENTITY_LOCK;
use photon_evt::{
    Aggregate, BoxError, Commit, CommitProjector, CommitView, EntityId, Error, EventId, EventLog,
    EventSequence, EventType, EventVersion, Metadata, Projector, REASON_CONDITION_FAILED,
    Repository, RetryPolicy, SerializedEvent, SerializedSnapshot, Store, StreamFilter,
    ViewCondition, ViewOperation, ViewOperationKind, ViewRepository, ViewRow, json,
};

/// A projector's own error for a claim that is already taken.
#[derive(Debug)]
struct NameTaken(String);

impl std::fmt::Display for NameTaken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the name {} is taken", self.0)
    }
}

impl std::error::Error for NameTaken {}

/// The table the commit-wide view cases write to on PostgreSQL: the application's own schema, which
/// evt does not define.
const PG_COMMIT_VIEWS_TABLE: &str = "evt_conformance_commit_views";

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

/// Resets the feature manifest a previous run may have left features in, so unsupported
/// declarations cannot refuse this run's writers. Every writer requires `stream-heads` (ADR 0007),
/// so the reset keeps it declared and nothing else.
async fn clear_manifest<R: photon_evt::FeatureMarker>(repository: &R) {
    repository
        .update_manifest(|manifest| {
            manifest.features.clear();
            manifest.declare_stream_heads();
            manifest.history_dependent.clear();
            manifest.retention.clear();
        })
        .await
        .expect("clear the feature manifest");
}

// One test, run in sequence: Moto rolls back a failed transaction by restoring a copy of every
// table it touched, which drops writes that a concurrent test made in the meantime. Real DynamoDB
// does not, but the projector-condition case deliberately fails a transaction.
#[tokio::test]
async fn dynamo_conforms() {
    // A private, stream-less table of its own: the emulator copies every table a transaction
    // touches (and its change-stream history) to be able to roll it back, so a table shared with
    // other suites, or one with a stream enabled, makes every commit slower than the last and
    // balloons the emulator's memory. A new table also starts empty, so the writer declares
    // `stream-heads` itself the way a fresh deployment does.
    let client = drivers::dynamo_client().await;
    let table = format!(
        "evt-conformance-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    create_dynamo_table(&client, &table).await;

    // The whole run is bounded: a case that loops without making progress fails the test rather
    // than holding the job until its timeout.
    tokio::time::timeout(
        Duration::from_secs(CONFORMANCE_BUDGET_SECS),
        Box::pin(run_dynamo_conformance(client.clone(), &table)),
    )
    .await
    .expect("the DynamoDB conformance run exceeded its time budget");

    client
        .delete_table()
        .table_name(&table)
        .send()
        .await
        .expect("delete the conformance table");
}

/// The longest the DynamoDB conformance run may take (about 25 s on a laptop-class machine).
const CONFORMANCE_BUDGET_SECS: u64 = 300;

async fn run_dynamo_conformance(client: aws_sdk_dynamodb::Client, table: &str) {
    // The table starts empty, and the first writer initializes it, as a lazily initializing
    // writer does (explicit initialization has its own case below).
    let repository = DynamoRepository::new(client, table).with_lazy_initialization(true);
    let views = DynamoViews {
        client: repository.client().clone(),
        table: drivers::views_table(),
    };

    clear_manifest(&repository).await;
    let heads = DynamoHeads::new(repository.client().clone(), repository.table().to_owned());
    Suite::new(&repository)
        .with_views(views)
        .with_head_fixtures(heads)
        .run()
        .await;

    // A second handle over the same table is a second writer with its own marker cache.
    let second = DynamoRepository::new(repository.client().clone(), repository.table())
        .with_lazy_initialization(true);
    feature_marker_cases(&repository, &second).await;
    outcome_cases(&repository, &second).await;
    Box::pin(dynamo_commit_wide_views(&repository)).await;
    Box::pin(dynamo_view_repository(&repository)).await;
    dynamo_outcome_row_matches_the_wire_format(&repository).await;
    dynamo_outcome_reads_are_consistent_by_default_and_by_force(&repository).await;
    dynamo_marker_row_matches_the_wire_format(&repository).await;
    projector_condition_failure_is_not_a_conflict(&repository).await;
    dynamo_refuses_projector_only_transactions(&repository).await;
    dynamo_legacy_reserved_reads(&repository).await;
    dynamo_enumeration(&repository).await;
    Box::pin(dynamo_fresh_and_unmigrated_tables(&repository)).await;
    Box::pin(dynamo_store_initialization(repository.client())).await;
    Box::pin(dynamo_rebuild_rows_leave_a_table_fresh(repository.client())).await;
    Box::pin(dynamo_retained_stream_heads(&repository)).await;
}

/// Reads the rows commit-wide view operations write to a views table.
struct DynamoViewRows {
    client: aws_sdk_dynamodb::Client,
    table: String,
}

impl DynamoViewRows {
    async fn item(
        &self,
        pk: &str,
        sk: &str,
    ) -> Result<Option<std::collections::HashMap<String, AttributeValue>>, BoxError> {
        Ok(self
            .client
            .get_item()
            .table_name(&self.table)
            .consistent_read(true)
            .key("pk", AttributeValue::S(pk.into()))
            .key("sk", AttributeValue::S(sk.into()))
            .send()
            .await?
            .item)
    }
}

impl CommitViewTarget<DynamoRepository> for DynamoViewRows {
    async fn row(&self, pk: &str, sk: &str) -> Result<Option<ViewRow>, BoxError> {
        let Some(item) = self.item(pk, sk).await? else {
            return Ok(None);
        };
        let text = |name: &str| -> Result<String, BoxError> {
            Ok(item
                .get(name)
                .and_then(|value| value.as_s().ok())
                .ok_or_else(|| format!("view item has no string {name}"))?
                .clone())
        };

        Ok(Some(ViewRow::new(
            text("pk")?,
            text("sk")?,
            text("entityID")?,
            text("entityType")?,
            text("payload")?.into_bytes(),
        )))
    }
}

/// A history-dependent projector for the expired-tail case: a row per stream with the total its
/// last replayed commit left.
struct ReplayedTotals {
    pk: String,
    history: Vec<photon_evt::EntityType>,
}

impl CommitProjector for ReplayedTotals {
    fn name(&self) -> &str {
        "replayed-totals"
    }

    fn history(&self) -> &[photon_evt::EntityType] {
        &self.history
    }

    fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
        commit
            .streams
            .iter()
            .map(|stream| {
                Ok(ViewOperation::put(
                    self.pk.clone(),
                    stream.entity_id.as_str(),
                    stream.entity_id.clone(),
                    stream.entity_type.as_str(),
                    stream.state.clone(),
                ))
            })
            .collect()
    }
}

/// Retention's TTL deletes lazily and in no order, so a stream can keep its first event and a
/// snapshot covering sequence 3 after events 2 and 3 are gone: its enumerated head is still 3. A
/// history-dependent rebuild must refuse it (`CompactionUncovered` through the head) rather than
/// replay the one event left and write the state at sequence 1. Moto never deletes expired items,
/// so the expiry is a raw delete.
async fn dynamo_rebuild_refuses_an_expired_tail(
    rebuilds: &DynamoRepository,
    rows: &DynamoViewRows,
) {
    use photon_evt::{NewSnapshot, ProjectionRebuild, RebuildFence};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let entity_type = photon_evt::EntityType::new(format!("expired-tail-{nanos}"));
    let id = EntityId::new(format!("expired-tail-{nanos}"));
    let events = (1..=3)
        .map(|sequence| SerializedEvent {
            id: EventId::for_event(&id, EventSequence(sequence)),
            sequence: EventSequence(sequence),
            event_type: EventType::new("ledger:added"),
            version: EventVersion(1),
            entity_id: id.clone(),
            entity_type: entity_type.clone(),
            payload: br#"{"amount":1}"#.to_vec(),
            metadata: Metadata::new("local"),
        })
        .collect();
    rebuilds
        .commit(
            Commit::new(id.clone(), events).with_snapshot(NewSnapshot::new(
                entity_type.clone(),
                EventSequence(1),
                r#"{"total":3,"entries":3}"#,
            )),
        )
        .await
        .unwrap();
    for sequence in 2..=3 {
        rebuilds
            .client()
            .delete_item()
            .table_name(rebuilds.table())
            .key("pk", AttributeValue::S(id.to_string()))
            .key("sk", AttributeValue::N(sequence.to_string()))
            .send()
            .await
            .unwrap();
    }
    assert_eq!(
        rebuilds
            .entity_heads(Some(&entity_type))
            .await
            .unwrap()
            .get(&id),
        Some(&EventSequence(3)),
        "the snapshot keeps the enumerated head at 3"
    );

    let pk = format!("expired-tail-{nanos}#totals");
    let error = ProjectionRebuild::new(rebuilds)
        .with_projector(Arc::new(ReplayedTotals {
            pk: pk.clone(),
            history: vec![entity_type.clone()],
        }))
        .aggregate_as::<Ledger>(entity_type.clone())
        .run(&format!("expired-tail-{nanos}"), Duration::from_secs(60))
        .await
        .unwrap_err();

    assert!(
        matches!(
            &error,
            Error::CompactionUncovered { entity_id, through, covered }
                if *entity_id == id
                    && *through == EventSequence(3)
                    && *covered == Some(EventSequence(3))
        ),
        "an expired tail is refused through the head: {error:?}"
    );
    assert!(
        rows.row(&pk, id.as_str()).await.unwrap().is_none(),
        "nothing is written from the state at sequence 1"
    );
    assert!(
        rebuilds.rebuild_generation().await.unwrap().is_settled(),
        "the run acquired from a settled generation and wrote nothing, so it released the lock"
    );
}

/// A private views table (string `pk` and `sk`, no stream), like the event-log table of the suite.
async fn create_dynamo_views_table(client: &aws_sdk_dynamodb::Client, name: &str) {
    create_dynamo_views_table_with(client, name, false).await;
}

/// A private views table, with the `entityType-index` index the view repository lists by when
/// `indexed` (the table `infra/local` creates has it).
async fn create_dynamo_views_table_with(
    client: &aws_sdk_dynamodb::Client,
    name: &str,
    indexed: bool,
) {
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType,
        Projection, ProjectionType, ScalarAttributeType,
    };

    let attribute = |name: &str| {
        AttributeDefinition::builder()
            .attribute_name(name)
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap()
    };
    let key = |name: &str, key_type| {
        KeySchemaElement::builder()
            .attribute_name(name)
            .key_type(key_type)
            .build()
            .unwrap()
    };

    let mut request = client
        .create_table()
        .table_name(name)
        .attribute_definitions(attribute("pk"))
        .attribute_definitions(attribute("sk"))
        .key_schema(key("pk", KeyType::Hash))
        .key_schema(key("sk", KeyType::Range))
        .billing_mode(BillingMode::PayPerRequest);

    if indexed {
        request = request
            .attribute_definitions(attribute("entityType"))
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name("entityType-index")
                    .key_schema(key("entityType", KeyType::Hash))
                    .projection(
                        Projection::builder()
                            .projection_type(ProjectionType::All)
                            .build(),
                    )
                    .build()
                    .unwrap(),
            );
    }

    request.send().await.expect("create the views table");
}

/// The view repository (P1, P10) against the emulator: the shared cases over a private table with
/// the entity-type index, the rows a commit wrote through the same table, and the stored items.
async fn dynamo_view_repository(repository: &DynamoRepository) {
    let client = repository.client().clone();
    let table = format!(
        "evt-conformance-view-repo-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    create_dynamo_views_table_with(&client, &table, true).await;

    let views = DynamoViewRepository::new(client.clone(), &table);
    view_repository_cases(&views).await;

    // The commit path writes to the table the view repository reads.
    let writer = repository.clone().with_views_table(&table);
    committed_view_cases(&writer, &writer.view_repository().unwrap()).await;

    // Without a views table there is nothing to read.
    assert!(matches!(
        repository.view_repository(),
        Err(Error::ViewsNotConfigured {
            backend: "dynamodb"
        })
    ));

    // The stored item is the five string attributes, and a `ttl` number only when set.
    let pk = format!("shape-{table}");
    let stored = |ttl: i64| ViewRow::new(&pk, "", "owner", "shape", "{}").with_ttl(ttl);
    for ttl in [0, 4_102_444_800] {
        views.put_view(&stored(ttl)).await.unwrap();
        let item = client
            .get_item()
            .table_name(&table)
            .consistent_read(true)
            .key("pk", AttributeValue::S(pk.clone()))
            .key("sk", AttributeValue::S("VIEW".into()))
            .send()
            .await
            .unwrap()
            .item
            .unwrap();

        let mut names: Vec<_> = item.keys().map(String::as_str).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            if ttl == 0 {
                vec!["entityID", "entityType", "payload", "pk", "sk"]
            } else {
                vec!["entityID", "entityType", "payload", "pk", "sk", "ttl"]
            }
        );
        assert_eq!(item["entityID"], AttributeValue::S("owner".into()));
        if ttl != 0 {
            assert_eq!(item["ttl"], AttributeValue::N(ttl.to_string()));
        }
    }

    client
        .delete_table()
        .table_name(&table)
        .send()
        .await
        .expect("delete the view repository table");
}

/// Commit-wide view operations (v0.9, C7) against the emulator: the shared cases, a repository
/// with no views table, and the stored item's shape.
async fn dynamo_commit_wide_views(repository: &DynamoRepository) {
    let client = repository.client().clone();
    let views = format!(
        "evt-conformance-views-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    create_dynamo_views_table(&client, &views).await;

    let writer = repository.clone().with_views_table(&views);
    let rows = DynamoViewRows {
        client: client.clone(),
        table: views.clone(),
    };
    commit_view_cases(&writer, &rows).await;
    // The journal counter's condition, on the backend that defines it.
    expected_payload_cases(&writer, &rows).await;

    // The rebuild fence (C10) over the same private tables, with an injected clock: the lock is
    // table-wide, and every case leaves it settled.
    let clock = TestClock::starting_now();
    let rebuilds = writer.clone().with_clock(clock.reader());
    Box::pin(
        RebuildSuite::new(&rebuilds, &clock)
            .with_views(DynamoViewRows {
                client: client.clone(),
                table: views.clone(),
            })
            .with_head_fixtures(DynamoHeads::new(
                client.clone(),
                repository.table().to_owned(),
            ))
            .run(),
    )
    .await;
    Box::pin(projection_rebuild_cases(
        &rebuilds,
        &clock,
        &DynamoViewRows {
            client: client.clone(),
            table: views.clone(),
        },
    ))
    .await;
    // The diff case lists its rows by type, so it needs the index, which refuses the empty entity
    // types other cases write: a views table of its own.
    let indexed = format!("{views}-indexed");
    create_dynamo_views_table_with(&client, &indexed, true).await;
    let diffing = rebuilds.clone().with_views_table(&indexed);
    Box::pin(projection_diff_cases(
        &diffing,
        &clock,
        &diffing.view_repository().expect("a views table"),
    ))
    .await;
    client
        .delete_table()
        .table_name(&indexed)
        .send()
        .await
        .expect("delete the indexed views table");
    dynamo_rebuild_refuses_an_expired_tail(&rebuilds, &rows).await;

    // The stored item has exactly the five string attributes, an empty entity type included.
    let store = Store::<Ledger, _>::new(&writer, ()).with_commit_projector(std::sync::Arc::new(
        FixedRows::new("shape", |commit| {
            let owner = commit.streams[0].entity_id.clone();

            vec![
                ViewOperation::put("shape", "row", owner.clone(), "", "{}"),
                ViewOperation::put("shape", "full", owner, "type", br#"{"a":1}"#.to_vec()),
            ]
        }),
    ));
    let id = EntityId::new(format!("conf-views-shape-{}", views));
    store
        .execute(&id, Add(1), Metadata::new("local"))
        .await
        .unwrap();
    let item = rows.item("shape", "row").await.unwrap().expect("the row");
    let mut attributes: Vec<_> = item.keys().cloned().collect();
    attributes.sort();
    assert_eq!(
        attributes,
        ["entityID", "entityType", "payload", "pk", "sk"],
        "all five attributes are written"
    );
    assert!(item.values().all(|value| value.as_s().is_ok()), "{item:?}");
    let full = rows.item("shape", "full").await.unwrap().expect("the row");
    assert_eq!(full["entityID"], AttributeValue::S(id.to_string()));
    assert_eq!(full["entityType"], AttributeValue::S("type".into()));
    assert_eq!(full["payload"], AttributeValue::S(r#"{"a":1}"#.into()));

    // Without a views table the commit is refused before any write, never silently dropped.
    let unconfigured = Store::<Ledger, _>::new(repository, ()).with_commit_projector(
        std::sync::Arc::new(FixedRows::new("unconfigured", |commit| {
            vec![ViewOperation::put(
                "p",
                "s",
                commit.streams[0].entity_id.clone(),
                "",
                "{}",
            )]
        })),
    );
    let id = EntityId::new(format!("conf-views-unconfigured-{}", views));
    let error = unconfigured
        .execute(&id, Add(1), Metadata::new("local"))
        .await
        .expect_err("no views table");
    assert!(
        matches!(
            error.as_store_error(),
            Some(Error::ViewsNotConfigured {
                backend: "dynamodb"
            })
        ),
        "{error:?}"
    );
    assert!(repository.events(&id).await.unwrap().is_empty());

    client
        .delete_table()
        .table_name(&views)
        .send()
        .await
        .expect("delete the views table");
}

type Rows = Box<dyn Fn(&CommitView) -> Vec<ViewOperation> + Send + Sync>;

/// A commit-wide projector over a closure, for the cases that need one fixed shape.
struct FixedRows {
    name: &'static str,
    rows: Rows,
}

impl FixedRows {
    fn new(
        name: &'static str,
        rows: impl Fn(&CommitView) -> Vec<ViewOperation> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name,
            rows: Box::new(rows),
        }
    }
}

impl CommitProjector for FixedRows {
    fn name(&self) -> &str {
        self.name
    }

    fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
        Ok((self.rows)(commit))
    }
}

/// A new, empty table for a case that needs a store nothing has written to.
async fn create_dynamo_table(client: &aws_sdk_dynamodb::Client, name: &str) {
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
    };

    client
        .create_table()
        .table_name(name)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::N)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .expect("create the table");
}

/// Writes a stream with no head item just before the first `PutItem` a client sends (the marker's
/// declaration), as a writer that never reads the feature marker committing between the empty-table
/// check and the declaration.
#[derive(Debug)]
struct HeadlessAfterDeclare {
    fired: AtomicBool,
    raw: DynamoHeads,
    event: SerializedEvent,
}

impl aws_sdk_dynamodb::config::Intercept for HeadlessAfterDeclare {
    fn name(&self) -> &'static str {
        "HeadlessAfterDeclare"
    }

    fn read_before_transmit(
        &self,
        context: &aws_sdk_dynamodb::config::interceptors::BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &aws_sdk_dynamodb::config::RuntimeComponents,
        _cfg: &mut aws_sdk_dynamodb::config::ConfigBag,
    ) -> Result<(), BoxError> {
        let is_put_item = context
            .request()
            .headers()
            .get("x-amz-target")
            .is_some_and(|target| target.ends_with(".PutItem"));

        if is_put_item && !self.fired.swap(true, Ordering::SeqCst) {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(
                    HeadFixtures::<DynamoRepository>::append_bypassing_head(&self.raw, &self.event),
                )
            })?;
        }

        Ok(())
    }
}

/// The empty-table check and the declaration are two steps of an initialization, and a writer that
/// never reads the marker can commit a stream between them. The declaration is committed first and
/// the table is looked at again afterward: a stream with no head item retracts the declaration and
/// the table is refused, rather than being left declared over a stream the heads cannot guard. A
/// lazily initializing writer's first write runs the same initialization.
#[tokio::test(flavor = "multi_thread")]
async fn dynamo_retracts_a_declaration_over_a_headless_stream() {
    let client = drivers::dynamo_client().await;
    let table = format!(
        "evt-window-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    create_dynamo_table(&client, &table).await;

    let id = EntityId::new("early-stream");
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
    let config = client
        .config()
        .to_builder()
        .interceptor(HeadlessAfterDeclare {
            fired: AtomicBool::new(false),
            raw: DynamoHeads::new(client.clone(), table.clone()),
            event,
        })
        .build();
    let operator = DynamoRepository::new(aws_sdk_dynamodb::Client::from_conf(config), &table);

    let error = operator
        .initialize()
        .await
        .expect_err("the table held a headless stream");
    assert!(
        matches!(error, Error::StreamHeadsNotMigrated),
        "refused as unmigrated, got {error:?}"
    );

    let manifest = DynamoRepository::new(client.clone(), &table)
        .read_manifest()
        .await
        .unwrap()
        .expect("the retraction leaves a manifest");
    assert!(
        !manifest.declares_stream_heads(),
        "the declaration is retracted"
    );

    client
        .delete_table()
        .table_name(&table)
        .send()
        .await
        .unwrap();
}

/// Counts the Scan requests a client sends.
#[derive(Debug, Default)]
struct ScanCounter(Arc<AtomicUsize>);

impl aws_sdk_dynamodb::config::Intercept for ScanCounter {
    fn name(&self) -> &'static str {
        "ScanCounter"
    }

    fn read_before_transmit(
        &self,
        context: &aws_sdk_dynamodb::config::interceptors::BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &aws_sdk_dynamodb::config::RuntimeComponents,
        _cfg: &mut aws_sdk_dynamodb::config::ConfigBag,
    ) -> Result<(), BoxError> {
        let is_scan = context
            .request()
            .headers()
            .get("x-amz-target")
            .is_some_and(|target| target.ends_with(".Scan"));
        if is_scan {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        Ok(())
    }
}

/// Explicit store initialization (issue #188) on a fresh table: a writer that does not initialize
/// stores (the default) refuses the table without a single Scan, the operator's initialization
/// declares `stream-heads`, and writers then commit, still without scanning.
async fn dynamo_store_initialization(client: &aws_sdk_dynamodb::Client) {
    let table = format!(
        "evt-init-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    create_dynamo_table(client, &table).await;

    let scans = Arc::new(AtomicUsize::new(0));
    let counted = |scans: &Arc<AtomicUsize>| {
        let config = client
            .config()
            .to_builder()
            .interceptor(ScanCounter(scans.clone()))
            .build();
        DynamoRepository::new(aws_sdk_dynamodb::Client::from_conf(config), &table)
    };

    let writer = counted(&scans);
    let refused = Store::<Ledger, _>::new(&writer, ())
        .execute("uninitialized-ledger", Add(1), Metadata::new("local"))
        .await
        .expect_err("an uninitialized table is refused");
    assert!(
        matches!(refused.as_store_error(), Some(Error::StoreNotInitialized)),
        "{refused:?}"
    );
    assert_eq!(
        scans.load(Ordering::SeqCst),
        0,
        "a writer never scans the table"
    );

    // The suite initializes through `writer`, which scans; the other handle is a second writer.
    let other = DynamoRepository::new(client.clone(), &table);
    let heads = DynamoHeads::new(client.clone(), table.clone());
    initialization_cases(&writer, &other, &heads, true).await;

    let runtime_scans = Arc::new(AtomicUsize::new(0));
    let runtime = counted(&runtime_scans);
    Store::<Ledger, _>::new(&runtime, ())
        .execute("initialized-ledger", Add(1), Metadata::new("local"))
        .await
        .expect("a writer commits to an initialized table");
    assert_eq!(
        runtime_scans.load(Ordering::SeqCst),
        0,
        "a writer never scans an initialized table"
    );

    client
        .delete_table()
        .table_name(&table)
        .send()
        .await
        .unwrap();
}

/// A clock in the past, so the `ttl`s it stamps have already expired by the time anything reads.
fn long_ago() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_700_000_000)
}

/// The `ttl` of a stream's head item, read with the raw client.
async fn head_ttl(
    client: &aws_sdk_dynamodb::Client,
    table: &str,
    id: &EntityId,
) -> (Option<i64>, Option<i64>) {
    let output = client
        .get_item()
        .table_name(table)
        .consistent_read(true)
        .key("pk", AttributeValue::S(id.to_string()))
        .key("sk", AttributeValue::N("-1".into()))
        .send()
        .await
        .unwrap();
    let Some(item) = output.item else {
        return (None, None);
    };
    let item = photon_evt::dynamodb::from_sdk_item(&item);
    let (head, ttl) = photon_evt::wire::dynamo::head_from_item(&item).unwrap();

    (Some(head.0), ttl)
}

/// Retained streams (ADR 0007, "Retention"): the head item carries a `ttl` at least that of every
/// other item and never lowered; a head item that TTL deleted before its events is the transient
/// expired-tail conflict when everything left has expired, and drift otherwise; and an
/// out-of-band snapshot refreshes the head's `ttl`. Moto does not delete expired items, so a
/// stream whose `ttl`s have passed stays readable, as one awaiting DynamoDB's lazy deletion.
async fn dynamo_retained_stream_heads(repository: &DynamoRepository) {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let client = repository.client();
    let table = repository.table();
    let entity_type = format!("scratch-{nanos}");
    let event = |id: &EntityId, sequence: i64| SerializedEvent {
        id: EventId::for_event(id, EventSequence(sequence)),
        sequence: EventSequence(sequence),
        event_type: EventType::new("ledger:added"),
        version: EventVersion(1),
        entity_id: id.clone(),
        entity_type: entity_type.as_str().into(),
        payload: br#"{"amount":1}"#.to_vec(),
        metadata: Metadata::new("local"),
    };
    let commit = |id: &EntityId, range: std::ops::RangeInclusive<i64>| {
        Commit::new(id.clone(), range.map(|n| event(id, n)).collect())
    };
    let handle = |retention: u64, clock: fn() -> SystemTime| {
        DynamoRepository::new(client.clone(), table)
            .with_lazy_initialization(true)
            .with_clock(clock)
            .with_retention(entity_type.as_str(), Duration::from_secs(retention))
    };
    let at = 1_700_000_000_i64;

    // The head is stamped with the commit's ttl, and only ever raised.
    let id = EntityId::new(format!("retained-{nanos}-ttl"));
    handle(3600, long_ago)
        .commit(commit(&id, 1..=2))
        .await
        .unwrap();
    assert_eq!(
        head_ttl(client, table, &id).await,
        (Some(2), Some(at + 3600))
    );

    // A second writer configured with a shorter retention has not loaded the stream, so its head
    // update would lower the ttl: the condition fails as an ordinary conflict, and the retry,
    // having loaded the stream, passes without lowering it.
    let short = handle(60, long_ago);
    let lowered = short.commit(commit(&id, 3..=3)).await;
    assert!(
        matches!(lowered, Err(Error::Conflict { .. })),
        "a head update that would lower the ttl is a conflict: {lowered:?}"
    );
    assert_eq!(
        head_ttl(client, table, &id).await,
        (Some(2), Some(at + 3600))
    );
    short.events(&id).await.unwrap();
    short.commit(commit(&id, 3..=3)).await.unwrap();
    assert_eq!(
        head_ttl(client, table, &id).await,
        (Some(3), Some(at + 3600)),
        "the head keeps the larger ttl; only the event takes the commit's own"
    );

    // A longer retention raises it.
    let long = handle(7200, long_ago);
    long.events(&id).await.unwrap();
    long.commit(commit(&id, 4..=4)).await.unwrap();
    assert_eq!(
        head_ttl(client, table, &id).await,
        (Some(4), Some(at + 7200))
    );

    // An out-of-band snapshot refreshes the head's ttl to its own, and a later one with a smaller
    // ttl falls back to a plain check and leaves the larger ttl in place.
    let id = EntityId::new(format!("retained-{nanos}-snapshot"));
    handle(60, long_ago)
        .commit(commit(&id, 1..=3))
        .await
        .unwrap();
    assert_eq!(head_ttl(client, table, &id).await, (Some(3), Some(at + 60)));
    let snapshot = |sequence: i64| SerializedSnapshot {
        entity_type: entity_type.as_str().into(),
        entity_id: id.clone(),
        sequence: EventSequence(sequence),
        event_sequence: EventSequence(3),
        payload: b"{}".to_vec(),
    };
    assert!(
        handle(3600, long_ago)
            .put_snapshot(&snapshot(1))
            .await
            .unwrap()
    );
    assert_eq!(
        head_ttl(client, table, &id).await,
        (Some(3), Some(at + 3600))
    );
    assert!(
        handle(60, long_ago)
            .put_snapshot(&snapshot(2))
            .await
            .unwrap(),
        "only the ttl condition failed, so the write retries as a plain check"
    );
    assert_eq!(
        head_ttl(client, table, &id).await,
        (Some(3), Some(at + 3600))
    );

    // TTL deleted the head item but not the events. Everything left expired: a conflict that
    // heals once DynamoDB deletes it, not drift.
    let expired = EntityId::new(format!("retained-{nanos}-expired"));
    handle(3600, long_ago)
        .commit(commit(&expired, 1..=2))
        .await
        .unwrap();
    let heads = DynamoHeads::new(client.clone(), table.to_owned());
    HeadFixtures::<DynamoRepository>::delete_head(&heads, &expired)
        .await
        .unwrap();
    let live = handle(3600, SystemTime::now);
    for (what, result) in [
        ("an append", live.commit(commit(&expired, 3..=3)).await),
        (
            "a create over the residue",
            live.commit(commit(&expired, 1..=1)).await,
        ),
    ] {
        assert!(
            matches!(result, Err(Error::Conflict { .. })),
            "{what} over an expired residue is a transient conflict: {result:?}"
        );
    }

    // The same with a residue that has not expired is drift.
    let residue = EntityId::new(format!("retained-{nanos}-residue"));
    live.commit(commit(&residue, 1..=2)).await.unwrap();
    HeadFixtures::<DynamoRepository>::delete_head(&heads, &residue)
        .await
        .unwrap();
    for (what, result) in [
        ("an append", live.commit(commit(&residue, 3..=3)).await),
        (
            "a create over the residue",
            live.commit(commit(&residue, 1..=1)).await,
        ),
    ] {
        assert!(
            matches!(
                &result,
                Err(Error::StreamHeadDrift {
                    stored: None,
                    derived: EventSequence(2),
                    ..
                })
            ),
            "{what} over a live residue is drift: {result:?}"
        );
    }

    // A head above the derived head: TTL deleted the tail events the head counted. For a type
    // with retention that is the expired-tail transient, a conflict that heals, whether or not
    // the items that remain have expired.
    let tail = EntityId::new(format!("retained-{nanos}-tail"));
    handle(3600, long_ago)
        .commit(commit(&tail, 1..=4))
        .await
        .unwrap();
    for sequence in 3..=4 {
        client
            .delete_item()
            .table_name(table)
            .key("pk", AttributeValue::S(tail.to_string()))
            .key("sk", AttributeValue::N(sequence.to_string()))
            .send()
            .await
            .unwrap();
    }
    let result = live.commit(commit(&tail, 3..=3)).await;
    assert!(
        matches!(result, Err(Error::Conflict { .. })),
        "a head above an expired log is a transient conflict: {result:?}"
    );
    let live_tail = EntityId::new(format!("retained-{nanos}-live-tail"));
    live.commit(commit(&live_tail, 1..=4)).await.unwrap();
    for sequence in 3..=4 {
        client
            .delete_item()
            .table_name(table)
            .key("pk", AttributeValue::S(live_tail.to_string()))
            .key("sk", AttributeValue::N(sequence.to_string()))
            .send()
            .await
            .unwrap();
    }
    let result = live.commit(commit(&live_tail, 3..=3)).await;
    assert!(
        matches!(result, Err(Error::Conflict { .. })),
        "a head above the log of a retained type is the expired-tail transient, expired or not: {result:?}"
    );
}

/// Fresh and unmigrated tables (ADR 0007): the first write to an empty table declares
/// `stream-heads` itself, a table that holds only reserved rows is still empty, and a table with
/// an event item and no marker row is refused by every write path.
async fn dynamo_fresh_and_unmigrated_tables(repository: &DynamoRepository) {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let client = repository.client();

    // An empty table.
    let table = format!("evt-fresh-{nanos}");
    create_dynamo_table(client, &table).await;
    let fresh = DynamoRepository::new(client.clone(), &table).with_lazy_initialization(true);
    let other = DynamoRepository::new(client.clone(), &table).with_lazy_initialization(true);
    fresh_store_cases(&fresh, &other).await;
    client
        .delete_table()
        .table_name(&table)
        .send()
        .await
        .unwrap();

    // A table holding only a reserved row is empty: the Scan skips `command-outcome#` rows.
    let table = format!("evt-reserved-{nanos}");
    create_dynamo_table(client, &table).await;
    let reserved = DynamoRepository::new(client.clone(), &table).with_lazy_initialization(true);
    reserved
        .commit(Commit::new("reserved-only", Vec::new()).with_outcome(
            photon_evt::Outcome::accepted(format!("reserved-{nanos}"), "digest"),
        ))
        .await
        .expect("an outcome-only commit declares on a table with no streams");
    assert!(
        reserved
            .read_manifest()
            .await
            .unwrap()
            .unwrap()
            .declares_stream_heads(),
        "the outcome-only commit declared the feature"
    );
    client
        .delete_table()
        .table_name(&table)
        .send()
        .await
        .unwrap();

    // A populated table with no marker row, as a v0.8 writer left it.
    let table = format!("evt-legacy-{nanos}");
    create_dynamo_table(client, &table).await;
    let legacy = DynamoRepository::new(client.clone(), &table).with_lazy_initialization(true);
    let heads = DynamoHeads::new(client.clone(), table.clone());
    let id = EntityId::new("legacy-stream");
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
    HeadFixtures::<DynamoRepository>::append_bypassing_head(&heads, &event)
        .await
        .unwrap();
    // A snapshot a writer without heads wrote, so that compaction reaches the marker check.
    let covering = SerializedSnapshot {
        entity_type: Ledger::TYPE.into(),
        entity_id: id.clone(),
        sequence: EventSequence(1),
        event_sequence: EventSequence(1),
        payload: b"{}".to_vec(),
    };
    client
        .put_item()
        .table_name(&table)
        .set_item(Some(photon_evt::dynamodb::to_sdk_item(
            &photon_evt::wire::dynamo::snapshot_item(&covering, None).unwrap(),
        )))
        .send()
        .await
        .unwrap();

    let next = SerializedEvent {
        id: EventId::for_event(&id, EventSequence(2)),
        sequence: EventSequence(2),
        ..event.clone()
    };
    for result in [
        legacy.commit(Commit::new(id.clone(), vec![next])).await,
        legacy
            .put_snapshot(&SerializedSnapshot {
                entity_type: Ledger::TYPE.into(),
                entity_id: id.clone(),
                sequence: EventSequence(1),
                event_sequence: EventSequence(0),
                payload: b"{}".to_vec(),
            })
            .await
            .map(|_| ()),
        legacy
            .compact_below(&id, EventSequence(1))
            .await
            .map(|_| ()),
    ] {
        assert!(
            matches!(result, Err(Error::StreamHeadsNotMigrated)),
            "an unmigrated table refuses writes: {result:?}"
        );
    }
    // Explicit initialization verifies the table the same way, and a writer that does not
    // initialize stores refuses it as uninitialized, without scanning it.
    assert!(
        matches!(
            legacy.initialize().await,
            Err(Error::StreamHeadsNotMigrated)
        ),
        "initializing a populated table is refused"
    );
    let strict = DynamoRepository::new(client.clone(), &table);
    assert!(
        matches!(
            strict
                .compact_below(&id, EventSequence(1))
                .await
                .map(|_| ()),
            Err(Error::StoreNotInitialized)
        ),
        "a writer that does not initialize refuses the table as uninitialized"
    );
    assert!(
        legacy.read_manifest().await.unwrap().is_none(),
        "a refused writer declares nothing on a populated table"
    );
    assert_eq!(legacy.events(&id).await.unwrap(), vec![event]);
    assert_eq!(
        legacy.entity_heads(None).await.unwrap().get(&id),
        Some(&EventSequence(1)),
        "entity heads scan the log, which needs no head items"
    );

    // The migration declares the feature after backfilling; here the head item is written by
    // hand, as the backfill would.
    client
        .put_item()
        .table_name(&table)
        .item("pk", AttributeValue::S(id.to_string()))
        .item("sk", AttributeValue::N("-1".into()))
        .item("head", AttributeValue::N("1".into()))
        .send()
        .await
        .unwrap();
    legacy.declare_stream_heads().await.unwrap();
    legacy
        .commit(Commit::new(
            id.clone(),
            vec![SerializedEvent {
                id: EventId::for_event(&id, EventSequence(2)),
                sequence: EventSequence(2),
                event_type: EventType::new("ledger:added"),
                version: EventVersion(1),
                entity_id: id.clone(),
                entity_type: Ledger::TYPE.into(),
                payload: br#"{"amount":1}"#.to_vec(),
                metadata: Metadata::new("local"),
            }],
        ))
        .await
        .expect("a migrated table accepts writes");
    client
        .delete_table()
        .table_name(&table)
        .send()
        .await
        .unwrap();
}

/// Reads an outcome row back through the raw client and asserts it is byte-for-byte the item the
/// wire format pins, with the `sk` stored as a number.
async fn dynamo_outcome_row_matches_the_wire_format(repository: &DynamoRepository) {
    use photon_evt::wire::dynamo;
    use photon_evt::{Outcome, Rejection};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let stream = EntityId::new(format!("conf-{nanos}-outcome-wire"));
    let event = |sequence: i64| SerializedEvent {
        id: EventId::for_event(&stream, EventSequence(sequence)),
        sequence: EventSequence(sequence),
        event_type: EventType::new("ledger:added"),
        version: EventVersion(1),
        entity_id: stream.clone(),
        entity_type: "ledger".into(),
        payload: br#"{"amount":1}"#.to_vec(),
        metadata: Metadata::default(),
    };

    let accepted = Outcome::accepted(format!("wire-{nanos}-accepted"), "digest")
        .with_response(vec![0, 1, 2, 254, 255]);
    repository
        .commit(
            Commit::new(stream.clone(), vec![event(1), event(2)]).with_outcome(accepted.clone()),
        )
        .await
        .expect("commit with an outcome");
    let rejected = Outcome::rejected(
        format!("wire-{nanos}-rejected"),
        "digest",
        Rejection::new("taken", "a<b>").with_details(serde_json::json!({"k": 1})),
    );
    repository
        .commit(Commit::new(stream.clone(), Vec::new()).with_outcome(rejected.clone()))
        .await
        .expect("outcome-only commit");

    for (outcome, event_ids) in [(accepted, 2usize), (rejected, 0)] {
        let output = repository
            .client()
            .get_item()
            .table_name(repository.table())
            .consistent_read(true)
            .key(
                "pk",
                AttributeValue::S(dynamo::outcome_key(&outcome.command_id)),
            )
            .key("sk", AttributeValue::N("1".into()))
            .send()
            .await
            .expect("read the outcome item");
        let stored = photon_evt::dynamodb::from_sdk_item(&output.item.expect("the outcome item"));

        let mut want = outcome.clone();
        want.event_ids = (1..=event_ids as i64)
            .map(|sequence| EventId::for_event(&stream, EventSequence(sequence)))
            .collect();
        assert_eq!(
            stored,
            dynamo::outcome_item(&want).expect("encode the outcome item"),
            "the stored outcome item must match the wire format"
        );
        assert_eq!(dynamo::outcome_from_item(&stored).unwrap(), want);
    }
}

/// The outcome pre-check is a strongly consistent read even when the repository is configured
/// for eventually consistent reads, so a committed outcome is always found.
async fn dynamo_outcome_reads_are_consistent_by_default_and_by_force(
    repository: &DynamoRepository,
) {
    let eventual = DynamoRepository::new(repository.client().clone(), repository.table())
        .with_consistent_read(false);
    let store = Store::<Ledger, _>::new(&eventual, ())
        .with_snapshot_interval(0)
        .with_outcomes(photon_evt::OutcomeConfig::new());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = EntityId::new(format!("conf-{nanos}-outcome-consistent"));

    for round in 0..10 {
        let metadata =
            Metadata::new("local").with_command_id(format!("consistent-{nanos}-{round}"));
        let first = store
            .execute_with_outcome(&id, Add(1), metadata.clone())
            .await
            .expect("first execution");
        let again = store
            .execute_with_outcome(&id, Add(1), metadata)
            .await
            .expect("repeat");
        assert!(!first.replayed && again.replayed, "round {round}");
    }
}

/// Reads the marker row back through the raw client and asserts it is byte-for-byte the item the
/// wire format pins, so the stored bytes match the storage spec, not just the crate's model.
async fn dynamo_marker_row_matches_the_wire_format(repository: &DynamoRepository) {
    use photon_evt::wire::dynamo;

    let manifest = repository
        .read_manifest()
        .await
        .expect("read the manifest")
        .expect("the feature-marker row exists");

    let output = repository
        .client()
        .get_item()
        .table_name(repository.table())
        .consistent_read(true)
        .key("pk", AttributeValue::S(photon_evt::FEATURES_ID.into()))
        .key("sk", AttributeValue::N("0".into()))
        .send()
        .await
        .expect("read the marker item");

    let stored = photon_evt::dynamodb::from_sdk_item(&output.item.expect("the marker item"));
    let want = dynamo::feature_marker_item(&manifest).expect("encode the marker item");

    assert_eq!(
        stored, want,
        "the stored marker item must match the wire format"
    );
    assert_eq!(
        manifest.features_json(),
        r#"{"stream-heads":1}"#,
        "the reset manifest declares stream-heads alone, as a sorted JSON object"
    );
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

/// A transaction with projector operations and no written event is refused before any write:
/// views are projections of committed events (ADR-0012).
async fn dynamo_refuses_projector_only_transactions(repository: &DynamoRepository) {
    let view = TransactWriteItem::builder()
        .put(
            Put::builder()
                .table_name(drivers::views_table())
                .item("pk", AttributeValue::S("projector-only".into()))
                .item("sk", AttributeValue::N("0".into()))
                .build()
                .unwrap(),
        )
        .build();
    let mut commit = photon_evt::TransactionCommit::new(Vec::new());
    commit.operations = vec![view.clone()];

    let err = repository.commit_transaction(commit).await.unwrap_err();

    assert!(
        matches!(err, Error::ProjectorWithoutEvents { operations: 1 }),
        "{err:?}"
    );

    // The single-stream path refuses the same shape.
    let single = Commit::new("projector-only", Vec::new()).with_operations([view]);
    let err = repository.commit(single).await.unwrap_err();
    assert!(
        matches!(err, Error::ProjectorWithoutEvents { operations: 1 }),
        "{err:?}"
    );
    let stored = repository
        .client()
        .get_item()
        .table_name(drivers::views_table())
        .key("pk", AttributeValue::S("projector-only".into()))
        .key("sk", AttributeValue::N("0".into()))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(stored.item().is_none(), "no view row was written");
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

/// Commits and snapshot writes for one entity take the same advisory lock, so a snapshot write
/// cannot check the stream's head while a commit is moving it. The other language runs the same
/// statement, so holding it here must block both kinds of write.
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
        event_sequence: EventSequence(0),
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

    // A snapshot of the empty stream needs no head item, so it is written in either order.
    assert!(put.await.unwrap().expect("the snapshot is written"));
    commit.await.unwrap().expect("the commit lands");
}

/// Inserts one view row per ledger version with a plain `INSERT`, so two writers committing from
/// the same load collide on it.
struct PlainInsertView;

impl Projector<Ledger, PgStatement> for PlainInsertView {
    fn project(
        &self,
        entity_id: &EntityId,
        ledger: &Ledger,
        _: &[LedgerEvent],
    ) -> Result<Vec<PgStatement>, BoxError> {
        Ok(vec![PgStatement::new(
            "INSERT INTO evt_plain_views (key) VALUES ($1)",
            vec![Box::new(format!("{entity_id}:{}", ledger.entries))],
        )])
    }
}

#[tokio::test]
async fn postgres_lost_race_is_a_conflict_with_a_plain_insert_projector() {
    let repository = drivers::postgres().await.expect("connect to PostgreSQL");
    repository
        .pool()
        .get()
        .await
        .expect("connect to PostgreSQL")
        .batch_execute("CREATE TABLE IF NOT EXISTS evt_plain_views (key text PRIMARY KEY)")
        .await
        .expect("create the view table");

    let store = Store::<Ledger, _>::new(&repository, ()).with_projector(PlainInsertView);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = EntityId::new(format!("conf-{nanos}-plain-insert"));

    let first = store.load(&id).await.unwrap();
    let second = store.load(&id).await.unwrap();
    let event = || {
        vec![LedgerEvent::from(photon_evt::conformance::Added {
            amount: 1,
        })]
    };
    store
        .commit(first, event(), &Metadata::new("local"))
        .await
        .unwrap();

    let lost = store.commit(second, event(), &Metadata::new("local")).await;
    assert!(
        matches!(lost, Err(Error::Conflict { .. })),
        "the loser fails on the event key: {lost:?}"
    );
}

#[tokio::test]
async fn postgres_schema_setup_is_concurrent_safe() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let events = format!("ddl_{nanos:x}");
    let snapshots = format!("{events}s");
    let repository = PostgresRepository::new(drivers::postgres_pool().unwrap())
        .with_tables(&events, &snapshots)
        .unwrap();
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let repository = repository.clone();
        tasks.push(tokio::spawn(
            async move { repository.ensure_schema().await },
        ));
    }
    let mut results = Vec::new();
    for task in tasks {
        results.push(task.await.unwrap());
    }
    repository
        .pool()
        .get()
        .await
        .unwrap()
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {events}, {snapshots}, {events}__evt_features, {events}__evt_stream_heads, {events}__evt_views"
        ))
        .await
        .unwrap();
    for result in results {
        result.expect("concurrent schema creation succeeds");
    }
}

#[tokio::test]
async fn postgres_conforms() {
    // C9 cases mutate table-global state, including unsupported declarations. Keep them
    // separate from parallel tests that exercise normal commits on the default event log.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let events = format!("cf_{nanos:x}");
    let snapshots = format!("{events}s");
    let repository = PostgresRepository::new(drivers::postgres_pool().unwrap())
        .with_tables(&events, &snapshots)
        .unwrap();
    repository
        .ensure_schema()
        .await
        .expect("create the event log");
    let views = PostgresViews::new(repository.pool().clone())
        .await
        .expect("create the view table");

    clear_manifest(&repository).await;

    // PostgreSQL checks the snapshot sequence chain under the entity lock, like every backend.
    let heads = PostgresHeads::new(repository.pool().clone(), events.clone());
    Suite::new(&repository)
        .with_views(views)
        .with_head_fixtures(heads)
        .run()
        .await;

    // A second handle over the same pool is a second writer with its own marker cache.
    let second = PostgresRepository::new(repository.pool().clone())
        .with_tables(&events, &snapshots)
        .unwrap();
    feature_marker_cases(&repository, &second).await;
    outcome_cases(&repository, &second).await;
    Box::pin(postgres_commit_wide_views(&repository, &events)).await;
    postgres_outcome_row_matches_the_wire_format(&repository, &events).await;
    postgres_features_text_matches_the_manifest(&repository, &events).await;
    postgres_refuses_projector_only_transactions(&repository).await;
    postgres_legacy_reserved_reads(&repository, &events, &snapshots).await;
    postgres_enumeration(&repository, &events).await;
    Box::pin(postgres_fresh_and_unmigrated_databases()).await;
    Box::pin(postgres_rebuild_relations(repository.pool().clone())).await;
    repository
        .pool()
        .get()
        .await
        .unwrap()
        .batch_execute(&format!(
            "DROP TABLE {events}, {snapshots}, {events}__evt_features, {events}__evt_command_outcomes, {events}__evt_stream_heads, {events}__evt_rebuild_lock, {events}__evt_rebuild_generation, {events}__evt_views"
        ))
        .await
        .expect("drop the isolated event log");
}

/// The view writer for [`PG_COMMIT_VIEWS_TABLE`], with the statement shapes the condition
/// semantics require and the owner enforced in SQL: an absent put is a plain `INSERT` (a violation
/// of the key constraint is its failed condition), any other put an upsert that only the row's
/// owner may overwrite, a delete a `DELETE` of the owner's row (zero rows fails `present`).
fn postgres_view_writer() -> ViewWriter {
    std::sync::Arc::new(|operation: &ViewOperation| {
        Ok(match operation.kind {
            ViewOperationKind::Put => {
                let conflict = if operation.condition == ViewCondition::Absent {
                    String::new()
                } else {
                    format!(
                        " ON CONFLICT (pk, sk) DO UPDATE SET entity_type = EXCLUDED.entity_type, \
                         payload = EXCLUDED.payload \
                         WHERE {PG_COMMIT_VIEWS_TABLE}.entity_id = EXCLUDED.entity_id"
                    )
                };

                PgStatement::new(
                    format!(
                        "INSERT INTO {PG_COMMIT_VIEWS_TABLE} (pk, sk, entity_id, entity_type, payload) \
                         VALUES ($1, $2, $3, $4, $5){conflict}"
                    ),
                    vec![
                        Box::new(operation.pk.clone()),
                        Box::new(operation.sk.clone()),
                        Box::new(operation.owner.to_string()),
                        Box::new(operation.entity_type.clone()),
                        Box::new(String::from_utf8_lossy(&operation.payload).into_owned()),
                    ],
                )
                .with_key_constraint(format!("{PG_COMMIT_VIEWS_TABLE}_pkey"))
            }
            ViewOperationKind::Delete => PgStatement::new(
                if operation.condition == ViewCondition::Present {
                    format!(
                        "DELETE FROM {PG_COMMIT_VIEWS_TABLE} WHERE pk = $1 AND sk = $2 \
                         AND entity_id = $3"
                    )
                } else {
                    format!(
                        "WITH gone AS (DELETE FROM {PG_COMMIT_VIEWS_TABLE} \
                         WHERE pk = $1 AND sk = $2 AND entity_id = $3) \
                         SELECT 1 FROM {PG_COMMIT_VIEWS_TABLE} \
                         WHERE pk = $1 AND sk = $2 AND entity_id <> $3"
                    )
                },
                vec![
                    Box::new(operation.pk.clone()),
                    Box::new(operation.sk.clone()),
                    Box::new(operation.owner.to_string()),
                ],
            ),
        })
    })
}

/// Reads the rows [`postgres_view_writer`] wrote.
struct PostgresViewRows {
    pool: Pool,
}

impl CommitViewTarget<PostgresRepository> for PostgresViewRows {
    async fn row(&self, pk: &str, sk: &str) -> Result<Option<ViewRow>, BoxError> {
        let row = self
            .pool
            .get()
            .await?
            .query_opt(
                &format!(
                    "SELECT entity_id, entity_type, payload FROM {PG_COMMIT_VIEWS_TABLE} \
                     WHERE pk = $1 AND sk = $2"
                ),
                &[&pk, &sk],
            )
            .await?;

        Ok(row.map(|row| {
            ViewRow::new(
                pk,
                sk,
                row.get::<_, String>(0),
                row.get::<_, String>(1),
                row.get::<_, String>(2).into_bytes(),
            )
        }))
    }

    fn budgets_view_bytes(&self) -> bool {
        false
    }
}

/// Commit-wide view operations (v0.9, C7) against PostgreSQL: the shared cases, the SQLSTATE
/// reasons of other statement failures, and the refusals that need no writer or a bad one.
async fn postgres_commit_wide_views(repository: &PostgresRepository, events: &str) {
    let pool = repository.pool().clone();
    pool.get()
        .await
        .unwrap()
        .batch_execute(&format!(
            "CREATE TABLE IF NOT EXISTS {PG_COMMIT_VIEWS_TABLE} (
                 pk text NOT NULL, sk text NOT NULL, entity_id text NOT NULL,
                 entity_type text NOT NULL, payload text NOT NULL, PRIMARY KEY (pk, sk))"
        ))
        .await
        .expect("create the commit-wide view table");

    let writer = repository.clone().with_view_writer(postgres_view_writer());
    let rows = PostgresViewRows { pool: pool.clone() };
    commit_view_cases(&writer, &rows).await;

    // The rebuild fence (C10) over the same private tables, with an injected clock: the lock is
    // table-wide, and every case leaves it settled.
    let clock = TestClock::starting_now();
    let rebuilds = writer.clone().with_clock(clock.reader());
    Box::pin(
        RebuildSuite::new(&rebuilds, &clock)
            .with_views(PostgresViewRows { pool: pool.clone() })
            .with_head_fixtures(PostgresHeads::new(pool.clone(), events.to_owned()))
            .run(),
    )
    .await;
    Box::pin(projection_rebuild_cases(
        &rebuilds,
        &clock,
        &PostgresViewRows { pool: pool.clone() },
    ))
    .await;

    let id = |label: &str| {
        EntityId::new(format!(
            "conf-views-{label}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    };

    // Without a writer the commit is refused before any write, never silently dropped.
    let unconfigured = Store::<Ledger, _>::new(repository, ()).with_commit_projector(
        std::sync::Arc::new(FixedRows::new("unconfigured", |commit| {
            vec![ViewOperation::put(
                "p",
                "s",
                commit.streams[0].entity_id.clone(),
                "",
                "{}",
            )]
        })),
    );
    let unwritten = id("unconfigured");
    let error = unconfigured
        .execute(&unwritten, Add(1), Metadata::new("local"))
        .await
        .expect_err("no view writer");
    assert!(
        matches!(
            error.as_store_error(),
            Some(Error::ViewsNotConfigured {
                backend: "postgres"
            })
        ),
        "{error:?}"
    );
    assert!(repository.events(&unwritten).await.unwrap().is_empty());

    // A statement the database rejects for another reason is the operation's failure, with the
    // SQLSTATE as its reason and the database error as its cause. Nothing else of the commit
    // survives.
    let failing = repository.clone().with_view_writer(std::sync::Arc::new(
        |operation: &ViewOperation| {
            // A NOT NULL violation (23502): the row's entity ID is a null parameter.
            Ok(PgStatement::new(
                format!(
                    "INSERT INTO {PG_COMMIT_VIEWS_TABLE} (pk, sk, entity_id, entity_type, payload) \
                     VALUES ($1, $2, NULL, '', '')"
                ),
                vec![
                    Box::new(operation.pk.clone()),
                    Box::new(operation.sk.clone()),
                ],
            ))
        },
    ));
    let store = Store::<Ledger, _>::new(&failing, ()).with_commit_projector(std::sync::Arc::new(
        FixedRows::new("broken", |commit| {
            let owner = commit.streams[0].entity_id.clone();

            vec![
                ViewOperation::put("sqlstate", "first", owner.clone(), "", "{}"),
                ViewOperation::put("sqlstate", "second", owner, "", "{}"),
            ]
        }),
    ));
    let broken = id("sqlstate");
    let error = store
        .execute(&broken, Add(1), Metadata::new("local"))
        .await
        .expect_err("the statement fails");
    match error.as_store_error() {
        Some(Error::ProjectorOperation(failed)) => {
            assert_eq!(
                (
                    failed.projector.as_str(),
                    failed.projector_index,
                    failed.operation
                ),
                ("broken", 0, 0)
            );
            assert_eq!(failed.reason, "23502");
            assert!(failed.mapped().is_none());
            assert!(failed.cause().is_some(), "the database error is the cause");
        }
        other => panic!("want a projector operation failure, got {other:?}"),
    }
    assert!(repository.events(&broken).await.unwrap().is_empty());

    // A writer that returns an empty statement, or fails, refuses the commit before any write.
    for (label, writer) in [
        (
            "empty",
            std::sync::Arc::new(|_: &ViewOperation| Ok(PgStatement::new("  ", Vec::new())))
                as ViewWriter,
        ),
        (
            "failing-writer",
            std::sync::Arc::new(|_: &ViewOperation| {
                Err(Error::InvalidArgument("no such view".into()))
            }) as ViewWriter,
        ),
    ] {
        let refusing = repository.clone().with_view_writer(writer);
        let store = Store::<Ledger, _>::new(&refusing, ()).with_commit_projector(
            std::sync::Arc::new(FixedRows::new("refused", |commit| {
                vec![ViewOperation::put(
                    "p",
                    "s",
                    commit.streams[0].entity_id.clone(),
                    "",
                    "{}",
                )]
            })),
        );
        let refused = id(label);
        let error = store
            .execute(&refused, Add(1), Metadata::new("local"))
            .await
            .expect_err("the writer refuses");
        assert!(
            matches!(
                error.as_store_error(),
                Some(Error::InvalidViewOperation(_) | Error::InvalidArgument(_))
            ),
            "{label}: {error:?}"
        );
        assert!(repository.events(&refused).await.unwrap().is_empty());
    }

    // A put that requires the row to be absent: only a violation of the (pk, sk) key constraint is
    // its failed condition. A unique violation of another constraint (here, a unique payload) is an
    // ordinary failure of the statement, with the SQLSTATE as its reason and no mapped error.
    let unique = format!("{PG_COMMIT_VIEWS_TABLE}_unique");
    pool.get()
        .await
        .unwrap()
        .batch_execute(&format!(
            "CREATE TABLE IF NOT EXISTS {unique} (
                 pk text NOT NULL, sk text NOT NULL, entity_id text NOT NULL,
                 entity_type text NOT NULL, payload text NOT NULL UNIQUE, PRIMARY KEY (pk, sk))"
        ))
        .await
        .expect("create the unique-payload view table");
    let table = unique.clone();
    let constrained = repository.clone().with_view_writer(std::sync::Arc::new(
        move |operation: &ViewOperation| {
            Ok(PgStatement::new(
                format!(
                    "INSERT INTO {table} (pk, sk, entity_id, entity_type, payload) \
                     VALUES ($1, $2, '', '', $3)"
                ),
                vec![
                    Box::new(operation.pk.clone()),
                    Box::new(operation.sk.clone()),
                    Box::new(String::from_utf8_lossy(&operation.payload).into_owned()),
                ],
            )
            .with_key_constraint(format!("{table}_pkey")))
        },
    ));
    let claims = |name: &'static str, rows: fn(EntityId) -> Vec<ViewOperation>| {
        Store::<Ledger, _>::new(&constrained, ()).with_commit_projector(std::sync::Arc::new(
            FixedRows::new(name, move |commit| {
                rows(commit.streams[0].entity_id.clone())
            }),
        ))
    };
    claims("first", |owner| {
        vec![ViewOperation::put("pk", "a", owner, "", "same").if_absent()]
    })
    .execute(&id("unique-first"), Add(1), Metadata::new("local"))
    .await
    .expect("the first row");

    let other = id("unique-other");
    let error = claims("second", |owner| {
        vec![
            ViewOperation::put("pk", "b", owner, "", "same")
                .if_absent()
                .on_conflict(NameTaken("b".into())),
        ]
    })
    .execute(&other, Add(1), Metadata::new("local"))
    .await
    .expect_err("the payload collides");
    match error.as_store_error() {
        Some(Error::ProjectorOperation(failed)) => {
            assert_eq!(
                failed.reason, "23505",
                "the SQLSTATE, not a failed condition"
            );
            assert!(failed.mapped().is_none());
            assert!(failed.cause().is_some());
        }
        other => panic!("want a projector operation failure, got {other:?}"),
    }
    assert!(repository.events(&other).await.unwrap().is_empty());

    let taken = id("unique-key");
    let error = claims("third", |owner| {
        vec![
            ViewOperation::put("pk", "a", owner, "", "different")
                .if_absent()
                .on_conflict(NameTaken("a".into())),
        ]
    })
    .execute(&taken, Add(1), Metadata::new("local"))
    .await
    .expect_err("the key exists");
    match error.as_store_error() {
        Some(Error::ProjectorOperation(failed)) => {
            assert_eq!(failed.reason, REASON_CONDITION_FAILED);
            assert!(failed.mapped().is_some());
        }
        other => panic!("want a failed condition, got {other:?}"),
    }

    // The writer must name the key constraint of an absent put.
    let unnamed =
        repository
            .clone()
            .with_view_writer(std::sync::Arc::new(|operation: &ViewOperation| {
                Ok(PgStatement::new(
                    "INSERT INTO nowhere VALUES ($1)",
                    vec![Box::new(operation.pk.clone())],
                ))
            }));
    let store = Store::<Ledger, _>::new(&unnamed, ()).with_commit_projector(std::sync::Arc::new(
        FixedRows::new("unnamed", |commit| {
            vec![
                ViewOperation::put("p", "s", commit.streams[0].entity_id.clone(), "", "{}")
                    .if_absent(),
            ]
        }),
    ));
    let refused = id("unnamed");
    let error = store
        .execute(&refused, Add(1), Metadata::new("local"))
        .await
        .expect_err("no key constraint named");
    assert!(
        matches!(error.as_store_error(), Some(Error::InvalidViewOperation(_))),
        "{error:?}"
    );
    assert!(repository.events(&refused).await.unwrap().is_empty());

    pool.get()
        .await
        .unwrap()
        .batch_execute(&format!("DROP TABLE {unique}"))
        .await
        .expect("drop the unique-payload view table");

    pool.get()
        .await
        .unwrap()
        .batch_execute(&format!("DROP TABLE {PG_COMMIT_VIEWS_TABLE}"))
        .await
        .expect("drop the commit-wide view table");
}

/// Fresh and unmigrated databases (ADR 0007): the first write to empty tables declares
/// `stream-heads`, and tables with an event row and no marker are refused by every write path
/// while `entity_heads` keeps aggregating the log until the feature is declared.
async fn postgres_fresh_and_unmigrated_databases() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pool = drivers::postgres_pool().unwrap();
    let handle = |events: &str| {
        PostgresRepository::new(pool.clone())
            .with_tables(events, &format!("{events}s"))
            .unwrap()
    };
    let drop_tables = |events: String| {
        let pool = pool.clone();
        async move {
            pool.get()
                .await
                .unwrap()
                .batch_execute(&format!(
                    "DROP TABLE {events}, {events}s, {events}__evt_features, {events}__evt_command_outcomes, {events}__evt_stream_heads, {events}__evt_views"
                ))
                .await
                .unwrap();
        }
    };

    // Empty tables.
    let events = format!("fr_{nanos:x}");
    let fresh = handle(&events);
    fresh.ensure_schema().await.unwrap();
    fresh_store_cases(&fresh, &handle(&events)).await;
    drop_tables(events).await;

    // Empty tables, initialized explicitly.
    let events = format!("in_{nanos:x}");
    let initialized = handle(&events);
    initialized.ensure_schema().await.unwrap();
    let heads = PostgresHeads::new(pool.clone(), events.clone());
    initialization_cases(&initialized, &handle(&events), &heads, false).await;
    drop_tables(events).await;

    // Tables with an event row and no marker.
    let events = format!("um_{nanos:x}");
    let legacy = handle(&events);
    legacy.ensure_schema().await.unwrap();
    let heads = PostgresHeads::new(pool.clone(), events.clone());
    let id = EntityId::new("legacy-stream");
    let event = |sequence: i64| SerializedEvent {
        id: EventId::for_event(&id, EventSequence(sequence)),
        sequence: EventSequence(sequence),
        event_type: EventType::new("ledger:added"),
        version: EventVersion(1),
        entity_id: id.clone(),
        entity_type: Ledger::TYPE.into(),
        payload: br#"{"amount":1}"#.to_vec(),
        metadata: Metadata::new("local"),
    };
    HeadFixtures::<PostgresRepository>::append_bypassing_head(&heads, &event(1))
        .await
        .unwrap();
    // A snapshot a writer without heads wrote, so that compaction reaches the marker check.
    legacy
        .pool()
        .get()
        .await
        .unwrap()
        .execute(
            &format!(
                "INSERT INTO {events}s (entity_id, entity_type, sequence, event_sequence, payload) \
                 VALUES ($1, $2, 1, 1, $3)"
            ),
            &[&id.as_str(), &Ledger::TYPE, &b"{}".to_vec()],
        )
        .await
        .unwrap();

    for result in [
        legacy.commit(Commit::new(id.clone(), vec![event(2)])).await,
        legacy
            .put_snapshot(&SerializedSnapshot {
                entity_type: Ledger::TYPE.into(),
                entity_id: id.clone(),
                sequence: EventSequence(1),
                event_sequence: EventSequence(0),
                payload: b"{}".to_vec(),
            })
            .await
            .map(|_| ()),
        legacy
            .compact_below(&id, EventSequence(1))
            .await
            .map(|_| ()),
    ] {
        assert!(
            matches!(result, Err(Error::StreamHeadsNotMigrated)),
            "an unmigrated database refuses writes: {result:?}"
        );
    }
    assert!(
        matches!(
            legacy.initialize().await,
            Err(Error::StreamHeadsNotMigrated)
        ),
        "initializing a populated database is refused"
    );
    assert!(legacy.read_manifest().await.unwrap().is_none());
    assert_eq!(
        legacy.entity_heads(None).await.unwrap().get(&id),
        Some(&EventSequence(1)),
        "until stream-heads is declared, entity heads aggregate the log"
    );

    // After the declaration entity heads read the heads table, which the (hand-run) backfill
    // fills; without a heads row the stream is not listed.
    legacy.declare_stream_heads().await.unwrap();
    assert_eq!(legacy.entity_heads(None).await.unwrap().get(&id), None);
    legacy
        .pool()
        .get()
        .await
        .unwrap()
        .execute(
            &format!(
                "INSERT INTO {}__evt_stream_heads (entity_id, entity_type, head) VALUES ($1, $2, 1)",
                events
            ),
            &[&id.as_str(), &Ledger::TYPE],
        )
        .await
        .unwrap();
    assert_eq!(
        legacy.entity_heads(None).await.unwrap().get(&id),
        Some(&EventSequence(1))
    );
    legacy
        .commit(Commit::new(id.clone(), vec![event(2)]))
        .await
        .expect("a migrated database accepts writes");
    drop_tables(events).await;
}

/// Reads outcome rows back through raw SQL: bytea response, jsonb events and rejection, and the
/// CHECK constraint that mirrors the once-in-final-state rule.
async fn postgres_outcome_row_matches_the_wire_format(
    repository: &PostgresRepository,
    events: &str,
) {
    use photon_evt::{Outcome, Rejection};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let stream = EntityId::new(format!("conf-{nanos}-outcome-wire"));
    let event = SerializedEvent {
        id: EventId::for_event(&stream, EventSequence(1)),
        sequence: EventSequence(1),
        event_type: EventType::new("ledger:added"),
        version: EventVersion(1),
        entity_id: stream.clone(),
        entity_type: "ledger".into(),
        payload: br#"{"amount":1}"#.to_vec(),
        metadata: Metadata::default(),
    };

    let accepted = Outcome::accepted(format!("wire-{nanos}-accepted"), "digest")
        .with_response(vec![0, 1, 255]);
    repository
        .commit(Commit::new(stream.clone(), vec![event]).with_outcome(accepted.clone()))
        .await
        .expect("commit with an outcome");
    let rejected = Outcome::rejected(
        format!("wire-{nanos}-rejected"),
        "digest",
        Rejection::new("taken", "a<b>").with_details(serde_json::json!({"k": 1})),
    );
    repository
        .commit(Commit::new(stream.clone(), Vec::new()).with_outcome(rejected.clone()))
        .await
        .expect("outcome-only commit");

    let client = repository.pool().get().await.expect("connect");
    let sql = format!(
        "SELECT status, digest, response, events::text, rejection::text, code \
         FROM {events}__evt_command_outcomes WHERE command_id = $1"
    );

    let row = client
        .query_one(&sql, &[&accepted.command_id.as_str()])
        .await
        .expect("accepted row");
    assert_eq!(row.get::<_, String>(0), "accepted");
    assert_eq!(row.get::<_, Option<Vec<u8>>>(2), Some(vec![0, 1, 255]));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(row.get(3)).unwrap(),
        serde_json::json!([stream.as_str().to_owned() + ":1"])
    );
    assert_eq!(row.get::<_, Option<String>>(4), None);
    assert_eq!(row.get::<_, Option<String>>(5), None);

    let row = client
        .query_one(&sql, &[&rejected.command_id.as_str()])
        .await
        .expect("rejected row");
    assert_eq!(row.get::<_, String>(0), "rejected");
    assert_eq!(row.get::<_, Option<Vec<u8>>>(2), None);
    assert_eq!(row.get::<_, String>(3), "[]");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(row.get(4)).unwrap(),
        serde_json::json!({"code": "taken", "message": "a<b>", "details": {"k": 1}})
    );
    assert_eq!(row.get::<_, Option<String>>(5), Some("taken".into()));

    // The CHECK constraints refuse a rejected row without its rejection.
    let violation = client
        .execute(
            &format!(
                "INSERT INTO {events}__evt_command_outcomes (command_id, status, digest) \
                 VALUES ($1, 'rejected', 'd')"
            ),
            &[&format!("wire-{nanos}-bad")],
        )
        .await
        .expect_err("a rejected row without a rejection violates the constraint");
    assert_eq!(
        violation.code(),
        Some(&tokio_postgres::error::SqlState::CHECK_VIOLATION)
    );
}

/// A transaction with a projector statement and no written event is refused before any write.
async fn postgres_refuses_projector_only_transactions(repository: &PostgresRepository) {
    let mut commit = photon_evt::TransactionCommit::new(Vec::new());
    commit.operations = vec![PgStatement::new("SELECT 1/0", Vec::new())];

    let err = repository.commit_transaction(commit).await.unwrap_err();

    assert!(
        matches!(err, Error::ProjectorWithoutEvents { operations: 1 }),
        "refused before the statement ever runs: {err:?}"
    );

    // The single-stream path refuses the same shape.
    let single = Commit::new("projector-only", Vec::new())
        .with_operations([PgStatement::new("SELECT 1/0", Vec::new())]);
    let err = repository.commit(single).await.unwrap_err();
    assert!(
        matches!(err, Error::ProjectorWithoutEvents { operations: 1 }),
        "refused before the statement ever runs: {err:?}"
    );
}

fn legacy_stream(id: EntityId) -> (Vec<SerializedEvent>, SerializedSnapshot) {
    let events = (1..=2)
        .map(|sequence| SerializedEvent {
            id: EventId::for_event(&id, EventSequence(sequence)),
            sequence: EventSequence(sequence),
            event_type: EventType::new("ledger:added"),
            version: EventVersion(1),
            entity_id: id.clone(),
            entity_type: Ledger::TYPE.into(),
            payload: br#"{"amount":1}"#.to_vec(),
            metadata: Metadata::new("local"),
        })
        .collect();
    let snapshot = SerializedSnapshot {
        entity_type: Ledger::TYPE.into(),
        entity_id: id,
        sequence: EventSequence(1),
        event_sequence: EventSequence(1),
        payload: br#"{"total":1,"entries":1}"#.to_vec(),
    };
    (events, snapshot)
}

async fn legacy_reserved_reads<R: Repository>(
    repository: &R,
    events: &[SerializedEvent],
    snapshot: &SerializedSnapshot,
) {
    let id = &snapshot.entity_id;
    assert_eq!(repository.events(id).await.unwrap(), events);
    assert_eq!(
        repository.events_after(id, EventSequence(1)).await.unwrap(),
        events[1..]
    );
    assert_eq!(
        repository.latest(id, 2).await.unwrap(),
        events.iter().rev().cloned().collect::<Vec<_>>()
    );
    assert_eq!(
        repository.snapshot(id).await.unwrap().as_ref(),
        Some(snapshot)
    );
    for filter in [None, Some(&snapshot.entity_type)] {
        assert!(
            !repository
                .entity_heads(filter)
                .await
                .unwrap()
                .contains_key(id)
        );
    }
}

async fn dynamo_legacy_reserved_reads(repository: &DynamoRepository) {
    use photon_evt::dynamodb::to_sdk_item;
    use photon_evt::wire::dynamo::{event_item, snapshot_item};

    for prefix in ["SYSTEM#", "command-outcome#"] {
        let (events, snapshot) = legacy_stream(EntityId::new(format!("{prefix}legacy-account")));
        // Legacy rows predate write-time namespace refusal; seed through the raw backend.
        let mut items = events
            .iter()
            .map(|event| event_item(event, None).unwrap())
            .collect::<Vec<_>>();
        items.push(snapshot_item(&snapshot, None).unwrap());
        for item in items {
            repository
                .client()
                .put_item()
                .table_name(repository.table())
                .set_item(Some(to_sdk_item(&item)))
                .send()
                .await
                .unwrap();
        }
        legacy_reserved_reads(repository, &events, &snapshot).await;
        for sequence in 0..=2 {
            repository
                .client()
                .delete_item()
                .table_name(repository.table())
                .key("pk", AttributeValue::S(snapshot.entity_id.to_string()))
                .key("sk", AttributeValue::N(sequence.to_string()))
                .send()
                .await
                .unwrap();
        }
    }
}

async fn postgres_legacy_reserved_reads(
    repository: &PostgresRepository,
    events_table: &str,
    snapshots_table: &str,
) {
    use photon_evt::wire::postgres::EventRow;

    let client = repository.pool().get().await.unwrap();
    for prefix in ["SYSTEM#", "command-outcome#"] {
        let (events, snapshot) = legacy_stream(EntityId::new(format!("{prefix}legacy-account")));
        for event in &events {
            let row = EventRow::from_event(event).unwrap();
            client
                .execute(
                    &format!(
                        "INSERT INTO {events_table}
                (entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text::jsonb)"
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
                .unwrap();
        }
        client
            .execute(
                &format!(
                    "INSERT INTO {snapshots_table}
            (entity_id, entity_type, sequence, event_sequence, payload) VALUES ($1, $2, 1, 1, $3)"
                ),
                &[
                    &snapshot.entity_id.as_str(),
                    &snapshot.entity_type.as_str(),
                    &snapshot.payload,
                ],
            )
            .await
            .unwrap();
        legacy_reserved_reads(repository, &events, &snapshot).await;
    }
}

/// Reads the features row back through raw SQL and asserts the stored JSON text is exactly the
/// manifest's encoding, matching the DynamoDB attribute byte for byte.
async fn postgres_features_text_matches_the_manifest(
    repository: &PostgresRepository,
    events_table: &str,
) {
    use photon_evt::wire::postgres::features_table;

    let manifest = repository
        .read_manifest()
        .await
        .expect("read the manifest")
        .expect("the feature-marker row exists");

    let row = repository
        .pool()
        .get()
        .await
        .expect("connect to PostgreSQL")
        .query_opt(
            &format!(
                "SELECT features, history_dependent, retention FROM {} WHERE id = 0",
                features_table(events_table)
            ),
            &[],
        )
        .await
        .expect("read the features row")
        .expect("the features row");

    assert_eq!(row.get::<_, String>(0), manifest.features_json());
    assert_eq!(
        row.get::<_, String>(1),
        manifest
            .history_dependent_json()
            .unwrap_or_else(|| "[]".into())
    );
    assert_eq!(
        row.get::<_, String>(2),
        manifest.retention_json().unwrap_or_else(|| "[]".into())
    );
    assert_eq!(
        manifest.features_json(),
        r#"{"stream-heads":1}"#,
        "the reset manifest declares stream-heads alone, as a sorted JSON object"
    );
}

/// Collects every batch a stream yields, failing on an error.
async fn batches<S>(stream: S) -> Vec<Vec<SerializedEvent>>
where
    S: futures_core::Stream<Item = Result<Vec<SerializedEvent>, Error>>,
{
    let mut stream = std::pin::pin!(stream);
    let mut batches = Vec::new();
    while let Some(batch) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        batches.push(batch.expect("a batch"));
    }

    batches
}

fn assert_no_reserved(batches: &[Vec<SerializedEvent>]) {
    for event in batches.iter().flatten() {
        assert!(
            !event.entity_id.as_str().starts_with("SYSTEM#")
                && !event.entity_id.as_str().starts_with("command-outcome#"),
            "a reserved-namespace event leaked into the enumeration: {}",
            event.id
        );
    }
}

/// Enumeration over DynamoDB: the shared cases, reserved legacy rows and the marker row skipped by
/// the Scan, and pagination with a one-item page.
async fn dynamo_enumeration(repository: &DynamoRepository) {
    use photon_evt::dynamodb::to_sdk_item;
    use photon_evt::wire::dynamo::{event_item, snapshot_item};

    // Seed legacy rows in both reserved namespaces, plus the feature-marker row.
    let mut seeded = Vec::new();
    for prefix in ["SYSTEM#", "command-outcome#"] {
        let (events, snapshot) = legacy_stream(EntityId::new(format!("{prefix}enumerate-legacy")));
        let mut items = events
            .iter()
            .map(|event| event_item(event, None).unwrap())
            .collect::<Vec<_>>();
        items.push(snapshot_item(&snapshot, None).unwrap());
        for item in items {
            repository
                .client()
                .put_item()
                .table_name(repository.table())
                .set_item(Some(to_sdk_item(&item)))
                .send()
                .await
                .unwrap();
        }
        seeded.push(snapshot.entity_id.to_string());
    }
    repository
        .update_manifest(|manifest| {
            manifest.history_dependent.insert("enumerate-marker".into());
        })
        .await
        .unwrap();

    // A head item with no events or snapshot (an orphan) is neither an event nor an entity: the
    // unfiltered Scan skips every row below `sk = 1`, and the heads scan skips `sk < 0`.
    let orphan = EntityId::new(format!("orphan-head-{}", std::process::id()));
    repository
        .client()
        .put_item()
        .table_name(repository.table())
        .set_item(Some(photon_evt::dynamodb::to_sdk_item(
            &photon_evt::wire::dynamo::head_item(&orphan, EventSequence(3), None),
        )))
        .send()
        .await
        .unwrap();
    enumeration_cases(repository).await;
    assert_no_reserved(&batches(repository.stream_all_events(StreamFilter::all())).await);
    assert!(
        batches(repository.stream_all_events(StreamFilter::all()))
            .await
            .iter()
            .flatten()
            .all(|event| event.entity_id != orphan),
        "an orphan head item is not an event"
    );
    assert!(
        !repository
            .entity_heads(None)
            .await
            .unwrap()
            .contains_key(&orphan),
        "an orphan head item is not an entity"
    );
    let partition = repository.partition_items(&orphan).await.unwrap();
    assert_eq!(
        partition.len(),
        1,
        "the partition listing returns the head item like any other row"
    );
    repository
        .client()
        .delete_item()
        .table_name(repository.table())
        .key("pk", AttributeValue::S(orphan.to_string()))
        .key("sk", AttributeValue::N("-1".into()))
        .send()
        .await
        .unwrap();

    // A small Scan page forces pagination: 120 events cannot fit in one page of at most 50 items,
    // so the stream must continue across pages without dropping or repeating one.
    let paged = DynamoRepository::new(repository.client().clone(), repository.table())
        .with_scan_page_size(50);
    let ns = format!("page-{}", std::process::id());
    let id = EntityId::new(format!("{ns}-stream"));
    let entity_type = format!("{ns}-ledger");
    let events: Vec<_> = (1..=120)
        .map(|sequence| SerializedEvent {
            id: EventId::for_event(&id, EventSequence(sequence)),
            sequence: EventSequence(sequence),
            event_type: EventType::new("ledger:added"),
            version: EventVersion(1),
            entity_id: id.clone(),
            entity_type: entity_type.as_str().into(),
            payload: br#"{"amount":1}"#.to_vec(),
            metadata: Metadata::new("local"),
        })
        .collect();
    for chunk in events.chunks(60) {
        paged
            .commit(Commit::new(id.clone(), chunk.to_vec()))
            .await
            .unwrap();
    }

    let pages = batches(paged.stream_all_events(StreamFilter::of_type(entity_type.as_str()))).await;
    assert!(pages.len() >= 3, "120 events span at least three pages");
    assert!(pages.iter().all(|page| page.len() <= 50));
    let streamed: Vec<_> = pages.into_iter().flatten().collect();
    let mut sorted = streamed.clone();
    sorted.sort_by_key(|event| event.sequence);
    assert_eq!(
        sorted, events,
        "pagination neither drops nor repeats an event"
    );

    // Entities fold from events spread over pages.
    let mut entities = std::pin::pin!(
        paged.stream_entities::<Ledger>(StreamFilter::of_type(entity_type.as_str()))
    );
    let loaded = std::future::poll_fn(|cx| entities.as_mut().poll_next(cx))
        .await
        .expect("an entity")
        .expect("it folds");
    assert_eq!(
        (loaded.aggregate().total, loaded.sequence()),
        (120, EventSequence(120))
    );

    // Clean up what this run seeded outside the namespaced cases.
    for id in seeded {
        for sequence in 0..=2 {
            repository
                .client()
                .delete_item()
                .table_name(repository.table())
                .key("pk", AttributeValue::S(id.clone()))
                .key("sk", AttributeValue::N(sequence.to_string()))
                .send()
                .await
                .unwrap();
        }
    }
    for sequence in std::iter::once(-1).chain(events.iter().map(|event| event.sequence.0)) {
        repository
            .client()
            .delete_item()
            .table_name(repository.table())
            .key("pk", AttributeValue::S(id.to_string()))
            .key("sk", AttributeValue::N(sequence.to_string()))
            .send()
            .await
            .unwrap();
    }
}

/// Enumeration over PostgreSQL: the shared cases, with legacy rows in the reserved namespaces
/// seeded out of band and skipped.
async fn postgres_enumeration(repository: &PostgresRepository, events_table: &str) {
    use photon_evt::wire::postgres::EventRow;

    let client = repository.pool().get().await.unwrap();
    for prefix in ["SYSTEM#", "command-outcome#"] {
        let (events, _) = legacy_stream(EntityId::new(format!("{prefix}enumerate-legacy")));
        for event in &events {
            let row = EventRow::from_event(event).unwrap();
            client
                .execute(
                    &format!(
                        "INSERT INTO {events_table} \
                         (entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8::text::jsonb)"
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
                .unwrap();
        }
    }
    drop(client);

    enumeration_cases(repository).await;
    assert_no_reserved(&batches(repository.stream_all_events(StreamFilter::all())).await);
}

// ---------------------------------------------------------------------------------------------
// The rebuild fence's DynamoDB requests (C10), against a recording endpoint
// ---------------------------------------------------------------------------------------------

/// A minimal DynamoDB endpoint on a local port, answering each request with `respond(target,
/// body)` and recording every request as `(target, body)`. It needs no emulator: it stands in for
/// one where the cases pin the exact requests and choose the responses.
struct RecordingEndpoint {
    url: String,
    calls: std::sync::Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>>,
}

type Responder = dyn Fn(&str, &serde_json::Value) -> (u16, serde_json::Value) + Send + Sync;

impl RecordingEndpoint {
    fn start(
        respond: impl Fn(&str, &serde_json::Value) -> (u16, serde_json::Value) + Send + Sync + 'static,
    ) -> Self {
        use std::io::{BufRead, BufReader, Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a local port");
        let url = format!("http://{}", listener.local_addr().unwrap());
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let respond: std::sync::Arc<Responder> = std::sync::Arc::new(respond);

        let recorded = calls.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let (recorded, respond) = (recorded.clone(), respond.clone());

                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut writer = stream;

                    // One request after another on a kept-alive connection, until it closes.
                    loop {
                        let mut target = String::new();
                        let mut length = 0;
                        let mut line = String::new();
                        loop {
                            line.clear();
                            if reader.read_line(&mut line).unwrap_or(0) == 0 {
                                return;
                            }
                            let header = line.trim_end();
                            if header.is_empty() {
                                break;
                            }
                            if let Some((name, value)) = header.split_once(':') {
                                match name.trim().to_ascii_lowercase().as_str() {
                                    "x-amz-target" => target = value.trim().to_owned(),
                                    "content-length" => length = value.trim().parse().unwrap(),
                                    _ => {}
                                }
                            }
                        }

                        let mut body = vec![0; length];
                        reader.read_exact(&mut body).unwrap();
                        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        let operation = target.rsplit('.').next().unwrap_or_default().to_owned();
                        let (status, response) = respond(&operation, &body);
                        recorded.lock().unwrap().push((operation, body));

                        let response = response.to_string();
                        write!(
                            writer,
                            "HTTP/1.1 {status} X\r\nContent-Type: application/x-amz-json-1.0\r\n\
                             Content-Length: {}\r\n\r\n{response}",
                            response.len()
                        )
                        .unwrap();
                    }
                });
            }
        });

        Self { url, calls }
    }

    /// A repository over the endpoint, with the given tables, reading `clock`.
    fn repository(&self, table: &str, views: &str, clock: &TestClock) -> DynamoRepository {
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .region(aws_sdk_dynamodb::config::Region::new("us-west-2"))
            .endpoint_url(&self.url)
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                "test", "test", None, None, "test",
            ))
            .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
            .build();

        DynamoRepository::new(aws_sdk_dynamodb::Client::from_conf(config), table)
            .with_lazy_initialization(true)
            .with_views_table(views)
            .with_clock(clock.reader())
    }

    /// The recorded requests, in the shape of the shared fixtures' `calls`.
    fn calls(&self) -> Vec<serde_json::Value> {
        use serde_json::{Value, json};

        let action = |operation: &str, body: &Value| {
            let mut action = json!({"operation": operation, "tableName": body["TableName"]});
            for (from, to) in [
                ("Key", "key"),
                ("Item", "item"),
                ("UpdateExpression", "updateExpression"),
                ("ConditionExpression", "conditionExpression"),
                ("ExpressionAttributeNames", "expressionAttributeNames"),
                ("ExpressionAttributeValues", "expressionAttributeValues"),
            ] {
                if !body[from].is_null() {
                    action[to] = body[from].clone();
                }
            }

            action
        };

        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|(operation, body)| match operation.as_str() {
                "GetItem" => json!({"call": "GetItem",
                    "consistentRead": body["ConsistentRead"],
                    "actions": [action("Get", body)]}),
                "UpdateItem" => json!({"call": "UpdateItem", "actions": [action("Update", body)]}),
                "TransactWriteItems" => json!({"call": "TransactWriteItems",
                    "actions": body["TransactItems"].as_array().unwrap().iter().map(|item| {
                        let (operation, inner) = item.as_object().unwrap().iter().next().unwrap();
                        action(operation, inner)
                    }).collect::<Vec<_>>()}),
                other => panic!("unexpected request {other}"),
            })
            .collect()
    }
}

/// A cancelled transaction whose failed item is `failed`, of `items`.
fn cancelled_response(items: usize, failed: usize) -> (u16, serde_json::Value) {
    let reasons: Vec<_> = (0..items)
        .map(|index| {
            serde_json::json!({"Code": if index == failed { "ConditionalCheckFailed" } else { "None" }})
        })
        .collect();

    (
        400,
        serde_json::json!({
            "__type": "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
            "message": "Transaction cancelled",
            "CancellationReasons": reasons,
        }),
    )
}

/// The rebuild-fence vectors Go recorded (`rebuildFence` in `feature-marker.json`): run against an
/// endpoint that holds each vector's state, every lifecycle operation sends exactly the recorded
/// requests, in order, and returns the recorded result.
#[tokio::test]
async fn dynamo_rebuild_requests_replay_the_go_vectors() {
    use photon_evt::RebuildFence;
    use serde_json::{Value, json};

    let data =
        std::fs::read(evt_compat::scenario::compat_dir().join("fixtures/feature-marker.json"))
            .unwrap();
    let fixtures: Value = serde_json::from_slice(&data).unwrap();

    for vector in fixtures["rebuildFence"].as_array().unwrap() {
        let name = vector["name"].as_str().unwrap().to_owned();
        let before = vector["before"].clone();
        let lost = vector["result"]["error"] == "leaseLost";

        let endpoint = RecordingEndpoint::start(move |operation, body| match operation {
            "GetItem" => {
                let stored = if body["Key"]["pk"]["S"] == "SYSTEM#REBUILD" {
                    &before["lock"]
                } else {
                    &before["generation"]
                };

                (
                    200,
                    if stored.is_null() {
                        json!({})
                    } else {
                        json!({"Item": stored})
                    },
                )
            }
            "UpdateItem" if lost => (
                400,
                json!({"__type": "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
                    "message": "The conditional request failed"}),
            ),
            "TransactWriteItems" if lost => {
                let items = body["TransactItems"].as_array().unwrap().len();
                // A release fails on its lock delete, a guarded write on its guard, the last item.
                let guarded = body["TransactItems"][items - 1]["ConditionCheck"].is_object();

                cancelled_response(items, if guarded { items - 1 } else { 0 })
            }
            _ => (200, json!({})),
        });
        let clock = TestClock::at(1_790_000_000);
        let repository = endpoint.repository("evt-compat", "views", &clock);
        let owner = vector["owner"].as_str().unwrap_or_default();
        // The lease a renew, an end, or a guarded write names: its owner and fencing token.
        let held = photon_evt::RebuildLease {
            owner: owner.to_owned(),
            generation: vector["leaseGeneration"].as_i64().unwrap_or_default(),
            lease_expires_at: 0,
        };
        let lease = Duration::from_secs(vector["leaseSeconds"].as_u64().unwrap_or_default());

        let error = |error: Error| match error {
            Error::RebuildLockHeld { .. } => json!({"error": "lockHeld"}),
            Error::RebuildLeaseLost { .. } => json!({"error": "leaseLost"}),
            other => panic!("{name}: unexpected {other}"),
        };
        let generation = |generation: photon_evt::RebuildGeneration| {
            let mut result = json!({"generation": generation.value});
            if generation.force_released {
                result["forceReleased"] = json!(true);
            }

            result
        };

        let result = match vector["operation"].as_str().unwrap() {
            "state" => {
                let state = repository.rebuild_state().await.unwrap();
                let mut result = generation(state.generation);
                if let Some(lock) = state.lock {
                    result["lockOwner"] = json!(lock.owner);
                    result["leaseExpiresAt"] = json!(lock.lease_expires_at);
                }

                result
            }
            "generation" => generation(repository.rebuild_generation().await.unwrap()),
            "begin" => match repository.begin_rebuild(owner, lease).await {
                Ok(lease) => json!({"generation": lease.generation,
                    "leaseExpiresAt": lease.lease_expires_at}),
                Err(err) => error(err),
            },
            "renew" => match repository.renew_rebuild(&held, lease).await {
                Ok(renewed) => json!({"leaseExpiresAt": renewed.lease_expires_at}),
                Err(err) => error(err),
            },
            "end" => repository
                .end_rebuild(&held)
                .await
                .map_or_else(error, generation),
            "forceRelease" => repository
                .force_release_rebuild(owner)
                .await
                .map_or_else(error, generation),
            "writeViews" => {
                let views = [
                    photon_evt::ProjectedView::new(
                        "rebuild",
                        0,
                        0,
                        ViewOperation::put("rebuilt", "row-1", "acct-1", "account", r#"{"n":1}"#),
                    ),
                    photon_evt::ProjectedView::new(
                        "rebuild",
                        0,
                        1,
                        ViewOperation::delete("rebuilt", "row-2", "acct-2"),
                    ),
                ];

                match repository.write_rebuild_views(&held, &views).await {
                    Ok(()) => json!({}),
                    Err(err) => error(err),
                }
            }
            other => panic!("{name}: unknown operation {other}"),
        };

        assert_eq!(
            json!(endpoint.calls()),
            vector["calls"],
            "{name}: the requests"
        );
        assert_eq!(result, vector["result"], "{name}: the result");
    }
}

/// `begin_rebuild` reads the generation, then the lock row. A transition that lands between the
/// two reads shows a pair no transaction produces: it reads both again and decides again, and
/// only a pair that stays inconsistent through every attempt is reported.
#[tokio::test]
async fn dynamo_rebuild_begin_reads_again_after_a_torn_read() {
    use photon_evt::RebuildFence;
    use serde_json::json;

    let lock = json!({"pk": {"S": "SYSTEM#REBUILD"}, "sk": {"N": "0"},
        "owner": {"S": "winner"}, "generation": {"N": "1"},
        "leaseExpiresAt": {"N": "1790000600"}});
    let generation = json!({"pk": {"S": "SYSTEM#REBUILD#GENERATION"}, "sk": {"N": "0"},
        "value": {"N": "1"}});

    // The first generation read sees no item; the winner's acquire lands before the lock read.
    let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = reads.clone();
    let (torn_lock, torn_generation) = (lock.clone(), generation.clone());
    let endpoint = RecordingEndpoint::start(move |operation, body| {
        assert_eq!(operation, "GetItem", "a held lock is never written");
        let read = counted.fetch_add(1, Ordering::SeqCst);

        if body["Key"]["pk"]["S"] == "SYSTEM#REBUILD" {
            (200, json!({"Item": torn_lock}))
        } else if read == 0 {
            (200, json!({}))
        } else {
            (200, json!({"Item": torn_generation}))
        }
    });
    let clock = TestClock::at(1_790_000_000);
    let repository = endpoint.repository("evt-compat", "views", &clock);
    let result = repository
        .begin_rebuild("loser", Duration::from_secs(60))
        .await;
    assert!(
        matches!(&result, Err(Error::RebuildLockHeld { owner, .. }) if owner == "winner"),
        "{result:?}"
    );
    assert_eq!(reads.load(Ordering::SeqCst), 4, "two passes of two reads");

    // A pair that stays inconsistent is reported after the last attempt, without a write.
    let endpoint = RecordingEndpoint::start(move |operation, body| {
        assert_eq!(
            operation, "GetItem",
            "an inconsistent state is never written"
        );

        if body["Key"]["pk"]["S"] == "SYSTEM#REBUILD" {
            (200, json!({"Item": lock}))
        } else {
            (200, json!({}))
        }
    });
    let repository = endpoint.repository("evt-compat", "views", &clock);
    let result = repository
        .begin_rebuild("loser", Duration::from_secs(60))
        .await;
    assert!(
        matches!(result, Err(Error::RebuildStateInvalid(_))),
        "{result:?}"
    );
    assert_eq!(
        endpoint.calls().len(),
        2 * photon_evt::MAX_REBUILD_BEGIN_ATTEMPTS,
        "every attempt read both items"
    );
}

// ---------------------------------------------------------------------------------------------
// The rebuild rows and fresh or older stores (C10)
// ---------------------------------------------------------------------------------------------

/// The rebuild rows live in the reserved namespace, so a table that holds nothing but them is
/// still fresh: its first (lazily initializing) writer declares `stream-heads` itself and commits.
async fn dynamo_rebuild_rows_leave_a_table_fresh(client: &aws_sdk_dynamodb::Client) {
    use photon_evt::RebuildFence;

    let table = format!(
        "evt-conformance-rebuild-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    create_dynamo_table(client, &table).await;

    let repository = DynamoRepository::new(client.clone(), &table).with_lazy_initialization(true);
    let lease = repository
        .begin_rebuild("fresh", Duration::from_secs(60))
        .await
        .unwrap();
    repository.end_rebuild(&lease).await.unwrap();

    Store::<Ledger, _>::new(&repository, ())
        .execute("fresh-ledger", Add(1), Metadata::new("local"))
        .await
        .expect("the first writer of a table holding only the rebuild rows commits");
    let manifest = photon_evt::FeatureMarker::read_manifest(&repository)
        .await
        .unwrap()
        .expect("the writer declared the manifest");
    assert!(manifest.declares_stream_heads(), "{manifest:?}");
    assert_eq!(
        repository.rebuild_generation().await.unwrap().value,
        2,
        "the generation survives"
    );

    client
        .delete_table()
        .table_name(&table)
        .send()
        .await
        .expect("delete the table");
}

/// A schema that predates the rebuild fence has neither relation: commits treat the missing lock
/// relation as no lock, the lifecycle fails until `ensure_schema` creates them, and a log holding
/// only the rebuild rows is still fresh for its first writer.
async fn postgres_rebuild_relations(pool: Pool) {
    use photon_evt::RebuildFence;

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let events = format!("rb_{nanos:x}");
    let snapshots = format!("{events}s");
    let repository = PostgresRepository::new(pool.clone())
        .with_tables(&events, &snapshots)
        .unwrap();
    repository.ensure_schema().await.unwrap();

    // Fresh: a rebuild before any commit leaves the log fresh for its first writer.
    let lease = repository
        .begin_rebuild("fresh", Duration::from_secs(60))
        .await
        .unwrap();
    repository.end_rebuild(&lease).await.unwrap();
    let store = Store::<Ledger, _>::new(&repository, ());
    store
        .execute("fresh-ledger", Add(1), Metadata::new("local"))
        .await
        .expect("the first writer of a log holding only the rebuild rows commits");

    // Older: without the relations, a new handle's commits pass the lock check and the lifecycle
    // fails.
    pool.get()
        .await
        .unwrap()
        .batch_execute(&format!(
            "DROP TABLE {events}__evt_rebuild_lock, {events}__evt_rebuild_generation"
        ))
        .await
        .unwrap();
    let older = PostgresRepository::new(pool.clone())
        .with_tables(&events, &snapshots)
        .unwrap();
    Store::<Ledger, _>::new(&older, ())
        .execute("fresh-ledger", Add(2), Metadata::new("local"))
        .await
        .expect("a commit on a log that predates the fence");
    let result = older.begin_rebuild("older", Duration::from_secs(60)).await;
    assert!(matches!(result, Err(Error::Backend(_))), "{result:?}");

    // Once the schema is ensured again, the fence works and fences that handle's commits.
    older.ensure_schema().await.unwrap();
    let lease = older
        .begin_rebuild("older", Duration::from_secs(60))
        .await
        .unwrap();
    let refused = Store::<Ledger, _>::new(&older, ())
        .execute("fresh-ledger", Add(3), Metadata::new("local"))
        .await;
    assert!(
        matches!(
            refused.as_ref().err().and_then(|err| err.as_store_error()),
            Some(Error::RebuildInProgress)
        ),
        "{refused:?}"
    );
    older.end_rebuild(&lease).await.unwrap();

    pool.get()
        .await
        .unwrap()
        .batch_execute(&format!(
            "DROP TABLE {events}, {snapshots}, {events}__evt_features, \
             {events}__evt_command_outcomes, {events}__evt_stream_heads, \
             {events}__evt_rebuild_lock, {events}__evt_rebuild_generation, {events}__evt_views"
        ))
        .await
        .expect("drop the isolated event log");
}

/// A repository over `events` whose sessions default to `REPEATABLE READ`, as a server or role
/// configured with `default_transaction_isolation` would give them.
fn repeatable_read(events: &str, snapshots: &str) -> PostgresRepository {
    use deadpool_postgres::{Manager, ManagerConfig, RecyclingMethod};

    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://evt:evt@localhost:5432/evt_local?sslmode=disable".into());
    let mut config: tokio_postgres::Config = url.parse().unwrap();
    config.options("-c default_transaction_isolation=repeatable\\ read");
    let manager = Manager::from_config(
        config,
        tokio_postgres::NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );

    PostgresRepository::new(Pool::builder(manager).max_size(2).build().unwrap())
        .with_tables(events, snapshots)
        .unwrap()
}

/// A commit reads the rebuild fence after it waits for its locks, so it must read it as of that
/// moment, whatever the server's default isolation. Under `REPEATABLE READ` its snapshot would be
/// the one taken before the wait: a rebuild that began meanwhile would be invisible to it, and the
/// commit would land under the fence.
#[tokio::test]
async fn postgres_commit_sees_a_rebuild_that_began_while_it_waited() {
    use photon_evt::{Commit, EntityId, EventId, EventType, EventVersion, Metadata, RebuildFence};

    // Tables of its own: the rebuild lock it takes must refuse nobody else's commits.
    let events = format!(
        "rc{:x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let snapshots = format!("{events}_s");
    let base = PostgresRepository::new(drivers::postgres_pool().unwrap())
        .with_tables(&events, &snapshots)
        .unwrap();
    base.ensure_schema().await.unwrap();
    let repo = repeatable_read(&events, &snapshots);
    let id = EntityId::from(format!("{events}-waits"));
    let pool = drivers::postgres_pool().unwrap();
    let mut holder = pool.get().await.unwrap();
    let lock = holder.transaction().await.unwrap();
    lock.execute(
        "SELECT pg_advisory_xact_lock(1702261760, hashtext($1))",
        &[&id.as_str()],
    )
    .await
    .unwrap();

    let event = SerializedEvent {
        id: EventId::for_event(&id, EventSequence(1)),
        sequence: EventSequence(1),
        event_type: EventType::new("account:opened"),
        version: EventVersion(1),
        entity_id: id.clone(),
        entity_type: "account".into(),
        payload: b"{}".to_vec(),
        metadata: Metadata::new("local"),
    };
    let committing = {
        let repo = repo.clone();
        let commit = Commit::new(id.clone(), vec![event.clone()]);

        tokio::spawn(async move { repo.commit(commit).await })
    };

    // Wait until the commit is blocked on the entity lock, after its first statement.
    let watcher = pool.get().await.unwrap();
    let waiting = "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted \
                   AND classid = 1702261760::oid \
                   AND objid = (hashtext($1)::bigint & 4294967295)::oid";
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row = watcher.query_one(waiting, &[&id.as_str()]).await.unwrap();
        if row.get::<_, i64>(0) > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the commit never waited on its entity lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let lease = base
        .begin_rebuild("job", Duration::from_secs(60))
        .await
        .unwrap();
    lock.rollback().await.unwrap();

    let refused = committing.await.unwrap();
    assert!(
        matches!(refused, Err(Error::RebuildInProgress)),
        "the commit saw the rebuild that began while it waited: {refused:?}"
    );
    assert!(
        base.events(&id).await.unwrap().is_empty(),
        "nothing landed under the fence"
    );

    base.end_rebuild(&lease).await.unwrap();
    repo.commit(Commit::new(id.clone(), vec![event]))
        .await
        .unwrap();

    drop(watcher);
    pool.get()
        .await
        .unwrap()
        .batch_execute(&format!(
            "DROP TABLE {events}, {snapshots}, {events}__evt_features, {events}__evt_command_outcomes, \
             {events}__evt_stream_heads, {events}__evt_rebuild_lock, {events}__evt_rebuild_generation, {events}__evt_views"
        ))
        .await
        .unwrap();
}

/// The evt-owned views table (ADR 0008) through [`photon_evt::postgres::PgViewRepository`]: the
/// shared view-repository cases, then what is PostgreSQL's own: exact payload bytes, byte order of
/// non-ASCII keys, the combined key bound, and `purge_expired`.
#[tokio::test]
async fn postgres_view_repository_conforms() {
    let events = format!(
        "vr{:x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let snapshots = format!("{events}_s");
    let repository = PostgresRepository::new(drivers::postgres_pool().unwrap())
        .with_tables(&events, &snapshots)
        .unwrap();
    repository.ensure_schema().await.unwrap();
    let views = repository.view_repository();
    assert_eq!(views.table(), format!("{events}__evt_views"));

    view_repository_cases(&views).await;

    // The payload is text, not jsonb: whitespace and key order survive.
    let exact = br#"{ "b": 1,   "a": [ 2 ] }"#.to_vec();
    let mut row = ViewRow::new("exact", "VIEW", "owner", "kind", exact.clone());
    views.put_view(&row).await.unwrap();
    assert_eq!(
        views.get_view("exact").await.unwrap().unwrap().payload,
        exact,
        "the payload bytes"
    );

    // Sort keys order by UTF-8 bytes, as on DynamoDB: "Z" (0x5A) < "b" (0x62) < "é" (0xC3 0xA9).
    for sk in ["é", "b", "Z"] {
        row = ViewRow::new("ordered", sk, "owner", "kind", b"{}".to_vec());
        views.put_view(&row).await.unwrap();
    }
    let order: Vec<String> = views
        .list_views_by_pk("ordered")
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.sk)
        .collect();
    assert_eq!(order, ["Z", "b", "é"], "bytewise order");

    // A key the btree index cannot hold is refused before anything is written.
    let long = ViewRow::new(
        "p".repeat(2_048),
        "s".repeat(1_000),
        "owner",
        "kind",
        b"{}".to_vec(),
    );
    assert!(matches!(
        views.put_view(&long).await,
        Err(Error::InvalidArgument(message)) if message.contains("2600")
    ));

    // At the bound, an incompressible key fits both btree indexes.
    let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut noise = |len: usize| -> String {
        (0..len)
            .map(|_| {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                char::from(b'!' + u8::try_from((seed >> 33) % 94).unwrap())
            })
            .collect()
    };
    let boundary = ViewRow::new(noise(1_598), noise(998), "owner", noise(4), b"{}".to_vec());
    assert_eq!(
        boundary.pk.len() + boundary.sk.len() + boundary.entity_type.len(),
        2_600
    );
    views.put_view(&boundary).await.unwrap();
    assert!(
        views
            .get_view_at(&boundary.pk, &boundary.sk)
            .await
            .unwrap()
            .is_some()
    );

    // PostgreSQL text cannot hold U+0000: a typed refusal, not the database's error.
    for row in [
        ViewRow::new("nul\0pk", "VIEW", "owner", "kind", b"{}".to_vec()),
        ViewRow::new("nul", "s\0k", "owner", "kind", b"{}".to_vec()),
        ViewRow::new("nul", "VIEW", "own\0er", "kind", b"{}".to_vec()),
        ViewRow::new("nul", "VIEW", "owner", "ki\0nd", b"{}".to_vec()),
        ViewRow::new("nul", "VIEW", "owner", "kind", b"{\"a\":\"\0\"}".to_vec()),
    ] {
        assert!(
            matches!(views.put_view(&row).await, Err(Error::InvalidArgument(_))),
            "{row:?}"
        );
    }
    assert!(matches!(
        views.get_view_at("nul\0pk", "VIEW").await,
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        views.list_views_by_pk_paged("nul\0pk", None, None).await,
        Err(Error::InvalidArgument(_))
    ));

    // A batch keeps a repeated key's last row, whatever order the keys come in.
    views
        .put_views(&[
            ViewRow::new("batch", "b", "owner", "kind", b"{\"n\":1}".to_vec()),
            ViewRow::new("batch", "a", "owner", "kind", b"{\"n\":2}".to_vec()),
            ViewRow::new("batch", "b", "owner", "kind", b"{\"n\":3}".to_vec()),
        ])
        .await
        .unwrap();
    let batch: Vec<(String, Vec<u8>)> = views
        .list_views_by_pk("batch")
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.sk, row.payload))
        .collect();
    assert_eq!(
        batch,
        [
            ("a".to_owned(), b"{\"n\":2}".to_vec()),
            ("b".to_owned(), b"{\"n\":3}".to_vec())
        ]
    );

    // A page without a limit holds at most 1,000 rows and says more follow; the full listing
    // follows the cursor past it.
    let many: Vec<ViewRow> = (0..1_001)
        .map(|i| ViewRow::new("many", format!("{i:04}"), "owner", "many", b"{}".to_vec()))
        .collect();
    views.put_views(&many).await.unwrap();
    let first = views
        .list_views_by_pk_paged("many", None, None)
        .await
        .unwrap();
    assert_eq!(first.views.len(), 1_000);
    assert!(
        first.next_cursor.is_some(),
        "a full unbounded page has a cursor"
    );
    assert_eq!(views.list_views_by_pk("many").await.unwrap().len(), 1_001);
    assert_eq!(
        views.list_views_by_entity_type("many").await.unwrap().len(),
        1_001
    );

    // Expired rows stay readable until a purge, which deletes only them.
    let mut expired = ViewRow::new("ttl", "old", "owner", "kind", b"{}".to_vec());
    expired.ttl = 1_000;
    let mut live = ViewRow::new("ttl", "new", "owner", "kind", b"{}".to_vec());
    live.ttl = 4_102_444_800;
    let forever = ViewRow::new("ttl", "none", "owner", "kind", b"{}".to_vec());
    views
        .put_views(&[expired.clone(), live.clone(), forever.clone()])
        .await
        .unwrap();
    assert!(views.get_view_at("ttl", "old").await.unwrap().is_some());
    assert_eq!(views.purge_expired(2_000).await.unwrap(), 1);
    let left: Vec<String> = views
        .list_views_by_pk("ttl")
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.sk)
        .collect();
    assert_eq!(left, ["new", "none"]);

    repository
        .pool()
        .get()
        .await
        .unwrap()
        .batch_execute(&format!(
            "DROP TABLE {events}, {snapshots}, {events}__evt_features, {events}__evt_command_outcomes, \
             {events}__evt_stream_heads, {events}__evt_rebuild_lock, {events}__evt_rebuild_generation, \
             {events}__evt_views"
        ))
        .await
        .unwrap();
}

/// The views table's rows through its view repository, for the commit-wide view cases.
struct PgViewsTableRows(photon_evt::postgres::PgViewRepository);

impl CommitViewTarget<PostgresRepository> for PgViewsTableRows {
    async fn row(&self, pk: &str, sk: &str) -> Result<Option<ViewRow>, BoxError> {
        Ok(self.0.get_view_at(pk, sk).await?)
    }

    fn max_view_key_bytes(&self) -> Option<usize> {
        Some(photon_evt::wire::postgres::MAX_VIEW_INDEX_KEY_BYTES)
    }
}

/// The evt-owned views table as the commit's view target (`with_views_table()`, ADR 0008): the
/// commit-wide view cases (the byte budget included, since its statements count as DynamoDB
/// items), the journal counter's expected-payload condition, the committed rows through
/// `PgViewRepository`, the rebuild suites, and the `postgres-views` marker feature, which keeps any
/// other writer off the store.
#[tokio::test]
async fn postgres_views_table_conforms() {
    use photon_evt::{POSTGRES_VIEWS_FEATURE, POSTGRES_VIEWS_VERSION};

    let events = format!(
        "vt{:x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let snapshots = format!("{events}_s");
    let base = PostgresRepository::new(drivers::postgres_pool().unwrap())
        .with_tables(&events, &snapshots)
        .unwrap();
    base.ensure_schema().await.unwrap();
    let repository = base.clone().with_views_table();
    assert!(repository.applies_expected_payload());
    assert!(!base.applies_expected_payload());
    let rows = || PgViewsTableRows(repository.view_repository());

    commit_view_cases(&repository, &rows()).await;
    expected_payload_cases(&repository, &rows()).await;
    committed_view_cases(&repository, &repository.view_repository()).await;

    // The first commit declared the feature, and every other writer now refuses the store.
    let manifest = repository.read_manifest().await.unwrap().unwrap();
    assert_eq!(
        manifest.features.get(POSTGRES_VIEWS_FEATURE),
        Some(&POSTGRES_VIEWS_VERSION)
    );
    let id = EntityId::new(format!("{events}-other-writer"));
    let event = SerializedEvent {
        id: EventId::for_event(&id, EventSequence(1)),
        sequence: EventSequence(1),
        event_type: EventType::new("account:opened"),
        version: EventVersion(1),
        entity_id: id.clone(),
        entity_type: "account".into(),
        payload: b"{}".to_vec(),
        metadata: Metadata::new("local"),
    };
    for other in [
        PostgresRepository::new(base.pool().clone())
            .with_tables(&events, &snapshots)
            .unwrap(),
        PostgresRepository::new(base.pool().clone())
            .with_tables(&events, &snapshots)
            .unwrap()
            .with_view_writer(postgres_view_writer()),
    ] {
        assert!(
            matches!(
                other.commit(Commit::new(id.clone(), vec![event.clone()])).await,
                Err(Error::UnsupportedFeature { ref feature, .. }) if feature == POSTGRES_VIEWS_FEATURE
            ),
            "a writer without the views table refuses the store"
        );
    }

    // A clone that switches to a view writer refuses the store too, though its marker
    // validation is cached from the views-table commits it was cloned from.
    let switched = repository.clone().with_view_writer(postgres_view_writer());
    assert!(
        matches!(
            switched.commit(Commit::new(id.clone(), vec![event.clone()])).await,
            Err(Error::UnsupportedFeature { ref feature, .. }) if feature == POSTGRES_VIEWS_FEATURE
        ),
        "a clone switched to a view writer refuses the store"
    );

    // Initialization still works on the declared store (a deploy step treats it like any other),
    // and a repository without the views table refuses the store on every other write path too.
    assert!(
        !repository.initialize().await.unwrap(),
        "already initialized"
    );
    let plain = PostgresRepository::new(base.pool().clone())
        .with_tables(&events, &snapshots)
        .unwrap();
    let foreign = |result: Result<_, Error>| matches!(result, Err(Error::UnsupportedFeature { ref feature, .. }) if feature == POSTGRES_VIEWS_FEATURE);
    let snapshot = SerializedSnapshot {
        entity_type: "account".into(),
        entity_id: EntityId::new(format!("{events}-snapshot")),
        sequence: EventSequence(0),
        event_sequence: EventSequence(0),
        payload: b"{}".to_vec(),
    };
    assert!(
        foreign(plain.put_snapshot(&snapshot).await.map(drop)),
        "a snapshot write"
    );
    {
        use photon_evt::{ProjectedView, RebuildFence};

        // A writer with its own view tables would rebuild into them: refused in the transaction.
        let plain = plain.clone().with_view_writer(postgres_view_writer());
        let lease = plain
            .begin_rebuild("foreign", std::time::Duration::from_secs(60))
            .await
            .unwrap();
        let view = ProjectedView::new(
            "rebuild",
            0,
            0,
            ViewOperation::put(format!("{events}-rebuilt"), "VIEW", id.clone(), "", "{}"),
        );
        assert!(
            foreign(plain.write_rebuild_views(&lease, &[view]).await),
            "a rebuild view write"
        );
        plain.end_rebuild(&lease).await.unwrap();
    }

    // The rebuild fence and the projection rebuild, writing their rows to the views table.
    let clock = TestClock::starting_now();
    let rebuilds = repository.clone().with_clock(clock.reader());
    Box::pin(
        RebuildSuite::new(&rebuilds, &clock)
            .with_views(rows())
            .with_head_fixtures(PostgresHeads::new(base.pool().clone(), events.clone()))
            .run(),
    )
    .await;
    Box::pin(projection_rebuild_cases(&rebuilds, &clock, &rows())).await;
    Box::pin(projection_diff_cases(&rebuilds, &clock, &rows().0)).await;

    base.pool()
        .get()
        .await
        .unwrap()
        .batch_execute(&format!(
            "DROP TABLE {events}, {snapshots}, {events}__evt_features, {events}__evt_command_outcomes, \
             {events}__evt_stream_heads, {events}__evt_rebuild_lock, {events}__evt_rebuild_generation, \
             {events}__evt_views"
        ))
        .await
        .unwrap();
}

/// A private event log on PostgreSQL whose commit-wide views go to its views table, dropped by
/// [`drop_log`].
async fn views_table_log(prefix: &str) -> (PostgresRepository, String, String) {
    let events = format!(
        "{prefix}{:x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let snapshots = format!("{events}_s");
    let repository = PostgresRepository::new(drivers::postgres_pool().unwrap())
        .with_tables(&events, &snapshots)
        .unwrap()
        .with_views_table();
    repository.ensure_schema().await.unwrap();

    (repository, events, snapshots)
}

async fn drop_log(repository: &PostgresRepository, events: &str, snapshots: &str) {
    repository
        .pool()
        .get()
        .await
        .unwrap()
        .batch_execute(&format!(
            "DROP TABLE {events}, {snapshots}, {events}__evt_features, {events}__evt_command_outcomes, \
             {events}__evt_stream_heads, {events}__evt_rebuild_lock, {events}__evt_rebuild_generation, \
             {events}__evt_views"
        ))
        .await
        .unwrap();
}

/// The sync journal (ADR 0005) and its compaction on PostgreSQL's views table (ADR 0008): the
/// shared journal, journal-rebuild and compaction cases, each over a private log.
#[tokio::test]
async fn postgres_journal_conforms() {
    use photon_evt::conformance::{compaction_cases, journal_cases, journal_rebuild_cases};

    let (repository, events, snapshots) = views_table_log("jr").await;
    journal_cases(&repository, &repository.view_repository()).await;
    journal_rebuild_cases(&repository, &repository.view_repository()).await;
    drop_log(&repository, &events, &snapshots).await;

    // Compaction on a log of its own, with a clock its lease cases move.
    let (repository, events, snapshots) = views_table_log("jc").await;
    let clock = TestClock::starting_now();
    let compacting = repository.clone().with_clock(clock.reader());
    compaction_cases(&compacting, &compacting.view_repository(), &clock).await;
    drop_log(&repository, &events, &snapshots).await;

    Box::pin(postgres_compaction_absent_key_race()).await;

    // Without the views table there is no journal to read or compact.
    {
        use photon_evt::journal::{
            CompactionStore, CompactionWrite, Expect, JournalCompactionStore, JournalStore, Scope,
        };

        let plain = PostgresRepository::new(drivers::postgres_pool().unwrap());
        let scope = Scope::new("s").unwrap();
        let unconfigured = |result: Result<(), Error>| {
            matches!(
                result,
                Err(Error::ViewsNotConfigured {
                    backend: "postgres"
                })
            )
        };
        assert!(unconfigured(plain.read_counter(&scope).await.map(drop)));
        assert!(unconfigured(
            plain
                .read_counters(std::slice::from_ref(&scope))
                .await
                .map(drop)
        ));
        assert!(unconfigured(
            plain.read_last_position(&scope).await.map(drop)
        ));
        assert!(unconfigured(plain.read_rows(&scope, 0, 10).await.map(drop)));
        assert!(unconfigured(
            plain.read_compaction_row(&scope, "LEASE").await.map(drop)
        ));
        assert!(unconfigured(
            plain
                .write_compaction(
                    None,
                    &[CompactionWrite::Check {
                        pk: "p".into(),
                        sk: "s".into(),
                        expect: Expect::Any,
                    }]
                )
                .await
                .map(drop)
        ));
    }
}

/// A compaction batch that finds a key absent, then meets a row another transaction inserted there
/// in the meantime (`FOR UPDATE` cannot lock an absent row), is refused at that write, with
/// nothing written: its plain `INSERT` waits for the other transaction and then violates the key.
async fn postgres_compaction_absent_key_race() {
    use photon_evt::journal::{
        CompactionStore, CompactionWrite, CompactionWritten, Expect, LEASE_SORT_KEY, Scope,
        compaction_pk,
    };

    let (repository, events, snapshots) = views_table_log("jx").await;
    let scope = Scope::new("race").unwrap();
    let (pk, table) = (compaction_pk(&scope), format!("{events}__evt_views"));
    let lease = |owner: &str| {
        ViewRow::new(
            pk.clone(),
            LEASE_SORT_KEY,
            "SYSTEM#JOURNAL",
            "journal-compaction",
            format!(r#"{{"scope":"race","token":"{owner}"}}"#).into_bytes(),
        )
    };

    // Another transaction inserts the lease row and holds it uncommitted.
    let pool = drivers::postgres_pool().unwrap();
    let mut holder = pool.get().await.unwrap();
    let held = holder.transaction().await.unwrap();
    let theirs = lease("theirs");
    held.execute(
        &format!(
            "INSERT INTO {table} (pk, sk, entity_id, entity_type, payload, ttl) \
             VALUES ($1, $2, $3, $4, $5, 0)"
        ),
        &[
            &theirs.pk,
            &theirs.sk,
            &theirs.entity_id,
            &theirs.entity_type,
            &std::str::from_utf8(&theirs.payload).unwrap(),
        ],
    )
    .await
    .unwrap();

    let racing = {
        let repository = repository.clone();
        let ours = lease("ours");

        tokio::spawn(async move {
            repository
                .write_compaction(
                    None,
                    &[CompactionWrite::Put {
                        row: ours,
                        expect: Expect::Absent,
                    }],
                )
                .await
        })
    };

    // The batch's insert waits on the holder's transaction.
    let watcher = pool.get().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let waiting: i64 = watcher
            .query_one(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'transactionid' AND NOT granted",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if waiting > 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the batch never waited on the other insert"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    held.commit().await.unwrap();

    assert_eq!(
        racing.await.unwrap().unwrap(),
        CompactionWritten::Refused(0),
        "the racing absent put is refused"
    );
    assert_eq!(
        repository
            .view_repository()
            .get_view_at(&pk, LEASE_SORT_KEY)
            .await
            .unwrap()
            .unwrap()
            .payload,
        theirs.payload,
        "the other transaction's row stands"
    );

    drop(watcher);
    drop(holder);
    drop_log(&repository, &events, &snapshots).await;
}
