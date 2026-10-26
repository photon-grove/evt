//! Replays the shared outcomes scenarios through the Rust store against a live DynamoDB emulator
//! and asserts every `TransactWriteItems` request each step sends — the action order and
//! conditions — against the calls the Go generator recorded in `compat/fixtures/scenarios.json`.
//! A private table keeps Moto's failed-transaction rollback from touching other tests.
#![cfg(feature = "drivers")]

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::config::interceptors::BeforeTransmitInterceptorContextRef;
use aws_sdk_dynamodb::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};
use evt_compat::drivers::{DynamoHeads, PostgresHeads};
use evt_compat::{Runner, ScenarioFixtures, Scenarios};
use photon_evt::dynamodb::DynamoRepository;
use serde_json::{Value, json};

/// Records the body of every `TransactWriteItems` request the client sends.
#[derive(Clone, Debug, Default)]
struct Recorder {
    requests: Arc<Mutex<Vec<Value>>>,
}

impl Intercept for Recorder {
    fn name(&self) -> &'static str {
        "TransactionRecorder"
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), aws_sdk_dynamodb::error::BoxError> {
        let request = context.request();
        let target = request
            .headers()
            .get("x-amz-target")
            .unwrap_or_default()
            .to_owned();

        if target.ends_with("TransactWriteItems")
            && let Some(body) = request.body().bytes()
        {
            self.requests
                .lock()
                .unwrap()
                .push(serde_json::from_slice(body).expect("a JSON request body"));
        }

        Ok(())
    }
}

/// One action in the fixtures' shape: operation, key, update expression, condition, names, and
/// values. An action on the views table also names it (`"table": "views"`) and records its string
/// sort key as `skText`, with `sk` 0.
fn summarize(action: &Value, views_table: &str) -> Value {
    let (operation, body) = action
        .as_object()
        .and_then(|map| map.iter().next())
        .expect("one operation per action");
    let key = if operation == "Put" {
        &body["Item"]
    } else {
        &body["Key"]
    };

    let mut summary = json!({
        "operation": operation,
        "pk": key["pk"]["S"],
        "sk": key["sk"]["N"].as_str().map_or(0, |sk| sk.parse::<i64>().unwrap()),
    });
    if body["TableName"] == views_table {
        summary["table"] = json!("views");
        summary["skText"] = key["sk"]["S"].clone();
    }
    if let Some(update) = body.get("UpdateExpression") {
        summary["update"] = update.clone();
    }
    if let Some(condition) = body.get("ConditionExpression") {
        summary["condition"] = condition.clone();
    }
    if let Some(names) = body
        .get("ExpressionAttributeNames")
        .filter(|names| names.as_object().is_some_and(|names| !names.is_empty()))
    {
        summary["names"] = names.clone();
    }
    if let Some(values) = body
        .get("ExpressionAttributeValues")
        .filter(|values| values.as_object().is_some_and(|values| !values.is_empty()))
    {
        summary["values"] = values.clone();
    }

    summary
}

#[tokio::test]
async fn outcome_scenarios_send_the_requests_go_recorded() {
    let scenarios = Scenarios::load().unwrap();
    let fixtures = ScenarioFixtures::load().unwrap();

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let run = format!("calls{nanos:x}");
    let table = format!("evt-calls-{}-{nanos}", std::process::id());
    let views_table = format!("evt-calls-views-{}-{nanos}", std::process::id());

    let endpoint =
        std::env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| "http://localhost:4566".into());
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-west-2"))
        .endpoint_url(endpoint)
        .load()
        .await;
    let recorder = Recorder::default();
    let client = aws_sdk_dynamodb::Client::from_conf(
        aws_sdk_dynamodb::config::Builder::from(&config)
            .interceptor(recorder.clone())
            .build(),
    );

    client
        .create_table()
        .table_name(&table)
        .billing_mode(BillingMode::PayPerRequest)
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
        .send()
        .await
        .unwrap();

    client
        .create_table()
        .table_name(&views_table)
        .billing_mode(BillingMode::PayPerRequest)
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
                .attribute_type(ScalarAttributeType::S)
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
        .send()
        .await
        .unwrap();

    let mut checked = 0;
    for scenario in scenarios.scenarios.iter().filter(|s| s.outcomes) {
        let repository = DynamoRepository::new(client.clone(), &table)
            .with_views_table(&views_table)
            .with_lazy_initialization(true);
        // Every step runs through the Rust store, whichever language the scenario assigns it to:
        // the requests are the same.
        let runner = Runner::for_scenario(repository, scenario, &run)
            .with_head_access(DynamoHeads::new(client.clone(), &table));
        let entity_id = scenario.entity_id(&run);
        let expected = fixtures.for_run(&scenario.name, &run).expect("expectation");

        for (index, step) in scenario.steps.iter().enumerate() {
            let before = recorder.requests.lock().unwrap().len();
            let outcome = runner.execute(&entity_id, step).await.unwrap();
            assert_eq!(outcome, step.expect, "{} step {index}", scenario.name);

            let calls: Vec<Vec<Value>> = recorder.requests.lock().unwrap()[before..]
                .iter()
                .map(|request| {
                    request["TransactItems"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|action| summarize(action, &views_table))
                        .collect()
                })
                .collect();
            let want = expected
                .transactions
                .iter()
                .find(|recorded| recorded.step == index)
                .map(|recorded| recorded.calls.clone())
                .unwrap_or_default();

            assert_eq!(
                calls, want,
                "{} step {index}: TransactWriteItems requests",
                scenario.name
            );
            checked += calls.len();

            // Outcome puts are the last action of every request that has one.
            for call in &calls {
                let outcome_puts: Vec<usize> = call
                    .iter()
                    .enumerate()
                    .filter(|(_, action)| {
                        action["pk"]
                            .as_str()
                            .is_some_and(|pk| pk.starts_with("command-outcome#"))
                    })
                    .map(|(position, _)| position)
                    .collect();
                assert!(
                    outcome_puts.is_empty() || outcome_puts == [call.len() - 1],
                    "{} step {index}: the outcome put is last",
                    scenario.name
                );
            }
        }
    }

    assert!(checked >= 20, "checked {checked} requests");

    for name in [&table, &views_table] {
        client.delete_table().table_name(name).send().await.unwrap();
    }
}

/// Runs the outcomes scenarios through the Rust store on PostgreSQL and compares the stored
/// outcome rows with the rows Go's DynamoDB items encode.
#[tokio::test]
async fn outcome_scenarios_store_the_rows_go_recorded_on_postgres() {
    use photon_evt::wire::dynamo;
    use photon_evt::wire::postgres::OutcomeRow;

    let scenarios = Scenarios::load().unwrap();
    let fixtures = ScenarioFixtures::load().unwrap();
    let repository = evt_compat::drivers::postgres().await.expect("connect");
    let run = format!(
        "pg{:x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    let comparable = |row: OutcomeRow| {
        (
            row.command_id,
            row.status,
            row.digest,
            row.response,
            serde_json::from_str::<Value>(&row.events).unwrap(),
            row.rejection
                .map(|rejection| serde_json::from_str::<Value>(&rejection).unwrap()),
            row.code,
        )
    };

    let mut rows = 0;
    for scenario in scenarios.scenarios.iter().filter(|s| s.outcomes) {
        let runner = Runner::for_scenario(repository.clone(), scenario, &run)
            .with_head_access(PostgresHeads::default_tables(repository.pool().clone()));
        let entity_id = scenario.entity_id(&run);

        for (index, step) in scenario.steps.iter().enumerate() {
            let outcome = runner.execute(&entity_id, step).await.unwrap();
            assert_eq!(outcome, step.expect, "{} step {index}", scenario.name);
        }

        let expected = fixtures.for_run(&scenario.name, &run).expect("expectation");
        for outcome in &expected.outcomes {
            let item: dynamo::Item = serde_json::from_value(outcome.dynamo_item.clone()).unwrap();
            let want =
                OutcomeRow::from_outcome(&dynamo::outcome_from_item(&item).unwrap()).unwrap();
            let got = repository
                .outcome_row(&outcome.command_id.as_str().into())
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("no row for {}", outcome.command_id));

            assert_eq!(comparable(got), comparable(want), "{}", outcome.command_id);
            rows += 1;
        }
    }

    assert!(rows >= 20, "compared {rows} rows");
}
