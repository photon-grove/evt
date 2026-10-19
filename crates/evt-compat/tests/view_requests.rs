//! The requests `DynamoViewRepository` sends, and how it retries, against a canned local HTTP
//! server: no emulator, so DynamoDB's `UnprocessedItems` and `UnprocessedKeys` can be simulated.
//! Needs the `drivers` feature (the SDK's HTTP client and Tokio runtime).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use photon_evt::dynamodb::DynamoViewRepository;
use photon_evt::wire::dynamo::decode_view_cursor;
use photon_evt::{Error, ViewRepository, ViewRow};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A server that answers each request with the next canned body and records `(target, body)`.
struct Canned {
    repository: DynamoViewRepository,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
}

async fn canned(responses: Vec<Value>) -> Canned {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let log = requests.clone();
    let mut responses = responses.into_iter();

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();

            let mut buffer = Vec::new();
            let (head, mut body) = loop {
                let mut chunk = [0u8; 4096];
                let read = stream.read(&mut chunk).await.unwrap();
                if read == 0 {
                    break (String::new(), Vec::new());
                }
                buffer.extend_from_slice(&chunk[..read]);
                if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&buffer[..end]).to_lowercase();
                    break (head, buffer[end + 4..].to_vec());
                }
            };
            if head.is_empty() {
                continue;
            }

            let length: usize = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse().ok())
                .unwrap_or(0);
            while body.len() < length {
                let mut chunk = [0u8; 4096];
                let read = stream.read(&mut chunk).await.unwrap();
                body.extend_from_slice(&chunk[..read]);
            }
            let target = head
                .lines()
                .find_map(|line| line.strip_prefix("x-amz-target:"))
                .unwrap_or_default()
                .trim()
                .to_owned();
            log.lock()
                .unwrap()
                .push((target, serde_json::from_slice(&body).unwrap()));

            let reply = responses.next().unwrap_or_else(|| json!({})).to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/x-amz-json-1.0\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
                reply.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let config = aws_sdk_dynamodb::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-west-2"))
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .credentials_provider(Credentials::new("test", "test", None, None, "test"))
        .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
        .timeout_config(
            aws_sdk_dynamodb::config::timeout::TimeoutConfig::builder()
                .operation_timeout(Duration::from_secs(10))
                .build(),
        )
        .build();

    Canned {
        repository: DynamoViewRepository::new(Client::from_conf(config), "views"),
        requests,
    }
}

fn row(pk: &str, sk: &str) -> ViewRow {
    ViewRow::new(pk, sk, "owner", "kind", "{}")
}

fn item(pk: &str, sk: &str) -> Value {
    json!({"pk": {"S": pk}, "sk": {"S": sk}, "entityID": {"S": "o"}, "entityType": {"S": "k"}, "payload": {"S": "{}"}})
}

impl Canned {
    fn recorded(&self) -> Vec<(String, Value)> {
        self.requests.lock().unwrap().clone()
    }
}

#[tokio::test]
async fn point_reads_are_strongly_consistent_and_an_empty_sort_key_is_view() {
    let server = canned(vec![json!({}), json!({"Item": item("p", "VIEW")})]).await;

    assert_eq!(server.repository.get_view_at("p", "").await.unwrap(), None);
    let found = server.repository.get_view("p").await.unwrap().unwrap();
    assert_eq!(found.pk, "p");

    for (target, body) in server.recorded() {
        assert_eq!(target, "dynamodb_20120810.getitem");
        assert_eq!(body["TableName"], "views");
        assert_eq!(body["ConsistentRead"], true);
        assert_eq!(body["Key"], json!({"pk": {"S": "p"}, "sk": {"S": "VIEW"}}));
    }
}

#[tokio::test]
async fn a_delete_sends_the_view_key_for_an_empty_sort_key() {
    let server = canned(vec![json!({})]).await;

    server.repository.delete_view("p", "").await.unwrap();

    let (target, body) = server.recorded().remove(0);
    assert_eq!(target, "dynamodb_20120810.batchwriteitem");
    assert_eq!(
        body["RequestItems"]["views"],
        json!([{"DeleteRequest": {"Key": {"pk": {"S": "p"}, "sk": {"S": "VIEW"}}}}])
    );
}

#[tokio::test]
async fn queries_set_the_expressions_the_limit_and_no_consistent_read() {
    let reply = json!({
        "Items": [item("p", "a")],
        "LastEvaluatedKey": {"pk": {"S": "p"}, "sk": {"S": "a"}},
    });
    let server = canned(vec![reply, json!({"Items": []})]).await;

    let page = server
        .repository
        .list_views_by_pk_paged("p", Some(1), None)
        .await
        .unwrap();
    assert_eq!(page.views.len(), 1);
    let cursor = page.next_cursor.unwrap();
    assert_eq!(
        decode_view_cursor(&cursor).unwrap(),
        photon_evt::wire::dynamo::view_key("p", "a")
    );

    server
        .repository
        .list_views_by_entity_type_paged("kind", None, None)
        .await
        .unwrap();

    let requests = server.recorded();
    let first = &requests[0].1;
    assert_eq!(first["KeyConditionExpression"], "pk = :pk");
    assert_eq!(first["Limit"], 1);
    assert!(first.get("ConsistentRead").is_none());
    assert!(first.get("IndexName").is_none());

    let second = &requests[1].1;
    assert_eq!(second["IndexName"], "entityType-index");
    assert_eq!(second["KeyConditionExpression"], "entityType = :entityType");
    assert!(second.get("Limit").is_none());
}

#[tokio::test]
async fn a_batch_write_chunks_to_25_and_resends_what_is_unprocessed() {
    let unprocessed = json!({"UnprocessedItems": {"views": [
        {"PutRequest": {"Item": item("p00", "VIEW")}},
        {"PutRequest": {"Item": item("p01", "VIEW")}},
    ]}});
    let server = canned(vec![unprocessed, json!({}), json!({}), json!({})]).await;
    let rows: Vec<ViewRow> = (0..60).map(|i| row(&format!("p{i:02}"), "VIEW")).collect();

    server.repository.put_views(&rows).await.unwrap();

    let sizes: Vec<usize> = server
        .recorded()
        .iter()
        .map(|(_, body)| body["RequestItems"]["views"].as_array().unwrap().len())
        .collect();
    // 25 (two come back unprocessed), the resend of those two, then 25 and 10.
    assert_eq!(sizes, vec![25, 2, 25, 10]);
}

#[tokio::test]
async fn a_batch_write_gives_up_after_three_attempts() {
    let stuck =
        json!({"UnprocessedItems": {"views": [{"PutRequest": {"Item": item("p", "VIEW")}}]}});
    let server = canned(vec![stuck.clone(), stuck.clone(), stuck]).await;

    let result = server.repository.put_view(&row("p", "VIEW")).await;

    assert!(matches!(result, Err(Error::Transient(_))), "{result:?}");
    assert_eq!(server.recorded().len(), 3);
}

#[tokio::test]
async fn a_repeated_key_in_a_batch_is_sent_once_with_the_last_row() {
    let server = canned(vec![json!({})]).await;
    let mut last = row("p", "VIEW");
    last.payload = br#"{"n":2}"#.to_vec();

    server
        .repository
        .put_views(&[row("p", ""), last])
        .await
        .unwrap();

    let writes = server.recorded().remove(0).1["RequestItems"]["views"].clone();
    assert_eq!(writes.as_array().unwrap().len(), 1);
    assert_eq!(
        writes[0]["PutRequest"]["Item"]["payload"]["S"],
        r#"{"n":2}"#
    );
}

#[tokio::test]
async fn a_batch_get_chunks_to_100_deduplicates_and_resends_unprocessed_keys() {
    let unprocessed = json!({
        "Responses": {"views": [item("p000", "VIEW")]},
        "UnprocessedKeys": {"views": {"Keys": [{"pk": {"S": "p001"}, "sk": {"S": "VIEW"}}], "ConsistentRead": true}},
    });
    let tail = json!({"Responses": {"views": [item("p001", "VIEW")]}});
    let server = canned(vec![unprocessed, tail, json!({"Responses": {"views": []}})]).await;

    let pks: Vec<String> = (0..130).map(|i| format!("p{i:03}")).collect();
    let mut keys: Vec<&str> = pks.iter().map(String::as_str).collect();
    keys.extend(["p000", "", "p129"]);

    let found = server.repository.batch_get_views(&keys).await.unwrap();
    assert_eq!(found.len(), 2);

    let requests = server.recorded();
    let counts: Vec<usize> = requests
        .iter()
        .map(|(_, body)| {
            body["RequestItems"]["views"]["Keys"]
                .as_array()
                .unwrap()
                .len()
        })
        .collect();
    // 100 keys (one comes back unprocessed), its resend, then the other 30.
    assert_eq!(counts, vec![100, 1, 30]);
    for (target, body) in &requests {
        assert_eq!(target, "dynamodb_20120810.batchgetitem");
        assert_eq!(body["RequestItems"]["views"]["ConsistentRead"], true);
    }
}
