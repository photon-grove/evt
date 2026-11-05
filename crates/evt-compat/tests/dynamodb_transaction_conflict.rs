//! A commit DynamoDB cancels with `TransactionConflict` on an item the command never changes, live
//! against the local emulator: a proxy in front of it answers the first commits with the
//! cancellation, as DynamoDB does when another transaction holds an item, and forwards everything
//! else. Moto cannot race two transactions itself. The command is unfenced, so nothing it read
//! went stale: it must never be recorded as a `stale-fence` rejection.
#![cfg(feature = "drivers")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
};
use evt_compat::drivers;
use photon_evt::conformance::{Add, Ledger};
use photon_evt::dynamodb::DynamoRepository;
use photon_evt::{
    CommandId, Error, Metadata, OutcomeConfig, Repository, RetryPolicy, Store, TransactionConflict,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A proxy to the emulator that cancels the next `remaining` commits carrying a command outcome
/// with `TransactionConflict` on the rebuild-lock check (item 1), and counts them.
struct Cancelling {
    endpoint: String,
    remaining: Arc<Mutex<usize>>,
    cancelled: Arc<AtomicUsize>,
}

impl Cancelling {
    async fn start() -> Self {
        let upstream = std::env::var("AWS_ENDPOINT_URL")
            .unwrap_or_else(|_| "http://localhost:4566".into())
            .strip_prefix("http://")
            .expect("the local emulator uses HTTP")
            .trim_end_matches('/')
            .to_owned();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let remaining = Arc::new(Mutex::new(0));
        let cancelled = Arc::new(AtomicUsize::new(0));

        let (left, count) = (remaining.clone(), cancelled.clone());
        tokio::spawn(async move {
            loop {
                let (incoming, _) = listener.accept().await.unwrap();
                tokio::spawn(relay(
                    incoming,
                    upstream.clone(),
                    left.clone(),
                    count.clone(),
                ));
            }
        });

        Self {
            endpoint,
            remaining,
            cancelled,
        }
    }

    /// Cancels the next `commits` outcome-carrying commits.
    fn cancel(&self, commits: usize) {
        self.cancelled.store(0, Ordering::SeqCst);
        *self.remaining.lock().unwrap() = commits;
    }

    /// A repository over `table` whose requests go through the proxy.
    async fn repository(&self, table: &str) -> DynamoRepository {
        let config = drivers::dynamo_client()
            .await
            .config()
            .to_builder()
            .endpoint_url(&self.endpoint)
            // The proxy closes every connection after one request, so the SDK's first attempt on a
            // pooled connection can fail; a cancelled transaction is never retried by the SDK.
            .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(3))
            .build();

        DynamoRepository::new(aws_sdk_dynamodb::Client::from_conf(config), table)
    }
}

/// Answers one request: a cancellation for an armed commit, the emulator's answer otherwise.
async fn relay(
    mut incoming: TcpStream,
    upstream: String,
    remaining: Arc<Mutex<usize>>,
    cancelled: Arc<AtomicUsize>,
) {
    let mut request = Vec::new();
    let header_end = loop {
        let mut chunk = [0; 4096];
        let read = incoming.read(&mut chunk).await.unwrap_or(0);
        if read == 0 {
            return;
        }
        request.extend_from_slice(&chunk[..read]);
        if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let headers = String::from_utf8(request[..header_end].to_vec()).unwrap();
    let length: usize = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().unwrap())
        })
        .unwrap_or(0);
    while request.len() < header_end + length {
        let mut chunk = [0; 4096];
        let read = incoming.read(&mut chunk).await.unwrap();
        assert!(read > 0, "incomplete request body");
        request.extend_from_slice(&chunk[..read]);
    }
    let body = &request[header_end..];

    let commit = headers.contains("TransactWriteItems")
        && std::str::from_utf8(body)
            .unwrap()
            .contains("command-outcome#");
    let armed = commit && {
        let mut left = remaining.lock().unwrap();
        let armed = *left > 0;
        *left = left.saturating_sub(1);

        armed
    };
    if armed {
        cancelled.fetch_add(1, Ordering::SeqCst);
        let request: Value = serde_json::from_slice(body).unwrap();
        let items = request["TransactItems"].as_array().unwrap().len();
        let reasons: Vec<Value> = (0..items)
            .map(|index| {
                if index == 1 {
                    json!({"Code": "TransactionConflict", "Message": "Transaction is ongoing for the item"})
                } else {
                    json!({"Code": "None"})
                }
            })
            .collect();
        let answer = json!({
            "__type": "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
            "Message": "Transaction cancelled, please refer cancellation reasons for specific reasons",
            "CancellationReasons": reasons,
        })
        .to_string();
        let response = format!(
            "HTTP/1.1 400 Bad Request\r\nContent-Type: application/x-amz-json-1.0\r\n\
             x-amzn-RequestId: cancelled\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
            answer.len()
        );
        incoming.write_all(response.as_bytes()).await.unwrap();

        return;
    }

    // One request per upstream connection, so the end of the response is the connection's.
    let mut forwarded = String::new();
    for line in headers[..header_end - 4].split("\r\n") {
        if !line.to_ascii_lowercase().starts_with("connection:") {
            forwarded.push_str(line);
            forwarded.push_str("\r\n");
        }
    }
    forwarded.push_str("Connection: close\r\n\r\n");
    let mut remote = TcpStream::connect(&upstream).await.unwrap();
    remote.write_all(forwarded.as_bytes()).await.unwrap();
    remote.write_all(body).await.unwrap();
    let mut response = Vec::new();
    remote.read_to_end(&mut response).await.unwrap();
    incoming.write_all(&response).await.unwrap();
}

/// A private table, so the cancelled commits cannot touch another test's rows.
async fn private_table() -> String {
    let client = drivers::dynamo_client().await;
    let table = format!(
        "evt-txn-conflict-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let attribute = |name: &str, kind| {
        AttributeDefinition::builder()
            .attribute_name(name)
            .attribute_type(kind)
            .build()
            .unwrap()
    };
    let key = |name: &str, kind| {
        KeySchemaElement::builder()
            .attribute_name(name)
            .key_type(kind)
            .build()
            .unwrap()
    };
    client
        .create_table()
        .table_name(&table)
        .billing_mode(BillingMode::PayPerRequest)
        .attribute_definitions(attribute("pk", ScalarAttributeType::S))
        .attribute_definitions(attribute("sk", ScalarAttributeType::N))
        .key_schema(key("pk", KeyType::Hash))
        .key_schema(key("sk", KeyType::Range))
        .send()
        .await
        .unwrap();
    DynamoRepository::new(client, &table)
        .declare_stream_heads()
        .await
        .unwrap();

    table
}

fn store(repository: DynamoRepository, attempts: u32) -> Store<Ledger, DynamoRepository> {
    Store::new(repository, ())
        .with_snapshot_interval(0)
        .with_outcomes(OutcomeConfig::new().with_retry(RetryPolicy::immediate(attempts)))
}

fn command(id: &str) -> Metadata {
    Metadata::new("local").with_command_id(CommandId::new(id))
}

#[tokio::test]
async fn a_transaction_conflict_is_never_recorded_as_a_stale_fence() {
    let table = private_table().await;
    let proxy = Cancelling::start().await;
    let repository = proxy.repository(&table).await;

    // One attempt: the conflict is returned as a retryable, transient error, and no outcome is
    // recorded, so the same command ID runs again and commits.
    proxy.cancel(1);
    let once = store(repository.clone(), 1);
    let error = once
        .execute_with_outcome("ledger-once", Add(5), command("once"))
        .await
        .expect_err("the cancelled commit fails");
    assert_eq!(proxy.cancelled.load(Ordering::SeqCst), 1);
    assert!(
        error.is_retryable() && !error.is_conflict(),
        "a transaction conflict is transient: {error:?}"
    );
    let Error::Transient(source) = &error else {
        panic!("expected a transient error, got {error:?}");
    };
    assert!(
        source.downcast_ref::<TransactionConflict>().is_some(),
        "{error:?}"
    );
    assert!(
        repository
            .outcome(&CommandId::new("once"))
            .await
            .unwrap()
            .is_none(),
        "no outcome is recorded"
    );

    let outcome = once
        .execute_with_outcome("ledger-once", Add(5), command("once"))
        .await
        .unwrap();
    assert!(outcome.is_accepted(), "the resend commits: {outcome:?}");
    assert!(!outcome.replayed);

    // Several attempts: the conflict is retried like throttling, and the retry commits.
    proxy.cancel(2);
    let outcome = store(repository.clone(), 3)
        .execute_with_outcome("ledger-retried", Add(7), command("retried"))
        .await
        .unwrap();
    assert_eq!(proxy.cancelled.load(Ordering::SeqCst), 2);
    assert!(
        outcome.is_accepted(),
        "the third attempt commits: {outcome:?}"
    );

    // When every attempt conflicts, the error is returned, still with no outcome recorded.
    proxy.cancel(3);
    let error = store(repository.clone(), 3)
        .execute_with_outcome("ledger-exhausted", Add(9), command("exhausted"))
        .await
        .expect_err("every attempt is cancelled");
    assert_eq!(proxy.cancelled.load(Ordering::SeqCst), 3);
    assert!(matches!(error, Error::Transient(_)), "{error:?}");
    assert!(
        repository
            .outcome(&CommandId::new("exhausted"))
            .await
            .unwrap()
            .is_none(),
        "exhausted attempts record no stale-fence rejection"
    );
}
