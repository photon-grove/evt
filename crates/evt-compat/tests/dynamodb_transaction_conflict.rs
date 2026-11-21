//! Faults DynamoDB can produce on a commit, live against the local emulator, through a proxy in
//! front of it that forwards everything else:
//! - A commit cancelled with `TransactionConflict` on an item the command never changes, as
//!   DynamoDB does when another transaction holds an item. Moto cannot race two transactions
//!   itself. The command is unfenced, so nothing it read went stale: it must never be recorded as
//!   a `stale-fence` rejection.
//! - A commit that applies but whose response is lost. The SDK retries it with the same
//!   `ClientRequestToken`, which DynamoDB answers as the same transaction, never as a conflict
//!   with it.
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
    CommandId, EntityId, Error, Metadata, OutcomeConfig, Repository, RetryPolicy, Store,
    TransactionConflict,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// What the proxy does to an armed commit.
#[derive(Clone, Copy, Debug)]
enum Fault {
    /// Answers with a cancellation, `TransactionConflict` on the rebuild-lock check (item 1),
    /// without forwarding the commit.
    Cancel,
    /// Forwards the commit, so it applies, then answers a server fault as if the response were
    /// lost.
    LoseResponse,
}

/// The proxy's state, shared with every connection it relays.
#[derive(Default)]
struct Shared {
    /// The fault for armed commits, and how many of the next commits carrying a command outcome
    /// are armed.
    armed: Mutex<Option<(Fault, usize)>>,
    /// How many commits were faulted since the proxy was last armed.
    faulted: AtomicUsize,
    /// The `ClientRequestToken` of every commit carrying a command outcome since the proxy was
    /// last armed, in arrival order (empty when a request carries none).
    tokens: Mutex<Vec<String>>,
}

/// A proxy to the emulator that faults armed commits.
struct Proxy {
    endpoint: String,
    shared: Arc<Shared>,
}

impl Proxy {
    async fn start() -> Self {
        let upstream = std::env::var("AWS_ENDPOINT_URL")
            .unwrap_or_else(|_| "http://localhost:4566".into())
            .strip_prefix("http://")
            .expect("the local emulator uses HTTP")
            .trim_end_matches('/')
            .to_owned();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let shared = Arc::new(Shared::default());

        let relayed = shared.clone();
        tokio::spawn(async move {
            loop {
                let (incoming, _) = listener.accept().await.unwrap();
                tokio::spawn(relay(incoming, upstream.clone(), relayed.clone()));
            }
        });

        Self { endpoint, shared }
    }

    /// Applies `fault` to the next `commits` outcome-carrying commits.
    fn arm(&self, fault: Fault, commits: usize) {
        self.shared.faulted.store(0, Ordering::SeqCst);
        self.shared.tokens.lock().unwrap().clear();
        *self.shared.armed.lock().unwrap() = Some((fault, commits));
    }

    /// How many commits were faulted since the proxy was last armed.
    fn faulted(&self) -> usize {
        self.shared.faulted.load(Ordering::SeqCst)
    }

    /// The tokens of the outcome-carrying commits since the proxy was last armed.
    fn tokens(&self) -> Vec<String> {
        self.shared.tokens.lock().unwrap().clone()
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

/// Answers one request: the armed fault for an armed commit, the emulator's answer otherwise.
async fn relay(mut incoming: TcpStream, upstream: String, shared: Arc<Shared>) {
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
    if commit {
        let request: Value = serde_json::from_slice(body).unwrap();
        let token = request["ClientRequestToken"].as_str().unwrap_or_default();
        shared.tokens.lock().unwrap().push(token.to_owned());
    }
    let fault = commit
        .then(|| {
            let mut armed = shared.armed.lock().unwrap();
            let (fault, left) = armed.as_mut()?;
            if *left == 0 {
                return None;
            }
            *left -= 1;

            Some(*fault)
        })
        .flatten();

    match fault {
        None => {
            let response = forward(&headers[..header_end - 4], body, &upstream).await;
            incoming.write_all(&response).await.unwrap();
        }
        Some(Fault::Cancel) => {
            shared.faulted.fetch_add(1, Ordering::SeqCst);
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
            });
            respond(&mut incoming, "400 Bad Request", &answer).await;
        }
        Some(Fault::LoseResponse) => {
            shared.faulted.fetch_add(1, Ordering::SeqCst);
            forward(&headers[..header_end - 4], body, &upstream).await;
            let answer = json!({
                "__type": "com.amazonaws.dynamodb.v20120810#InternalServerError",
                "message": "Internal server error",
            });
            respond(&mut incoming, "500 Internal Server Error", &answer).await;
        }
    }
}

/// Forwards one request (its `head` without the blank line, and its body) to the emulator and
/// returns the whole response.
async fn forward(head: &str, body: &[u8], upstream: &str) -> Vec<u8> {
    // One request per upstream connection, so the end of the response is the connection's.
    let mut forwarded = String::new();
    for line in head.split("\r\n") {
        if !line.to_ascii_lowercase().starts_with("connection:") {
            forwarded.push_str(line);
            forwarded.push_str("\r\n");
        }
    }
    forwarded.push_str("Connection: close\r\n\r\n");
    let mut remote = TcpStream::connect(upstream).await.unwrap();
    remote.write_all(forwarded.as_bytes()).await.unwrap();
    remote.write_all(body).await.unwrap();
    let mut response = Vec::new();
    remote.read_to_end(&mut response).await.unwrap();

    response
}

/// Answers a request with a DynamoDB JSON error.
async fn respond(incoming: &mut TcpStream, status: &str, answer: &Value) {
    let answer = answer.to_string();
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/x-amz-json-1.0\r\n\
         x-amzn-RequestId: faulted\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
        answer.len()
    );
    incoming.write_all(response.as_bytes()).await.unwrap();
}

/// A private table, so the faulted commits cannot touch another test's rows. Tests in one process
/// run in parallel, and the clock may tick in microseconds, so a counter keeps their names apart.
async fn private_table() -> String {
    static TABLES: AtomicUsize = AtomicUsize::new(0);
    let client = drivers::dynamo_client().await;
    let table = format!(
        "evt-txn-conflict-{}-{}-{}",
        std::process::id(),
        TABLES.fetch_add(1, Ordering::SeqCst),
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
    let proxy = Proxy::start().await;
    let repository = proxy.repository(&table).await;

    // One attempt: the conflict is returned as a retryable, transient error, and no outcome is
    // recorded, so the same command ID runs again and commits.
    proxy.arm(Fault::Cancel, 1);
    let once = store(repository.clone(), 1);
    let error = once
        .execute_with_outcome("ledger-once", Add(5), command("once"))
        .await
        .expect_err("the cancelled commit fails");
    assert_eq!(proxy.faulted(), 1);
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
    // The resend is a new call with its own token: evt never derives one from the command ID.
    let tokens = proxy.tokens();
    assert_eq!(tokens.len(), 2, "{tokens:?}");
    assert_ne!(
        tokens[0], tokens[1],
        "the same command ID sent again gets a new token"
    );

    // Several attempts: the conflict is retried like throttling, and the retry commits.
    proxy.arm(Fault::Cancel, 2);
    let outcome = store(repository.clone(), 3)
        .execute_with_outcome("ledger-retried", Add(7), command("retried"))
        .await
        .unwrap();
    assert_eq!(proxy.faulted(), 2);
    assert!(
        outcome.is_accepted(),
        "the third attempt commits: {outcome:?}"
    );

    // When every attempt conflicts, the error is returned, still with no outcome recorded.
    proxy.arm(Fault::Cancel, 3);
    let error = store(repository.clone(), 3)
        .execute_with_outcome("ledger-exhausted", Add(9), command("exhausted"))
        .await
        .expect_err("every attempt is cancelled");
    assert_eq!(proxy.faulted(), 3);
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

#[tokio::test]
async fn an_sdk_retry_resends_the_commit_token() {
    let table = private_table().await;
    let proxy = Proxy::start().await;
    let repository = proxy.repository(&table).await;

    // The commit applies, but its response is lost: the SDK retries it with the token of the
    // first attempt, so DynamoDB answers the retry as the same transaction (success, or
    // `TransactionInProgressException` while the first is still running), never as a conflict.
    proxy.arm(Fault::LoseResponse, 1);
    let outcome = store(repository.clone(), 1)
        .execute_with_outcome("ledger-lost", Add(5), command("lost"))
        .await
        .unwrap();
    assert_eq!(proxy.faulted(), 1);
    assert!(outcome.is_accepted(), "the command applied: {outcome:?}");

    let tokens = proxy.tokens();
    assert_eq!(
        tokens.len(),
        2,
        "the SDK retried the commit once: {tokens:?}"
    );
    assert!(!tokens[0].is_empty(), "the SDK fills in a token");
    assert_eq!(tokens[0], tokens[1], "the retry resends the first token");

    // Moto does not honour the token, so its retry fails on the outcome row the first attempt
    // wrote and the store replays it; DynamoDB answers the retry with success. Either way the
    // command applied once.
    let events = repository
        .events(&EntityId::new("ledger-lost"))
        .await
        .unwrap();
    assert_eq!(events.len(), 1, "the command applied once: {events:?}");

    // A new command is a new call, with a new token.
    proxy.arm(Fault::LoseResponse, 0);
    store(repository.clone(), 1)
        .execute_with_outcome("ledger-lost", Add(1), command("next"))
        .await
        .unwrap();
    let next = proxy.tokens();
    assert_eq!(next.len(), 1, "{next:?}");
    assert_ne!(next[0], tokens[0], "a new call gets a new token");
}
