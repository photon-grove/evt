//! Live Moto regressions for maintenance writes. Uses private tables, not conformance's shared
//! table: Moto's failed-transaction rollback must not undo another test's writes.
#![cfg(feature = "drivers")]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use evt_compat::drivers;
use photon_evt::dynamodb::{DynamoRepository, to_sdk_item};
use photon_evt::wire::dynamo;
use photon_evt::{EntityId, Error, EventSequence, Repository, SerializedEvent, SerializedSnapshot};

fn snapshot(id: &str, through: i64) -> SerializedSnapshot {
    SerializedSnapshot {
        entity_id: id.into(),
        entity_type: "account".into(),
        sequence: EventSequence(1),
        event_sequence: EventSequence(through),
        payload: b"{}".to_vec(),
    }
}

/// Seeds `count` events through the raw client, as a head-aware writer would have left them: the
/// events, the stream's head item, and a covering snapshot.
async fn seed(repository: &DynamoRepository, id: &str, count: i64) {
    repository
        .client()
        .put_item()
        .table_name(repository.table())
        .set_item(Some(to_sdk_item(&dynamo::head_item(
            &EntityId::new(id),
            EventSequence(count),
            None,
        ))))
        .send()
        .await
        .unwrap();
    for sequence in 1..=count {
        let event = SerializedEvent {
            id: format!("{id}:{sequence}").into(),
            entity_id: id.into(),
            entity_type: "account".into(),
            sequence: EventSequence(sequence),
            event_type: "account:opened".into(),
            version: photon_evt::EventVersion(1),
            payload: b"{}".to_vec(),
            ..Default::default()
        };
        repository
            .client()
            .put_item()
            .table_name(repository.table())
            .set_item(Some(to_sdk_item(
                &dynamo::event_item(&event, None).unwrap(),
            )))
            .send()
            .await
            .unwrap();
    }
    assert!(repository.put_snapshot(&snapshot(id, count)).await.unwrap());
}

/// A one-shot HTTP gate immediately before a matching transaction reaches Moto. This intercepts
/// the actual serialized request (no production hooks or sleeps in repository code). All manifest
/// declarations use the direct client, so they can complete while this request is held back.
struct TransactionGate {
    endpoint: String,
    reached: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
    stop: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl TransactionGate {
    fn new(action: &'static str) -> Self {
        let endpoint =
            std::env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| "http://localhost:4566".into());
        let upstream = endpoint
            .strip_prefix("http://")
            .expect("local Moto uses HTTP")
            .trim_end_matches('/')
            .to_owned();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let (arrived_tx, reached) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
        let armed = Arc::new(AtomicBool::new(true));
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let worker = std::thread::spawn(move || {
            while !stopped.load(Ordering::SeqCst) {
                let (mut incoming, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(err) => panic!("accept proxy request: {err}"),
                };
                // The SDK may speculatively open idle connections; do not let one block a
                // later connection carrying the transaction we are waiting for.
                let upstream = upstream.clone();
                let arrived_tx = arrived_tx.clone();
                let release_rx = release_rx.clone();
                let armed = armed.clone();
                std::thread::spawn(move || {
                    incoming
                        .set_read_timeout(Some(Duration::from_secs(15)))
                        .unwrap();
                    let mut request = Vec::new();
                    let header_end = loop {
                        let mut bytes = [0; 4096];
                        let count = match incoming.read(&mut bytes) {
                            Ok(0) => return,
                            Ok(count) => count,
                            Err(err)
                                if request.is_empty()
                                    && matches!(
                                        err.kind(),
                                        std::io::ErrorKind::WouldBlock
                                            | std::io::ErrorKind::TimedOut
                                    ) =>
                            {
                                return;
                            }
                            Err(err) => panic!("read proxy request: {err}"),
                        };
                        request.extend_from_slice(&bytes[..count]);
                        if let Some(end) = request.windows(4).position(|s| s == b"\r\n\r\n") {
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
                        .expect("SDK sends Content-Length");
                    while request.len() < header_end + length {
                        let mut bytes = [0; 4096];
                        let count = incoming.read(&mut bytes).unwrap();
                        assert!(count > 0, "incomplete request body");
                        request.extend_from_slice(&bytes[..count]);
                    }
                    let body = std::str::from_utf8(&request[header_end..]).unwrap();
                    if headers.contains("TransactWriteItems") && body.contains("\"Delete\"") {
                        let request: serde_json::Value = serde_json::from_str(body).unwrap();
                        let items = request["TransactItems"].as_array().unwrap();
                        assert!(items.len() <= 100);
                        let condition = items[0]["ConditionCheck"]["ConditionExpression"]
                            .as_str()
                            .unwrap();
                        assert!(matches!(
                            condition,
                            "version = :observed" | "attribute_not_exists(pk)"
                        ));
                        assert!(items[1..].iter().all(|item| item.get("Delete").is_some()));
                    }
                    if headers.contains("TransactWriteItems")
                        && body.contains(action)
                        && armed.swap(false, Ordering::SeqCst)
                    {
                        arrived_tx.send(()).unwrap();
                        release_rx
                            .lock()
                            .unwrap()
                            .recv_timeout(Duration::from_secs(15))
                            .expect("release paused transaction");
                    }
                    let mut remote = TcpStream::connect(&upstream).unwrap();
                    remote
                        .set_read_timeout(Some(Duration::from_secs(15)))
                        .unwrap();
                    // Force one request per connection, so response EOF is an unambiguous boundary.
                    let mut forwarded = String::new();
                    for line in headers[..headers.len() - 4].split("\r\n") {
                        if !line.to_ascii_lowercase().starts_with("connection:") {
                            forwarded.push_str(line);
                            forwarded.push_str("\r\n");
                        }
                    }
                    forwarded.push_str("Connection: close\r\n\r\n");
                    remote.write_all(forwarded.as_bytes()).unwrap();
                    remote.write_all(&request[header_end..]).unwrap();
                    let mut response = Vec::new();
                    remote.read_to_end(&mut response).unwrap();
                    incoming.write_all(&response).unwrap();
                });
            }
        });
        Self {
            endpoint,
            reached,
            release,
            stop,
            worker: Some(worker),
        }
    }

    fn wait(&self) {
        self.reached
            .recv_timeout(Duration::from_secs(15))
            .expect("transaction reached gate");
    }

    async fn repository(&self, repository: &DynamoRepository) -> DynamoRepository {
        let config = repository
            .client()
            .config()
            .to_builder()
            .endpoint_url(&self.endpoint)
            // The proxy closes every connection after one request, so the SDK's first attempt on
            // a pooled connection can fail; the drivers' client never retries, this one must.
            .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(3))
            .build();
        DynamoRepository::new(
            aws_sdk_dynamodb::Client::from_conf(config),
            repository.table(),
        )
        .with_lazy_initialization(true)
    }
}

impl Drop for TransactionGate {
    fn drop(&mut self) {
        let _ = self.release.send(());
        self.stop.store(true, Ordering::SeqCst);
        let result = self.worker.take().unwrap().join();
        if !std::thread::panicking() {
            result.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_writes_are_fenced_by_the_durable_manifest() {
    let client = drivers::dynamo_client().await;
    let table = format!(
        "evt-atomicity-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
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
    let repository = DynamoRepository::new(client.clone(), &table);
    // The table is head-aware; its streams are seeded below with their head items.
    repository.declare_stream_heads().await.unwrap();

    seed(&repository, "account-race", 1).await;
    {
        let gate = TransactionGate::new("\"Delete\"");
        let compactor = gate.repository(&repository).await;
        let task = tokio::spawn(async move {
            compactor
                .compact_below(&"account-race".into(), EventSequence(1))
                .await
        });
        gate.wait();
        repository
            .update_manifest(|manifest| {
                manifest.history_dependent.insert("account".into());
            })
            .await
            .unwrap();
        gate.release.send(()).unwrap();
        assert!(matches!(
            task.await.unwrap(),
            Err(Error::HistoryDependentProjection { .. })
        ));
        assert_eq!(
            repository
                .events(&"account-race".into())
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            repository.snapshot(&"account-race".into()).await.unwrap(),
            Some(snapshot("account-race", 1))
        );
    }
    repository
        .update_manifest(|manifest| manifest.history_dependent.clear())
        .await
        .unwrap();

    // A supported marker change retries rather than spuriously refusing maintenance.
    {
        let gate = TransactionGate::new("\"Delete\"");
        let compactor = gate.repository(&repository).await;
        let task = tokio::spawn(async move {
            compactor
                .compact_below(&"account-race".into(), EventSequence(1))
                .await
        });
        gate.wait();
        repository
            .update_manifest(|manifest| {
                manifest.retention.insert("session".into());
            })
            .await
            .unwrap();
        gate.release.send(()).unwrap();
        assert_eq!(task.await.unwrap().unwrap(), 1);
    }

    // Cross the 99-delete boundary, preserve sk=0, and count only extant event keys on repeat.
    seed(&repository, "account-batches", 105).await;
    // A huge count must neither preallocate usize::MAX nor overflow the SDK's i32 limit,
    // and must keep paging past the bounded 100-event query size.
    let latest = repository
        .latest(&"account-batches".into(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        latest
            .iter()
            .map(|event| event.sequence.0)
            .collect::<Vec<_>>(),
        (1..=105).rev().collect::<Vec<_>>()
    );
    assert_eq!(
        repository
            .compact_below(&"account-batches".into(), EventSequence(105))
            .await
            .unwrap(),
        105
    );
    assert_eq!(
        repository
            .compact_below(&"account-batches".into(), EventSequence(105))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        repository
            .snapshot(&"account-batches".into())
            .await
            .unwrap(),
        Some(snapshot("account-batches", 105))
    );
    assert!(
        !repository
            .put_snapshot(&snapshot("account-batches", 1))
            .await
            .unwrap()
    );

    // A changed but still supported marker also retries the standalone snapshot transaction.
    {
        let gate = TransactionGate::new("\"Put\"");
        let writer = gate.repository(&repository).await;
        let task =
            tokio::spawn(
                async move { writer.put_snapshot(&snapshot("account-batches", 105)).await },
            );
        gate.wait();
        repository
            .update_manifest(|manifest| manifest.retention.clear())
            .await
            .unwrap();
        gate.release.send(()).unwrap();
        assert!(task.await.unwrap().unwrap());
    }

    // Snapshot writes must fence a marker declaration after validation, too.
    {
        let gate = TransactionGate::new("\"Put\"");
        let writer = gate.repository(&repository).await;
        let task =
            tokio::spawn(async move { writer.put_snapshot(&snapshot("snapshot-race", 1)).await });
        gate.wait();
        repository
            .update_manifest(|manifest| {
                manifest.features.insert("unsupported".into(), 1);
            })
            .await
            .unwrap();
        gate.release.send(()).unwrap();
        assert!(matches!(
            task.await.unwrap(),
            Err(Error::UnsupportedFeature { .. })
        ));
        assert!(
            repository
                .snapshot(&"snapshot-race".into())
                .await
                .unwrap()
                .is_none()
        );
    }
    repository
        .update_manifest(|manifest| {
            manifest.features.clear();
            manifest.declare_stream_heads();
        })
        .await
        .unwrap();

    // A declaration on the very same handle invalidates its cache while a commit is in
    // flight. The commit must retain its validated snapshot, fail the version check, and
    // revalidate rather than treating an invalidated cache as a validated absent marker.
    {
        let gate = TransactionGate::new("\"Put\"");
        let writer = Arc::new(gate.repository(&repository).await);
        let task = tokio::spawn({
            let writer = Arc::clone(&writer);
            async move {
                let event = SerializedEvent {
                    id: "commit-race:1".into(),
                    entity_id: "commit-race".into(),
                    entity_type: "account".into(),
                    sequence: EventSequence(1),
                    event_type: "account:opened".into(),
                    version: photon_evt::EventVersion(1),
                    payload: b"{}".to_vec(),
                    ..Default::default()
                };
                writer
                    .commit(photon_evt::Commit::new("commit-race", vec![event]))
                    .await
            }
        });
        gate.wait();
        writer
            .update_manifest(|manifest| {
                manifest.features.insert("unsupported".into(), 1);
            })
            .await
            .unwrap();
        gate.release.send(()).unwrap();
        assert!(matches!(
            task.await.unwrap(),
            Err(Error::UnsupportedFeature { .. })
        ));
        assert!(
            repository
                .events(&"commit-race".into())
                .await
                .unwrap()
                .is_empty()
        );
    }
    repository
        .update_manifest(|manifest| {
            manifest.features.clear();
            manifest.declare_stream_heads();
        })
        .await
        .unwrap();

    // Engine rows with a normal entityType must be excluded from BOTH scan shapes.
    for id in ["SYSTEM#ordinary-type", "command-outcome#ordinary-type"] {
        client
            .put_item()
            .table_name(&table)
            .item("pk", AttributeValue::S(id.into()))
            .item("sk", AttributeValue::N("9".into()))
            .item("entityType", AttributeValue::S("account".into()))
            .send()
            .await
            .unwrap();
    }
    for filter in [None, Some(photon_evt::EntityType::new("account"))] {
        let heads = repository.entity_heads(filter.as_ref()).await.unwrap();
        assert_eq!(heads.len(), 2);
        assert_eq!(
            heads.get(&EntityId::new("account-race")),
            Some(&EventSequence(1))
        );
        assert_eq!(
            heads.get(&EntityId::new("account-batches")),
            Some(&EventSequence(105))
        );
    }

    // A retention declaration that races a fenced commit's marker revalidation must still refuse
    // the fence: the commit re-checks retention against the revalidated manifest on every
    // iteration instead of only before the first attempt.
    seed(&repository, "fence-target", 1).await;
    {
        let gate = TransactionGate::new("\"Put\"");
        let writer = Arc::new(gate.repository(&repository).await);
        let task = tokio::spawn({
            let writer = Arc::clone(&writer);
            async move {
                let event = SerializedEvent {
                    id: "fence-writer:1".into(),
                    entity_id: "fence-writer".into(),
                    entity_type: "account".into(),
                    sequence: EventSequence(1),
                    event_type: "account:opened".into(),
                    version: photon_evt::EventVersion(1),
                    payload: b"{}".to_vec(),
                    ..Default::default()
                };
                let mut commit = photon_evt::TransactionCommit::new(vec![photon_evt::Commit::new(
                    "fence-writer",
                    vec![event],
                )]);
                commit.fences = vec![photon_evt::ReadFence::new(
                    "fence-target",
                    "account",
                    EventSequence(1),
                    photon_evt::FenceProvenance::Engine,
                )];
                writer.commit_transaction(commit).await
            }
        });
        gate.wait();
        repository
            .update_manifest(|manifest| {
                manifest.retention.insert("account".into());
            })
            .await
            .unwrap();
        gate.release.send(()).unwrap();
        assert!(
            matches!(task.await.unwrap(), Err(Error::RetentionConflict { .. })),
            "the revalidated manifest refuses the fence"
        );
        assert!(
            repository
                .events(&"fence-writer".into())
                .await
                .unwrap()
                .is_empty(),
            "nothing is written"
        );
    }
    repository
        .update_manifest(|manifest| manifest.retention.clear())
        .await
        .unwrap();

    client
        .delete_table()
        .table_name(&table)
        .send()
        .await
        .unwrap();
}
