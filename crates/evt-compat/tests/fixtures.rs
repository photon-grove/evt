//! Asserts the Rust implementation against the golden fixtures the Go implementation generated.
//! Regenerate them with `go test ./internal/compat -run TestFixtures -update`.

use std::collections::BTreeMap;
use std::time::{Duration, UNIX_EPOCH};

use evt_compat::scenario::{FIXTURE_RUN, ORIGIN_SOURCE, compat_dir};
use evt_compat::{Account, ManualClock, RebuildDriver, Runner, ScenarioFixtures, Scenarios};

/// The injected clock of the Go fixture runs of the rebuild fence, in Unix seconds.
const REBUILD_FIXTURE_CLOCK: i64 = 1_790_000_000;
use photon_evt::wire::postgres::OutcomeRow;
use photon_evt::wire::{dynamo, envelope};
use photon_evt::{
    Aggregate, CommandIdentity, DigestInput, EntityId, EntityType, Error, EventId, EventSequence,
    EventType, EventVersion, FeatureManifest, FeatureMarker, MAX_COMMAND_ID_BYTES,
    MAX_OUTCOME_BYTES, MemRepository, Metadata, Outcome, Rejection, RejectionCode, RetryPolicy,
    SerializedEvent, SerializedSnapshot, canonical_digest_bytes, canonical_numbers, default_digest,
    json, snapshot_events_due,
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodecFixtures {
    events: Vec<EventVector>,
    snapshots: Vec<SnapshotVector>,
    metadata_decode: Vec<DecodeVector>,
    event_decode: Vec<DecodeVector>,
    snapshot_policy: Vec<[i64; 4]>,
    views: Vec<ViewVector>,
    int_key_map: String,
    ttl: TtlVector,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TtlVector {
    clock_unix_nano: u64,
    streams: Vec<TtlStream>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TtlStream {
    name: String,
    retention_nanos: u64,
    events: Vec<String>,
    snapshot: String,
    dynamo_items: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ViewVector {
    name: String,
    entity_id: String,
    state: String,
    dynamo_item: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventVector {
    name: String,
    input: EventInput,
    serialized_event_json: String,
    dynamo_item: Value,
    sns_message: String,
    sns_attributes: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventInput {
    id: String,
    sequence: i64,
    #[serde(rename = "type")]
    event_type: String,
    version: i64,
    entity_id: String,
    entity_type: String,
    payload: String,
    metadata_json: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotVector {
    name: String,
    input: SnapshotInput,
    serialized_snapshot_json: String,
    dynamo_item: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotInput {
    entity_type: String,
    entity_id: String,
    sequence: i64,
    event_sequence: i64,
    payload: String,
}

#[derive(Deserialize)]
struct DecodeVector {
    name: String,
    input: String,
    canonical: String,
}

fn codec_fixtures() -> CodecFixtures {
    let data =
        std::fs::read(compat_dir().join("fixtures/codec.json")).expect("read codec fixtures");

    serde_json::from_slice(&data).expect("parse codec fixtures")
}

fn utf8(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).unwrap()
}

#[derive(Deserialize)]
struct FeatureMarkerFixtures {
    declarations: Vec<FeatureDeclarationVector>,
}

#[derive(Deserialize)]
struct FeatureDeclarationVector {
    name: String,
    input: FeatureDeclarationInput,
    output: FeatureDeclarationOutput,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FeatureDeclarationInput {
    features: BTreeMap<String, i64>,
    history_dependent: Vec<String>,
    retention: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FeatureDeclarationOutput {
    features_json: String,
    history_dependent_json: String,
    retention_json: String,
    dynamo_item: Value,
    postgres_row: Value,
}

#[tokio::test]
async fn feature_marker_declarations_match_go_byte_for_byte() {
    let data = std::fs::read(compat_dir().join("fixtures/feature-marker.json")).unwrap();
    let fixtures: FeatureMarkerFixtures = serde_json::from_slice(&data).unwrap();
    let repo = MemRepository::new();

    for vector in fixtures.declarations {
        let name = &vector.name;
        repo.update_manifest(|manifest| {
            manifest.features = vector.input.features.clone();
            manifest.history_dependent = vector.input.history_dependent.iter().cloned().collect();
            manifest.retention = vector.input.retention.iter().cloned().collect();
        })
        .await
        .unwrap();
        let manifest = repo.read_manifest().await.unwrap().unwrap();
        let features = manifest.features_json();
        let history = manifest
            .history_dependent_json()
            .unwrap_or_else(|| "[]".into());
        let retention = manifest.retention_json().unwrap_or_else(|| "[]".into());
        assert_eq!(
            features, vector.output.features_json,
            "{name}: features bytes"
        );
        assert_eq!(
            history, vector.output.history_dependent_json,
            "{name}: history bytes"
        );
        assert_eq!(
            retention, vector.output.retention_json,
            "{name}: retention bytes"
        );

        assert_eq!(
            serde_json::to_value(dynamo::feature_marker_item(&manifest).unwrap()).unwrap(),
            vector.output.dynamo_item,
            "{name}: complete marker item"
        );
        let go_item: dynamo::Item = serde_json::from_value(vector.output.dynamo_item).unwrap();
        assert_eq!(
            dynamo::feature_marker_from_item(&go_item).unwrap(),
            manifest,
            "{name}: decode Go marker"
        );
        assert_eq!(
            serde_json::json!({
                "id": 0,
                "version": manifest.version,
                "features": features,
                "history_dependent": history,
                "retention": retention,
            }),
            vector.output.postgres_row,
            "{name}: PostgreSQL row fields"
        );
        assert_eq!(
            FeatureManifest::from_attributes(
                vector.output.postgres_row["version"].as_i64().unwrap(),
                vector.output.postgres_row["features"].as_str().unwrap(),
                vector.output.postgres_row["history_dependent"].as_str(),
                vector.output.postgres_row["retention"].as_str(),
            )
            .unwrap(),
            manifest,
            "{name}: decode Go PostgreSQL row"
        );
    }
}

#[test]
fn events_encode_and_decode_like_go() {
    for vector in codec_fixtures().events {
        let name = &vector.name;
        let input = vector.input;

        let metadata: Metadata = serde_json::from_str(&input.metadata_json).unwrap();
        assert_eq!(
            json::to_string(&metadata).unwrap(),
            input.metadata_json,
            "{name}: metadata"
        );

        let event = SerializedEvent {
            id: EventId::new(input.id),
            sequence: EventSequence(input.sequence),
            event_type: EventType::new(input.event_type),
            version: EventVersion(input.version),
            entity_id: EntityId::new(input.entity_id),
            entity_type: EntityType::new(input.entity_type),
            payload: input.payload.into_bytes(),
            metadata,
        };

        assert_eq!(
            utf8(event.to_json().unwrap()),
            vector.serialized_event_json,
            "{name}: serialized event"
        );
        assert_eq!(
            SerializedEvent::from_json(vector.serialized_event_json.as_bytes()).unwrap(),
            event,
            "{name}: decode serialized event"
        );

        let item = dynamo::event_item(&event, None).unwrap();
        assert_eq!(
            serde_json::to_value(&item).unwrap(),
            vector.dynamo_item,
            "{name}: dynamo item"
        );

        let go_item: dynamo::Item = serde_json::from_value(vector.dynamo_item).unwrap();
        assert_eq!(
            dynamo::event_from_item(&go_item).unwrap(),
            event,
            "{name}: decode dynamo item"
        );

        assert_eq!(
            utf8(envelope::encode(&event, ORIGIN_SOURCE).unwrap()),
            vector.sns_message,
            "{name}: envelope"
        );
        assert_eq!(
            envelope::decode(vector.sns_message.as_bytes()).unwrap(),
            event,
            "{name}: decode envelope"
        );
        assert_eq!(
            envelope::message_attributes(&event),
            vector.sns_attributes,
            "{name}: attributes"
        );
    }
}

#[test]
fn snapshots_encode_and_decode_like_go() {
    for vector in codec_fixtures().snapshots {
        let name = &vector.name;
        let snapshot = SerializedSnapshot {
            entity_type: EntityType::new(vector.input.entity_type),
            entity_id: EntityId::new(vector.input.entity_id),
            sequence: EventSequence(vector.input.sequence),
            event_sequence: EventSequence(vector.input.event_sequence),
            payload: vector.input.payload.into_bytes(),
        };

        assert_eq!(
            utf8(snapshot.to_json().unwrap()),
            vector.serialized_snapshot_json,
            "{name}: json"
        );
        assert_eq!(
            SerializedSnapshot::from_json(vector.serialized_snapshot_json.as_bytes()).unwrap(),
            snapshot,
            "{name}: decode json"
        );

        let item = dynamo::snapshot_item(&snapshot, None).unwrap();
        assert_eq!(
            serde_json::to_value(&item).unwrap(),
            vector.dynamo_item,
            "{name}: dynamo item"
        );

        let go_item: dynamo::Item = serde_json::from_value(vector.dynamo_item).unwrap();
        assert!(dynamo::is_snapshot_item(&go_item), "{name}: sk=0");
        assert_eq!(
            dynamo::snapshot_from_item(&go_item).unwrap(),
            snapshot,
            "{name}: decode item"
        );
    }
}

#[test]
fn lenient_decoding_matches_go() {
    let fixtures = codec_fixtures();

    for vector in fixtures.metadata_decode {
        let metadata: Metadata = serde_json::from_str(&vector.input).unwrap();
        assert_eq!(
            json::to_string(&metadata).unwrap(),
            vector.canonical,
            "metadata {}",
            vector.name
        );
    }

    for vector in fixtures.event_decode {
        let event = SerializedEvent::from_json(vector.input.as_bytes()).unwrap();
        let canonical = utf8(event.to_json().unwrap());

        // Go keeps a JSON null payload as a nil []byte and writes it back as null; Rust decodes it
        // to an empty payload and writes "". Both decode either form to an empty payload.
        let expected = vector
            .canonical
            .replace(r#""payload":null"#, r#""payload":"""#);
        assert_eq!(canonical, expected, "event {}", vector.name);
    }
}

#[test]
fn ttl_matches_go() {
    let vector = codec_fixtures().ttl;
    let clock = UNIX_EPOCH + Duration::from_nanos(vector.clock_unix_nano);

    for stream in vector.streams {
        let name = &stream.name;
        let ttl = dynamo::expiry(clock, Duration::from_nanos(stream.retention_nanos));

        let snapshot = SerializedSnapshot::from_json(stream.snapshot.as_bytes()).unwrap();
        // The head item comes first (`sk = -1`) and carries the same ttl as the stream's items.
        let mut items = vec![
            dynamo::head_item(&snapshot.entity_id, snapshot.event_sequence, ttl),
            dynamo::snapshot_item(&snapshot, ttl).unwrap(),
        ];
        for event in &stream.events {
            let event = SerializedEvent::from_json(event.as_bytes()).unwrap();
            items.push(dynamo::event_item(&event, ttl).unwrap());
        }

        let items: Vec<Value> = items
            .iter()
            .map(|item| serde_json::to_value(item).unwrap())
            .collect();
        assert_eq!(items, stream.dynamo_items, "{name}: items");
    }
}

#[test]
fn integer_map_keys_sort_like_go() {
    #[derive(serde::Serialize)]
    struct Keys(#[serde(serialize_with = "json::go_key_order")] BTreeMap<i64, &'static str>);

    let map = BTreeMap::from([(-1, "minus one"), (2, "two"), (10, "ten")]);

    assert_eq!(
        utf8(json::to_vec(&Keys(map)).unwrap()),
        codec_fixtures().int_key_map
    );
}

#[test]
fn view_items_match_go() {
    for vector in codec_fixtures().views {
        let name = &vector.name;
        let account: Account = serde_json::from_str(&vector.state).unwrap();
        let payload = json::to_vec(&account).unwrap();
        assert_eq!(utf8(payload.clone()), vector.state, "{name}: state");

        let item = dynamo::view_item(
            &EntityType::new(Account::TYPE),
            &EntityId::new(vector.entity_id),
            &payload,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(&item).unwrap(),
            vector.dynamo_item,
            "{name}: view item"
        );
    }
}

#[test]
fn snapshot_policy_matches_go() {
    for [current, events, interval, expected] in codec_fixtures().snapshot_policy {
        let due = snapshot_events_due(EventSequence(current), events as usize, interval as usize);

        assert_eq!(
            due as i64, expected,
            "current={current} events={events} interval={interval}"
        );
    }
}

#[tokio::test]
async fn scenarios_match_go_byte_for_byte() {
    let scenarios = Scenarios::load().unwrap();
    let fixtures = ScenarioFixtures::load().unwrap();
    assert_eq!(fixtures.run, FIXTURE_RUN);
    assert_eq!(
        fixtures.scenarios.len(),
        scenarios.scenarios.len(),
        "one expectation per scenario"
    );

    for scenario in &scenarios.scenarios {
        // The memory repository stands in for a backend, including its raw access to the heads.
        // Its rebuild fence reads an injected clock, which a `wait` step advances, starting where
        // Go's fixture run starts.
        let clock = ManualClock::at(REBUILD_FIXTURE_CLOCK);
        let repository = MemRepository::new().with_clock(clock.reader());
        let runner = Runner::for_scenario(repository.clone(), scenario, FIXTURE_RUN)
            .with_head_access(repository.clone())
            .with_rebuild_access(RebuildDriver::manual(repository.clone(), clock))
            .with_view_access(repository);
        let entity_id = scenario.entity_id(FIXTURE_RUN);

        for (index, step) in scenario.steps.iter().enumerate() {
            let outcome = runner.execute(&entity_id, step).await.unwrap();
            assert_eq!(outcome, step.expect, "{} step {index}", scenario.name);
        }

        let expected = fixtures
            .for_run(&scenario.name, FIXTURE_RUN)
            .expect("expectation");
        let observed = runner.observe(&scenario.name, &entity_id).await.unwrap();

        assert_eq!(observed.state, expected.state, "{}: state", scenario.name);
        assert_eq!(
            observed.sequence, expected.sequence,
            "{}: sequence",
            scenario.name
        );
        assert_eq!(
            observed.snapshot_sequence, expected.snapshot_sequence,
            "{}: snapshot seq",
            scenario.name
        );
        assert_eq!(
            observed.events, expected.events,
            "{}: events",
            scenario.name
        );
        assert_eq!(
            observed.snapshot, expected.snapshot,
            "{}: snapshot",
            scenario.name
        );
        assert_eq!(observed.head, expected.head, "{}: head", scenario.name);
        assert_eq!(
            observed.dynamo_items, expected.dynamo_items,
            "{}: dynamo items",
            scenario.name
        );
        assert_eq!(
            observed.sns_messages, expected.sns_messages,
            "{}: sns messages",
            scenario.name
        );

        // Every other stream the scenario touches, written or only fenced, must match too: a
        // regression that drops or partially writes a sibling must not pass.
        let siblings = runner
            .observe_siblings(&scenario.sibling_ids(FIXTURE_RUN))
            .await
            .unwrap();
        assert_eq!(
            siblings, expected.siblings,
            "{}: sibling streams",
            scenario.name
        );

        // The view rows commit-wide projectors left in the scenario's partitions: the bytes Go
        // stored, in the order a verifier reads them.
        let views = runner.observe_views(scenario, &entity_id).await.unwrap();
        assert_eq!(views, expected.views, "{}: view rows", scenario.name);

        // The outcome rows of an outcomes scenario: digests, event IDs, responses, and rejections
        // are the bytes Go stored.
        let outcomes = runner
            .observe_outcomes(scenario, FIXTURE_RUN)
            .await
            .unwrap();
        assert_eq!(
            outcomes, expected.outcomes,
            "{}: command outcome rows",
            scenario.name
        );
        assert_eq!(
            !outcomes.is_empty(),
            scenario.outcomes,
            "{}: an outcomes scenario records rows",
            scenario.name
        );
    }
}

#[tokio::test]
async fn sibling_fixtures_pin_the_streams_scenarios_write() {
    let fixtures = ScenarioFixtures::load().unwrap();

    // The multi-stream scenario writes a `-ledger` sibling and the read-fences scenario a
    // `-watched` one; an empty or missing recording would let a dropped write pass.
    for (scenario, suffix, events) in [("multi-stream", "ledger", 2), ("read-fences", "watched", 1)]
    {
        let expected = fixtures.for_run(scenario, FIXTURE_RUN).unwrap();
        let id = format!("compat-{FIXTURE_RUN}-{scenario}-{suffix}");
        let sibling = expected
            .siblings
            .iter()
            .find(|sibling| sibling.entity_id == id)
            .unwrap_or_else(|| panic!("{scenario}: no recording for {id}"));

        assert_eq!(sibling.events.len(), events, "{scenario}: {id} events");
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OutcomeFixtures {
    limits: OutcomeLimits,
    retry: RetryDefaults,
    digests: Vec<DigestVector>,
    rows: Vec<RowVector>,
    #[serde(default)]
    malformed_rows: Vec<MalformedRow>,
    #[serde(default)]
    rejection_numbers: Vec<RejectionNumberVector>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RejectionNumberVector {
    name: String,
    input: String,
    #[serde(default)]
    stored: String,
    #[serde(default)]
    error: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RetryDefaults {
    max_attempts: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
    stale_code: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OutcomeLimits {
    max_command_id_bytes: usize,
    max_outcome_bytes: usize,
}

#[derive(Deserialize)]
struct DigestVector {
    name: String,
    input: DigestInputVector,
    #[serde(rename = "canonicalHex")]
    canonical_hex: String,
    digest: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DigestInputVector {
    command_type: String,
    targets: Vec<String>,
    payload: Option<String>,
    principal: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RowVector {
    name: String,
    input: OutcomeInputVector,
    #[serde(default)]
    events_json: String,
    #[serde(default, rename = "rejectionJson")]
    rejection_json: String,
    dynamo_item: Value,
    item_size: usize,
    postgres_row: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MalformedRow {
    name: String,
    dynamo_item: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OutcomeInputVector {
    command_id: String,
    status: String,
    digest: String,
    response: Option<String>,
    event_ids: Vec<String>,
    rejection: Option<String>,
}

fn outcome_fixtures() -> OutcomeFixtures {
    let data =
        std::fs::read(compat_dir().join("fixtures/outcomes.json")).expect("read outcome fixtures");

    serde_json::from_slice(&data).expect("parse outcome fixtures")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn outcome_limits_match_go() {
    let limits = outcome_fixtures().limits;

    assert_eq!(MAX_COMMAND_ID_BYTES, limits.max_command_id_bytes);
    assert_eq!(MAX_OUTCOME_BYTES, limits.max_outcome_bytes);
}

#[test]
fn retry_defaults_match_go() {
    let retry = outcome_fixtures().retry;
    let policy = RetryPolicy::default();

    assert_eq!(policy.max_attempts, retry.max_attempts);
    assert_eq!(
        policy.base_delay,
        Duration::from_millis(retry.base_delay_ms)
    );
    assert_eq!(policy.max_delay, Duration::from_millis(retry.max_delay_ms));
    assert_eq!(RejectionCode::STALE_FENCE, retry.stale_code.as_str());
}

/// The stale-fence rejections the engine records are built, not just decoded: Rust's constructors
/// must produce the bytes Go's do.
#[test]
fn stale_fence_rejections_match_go_byte_for_byte() {
    let rows = outcome_fixtures().rows;
    let stored = |name: &str| {
        rows.iter()
            .find(|row| row.name == name)
            .unwrap_or_else(|| panic!("no row vector {name:?}"))
            .input
            .rejection
            .clone()
            .expect("a rejection")
    };

    assert_eq!(
        Rejection::stale_fence().encode().unwrap(),
        stored("rejected stale fence")
    );
    assert_eq!(
        Rejection::retries_exhausted(5).encode().unwrap(),
        stored("rejected stale fence, retries exhausted")
    );
}

/// A rejection that carries events lists them on its row, beside the rejection and its code; a
/// plain rejection's row has no `events` attribute, exactly as it never did.
#[test]
fn a_rejection_lists_events_only_when_it_committed_some() {
    let rows = outcome_fixtures().rows;
    let row = |name: &str| {
        rows.iter()
            .find(|row| row.name == name)
            .unwrap_or_else(|| panic!("no row vector {name:?}"))
    };

    let with_events = row("rejected with events");
    assert_eq!(with_events.input.status, "rejected");
    assert_eq!(
        with_events.input.event_ids,
        ["proposal#p-1:4", "proposal#p-1:5"]
    );
    assert!(with_events.dynamo_item["events"]["S"].is_string());
    assert!(with_events.dynamo_item["rejection"]["S"].is_string());

    for plain in [
        "rejected",
        "rejected stale fence",
        "rejected stale fence, retries exhausted",
    ] {
        let plain = row(plain);

        assert!(plain.input.event_ids.is_empty(), "{}", plain.name);
        assert!(plain.dynamo_item.get("events").is_none(), "{}", plain.name);
        assert_eq!(plain.postgres_row["events"], "[]", "{}", plain.name);
    }
}

/// A command's identity is its intent: the digest of a rename, whose write set comes from state, is
/// computed from the request alone, and Rust's `CommandIdentity` reproduces Go's vector.
#[test]
fn a_command_identity_digests_from_the_request_alone() {
    let fixtures = outcome_fixtures();
    let vector = fixtures
        .digests
        .iter()
        .find(|vector| vector.name == "rename, state-derived write set is not identity")
        .expect("the rename digest vector");

    let identity = CommandIdentity::new(&vector.input.command_type)
        .with_targets(vector.input.targets.iter().map(String::as_str))
        .with_payload_json(vector.input.payload.clone().unwrap_or_default());
    let metadata = Metadata::default().with_principal_id(vector.input.principal.as_str());

    assert_eq!(
        default_digest(&identity.digest_input(&metadata)).unwrap(),
        vector.digest
    );

    let row = fixtures
        .rows
        .iter()
        .find(|row| row.name == "accepted rename, state-derived write set")
        .expect("the rename row vector");
    assert_eq!(
        row.input.digest, vector.digest,
        "the stored outcome carries the request's digest, whatever streams the commit wrote"
    );
}

#[test]
fn command_identity_digests_match_go_byte_for_byte() {
    for vector in outcome_fixtures().digests {
        let name = &vector.name;
        let input = DigestInput {
            command_type: vector.input.command_type,
            targets: vector.input.targets.iter().map(EntityId::new).collect(),
            payload: vector.input.payload.unwrap_or_default().into_bytes(),
            principal: vector.input.principal,
        };

        assert_eq!(
            hex(&canonical_digest_bytes(&input)),
            vector.canonical_hex,
            "{name}: canonical bytes"
        );
        assert_eq!(
            default_digest(&input).unwrap(),
            vector.digest,
            "{name}: digest"
        );
    }
}

#[test]
fn outcome_rows_match_go_byte_for_byte() {
    for vector in outcome_fixtures().rows {
        let name = &vector.name;
        let input = vector.input;

        let mut outcome = match input.status.as_str() {
            "accepted" => Outcome::accepted(input.command_id.clone(), input.digest.clone()),
            "rejected" => Outcome::rejected(
                input.command_id.clone(),
                input.digest.clone(),
                Rejection::decode(input.rejection.as_deref().expect("a rejection")).unwrap(),
            ),
            other => panic!("{name}: unknown status {other}"),
        };
        // The response's raw text is lossy for binary bytes; the PostgreSQL row has its base64.
        if let Some(response) = vector.postgres_row["response"].as_str() {
            outcome = outcome.with_response(json::base64_bytes::decode(response).unwrap());
            if let (Some(text), Ok(bytes)) = (
                &input.response,
                std::str::from_utf8(outcome.response.as_deref().unwrap_or_default()),
            ) {
                assert_eq!(bytes, text, "{name}");
            }
        }
        outcome.event_ids = input.event_ids.iter().map(EventId::new).collect();

        // The DynamoDB item, byte for byte, and its budget size.
        let item = dynamo::outcome_item(&outcome).unwrap();
        assert_eq!(
            serde_json::to_value(&item).unwrap(),
            vector.dynamo_item,
            "{name}: dynamo item"
        );
        assert_eq!(
            dynamo::outcome_item_size(&outcome).unwrap(),
            vector.item_size,
            "{name}: item size"
        );
        let go_item: dynamo::Item = serde_json::from_value(vector.dynamo_item).unwrap();
        assert_eq!(
            dynamo::outcome_from_item(&go_item).unwrap(),
            outcome,
            "{name}: decode Go item"
        );

        // The PostgreSQL row fields.
        let row = OutcomeRow::from_outcome(&outcome).unwrap();
        let expected_events = if vector.events_json.is_empty() {
            "[]"
        } else {
            vector.events_json.as_str()
        };
        assert_eq!(row.events, expected_events, "{name}: events text");
        assert_eq!(
            row.rejection.as_deref().unwrap_or_default(),
            vector.rejection_json,
            "{name}: rejection text"
        );
        let pg = serde_json::json!({
            "command_id": row.command_id,
            "status": row.status,
            "digest": row.digest,
            "response": row.response.as_ref().map(|response| json::base64_bytes::encode(response)),
            "events": row.events,
            "rejection": row.rejection,
            "code": row.code,
        });
        assert_eq!(pg, vector.postgres_row, "{name}: postgres row");
        assert_eq!(row.into_outcome().unwrap(), outcome, "{name}: decode row");
    }
}

#[test]
fn rejection_numbers_take_the_canonical_text_like_go() {
    let vectors = outcome_fixtures().rejection_numbers;
    assert!(!vectors.is_empty());

    // The literal text matters (serde_json would parse 0.0010e2 to 0.1), so the vectors go
    // through the text-level rewrite that `Rejection::encode` ends with.
    for vector in vectors {
        let name = &vector.name;
        let result = canonical_numbers(&vector.input);

        match vector.error.as_str() {
            "" => assert_eq!(result.unwrap(), vector.stored, "{name}"),
            "invalid-argument" => assert!(
                matches!(result, Err(Error::InvalidArgument(_))),
                "{name}: {result:?}"
            ),
            "outcome-too-large" => assert!(
                matches!(
                    result,
                    Err(Error::OutcomeTooLarge {
                        what: "rejection",
                        ..
                    })
                ),
                "{name}: {result:?}"
            ),
            other => panic!("{name}: unknown fixture error {other}"),
        }
    }
}

#[test]
fn malformed_outcome_rows_are_refused_as_corrupt_like_go() {
    let rows = outcome_fixtures().malformed_rows;
    assert!(!rows.is_empty());

    for row in rows {
        let item: dynamo::Item = serde_json::from_value(row.dynamo_item).unwrap();
        let decoded = dynamo::outcome_from_item(&item);

        assert!(
            matches!(decoded, Err(photon_evt::Error::InvalidRecord(_))),
            "{}: a malformed row must be refused, never replayed: {decoded:?}",
            row.name
        );
    }
}

/// The marker row a writer stores when it declares `stream-heads` on a fresh table (ADR 0007)
/// is the item Go recorded: the first commit to an empty table creates it at version 1.
#[tokio::test]
async fn the_marker_a_fresh_table_declares_matches_go() {
    use photon_evt::{Commit, FeatureMarker, Repository};

    let data = std::fs::read(compat_dir().join("fixtures/feature-marker.json")).unwrap();
    let fixtures: Value = serde_json::from_slice(&data).unwrap();
    let declared: Vec<_> = fixtures["commits"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case.get("declared").is_some())
        .collect();
    assert!(
        !declared.is_empty(),
        "the vectors record a fresh declaration"
    );

    for case in declared {
        let repo = MemRepository::new();
        let id = EntityId::new("fresh-declaration");
        let event = SerializedEvent {
            id: EventId::for_event(&id, EventSequence(1)),
            sequence: EventSequence(1),
            event_type: "account:opened".into(),
            version: photon_evt::EventVersion(1),
            entity_id: id.clone(),
            entity_type: "account".into(),
            payload: b"{}".to_vec(),
            ..Default::default()
        };
        repo.commit(Commit::new(id, vec![event])).await.unwrap();

        let manifest = repo.read_manifest().await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(dynamo::feature_marker_item(&manifest).unwrap()).unwrap(),
            case["declared"],
            "{}: the declared marker item",
            case["name"]
        );
    }
}

/// Go's schema DDL and Rust's create the same relations; the live PostgreSQL tests execute both in
/// either order. Offline, both define the heads table with the same columns.
#[test]
fn the_schema_ddl_defines_the_heads_table_like_go() {
    let data = std::fs::read(compat_dir().join("fixtures/feature-marker.json")).unwrap();
    let fixtures: Value = serde_json::from_slice(&data).unwrap();

    for vector in fixtures["postgresSchemas"].as_array().unwrap() {
        let events = vector["eventsTable"].as_str().unwrap();
        let snapshots = vector["snapshotsTable"].as_str().unwrap();
        let squash = |text: &str| text.split_whitespace().collect::<Vec<_>>().join(" ");
        let go = squash(vector["ddl"].as_str().unwrap());
        let rust = squash(&photon_evt::wire::postgres::schema_ddl(events, snapshots));

        assert_eq!(rust, go, "{events}: the DDL, whitespace aside");
    }
}

/// The rebuild-fence vectors (`rebuildFence` in `feature-marker.json`, C10) as semantics: from
/// each vector's state of the two rows, under Go's fixed clock, the in-memory fence returns the
/// recorded result and leaves the recorded items. The DynamoDB requests themselves are asserted
/// by `photon-evt`'s own tests, and against a recording endpoint by the live suite.
#[tokio::test]
async fn rebuild_fence_vectors_match_go() {
    use photon_evt::{RebuildFence, RebuildGeneration, RebuildState, ViewOperation};

    let data = std::fs::read(compat_dir().join("fixtures/feature-marker.json")).unwrap();
    let fixtures: Value = serde_json::from_slice(&data).unwrap();
    let vectors = fixtures["rebuildFence"].as_array().unwrap();
    assert!(vectors.len() >= 16, "every lifecycle operation is pinned");

    let item = |value: &Value| -> Option<dynamo::Item> {
        (!value.is_null()).then(|| serde_json::from_value(value.clone()).unwrap())
    };
    let items = |state: &RebuildState| {
        serde_json::json!({
            "generation": (state.generation != RebuildGeneration::default())
                .then(|| dynamo::rebuild_generation_item(state.generation)),
            "lock": state.lock.as_ref()
                .map(dynamo::rebuild_lock_item),
        })
    };
    let error = |error: Error| match error {
        Error::RebuildLockHeld { .. } => serde_json::json!({"error": "lockHeld"}),
        Error::RebuildLeaseLost { .. } => serde_json::json!({"error": "leaseLost"}),
        other => panic!("unexpected {other}"),
    };
    let generation = |generation: RebuildGeneration| {
        let mut result = serde_json::json!({"generation": generation.value});
        if generation.force_released {
            result["forceReleased"] = serde_json::json!(true);
        }

        result
    };

    for vector in vectors {
        let name = vector["name"].as_str().unwrap();
        let owner = vector["owner"].as_str().unwrap_or_default();
        // The lease a renew, an end, or a guarded write names: its owner and fencing token.
        let held = photon_evt::RebuildLease {
            owner: owner.to_owned(),
            generation: vector["leaseGeneration"].as_i64().unwrap_or_default(),
            lease_expires_at: 0,
        };
        let lease = Duration::from_secs(vector["leaseSeconds"].as_u64().unwrap_or_default());
        let before = RebuildState {
            generation: item(&vector["before"]["generation"])
                .map(|item| dynamo::rebuild_generation_from_item(&item).unwrap())
                .unwrap_or_default(),
            lock: item(&vector["before"]["lock"])
                .map(|item| dynamo::rebuild_lock_from_item(&item).unwrap()),
        };
        assert_eq!(
            items(&before),
            vector["before"],
            "{name}: the items decode and re-encode"
        );

        let repository =
            MemRepository::new().with_clock(|| UNIX_EPOCH + Duration::from_secs(1_790_000_000));
        repository.set_rebuild_state(before);

        let result = match vector["operation"].as_str().unwrap() {
            "state" => {
                let state = repository.rebuild_state().await.unwrap();
                let mut result = generation(state.generation);
                if let Some(lock) = state.lock {
                    result["lockOwner"] = serde_json::json!(lock.owner);
                    result["leaseExpiresAt"] = serde_json::json!(lock.lease_expires_at);
                }

                result
            }
            "generation" => generation(repository.rebuild_generation().await.unwrap()),
            "begin" => match repository.begin_rebuild(owner, lease).await {
                Ok(lease) => serde_json::json!({
                    "generation": lease.generation, "leaseExpiresAt": lease.lease_expires_at}),
                Err(err) => error(err),
            },
            "renew" => match repository.renew_rebuild(&held, lease).await {
                Ok(renewed) => serde_json::json!({"leaseExpiresAt": renewed.lease_expires_at}),
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
                    Ok(()) => serde_json::json!({}),
                    Err(err) => error(err),
                }
            }
            other => panic!("{name}: unknown operation {other}"),
        };

        assert_eq!(result, vector["result"], "{name}: result");
        assert_eq!(
            items(&repository.rebuild_state().await.unwrap()),
            vector["after"],
            "{name}: the items after"
        );
    }
}
