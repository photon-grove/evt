//! Asserts the Rust implementation against the golden fixtures the Go implementation generated.
//! Regenerate them with `go test ./internal/compat -run TestFixtures -update`.

use std::collections::BTreeMap;
use std::time::{Duration, UNIX_EPOCH};

use evt_compat::scenario::{FIXTURE_RUN, ORIGIN_SOURCE, compat_dir};
use evt_compat::{Account, Runner, ScenarioFixtures, Scenarios};
use photon_evt::wire::postgres::OutcomeRow;
use photon_evt::wire::{dynamo, envelope};
use photon_evt::{
    Aggregate, DigestInput, EntityId, EntityType, EventId, EventSequence, EventType, EventVersion,
    FeatureManifest, FeatureMarker, MAX_COMMAND_ID_BYTES, MAX_OUTCOME_BYTES, MemRepository,
    Metadata, Outcome, Rejection, SerializedEvent, SerializedSnapshot, canonical_digest_bytes,
    default_digest, json, snapshot_events_due,
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
        let mut items = vec![dynamo::snapshot_item(&snapshot, ttl).unwrap()];
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
        let runner = Runner::for_scenario(MemRepository::new(), scenario, FIXTURE_RUN);
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
    digests: Vec<DigestVector>,
    rows: Vec<RowVector>,
    #[serde(default)]
    malformed_rows: Vec<MalformedRow>,
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
    streams: Vec<String>,
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
fn command_identity_digests_match_go_byte_for_byte() {
    for vector in outcome_fixtures().digests {
        let name = &vector.name;
        let input = DigestInput {
            command_type: vector.input.command_type,
            streams: vector.input.streams.iter().map(EntityId::new).collect(),
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
