//! Asserts the Rust implementation against the golden fixtures the Go implementation generated.
//! Regenerate them with `go test ./internal/compat -run TestFixtures -update`.

use std::collections::BTreeMap;

use evt_compat::scenario::{FIXTURE_RUN, ORIGIN_SOURCE, compat_dir};
use evt_compat::{Account, Runner, ScenarioFixtures, Scenarios};
use photon_evt::wire::{dynamo, envelope};
use photon_evt::{
    Aggregate, EntityId, EntityType, EventId, EventSequence, EventType, EventVersion,
    MemRepository, Metadata, SerializedEvent, SerializedSnapshot, json, snapshot_events_due,
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
        let runner = Runner::new(MemRepository::new(), scenario.snapshot_interval);
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
    }
}
