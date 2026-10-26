//! Asserts the Rust view repository encoding and the typed view codec against the golden fixtures
//! the Go implementation generated (`compat/fixtures/views.json`). Regenerate them with
//! `go -C go test ./internal/compat -run TestFixtures -update`.

use std::collections::BTreeMap;

use evt_compat::scenario::compat_dir;
use photon_evt::wire::dynamo::{
    self, AttributeValue, Item, MAX_VIEW_BATCH_GET, MAX_VIEW_BATCH_WRITE, ViewQuery,
};
use photon_evt::{Error, ViewRow, decode_json_view, json};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize)]
struct Fixtures {
    limits: Limits,
    puts: Vec<Put>,
    deletes: Vec<Delete>,
    gets: Vec<Get>,
    decodes: Vec<Decode>,
    queries: Vec<Query>,
    cursors: Vec<Cursor>,
    #[serde(rename = "startKeys")]
    start_keys: Vec<StartKey>,
    codec: Vec<CodecPut>,
    #[serde(rename = "codecDecode")]
    codec_decode: Vec<CodecDecode>,
}

#[derive(Deserialize)]
struct Limits {
    write: usize,
    get: usize,
}

#[derive(Deserialize)]
struct RowInput {
    pk: String,
    sk: String,
    #[serde(rename = "entityId")]
    entity_id: String,
    #[serde(rename = "entityType")]
    entity_type: String,
    payload: String,
    ttl: i64,
}

impl RowInput {
    fn row(&self) -> ViewRow {
        ViewRow::new(
            &self.pk,
            &self.sk,
            &self.entity_id,
            &self.entity_type,
            self.payload.as_bytes(),
        )
        .with_ttl(self.ttl)
    }
}

#[derive(Deserialize)]
struct Put {
    name: String,
    row: RowInput,
    #[serde(rename = "dynamoItem")]
    item: Value,
}

#[derive(Deserialize)]
struct Delete {
    name: String,
    pk: String,
    sk: String,
    key: Value,
}

#[derive(Deserialize)]
struct Get {
    name: String,
    pk: String,
    key: Value,
    #[serde(rename = "consistentRead")]
    consistent_read: bool,
}

#[derive(Deserialize)]
struct Decode {
    name: String,
    #[serde(rename = "dynamoItem")]
    item: Item,
    row: Option<RowInput>,
    error: bool,
}

#[derive(Deserialize)]
struct Query {
    name: String,
    kind: String,
    key: String,
    limit: i64,
    cursor: String,
    index: String,
    #[serde(rename = "keyCondition")]
    key_condition: String,
    values: Value,
    #[serde(rename = "requestLimit")]
    request_limit: Option<i32>,
    #[serde(rename = "startKey")]
    start_key: Option<Value>,
    #[serde(rename = "consistentSet")]
    consistent_set: bool,
    #[serde(rename = "consistentRead")]
    consistent_read: bool,
}

#[derive(Deserialize)]
struct Cursor {
    name: String,
    #[serde(rename = "lastEvaluatedKey")]
    last_evaluated_key: Item,
    cursor: String,
}

#[derive(Deserialize)]
struct StartKey {
    name: String,
    cursor: String,
    #[serde(rename = "startKey")]
    start_key: Option<Item>,
    error: bool,
}

#[derive(Deserialize)]
struct CodecPut {
    name: String,
    pk: String,
    sk: String,
    #[serde(rename = "entityId")]
    entity_id: String,
    value: String,
    #[serde(rename = "dynamoItem")]
    item: Value,
}

#[derive(Deserialize)]
struct CodecDecode {
    name: String,
    payload: String,
    valid: bool,
    canonical: String,
}

/// The typed view of the codec vectors: the Rust twin of Go's `CodecProfile`, fields in the same
/// order. `#[serde(default)]` makes a missing field its zero value, as Go's decoder does.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
struct CodecProfile {
    name: String,
    note: String,
    bio: Option<String>,
    tags: Option<Vec<String>>,
    labels: Option<BTreeMap<String, String>>,
    count: i64,
    big: u64,
}

fn fixtures() -> Fixtures {
    let data = std::fs::read(compat_dir().join("fixtures/views.json")).expect("read view fixtures");

    serde_json::from_slice(&data).expect("parse view fixtures")
}

fn value<T: Serialize>(item: &T) -> Value {
    serde_json::to_value(item).unwrap()
}

#[test]
fn batch_limits_match_go() {
    let limits = fixtures().limits;

    assert_eq!(MAX_VIEW_BATCH_WRITE, limits.write);
    assert_eq!(MAX_VIEW_BATCH_GET, limits.get);
}

#[test]
fn put_items_match_go() {
    for case in fixtures().puts {
        let item = dynamo::view_row_item(&case.row.row()).unwrap();

        assert_eq!(value(&item), case.item, "{}: item", case.name);
    }
}

#[test]
fn delete_and_get_keys_match_go() {
    for case in fixtures().deletes {
        let sk = if case.sk.is_empty() { "VIEW" } else { &case.sk };

        assert_eq!(
            value(&dynamo::view_key(&case.pk, sk)),
            case.key,
            "{}: delete key",
            case.name
        );
    }

    for case in fixtures().gets {
        assert!(case.consistent_read, "{}: Go reads strongly", case.name);
        assert_eq!(
            value(&dynamo::view_key(&case.pk, "VIEW")),
            case.key,
            "{}: get key",
            case.name
        );
    }
}

#[test]
fn items_decode_like_go() {
    for case in fixtures().decodes {
        match (dynamo::view_row_from_item(&case.item), case.error) {
            (Err(Error::InvalidRecord(_)), true) => {}
            (Ok(row), false) => {
                assert_eq!(row, case.row.unwrap().row(), "{}: row", case.name);
            }
            (other, _) => panic!("{}: got {other:?}, want error {}", case.name, case.error),
        }
    }
}

#[test]
fn query_requests_match_go() {
    for case in fixtures().queries {
        // Go ignores a limit at or below zero; the Rust API cannot express one.
        let limit = usize::try_from(case.limit).ok();
        let cursor = Some(case.cursor.as_str());

        let query: ViewQuery = match case.kind.as_str() {
            "pk" => dynamo::view_query_by_pk(&case.key, limit, cursor),
            "entityType" => dynamo::view_query_by_entity_type(&case.key, limit, cursor),
            other => panic!("{}: unknown kind {other}", case.name),
        }
        .unwrap();

        let name = &case.name;
        assert_eq!(query.index.unwrap_or_default(), case.index, "{name}: index");
        assert_eq!(query.key_condition, case.key_condition, "{name}: condition");
        assert_eq!(value(&query.values), case.values, "{name}: values");
        assert_eq!(query.limit, case.request_limit, "{name}: limit");
        assert_eq!(
            query.start_key.as_ref().map(value),
            case.start_key,
            "{name}: start key"
        );
        // Go reads a partition of the table strongly and leaves ConsistentRead unset on the
        // entity-type index, which cannot be read consistently. The Rust repository sends
        // ConsistentRead exactly when the query reads the table (the request bodies are pinned in
        // view_requests.rs).
        let table = case.index.is_empty();
        assert_eq!(case.consistent_set, table, "{name}: consistent read set");
        assert_eq!(case.consistent_read, table, "{name}: consistent read");
        assert_eq!(query.reads_base_table(), table, "{name}: reads the table");
    }
}

#[test]
fn cursors_match_go() {
    for case in fixtures().cursors {
        assert_eq!(
            dynamo::encode_view_cursor(&case.last_evaluated_key).unwrap(),
            case.cursor,
            "{}: cursor",
            case.name
        );
    }
}

#[test]
fn cursors_decode_like_go() {
    for case in fixtures().start_keys {
        match (dynamo::decode_view_cursor(&case.cursor), case.error) {
            (Err(Error::InvalidArgument(_)), true) => {}
            (Ok(key), false) => assert_eq!(Some(key), case.start_key, "{}: key", case.name),
            (other, _) => panic!("{}: got {other:?}, want error {}", case.name, case.error),
        }
    }

    // A cursor made from a key decodes back to the string attributes of that key.
    let key = Item::from([
        ("pk".to_owned(), AttributeValue::S("p<&>".into())),
        ("sk".to_owned(), AttributeValue::S("s".into())),
        ("n".to_owned(), AttributeValue::N("1".into())),
    ]);
    let back = dynamo::decode_view_cursor(&dynamo::encode_view_cursor(&key).unwrap()).unwrap();

    assert_eq!(back.len(), 2);
    assert_eq!(back["pk"], AttributeValue::S("p<&>".into()));
}

#[test]
fn typed_values_write_the_bytes_go_writes() {
    for case in fixtures().codec {
        let name = &case.name;
        let profile: CodecProfile = serde_json::from_str(&case.value).unwrap();
        let payload = json::to_vec(&profile).unwrap();

        assert_eq!(
            String::from_utf8(payload.clone()).unwrap(),
            case.value,
            "{name}: payload"
        );

        let row = ViewRow::new(&case.pk, &case.sk, &case.entity_id, "profile", payload);

        assert_eq!(
            value(&dynamo::view_row_item(&row).unwrap()),
            case.item,
            "{name}: item"
        );
    }
}

#[test]
fn typed_payloads_decode_like_go() {
    for case in fixtures().codec_decode {
        let row = ViewRow::new("p", "s", "", "", case.payload.as_bytes());
        let decoded = decode_json_view::<CodecProfile>(&row);

        match (decoded, case.valid) {
            (Ok(profile), true) => assert_eq!(
                String::from_utf8(json::to_vec(&profile).unwrap()).unwrap(),
                case.canonical,
                "{}: canonical",
                case.name
            ),
            (Err(err), false) => {
                assert_eq!(
                    (err.pk.as_str(), err.sk.as_str()),
                    ("p", "s"),
                    "{}",
                    case.name
                );
            }
            (other, valid) => panic!("{}: got {other:?}, want valid {valid}", case.name),
        }
    }
}
