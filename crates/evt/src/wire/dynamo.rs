//! The DynamoDB item layout of the event log, independent of any AWS SDK.
//!
//! One table holds every stream. Events are items keyed by `pk` (entity ID, string) and `sk`
//! (event sequence, number, starting at 1). The latest snapshot lives inline in the same
//! partition at `sk = 0`.
//!
//! | Attribute    | Type | Event item               | Snapshot item (`sk = 0`)        |
//! |--------------|------|--------------------------|---------------------------------|
//! | `pk`         | S    | entity ID                | entity ID                       |
//! | `sk`         | N    | event sequence           | `0`                             |
//! | `type`       | S    | event type               |                                 |
//! | `version`    | N    | payload version          |                                 |
//! | `entityType` | S    | aggregate type           | aggregate type                  |
//! | `payload`    | S    | event JSON (raw text)    | aggregate state JSON (raw text) |
//! | `metadata`   | S    | metadata JSON            |                                 |
//! | `seq`        | N    |                          | snapshot sequence               |
//! | `eventSeq`   | N    |                          | last event sequence covered     |
//! | `ttl`        | N    | optional, epoch seconds  | optional, epoch seconds         |
//!
//! Items are modeled with [`AttributeValue`], whose serde encoding is DynamoDB JSON
//! (`{"S": "..."}`), so they can be compared against fixtures and converted to SDK types.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::ids::{EntityId, EntityType, EventId, EventSequence, EventType, EventVersion};
use crate::json;
use crate::metadata::Metadata;

/// The sort key of the inline snapshot item.
pub const SNAPSHOT_SORT_KEY: i64 = 0;

/// The sort key of a single-item view row.
pub const DEFAULT_VIEW_SORT_KEY: &str = "VIEW";

/// Condition for event puts: the `(pk, sk)` must not exist yet.
pub const EVENT_CONDITION: &str = "attribute_not_exists(sk)";

/// Condition for snapshot puts in a commit: no snapshot yet, or the stored snapshot sequence is
/// the one the writer loaded (`:seq`).
pub const SNAPSHOT_CONDITION: &str = "attribute_not_exists(seq) OR (seq = :seq)";

/// Condition for out-of-band snapshot puts: never lower the stored `eventSeq` (`:new`).
pub const PUT_SNAPSHOT_CONDITION: &str = "attribute_not_exists(eventSeq) OR eventSeq <= :new";

/// A DynamoDB attribute value. Serializes as DynamoDB JSON.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttributeValue {
    /// A string.
    S(String),
    /// A number, as its decimal string.
    N(String),
    /// Binary data (base64 in DynamoDB JSON).
    B(#[serde(with = "json::base64_bytes")] Vec<u8>),
    /// A boolean.
    #[serde(rename = "BOOL")]
    Bool(bool),
    /// Null.
    #[serde(rename = "NULL")]
    Null(bool),
    /// A map.
    M(BTreeMap<String, AttributeValue>),
    /// A list.
    L(Vec<AttributeValue>),
    /// A string set.
    #[serde(rename = "SS")]
    Ss(Vec<String>),
    /// A number set.
    #[serde(rename = "NS")]
    Ns(Vec<String>),
    /// A binary set.
    #[serde(rename = "BS")]
    Bs(Vec<String>),
}

/// A DynamoDB item.
pub type Item = BTreeMap<String, AttributeValue>;

fn s(value: impl Into<String>) -> AttributeValue {
    AttributeValue::S(value.into())
}

fn n(value: i64) -> AttributeValue {
    AttributeValue::N(value.to_string())
}

fn utf8(bytes: &[u8], what: &str) -> Result<String, Error> {
    String::from_utf8(bytes.to_vec()).map_err(|_| {
        Error::InvalidRecord(format!(
            "{what} is not valid UTF-8 and cannot be stored as a string"
        ))
    })
}

/// Encodes an event as an event-log item. `ttl` (epoch seconds) is written only when positive.
pub fn event_item(event: &SerializedEvent, ttl: Option<i64>) -> Result<Item, Error> {
    let mut item = Item::new();
    item.insert("pk".into(), s(event.entity_id.as_str()));
    item.insert("sk".into(), n(event.sequence.0));
    item.insert("type".into(), s(event.event_type.as_str()));
    item.insert("version".into(), n(event.version.0));
    item.insert("entityType".into(), s(event.entity_type.as_str()));
    item.insert("payload".into(), s(utf8(&event.payload, "event payload")?));
    item.insert("metadata".into(), s(json::to_string(&event.metadata)?));
    insert_ttl(&mut item, ttl);

    Ok(item)
}

/// Encodes the inline snapshot item (`sk = 0`). `ttl` (epoch seconds) is written only when positive.
pub fn snapshot_item(snapshot: &SerializedSnapshot, ttl: Option<i64>) -> Result<Item, Error> {
    let mut item = Item::new();
    item.insert("pk".into(), s(snapshot.entity_id.as_str()));
    item.insert("sk".into(), n(SNAPSHOT_SORT_KEY));
    item.insert("seq".into(), n(snapshot.sequence.0));
    item.insert("eventSeq".into(), n(snapshot.event_sequence.0));
    item.insert("entityType".into(), s(snapshot.entity_type.as_str()));
    item.insert(
        "payload".into(),
        s(utf8(&snapshot.payload, "snapshot payload")?),
    );
    insert_ttl(&mut item, ttl);

    Ok(item)
}

/// Encodes a single-item view row: `pk = {entityType}#{entityId}`, `sk = "VIEW"`.
pub fn view_item(
    entity_type: &EntityType,
    entity_id: &EntityId,
    payload: &[u8],
) -> Result<Item, Error> {
    let mut item = Item::new();
    item.insert("pk".into(), s(format!("{entity_type}#{entity_id}")));
    item.insert("sk".into(), s(DEFAULT_VIEW_SORT_KEY));
    item.insert("entityID".into(), s(entity_id.as_str()));
    item.insert("entityType".into(), s(entity_type.as_str()));
    item.insert("payload".into(), s(utf8(payload, "view payload")?));

    Ok(item)
}

fn insert_ttl(item: &mut Item, ttl: Option<i64>) {
    if let Some(ttl) = ttl.filter(|ttl| *ttl > 0) {
        item.insert("ttl".into(), n(ttl));
    }
}

/// Reports whether an item is the inline snapshot (`sk = 0`).
pub fn is_snapshot_item(item: &Item) -> bool {
    matches!(item.get("sk"), Some(AttributeValue::N(sk)) if sk.parse::<i64>() == Ok(SNAPSHOT_SORT_KEY))
}

/// Decodes an event-log item. Missing non-key attributes decode to empty values, as in Go;
/// missing or empty metadata is an error, as in Go.
pub fn event_from_item(item: &Item) -> Result<SerializedEvent, Error> {
    let entity_id = EntityId::new(required_string(item, "pk")?);
    let sequence = EventSequence(required_number(item, "sk")?);
    let metadata_json = optional_string(item, "metadata")?.unwrap_or_default();

    if metadata_json.trim().is_empty() {
        return Err(Error::InvalidRecord(format!(
            "event {entity_id}:{sequence} has empty metadata"
        )));
    }

    let metadata: Metadata = serde_json::from_str(metadata_json).map_err(|err| {
        Error::InvalidRecord(format!("event {entity_id}:{sequence} metadata: {err}"))
    })?;

    Ok(SerializedEvent {
        id: EventId::for_event(&entity_id, sequence),
        sequence,
        event_type: EventType::new(optional_string(item, "type")?.unwrap_or_default()),
        version: EventVersion(optional_number(item, "version")?.unwrap_or_default()),
        entity_type: EntityType::new(optional_string(item, "entityType")?.unwrap_or_default()),
        payload: optional_string(item, "payload")?
            .unwrap_or_default()
            .as_bytes()
            .to_vec(),
        metadata,
        entity_id,
    })
}

/// Decodes the inline snapshot item.
pub fn snapshot_from_item(item: &Item) -> Result<SerializedSnapshot, Error> {
    Ok(SerializedSnapshot {
        entity_type: EntityType::new(optional_string(item, "entityType")?.unwrap_or_default()),
        entity_id: EntityId::new(required_string(item, "pk")?),
        sequence: EventSequence(optional_number(item, "seq")?.unwrap_or_default()),
        event_sequence: EventSequence(optional_number(item, "eventSeq")?.unwrap_or_default()),
        payload: optional_string(item, "payload")?
            .unwrap_or_default()
            .as_bytes()
            .to_vec(),
    })
}

fn required_string<'a>(item: &'a Item, name: &str) -> Result<&'a str, Error> {
    optional_string(item, name)?
        .ok_or_else(|| Error::InvalidRecord(format!("item has no {name} attribute")))
}

fn required_number(item: &Item, name: &str) -> Result<i64, Error> {
    optional_number(item, name)?
        .ok_or_else(|| Error::InvalidRecord(format!("item has no {name} attribute")))
}

fn optional_string<'a>(item: &'a Item, name: &str) -> Result<Option<&'a str>, Error> {
    match item.get(name) {
        None | Some(AttributeValue::Null(_)) => Ok(None),
        Some(AttributeValue::S(value)) => Ok(Some(value)),
        Some(other) => Err(Error::InvalidRecord(format!(
            "attribute {name} is not a string: {other:?}"
        ))),
    }
}

fn optional_number(item: &Item, name: &str) -> Result<Option<i64>, Error> {
    match item.get(name) {
        None | Some(AttributeValue::Null(_)) => Ok(None),
        Some(AttributeValue::N(value)) => value.parse().map(Some).map_err(|_| {
            Error::InvalidRecord(format!("attribute {name} is not an integer: {value}"))
        }),
        Some(other) => Err(Error::InvalidRecord(format!(
            "attribute {name} is not a number: {other:?}"
        ))),
    }
}
