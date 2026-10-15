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
//! | `version`    | N    | event version            |                                 |
//! | `entityType` | S    | aggregate type           | aggregate type                  |
//! | `payload`    | S    | payload JSON (raw text)  | aggregate state JSON (raw text) |
//! | `metadata`   | S    | metadata JSON            |                                 |
//! | `seq`        | N    |                          | snapshot sequence               |
//! | `eventSeq`   | N    |                          | last event sequence covered     |
//! | `ttl`        | N    | optional, epoch seconds  | optional, epoch seconds         |
//!
//! Items are modeled with [`AttributeValue`], whose serde encoding is DynamoDB JSON
//! (`{"S": "..."}`), so they can be compared against fixtures and converted to SDK types.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::features::{FEATURES_ID, FEATURES_SORT_KEY, FeatureManifest};
use crate::ids::{EntityId, EntityType, EventId, EventSequence, EventType, EventVersion};
use crate::json;
use crate::metadata::Metadata;

/// The sort key of the inline snapshot item.
pub const SNAPSHOT_SORT_KEY: i64 = 0;

/// The sort key of a single-item view row.
pub const DEFAULT_VIEW_SORT_KEY: &str = "VIEW";

/// Condition for event puts: the `(pk, sk)` must not exist yet.
pub const EVENT_CONDITION: &str = "attribute_not_exists(sk)";

/// Condition for the snapshot-item check in a commit without a snapshot: the commit's first event
/// (`:first`) is above the stored snapshot's `eventSeq`. Compaction deletes the events a snapshot
/// covers, so [`EVENT_CONDITION`] alone would let a writer that loaded before the snapshot recreate
/// a compacted sequence.
pub const FLOOR_CONDITION: &str = "attribute_not_exists(eventSeq) OR eventSeq < :first";

/// Condition for the first snapshot put in a commit (snapshot sequence 1): no snapshot exists yet.
pub const FIRST_SNAPSHOT_CONDITION: &str = "attribute_not_exists(seq)";

/// Condition for later snapshot puts in a commit: the stored snapshot sequence is the one the
/// writer loaded (`:seq`), and the commit starts above its floor (`:first`). It folds in
/// [`FLOOR_CONDITION`] because a transaction cannot touch the snapshot item twice.
pub const NEXT_SNAPSHOT_CONDITION: &str = "seq = :seq AND eventSeq < :first";

/// Condition for a fenced read-only stream's slot check: `(pk, sk = head + 1)` must be free, so
/// no writer has appended past the head the reader saw (ADR 0004, C2).
pub const FENCE_SLOT_CONDITION: &str = "attribute_not_exists(sk)";

/// Condition for a fenced read-only stream's snapshot companion on `(pk, sk = 0)`: the stored
/// snapshot must not cover past the fenced head (`:head`), which closes the compaction ABA. It
/// passes on a stream with no snapshot item. Always included — any store instance can snapshot
/// the stream and compaction deletes whatever a durable snapshot covers.
pub const FENCE_COMPANION_CONDITION: &str = "attribute_not_exists(sk) OR eventSeq <= :head";

/// Condition for out-of-band snapshot puts: never lower the stored `eventSeq` (`:new`).
pub const PUT_SNAPSHOT_CONDITION: &str = "attribute_not_exists(eventSeq) OR eventSeq <= :new";

/// Condition for the feature-marker check every v0.9 commit carries (`:v` is the version the
/// writer validated). A writer that validated a table with no marker conditions on
/// `attribute_not_exists(pk)` instead.
pub const MARKER_CONDITION: &str = "attribute_not_exists(pk) OR version = :v";

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

/// Encodes the feature-marker item (`pk = SYSTEM#FEATURES`, `sk = 0`). `historyDependent` and
/// `retention` are written only when their set is non-empty.
pub fn feature_marker_item(manifest: &FeatureManifest) -> Result<Item, Error> {
    let mut item = Item::new();
    item.insert("pk".into(), s(FEATURES_ID));
    item.insert("sk".into(), n(FEATURES_SORT_KEY));
    item.insert("version".into(), n(manifest.version));
    item.insert("features".into(), s(manifest.features_json()));
    if let Some(history_dependent) = manifest.history_dependent_json() {
        item.insert("historyDependent".into(), s(history_dependent));
    }
    if let Some(retention) = manifest.retention_json() {
        item.insert("retention".into(), s(retention));
    }

    Ok(item)
}

/// Decodes the feature-marker item.
pub fn feature_marker_from_item(item: &Item) -> Result<FeatureManifest, Error> {
    FeatureManifest::from_attributes(
        required_number(item, "version")?,
        required_string(item, "features")?,
        optional_string(item, "historyDependent")?,
        optional_string(item, "retention")?,
    )
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

/// The `ttl` for an item written at `now` under a retention of `retention`: `now + retention` in
/// whole Unix seconds, rounded down after adding, as Go's `time.Time.Add(...).Unix()` does. Returns
/// `None` (no `ttl` attribute) for a zero retention, or when the expiry is out of range.
pub fn expiry(now: SystemTime, retention: Duration) -> Option<i64> {
    if retention.is_zero() {
        return None;
    }

    let seconds = now
        .checked_add(retention)?
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_secs();

    i64::try_from(seconds).ok()
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

/// Decodes an event-log item. As in Go, missing non-key attributes decode to empty values,
/// `null` metadata decodes to the default, and missing or empty metadata is an error.
pub fn event_from_item(item: &Item) -> Result<SerializedEvent, Error> {
    let entity_id = EntityId::new(required_string(item, "pk")?);
    let sequence = EventSequence(required_number(item, "sk")?);
    let metadata_json = optional_string(item, "metadata")?.unwrap_or_default();

    if metadata_json.trim().is_empty() {
        return Err(Error::InvalidRecord(format!(
            "event {entity_id}:{sequence} has empty metadata"
        )));
    }

    // Go's json.Unmarshal leaves metadata at its zero value for a JSON null.
    let metadata = serde_json::from_str::<Option<Metadata>>(metadata_json)
        .map_err(|err| {
            Error::InvalidRecord(format!("event {entity_id}:{sequence} metadata: {err}"))
        })?
        .unwrap_or_default();

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_metadata_like_go() {
        let item = |metadata: &str| {
            Item::from([
                ("pk".into(), s("acct-1")),
                ("sk".into(), n(1)),
                ("metadata".into(), s(metadata)),
            ])
        };

        let event = event_from_item(&item("null")).unwrap();
        assert_eq!(event.metadata, Metadata::default(), "null metadata");

        assert!(
            matches!(event_from_item(&item(" ")), Err(Error::InvalidRecord(_))),
            "empty metadata"
        );
    }

    #[test]
    fn feature_marker_round_trips() {
        let mut manifest = FeatureManifest {
            version: 3,
            ..FeatureManifest::default()
        };
        manifest.features.insert("journal".into(), 1);
        manifest.history_dependent.insert("audit-feed".into());

        let item = feature_marker_item(&manifest).unwrap();
        assert_eq!(item.get("pk"), Some(&s(crate::features::FEATURES_ID)));
        assert_eq!(item.get("sk"), Some(&n(0)));
        assert_eq!(
            item.get("features"),
            Some(&s(r#"{"journal":1}"#)),
            "sorted keys"
        );
        assert_eq!(item.get("historyDependent"), Some(&s(r#"["audit-feed"]"#)));
        assert_eq!(item.get("retention"), None, "an empty set is absent");

        assert_eq!(feature_marker_from_item(&item).unwrap(), manifest);
        assert!(feature_marker_from_item(&Item::new()).is_err());
    }
}
