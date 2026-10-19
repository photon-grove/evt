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
//!
//! Every stream also has one small **head item** at `sk = -1` (ADR 0007), which records the
//! highest event sequence ever committed to the stream and is the only item a commit conditions on
//! for concurrency:
//!
//! | Attribute | Type | Head item (`sk = -1`)                                         |
//! |-----------|------|---------------------------------------------------------------|
//! | `pk`      | S    | entity ID                                                     |
//! | `sk`      | N    | `-1`                                                          |
//! | `head`    | N    | the highest event sequence ever committed to the stream       |
//! | `ttl`     | N    | optional, epoch seconds; only for entity types with retention |
//!
//! Items are modeled with [`AttributeValue`], whose serde encoding is DynamoDB JSON
//! (`{"S": "..."}`), so they can be compared against fixtures and converted to SDK types.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::features::{COMMAND_OUTCOME_PREFIX, FEATURES_ID, FEATURES_SORT_KEY, FeatureManifest};
use crate::ids::{EntityId, EntityType, EventId, EventSequence, EventType, EventVersion};
use crate::json;
use crate::metadata::Metadata;
use crate::outcome::{Outcome, OutcomeStatus, Rejection};
use crate::projection::ViewRow;

/// The sort key of the inline snapshot item.
pub const SNAPSHOT_SORT_KEY: i64 = 0;

/// The sort key of a single-item view row.
pub const DEFAULT_VIEW_SORT_KEY: &str = "VIEW";

/// Condition for event puts: the `(pk, sk)` must not exist yet.
pub const EVENT_CONDITION: &str = "attribute_not_exists(sk)";

/// Condition for the first snapshot put in a commit (snapshot sequence 1): no snapshot exists yet.
pub const FIRST_SNAPSHOT_CONDITION: &str = "attribute_not_exists(seq)";

/// Condition for later snapshot puts in a commit: the stored snapshot sequence is the one the
/// writer loaded (`:seq`). It is the whole condition: the head update's `head = :prev` already
/// implies the snapshot lies at or below the commit's first sequence (ADR 0007, H3).
pub const NEXT_SNAPSHOT_CONDITION: &str = "seq = :seq";

/// The sort key of a stream's head item (ADR 0007): below the snapshot at `0` and the events from
/// `1`, so no events query, `latest(n)`, or compaction range returns it.
pub const HEAD_SORT_KEY: i64 = -1;

/// The update expression of a head update on a stream without retention.
pub const HEAD_UPDATE: &str = "SET head = :last";

/// The update expression of a head update on a stream with retention: it raises `ttl` with the
/// head. `ttl` is a DynamoDB reserved word, so the expression goes through `#ttl`.
pub const HEAD_UPDATE_RETAINED: &str = "SET head = :last, #ttl = :t";

/// Condition for the head update of a stream the commit creates (first sequence 1).
pub const HEAD_CREATE_CONDITION: &str = "attribute_not_exists(head)";

/// Condition for the head update of a stream the commit appends to (first sequence above 1):
/// the stored head is the sequence before the commit's first (`:prev`).
pub const HEAD_APPEND_CONDITION: &str = "head = :prev";

/// [`HEAD_APPEND_CONDITION`] for a stream with retention: the head's `ttl` is raised by value and
/// never lowered (the never-lower rule), so an unseen writer that raised it above `:t` fails the
/// condition as an ordinary conflict.
pub const HEAD_APPEND_RETAINED_CONDITION: &str =
    "head = :prev AND (attribute_not_exists(#ttl) OR #ttl <= :t)";

/// Condition for a fenced read-only stream the reader saw at head `:head` (at least 1): no commit
/// has written the stream since.
pub const HEAD_FENCE_CONDITION: &str = "head = :head";

/// Condition for a fenced read-only stream the reader saw with no events (head 0).
pub const HEAD_FENCE_ABSENT_CONDITION: &str = "attribute_not_exists(head)";

/// Condition for the head item in an out-of-band snapshot write (H3): the stream's head covers the
/// snapshot's event sequence (`:new`).
pub const HEAD_COVERS_CONDITION: &str = "head >= :new";

/// [`HEAD_COVERS_CONDITION`] when the snapshot's event sequence is 0: a snapshot of an empty
/// stream may have no head item at all.
pub const HEAD_COVERS_OR_ABSENT_CONDITION: &str = "attribute_not_exists(head) OR head >= :new";

/// The update expression of an out-of-band snapshot write's head refresh on a stream with
/// retention: the head's `ttl` is raised to the snapshot's own (`:t`).
pub const HEAD_REFRESH_UPDATE: &str = "SET #ttl = :t";

/// Condition for the head refresh: [`HEAD_COVERS_CONDITION`] and the never-lower rule for `ttl`.
pub const HEAD_REFRESH_CONDITION: &str =
    "head >= :new AND (attribute_not_exists(#ttl) OR #ttl <= :t)";

/// Condition for out-of-band snapshot puts: never lower the stored `eventSeq` (`:new`).
pub const PUT_SNAPSHOT_CONDITION: &str = "attribute_not_exists(eventSeq) OR eventSeq <= :new";

/// Condition for the command-outcome put (v0.9, C3): no outcome is recorded for the command ID
/// yet. The row is written once, in its final state.
pub const OUTCOME_CONDITION: &str = "attribute_not_exists(pk)";

/// The placeholder of the owner value that every commit-wide view write binds (v0.9, C7), except
/// a put that requires the row to be absent.
pub const OWNER_PLACEHOLDER: &str = ":owner";

/// Condition for a commit-wide view put that requires no row at the key (v0.9, C7).
pub const VIEW_ABSENT_CONDITION: &str = "attribute_not_exists(pk)";

/// Condition for any other commit-wide view put: no row at the key, or the owner's own.
pub const VIEW_PUT_CONDITION: &str = "attribute_not_exists(pk) OR entityID = :owner";

/// Condition for a commit-wide view delete that requires the owner's row at the key.
pub const VIEW_PRESENT_CONDITION: &str = "attribute_exists(pk) AND entityID = :owner";

/// Condition for any other commit-wide view delete: no row at the key, or the owner's own.
pub const VIEW_DELETE_CONDITION: &str = "attribute_not_exists(pk) OR entityID = :owner";

/// The condition expression of a view put: it binds [`OWNER_PLACEHOLDER`] unless the put requires
/// the row to be absent.
pub fn put_condition(condition: crate::projection::ViewCondition) -> &'static str {
    match condition {
        crate::projection::ViewCondition::Absent => VIEW_ABSENT_CONDITION,
        _ => VIEW_PUT_CONDITION,
    }
}

/// The condition expression of a view delete, which always binds [`OWNER_PLACEHOLDER`].
pub fn delete_condition(condition: crate::projection::ViewCondition) -> &'static str {
    match condition {
        crate::projection::ViewCondition::Present => VIEW_PRESENT_CONDITION,
        _ => VIEW_DELETE_CONDITION,
    }
}

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

/// The sort key of a command-outcome item.
pub const OUTCOME_SORT_KEY: i64 = 1;

/// The partition key of a command's outcome row: `command-outcome#<id>`.
pub fn outcome_key(command_id: &crate::ids::CommandId) -> String {
    format!("{COMMAND_OUTCOME_PREFIX}{command_id}")
}

/// Encodes a command outcome as its item (`pk = command-outcome#<id>`, `sk = 1`): `status`,
/// `digest`, then `response` (padded base64) when there are response bytes, `events` (a JSON array
/// of event IDs) when the commit wrote any, and `rejection` with `code` when rejected. This is
/// the single source of the item's shape, shared by the writer and the commit budget.
pub fn outcome_item(outcome: &Outcome) -> Result<Item, Error> {
    let mut item = Item::new();
    item.insert("pk".into(), s(outcome_key(&outcome.command_id)));
    item.insert("sk".into(), n(OUTCOME_SORT_KEY));
    item.insert("status".into(), s(outcome.status.as_str()));
    item.insert("digest".into(), s(outcome.digest.as_str()));

    if let Some(response) = outcome
        .response
        .as_ref()
        .filter(|response| !response.is_empty())
    {
        item.insert("response".into(), s(json::base64_bytes::encode(response)));
    }

    if !outcome.event_ids.is_empty() {
        item.insert(
            "events".into(),
            s(crate::outcome::encode_event_ids(&outcome.event_ids)?),
        );
    }

    if let Some(rejection) = &outcome.rejection {
        item.insert("rejection".into(), s(rejection.encode()?));
        item.insert("code".into(), s(rejection.code.as_str()));
    }

    Ok(item)
}

/// The DynamoDB-encoded size of the outcome item, as the commit budget counts it: the sum over its
/// attributes of the name's UTF-8 length plus the value's length. `outcome.event_ids` fills the
/// `events` attribute.
pub fn outcome_item_size(outcome: &Outcome) -> Result<usize, Error> {
    Ok(crate::repository::item_size(&outcome_item(outcome)?))
}

/// Decodes a command-outcome item. The command ID comes from the key.
pub fn outcome_from_item(item: &Item) -> Result<Outcome, Error> {
    let pk = required_string(item, "pk")?;
    let command_id = pk
        .strip_prefix(COMMAND_OUTCOME_PREFIX)
        .ok_or_else(|| Error::InvalidRecord(format!("item {pk} is not a command outcome")))?;

    let status = required_string(item, "status")?;
    let status = OutcomeStatus::parse(status)
        .ok_or_else(|| Error::InvalidRecord(format!("unknown outcome status {status:?}")))?;

    let response = match optional_string(item, "response")?.filter(|text| !text.is_empty()) {
        Some(text) => Some(
            json::base64_bytes::decode(text)
                .map_err(|err| Error::InvalidRecord(format!("decode outcome response: {err}")))?,
        ),
        None => None,
    };

    let event_ids = match optional_string(item, "events")?.filter(|text| !text.is_empty()) {
        Some(text) => crate::outcome::decode_event_ids(text)?,
        None => Vec::new(),
    };

    let rejection = optional_string(item, "rejection")?
        .filter(|text| !text.is_empty())
        .map(Rejection::decode)
        .transpose()?;
    if (status == OutcomeStatus::Rejected) != rejection.is_some() {
        return Err(Error::InvalidRecord(format!(
            "outcome {command_id} is {status} but its rejection attribute disagrees"
        )));
    }

    let outcome = Outcome {
        command_id: command_id.into(),
        status,
        digest: optional_string(item, "digest")?
            .unwrap_or_default()
            .to_owned(),
        response,
        event_ids,
        rejection,
    };
    outcome.validate_stored()?;

    // The code attribute exists exactly with a rejection, and repeats its code.
    if outcome.rejection.is_none() && optional_string(item, "code")?.is_some() {
        return Err(Error::InvalidRecord(format!(
            "stored outcome for command {command_id} is corrupt: an accepted outcome carries a code attribute"
        )));
    }

    if let Some(rejection) = &outcome.rejection
        && optional_string(item, "code")?.unwrap_or_default() != rejection.code.as_str()
    {
        return Err(Error::InvalidRecord(format!(
            "stored outcome for command {command_id} is corrupt: the code attribute disagrees with the rejection"
        )));
    }

    Ok(outcome)
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

/// Encodes the row a commit-wide view put writes (v0.9, C7): all five attributes, the owner as
/// `entityID`. The payload must be valid UTF-8 (the engine checks it before any write).
pub fn view_operation_item(operation: &crate::projection::ViewOperation) -> Result<Item, Error> {
    let mut item = Item::new();
    item.insert("pk".into(), s(operation.pk.as_str()));
    item.insert("sk".into(), s(operation.sk.as_str()));
    item.insert("entityID".into(), s(operation.owner.as_str()));
    item.insert("entityType".into(), s(operation.entity_type.as_str()));
    item.insert(
        "payload".into(),
        s(utf8(&operation.payload, "view payload")?),
    );

    Ok(item)
}

/// The global secondary index over `entityType` that lists views by entity type (hash key
/// `entityType`, no range key; it projects every attribute).
pub const ENTITY_TYPE_INDEX: &str = "entityType-index";

/// DynamoDB's limit on requests per `BatchWriteItem` call, the chunk size of
/// [`put_views`](crate::ViewRepository::put_views).
pub const MAX_VIEW_BATCH_WRITE: usize = 25;

/// DynamoDB's limit on keys per `BatchGetItem` call, the chunk size of
/// [`batch_get_views`](crate::ViewRepository::batch_get_views).
pub const MAX_VIEW_BATCH_GET: usize = 100;

/// Encodes a view row as Go's `dynamo.ViewRepository.PutView` writes it: the five attributes of a
/// commit-wide view row, with an empty `sk` replaced by `"VIEW"` and a `ttl` (epoch seconds)
/// written whenever it is not zero. The payload must be valid UTF-8.
pub fn view_row_item(row: &ViewRow) -> Result<Item, Error> {
    let mut item = Item::new();
    item.insert("pk".into(), s(row.pk.as_str()));
    item.insert(
        "sk".into(),
        s(if row.sk.is_empty() {
            DEFAULT_VIEW_SORT_KEY
        } else {
            row.sk.as_str()
        }),
    );
    item.insert("entityID".into(), s(row.entity_id.as_str()));
    item.insert("entityType".into(), s(row.entity_type.as_str()));
    item.insert("payload".into(), s(utf8(&row.payload, "view payload")?));

    if row.ttl != 0 {
        item.insert("ttl".into(), n(row.ttl));
    }

    Ok(item)
}

/// Decodes a views-table item as Go's reader does: an absent or `NULL` attribute is empty (or
/// zero), a
/// string attribute may hold a number (its text), and a `ttl` must be an integer.
pub fn view_row_from_item(item: &Item) -> Result<ViewRow, Error> {
    let text = |name: &str| -> Result<String, Error> {
        match item.get(name) {
            None | Some(AttributeValue::Null(_)) => Ok(String::new()),
            Some(AttributeValue::S(value) | AttributeValue::N(value)) => Ok(value.clone()),
            Some(_) => Err(Error::InvalidRecord(format!(
                "view attribute {name} is not a string"
            ))),
        }
    };

    let ttl = match item.get("ttl") {
        None | Some(AttributeValue::Null(_)) => 0,
        Some(AttributeValue::N(value)) => value.parse().map_err(|_| {
            Error::InvalidRecord(format!("view attribute ttl is not an integer: {value}"))
        })?,
        Some(_) => {
            return Err(Error::InvalidRecord(
                "view attribute ttl is not a number".into(),
            ));
        }
    };

    Ok(ViewRow {
        pk: text("pk")?,
        sk: text("sk")?,
        entity_id: text("entityID")?,
        entity_type: text("entityType")?,
        payload: text("payload")?.into_bytes(),
        ttl,
    })
}

/// The primary key of a views-table item.
pub fn view_key(pk: &str, sk: &str) -> Item {
    Item::from([("pk".into(), s(pk)), ("sk".into(), s(sk))])
}

/// The `Query` a paged view list sends, independent of any SDK.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewQuery {
    /// The index to query, or `None` for the table.
    pub index: Option<&'static str>,
    /// The key condition expression.
    pub key_condition: &'static str,
    /// The expression attribute values.
    pub values: Item,
    /// The page size (`Limit`), when bounded.
    pub limit: Option<i32>,
    /// The decoded cursor (`ExclusiveStartKey`), when continuing.
    pub start_key: Option<Item>,
}

/// The query listing the rows of one partition, optionally bounded and continued from a cursor.
/// A `limit` of `None` or `Some(0)` (or above `i32::MAX`) leaves the page to DynamoDB's size cap.
pub fn view_query_by_pk(
    pk: &str,
    limit: Option<usize>,
    cursor: Option<&str>,
) -> Result<ViewQuery, Error> {
    view_query(
        None,
        "pk = :pk",
        Item::from([(":pk".into(), s(pk))]),
        limit,
        cursor,
        ("pk", pk),
    )
}

/// The query listing the rows of one entity type through [`ENTITY_TYPE_INDEX`].
pub fn view_query_by_entity_type(
    entity_type: &str,
    limit: Option<usize>,
    cursor: Option<&str>,
) -> Result<ViewQuery, Error> {
    view_query(
        Some(ENTITY_TYPE_INDEX),
        "entityType = :entityType",
        Item::from([(":entityType".into(), s(entity_type))]),
        limit,
        cursor,
        ("entityType", entity_type),
    )
}

fn view_query(
    index: Option<&'static str>,
    key_condition: &'static str,
    values: Item,
    limit: Option<usize>,
    cursor: Option<&str>,
    (key_name, key_value): (&str, &str),
) -> Result<ViewQuery, Error> {
    let start_key = cursor
        .filter(|cursor| !cursor.is_empty())
        .map(decode_view_cursor)
        .transpose()?;

    // A cursor continues the listing that produced it; DynamoDB would refuse a foreign one with a
    // validation error, so refuse it the same way on every backend.
    if let Some(key) = &start_key
        && key.get(key_name) != Some(&AttributeValue::S(key_value.to_owned()))
    {
        return Err(Error::InvalidArgument(
            "invalid pagination cursor: it belongs to another listing".into(),
        ));
    }

    Ok(ViewQuery {
        index,
        key_condition,
        values,
        limit: limit
            .filter(|limit| *limit > 0)
            .and_then(|limit| i32::try_from(limit).ok()),
        start_key,
    })
}

/// Encodes a `LastEvaluatedKey` as a pagination cursor: the key's string attributes as JSON
/// (sorted by name, Go's escaping) in padded, URL-safe base64. Other attribute types are dropped.
pub fn encode_view_cursor(key: &Item) -> Result<String, Error> {
    let strings: BTreeMap<&str, &str> = key
        .iter()
        .filter_map(|(name, value)| match value {
            AttributeValue::S(text) => Some((name.as_str(), text.as_str())),
            _ => None,
        })
        .collect();

    let encoded = json::base64_bytes::encode(&json::to_vec(&strings)?);

    Ok(encoded.replace('+', "-").replace('/', "_"))
}

/// Decodes a pagination cursor into an `ExclusiveStartKey`. Unlike Go, which forwards whatever
/// the JSON holds and lets DynamoDB refuse it, a key with no `pk` and `sk` string is refused here.
pub fn decode_view_cursor(cursor: &str) -> Result<Item, Error> {
    let invalid =
        |reason: String| Error::InvalidArgument(format!("invalid pagination cursor: {reason}"));

    if cursor.contains(['+', '/']) {
        return Err(invalid("illegal base64url data".into()));
    }

    let bytes =
        json::base64_bytes::decode(&cursor.replace('-', "+").replace('_', "/")).map_err(invalid)?;
    let strings: BTreeMap<String, String> =
        serde_json::from_slice(&bytes).map_err(|err| invalid(err.to_string()))?;

    if !strings.contains_key("pk") || !strings.contains_key("sk") {
        return Err(invalid("the key has no pk and sk".into()));
    }

    Ok(strings
        .into_iter()
        .map(|(name, value)| (name, AttributeValue::S(value)))
        .collect())
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

/// Encodes a stream's head item (`sk = -1`). `ttl` (epoch seconds) is written only when positive.
pub fn head_item(entity_id: &EntityId, head: EventSequence, ttl: Option<i64>) -> Item {
    let mut item = Item::new();
    item.insert("pk".into(), s(entity_id.as_str()));
    item.insert("sk".into(), n(HEAD_SORT_KEY));
    item.insert("head".into(), n(head.0));
    insert_ttl(&mut item, ttl);

    item
}

/// Decodes a head item into its `head` and optional `ttl`.
pub fn head_from_item(item: &Item) -> Result<(EventSequence, Option<i64>), Error> {
    Ok((
        EventSequence(required_number(item, "head")?),
        optional_number(item, "ttl")?,
    ))
}

/// The numeric sort key of an item, if it has one.
pub fn sort_key(item: &Item) -> Option<i64> {
    match item.get("sk") {
        Some(AttributeValue::N(sk)) => sk.parse().ok(),
        _ => None,
    }
}

/// The `ttl` attribute of an item, if it carries a numeric one.
pub fn item_ttl(item: &Item) -> Option<i64> {
    match item.get("ttl") {
        Some(AttributeValue::N(ttl)) => ttl.parse().ok(),
        _ => None,
    }
}

/// A DynamoDB `Update` or `ConditionCheck` on a stream's head item, independent of any SDK: the
/// single source of the head item's expression strings, which the writer builds its actions from
/// and the commit budget sizes (ADR 0007).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeadAction {
    /// The stream the head item belongs to.
    pub entity_id: EntityId,
    /// The update expression, or `None` for a `ConditionCheck`.
    pub update_expression: Option<&'static str>,
    /// The condition expression.
    pub condition_expression: &'static str,
    /// `ExpressionAttributeNames`: placeholder and attribute name.
    pub names: Vec<(&'static str, &'static str)>,
    /// `ExpressionAttributeValues`: placeholder and number.
    pub values: Vec<(&'static str, i64)>,
}

impl HeadAction {
    /// The head update of a written stream whose commit writes events `first` to `last`. A first
    /// sequence of 1 creates the head item. `ttl` is the largest `ttl` the head item must carry
    /// (the `:t` of the never-lower rule) and is `Some` only for a stream with retention.
    pub fn update(
        entity_id: &EntityId,
        first: EventSequence,
        last: EventSequence,
        ttl: Option<i64>,
    ) -> Self {
        let create = first <= EventSequence(1);
        let mut values = vec![(":last", last.0)];
        let mut names = Vec::new();

        if !create {
            values.push((":prev", first.0 - 1));
        }
        if let Some(ttl) = ttl {
            names.push(("#ttl", "ttl"));
            values.push((":t", ttl));
        }

        Self {
            entity_id: entity_id.clone(),
            update_expression: Some(if ttl.is_some() {
                HEAD_UPDATE_RETAINED
            } else {
                HEAD_UPDATE
            }),
            condition_expression: match (create, ttl.is_some()) {
                (true, _) => HEAD_CREATE_CONDITION,
                (false, false) => HEAD_APPEND_CONDITION,
                (false, true) => HEAD_APPEND_RETAINED_CONDITION,
            },
            names,
            values,
        }
    }

    /// The fence of a read-only stream the reader saw at `head`.
    pub fn fence(entity_id: &EntityId, head: EventSequence) -> Self {
        if head <= EventSequence::ZERO {
            return Self {
                entity_id: entity_id.clone(),
                update_expression: None,
                condition_expression: HEAD_FENCE_ABSENT_CONDITION,
                names: Vec::new(),
                values: Vec::new(),
            };
        }

        Self {
            entity_id: entity_id.clone(),
            update_expression: None,
            condition_expression: HEAD_FENCE_CONDITION,
            names: Vec::new(),
            values: vec![(":head", head.0)],
        }
    }

    /// The head item's part of an out-of-band snapshot write whose snapshot covers events up to
    /// `new`: a `ConditionCheck` on `head >= :new`, or, for a stream with retention (`ttl` is
    /// `Some`, the snapshot's own) and `new` above 0, an `Update` that also refreshes the head's
    /// `ttl`. When `new` is 0 the head item may be absent, and the check never becomes an update:
    /// an `Update` would create a head item with no `head` attribute.
    pub fn snapshot_guard(entity_id: &EntityId, new: EventSequence, ttl: Option<i64>) -> Self {
        match ttl {
            Some(ttl) if new > EventSequence::ZERO => Self {
                entity_id: entity_id.clone(),
                update_expression: Some(HEAD_REFRESH_UPDATE),
                condition_expression: HEAD_REFRESH_CONDITION,
                names: vec![("#ttl", "ttl")],
                values: vec![(":new", new.0), (":t", ttl)],
            },
            _ => Self::snapshot_cover(entity_id, new),
        }
    }

    /// The `ConditionCheck` form of [`snapshot_guard`](Self::snapshot_guard) with no `ttl`
    /// refresh, which an out-of-band snapshot write falls back to when only the refresh's `ttl`
    /// condition failed.
    pub fn snapshot_cover(entity_id: &EntityId, new: EventSequence) -> Self {
        Self {
            entity_id: entity_id.clone(),
            update_expression: None,
            condition_expression: if new > EventSequence::ZERO {
                HEAD_COVERS_CONDITION
            } else {
                HEAD_COVERS_OR_ABSENT_CONDITION
            },
            names: Vec::new(),
            values: vec![(":new", new.0)],
        }
    }

    /// The action's key.
    pub fn key(&self) -> Item {
        Item::from([
            ("pk".into(), s(self.entity_id.as_str())),
            ("sk".into(), n(HEAD_SORT_KEY)),
        ])
    }

    /// The action's size in the commit budget, by the rule for any update, delete, or condition
    /// check: its key attributes plus its expression strings, name placeholders (the placeholder
    /// and the name), and attribute values (the placeholder and the number's text).
    pub fn size(&self) -> usize {
        let expressions =
            self.update_expression.map_or(0, str::len) + self.condition_expression.len();
        let names: usize = self
            .names
            .iter()
            .map(|(placeholder, name)| placeholder.len() + name.len())
            .sum();
        let values: usize = self
            .values
            .iter()
            .map(|(placeholder, number)| placeholder.len() + number.to_string().len())
            .sum();

        crate::repository::item_size(&self.key()) + expressions + names + values
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

    fn outcome_row(item: &Item) -> Vec<(&str, &str)> {
        item.iter()
            .map(|(name, value)| match value {
                AttributeValue::S(text) | AttributeValue::N(text) => (name.as_str(), text.as_str()),
                other => panic!("unexpected attribute {other:?}"),
            })
            .collect()
    }

    #[test]
    fn outcome_items_match_the_spec_rows() {
        let accepted = Outcome {
            event_ids: vec!["acct-1:1".into()],
            ..Outcome::accepted(
                "cmd-0001",
                "08f63bf06ef7167f79dda7f5de9307e1ffb626de015ecbc1c1482d1e67e5abd6",
            )
            .with_response(br#"{"accountId":"acct-1","balanceCents":0}"#.to_vec())
        };
        let item = outcome_item(&accepted).unwrap();

        assert_eq!(
            outcome_row(&item),
            vec![
                (
                    "digest",
                    "08f63bf06ef7167f79dda7f5de9307e1ffb626de015ecbc1c1482d1e67e5abd6"
                ),
                ("events", r#"["acct-1:1"]"#),
                ("pk", "command-outcome#cmd-0001"),
                (
                    "response",
                    "eyJhY2NvdW50SWQiOiJhY2N0LTEiLCJiYWxhbmNlQ2VudHMiOjB9"
                ),
                ("sk", "1"),
                ("status", "accepted"),
            ]
        );
        assert_eq!(item.get("sk"), Some(&n(1)), "sk is a number");

        let noop = outcome_item(&Outcome::accepted("cmd-0003", "d")).unwrap();
        assert_eq!(
            noop.len(),
            4,
            "an accepted no-op has no events and no response"
        );

        let rejected = outcome_item(&Outcome::rejected(
            "cmd-0002",
            "d",
            Rejection::new("username-taken", "handle ada is taken")
                .with_details(serde_json::json!({ "handle": "ada" })),
        ))
        .unwrap();
        assert_eq!(
            rejected.get("rejection"),
            Some(&s(
                r#"{"code":"username-taken","message":"handle ada is taken","details":{"handle":"ada"}}"#
            ))
        );
        assert_eq!(rejected.get("code"), Some(&s("username-taken")));
        assert_eq!(rejected.get("events"), None);
    }

    #[test]
    fn outcome_items_round_trip() {
        let accepted = Outcome {
            event_ids: vec!["a:1".into(), "b:1".into()],
            ..Outcome::accepted("cmd-1", "d").with_response(vec![0, 1, 2, 255])
        };
        assert_eq!(
            outcome_from_item(&outcome_item(&accepted).unwrap()).unwrap(),
            accepted
        );

        let rejected = Outcome::rejected("cmd-2", "d", Rejection::stale_fence());
        assert_eq!(
            outcome_from_item(&outcome_item(&rejected).unwrap()).unwrap(),
            rejected
        );

        let noop = Outcome::accepted("cmd-3", "d");
        assert_eq!(
            outcome_from_item(&outcome_item(&noop).unwrap()).unwrap(),
            noop
        );
    }

    #[test]
    fn malformed_outcome_items_are_invalid_records() {
        let good = outcome_item(&Outcome::accepted("cmd-1", "d")).unwrap();

        for (name, mutate) in [
            ("status", Some(s("pending"))),
            ("status", None),
            ("digest", None),
            (
                "rejection",
                Some(s(r#"{"code":"c","message":"m","details":null}"#)),
            ),
            ("response", Some(s("not base64!"))),
            ("events", Some(s("{"))),
            ("pk", Some(s("acct-1"))),
        ] {
            let mut item = good.clone();
            match mutate {
                Some(value) => item.insert(name.into(), value),
                None => item.remove(name),
            };

            assert!(
                matches!(outcome_from_item(&item), Err(Error::InvalidRecord(_))),
                "{name}"
            );
        }
    }

    #[test]
    fn outcome_item_sizes_sum_names_and_values() {
        let item = outcome_item(&Outcome::accepted("c", "dd")).unwrap();

        // pk(2) + "command-outcome#c"(17) + sk(2) + "1"(1) + status(6) + "accepted"(8)
        // + digest(6) + "dd"(2)
        assert_eq!(
            crate::repository::item_size(&item),
            2 + 17 + 2 + 1 + 6 + 8 + 6 + 2
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
