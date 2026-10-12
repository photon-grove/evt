//! A DynamoDB event log, item-for-item compatible with Go's `dynamo.Repository`.
//!
//! Bring your own [`aws_sdk_dynamodb::Client`]; this crate disables the SDK's default HTTP and
//! runtime features, so enable them in your application (for example by depending on
//! `aws-config` with its defaults). The table needs a string partition key `pk` and a number sort
//! key `sk`. See [`crate::wire::dynamo`] for the item layout.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{self as sdk, DeleteRequest, Put, TransactWriteItem, WriteRequest};

use crate::aggregate::{Aggregate, Projector};
use crate::error::{BoxError, Error};
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::ids::{EntityId, EntityType, EventSequence};
use crate::json;
use crate::repository::{Commit, Repository};
use crate::wire::dynamo::{self as wire, AttributeValue, Item};

/// DynamoDB's limit on actions per transaction.
pub const MAX_TRANSACTION_ITEMS: usize = 100;

const BATCH_DELETE_SIZE: usize = 25;
const MAX_UNPROCESSED_RETRIES: usize = 8;

/// A DynamoDB-backed [`Repository`]. Clones share the client.
#[derive(Clone, Debug)]
pub struct DynamoRepository {
    client: Client,
    table: String,
    consistent_read: bool,
    retention: BTreeMap<EntityType, Duration>,
}

impl DynamoRepository {
    /// Creates a repository over the given event-log table. Reads are strongly consistent.
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
            consistent_read: true,
            retention: BTreeMap::new(),
        }
    }

    /// Uses eventually consistent reads (half the read cost) when `false`.
    pub fn with_consistent_read(mut self, consistent: bool) -> Self {
        self.consistent_read = consistent;
        self
    }

    /// Stamps a `ttl` attribute on events and snapshots of `entity_type`, expiring them after
    /// `duration`. Only for short-lived streams that are never rebuilt: DynamoDB TTL deletes items
    /// independently and can leave gaps. Prefer snapshots plus compaction for retained history.
    pub fn with_retention(
        mut self,
        entity_type: impl Into<EntityType>,
        duration: Duration,
    ) -> Self {
        self.retention.insert(entity_type.into(), duration);
        self
    }

    /// The DynamoDB client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// The event-log table name.
    pub fn table(&self) -> &str {
        &self.table
    }

    fn ttl_for(&self, entity_type: &EntityType) -> Option<i64> {
        let duration = self
            .retention
            .get(entity_type)
            .filter(|duration| !duration.is_zero())?;
        let expires = SystemTime::now() + *duration;

        expires
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|elapsed| elapsed.as_secs() as i64)
    }

    /// Returns every item in an entity's partition, including the snapshot at `sk = 0`, in sort
    /// key order. Intended for diagnostics and compatibility checks.
    pub async fn partition_items(&self, entity_id: &EntityId) -> Result<Vec<Item>, Error> {
        let mut items = Vec::new();
        let mut start = None;

        loop {
            let page = self
                .client
                .query()
                .table_name(&self.table)
                .consistent_read(self.consistent_read)
                .key_condition_expression("pk = :pk")
                .expression_attribute_values(":pk", s(entity_id.as_str()))
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(Error::backend)?;

            items.extend(page.items().iter().map(from_sdk_item));

            match page.last_evaluated_key {
                Some(key) if !key.is_empty() => start = Some(key),
                _ => return Ok(items),
            }
        }
    }

    /// Returns the highest event sequence for an entity without reading payloads (0 if none).
    pub async fn max_sequence(&self, entity_id: &EntityId) -> Result<EventSequence, Error> {
        let page = self
            .client
            .query()
            .table_name(&self.table)
            .consistent_read(self.consistent_read)
            .key_condition_expression("pk = :pk AND sk > :sk")
            .expression_attribute_values(":pk", s(entity_id.as_str()))
            .expression_attribute_values(":sk", n(0))
            .projection_expression("sk")
            .scan_index_forward(false)
            .limit(1)
            .send()
            .await
            .map_err(Error::backend)?;

        let Some(item) = page.items().first() else {
            return Ok(EventSequence::ZERO);
        };

        Ok(EventSequence(
            number_attribute(item, "sk")?.unwrap_or_default(),
        ))
    }

    async fn query_events(
        &self,
        entity_id: &EntityId,
        after: EventSequence,
    ) -> Result<Vec<SerializedEvent>, Error> {
        let mut events = Vec::new();
        let mut start = None;

        loop {
            let page = self
                .client
                .query()
                .table_name(&self.table)
                .consistent_read(self.consistent_read)
                .key_condition_expression("pk = :pk AND sk > :sk")
                .expression_attribute_values(":pk", s(entity_id.as_str()))
                .expression_attribute_values(":sk", n(after.0.max(0)))
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(Error::backend)?;

            for item in page.items() {
                let item = from_sdk_item(item);
                if !wire::is_snapshot_item(&item) {
                    events.push(wire::event_from_item(&item)?);
                }
            }

            match page.last_evaluated_key {
                Some(key) if !key.is_empty() => start = Some(key),
                _ => return Ok(events),
            }
        }
    }

    async fn event_keys_through(
        &self,
        entity_id: &EntityId,
        through: EventSequence,
    ) -> Result<Vec<HashMap<String, sdk::AttributeValue>>, Error> {
        let mut keys = Vec::new();
        let mut start = None;

        loop {
            let page = self
                .client
                .query()
                .table_name(&self.table)
                .consistent_read(self.consistent_read)
                .key_condition_expression("pk = :pk AND sk BETWEEN :lo AND :hi")
                .expression_attribute_values(":pk", s(entity_id.as_str()))
                .expression_attribute_values(":lo", n(1))
                .expression_attribute_values(":hi", n(through.0))
                .projection_expression("pk, sk")
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(Error::backend)?;

            for item in page.items() {
                if let (Some(pk), Some(sk)) = (item.get("pk"), item.get("sk")) {
                    keys.push(HashMap::from([
                        ("pk".to_owned(), pk.clone()),
                        ("sk".to_owned(), sk.clone()),
                    ]));
                }
            }

            match page.last_evaluated_key {
                Some(key) if !key.is_empty() => start = Some(key),
                _ => return Ok(keys),
            }
        }
    }

    async fn delete_keys(
        &self,
        keys: Vec<HashMap<String, sdk::AttributeValue>>,
    ) -> Result<usize, Error> {
        let mut deleted = 0;

        for chunk in keys.chunks(BATCH_DELETE_SIZE) {
            let mut pending = chunk
                .iter()
                .map(|key| {
                    let request = DeleteRequest::builder()
                        .set_key(Some(key.clone()))
                        .build()
                        .map_err(Error::backend)?;
                    Ok(WriteRequest::builder().delete_request(request).build())
                })
                .collect::<Result<Vec<_>, Error>>()?;

            let total = pending.len();

            for attempt in 0.. {
                let output = self
                    .client
                    .batch_write_item()
                    .request_items(&self.table, pending.clone())
                    .send()
                    .await
                    .map_err(Error::backend)?;

                pending = output
                    .unprocessed_items
                    .and_then(|mut unprocessed| unprocessed.remove(&self.table))
                    .unwrap_or_default();

                if pending.is_empty() {
                    break;
                }
                if attempt >= MAX_UNPROCESSED_RETRIES {
                    return Err(Error::backend(format!(
                        "compaction: {} items still unprocessed after {MAX_UNPROCESSED_RETRIES} retries",
                        pending.len()
                    )));
                }
            }

            deleted += total;
        }

        Ok(deleted)
    }
}

impl Repository for DynamoRepository {
    type Operation = TransactWriteItem;

    async fn commit(&self, commit: Commit<TransactWriteItem>) -> Result<(), Error> {
        let Some(last_sequence) = commit.events.last().map(|event| event.sequence) else {
            return Ok(());
        };

        let mut items = commit.operations;

        for event in &commit.events {
            let item = wire::event_item(event, self.ttl_for(&event.entity_type))?;
            items.push(put(&self.table, item, Some(wire::EVENT_CONDITION), None)?);
        }

        if let Some(snapshot) = commit.snapshot {
            let ttl = self.ttl_for(&snapshot.entity_type);
            let expected = n(snapshot.sequence.0 - 1);
            let item = wire::snapshot_item(
                &SerializedSnapshot {
                    entity_type: snapshot.entity_type,
                    entity_id: commit.entity_id.clone(),
                    sequence: snapshot.sequence,
                    event_sequence: last_sequence,
                    payload: snapshot.payload,
                },
                ttl,
            )?;
            items.push(put(
                &self.table,
                item,
                Some(wire::SNAPSHOT_CONDITION),
                Some((":seq", expected)),
            )?);
        }

        if items.len() > MAX_TRANSACTION_ITEMS {
            return Err(Error::TooManyOperations {
                count: items.len(),
                limit: MAX_TRANSACTION_ITEMS,
            });
        }

        self.client
            .transact_write_items()
            .set_transact_items(Some(items))
            .send()
            .await
            .map(|_| ())
            .map_err(|err| classify_transaction_error(&commit.entity_id, err))
    }

    async fn events(&self, entity_id: &EntityId) -> Result<Vec<SerializedEvent>, Error> {
        self.query_events(entity_id, EventSequence::ZERO).await
    }

    async fn events_after(
        &self,
        entity_id: &EntityId,
        after: EventSequence,
    ) -> Result<Vec<SerializedEvent>, Error> {
        self.query_events(entity_id, after).await
    }

    async fn snapshot(&self, entity_id: &EntityId) -> Result<Option<SerializedSnapshot>, Error> {
        let output = self
            .client
            .get_item()
            .table_name(&self.table)
            .consistent_read(self.consistent_read)
            .key("pk", s(entity_id.as_str()))
            .key("sk", n(wire::SNAPSHOT_SORT_KEY))
            .send()
            .await
            .map_err(Error::backend)?;

        match output.item {
            Some(item) if !item.is_empty() => {
                Ok(Some(wire::snapshot_from_item(&from_sdk_item(&item))?))
            }
            _ => Ok(None),
        }
    }

    async fn put_snapshot(&self, snapshot: &SerializedSnapshot) -> Result<bool, Error> {
        let item = wire::snapshot_item(snapshot, self.ttl_for(&snapshot.entity_type))?;

        let result = self
            .client
            .put_item()
            .table_name(&self.table)
            .set_item(Some(to_sdk_item(&item)))
            .condition_expression(wire::PUT_SNAPSHOT_CONDITION)
            .expression_attribute_values(":new", n(snapshot.event_sequence.0))
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(err))
                if matches!(err.err(), PutItemError::ConditionalCheckFailedException(_)) =>
            {
                Ok(false)
            }
            Err(err) => Err(Error::backend(err)),
        }
    }

    async fn compact_below(
        &self,
        entity_id: &EntityId,
        through: EventSequence,
    ) -> Result<usize, Error> {
        if through < EventSequence(1) {
            return Ok(0);
        }

        let covered = self
            .snapshot(entity_id)
            .await?
            .map(|snapshot| snapshot.event_sequence);
        if covered.is_none_or(|covered| covered < through) {
            return Err(Error::CompactionUncovered {
                entity_id: entity_id.clone(),
                through,
                covered,
            });
        }

        let keys = self.event_keys_through(entity_id, through).await?;
        if keys.is_empty() {
            return Ok(0);
        }

        self.delete_keys(keys).await
    }

    async fn entity_heads(
        &self,
        entity_type: Option<&EntityType>,
    ) -> Result<BTreeMap<EntityId, EventSequence>, Error> {
        let mut heads = BTreeMap::new();
        let mut start = None;

        loop {
            let mut scan = self
                .client
                .scan()
                .table_name(&self.table)
                .consistent_read(self.consistent_read)
                .projection_expression("pk, sk, eventSeq")
                .set_exclusive_start_key(start);

            if let Some(entity_type) = entity_type {
                scan = scan
                    .filter_expression("entityType = :et")
                    .expression_attribute_values(":et", s(entity_type.as_str()));
            }

            let page = scan.send().await.map_err(Error::backend)?;

            for item in page.items() {
                let Some(sdk::AttributeValue::S(pk)) = item.get("pk") else {
                    continue;
                };
                let sk = number_attribute(item, "sk")?.unwrap_or_default();
                let head = if sk == wire::SNAPSHOT_SORT_KEY {
                    number_attribute(item, "eventSeq")?.unwrap_or_default()
                } else {
                    sk
                };

                let entry = heads
                    .entry(EntityId::new(pk))
                    .or_insert(EventSequence::ZERO);
                *entry = (*entry).max(EventSequence(head));
            }

            match page.last_evaluated_key {
                Some(key) if !key.is_empty() => start = Some(key),
                _ => break,
            }
        }

        heads.retain(|_, head| *head > EventSequence::ZERO);

        Ok(heads)
    }
}

/// Projects an aggregate to a single-item view row (`pk = {type}#{id}`, `sk = "VIEW"`) in the same
/// transaction as its events, like Go's `dynamo.NewViewProjector`.
#[derive(Clone, Debug)]
pub struct ViewProjector {
    table: String,
}

impl ViewProjector {
    /// Creates a projector writing to the given views table.
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
        }
    }
}

impl<A: Aggregate> Projector<A, TransactWriteItem> for ViewProjector {
    fn project(
        &self,
        entity_id: &EntityId,
        aggregate: &A,
        _: &[A::Event],
    ) -> Result<Vec<TransactWriteItem>, BoxError> {
        let payload = json::to_vec(aggregate)?;
        let item = wire::view_item(&EntityType::new(A::TYPE), entity_id, &payload)?;

        Ok(vec![put(&self.table, item, None, None)?])
    }
}

fn put(
    table: &str,
    item: Item,
    condition: Option<&str>,
    value: Option<(&str, sdk::AttributeValue)>,
) -> Result<TransactWriteItem, Error> {
    let mut put = Put::builder()
        .table_name(table)
        .set_item(Some(to_sdk_item(&item)))
        .set_condition_expression(condition.map(str::to_owned));

    if let Some((name, value)) = value {
        put = put.expression_attribute_values(name, value);
    }

    Ok(TransactWriteItem::builder()
        .put(put.build().map_err(Error::backend)?)
        .build())
}

fn classify_transaction_error<R>(
    entity_id: &EntityId,
    err: SdkError<TransactWriteItemsError, R>,
) -> Error
where
    R: std::fmt::Debug + Send + Sync + 'static,
{
    if let SdkError::ServiceError(service) = &err
        && let TransactWriteItemsError::TransactionCanceledException(cancelled) = service.err()
        && cancelled
            .cancellation_reasons()
            .iter()
            .any(|reason| reason.code() == Some("ConditionalCheckFailed"))
    {
        return Error::Conflict {
            entity_id: entity_id.clone(),
            message: "conditional check failed: the sequence or snapshot changed since it was read"
                .into(),
        };
    }

    Error::backend(err)
}

fn s(value: &str) -> sdk::AttributeValue {
    sdk::AttributeValue::S(value.to_owned())
}

fn n(value: i64) -> sdk::AttributeValue {
    sdk::AttributeValue::N(value.to_string())
}

fn number_attribute(
    item: &HashMap<String, sdk::AttributeValue>,
    name: &str,
) -> Result<Option<i64>, Error> {
    match item.get(name) {
        None => Ok(None),
        Some(sdk::AttributeValue::N(value)) => value.parse().map(Some).map_err(|_| {
            Error::InvalidRecord(format!("attribute {name} is not an integer: {value}"))
        }),
        Some(_) => Err(Error::InvalidRecord(format!(
            "attribute {name} is not a number"
        ))),
    }
}

/// Converts a wire item to SDK attribute values.
pub fn to_sdk_item(item: &Item) -> HashMap<String, sdk::AttributeValue> {
    item.iter()
        .map(|(name, value)| (name.clone(), to_sdk(value)))
        .collect()
}

/// Converts SDK attribute values to a wire item.
pub fn from_sdk_item(item: &HashMap<String, sdk::AttributeValue>) -> Item {
    item.iter()
        .map(|(name, value)| (name.clone(), from_sdk(value)))
        .collect()
}

fn to_sdk(value: &AttributeValue) -> sdk::AttributeValue {
    match value {
        AttributeValue::S(v) => sdk::AttributeValue::S(v.clone()),
        AttributeValue::N(v) => sdk::AttributeValue::N(v.clone()),
        AttributeValue::B(v) => sdk::AttributeValue::B(Blob::new(v.clone())),
        AttributeValue::Bool(v) => sdk::AttributeValue::Bool(*v),
        AttributeValue::Null(v) => sdk::AttributeValue::Null(*v),
        AttributeValue::M(v) => {
            sdk::AttributeValue::M(v.iter().map(|(k, v)| (k.clone(), to_sdk(v))).collect())
        }
        AttributeValue::L(v) => sdk::AttributeValue::L(v.iter().map(to_sdk).collect()),
        AttributeValue::Ss(v) => sdk::AttributeValue::Ss(v.clone()),
        AttributeValue::Ns(v) => sdk::AttributeValue::Ns(v.clone()),
        AttributeValue::Bs(v) => sdk::AttributeValue::Bs(
            v.iter()
                .map(|encoded| Blob::new(json::base64_bytes::decode(encoded).unwrap_or_default()))
                .collect(),
        ),
    }
}

fn from_sdk(value: &sdk::AttributeValue) -> AttributeValue {
    match value {
        sdk::AttributeValue::S(v) => AttributeValue::S(v.clone()),
        sdk::AttributeValue::N(v) => AttributeValue::N(v.clone()),
        sdk::AttributeValue::B(v) => AttributeValue::B(v.as_ref().to_vec()),
        sdk::AttributeValue::Bool(v) => AttributeValue::Bool(*v),
        sdk::AttributeValue::Null(v) => AttributeValue::Null(*v),
        sdk::AttributeValue::M(v) => {
            AttributeValue::M(v.iter().map(|(k, v)| (k.clone(), from_sdk(v))).collect())
        }
        sdk::AttributeValue::L(v) => AttributeValue::L(v.iter().map(from_sdk).collect()),
        sdk::AttributeValue::Ss(v) => AttributeValue::Ss(v.clone()),
        sdk::AttributeValue::Ns(v) => AttributeValue::Ns(v.clone()),
        sdk::AttributeValue::Bs(v) => AttributeValue::Bs(
            v.iter()
                .map(|blob| json::base64_bytes::encode(blob.as_ref()))
                .collect(),
        ),
        _ => AttributeValue::Null(true),
    }
}
