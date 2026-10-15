//! A DynamoDB event log, item-for-item compatible with Go's `dynamo.Repository`.
//!
//! Bring your own [`aws_sdk_dynamodb::Client`]. This crate disables the SDK's default HTTP and
//! runtime features, so enable them in your application (for example by depending on
//! `aws-config` with its defaults). The table needs a string partition key `pk` and a number sort
//! key `sk`. See [`crate::wire::dynamo`] for the item layout.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{self as sdk, ConditionCheck, Delete, Put, TransactWriteItem};

use crate::aggregate::{Aggregate, Projector};
use crate::error::{BoxError, Error};
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::features::{
    self, COMMAND_OUTCOME_PREFIX, CachedMarker, FeatureManifest, FeatureMarker, SYSTEM_PREFIX,
    ValidatedMarker,
};
use crate::ids::{EntityId, EntityType, EventSequence};
use crate::json;
use crate::repository::{Commit, Repository};
use crate::wire::dynamo::{self as wire, AttributeValue, Item};

/// DynamoDB's limit on actions per transaction.
pub const MAX_TRANSACTION_ITEMS: usize = 100;

/// How many times a commit re-validates the feature marker when its condition fails before the
/// commit gives up. Each revalidation re-reads the manifest, so only a table whose manifest keeps
/// changing under the writer can exhaust it.
const MAX_MARKER_REVALIDATIONS: usize = 8;

/// How many times a manifest declaration retries its conditional write when another declaration
/// changed the row first.
const MAX_DECLARATION_RETRIES: usize = 8;

const COMPACTION_BATCH_SIZE: usize = MAX_TRANSACTION_ITEMS - 1;

/// Bound allocation and SDK limits independently of the caller's `usize` count.
const LATEST_PAGE_SIZE: usize = 100;

/// A DynamoDB-backed [`Repository`]. Clones share the client.
#[derive(Debug)]
pub struct DynamoRepository {
    client: Client,
    table: String,
    consistent_read: bool,
    retention: BTreeMap<EntityType, Duration>,
    clock: fn() -> SystemTime,
    /// The feature-marker version this handle validated, if it has written yet. Each handle is
    /// its own writer: clones validate independently, like separate processes.
    validated_marker: Mutex<CachedMarker>,
}

impl Clone for DynamoRepository {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            table: self.table.clone(),
            consistent_read: self.consistent_read,
            retention: self.retention.clone(),
            clock: self.clock,
            validated_marker: Mutex::new(*self.validated_marker()),
        }
    }
}

impl DynamoRepository {
    /// Creates a repository over the given event-log table. Reads are strongly consistent.
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
            consistent_read: true,
            retention: BTreeMap::new(),
            clock: SystemTime::now,
            validated_marker: Mutex::new(None),
        }
    }

    /// Uses `clock` instead of the system time to compute `ttl` expiries. For tests.
    #[must_use]
    pub fn with_clock(mut self, clock: fn() -> SystemTime) -> Self {
        self.clock = clock;
        self
    }

    /// Uses eventually consistent reads (half the read cost) when `false`.
    #[must_use]
    pub fn with_consistent_read(mut self, consistent: bool) -> Self {
        self.consistent_read = consistent;
        self
    }

    /// Stamps a `ttl` attribute on events and snapshots of `entity_type`, expiring them after
    /// `duration`. Only for short-lived streams that are never rebuilt: DynamoDB TTL deletes items
    /// independently and can leave gaps. Prefer snapshots plus compaction for retained history.
    #[must_use]
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
        let retention = self.retention.get(entity_type)?;

        wire::expiry((self.clock)(), *retention)
    }

    fn validated_marker(&self) -> MutexGuard<'_, CachedMarker> {
        self.validated_marker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Returns the marker version this handle validated, reading and validating the manifest at
    /// first write. The read is strongly consistent regardless of `with_consistent_read`, so a
    /// declaration another writer just wrote cannot be missed. The returned non-optional snapshot
    /// is used for the entire transaction check, even if a concurrent declaration invalidates the
    /// cache afterwards. A stale snapshot is safe: the transaction condition forces revalidation.
    async fn ensure_manifest_validated(&self) -> Result<ValidatedMarker, Error> {
        if let Some(cached) = *self.validated_marker() {
            return Ok(cached);
        }

        let observed = self.read_manifest().await?;
        let validated = features::validate_marker(observed.as_ref())?;

        let mut cached = self.validated_marker();

        Ok(*cached.get_or_insert(validated))
    }

    /// Drops the cached validation, so the next write re-reads and re-validates the manifest.
    fn invalidate_manifest(&self) {
        *self.validated_marker() = None;
    }

    /// Reads the feature manifest row, if one exists. Always strongly consistent.
    pub async fn read_manifest(&self) -> Result<Option<FeatureManifest>, Error> {
        let output = self
            .client
            .get_item()
            .table_name(&self.table)
            .consistent_read(true)
            .key("pk", s(crate::features::FEATURES_ID))
            .key("sk", n(crate::features::FEATURES_SORT_KEY))
            .send()
            .await
            .map_err(sdk_error)?;

        match output.item {
            Some(item) if !item.is_empty() => {
                Ok(Some(wire::feature_marker_from_item(&from_sdk_item(&item))?))
            }
            _ => Ok(None),
        }
    }

    /// Applies the change to the stored feature manifest in one conditional write that bumps
    /// `version` by exactly one. Declaring does not require support — commits do — so a table
    /// can be prepared for, or tested against, a feature this build does not model. The change
    /// is applied to the freshly observed manifest on every retry, so a racing declaration is
    /// merged, not overwritten.
    pub async fn update_manifest(
        &self,
        change: impl Fn(&mut FeatureManifest),
    ) -> Result<(), Error> {
        for _ in 0..=MAX_DECLARATION_RETRIES {
            let observed = self.read_manifest().await?;
            let mut next = observed.clone().unwrap_or_default();
            change(&mut next);
            next.version = observed.as_ref().map_or(1, |manifest| manifest.version + 1);

            let item = wire::feature_marker_item(&next)?;
            let mut put = self
                .client
                .put_item()
                .table_name(&self.table)
                .set_item(Some(to_sdk_item(&item)));

            put = match &observed {
                None => put.condition_expression("attribute_not_exists(pk)"),
                Some(manifest) => put
                    .condition_expression("version = :observed")
                    .expression_attribute_values(":observed", n(manifest.version)),
            };

            match put.send().await {
                Ok(_) => {
                    self.invalidate_manifest();
                    return Ok(());
                }
                Err(SdkError::ServiceError(err))
                    if matches!(err.err(), PutItemError::ConditionalCheckFailedException(_)) => {}
                Err(err) => return Err(sdk_error(err)),
            }
        }

        Err(Error::Backend(
            "feature-marker declaration kept racing another declaration".into(),
        ))
    }

    /// Builds the feature-marker check every commit carries: the validated version must still be
    /// the stored one, or the row must still be absent.
    fn marker_check(&self, validated: ValidatedMarker) -> Result<TransactWriteItem, Error> {
        let mut check = ConditionCheck::builder()
            .table_name(&self.table)
            .key("pk", s(crate::features::FEATURES_ID))
            .key("sk", n(crate::features::FEATURES_SORT_KEY));

        check = match validated {
            ValidatedMarker::Version(version) => check
                .condition_expression(wire::MARKER_CONDITION)
                .expression_attribute_values(":v", n(version)),
            ValidatedMarker::Absent => check.condition_expression("attribute_not_exists(pk)"),
        };

        Ok(TransactWriteItem::builder()
            .condition_check(check.build().map_err(Error::backend)?)
            .build())
    }

    /// Builds the commit's transaction items in the pinned order: the feature-marker check, the
    /// projector operations, the event puts, then the snapshot put or floor check.
    fn transaction_items(
        &self,
        commit: &Commit<TransactWriteItem>,
        validated: ValidatedMarker,
    ) -> Result<Vec<TransactWriteItem>, Error> {
        let (Some(first), Some(last_sequence)) = (
            commit.events.first().map(|event| n(event.sequence.0)),
            commit.events.last().map(|event| event.sequence),
        ) else {
            return Ok(Vec::new());
        };

        let mut items = Vec::with_capacity(commit.events.len() + commit.operations.len() + 2);
        items.push(self.marker_check(validated)?);
        items.extend(commit.operations.iter().cloned());

        for event in &commit.events {
            let item = wire::event_item(event, self.ttl_for(&event.entity_type))?;
            items.push(put(&self.table, item, Some(wire::EVENT_CONDITION), &[])?);
        }

        if let Some(snapshot) = &commit.snapshot {
            let ttl = self.ttl_for(&snapshot.entity_type);
            let item = wire::snapshot_item(
                &SerializedSnapshot {
                    entity_type: snapshot.entity_type.clone(),
                    entity_id: commit.entity_id.clone(),
                    sequence: snapshot.sequence,
                    event_sequence: last_sequence,
                    payload: snapshot.payload.clone(),
                },
                ttl,
            )?;
            // Commit::validate guarantees the sequence is at least 1.
            let expected = snapshot.sequence.0 - 1;
            items.push(if expected == 0 {
                put(&self.table, item, Some(wire::FIRST_SNAPSHOT_CONDITION), &[])?
            } else {
                put(
                    &self.table,
                    item,
                    Some(wire::NEXT_SNAPSHOT_CONDITION),
                    &[(":seq", n(expected)), (":first", first)],
                )?
            });
        } else {
            items.push(floor_check(&self.table, &commit.entity_id, first)?);
        }

        Ok(items)
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
                .map_err(sdk_error)?;

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
            .map_err(sdk_error)?;

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
                .map_err(sdk_error)?;

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

    async fn read_snapshot(
        &self,
        entity_id: &EntityId,
        consistent: bool,
    ) -> Result<Option<SerializedSnapshot>, Error> {
        let output = self
            .client
            .get_item()
            .table_name(&self.table)
            .consistent_read(consistent)
            .key("pk", s(entity_id.as_str()))
            .key("sk", n(wire::SNAPSHOT_SORT_KEY))
            .send()
            .await
            .map_err(sdk_error)?;

        match output.item {
            Some(item) if !item.is_empty() => {
                Ok(Some(wire::snapshot_from_item(&from_sdk_item(&item))?))
            }
            _ => Ok(None),
        }
    }

    /// Lists the keys of events in `[1, through]`. Always strongly consistent: an eventually
    /// consistent query could miss recent events, and compaction would report success while
    /// leaving them behind.
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
                .consistent_read(true)
                .key_condition_expression("pk = :pk AND sk BETWEEN :lo AND :hi")
                .expression_attribute_values(":pk", s(entity_id.as_str()))
                .expression_attribute_values(":lo", n(1))
                .expression_attribute_values(":hi", n(through.0))
                .projection_expression("pk, sk")
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(sdk_error)?;

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

    /// Compaction never uses the writer cache: history dependence is durable shared state.
    /// Pin the exact observed version (or absence) into every delete transaction.
    async fn compaction_check(
        &self,
        snapshot: &SerializedSnapshot,
    ) -> Result<TransactWriteItem, Error> {
        let manifest = self.read_manifest().await?;
        let observed = features::validate_marker(manifest.as_ref())?;
        if manifest
            .as_ref()
            .is_some_and(|manifest| manifest.is_history_dependent(&snapshot.entity_type))
        {
            return Err(Error::HistoryDependentProjection {
                entity_type: snapshot.entity_type.clone(),
                entity_id: snapshot.entity_id.clone(),
            });
        }

        let check = ConditionCheck::builder()
            .table_name(&self.table)
            .key("pk", s(features::FEATURES_ID))
            .key("sk", n(features::FEATURES_SORT_KEY));
        // Unlike commit's compatibility check, compaction pins presence as well as version.
        let check = match observed {
            ValidatedMarker::Version(version) => check
                .condition_expression("version = :observed")
                .expression_attribute_values(":observed", n(version)),
            ValidatedMarker::Absent => check.condition_expression("attribute_not_exists(pk)"),
        };

        Ok(TransactWriteItem::builder()
            .condition_check(check.build().map_err(Error::backend)?)
            .build())
    }

    async fn delete_compaction_keys(
        &self,
        keys: Vec<HashMap<String, sdk::AttributeValue>>,
        snapshot: &SerializedSnapshot,
        mut check: TransactWriteItem,
    ) -> Result<usize, Error> {
        let mut deleted = 0;

        for chunk in keys.chunks(COMPACTION_BATCH_SIZE) {
            let deletes = chunk
                .iter()
                .map(|key| {
                    Ok(TransactWriteItem::builder()
                        .delete(
                            Delete::builder()
                                .table_name(&self.table)
                                .set_key(Some(key.clone()))
                                .build()
                                .map_err(Error::backend)?,
                        )
                        .build())
                })
                .collect::<Result<Vec<_>, Error>>()?;

            for attempt in 0.. {
                let mut items = vec![check.clone()];
                items.extend(deletes.iter().cloned());
                match self
                    .client
                    .transact_write_items()
                    .set_transact_items(Some(items))
                    .send()
                    .await
                {
                    Ok(_) => break,
                    Err(err) => {
                        if cancellation_reasons(&err).first().copied().flatten()
                            != Some("ConditionalCheckFailed")
                        {
                            return Err(sdk_error(err));
                        }

                        // Re-read before retrying the unchanged batch, refusing a declaration
                        // that completed after validation but before the delete.
                        check = self.compaction_check(snapshot).await?;
                        if attempt >= MAX_MARKER_REVALIDATIONS {
                            return Err(Error::Transient("compaction marker kept changing".into()));
                        }
                    }
                }
            }

            deleted += chunk.len();
        }

        Ok(deleted)
    }
}

impl Repository for DynamoRepository {
    type Operation = TransactWriteItem;

    async fn commit(&self, commit: Commit<TransactWriteItem>) -> Result<(), Error> {
        commit.validate()?;
        features::check_not_reserved(&commit.entity_id)?;

        if commit.events.is_empty() {
            return Ok(());
        }

        let operations = commit.operations.len();
        let mut revalidations = 0;

        loop {
            let cached = self.ensure_manifest_validated().await?;
            let items = self.transaction_items(&commit, cached)?;

            if items.len() > MAX_TRANSACTION_ITEMS {
                return Err(Error::TooManyOperations {
                    count: items.len(),
                    limit: MAX_TRANSACTION_ITEMS,
                });
            }

            match self
                .client
                .transact_write_items()
                .set_transact_items(Some(items))
                .send()
                .await
            {
                Ok(_) => return Ok(()),
                Err(err) => {
                    let codes = cancellation_reasons(&err);
                    if classify_cancellation(operations, &codes) == Some(Cancellation::Marker) {
                        // The feature set changed under this writer: re-validate it, then
                        // retry. Re-validating refuses a feature this writer does not support.
                        if revalidations >= MAX_MARKER_REVALIDATIONS {
                            return Err(Error::Backend(Box::new(err)));
                        }
                        revalidations += 1;
                        self.invalidate_manifest();

                        continue;
                    }

                    return Err(classify_transaction_error(
                        &commit.entity_id,
                        operations,
                        err,
                    ));
                }
            }
        }
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

    async fn latest(
        &self,
        entity_id: &EntityId,
        count: usize,
    ) -> Result<Vec<SerializedEvent>, Error> {
        if count == 0 {
            return Err(Error::InvalidArgument(
                "latest(n) requires at least one event: got n = 0".into(),
            ));
        }

        // Newest first, with bounded allocation and query limits even for usize::MAX. Large
        // items can close a page early, so keep paging until `count` events or the stream ends.
        let mut events = Vec::with_capacity(count.min(LATEST_PAGE_SIZE));
        let mut start = None;

        loop {
            let page = self
                .client
                .query()
                .table_name(&self.table)
                .consistent_read(self.consistent_read)
                .key_condition_expression("pk = :pk AND sk > :sk")
                .expression_attribute_values(":pk", s(entity_id.as_str()))
                .expression_attribute_values(":sk", n(0))
                .scan_index_forward(false)
                .limit((count - events.len()).min(LATEST_PAGE_SIZE) as i32)
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(sdk_error)?;

            for item in page.items() {
                let item = from_sdk_item(item);
                if !wire::is_snapshot_item(&item) {
                    events.push(wire::event_from_item(&item)?);
                }
            }

            if events.len() >= count {
                events.truncate(count);
                return Ok(events);
            }

            match page.last_evaluated_key {
                Some(key) if !key.is_empty() => start = Some(key),
                _ => return Ok(events),
            }
        }
    }

    async fn snapshot(&self, entity_id: &EntityId) -> Result<Option<SerializedSnapshot>, Error> {
        self.read_snapshot(entity_id, self.consistent_read).await
    }

    async fn put_snapshot(&self, snapshot: &SerializedSnapshot) -> Result<bool, Error> {
        features::check_not_reserved(&snapshot.entity_id)?;

        let item = wire::snapshot_item(snapshot, self.ttl_for(&snapshot.entity_type))?;

        let snapshot_put = put(
            &self.table,
            item,
            Some(wire::PUT_SNAPSHOT_CONDITION),
            &[(":new", n(snapshot.event_sequence.0))],
        )?;

        let mut revalidations = 0;
        loop {
            let cached = self.ensure_manifest_validated().await?;
            let items = vec![self.marker_check(cached)?, snapshot_put.clone()];
            match self
                .client
                .transact_write_items()
                .set_transact_items(Some(items))
                .send()
                .await
            {
                Ok(_) => return Ok(true),
                Err(err) => {
                    let codes = cancellation_reasons(&err);
                    // A failed marker takes precedence over a stale snapshot condition.
                    if codes.first().copied().flatten() == Some("ConditionalCheckFailed") {
                        self.invalidate_manifest();
                        if revalidations >= MAX_MARKER_REVALIDATIONS {
                            return Err(sdk_error(err));
                        }
                        revalidations += 1;
                        continue;
                    }
                    if codes.get(1).copied().flatten() == Some("ConditionalCheckFailed") {
                        return Ok(false);
                    }

                    return Err(sdk_error(err));
                }
            }
        }
    }

    async fn compact_below(
        &self,
        entity_id: &EntityId,
        through: EventSequence,
    ) -> Result<usize, Error> {
        features::check_not_reserved(entity_id)?;

        if through < EventSequence(1) {
            return Ok(0);
        }

        // Strongly consistent regardless of `with_consistent_read`, like the key query below.
        let snapshot = self.read_snapshot(entity_id, true).await?;
        let covered = snapshot.as_ref().map(|snapshot| snapshot.event_sequence);
        if covered.is_none_or(|covered| covered < through) {
            return Err(Error::CompactionUncovered {
                entity_id: entity_id.clone(),
                through,
                covered,
            });
        }

        // Coverage above guarantees there is a durable snapshot.
        let snapshot = snapshot.expect("covered snapshot");
        let check = self.compaction_check(&snapshot).await?;

        let keys = self.event_keys_through(entity_id, through).await?;
        if keys.is_empty() {
            return Ok(0);
        }

        self.delete_compaction_keys(keys, &snapshot, check).await
    }

    async fn entity_heads(
        &self,
        entity_type: Option<&EntityType>,
    ) -> Result<BTreeMap<EntityId, EventSequence>, Error> {
        let mut heads = BTreeMap::new();
        let mut start = None;
        // Reserved partitions are engine-managed rows, never ordinary aggregates (v0.9, C8).
        let mut filter =
            "NOT begins_with(pk, :system) AND NOT begins_with(pk, :outcome)".to_owned();
        if entity_type.is_some() {
            filter.push_str(" AND entityType = :et");
        }

        loop {
            let mut scan = self
                .client
                .scan()
                .table_name(&self.table)
                .consistent_read(self.consistent_read)
                .projection_expression("pk, sk, eventSeq")
                .filter_expression(&filter)
                .expression_attribute_values(":system", s(SYSTEM_PREFIX))
                .expression_attribute_values(":outcome", s(COMMAND_OUTCOME_PREFIX))
                .set_exclusive_start_key(start);

            if let Some(entity_type) = entity_type {
                scan = scan.expression_attribute_values(":et", s(entity_type.as_str()));
            }

            let page = scan.send().await.map_err(sdk_error)?;

            for (pk, head) in page.items().iter().filter_map(item_head) {
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

impl FeatureMarker for DynamoRepository {
    async fn read_manifest(&self) -> Result<Option<FeatureManifest>, Error> {
        DynamoRepository::read_manifest(self).await
    }

    async fn update_manifest(
        &self,
        change: impl Fn(&mut FeatureManifest) + Send,
    ) -> Result<(), Error> {
        DynamoRepository::update_manifest(self, change).await
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

        Ok(vec![put(&self.table, item, None, &[])?])
    }
}

fn put(
    table: &str,
    item: Item,
    condition: Option<&str>,
    values: &[(&str, sdk::AttributeValue)],
) -> Result<TransactWriteItem, Error> {
    let mut put = Put::builder()
        .table_name(table)
        .set_item(Some(to_sdk_item(&item)))
        .set_condition_expression(condition.map(str::to_owned));

    for (name, value) in values {
        put = put.expression_attribute_values(*name, value.clone());
    }

    Ok(TransactWriteItem::builder()
        .put(put.build().map_err(Error::backend)?)
        .build())
}

/// Checks that a commit starting at `first` is above the entity's snapshot floor.
fn floor_check(
    table: &str,
    entity_id: &EntityId,
    first: sdk::AttributeValue,
) -> Result<TransactWriteItem, Error> {
    let check = ConditionCheck::builder()
        .table_name(table)
        .key("pk", s(entity_id.as_str()))
        .key("sk", n(wire::SNAPSHOT_SORT_KEY))
        .condition_expression(wire::FLOOR_CONDITION)
        .expression_attribute_values(":first", first)
        .build()
        .map_err(Error::backend)?;

    Ok(TransactWriteItem::builder().condition_check(check).build())
}

/// Reads an entity-heads scan item: an event's `sk`, or a snapshot's `eventSeq`. Like Go's
/// `headSeqFromItem`, it skips an item whose key or sequence is missing or not a number, so one bad
/// row does not stop a rebuild.
fn item_head(item: &HashMap<String, sdk::AttributeValue>) -> Option<(&str, i64)> {
    let number = |name| match item.get(name) {
        Some(sdk::AttributeValue::N(value)) => value.parse::<i64>().ok(),
        _ => None,
    };
    let Some(sdk::AttributeValue::S(pk)) = item.get("pk") else {
        return None;
    };

    let head = match number("sk")? {
        wire::SNAPSHOT_SORT_KEY => number("eventSeq")?,
        sk => sk,
    };

    Some((pk, head))
}

/// How a cancelled commit transaction failed.
#[derive(Debug, PartialEq, Eq)]
enum Cancellation {
    /// The feature-marker check failed: the manifest changed under the writer, which re-validates
    /// it and retries the commit (or refuses, if it does not support the declared features).
    Marker,
    /// A stream event or snapshot condition failed: another writer moved the stream.
    StreamCondition,
    /// Only projector conditions failed. Reloading cannot fix it.
    ProjectorCondition,
    /// Another transaction was writing the same items at the same time.
    TransactionConflict,
    /// DynamoDB throttled the transaction.
    Throttled,
}

/// Classifies cancellation reason codes, which are in transaction order. Item 0 is always the
/// feature-marker check; the next `operations` are the projector operations, and the rest are the
/// stream's events and snapshot.
fn classify_cancellation(operations: usize, codes: &[Option<&str>]) -> Option<Cancellation> {
    let failed = |code| failed_items(codes, code);

    if failed("ConditionalCheckFailed").next() == Some(0) {
        return Some(Cancellation::Marker);
    }
    if failed("ConditionalCheckFailed").any(|index| index > operations) {
        return Some(Cancellation::StreamCondition);
    }
    if failed("ConditionalCheckFailed").next().is_some() {
        return Some(Cancellation::ProjectorCondition);
    }
    if failed("TransactionConflict").next().is_some() {
        return Some(Cancellation::TransactionConflict);
    }
    if failed("ThrottlingError").next().is_some()
        || failed("ProvisionedThroughputExceeded").next().is_some()
    {
        return Some(Cancellation::Throttled);
    }

    None
}

/// The cancellation reason codes of a failed commit, in transaction order.
fn cancellation_reasons<R>(err: &SdkError<TransactWriteItemsError, R>) -> Vec<Option<&str>>
where
    R: std::fmt::Debug + Send + Sync + 'static,
{
    match err {
        SdkError::ServiceError(service) => match service.err() {
            TransactWriteItemsError::TransactionCanceledException(cancelled) => cancelled
                .cancellation_reasons()
                .iter()
                .map(|reason| reason.code())
                .collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// The indexes of the transaction items cancelled with the given reason code.
fn failed_items<'a>(codes: &'a [Option<&str>], code: &'a str) -> impl Iterator<Item = usize> + 'a {
    codes
        .iter()
        .enumerate()
        .filter(move |(_, reason)| **reason == Some(code))
        .map(|(index, _)| index)
}

/// Maps a failed commit to an error. A failed stream condition or a concurrent transaction is an
/// optimistic-concurrency conflict, and throttling is transient: `execute_with_retry` retries
/// both. A failed projector condition is a projector error, so the command is not rerun for it.
fn classify_transaction_error<R>(
    entity_id: &EntityId,
    operations: usize,
    err: SdkError<TransactWriteItemsError, R>,
) -> Error
where
    R: std::fmt::Debug + Send + Sync + 'static,
{
    let codes = cancellation_reasons(&err);

    let conflict = |message: &str| Error::Conflict {
        entity_id: entity_id.clone(),
        message: message.into(),
    };

    match classify_cancellation(operations, &codes) {
        // Handled by the commit loop's revalidation before this mapper runs.
        Some(Cancellation::Marker) => Error::backend(err),
        Some(Cancellation::StreamCondition) => {
            conflict("conditional check failed: the sequence or snapshot changed since it was read")
        }
        Some(Cancellation::TransactionConflict) => {
            conflict("transaction conflict: another writer was committing at the same time")
        }
        Some(Cancellation::ProjectorCondition) => Error::Projector(Box::new(err)),
        Some(Cancellation::Throttled) => Error::Transient(Box::new(err)),
        // A failed commit is only retryable when DynamoDB rejected it. After a timeout or a lost
        // response the transaction may have been applied, and running the command again would
        // write its events twice.
        None if throttled(&err) => Error::Transient(Box::new(err)),
        None => Error::backend(err),
    }
}

/// Maps an SDK error from a read or an idempotent write, marking failures that may succeed on
/// retry as [`Error::Transient`]: throttling, timeouts, dispatch failures, and internal server
/// errors. The SDK has already retried these with its own retry policy by the time they reach here.
/// Commits use [`classify_transaction_error`] instead, because a timed-out commit may have landed.
fn sdk_error<E, R>(err: SdkError<E, R>) -> Error
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug + Send + Sync + 'static,
{
    let outcome_unknown = match &err {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) => true,
        SdkError::ServiceError(service) => matches!(
            service.err().code(),
            Some("InternalServerError" | "TransactionInProgressException")
        ),
        _ => false,
    };

    if outcome_unknown || throttled(&err) {
        Error::Transient(Box::new(err))
    } else {
        Error::backend(err)
    }
}

/// Reports whether DynamoDB rejected the request for capacity, so it did not take effect.
fn throttled<E, R>(err: &SdkError<E, R>) -> bool
where
    E: ProvideErrorMetadata,
{
    match err {
        SdkError::ServiceError(service) => matches!(
            service.err().code(),
            Some(
                "ThrottlingException"
                    | "ProvisionedThroughputExceededException"
                    | "RequestLimitExceeded"
            )
        ),
        _ => false,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_cancellations_by_item() {
        const CHECK: Option<&str> = Some("ConditionalCheckFailed");
        const CONFLICT: Option<&str> = Some("TransactionConflict");
        const NONE: Option<&str> = Some("None");

        // Item 0 is the feature-marker check; item 1 is the one projector operation.
        let cases = [
            (vec![CHECK, NONE], Some(Cancellation::Marker)),
            (vec![NONE, CHECK], Some(Cancellation::ProjectorCondition)),
            (vec![NONE, NONE, CHECK], Some(Cancellation::StreamCondition)),
            (
                vec![NONE, CHECK, CONFLICT],
                Some(Cancellation::ProjectorCondition),
            ),
            (
                vec![NONE, NONE, CONFLICT],
                Some(Cancellation::TransactionConflict),
            ),
            (
                vec![CONFLICT, NONE, NONE],
                Some(Cancellation::TransactionConflict),
            ),
            (
                vec![NONE, NONE, Some("ThrottlingError")],
                Some(Cancellation::Throttled),
            ),
            (vec![NONE, NONE, Some("ValidationError")], None),
            (vec![], None),
        ];

        for (codes, want) in cases {
            assert_eq!(classify_cancellation(1, &codes), want, "{codes:?}");
        }
    }

    #[test]
    fn ttl_applies_only_to_types_with_retention() {
        fn clock() -> SystemTime {
            SystemTime::UNIX_EPOCH + Duration::from_millis(1_000_900)
        }

        let repository = offline_repository()
            .with_clock(clock)
            .with_retention("scratch", Duration::from_millis(1_500))
            .with_retention("disabled", Duration::ZERO);

        // 1000.9 s + 1.5 s = 1002.4 s, rounded down after adding.
        assert_eq!(repository.ttl_for(&"scratch".into()), Some(1_002));
        assert_eq!(repository.ttl_for(&"disabled".into()), None);
        assert_eq!(repository.ttl_for(&"account".into()), None);
        assert_eq!(wire::expiry(clock(), Duration::MAX), None, "overflow");
    }

    /// Builds a repository whose client never sends a request: no async runtime is configured, so
    /// every client feature that sleeps is turned off. Only offline builders are exercised.
    fn offline_repository() -> DynamoRepository {
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .retry_config(aws_sdk_dynamodb::config::retry::RetryConfig::disabled())
            .timeout_config(aws_sdk_dynamodb::config::timeout::TimeoutConfig::disabled())
            .stalled_stream_protection(
                aws_sdk_dynamodb::config::StalledStreamProtectionConfig::disabled(),
            )
            .identity_cache(aws_sdk_dynamodb::config::IdentityCache::no_cache())
            .build();

        DynamoRepository::new(Client::from_conf(config), "events")
    }

    #[test]
    fn the_marker_check_conditions_on_the_validated_version() {
        let repository = offline_repository();

        let check = repository
            .marker_check(ValidatedMarker::Version(7))
            .unwrap()
            .condition_check()
            .unwrap()
            .clone();
        assert_eq!(check.condition_expression(), wire::MARKER_CONDITION);
        assert_eq!(
            check.key().get("pk").and_then(|value| value.as_s().ok()),
            Some(&crate::features::FEATURES_ID.to_owned())
        );
        assert_eq!(
            check
                .expression_attribute_values()
                .and_then(|values| values.get(":v"))
                .and_then(|value| value.as_n().ok()),
            Some(&"7".to_owned())
        );

        let absent = repository
            .marker_check(ValidatedMarker::Absent)
            .unwrap()
            .condition_check()
            .unwrap()
            .clone();
        assert_eq!(absent.condition_expression(), "attribute_not_exists(pk)");
        assert!(absent.expression_attribute_values().is_none());
    }

    #[tokio::test]
    async fn validated_snapshot_survives_concurrent_cache_invalidation() {
        let repository = std::sync::Arc::new(offline_repository());
        *repository.validated_marker() = Some(ValidatedMarker::Version(7));
        let validated = repository.ensure_manifest_validated().await.unwrap();

        std::thread::spawn({
            let repository = std::sync::Arc::clone(&repository);
            move || repository.invalidate_manifest()
        })
        .join()
        .unwrap();
        assert!(repository.validated_marker().is_none());

        let item = repository.marker_check(validated).unwrap();
        let check = item.condition_check().unwrap();
        assert_eq!(check.condition_expression(), wire::MARKER_CONDITION);
        assert_eq!(check.expression_attribute_values().unwrap()[":v"], n(7));
    }

    #[test]
    fn transaction_items_match_shared_feature_marker_vectors() {
        use serde_json::{Value, json};

        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../compat/fixtures/feature-marker.json");
        let fixture: Value = serde_json::from_slice(
            &std::fs::read(&path)
                .unwrap_or_else(|err| panic!("read shared fixture {}: {err}", path.display())),
        )
        .unwrap();
        for case in fixture["commits"].as_array().unwrap() {
            let input = &case["input"];
            let event = &input["event"];
            let text = |key: &str| event[key].as_str().unwrap();
            let event = SerializedEvent {
                id: text("id").into(),
                sequence: EventSequence(event["sequence"].as_i64().unwrap()),
                event_type: text("type").into(),
                version: crate::EventVersion(event["version"].as_i64().unwrap()),
                entity_id: text("entityId").into(),
                entity_type: text("entityType").into(),
                payload: text("payload").as_bytes().to_vec(),
                metadata: serde_json::from_str(text("metadataJson")).unwrap(),
            };
            let view = wire::view_item(
                &event.entity_type,
                &event.entity_id,
                input["viewState"].as_str().unwrap().as_bytes(),
            )
            .unwrap();
            let mut commit =
                Commit::new(event.entity_id.clone(), vec![event]).with_operations([put(
                    input["viewsTable"].as_str().unwrap(),
                    view,
                    None,
                    &[],
                )
                .unwrap()]);
            if !input["snapshot"].is_null() {
                let snapshot = &input["snapshot"];
                commit = commit.with_snapshot(crate::NewSnapshot::new(
                    snapshot["entityType"].as_str().unwrap(),
                    EventSequence(snapshot["sequence"].as_i64().unwrap()),
                    snapshot["payload"].as_str().unwrap().as_bytes(),
                ));
            }
            let declaration = input["markerDeclaration"].as_str().unwrap();
            let observed = if declaration.is_empty() {
                ValidatedMarker::Absent
            } else {
                let declared = fixture["declarations"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|case| case["name"] == declaration)
                    .unwrap();
                let version = declared["output"]["dynamoItem"]["version"]["N"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                ValidatedMarker::Version(version)
            };
            let mut repository = offline_repository();
            repository.table = input["eventsTable"].as_str().unwrap().into();
            let actual = repository
                .transaction_items(&commit, observed)
                .unwrap()
                .iter()
                .map(|action| {
                    let (mut value, condition, values, names) = if let Some(put) = action.put() {
                        assert!(put.return_values_on_condition_check_failure().is_none());
                        (
                            json!({"operation": "Put", "tableName": put.table_name(),
                            "item": from_sdk_item(put.item())}),
                            put.condition_expression(),
                            put.expression_attribute_values(),
                            put.expression_attribute_names(),
                        )
                    } else {
                        let check = action.condition_check().expect("only puts and checks");
                        assert!(check.return_values_on_condition_check_failure().is_none());
                        (
                            json!({"operation": "ConditionCheck", "tableName": check.table_name(),
                            "key": from_sdk_item(check.key())}),
                            Some(check.condition_expression()),
                            check.expression_attribute_values(),
                            check.expression_attribute_names(),
                        )
                    };
                    if let Some(condition) = condition {
                        value["conditionExpression"] = json!(condition);
                    }
                    if let Some(values) = values {
                        value["expressionAttributeValues"] = json!(from_sdk_item(values));
                    }
                    if let Some(names) = names {
                        value["expressionAttributeNames"] = json!(names);
                    }
                    assert!(action.delete().is_none() && action.update().is_none());
                    value
                })
                .collect::<Vec<_>>();
            assert_eq!(json!(actual), case["output"], "{}", case["name"]);
        }
    }

    #[test]
    fn entity_heads_skip_malformed_items() {
        let item = |sk: sdk::AttributeValue, event_seq: Option<sdk::AttributeValue>| {
            let mut item = HashMap::from([("pk".to_owned(), s("acct-1")), ("sk".to_owned(), sk)]);
            item.extend(event_seq.map(|value| ("eventSeq".to_owned(), value)));
            item
        };

        assert_eq!(item_head(&item(n(7), None)), Some(("acct-1", 7)));
        assert_eq!(item_head(&item(n(0), Some(n(5)))), Some(("acct-1", 5)));
        assert_eq!(item_head(&item(n(0), Some(s("five")))), None);
        assert_eq!(item_head(&item(n(0), None)), None);
        assert_eq!(item_head(&item(s("seven"), None)), None);
    }

    #[test]
    fn a_timed_out_commit_is_not_retryable() {
        let err = SdkError::<TransactWriteItemsError, ()>::timeout_error("response lost");

        let classified = classify_transaction_error(&EntityId::new("acct-1"), 0, err);

        assert!(
            matches!(classified, Error::Backend(_)),
            "the commit may have landed, so retrying could apply it twice: {classified:?}"
        );
    }
}
