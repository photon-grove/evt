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
use crate::ids::{CommandId, EntityId, EntityType, EventSequence};
use crate::json;
use crate::outcome::Outcome;
use crate::repository::{Commit, Repository, TransactionCommit};
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
    scan_page_size: Option<i32>,
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
            scan_page_size: self.scan_page_size,
            retention: self.retention.clone(),
            clock: self.clock,
            validated_marker: Mutex::new(*self.validated_marker()),
        }
    }
}

impl DynamoRepository {
    /// Creates a repository over the given event-log table. Reads are strongly consistent.
    /// The table binding is immutable: event writes and feature-marker checks always use it.
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
            consistent_read: true,
            scan_page_size: None,
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

    /// Caps the items a Scan evaluates per page in [`EventLog::stream_all_events`], so a batch
    /// holds at most `limit` events. By default a page is DynamoDB's 1 MB. For bounding memory and
    /// for exercising pagination.
    ///
    /// [`EventLog::stream_all_events`]: crate::EventLog::stream_all_events
    #[must_use]
    pub fn with_scan_page_size(mut self, limit: i32) -> Self {
        self.scan_page_size = Some(limit.max(1));
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

    /// Reports whether this repository was configured to expire the entity type's rows.
    fn retains(&self, entity_type: &EntityType) -> bool {
        self.retention
            .get(entity_type)
            .is_some_and(|duration| !duration.is_zero())
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
    /// projector operations, then the stream's group (see [`stream_group`](Self::stream_group)).
    /// An outcome-only commit has no group; the outcome put is appended by the caller.
    fn transaction_items(
        &self,
        commit: &Commit<TransactWriteItem>,
        validated: ValidatedMarker,
        pin: Option<Continuation>,
    ) -> Result<Vec<TransactWriteItem>, Error> {
        let mut items = Vec::with_capacity(commit.events.len() + commit.operations.len() + 3);
        items.push(self.marker_check(validated)?);
        items.extend(commit.operations.iter().cloned());
        items.extend(self.stream_group(commit, pin)?);

        Ok(items)
    }

    /// Builds the command-outcome put: the last action of every commit that records one, written
    /// once in its final state and conditioned on the row not existing.
    fn outcome_put(&self, outcome: &Outcome) -> Result<TransactWriteItem, Error> {
        let item = wire::outcome_item(outcome)?;

        put(&self.table, item, Some(wire::OUTCOME_CONDITION), &[])
    }

    /// Builds one written stream's contiguous group: the continuation check on the event just
    /// before its first (when the first sequence is above 1 and the head event exists), the event
    /// puts, then the snapshot put or floor check.
    ///
    /// Compaction can delete every event a snapshot covers, including the head, so a stream whose
    /// snapshot ends exactly at the head is pinned through the snapshot instead: the group then
    /// carries no continuation check, and the snapshot put or floor check requires the snapshot's
    /// `eventSeq` to still equal the sequence before the commit's first.
    fn stream_group(
        &self,
        stream: &Commit<TransactWriteItem>,
        pin: Option<Continuation>,
    ) -> Result<Vec<TransactWriteItem>, Error> {
        let (Some(first), Some(last_sequence)) = (
            stream.events.first().map(|event| n(event.sequence.0)),
            stream.events.last().map(|event| event.sequence),
        ) else {
            return Ok(Vec::new());
        };

        let mut items = Vec::with_capacity(stream.events.len() + 2);

        if let Some(pin) = pin.filter(|pin| !pin.via_snapshot) {
            items.push(continuation_check(
                &self.table,
                &stream.entity_id,
                pin.previous,
            )?);
        }

        for event in &stream.events {
            let item = wire::event_item(event, self.ttl_for(&event.entity_type))?;
            items.push(put(&self.table, item, Some(wire::EVENT_CONDITION), &[])?);
        }

        let via_snapshot = pin.filter(|pin| pin.via_snapshot);

        if let Some(snapshot) = &stream.snapshot {
            let ttl = self.ttl_for(&snapshot.entity_type);
            let item = wire::snapshot_item(
                &SerializedSnapshot {
                    entity_type: snapshot.entity_type.clone(),
                    entity_id: stream.entity_id.clone(),
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
            } else if let Some(pin) = via_snapshot {
                put(
                    &self.table,
                    item,
                    Some(wire::PINNED_NEXT_SNAPSHOT_CONDITION),
                    &[(":seq", n(expected)), (":prev", n(pin.previous.0))],
                )?
            } else {
                put(
                    &self.table,
                    item,
                    Some(wire::NEXT_SNAPSHOT_CONDITION),
                    &[(":seq", n(expected)), (":first", first)],
                )?
            });
        } else if let Some(pin) = via_snapshot {
            items.push(pinned_floor_check(
                &self.table,
                &stream.entity_id,
                pin.previous,
            )?);
        } else {
            items.push(floor_check(&self.table, &stream.entity_id, first)?);
        }

        Ok(items)
    }

    /// Reads, strongly consistent, how each written stream whose first sequence is above 1
    /// continues from its stored head, and refuses a gap with [`Error::SequenceGap`] before any
    /// write. The returned pins are applied as conditions, so a head that moves between this read
    /// and the write fails the transaction as a conflict instead of committing a gap.
    ///
    /// Which item pins the head depends on the snapshot. Compaction deletes events `[1, through]`
    /// where `through` can equal the snapshot's `eventSeq`, so a head event the snapshot covers
    /// can vanish at any moment: a stream whose snapshot ends exactly at the head is pinned
    /// through the snapshot, which compaction never changes. Otherwise the head event lies above
    /// the snapshot, compaction cannot touch it, and an existence check pins it.
    async fn observe_continuations<'c>(
        &self,
        streams: impl IntoIterator<Item = &'c Commit<TransactWriteItem>>,
    ) -> Result<HashMap<EntityId, Continuation>, Error> {
        let mut pins = HashMap::new();

        for stream in streams {
            let Some(first) = stream.events.first().map(|event| event.sequence) else {
                continue;
            };
            if first <= EventSequence(1) {
                continue;
            }

            let previous = EventSequence(first.0 - 1);
            let snapshot = self.read_snapshot(&stream.entity_id, true).await?;
            let snapshot_head = snapshot.as_ref().map(|snapshot| snapshot.event_sequence);

            let via_snapshot = match snapshot_head {
                Some(head) if head == previous => true,
                Some(head) if head > previous => {
                    // The stream moved past the writer's head; the floor check would refuse the
                    // commit.
                    return Err(Error::Conflict {
                        entity_id: stream.entity_id.clone(),
                        message: "the stream's snapshot is ahead of the commit's first sequence"
                            .to_owned(),
                    });
                }
                _ => {
                    let head = self
                        .client
                        .get_item()
                        .table_name(&self.table)
                        .consistent_read(true)
                        .key("pk", s(stream.entity_id.as_str()))
                        .key("sk", n(previous.0))
                        .projection_expression("sk")
                        .send()
                        .await
                        .map_err(sdk_error)?;

                    if head.item.is_none_or(|item| item.is_empty()) {
                        let stored = self.max_sequence(&stream.entity_id).await?;

                        return Err(Error::SequenceGap {
                            entity_id: stream.entity_id.clone(),
                            after: stored.max(snapshot_head.unwrap_or_default()),
                            found: first,
                        });
                    }

                    false
                }
            };

            pins.insert(
                stream.entity_id.clone(),
                Continuation {
                    previous,
                    via_snapshot,
                },
            );
        }

        Ok(pins)
    }

    /// Builds a multi-stream commit's transaction items in the pinned order: the feature-marker
    /// check, the projector operations, then per written stream in bytewise stream-ID order its
    /// group (continuation check, event puts, snapshot put or floor check), then per fenced
    /// read-only stream its slot check and snapshot companion. The layout records each item's index so a failed
    /// commit is classified by the item that failed.
    fn transaction_stream_items(
        &self,
        commit: &TransactionCommit<TransactWriteItem>,
        validated: ValidatedMarker,
        pins: &HashMap<EntityId, Continuation>,
    ) -> Result<(Vec<TransactWriteItem>, TransactionLayout), Error> {
        let mut items = Vec::new();
        let mut layout = TransactionLayout::default();

        items.push(self.marker_check(validated)?);
        items.extend(commit.operations.iter().cloned());
        layout.operations = items.len() - 1;

        for stream in &commit.streams {
            if stream.events.is_empty() {
                continue;
            }

            let group_start = items.len();
            items.extend(self.stream_group(stream, pins.get(&stream.entity_id).copied())?);

            layout
                .streams
                .push((group_start..items.len(), stream.entity_id.clone()));
        }

        for fence in &commit.fences {
            items.extend(fence_checks(&self.table, fence)?);
            let base = items.len() - 2;
            layout.fences.push((base, fence.clone()));
            layout.fences.push((base + 1, fence.clone()));
        }

        Ok((items, layout))
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

        if commit.events.is_empty() && commit.outcome.is_none() {
            return Ok(());
        }

        let operations = commit.operations.len();
        let outcome = commit.stored_outcome();
        let mut revalidations = 0;

        // The shared budget covers every item the transaction carries, with the projector
        // operations sized exactly from the items this backend will write. It fails with
        // CommitTooLarge before any write, identically on every backend.
        let mut sizes = commit.sizes(|entity_type| self.ttl_for(entity_type))?;
        for operation in &commit.operations {
            sizes.push(sdk_item_size(operation));
        }
        crate::repository::check_budget(sizes)?;

        // The stream must continue exactly from its stored head; a gap is refused before any
        // write, and what was observed is pinned in the transaction.
        let pins = self.observe_continuations([&commit]).await?;
        let pin = pins.get(&commit.entity_id).copied();

        loop {
            let cached = self.ensure_manifest_validated().await?;
            let mut items = self.transaction_items(&commit, cached, pin)?;

            // The outcome row is the last action. Its index is how a failed condition on it is
            // told apart from a stream's.
            let outcome_index = match &outcome {
                Some(outcome) => {
                    items.push(self.outcome_put(outcome)?);

                    Some(items.len() - 1)
                }
                None => None,
            };

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

                    // A lost outcome put outranks every other failure: the command already ran, and
                    // the writer resolves it through the winner's row. It is never a conflict.
                    if let Some(outcome) = &outcome
                        && outcome_failed(outcome_index, &codes)
                    {
                        return Err(Error::OutcomeConflict {
                            command_id: outcome.command_id.clone(),
                        });
                    }

                    return Err(classify_transaction_error(
                        &commit.entity_id,
                        operations,
                        outcome.is_some(),
                        err,
                    ));
                }
            }
        }
    }

    async fn commit_transaction(
        &self,
        commit: TransactionCommit<TransactWriteItem>,
    ) -> Result<(), Error> {
        let mut commit = commit;
        commit.normalize()?;

        if commit.is_empty() {
            return Ok(());
        }

        let outcome = commit.stored_outcome();

        // Fences are refused for entity types this repository was configured to expire: it needs
        // no I/O, so it runs first. The durable manifest's retention set is checked in the loop,
        // and the refusal is the union of the two.
        commit.check_retention(None, |entity_type| self.retains(entity_type))?;

        // Each written stream must continue exactly from its stored head; a gap is refused before
        // any write, and what was observed is pinned in the transaction.
        let pins = self.observe_continuations(&commit.streams).await?;

        let mut revalidations = 0;

        loop {
            let cached = self.ensure_manifest_validated().await?;

            // Fences are refused for entity types durably declared with retention: TTL deletes
            // reopen fenced slots without leaving a successor snapshot. The refusal consults the
            // stored manifest, not this instance's configuration, and runs again on every
            // iteration: a declaration that races the marker revalidation bumps the version, the
            // marker check fails, and the retry must see the type it now declares.
            if !commit.fences.is_empty() {
                let manifest = self.read_manifest().await?;
                commit
                    .check_retention(manifest.as_ref(), |entity_type| self.retains(entity_type))?;
            }

            let (mut items, mut layout) = self.transaction_stream_items(&commit, cached, &pins)?;
            if let Some(outcome) = &outcome {
                items.push(self.outcome_put(outcome)?);
                layout.outcome = Some(items.len() - 1);
            }

            let mut sizes = commit.sizes(|entity_type| self.ttl_for(entity_type))?;
            for operation in &commit.operations {
                sizes.push(sdk_item_size(operation));
            }
            crate::repository::check_budget(sizes)?;

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

                    let failed_check = failed_items(&codes, "ConditionalCheckFailed").next();
                    let fence_conflict =
                        failed_check.and_then(|index| layout.fence_at(index).cloned());

                    if failed_check == Some(0) {
                        // The feature set changed under this writer: re-validate it, then
                        // retry. Re-validating refuses a feature this writer does not support.
                        if revalidations >= MAX_MARKER_REVALIDATIONS {
                            return Err(Error::Backend(Box::new(err)));
                        }
                        revalidations += 1;
                        self.invalidate_manifest();

                        continue;
                    }

                    // A lost outcome put outranks every other failure, a failed fence included:
                    // the command already ran, and the writer resolves it through the winner's
                    // row. It is never a conflict.
                    if let Some(outcome) = &outcome
                        && outcome_failed(layout.outcome, &codes)
                    {
                        return Err(Error::OutcomeConflict {
                            command_id: outcome.command_id.clone(),
                        });
                    }

                    // The stream whose action failed, when the cancellation index maps back to
                    // one; the first stream is only a fallback for an index that does not.
                    let fallback_entity = || {
                        commit.streams.first().map_or_else(
                            || EntityId::new("unknown"),
                            |stream| stream.entity_id.clone(),
                        )
                    };
                    let entity_at = |index: Option<usize>| {
                        index
                            .and_then(|index| layout.stream_at(index).cloned())
                            .unwrap_or_else(fallback_entity)
                    };

                    if let Some(fence) = fence_conflict {
                        return Err(Error::FenceConflict {
                            entity_id: fence.entity_id,
                            head: fence.head,
                            provenance: fence.provenance,
                        });
                    }

                    if let Some(index) = failed_check {
                        if index > 0 && index <= layout.operations {
                            // Projector conditions are not retried by reloading.
                            return Err(Error::Projector(Box::new(err)));
                        }

                        return Err(Error::Conflict {
                            entity_id: entity_at(Some(index)),
                            message:
                                "conditional check failed: the sequence or snapshot changed since it was read"
                                    .to_owned(),
                        });
                    }

                    // A concurrent transaction is reported on the item it collided with.
                    let colliding = failed_items(&codes, "TransactionConflict").next();

                    return Err(classify_transaction_error(
                        &entity_at(colliding),
                        layout.operations,
                        outcome.is_some(),
                        err,
                    ));
                }
            }
        }
    }

    /// Reads the outcome row with a strongly consistent `GetItem`, whatever
    /// [`with_consistent_read`](DynamoRepository::with_consistent_read) says: an eventually
    /// consistent miss would re-execute a command that already committed.
    async fn outcome(&self, command_id: &CommandId) -> Result<Option<Outcome>, Error> {
        crate::outcome::check_command_id(command_id)?;

        let output = self
            .client
            .get_item()
            .table_name(&self.table)
            .consistent_read(true)
            .key("pk", s(&wire::outcome_key(command_id)))
            .key("sk", n(wire::OUTCOME_SORT_KEY))
            .send()
            .await
            .map_err(sdk_error)?;

        match output.item {
            Some(item) if !item.is_empty() => {
                Ok(Some(wire::outcome_from_item(&from_sdk_item(&item))?))
            }
            _ => Ok(None),
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

                    // Per-item throttling and write conflicts arrive inside the cancellation
                    // reasons, not as the outer error code, so classify them here to keep
                    // them retryable.
                    return Err(match classify_cancellation(0, &codes) {
                        Some(Cancellation::Throttled) => Error::Transient(Box::new(err)),
                        Some(Cancellation::TransactionConflict) => Error::Conflict {
                            entity_id: snapshot.entity_id.clone(),
                            message: "transaction conflict: another writer was committing at the same time".into(),
                        },
                        _ => sdk_error(err),
                    });
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

/// Where each kind of item sits in a multi-stream transaction, for failure classification.
#[derive(Default)]
struct TransactionLayout {
    /// Number of projector operations, which start at index 1 after the marker check.
    operations: usize,
    /// Fences by their slot-check and companion-check item indexes.
    fences: Vec<(usize, crate::repository::ReadFence)>,
    /// Each written stream's contiguous run of action indexes (event puts and the snapshot put or
    /// floor check), so a failed action maps back to the stream that owns it.
    streams: Vec<(std::ops::Range<usize>, EntityId)>,
    /// The index of the command-outcome put, the last action, when the commit records one.
    outcome: Option<usize>,
}

impl TransactionLayout {
    fn fence_at(&self, index: usize) -> Option<&crate::repository::ReadFence> {
        self.fences
            .iter()
            .find(|(item_index, _)| *item_index == index)
            .map(|(_, fence)| fence)
    }

    /// The written stream whose action sits at `index`, if the index is one of a stream's.
    fn stream_at(&self, index: usize) -> Option<&EntityId> {
        self.streams
            .iter()
            .find(|(range, _)| range.contains(&index))
            .map(|(_, entity_id)| entity_id)
    }
}

/// Builds a fenced read-only stream's two ConditionChecks: the slot check on
/// `(pk, sk = head + 1)` and the snapshot companion on `(pk, sk = 0)`, which is always included.
fn fence_checks(
    table: &str,
    fence: &crate::repository::ReadFence,
) -> Result<[TransactWriteItem; 2], Error> {
    let slot = ConditionCheck::builder()
        .table_name(table)
        .key("pk", s(fence.entity_id.as_str()))
        .key("sk", n(fence.head.next().0))
        .condition_expression(wire::FENCE_SLOT_CONDITION)
        .build()
        .map_err(Error::backend)?;

    let companion = ConditionCheck::builder()
        .table_name(table)
        .key("pk", s(fence.entity_id.as_str()))
        .key("sk", n(wire::SNAPSHOT_SORT_KEY))
        .condition_expression(wire::FENCE_COMPANION_CONDITION)
        .expression_attribute_values(":head", n(fence.head.0))
        .build()
        .map_err(Error::backend)?;

    Ok([
        TransactWriteItem::builder().condition_check(slot).build(),
        TransactWriteItem::builder()
            .condition_check(companion)
            .build(),
    ])
}

/// A projector operation's DynamoDB-encoded request size. A put counts its item. An update,
/// delete, or condition check counts its key attributes plus its expression strings,
/// attribute-name placeholders (both sides), and attribute values: DynamoDB applies the 4 MB
/// aggregate request limit to every action type, so a large expression value must not bypass the
/// `CommitTooLarge` preflight. Names count their UTF-8 bytes and values their encoded bytes.
fn sdk_item_size(operation: &TransactWriteItem) -> usize {
    fn values_size(values: Option<&HashMap<String, sdk::AttributeValue>>) -> usize {
        values.map_or(0, |values| {
            crate::repository::item_size(&from_sdk_item(values))
        })
    }

    fn expression_size(
        expressions: &[Option<&str>],
        names: Option<&HashMap<String, String>>,
        values: Option<&HashMap<String, sdk::AttributeValue>>,
    ) -> usize {
        let text: usize = expressions.iter().flatten().map(|text| text.len()).sum();
        let placeholders: usize = names.map_or(0, |names| {
            names
                .iter()
                .map(|(placeholder, name)| placeholder.len() + name.len())
                .sum()
        });

        text + placeholders + values_size(values)
    }

    if let Some(put) = operation.put() {
        return crate::repository::item_size(&from_sdk_item(put.item()));
    }
    if let Some(update) = operation.update() {
        return values_size(Some(update.key()))
            + expression_size(
                &[
                    Some(update.update_expression()),
                    update.condition_expression(),
                ],
                update.expression_attribute_names(),
                update.expression_attribute_values(),
            );
    }
    if let Some(delete) = operation.delete() {
        return values_size(Some(delete.key()))
            + expression_size(
                &[delete.condition_expression()],
                delete.expression_attribute_names(),
                delete.expression_attribute_values(),
            );
    }
    if let Some(check) = operation.condition_check() {
        return values_size(Some(check.key()))
            + expression_size(
                &[Some(check.condition_expression())],
                check.expression_attribute_names(),
                check.expression_attribute_values(),
            );
    }

    0
}

/// How a written stream's first sequence was tied to its stored head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Continuation {
    /// The commit's first sequence minus one: the head the writer believes in.
    previous: EventSequence,
    /// The snapshot ends exactly at the head, so compaction may delete the head event at any time;
    /// the snapshot's `eventSeq` (equal to `previous`) pins the head instead of the event item.
    via_snapshot: bool,
}

/// Checks that the event just before a stream's first new event still exists.
fn continuation_check(
    table: &str,
    entity_id: &EntityId,
    previous: EventSequence,
) -> Result<TransactWriteItem, Error> {
    let check = ConditionCheck::builder()
        .table_name(table)
        .key("pk", s(entity_id.as_str()))
        .key("sk", n(previous.0))
        .condition_expression(wire::CONTINUATION_CONDITION)
        .build()
        .map_err(Error::backend)?;

    Ok(TransactWriteItem::builder().condition_check(check).build())
}

/// Checks that a compacted stream's snapshot still ends exactly at the sequence before the commit.
fn pinned_floor_check(
    table: &str,
    entity_id: &EntityId,
    previous: EventSequence,
) -> Result<TransactWriteItem, Error> {
    let check = ConditionCheck::builder()
        .table_name(table)
        .key("pk", s(entity_id.as_str()))
        .key("sk", n(wire::SNAPSHOT_SORT_KEY))
        .condition_expression(wire::PINNED_FLOOR_CONDITION)
        .expression_attribute_values(":prev", n(previous.0))
        .build()
        .map_err(Error::backend)?;

    Ok(TransactWriteItem::builder().condition_check(check).build())
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

/// Reports whether the command-outcome put (the action at `outcome_index`) failed its condition.
fn outcome_failed(outcome_index: Option<usize>, codes: &[Option<&str>]) -> bool {
    outcome_index.is_some_and(|index| {
        failed_items(codes, "ConditionalCheckFailed").any(|failed| failed == index)
    })
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
///
/// A commit that carries an outcome (`carries_outcome`) whose result is genuinely unknown (see
/// [`commit_result_unknown`]) is [`Error::Indeterminate`], which the store settles by re-reading the
/// outcome row; every other failure keeps its classification and is never recovered.
fn classify_transaction_error<R>(
    entity_id: &EntityId,
    operations: usize,
    carries_outcome: bool,
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
        None if carries_outcome && commit_result_unknown(&err) => {
            Error::Indeterminate(Box::new(err))
        }
        None => Error::backend(err),
    }
}

/// Reports whether a failed commit leaves it unknown whether the transaction applied: a timeout, a
/// connection or dispatch failure, an unparseable response, or a server fault. A definitive refusal
/// (validation, a missing table, authorization, a cancelled transaction with known reasons) is not.
fn commit_result_unknown<R>(err: &SdkError<TransactWriteItemsError, R>) -> bool
where
    R: std::fmt::Debug + Send + Sync + 'static,
{
    match err {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => {
            true
        }
        SdkError::ServiceError(service) => matches!(
            service.err().code(),
            Some(
                "InternalServerError"
                    | "ServiceUnavailable"
                    | "TransactionInProgressException"
                    | "IdempotentParameterMismatchException"
            )
        ),
        _ => false,
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

mod enumerate;

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
                .transaction_items(&commit, observed, None)
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

    /// Summarizes a transaction action the way the Go fixtures record it: operation, key, and
    /// the condition with its expression values.
    fn action_summary(action: &TransactWriteItem) -> serde_json::Value {
        use serde_json::json;

        let key_number = |item: &HashMap<String, sdk::AttributeValue>| {
            item["sk"].as_n().unwrap().parse::<i64>().unwrap()
        };

        let (operation, key, condition, values) = if let Some(put) = action.put() {
            (
                "Put",
                (
                    put.item()["pk"].as_s().unwrap().clone(),
                    key_number(put.item()),
                ),
                put.condition_expression(),
                put.expression_attribute_values(),
            )
        } else {
            let check = action.condition_check().expect("only puts and checks");

            (
                "ConditionCheck",
                (
                    check.key()["pk"].as_s().unwrap().clone(),
                    key_number(check.key()),
                ),
                Some(check.condition_expression()),
                check.expression_attribute_values(),
            )
        };

        let mut summary = json!({"operation": operation, "pk": key.0, "sk": key.1});
        if let Some(condition) = condition {
            summary["condition"] = json!(condition);
        }
        if let Some(values) = values.filter(|values| !values.is_empty()) {
            summary["values"] = json!(from_sdk_item(values));
        }

        summary
    }

    /// The shared scenarios' transaction steps build the same `TransactWriteItems` action order
    /// the Go generator recorded from its repository: a divergence fails here even when both
    /// languages store identical state.
    #[test]
    fn transaction_action_order_matches_the_go_fixtures() {
        use serde_json::Value;

        let compat = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compat");
        let read = |path: &str| -> Value {
            serde_json::from_slice(&std::fs::read(compat.join(path)).unwrap()).unwrap()
        };
        let scenarios = read("scenarios.json");
        let fixtures = read("fixtures/scenarios.json");
        let run = fixtures["run"].as_str().unwrap();

        let id_of = |base: &str, suffix: &str| {
            if suffix.is_empty() {
                EntityId::new(base)
            } else {
                EntityId::new(format!("{base}-{suffix}"))
            }
        };

        let mut transactions = 0;
        for scenario in scenarios["scenarios"].as_array().unwrap() {
            let name = scenario["name"].as_str().unwrap();
            let base = format!("compat-{run}-{name}");
            let expected = fixtures["scenarios"]
                .as_array()
                .unwrap()
                .iter()
                .find(|fixture| fixture["name"] == name)
                .unwrap_or_else(|| panic!("no fixture for {name}"));
            let recorded = expected["transactions"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            // Steps of outcomes scenarios record every request they sent (`calls`); those are
            // asserted against a live run by the evt-compat tests.
            let recorded: Vec<Value> = recorded
                .into_iter()
                .filter(|recorded| recorded.get("actions").is_some())
                .collect();

            let steps = scenario["steps"].as_array().unwrap();
            let planned: Vec<_> = steps
                .iter()
                .enumerate()
                .filter(|(_, step)| step["op"] == "transaction")
                .collect();
            assert_eq!(
                planned.len(),
                recorded.len(),
                "{name}: one recorded order per transaction step"
            );

            for ((index, step), recorded) in planned.into_iter().zip(&recorded) {
                assert_eq!(recorded["step"], index, "{name}: recorded step");

                let streams = step["streams"]
                    .as_array()
                    .map(|streams| {
                        streams
                            .iter()
                            .map(|stream| {
                                let id = id_of(&base, stream["suffix"].as_str().unwrap_or(""));
                                let first = stream["sequence"].as_i64().unwrap();
                                let events = stream["events"]
                                    .as_array()
                                    .unwrap()
                                    .iter()
                                    .enumerate()
                                    .map(|(offset, raw)| SerializedEvent {
                                        id: crate::EventId::for_event(
                                            &id,
                                            EventSequence(first + offset as i64),
                                        ),
                                        sequence: EventSequence(first + offset as i64),
                                        event_type: raw["type"].as_str().unwrap().into(),
                                        version: crate::EventVersion(
                                            raw["version"].as_i64().unwrap(),
                                        ),
                                        entity_id: id.clone(),
                                        entity_type: "account".into(),
                                        payload: raw["payload"].as_str().unwrap().into(),
                                        metadata: crate::Metadata::default(),
                                    })
                                    .collect();
                                let mut commit = Commit::new(id, events);
                                if let Some(snapshot) = stream.get("snapshot") {
                                    commit = commit.with_snapshot(crate::NewSnapshot::new(
                                        "account",
                                        EventSequence(snapshot["sequence"].as_i64().unwrap()),
                                        snapshot["payload"].as_str().unwrap(),
                                    ));
                                }

                                commit
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let mut commit = TransactionCommit::new(streams);
                commit.fences = step["fences"]
                    .as_array()
                    .map(|fences| {
                        fences
                            .iter()
                            .map(|fence| {
                                crate::ReadFence::new(
                                    id_of(&base, fence["suffix"].as_str().unwrap_or("")),
                                    "account",
                                    EventSequence(fence["head"].as_i64().unwrap()),
                                    if fence["callerStamped"].as_bool().unwrap_or(false) {
                                        crate::FenceProvenance::Caller
                                    } else {
                                        crate::FenceProvenance::Engine
                                    },
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                commit.normalize().unwrap();

                // The scenarios declare no features, so every writer validated an absent marker.
                // Every transaction step continues from an existing head event (none of the
                // recorded steps writes a compacted stream), so each stream above sequence 1 is
                // pinned through its head event.
                let pins: HashMap<EntityId, Continuation> = commit
                    .streams
                    .iter()
                    .filter_map(|stream| {
                        let first = stream.events.first()?.sequence;
                        (first > EventSequence(1)).then(|| {
                            (
                                stream.entity_id.clone(),
                                Continuation {
                                    previous: EventSequence(first.0 - 1),
                                    via_snapshot: false,
                                },
                            )
                        })
                    })
                    .collect();
                let (items, _) = offline_repository()
                    .transaction_stream_items(&commit, ValidatedMarker::Absent, &pins)
                    .unwrap();
                let actual: Vec<_> = items.iter().map(action_summary).collect();

                assert_eq!(
                    Value::Array(actual),
                    recorded["actions"],
                    "{name} step {index}: TransactWriteItems action order"
                );
                transactions += 1;
            }
        }

        assert!(
            transactions >= 8,
            "the scenarios cover multi-stream and fenced commits"
        );
    }

    #[test]
    fn projector_item_sizes_cover_keys_expressions_names_and_values() {
        let key_map = HashMap::from([("pk".to_owned(), s("view#1")), ("sk".to_owned(), n(0))]);
        // "pk" + "view#1" + "sk" + "0"
        let key_bytes = 2 + 6 + 2 + 1;
        // "#n" + "name" + ":v" + "value"
        let placeholder_bytes = 2 + 4 + 2 + 5;

        let update = TransactWriteItem::builder()
            .update(
                sdk::Update::builder()
                    .table_name("views")
                    .set_key(Some(key_map.clone()))
                    .update_expression("SET #n = :v")
                    .condition_expression("attribute_exists(pk)")
                    .expression_attribute_names("#n", "name")
                    .expression_attribute_values(":v", s("value"))
                    .build()
                    .unwrap(),
            )
            .build();
        assert_eq!(
            sdk_item_size(&update),
            key_bytes + "SET #n = :v".len() + "attribute_exists(pk)".len() + placeholder_bytes
        );

        let delete = TransactWriteItem::builder()
            .delete(
                Delete::builder()
                    .table_name("views")
                    .set_key(Some(key_map.clone()))
                    .condition_expression("#n = :v")
                    .expression_attribute_names("#n", "name")
                    .expression_attribute_values(":v", s("value"))
                    .build()
                    .unwrap(),
            )
            .build();
        assert_eq!(
            sdk_item_size(&delete),
            key_bytes + "#n = :v".len() + placeholder_bytes
        );

        let check = TransactWriteItem::builder()
            .condition_check(
                ConditionCheck::builder()
                    .table_name("views")
                    .set_key(Some(key_map.clone()))
                    .condition_expression("#n = :v")
                    .expression_attribute_names("#n", "name")
                    .expression_attribute_values(":v", s("value"))
                    .build()
                    .unwrap(),
            )
            .build();
        assert_eq!(
            sdk_item_size(&check),
            key_bytes + "#n = :v".len() + placeholder_bytes
        );

        let put = put(
            "views",
            Item::from([("pk".into(), AttributeValue::S("view#1".into()))]),
            None,
            &[],
        )
        .unwrap();
        assert_eq!(sdk_item_size(&put), 2 + 6);
    }

    #[test]
    fn a_large_non_put_projector_value_exceeds_the_commit_budget() {
        let large = "x".repeat(3_900_000);
        let operation = TransactWriteItem::builder()
            .update(
                sdk::Update::builder()
                    .table_name("views")
                    .key("pk", s("view#1"))
                    .update_expression("SET v = :v")
                    .expression_attribute_values(":v", s(&large))
                    .build()
                    .unwrap(),
            )
            .build();

        let mut sizes = vec![sdk_item_size(&operation)];
        sizes.push(10);

        assert!(matches!(
            crate::repository::check_budget(sizes),
            Err(Error::CommitTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn fences_for_locally_retained_types_are_refused_without_a_manifest() {
        let repository = offline_repository()
            .with_retention("scratch", Duration::from_secs(60))
            .with_retention("disabled", Duration::ZERO);

        let fenced = |entity_type: &str| {
            let mut commit = crate::TransactionCommit::<TransactWriteItem>::new(Vec::new());
            commit.fences = vec![crate::ReadFence::new(
                "watched",
                entity_type,
                EventSequence(0),
                crate::FenceProvenance::Engine,
            )];

            commit
        };

        // The refusal needs no manifest read: the offline client could not make one.
        let error = repository
            .commit_transaction(fenced("scratch"))
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::RetentionConflict { entity_type } if entity_type.as_str() == "scratch"),
            "{error:?}"
        );

        // A zero duration disables retention, so it is not refused here (the commit then needs
        // the table, which this offline repository lacks).
        assert!(!matches!(
            repository.commit_transaction(fenced("disabled")).await,
            Err(Error::RetentionConflict { .. })
        ));
    }

    /// Every public write entry point refuses projector operations that have no backing event
    /// (ADR-0012) before it touches the network: the offline client could not have served it.
    #[tokio::test]
    async fn every_write_entry_point_refuses_projector_only_input() {
        let repository = offline_repository();
        let operation = || {
            put(
                "views",
                Item::from([("pk".into(), AttributeValue::S("view".into()))]),
                None,
                &[],
            )
            .unwrap()
        };

        let single = Commit::new("a", Vec::new()).with_operations([operation()]);
        let mut transaction = crate::TransactionCommit::new(Vec::new());
        transaction.operations = vec![operation()];
        let mut eventless_stream = crate::TransactionCommit::new(vec![
            Commit::new("a", Vec::new()).with_operations([operation()]),
        ]);
        eventless_stream.operations = Vec::new();

        let results = [
            ("commit", repository.commit(single).await),
            (
                "commit_transaction (top-level operations)",
                repository.commit_transaction(transaction).await,
            ),
            (
                "commit_transaction (eventless stream operations)",
                repository.commit_transaction(eventless_stream).await,
            ),
        ];

        for (entry_point, result) in results {
            assert!(
                matches!(result, Err(Error::ProjectorWithoutEvents { operations: 1 })),
                "{entry_point}: {result:?}"
            );
        }
    }

    /// A stream pins its head through the item compaction cannot delete: the head event when it
    /// lies above the snapshot, the snapshot when it ends exactly at the head.
    #[test]
    fn a_stream_pins_its_head_through_the_item_compaction_cannot_delete() {
        let repository = offline_repository();
        let event = SerializedEvent {
            id: "pin-a:5".into(),
            entity_id: "pin-a".into(),
            entity_type: "account".into(),
            sequence: EventSequence(5),
            event_type: "account:opened".into(),
            version: crate::EventVersion(1),
            payload: b"{}".to_vec(),
            ..Default::default()
        };
        let commit: Commit<TransactWriteItem> = Commit::new("pin-a", vec![event]);
        let conditions = |commit: &Commit<TransactWriteItem>, pin: Option<Continuation>| {
            repository
                .stream_group(commit, pin)
                .unwrap()
                .iter()
                .map(|action| match (action.put(), action.condition_check()) {
                    (Some(put), _) => put.condition_expression().unwrap().to_owned(),
                    (_, Some(check)) => check.condition_expression().to_owned(),
                    _ => unreachable!("only puts and checks"),
                })
                .collect::<Vec<_>>()
        };

        let event_pin = Continuation {
            previous: EventSequence(4),
            via_snapshot: false,
        };
        assert_eq!(
            conditions(&commit, Some(event_pin)),
            vec![
                wire::CONTINUATION_CONDITION,
                wire::EVENT_CONDITION,
                wire::FLOOR_CONDITION
            ]
        );

        let snapshot_pin = Continuation {
            previous: EventSequence(4),
            via_snapshot: true,
        };
        assert_eq!(
            conditions(&commit, Some(snapshot_pin)),
            vec![wire::EVENT_CONDITION, wire::PINNED_FLOOR_CONDITION],
            "no event check: compaction may delete the head event the snapshot covers"
        );

        // A stream creating itself carries neither.
        assert_eq!(
            conditions(&commit, None),
            vec![wire::EVENT_CONDITION, wire::FLOOR_CONDITION]
        );

        // A later snapshot put pins through the snapshot too.
        let with_snapshot = Commit::new("pin-a", commit.events.clone())
            .with_snapshot(crate::NewSnapshot::new("account", EventSequence(2), "{}"));
        assert_eq!(
            conditions(&with_snapshot, Some(snapshot_pin)),
            vec![wire::EVENT_CONDITION, wire::PINNED_NEXT_SNAPSHOT_CONDITION]
        );
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

        let classified = classify_transaction_error(&EntityId::new("acct-1"), 0, false, err);

        assert!(
            matches!(classified, Error::Backend(_)),
            "the commit may have landed, so retrying could apply it twice: {classified:?}"
        );
    }

    #[test]
    fn only_a_genuinely_unknown_result_of_an_outcome_commit_is_indeterminate() {
        use aws_sdk_dynamodb::error::ErrorMetadata;

        let entity = EntityId::new("acct-1");
        let service = |code: &str| {
            SdkError::<TransactWriteItemsError, ()>::service_error(
                TransactWriteItemsError::generic(ErrorMetadata::builder().code(code).build()),
                (),
            )
        };

        // Unknown results: a timeout, a server fault, and a transaction still in progress.
        for unknown in [
            SdkError::<TransactWriteItemsError, ()>::timeout_error("response lost"),
            service("InternalServerError"),
            service("ServiceUnavailable"),
            service("TransactionInProgressException"),
        ] {
            assert!(
                matches!(
                    classify_transaction_error(&entity, 0, true, unknown),
                    Error::Indeterminate(_)
                ),
                "an unknown result of an outcome commit is indeterminate"
            );
        }

        // Without an outcome there is nothing to recover from: the classification is unchanged.
        let classified = classify_transaction_error(
            &entity,
            0,
            false,
            SdkError::<TransactWriteItemsError, ()>::timeout_error("response lost"),
        );
        assert!(matches!(classified, Error::Backend(_)), "{classified:?}");

        // Definitive refusals are plain errors, with or without an outcome: recovering them would
        // turn a request that can never succeed into a retryable ambiguous write.
        for definitive in [
            "ValidationException",
            "ResourceNotFoundException",
            "AccessDeniedException",
        ] {
            let classified = classify_transaction_error(&entity, 0, true, service(definitive));
            assert!(
                matches!(classified, Error::Backend(_)),
                "{definitive} is definitive: {classified:?}"
            );
        }
    }

    #[test]
    fn a_lost_outcome_put_is_told_apart_from_a_stream_condition() {
        const CHECK: Option<&str> = Some("ConditionalCheckFailed");
        const NONE: Option<&str> = Some("None");

        // marker, event put, floor check, outcome put
        assert!(outcome_failed(Some(3), &[NONE, NONE, NONE, CHECK]));
        assert!(outcome_failed(Some(3), &[NONE, CHECK, NONE, CHECK]));
        assert!(!outcome_failed(Some(3), &[NONE, CHECK, NONE, NONE]));
        assert!(!outcome_failed(None, &[NONE, NONE, NONE, CHECK]));
        assert!(!outcome_failed(Some(3), &[]));
    }

    #[test]
    fn the_outcome_put_is_the_last_action_and_an_outcome_only_commit_is_marker_then_outcome() {
        let repository = offline_repository();
        let outcome = Outcome::accepted("cmd", "digest");

        // Outcome-only, no fences: the marker check and the outcome put.
        let mut commit = TransactionCommit::new(Vec::new()).with_outcome(outcome.clone());
        commit.normalize().unwrap();
        let (mut items, mut layout) = repository
            .transaction_stream_items(&commit, ValidatedMarker::Absent, &HashMap::new())
            .unwrap();
        items.push(
            repository
                .outcome_put(&commit.stored_outcome().unwrap())
                .unwrap(),
        );
        layout.outcome = Some(items.len() - 1);

        let summary: Vec<_> = items.iter().map(action_summary).collect();
        assert_eq!(
            summary,
            vec![
                serde_json::json!({
                    "operation": "ConditionCheck", "pk": "SYSTEM#FEATURES", "sk": 0,
                    "condition": "attribute_not_exists(pk)"
                }),
                serde_json::json!({
                    "operation": "Put", "pk": "command-outcome#cmd", "sk": 1,
                    "condition": "attribute_not_exists(pk)"
                }),
            ]
        );
        assert_eq!(layout.outcome, Some(1));

        // With events and a fence, the outcome stays last, after the fence checks.
        let event = SerializedEvent {
            id: "a:1".into(),
            sequence: EventSequence(1),
            event_type: "t:e".into(),
            version: crate::EventVersion(1),
            entity_id: "a".into(),
            entity_type: "t".into(),
            payload: b"{}".to_vec(),
            ..Default::default()
        };
        let mut commit =
            TransactionCommit::new(vec![Commit::new("a", vec![event])]).with_outcome(outcome);
        commit.fences = vec![crate::ReadFence::new(
            "f",
            "t",
            EventSequence(2),
            crate::FenceProvenance::Engine,
        )];
        commit.normalize().unwrap();
        let (mut items, _) = repository
            .transaction_stream_items(&commit, ValidatedMarker::Absent, &HashMap::new())
            .unwrap();
        items.push(
            repository
                .outcome_put(&commit.stored_outcome().unwrap())
                .unwrap(),
        );

        let keys: Vec<_> = items
            .iter()
            .map(|item| {
                let summary = action_summary(item);
                (
                    summary["pk"].as_str().unwrap().to_owned(),
                    summary["sk"].as_i64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                ("SYSTEM#FEATURES".to_owned(), 0),
                ("a".to_owned(), 1),
                ("a".to_owned(), 0),
                ("f".to_owned(), 3),
                ("f".to_owned(), 0),
                ("command-outcome#cmd".to_owned(), 1),
            ]
        );
        let outcome_put = items.last().unwrap().put().unwrap();
        assert_eq!(outcome_put.item()["events"].as_s().unwrap(), r#"["a:1"]"#);
    }
}
