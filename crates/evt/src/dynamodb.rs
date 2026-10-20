//! A DynamoDB event log, item-for-item compatible with Go's `dynamo.Repository`.
//!
//! Bring your own [`aws_sdk_dynamodb::Client`]. This crate disables the SDK's default HTTP and
//! runtime features, so enable them in your application (for example by depending on
//! `aws-config` with its defaults). The table needs a string partition key `pk` and a number sort
//! key `sk`. See [`crate::wire::dynamo`] for the item layout.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::Range;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::AsyncSleep;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
    self as sdk, ConditionCheck, Delete, Put, TransactWriteItem, Update,
};

use crate::aggregate::{Aggregate, Projector};
use crate::error::{BoxError, Error};
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::features::{
    self, COMMAND_OUTCOME_PREFIX, CachedMarker, FeatureManifest, FeatureMarker, SYSTEM_PREFIX,
    ValidatedMarker,
};
use crate::heads::{self, Probe};
use crate::ids::{CommandId, EntityId, EntityType, EventSequence};
use crate::json;
use crate::outcome::Outcome;
use crate::projection::{ProjectedView, ProjectorOperationError, ViewCondition, ViewOperationKind};
use crate::repository::{Commit, ReadFence, Repository, TransactionCommit};
use crate::wire::dynamo::{self as wire, AttributeValue, HeadAction, Item};

/// DynamoDB's limit on actions per transaction.
pub const MAX_TRANSACTION_ITEMS: usize = 100;

/// How many times a commit re-validates the feature marker when its condition fails before the
/// commit gives up. Each revalidation re-reads the manifest, so only a table whose manifest keeps
/// changing under the writer can exhaust it.
const MAX_MARKER_REVALIDATIONS: usize = 8;

/// How often an out-of-band snapshot write re-sends its head guard after a concurrent writer moved
/// the head item under it.
const MAX_SNAPSHOT_GUARD_RETRIES: usize = 4;

/// What a failed snapshot head guard should do next, given the head item read back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GuardRetry {
    /// The head covers the snapshot but carries a lower (or no) `ttl` than the snapshot's: send
    /// the refreshing guard again.
    Refresh,
    /// The head covers the snapshot and its `ttl` is already at least the snapshot's: a plain
    /// check is enough.
    Cover,
    /// The head is absent or behind the snapshot: drift.
    Drift,
}

/// Decides the retry after a refreshing guard failed. The guard fails on either of its two
/// conditions, and a head that was behind when it failed may have been advanced by a concurrent
/// writer with a shorter `ttl` since, so only the stored `ttl` says whether the refresh is still
/// needed.
fn snapshot_guard_retry(
    stored: Option<(EventSequence, Option<i64>)>,
    snapshot: EventSequence,
    ttl: Option<i64>,
) -> GuardRetry {
    match stored {
        Some((head, head_ttl)) if head >= snapshot => {
            if ttl.is_some_and(|ttl| head_ttl.is_none_or(|head_ttl| head_ttl < ttl)) {
                GuardRetry::Refresh
            } else {
                GuardRetry::Cover
            }
        }
        _ => GuardRetry::Drift,
    }
}

/// How many times a manifest declaration retries its conditional write when another declaration
/// changed the row first.
const MAX_DECLARATION_RETRIES: usize = 8;

const COMPACTION_BATCH_SIZE: usize = MAX_TRANSACTION_ITEMS - 1;

/// Bound allocation and SDK limits independently of the caller's `usize` count.
const LATEST_PAGE_SIZE: usize = 100;

/// How many streams' loaded `ttl`s a repository instance remembers (see [`LoadedTtls`]).
const LOADED_TTL_CAPACITY: usize = 1024;

/// The largest `ttl` this repository instance has seen on the items of a stream, from its own
/// snapshot and event reads and from the failure-path probe's head read. A retained stream's head
/// update raises the head's `ttl` to at least this (the never-lower rule), and the load API does
/// not expose item `ttl`s, so the repository remembers them itself. It is bounded: the oldest
/// stream is forgotten first, and a forgotten stream only costs an ordinary conflict, because the
/// retry reloads the stream and sees the larger `ttl`.
#[derive(Debug, Default)]
struct LoadedTtls {
    ttls: HashMap<EntityId, i64>,
    order: VecDeque<EntityId>,
}

impl LoadedTtls {
    fn note(&mut self, entity_id: &EntityId, ttl: i64) {
        if let Some(known) = self.ttls.get_mut(entity_id) {
            *known = (*known).max(ttl);

            return;
        }

        if self.ttls.len() >= LOADED_TTL_CAPACITY
            && let Some(oldest) = self.order.pop_front()
        {
            self.ttls.remove(&oldest);
        }

        self.ttls.insert(entity_id.clone(), ttl);
        self.order.push_back(entity_id.clone());
    }

    fn get(&self, entity_id: &EntityId) -> Option<i64> {
        self.ttls.get(entity_id).copied()
    }
}

/// A DynamoDB-backed [`Repository`]. Clones share the client.
#[derive(Debug)]
pub struct DynamoRepository {
    client: Client,
    table: String,
    /// The table commit-wide view operations write to, when configured (v0.9, C7).
    views_table: Option<String>,
    consistent_read: bool,
    scan_page_size: Option<i32>,
    retention: BTreeMap<EntityType, Duration>,
    clock: fn() -> SystemTime,
    /// The feature-marker version this handle validated, if it has written yet. Each handle is
    /// its own writer: clones validate independently, like separate processes.
    validated_marker: Mutex<CachedMarker>,
    /// The `ttl`s this instance loaded, for the head update of a stream with retention.
    loaded_ttls: Mutex<LoadedTtls>,
}

impl Clone for DynamoRepository {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            table: self.table.clone(),
            views_table: self.views_table.clone(),
            consistent_read: self.consistent_read,
            scan_page_size: self.scan_page_size,
            retention: self.retention.clone(),
            clock: self.clock,
            validated_marker: Mutex::new(*self.validated_marker()),
            // Like a separate process, a clone remembers nothing it did not load itself.
            loaded_ttls: Mutex::new(LoadedTtls::default()),
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
            views_table: None,
            consistent_read: true,
            scan_page_size: None,
            retention: BTreeMap::new(),
            clock: SystemTime::now,
            validated_marker: Mutex::new(None),
            loaded_ttls: Mutex::new(LoadedTtls::default()),
        }
    }

    /// Writes commit-wide view operations (v0.9, C7) to `table`, in the same transaction as the
    /// events. A put writes an item with `pk`, `sk`, `entityID`, `entityType`, and `payload` (all
    /// strings, `attribute_not_exists(pk)` when the operation requires the row to be absent); a
    /// delete removes the `(pk, sk)` key (`attribute_exists(pk)` when it requires the row to be
    /// present). The table needs a string partition key `pk` and a string sort key `sk`. Without
    /// one, a commit that carries view operations is refused with [`Error::ViewsNotConfigured`]
    /// before any write.
    #[must_use]
    pub fn with_views_table(mut self, table: impl Into<String>) -> Self {
        self.views_table = Some(table.into());
        self
    }

    /// The views table, when one is configured.
    pub fn views_table(&self) -> Option<&str> {
        self.views_table.as_deref()
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

    fn loaded_ttls(&self) -> MutexGuard<'_, LoadedTtls> {
        self.loaded_ttls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Remembers an item `ttl` this instance read, for the head update of a retained stream.
    fn note_ttl(&self, entity_id: &EntityId, ttl: i64) {
        self.loaded_ttls().note(entity_id, ttl);
    }

    /// The current time in whole Unix seconds, by the repository's clock.
    fn now(&self) -> i64 {
        (self.clock)()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX)
            })
    }

    /// Returns the marker version this handle validated, reading and validating the manifest at
    /// first write. The read is strongly consistent regardless of `with_consistent_read`, so a
    /// declaration another writer just wrote cannot be missed. The returned non-optional snapshot
    /// is used for the entire transaction check, even if a concurrent declaration invalidates the
    /// cache afterwards. A stale snapshot is safe: the transaction condition forces revalidation.
    ///
    /// A writer requires the `stream-heads` feature (ADR 0007). A table whose manifest lacks it is
    /// declared by the writer itself, but only after a strongly consistent scan finds no
    /// non-reserved item in the table; a populated table is refused with
    /// [`Error::StreamHeadsNotMigrated`].
    async fn ensure_manifest_validated(&self) -> Result<ValidatedMarker, Error> {
        if let Some(cached) = *self.validated_marker() {
            return Ok(cached);
        }

        let mut declared = false;
        let validated = loop {
            let observed = self.read_manifest().await?;

            match features::validate_marker_for_write(observed.as_ref()) {
                Ok(validated) => break validated,
                Err(Error::StreamHeadsNotMigrated) if !declared => {
                    // Boxed: a first write is rare, and its scan and declaration must not widen
                    // every commit's future.
                    Box::pin(self.declare_on_empty_table()).await?;
                    declared = true;
                }
                Err(error) => return Err(error),
            }
        };

        let mut cached = self.validated_marker();

        Ok(*cached.get_or_insert(validated))
    }

    /// Declares `stream-heads` on a table with no data: a strongly consistent scan must find no
    /// item outside the reserved namespaces, or the table is refused with
    /// [`Error::StreamHeadsNotMigrated`].
    ///
    /// The scan and the declaration are two steps, and no transaction can exclude a writer that
    /// never reads the marker, so the order does the work: the declaration is committed first,
    /// which makes every marker-reading writer fail its per-commit check or refuse the table from
    /// then on, and only then is the table looked at once more. A stream such a writer committed
    /// before the declaration is durable by then, so the second look sees it. A stream with no
    /// head item means the table was not fresh after all: the declaration is retracted (a new
    /// manifest version, so every cached validation is dropped) and the table is refused. Streams
    /// other head-aware writers created in the meantime have a head item and are no reason to
    /// retract.
    async fn declare_on_empty_table(&self) -> Result<(), Error> {
        if self.holds_stream_data().await? {
            return Err(Error::StreamHeadsNotMigrated);
        }

        self.declare_stream_heads().await?;

        if self.holds_headless_stream().await? {
            self.update_manifest(|manifest| {
                manifest
                    .features
                    .remove(crate::features::STREAM_HEADS_FEATURE);
            })
            .await?;

            return Err(Error::StreamHeadsNotMigrated);
        }

        Ok(())
    }

    /// Reports whether any stream in the table has items but no head item. A scan is not atomic
    /// across items, so a stream that looks headless is confirmed with a strongly consistent read
    /// of its head item before it counts.
    async fn holds_headless_stream(&self) -> Result<bool, Error> {
        let mut streams = std::collections::BTreeSet::new();
        let mut headed = std::collections::BTreeSet::new();
        let mut start = None;

        loop {
            let page = self
                .client
                .scan()
                .table_name(&self.table)
                .consistent_read(true)
                .projection_expression("pk, sk")
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(sdk_error)?;

            for item in page.items() {
                let Some(sdk::AttributeValue::S(pk)) = item.get("pk") else {
                    continue;
                };
                if pk.starts_with(SYSTEM_PREFIX) || pk.starts_with(COMMAND_OUTCOME_PREFIX) {
                    continue;
                }

                streams.insert(pk.clone());
                if matches!(item.get("sk"), Some(sdk::AttributeValue::N(sk)) if sk == "-1") {
                    headed.insert(pk.clone());
                }
            }

            match page.last_evaluated_key {
                Some(key) if !key.is_empty() => start = Some(key),
                _ => break,
            }
        }

        for pk in streams.difference(&headed) {
            if self.read_head(&EntityId::new(pk.as_str())).await?.is_none() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Reports whether the table holds any item outside the reserved namespaces, with a strongly
    /// consistent Scan that reads only `pk` and stops at the first such item. A table is fresh
    /// only if this returns `false`.
    async fn holds_stream_data(&self) -> Result<bool, Error> {
        let mut start = None;

        loop {
            let page = self
                .client
                .scan()
                .table_name(&self.table)
                .consistent_read(true)
                .projection_expression("pk")
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(sdk_error)?;

            let found = page.items().iter().any(|item| {
                item.get("pk").is_some_and(|pk| match pk {
                    sdk::AttributeValue::S(pk) => {
                        !pk.starts_with(SYSTEM_PREFIX) && !pk.starts_with(COMMAND_OUTCOME_PREFIX)
                    }
                    // Not a string key: not ours, so not a reserved row either.
                    _ => true,
                })
            });
            if found {
                return Ok(true);
            }

            match page.last_evaluated_key {
                Some(key) if !key.is_empty() => start = Some(key),
                _ => return Ok(false),
            }
        }
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

    /// Declares `stream-heads: 1` in the feature manifest through the ordinary declaration
    /// transaction (one version bump), **without** checking that the table is empty or that
    /// every stream has a head item.
    ///
    /// This is the hook for a migration tool: after it has backfilled and verified a head item
    /// for every stream, with every writer that predates stream heads stopped, it declares the
    /// feature with this call. A fresh table needs no call: the first writer declares the
    /// feature itself after confirming the table is empty. Declaring on a populated table whose
    /// streams have no head items makes every later commit to those streams fail with
    /// [`Error::StreamHeadDrift`].
    pub async fn declare_stream_heads(&self) -> Result<(), Error> {
        self.update_manifest(FeatureManifest::declare_stream_heads)
            .await
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

    /// The transaction actions of a commit's view operations, in order. Every write is conditioned
    /// on the row's owner (the operation's owner, bound as `:owner`): a `Put` of the five-attribute
    /// item whose condition is `attribute_not_exists(pk)` when the row must be absent, else
    /// `attribute_not_exists(pk) OR entityID = :owner`; a `Delete` of the key whose condition is
    /// `attribute_exists(pk) AND entityID = :owner` when the row must be present, else
    /// `attribute_not_exists(pk) OR entityID = :owner`. With no views table configured and views
    /// present it refuses with [`Error::ViewsNotConfigured`], which the write entry points call
    /// before anything else.
    fn view_items(&self, views: &[ProjectedView]) -> Result<Vec<TransactWriteItem>, Error> {
        if views.is_empty() {
            return Ok(Vec::new());
        }
        let Some(table) = &self.views_table else {
            return Err(Error::ViewsNotConfigured {
                backend: "dynamodb",
            });
        };

        views
            .iter()
            .map(|view| {
                let owner = s(view.op.owner.as_str());

                match view.op.kind {
                    ViewOperationKind::Put => {
                        let owned = view.op.condition != ViewCondition::Absent;
                        let values = [(wire::OWNER_PLACEHOLDER, owner)];

                        put(
                            table,
                            wire::view_operation_item(&view.op)?,
                            Some(wire::put_condition(view.op.condition)),
                            if owned { &values } else { &[] },
                        )
                    }
                    ViewOperationKind::Delete => {
                        let delete = Delete::builder()
                            .table_name(table)
                            .key("pk", s(&view.op.pk))
                            .key("sk", s(&view.op.sk))
                            .condition_expression(wire::delete_condition(view.op.condition))
                            .expression_attribute_values(wire::OWNER_PLACEHOLDER, owner)
                            .build()
                            .map_err(Error::backend)?;

                        Ok(TransactWriteItem::builder().delete(delete).build())
                    }
                }
            })
            .collect()
    }

    /// Builds a single-stream commit's transaction items in the pinned order: the feature-marker
    /// check, the projector operations (the legacy backend operations, then the commit-wide view
    /// operations in `view_items`), then the stream's group (see
    /// [`stream_group`](Self::stream_group)). An outcome-only commit has no group; the outcome put
    /// is appended by the caller. The layout records each item's index so a failed commit is
    /// classified by the item that failed.
    fn transaction_items_with(
        &self,
        commit: &Commit<TransactWriteItem>,
        validated: ValidatedMarker,
        view_items: &[TransactWriteItem],
    ) -> Result<(Vec<TransactWriteItem>, TransactionLayout), Error> {
        let mut items = Vec::with_capacity(
            commit.events.len() + commit.operations.len() + view_items.len() + 3,
        );
        let mut layout = TransactionLayout::new(commit.entity_id.clone());

        items.push(self.marker_check(validated)?);
        // C10: the rebuild-lock check (`SYSTEM#REBUILD`) goes here, between the marker check and
        // the projector operations (spec, Writes step 2). It is not emitted yet, so it is not
        // budgeted; the change that starts emitting it adds it to the budget too.
        self.push_projector_items(
            commit.operations.iter().cloned(),
            &commit.views,
            view_items,
            &mut items,
            &mut layout,
        );
        self.push_group(commit, &mut items, &mut layout)?;

        Ok((items, layout))
    }

    /// [`transaction_items_with`](Self::transaction_items_with), building the view actions itself.
    #[cfg(test)]
    fn transaction_items(
        &self,
        commit: &Commit<TransactWriteItem>,
        validated: ValidatedMarker,
    ) -> Result<(Vec<TransactWriteItem>, TransactionLayout), Error> {
        let view_items = self.view_items(&commit.views)?;

        self.transaction_items_with(commit, validated, &view_items)
    }

    /// Builds a multi-stream commit's transaction items in the pinned order: the feature-marker
    /// check, the projector operations (legacy backend operations, then the commit-wide view
    /// operations in `view_items`), then per written stream in bytewise stream-ID order its
    /// group (head update, event puts, snapshot put), then per fenced read-only stream its head
    /// check. The layout records each item's index so a failed commit is classified by the item
    /// that failed.
    fn transaction_stream_items_with(
        &self,
        commit: &TransactionCommit<TransactWriteItem>,
        validated: ValidatedMarker,
        view_items: &[TransactWriteItem],
    ) -> Result<(Vec<TransactWriteItem>, TransactionLayout), Error> {
        let mut items = Vec::new();
        let mut layout = TransactionLayout::new(commit.streams.first().map_or_else(
            || EntityId::new("unknown"),
            |stream| stream.entity_id.clone(),
        ));

        items.push(self.marker_check(validated)?);
        // C10: the rebuild-lock check goes here (see `transaction_items_with`).
        self.push_projector_items(
            commit.operations.iter().cloned(),
            &commit.views,
            view_items,
            &mut items,
            &mut layout,
        );

        for stream in &commit.streams {
            self.push_group(stream, &mut items, &mut layout)?;
        }

        for fence in &commit.fences {
            items.push(head_action(
                &self.table,
                &HeadAction::fence(&fence.entity_id, fence.head),
            )?);
            layout.fences.push((items.len() - 1, fence.clone()));
        }

        Ok((items, layout))
    }

    /// [`transaction_stream_items_with`](Self::transaction_stream_items_with), building the view
    /// actions itself.
    #[cfg(test)]
    fn transaction_stream_items(
        &self,
        commit: &TransactionCommit<TransactWriteItem>,
        validated: ValidatedMarker,
    ) -> Result<(Vec<TransactWriteItem>, TransactionLayout), Error> {
        let view_items = self.view_items(&commit.views)?;

        self.transaction_stream_items_with(commit, validated, &view_items)
    }

    /// Appends the projector operations after the marker check, in the pinned order: the legacy
    /// backend operations first, then the commit-wide view operations (declaration, then emission
    /// order), and records where each sits. Both precede every written stream's group.
    fn push_projector_items(
        &self,
        operations: impl Iterator<Item = TransactWriteItem>,
        views: &[ProjectedView],
        view_items: &[TransactWriteItem],
        items: &mut Vec<TransactWriteItem>,
        layout: &mut TransactionLayout,
    ) {
        items.extend(operations);
        layout.operations = items.len() - 1;

        layout.views = views
            .iter()
            .zip(view_items)
            .enumerate()
            .map(|(offset, (view, _))| (items.len() + offset, view.provenance()))
            .collect();
        items.extend(view_items.iter().cloned());
    }

    /// Builds the command-outcome put: the last action of every commit that records one, written
    /// once in its final state and conditioned on the row not existing.
    fn outcome_put(&self, outcome: &Outcome) -> Result<TransactWriteItem, Error> {
        let item = wire::outcome_item(outcome)?;

        put(&self.table, item, Some(wire::OUTCOME_CONDITION), &[])
    }

    /// Appends one written stream's group to the transaction and records where it sits.
    fn push_group(
        &self,
        stream: &Commit<TransactWriteItem>,
        items: &mut Vec<TransactWriteItem>,
        layout: &mut TransactionLayout,
    ) -> Result<(), Error> {
        let Some(first) = stream.events.first() else {
            return Ok(());
        };

        let start = items.len();
        // One reading of the clock for the whole group, so the head's `ttl` is never below the
        // events' or the snapshot's.
        let ttl = self.ttl_for(&first.entity_type);
        let head_ttl = self.head_ttl(stream, ttl);
        items.extend(self.stream_group(stream, ttl, head_ttl)?);

        let events = start + 1..start + 1 + stream.events.len();
        layout.streams.push(StreamLayout {
            entity_id: stream.entity_id.clone(),
            entity_type: first.entity_type.clone(),
            first: first.sequence,
            head: start,
            snapshot: stream.snapshot.is_some().then_some(events.end),
            events,
            head_ttl,
        });

        Ok(())
    }

    /// The `ttl` a stream's head update carries (`:t`), `Some` only for a stream with retention.
    /// It is the commit's own `ttl`; on an append it is raised to the largest `ttl` this
    /// repository instance loaded for the stream, because the head's `ttl` is never lowered (the
    /// update's condition fails otherwise, as an ordinary conflict the retry heals).
    fn head_ttl(&self, stream: &Commit<TransactWriteItem>, own: Option<i64>) -> Option<i64> {
        let first = stream.events.first()?;
        let own = own?;

        if first.sequence <= EventSequence(1) {
            return Some(own);
        }

        Some(own.max(self.loaded_ttls().get(&stream.entity_id).unwrap_or(own)))
    }

    /// Builds one written stream's contiguous group: the head update, the event puts, then the
    /// snapshot put when the commit writes one. There is no continuation check and no floor
    /// check: the head update's condition (`head = first - 1`, or `attribute_not_exists(head)`
    /// when the stream is created) is the stream's only concurrency control beyond the per-item
    /// `attribute_not_exists` conditions (ADR 0007).
    ///
    /// The events and the snapshot carry the commit's own `ttl`; only the head takes the larger
    /// `head_ttl`.
    fn stream_group(
        &self,
        stream: &Commit<TransactWriteItem>,
        ttl: Option<i64>,
        head_ttl: Option<i64>,
    ) -> Result<Vec<TransactWriteItem>, Error> {
        let (Some(first), Some(last)) = (stream.events.first(), stream.events.last()) else {
            return Ok(Vec::new());
        };

        let mut items = Vec::with_capacity(stream.events.len() + 2);

        items.push(head_action(
            &self.table,
            &HeadAction::update(&stream.entity_id, first.sequence, last.sequence, head_ttl),
        )?);

        for event in &stream.events {
            let item = wire::event_item(event, ttl)?;
            items.push(put(&self.table, item, Some(wire::EVENT_CONDITION), &[])?);
        }

        if let Some(snapshot) = &stream.snapshot {
            let item = wire::snapshot_item(
                &SerializedSnapshot {
                    entity_type: snapshot.entity_type.clone(),
                    entity_id: stream.entity_id.clone(),
                    sequence: snapshot.sequence,
                    event_sequence: last.sequence,
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
                    &[(":seq", n(expected))],
                )?
            });
        }

        Ok(items)
    }

    /// Returns every item in an entity's partition, in sort key order: the stream's head item
    /// at `sk = -1` (ADR 0007), the snapshot at `sk = 0`, and the events. The head item is
    /// returned like any other row, so a caller that wants only events or the snapshot skips
    /// `sk < 1` or `sk < 0`. Intended for diagnostics and compatibility checks.
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
        self.highest_event(entity_id, self.consistent_read).await
    }

    /// The highest event `sk` of a stream: a `Query` of `sk > 0`, descending, limit 1.
    async fn highest_event(
        &self,
        entity_id: &EntityId,
        consistent: bool,
    ) -> Result<EventSequence, Error> {
        let page = self
            .client
            .query()
            .table_name(&self.table)
            .consistent_read(consistent)
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

    /// Derives a stream's head with strongly consistent reads (H4): the larger of its snapshot's
    /// `eventSeq` and its highest event `sk`.
    async fn derive_head(&self, entity_id: &EntityId) -> Result<EventSequence, Error> {
        let snapshot = self
            .client
            .get_item()
            .table_name(&self.table)
            .consistent_read(true)
            .key("pk", s(entity_id.as_str()))
            .key("sk", n(wire::SNAPSHOT_SORT_KEY))
            .projection_expression("eventSeq")
            .send()
            .await
            .map_err(sdk_error)?;
        let covered = match &snapshot.item {
            Some(item) => number_attribute(item, "eventSeq")?.unwrap_or_default(),
            None => 0,
        };

        Ok(self
            .highest_event(entity_id, true)
            .await?
            .max(EventSequence(covered)))
    }

    /// Reads a stream's head item with a strongly consistent `GetItem`: its `head` and `ttl`, or
    /// `None` when the stream has no head item. The `ttl` is remembered for the stream's next
    /// head update.
    async fn read_head(&self, entity_id: &EntityId) -> Result<Option<EventSequence>, Error> {
        Ok(self.read_head_item(entity_id).await?.map(|(head, _)| head))
    }

    /// The stream's head item read with a strongly consistent `GetItem`: its `head` and its `ttl`
    /// (`None` for an item with no `ttl`), or `None` when there is no head item.
    async fn read_head_item(
        &self,
        entity_id: &EntityId,
    ) -> Result<Option<(EventSequence, Option<i64>)>, Error> {
        let output = self
            .client
            .get_item()
            .table_name(&self.table)
            .consistent_read(true)
            .key("pk", s(entity_id.as_str()))
            .key("sk", n(wire::HEAD_SORT_KEY))
            .send()
            .await
            .map_err(sdk_error)?;

        let Some(item) = output.item.filter(|item| !item.is_empty()) else {
            return Ok(None);
        };

        let item = from_sdk_item(&item);
        if let Some(ttl) = wire::item_ttl(&item) {
            self.note_ttl(entity_id, ttl);
        }

        wire::head_from_item(&item).map(Some)
    }

    /// Reports whether every item remaining in a stream (its snapshot, and the events from
    /// `sk > 0`, each read with its `ttl`) has a `ttl` that has passed: the stream is only
    /// waiting for DynamoDB's lazy TTL deletion. A stream with no items left is trivially
    /// expired. An item with no `ttl` is not.
    async fn remaining_items_expired(&self, entity_id: &EntityId) -> Result<bool, Error> {
        let now = self.now();
        let expired = |item: &HashMap<String, sdk::AttributeValue>| {
            wire::item_ttl(&from_sdk_item(item)).is_some_and(|ttl| ttl <= now)
        };

        let snapshot = self
            .client
            .get_item()
            .table_name(&self.table)
            .consistent_read(true)
            .key("pk", s(entity_id.as_str()))
            .key("sk", n(wire::SNAPSHOT_SORT_KEY))
            .projection_expression("#ttl")
            .expression_attribute_names("#ttl", "ttl")
            .send()
            .await
            .map_err(sdk_error)?;
        if snapshot
            .item
            .as_ref()
            .is_some_and(|item| !item.is_empty() && !expired(item))
        {
            return Ok(false);
        }

        let mut start = None;
        loop {
            let page = self
                .client
                .query()
                .table_name(&self.table)
                .consistent_read(true)
                .key_condition_expression("pk = :pk AND sk > :sk")
                .expression_attribute_values(":pk", s(entity_id.as_str()))
                .expression_attribute_values(":sk", n(0))
                .projection_expression("sk, #ttl")
                .expression_attribute_names("#ttl", "ttl")
                .set_exclusive_start_key(start)
                .send()
                .await
                .map_err(sdk_error)?;

            if page.items().iter().any(|item| !expired(item)) {
                return Ok(false);
            }

            match page.last_evaluated_key {
                Some(key) if !key.is_empty() => start = Some(key),
                _ => return Ok(true),
            }
        }
    }

    /// The failure-path probe for a failed head update or fence check on a stream (ADR 0007): no
    /// `ReturnValues` on the transaction, a few strongly consistent reads instead.
    ///
    /// Derive the head `D1`, then read the stored head `S`. `S < D1` is drift, and `S = D1`
    /// means no drift. For `S > D1`, derive again as `D2`: `D2 < S` is drift, otherwise a commit
    /// landed between the reads, an ordinary conflict. The order (derive, then read the head,
    /// then re-derive only when the head is ahead) is what keeps a concurrent commit from
    /// reading as drift.
    ///
    /// A stream with retention (this repository's `with_retention` configuration) treats a head
    /// above the derived head as the expired-tail transient and not drift, and so is an absent
    /// head item with items remaining if every remaining item's `ttl` has passed: TTL deletion is
    /// lazy and unordered.
    async fn probe_stream(&self, entity_id: &EntityId, retained: bool) -> Result<Probed, Error> {
        let derived = Box::pin(self.derive_head(entity_id)).await?;
        let stored = Box::pin(self.read_head(entity_id))
            .await?
            .unwrap_or_default();

        let derived = match Probe::first(stored, derived) {
            Probe::Settled => return Ok(Probed::Settled(stored)),
            // A head item that exists but is behind its log is never an expiry artifact: TTL
            // deletes the head item whole, not down to a lower value.
            Probe::Drift if stored > EventSequence::ZERO => {
                return Ok(Probed::Drift(heads::drift(entity_id, stored, derived)));
            }
            Probe::Drift => derived,
            // For a type with retention, a head above the derived head is the expired-tail
            // transient: TTL deleted the tail the head counted. It heals; it is not drift.
            Probe::Ahead if retained => return Ok(Probed::Conflict),
            Probe::Ahead => {
                let rederived = Box::pin(self.derive_head(entity_id)).await?;
                if Probe::after_rederive(stored, rederived) == Probe::Settled {
                    return Ok(Probed::Conflict);
                }

                rederived
            }
        };

        if retained && Box::pin(self.remaining_items_expired(entity_id)).await? {
            return Ok(Probed::Conflict);
        }

        Ok(Probed::Drift(heads::drift(entity_id, stored, derived)))
    }

    /// The case where a stream's head update passed and one of its event puts failed: an event
    /// exists above the head, which is drift. A stream with retention reads its remaining items
    /// first, because TTL can delete the head item before an expired event: if every one has a
    /// `ttl` in the past, it is a transient conflict.
    async fn certain_drift(&self, entity_id: &EntityId, retained: bool) -> Result<Error, Error> {
        if retained && Box::pin(self.remaining_items_expired(entity_id)).await? {
            return Ok(heads::conflict(entity_id));
        }

        let derived = Box::pin(self.derive_head(entity_id)).await?;
        let stored = Box::pin(self.read_head(entity_id))
            .await?
            .unwrap_or_default();

        Ok(heads::drift(entity_id, stored, derived))
    }

    /// The two parts of an out-of-band snapshot write besides the marker check: the head item's
    /// guard (H3, see [`HeadAction::snapshot_guard`]) and the snapshot put. Both carry the
    /// snapshot's own `ttl`, read from the clock once.
    fn snapshot_parts(
        &self,
        snapshot: &SerializedSnapshot,
    ) -> Result<(HeadAction, TransactWriteItem, Option<i64>), Error> {
        let ttl = self.ttl_for(&snapshot.entity_type);
        let item = wire::snapshot_item(snapshot, ttl)?;
        let snapshot_put = put(
            &self.table,
            item,
            Some(wire::PUT_SNAPSHOT_CONDITION),
            &[(":new", n(snapshot.event_sequence.0))],
        )?;

        Ok((
            HeadAction::snapshot_guard(&snapshot.entity_id, snapshot.event_sequence, ttl),
            snapshot_put,
            ttl,
        ))
    }

    /// An out-of-band snapshot write's transaction: the feature-marker check, the head item, then
    /// the snapshot put.
    fn snapshot_items(
        &self,
        validated: ValidatedMarker,
        guard: &HeadAction,
        snapshot_put: &TransactWriteItem,
    ) -> Result<Vec<TransactWriteItem>, Error> {
        Ok(vec![
            self.marker_check(validated)?,
            head_action(&self.table, guard)?,
            snapshot_put.clone(),
        ])
    }

    /// Remembers the `ttl` a committed stream's head now carries, so this instance's next head
    /// update for the stream never lowers it.
    fn note_committed(&self, layout: &TransactionLayout) {
        for stream in &layout.streams {
            if let Some(ttl) = stream.head_ttl {
                self.note_ttl(&stream.entity_id, ttl);
            }
        }
    }

    /// The error a failed written stream contributes to the ranking, or `None` when none of its
    /// actions failed. The head update's cancellation reason decides how: a failed head update
    /// is classified by the probe; a head update that passed while an event put failed is certain
    /// drift; a failed snapshot put alone is an ordinary conflict.
    async fn stream_verdict(
        &self,
        stream: &StreamLayout,
        codes: &[Option<&str>],
    ) -> Result<Option<Error>, Error> {
        let failed =
            |index: usize| codes.get(index).copied().flatten() == Some("ConditionalCheckFailed");
        let retained = self.retains(&stream.entity_type);

        if failed(stream.head) {
            let verdict = match Box::pin(self.probe_stream(&stream.entity_id, retained)).await? {
                Probed::Drift(drift) => drift,
                Probed::Settled(stored) => {
                    heads::gap_or_conflict(&stream.entity_id, stream.first, stored)
                }
                Probed::Conflict => heads::conflict(&stream.entity_id),
            };

            return Ok(Some(verdict));
        }

        if stream.events.clone().any(failed) {
            if codes.get(stream.head).copied().flatten() == Some("None") {
                return Box::pin(self.certain_drift(&stream.entity_id, retained))
                    .await
                    .map(Some);
            }

            return Ok(Some(heads::conflict(&stream.entity_id)));
        }

        if stream.snapshot.is_some_and(failed) {
            // The head update passed (or was not the failing item): an out-of-band snapshot can
            // bump the snapshot sequence without moving the head, an ordinary conflict.
            return Ok(Some(heads::conflict(&stream.entity_id)));
        }

        Ok(None)
    }

    /// Maps a cancelled commit (whose marker check and outcome put held) to the error it
    /// reports. Every failed stream and fence item is classified before any is reported, and the
    /// ranking is: `StreamHeadDrift`, then `SequenceGap`, then any other conflict (stream items
    /// and fences in transaction order, a fence with its provenance), then projector conditions.
    /// Throttling and `TransactionConflict` keep their classification.
    async fn classify_commit_failure<R>(
        &self,
        layout: &TransactionLayout,
        carries_outcome: bool,
        err: SdkError<TransactWriteItemsError, R>,
    ) -> Error
    where
        R: HttpStatus + std::fmt::Debug + Send + Sync + 'static,
    {
        let codes = cancellation_reasons(&err);
        let failed =
            |index: usize| codes.get(index).copied().flatten() == Some("ConditionalCheckFailed");

        let mut verdicts = Vec::new();

        for stream in &layout.streams {
            match Box::pin(self.stream_verdict(stream, &codes)).await {
                Ok(Some(verdict)) => verdicts.push(verdict),
                Ok(None) => {}
                Err(error) => return error,
            }
        }

        for (index, fence) in &layout.fences {
            if !failed(*index) {
                continue;
            }

            match Box::pin(self.probe_stream(&fence.entity_id, false)).await {
                Ok(Probed::Drift(drift)) => verdicts.push(drift),
                Ok(Probed::Settled(_) | Probed::Conflict) => {
                    verdicts.push(heads::fence_conflict(fence));
                }
                Err(error) => return error,
            }
        }

        if let Some(error) = heads::rank(verdicts) {
            return error;
        }

        // Rank 6: only projector operations failed, which reloading cannot fix. The first failed
        // one in transaction order is named. A legacy backend operation precedes every view
        // operation and maps to the legacy projector error as before; a view operation maps to a
        // typed error when its cancellation reason is a condition, validation, or item-collection
        // failure. `TransactionConflict` and throttling on a projector item are not projector
        // failures: they keep their transient classification below.
        if failed_items(&codes, "ConditionalCheckFailed")
            .any(|index| (1..=layout.operations).contains(&index))
        {
            return Error::Projector(Box::new(err));
        }

        let failed_view = layout.views.iter().find_map(|(index, view)| {
            let code = codes.get(*index).copied().flatten()?;

            matches!(
                code,
                "ConditionalCheckFailed" | "ValidationError" | "ItemCollectionSizeLimitExceeded"
            )
            .then(|| (view, code.to_owned()))
        });
        if let Some((view, code)) = failed_view {
            // A failed condition is the operation's own doing; any other reason keeps the SDK's
            // error as the backend's cause.
            let cause: Option<BoxError> = (code != crate::projection::REASON_CONDITION_FAILED)
                .then(|| Box::new(err) as BoxError);

            return Error::ProjectorOperation(Box::new(ProjectorOperationError::for_view(
                view, code, cause,
            )));
        }

        // A concurrent transaction is reported on the item it collided with.
        let colliding = failed_items(&codes, "TransactionConflict").next();
        let entity = colliding
            .and_then(|index| layout.entity_at(index))
            .unwrap_or_else(|| layout.fallback.clone());

        classify_transaction_error(&entity, layout.projector_items(), carries_outcome, err)
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
                if let Some(ttl) = wire::item_ttl(&item) {
                    self.note_ttl(entity_id, ttl);
                }
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
                let item = from_sdk_item(&item);
                if let Some(ttl) = wire::item_ttl(&item) {
                    self.note_ttl(entity_id, ttl);
                }

                Ok(Some(wire::snapshot_from_item(&item)?))
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
        // Compaction is a write path: it refuses an unsupported feature and a table that is not
        // migrated to stream heads. It never touches a head item.
        let observed = features::validate_marker_for_write(manifest.as_ref())?;
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

    fn writes_views(&self) -> bool {
        true
    }

    async fn declare_history_dependent(&self, types: &[EntityType]) -> Result<(), Error> {
        crate::features::declare_history(self, types).await
    }

    async fn commit(&self, commit: Commit<TransactWriteItem>) -> Result<(), Error> {
        commit.validate()?;
        features::check_not_reserved(&commit.entity_id)?;

        if commit.events.is_empty() && commit.outcome.is_none() {
            return Ok(());
        }

        let outcome = commit.stored_outcome();
        let mut revalidations = 0;

        // Views with nowhere to write them are refused before anything else, so they are never
        // silently dropped.
        let view_items = self.view_items(&commit.views)?;

        // The shared budget covers every item the transaction carries, with the projector
        // operations and view operations sized exactly from the items this backend will write. It
        // fails with CommitTooLarge before any write, identically on every backend.
        let mut sizes = commit.sizes(|entity_type| self.ttl_for(entity_type))?;
        sizes.extend(commit.operations.iter().map(sdk_item_size));
        sizes.extend(view_items.iter().map(sdk_item_size));
        crate::repository::check_budget(sizes)?;

        loop {
            let cached = self.ensure_manifest_validated().await?;
            let (mut items, mut layout) =
                self.transaction_items_with(&commit, cached, &view_items)?;

            // The outcome row is the last action. Its index is how a failed condition on it is
            // told apart from a stream's.
            if let Some(outcome) = &outcome {
                items.push(self.outcome_put(outcome)?);
                layout.outcome = Some(items.len() - 1);
            }

            match self
                .client
                .transact_write_items()
                .set_transact_items(Some(items))
                .send()
                .await
            {
                Ok(_) => {
                    self.note_committed(&layout);

                    return Ok(());
                }
                Err(err) => {
                    let codes = cancellation_reasons(&err);
                    if classify_cancellation(layout.projector_items(), &codes)
                        == Some(Cancellation::Marker)
                    {
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
                        && outcome_failed(layout.outcome, &codes)
                    {
                        return Err(Error::OutcomeConflict {
                            command_id: outcome.command_id.clone(),
                        });
                    }

                    // Boxed: the failure path is rare, and its reads must not widen every
                    // commit's future (the conformance cases chain many commits in debug builds
                    // on a default test-thread stack).
                    return Err(Box::pin(self.classify_commit_failure(
                        &layout,
                        outcome.is_some(),
                        err,
                    ))
                    .await);
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

        // Views with nowhere to write them are refused before anything else, so they are never
        // silently dropped.
        let view_items = self.view_items(&commit.views)?;

        // The shared budget covers every item the transaction carries, projector operations and
        // view operations included, and fails with CommitTooLarge before any request.
        let mut sizes = commit.sizes(|entity_type| self.ttl_for(entity_type))?;
        sizes.extend(commit.operations.iter().map(sdk_item_size));
        sizes.extend(view_items.iter().map(sdk_item_size));
        crate::repository::check_budget(sizes)?;

        // Fences are refused for entity types this repository was configured to expire: it needs
        // no I/O, so it runs first. The durable manifest's retention set is checked in the loop,
        // and the refusal is the union of the two.
        commit.check_retention(None, |entity_type| self.retains(entity_type))?;

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

            let (mut items, mut layout) =
                self.transaction_stream_items_with(&commit, cached, &view_items)?;
            if let Some(outcome) = &outcome {
                items.push(self.outcome_put(outcome)?);
                layout.outcome = Some(items.len() - 1);
            }

            match self
                .client
                .transact_write_items()
                .set_transact_items(Some(items))
                .send()
                .await
            {
                Ok(_) => {
                    self.note_committed(&layout);

                    return Ok(());
                }
                Err(err) => {
                    let codes = cancellation_reasons(&err);

                    if failed_items(&codes, "ConditionalCheckFailed").next() == Some(0) {
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

                    // Boxed: the failure path is rare, and its reads must not widen every
                    // commit's future (the conformance cases chain many commits in debug builds
                    // on a default test-thread stack).
                    return Err(Box::pin(self.classify_commit_failure(
                        &layout,
                        outcome.is_some(),
                        err,
                    ))
                    .await);
                }
            }
        }
    }

    /// Reads the outcome row with a strongly consistent `GetItem`, whatever
    /// [`with_consistent_read`](DynamoRepository::with_consistent_read) says: an eventually
    /// consistent miss would re-execute a command that already committed.
    fn backoff(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        // The SDK's own timer, so recovery polling works on whatever runtime the client was built
        // for. A client with no timer returns at once: the polls then run back to back.
        let sleeper = self.client.config().sleep_impl();

        async move {
            if let Some(sleeper) = sleeper {
                sleeper.sleep(duration).await;
            }
        }
    }

    fn deadline(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        // The SDK's own timer. A client with no timer never fires, so its reads are bounded only
        // by the SDK's operation timeouts.
        let sleeper = self.client.config().sleep_impl();

        async move {
            match sleeper {
                Some(sleeper) => sleeper.sleep(duration).await,
                None => std::future::pending().await,
            }
        }
    }

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
                if let Some(ttl) = wire::item_ttl(&item) {
                    self.note_ttl(entity_id, ttl);
                }
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

        // H3: the stream's head item must cover the snapshot, and a stream with retention
        // refreshes the head's `ttl` to the snapshot's own in the same transaction, because the
        // snapshot would otherwise outlive the head item.
        let (refresh_guard, snapshot_put, ttl) = self.snapshot_parts(snapshot)?;
        let mut guard = refresh_guard.clone();
        let mut guard_retries = 0;

        let mut revalidations = 0;
        loop {
            let cached = self.ensure_manifest_validated().await?;
            let items = self.snapshot_items(cached, &guard, &snapshot_put)?;
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
                    let failed = |index: usize| {
                        codes.get(index).copied().flatten() == Some("ConditionalCheckFailed")
                    };

                    // A failed marker takes precedence over a stale snapshot condition.
                    if failed(0) {
                        self.invalidate_manifest();
                        if revalidations >= MAX_MARKER_REVALIDATIONS {
                            return Err(sdk_error(err));
                        }
                        revalidations += 1;
                        continue;
                    }

                    // A failed head condition means the snapshot is ahead of the stream, or the
                    // stream has no head item. It ranks above the snapshot's own condition,
                    // which keeps its meaning: a newer snapshot already covers this write.
                    if failed(1) {
                        // A refreshing guard has two conditions (the head covers the snapshot,
                        // and its stored `ttl` is not above the snapshot's) and the read-back
                        // says which still matters. A stored `ttl` at least the snapshot's needs
                        // no refresh: retry as a plain check. A lower or missing one means the
                        // head was behind when the guard failed and a concurrent writer then
                        // advanced it with a shorter `ttl`: retry the refresh, because a plain
                        // check would leave the snapshot outliving the head item.
                        if guard.update_expression.is_some() {
                            let stored = self.read_head_item(&snapshot.entity_id).await?;
                            let next = snapshot_guard_retry(stored, snapshot.event_sequence, ttl);
                            if next != GuardRetry::Drift {
                                if guard_retries >= MAX_SNAPSHOT_GUARD_RETRIES {
                                    return Err(Error::Transient(
                                        format!(
                                            "the head item of {} kept moving under its snapshot write",
                                            snapshot.entity_id
                                        )
                                        .into(),
                                    ));
                                }
                                guard_retries += 1;

                                guard = if next == GuardRetry::Refresh {
                                    refresh_guard.clone()
                                } else {
                                    HeadAction::snapshot_cover(
                                        &snapshot.entity_id,
                                        snapshot.event_sequence,
                                    )
                                };

                                continue;
                            }
                        }

                        let stored = self
                            .read_head(&snapshot.entity_id)
                            .await?
                            .unwrap_or_default();
                        let derived = Box::pin(self.derive_head(&snapshot.entity_id))
                            .await?
                            .max(snapshot.event_sequence);

                        return Err(heads::drift(&snapshot.entity_id, stored, derived));
                    }

                    if failed(2) {
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

        // Compaction is a write path: a table that does not declare `stream-heads` (or declares a
        // feature this build does not model) is refused before anything is read, so the refusal
        // wins over `CompactionUncovered`. It never declares the feature itself.
        features::validate_marker_for_write(self.read_manifest().await?.as_ref())?;

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

/// What the failure-path probe concluded about a stream (see
/// [`DynamoRepository::probe_stream`]).
enum Probed {
    /// The stream's head item disagrees with its log.
    Drift(Error),
    /// The head matches the log; the stored head decides between a gap and a conflict.
    Settled(EventSequence),
    /// An ordinary conflict: a commit landed between the probe's reads, or a stream with
    /// retention is waiting for TTL deletion.
    Conflict,
}

/// Where each kind of item sits in a transaction, for failure classification.
struct TransactionLayout {
    /// Number of legacy backend projector operations, which start at index 1 after the marker
    /// check (and the rebuild-lock check, once C10 emits it).
    operations: usize,
    /// The commit-wide view operations by item index, in transaction order, directly after the
    /// legacy operations. Each is the view without its row data: provenance, kind, keys, and the
    /// mapped error, enough to name it in an error.
    views: Vec<(usize, ProjectedView)>,
    /// Each written stream's group, in transaction order.
    streams: Vec<StreamLayout>,
    /// Fences by their head-check item index, in transaction order.
    fences: Vec<(usize, ReadFence)>,
    /// The index of the command-outcome put, the last action, when the commit records one.
    outcome: Option<usize>,
    /// The entity a failure is attributed to when its index maps to no stream.
    fallback: EntityId,
}

/// One written stream's contiguous group of actions.
struct StreamLayout {
    entity_id: EntityId,
    entity_type: EntityType,
    /// The commit's first sequence for the stream.
    first: EventSequence,
    /// The head update's index.
    head: usize,
    /// The event puts' indexes.
    events: Range<usize>,
    /// The snapshot put's index, when the commit writes one.
    snapshot: Option<usize>,
    /// The `ttl` the head update carries, for a stream with retention.
    head_ttl: Option<i64>,
}

impl StreamLayout {
    fn contains(&self, index: usize) -> bool {
        index >= self.head
            && index
                < self
                    .snapshot
                    .map_or(self.events.end, |snapshot| snapshot + 1)
    }
}

impl TransactionLayout {
    fn new(fallback: EntityId) -> Self {
        Self {
            operations: 0,
            views: Vec::new(),
            streams: Vec::new(),
            fences: Vec::new(),
            outcome: None,
            fallback,
        }
    }

    /// The number of projector items after the marker check: legacy backend operations, then view
    /// operations.
    fn projector_items(&self) -> usize {
        self.operations + self.views.len()
    }

    /// The stream or fenced stream whose action sits at `index`, if it is one of theirs.
    fn entity_at(&self, index: usize) -> Option<EntityId> {
        self.streams
            .iter()
            .find(|stream| stream.contains(index))
            .map(|stream| stream.entity_id.clone())
            .or_else(|| {
                self.fences
                    .iter()
                    .find(|(fence_index, _)| *fence_index == index)
                    .map(|(_, fence)| fence.entity_id.clone())
            })
    }
}

/// Builds the transaction action for a head-item [`HeadAction`]: an `Update` or a
/// `ConditionCheck` on `(pk, sk = -1)`. The expression strings, name placeholders, and values come
/// from the action, the single source the commit budget also sizes.
fn head_action(table: &str, action: &HeadAction) -> Result<TransactWriteItem, Error> {
    let key = to_sdk_item(&action.key());

    if let Some(update_expression) = action.update_expression {
        let mut update = Update::builder()
            .table_name(table)
            .set_key(Some(key))
            .update_expression(update_expression)
            .condition_expression(action.condition_expression);
        for (placeholder, name) in &action.names {
            update = update.expression_attribute_names(*placeholder, *name);
        }
        for (placeholder, number) in &action.values {
            update = update.expression_attribute_values(*placeholder, n(*number));
        }

        return Ok(TransactWriteItem::builder()
            .update(update.build().map_err(Error::backend)?)
            .build());
    }

    let mut check = ConditionCheck::builder()
        .table_name(table)
        .set_key(Some(key))
        .condition_expression(action.condition_expression);
    for (placeholder, name) in &action.names {
        check = check.expression_attribute_names(*placeholder, *name);
    }
    for (placeholder, number) in &action.values {
        check = check.expression_attribute_values(*placeholder, n(*number));
    }

    Ok(TransactWriteItem::builder()
        .condition_check(check.build().map_err(Error::backend)?)
        .build())
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

/// Reads an entity-heads scan item: an event's `sk`, or a snapshot's `eventSeq`. Like Go's
/// `headSeqFromItem`, it skips an item whose key or sequence is missing or not a number, so one bad
/// row does not stop a rebuild. It also skips a stream's head item (`sk = -1`): the heads scan
/// derives each head from the events and the snapshot, and the head item would otherwise
/// contribute `-1`.
fn item_head(item: &HashMap<String, sdk::AttributeValue>) -> Option<(&str, i64)> {
    let number = |name| match item.get(name) {
        Some(sdk::AttributeValue::N(value)) => value.parse::<i64>().ok(),
        _ => None,
    };
    let Some(sdk::AttributeValue::S(pk)) = item.get("pk") else {
        return None;
    };

    let head = match number("sk")? {
        sk if sk < wire::SNAPSHOT_SORT_KEY => return None,
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
    R: HttpStatus + std::fmt::Debug + Send + Sync + 'static,
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
    R: HttpStatus + std::fmt::Debug + Send + Sync + 'static,
{
    match err {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => {
            true
        }
        // Any 5xx is a server or gateway fault, whatever its error code: a proxy's 502 or 504 can
        // arrive after the transaction applied.
        SdkError::ServiceError(service) => {
            service
                .raw()
                .http_status()
                .is_some_and(|status| status >= 500)
                || matches!(
                    service.err().code(),
                    Some(
                        "InternalServerError"
                            | "ServiceUnavailable"
                            | "TransactionInProgressException"
                            | "IdempotentParameterMismatchException"
                    )
                )
        }
        _ => false,
    }
}

/// The HTTP status of a raw SDK response, when it has one.
trait HttpStatus {
    fn http_status(&self) -> Option<u16>;
}

impl HttpStatus for aws_sdk_dynamodb::config::http::HttpResponse {
    fn http_status(&self) -> Option<u16> {
        Some(self.status().as_u16())
    }
}

#[cfg(test)]
impl HttpStatus for () {
    fn http_status(&self) -> Option<u16> {
        None
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
#[cfg(feature = "journal")]
mod journal;
mod views;

pub use views::DynamoViewRepository;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_snapshot_guard_retries_the_refresh_when_the_head_advanced_with_a_shorter_ttl() {
        let covers = EventSequence(3);

        // The guard failed while the head was behind; a concurrent writer advanced it, keeping
        // the earlier ttl: the refresh must be sent again.
        assert_eq!(
            snapshot_guard_retry(Some((EventSequence(3), Some(100))), covers, Some(200)),
            GuardRetry::Refresh
        );
        assert_eq!(
            snapshot_guard_retry(Some((EventSequence(9), None)), covers, Some(200)),
            GuardRetry::Refresh
        );

        // The stored ttl already covers the snapshot's (or the stream is not retained).
        assert_eq!(
            snapshot_guard_retry(Some((EventSequence(3), Some(200))), covers, Some(200)),
            GuardRetry::Cover
        );
        assert_eq!(
            snapshot_guard_retry(Some((EventSequence(3), Some(300))), covers, Some(200)),
            GuardRetry::Cover
        );
        assert_eq!(
            snapshot_guard_retry(Some((EventSequence(3), None)), covers, None),
            GuardRetry::Cover
        );

        // A head behind the snapshot, or no head, is drift.
        assert_eq!(
            snapshot_guard_retry(Some((EventSequence(2), Some(500))), covers, Some(200)),
            GuardRetry::Drift
        );
        assert_eq!(
            snapshot_guard_retry(None, covers, Some(200)),
            GuardRetry::Drift
        );
    }

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
                // A fresh table: the first writer declared `stream-heads` itself, which created
                // the marker row at version 1.
                ValidatedMarker::Version(1)
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
                .0
                .iter()
                .map(fixture_action)
                .collect::<Vec<_>>();
            assert_eq!(json!(actual), case["output"], "{}", case["name"]);
        }
    }

    /// An action in the shape of the shared fixtures (`feature-marker.json`): operation, table,
    /// key or item, expressions, names, and values.
    fn fixture_action(action: &TransactWriteItem) -> serde_json::Value {
        use serde_json::json;

        let (mut value, condition, values, names) = if let Some(put) = action.put() {
            assert!(put.return_values_on_condition_check_failure().is_none());
            (
                json!({"operation": "Put", "tableName": put.table_name(),
                "item": from_sdk_item(put.item())}),
                put.condition_expression(),
                put.expression_attribute_values(),
                put.expression_attribute_names(),
            )
        } else if let Some(update) = action.update() {
            assert!(update.return_values_on_condition_check_failure().is_none());
            (
                json!({"operation": "Update", "tableName": update.table_name(),
                "key": from_sdk_item(update.key()),
                "updateExpression": update.update_expression()}),
                update.condition_expression(),
                update.expression_attribute_values(),
                update.expression_attribute_names(),
            )
        } else if let Some(delete) = action.delete() {
            assert!(delete.return_values_on_condition_check_failure().is_none());
            (
                json!({"operation": "Delete", "tableName": delete.table_name(),
                "key": from_sdk_item(delete.key())}),
                delete.condition_expression(),
                delete.expression_attribute_values(),
                delete.expression_attribute_names(),
            )
        } else {
            let check = action
                .condition_check()
                .expect("only puts, updates, deletes, and checks");
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

        value
    }

    static VECTOR_CLOCK_SECONDS: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);

    fn vector_clock() -> SystemTime {
        SystemTime::UNIX_EPOCH
            + Duration::from_secs(VECTOR_CLOCK_SECONDS.load(std::sync::atomic::Ordering::SeqCst))
    }

    /// The head-item vectors Go recorded (`streamHeads` in `feature-marker.json`): the exact
    /// actions, expression strings, names, and values of each commit shape, and the budgeted
    /// size of every action. The head update's budgeted size is the size of the action built.
    #[test]
    fn stream_head_actions_and_sizes_match_the_go_vectors() {
        use serde_json::Value;

        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../compat/fixtures/feature-marker.json");
        let fixture: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let cases = fixture["streamHeads"].as_array().unwrap();
        assert!(cases.len() >= 7, "the vectors cover every commit shape");

        for case in cases {
            let name = case["name"].as_str().unwrap();
            let request = &case["request"];
            let expected = case["output"].as_array().unwrap();

            let seconds = request["clockUnixNano"].as_u64().unwrap_or(0) / 1_000_000_000;
            VECTOR_CLOCK_SECONDS.store(seconds, std::sync::atomic::Ordering::SeqCst);
            let mut repository = offline_repository().with_clock(vector_clock);
            repository.table = "evt-compat".into();
            for (entity_type, nanos) in request["retentionNanos"].as_object().into_iter().flatten()
            {
                repository = repository.with_retention(
                    entity_type.as_str(),
                    Duration::from_nanos(nanos.as_u64().unwrap()),
                );
            }
            for seeded in request["seeded"].as_array().into_iter().flatten() {
                if let Some(ttl) = seeded["ttl"].as_i64() {
                    repository.note_ttl(&EntityId::new(seeded["entityId"].as_str().unwrap()), ttl);
                }
            }

            // An out-of-band snapshot write: the marker check, the head item, the snapshot put.
            if let Some(write) = request.get("putSnapshot") {
                let snapshot = SerializedSnapshot {
                    entity_type: write["entityType"].as_str().unwrap().into(),
                    entity_id: EntityId::new(write["entityId"].as_str().unwrap()),
                    sequence: EventSequence(write["snapshotSequence"].as_i64().unwrap()),
                    event_sequence: EventSequence(write["eventSequence"].as_i64().unwrap()),
                    payload: b"{}".to_vec(),
                };
                let (guard, snapshot_put, _) = repository.snapshot_parts(&snapshot).unwrap();
                let items = repository
                    .snapshot_items(ValidatedMarker::Version(1), &guard, &snapshot_put)
                    .unwrap();
                let actual: Vec<_> = items.iter().map(fixture_action).collect();
                assert_eq!(serde_json::Value::Array(actual), case["output"], "{name}");

                continue;
            }

            // The events are not in the request: they are the vectors' fixed event, with the
            // metadata and payload of the first event put Go recorded.
            let sample = expected
                .iter()
                .find(|action| action["operation"] == "Put" && action["item"]["sk"]["N"] != "0")
                .map(|action| &action["item"])
                .unwrap_or_else(|| panic!("{name}: an event put"));
            let text = |key: &str| sample[key]["S"].as_str().unwrap().to_owned();
            let snapshot_payload = expected
                .iter()
                .find(|action| action["operation"] == "Put" && action["item"]["sk"]["N"] == "0")
                .map(|action| action["item"]["payload"]["S"].as_str().unwrap().to_owned());

            let streams = request["streams"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|stream| {
                    let id = EntityId::new(stream["entityId"].as_str().unwrap());
                    let entity_type = stream["entityType"].as_str().unwrap();
                    let first = stream["first"].as_i64().unwrap();
                    let events = (first..first + stream["count"].as_i64().unwrap())
                        .map(|sequence| SerializedEvent {
                            id: crate::EventId::for_event(&id, EventSequence(sequence)),
                            sequence: EventSequence(sequence),
                            event_type: text("type").into(),
                            version: crate::EventVersion(1),
                            entity_id: id.clone(),
                            entity_type: entity_type.into(),
                            payload: text("payload").into_bytes(),
                            metadata: serde_json::from_str(&text("metadata")).unwrap(),
                        })
                        .collect();
                    let mut commit = Commit::new(id, events);
                    if let Some(sequence) = stream["snapshotSequence"].as_i64() {
                        commit = commit.with_snapshot(crate::NewSnapshot::new(
                            entity_type,
                            EventSequence(sequence),
                            snapshot_payload.clone().unwrap().into_bytes(),
                        ));
                    }

                    commit
                })
                .collect();
            let mut commit = TransactionCommit::new(streams);
            commit.fences = request["fences"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|fence| {
                    crate::ReadFence::new(
                        fence["entityId"].as_str().unwrap(),
                        "account",
                        EventSequence(fence["head"].as_i64().unwrap()),
                        crate::FenceProvenance::Engine,
                    )
                })
                .collect();
            commit.normalize().unwrap();

            let (items, _) = repository
                .transaction_stream_items(&commit, ValidatedMarker::Version(1))
                .unwrap();
            let actual: Vec<_> = items.iter().map(fixture_action).collect();
            assert_eq!(serde_json::Value::Array(actual), case["output"], "{name}");

            // The same actions, as the budget counts them (order aside).
            let mut sizes = commit
                .sizes(|entity_type| repository.ttl_for(entity_type))
                .unwrap();
            let mut recorded: Vec<usize> = case["sizes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|size| size.as_u64().unwrap() as usize)
                .collect();
            sizes.sort_unstable();
            recorded.sort_unstable();
            assert_eq!(sizes, recorded, "{name}: budgeted sizes");
        }
    }

    /// Summarizes a transaction action the way the Go fixtures record it: operation, key, the
    /// update expression of an `Update`, and the condition with its expression names and values. An
    /// action on the views table also names it (`"table": "views"`) and records its string sort key
    /// as `skText`, with `sk` 0.
    /// The views table the offline fixtures name.
    const VIEWS_TABLE: &str = "views";

    fn action_summary(action: &TransactWriteItem) -> serde_json::Value {
        use serde_json::json;

        let key_of = |_: &str, item: &HashMap<String, sdk::AttributeValue>| {
            let pk = item["pk"].as_s().unwrap().clone();

            match &item["sk"] {
                sdk::AttributeValue::N(number) => (pk, number.parse::<i64>().unwrap(), None),
                sdk::AttributeValue::S(text) => (pk, 0, Some(text.clone())),
                other => panic!("unexpected sort key {other:?}"),
            }
        };

        let (operation, table, key, update, condition, names, values) =
            if let Some(put) = action.put() {
                (
                    "Put",
                    put.table_name(),
                    key_of(put.table_name(), put.item()),
                    None,
                    put.condition_expression(),
                    put.expression_attribute_names(),
                    put.expression_attribute_values(),
                )
            } else if let Some(update) = action.update() {
                (
                    "Update",
                    update.table_name(),
                    key_of(update.table_name(), update.key()),
                    Some(update.update_expression()),
                    update.condition_expression(),
                    update.expression_attribute_names(),
                    update.expression_attribute_values(),
                )
            } else if let Some(delete) = action.delete() {
                (
                    "Delete",
                    delete.table_name(),
                    key_of(delete.table_name(), delete.key()),
                    None,
                    delete.condition_expression(),
                    delete.expression_attribute_names(),
                    delete.expression_attribute_values(),
                )
            } else {
                let check = action
                    .condition_check()
                    .expect("only puts, updates, deletes, and checks");

                (
                    "ConditionCheck",
                    check.table_name(),
                    key_of(check.table_name(), check.key()),
                    None,
                    Some(check.condition_expression()),
                    check.expression_attribute_names(),
                    check.expression_attribute_values(),
                )
            };

        let mut summary = json!({"operation": operation, "pk": key.0, "sk": key.1});
        if table == VIEWS_TABLE {
            summary["table"] = json!("views");
        }
        if let Some(text) = key.2 {
            summary["skText"] = json!(text);
        }
        if let Some(update) = update {
            summary["update"] = json!(update);
        }
        if let Some(condition) = condition {
            summary["condition"] = json!(condition);
        }
        if let Some(names) = names.filter(|names| !names.is_empty()) {
            summary["names"] = json!(names);
        }
        if let Some(values) = values.filter(|values| !values.is_empty()) {
            summary["values"] = json!(from_sdk_item(values));
        }

        summary
    }

    /// The commit-wide view operations a scenario step names (`projectors`), with `{id}` resolved
    /// to the scenario's entity ID and keys padded with `x` to `pkPad` and `skPad`.
    fn step_views(base: &str, step: &serde_json::Value) -> Vec<ProjectedView> {
        use crate::projection::ViewOperation;

        let scope = |value: &serde_json::Value| value.as_str().unwrap_or("").replace("{id}", base);
        let pad = |mut text: String, size: u64| {
            while (text.len() as u64) < size {
                text.push('x');
            }

            text
        };

        let mut views = Vec::new();
        for (index, projector) in step["projectors"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
        {
            for (operation, item) in projector["operations"]
                .as_array()
                .into_iter()
                .flatten()
                .enumerate()
            {
                let pk = pad(scope(&item["pk"]), item["pkPad"].as_u64().unwrap_or(0));
                let sk = pad(scope(&item["sk"]), item["skPad"].as_u64().unwrap_or(0));
                let mut op = if item["op"] == "delete" {
                    ViewOperation::delete(pk, sk, scope(&item["owner"]))
                } else {
                    ViewOperation::put(
                        pk,
                        sk,
                        scope(&item["owner"]),
                        scope(&item["entityType"]),
                        scope(&item["payload"]).into_bytes(),
                    )
                };
                op = match item["condition"].as_str() {
                    Some("absent") => op.if_absent(),
                    Some("present") => op.if_present(),
                    _ => op,
                };

                views.push(ProjectedView::new(
                    projector["name"].as_str().unwrap(),
                    index,
                    operation,
                    op,
                ));
            }
        }

        views
    }

    /// The commit-wide view vectors Go recorded (`commitViews` in `feature-marker.json`): the exact
    /// actions of a commit that carries view operations, in order, and the budgeted sizes (the
    /// events, snapshots, head updates, and fences, then each view operation's size in emission
    /// order). A view put counts its five attributes and a view delete its key and condition, and
    /// the size is the size of the action built.
    #[test]
    fn commit_view_actions_and_sizes_match_the_go_vectors() {
        use serde_json::Value;

        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../compat/fixtures/feature-marker.json");
        let fixture: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let cases = fixture["commitViews"].as_array().unwrap();
        assert!(
            cases.len() >= 2,
            "the vectors cover multi- and single-stream commits"
        );

        for case in cases {
            let name = case["name"].as_str().unwrap();
            let request = &case["request"];
            let expected = case["output"].as_array().unwrap();
            let events_table = request["eventsTable"].as_str().unwrap();
            let views_table = request["viewsTable"].as_str().unwrap();

            let mut repository = offline_repository().with_views_table(views_table);
            repository.table = events_table.into();

            // The events are the stream-head vectors' fixed event: the metadata and payload of
            // the first event put Go recorded.
            let events_put = |sequence_is_zero: bool| {
                expected.iter().find(|action| {
                    action["operation"] == "Put"
                        && action["tableName"] == events_table
                        && (action["item"]["sk"]["N"] == "0") == sequence_is_zero
                })
            };
            let sample =
                &events_put(false).unwrap_or_else(|| panic!("{name}: an event put"))["item"];
            let text = |key: &str| sample[key]["S"].as_str().unwrap().to_owned();
            let snapshot_payload = events_put(true)
                .map(|action| action["item"]["payload"]["S"].as_str().unwrap().to_owned());

            let streams = request["streams"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|stream| {
                    let id = EntityId::new(stream["entityId"].as_str().unwrap());
                    let entity_type = stream["entityType"].as_str().unwrap();
                    let first = stream["first"].as_i64().unwrap();
                    let events = (first..first + stream["count"].as_i64().unwrap())
                        .map(|sequence| SerializedEvent {
                            id: crate::EventId::for_event(&id, EventSequence(sequence)),
                            sequence: EventSequence(sequence),
                            event_type: text("type").into(),
                            version: crate::EventVersion(1),
                            entity_id: id.clone(),
                            entity_type: entity_type.into(),
                            payload: text("payload").into_bytes(),
                            metadata: serde_json::from_str(&text("metadata")).unwrap(),
                        })
                        .collect();
                    let mut commit = Commit::new(id, events);
                    if let Some(sequence) = stream["snapshotSequence"].as_i64() {
                        commit = commit.with_snapshot(crate::NewSnapshot::new(
                            entity_type,
                            EventSequence(sequence),
                            snapshot_payload.clone().unwrap().into_bytes(),
                        ));
                    }

                    commit
                })
                .collect();
            let mut commit = TransactionCommit::new(streams);
            commit.fences = request["fences"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|fence| {
                    crate::ReadFence::new(
                        fence["entityId"].as_str().unwrap(),
                        "account",
                        EventSequence(fence["head"].as_i64().unwrap()),
                        crate::FenceProvenance::Engine,
                    )
                })
                .collect();
            commit.views = step_views("", request);
            commit.normalize().unwrap();

            let (items, layout) = repository
                .transaction_stream_items(&commit, ValidatedMarker::Version(1))
                .unwrap();
            let actual: Vec<_> = items.iter().map(fixture_action).collect();
            assert_eq!(serde_json::Value::Array(actual), case["output"], "{name}");
            assert_eq!(
                layout.views.len(),
                commit.views.len(),
                "{name}: view layout"
            );

            // The budget: the non-view actions in any order, then the views in emission order,
            // each the size of the action built.
            let recorded: Vec<usize> = case["sizes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|size| size.as_u64().unwrap() as usize)
                .collect();
            let (rest, view_sizes) = recorded.split_at(recorded.len() - commit.views.len());
            let mut sizes = commit
                .sizes(|entity_type| repository.ttl_for(entity_type))
                .unwrap();
            let mut rest = rest.to_vec();
            sizes.sort_unstable();
            rest.sort_unstable();
            assert_eq!(sizes, rest, "{name}: budgeted sizes");
            assert_eq!(
                commit.view_sizes().collect::<Vec<_>>(),
                view_sizes,
                "{name}: view operation sizes"
            );

            // The analytic size is the size of the action the repository builds.
            let built = repository.view_items(&commit.views).unwrap();
            assert_eq!(
                built.iter().map(sdk_item_size).collect::<Vec<_>>(),
                view_sizes,
                "{name}: the built actions"
            );
        }
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
            // Outcome scenarios record every request they sent (`calls`), including the ones the
            // table cancelled; the evt-compat tests assert those against a live run.
            if scenario["outcomes"].as_bool().unwrap_or(false) {
                continue;
            }
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
            // A transaction refused before any write (a key over the limit, a repeated key,
            // operations with no event) sends no request and records no order.
            let planned: Vec<_> = steps
                .iter()
                .enumerate()
                .filter(|(_, step)| step["op"] == "transaction")
                .filter(|(_, step)| {
                    !matches!(
                        step["expect"]["result"].as_str(),
                        Some("viewKeyTooLong" | "duplicateViewKey" | "projectorOnly")
                    )
                })
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
                commit.views = step_views(&base, step);
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

                // The scenarios start on a fresh table, so the first writer declared
                // `stream-heads` itself, which created the marker row at version 1.
                let (items, _) = offline_repository()
                    .with_views_table(VIEWS_TABLE)
                    .transaction_stream_items(&commit, ValidatedMarker::Version(1))
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

        // A stream's head item (`sk = -1`) is not an event and not a snapshot: it contributes
        // nothing, whatever its other attributes say.
        assert_eq!(item_head(&item(n(-1), None)), None);
        assert_eq!(item_head(&item(n(-1), Some(n(9)))), None);
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
            // Still running: it may yet commit, so the store polls for its row.
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
    fn a_gateway_fault_with_an_unknown_code_is_an_unknown_result() {
        use aws_sdk_dynamodb::error::ErrorMetadata;

        #[derive(Debug)]
        struct Status(u16);

        impl HttpStatus for Status {
            fn http_status(&self) -> Option<u16> {
                Some(self.0)
            }
        }

        let entity = EntityId::new("acct-1");
        let service = |status: u16| {
            SdkError::<TransactWriteItemsError, Status>::service_error(
                TransactWriteItemsError::generic(
                    ErrorMetadata::builder().code("SomeGatewayCode").build(),
                ),
                Status(status),
            )
        };

        for status in [500, 502, 503, 504] {
            assert!(
                matches!(
                    classify_transaction_error(&entity, 0, true, service(status)),
                    Error::Indeterminate(_)
                ),
                "{status} may have arrived after the transaction applied"
            );
        }

        let classified = classify_transaction_error(&entity, 0, true, service(400));
        assert!(matches!(classified, Error::Backend(_)), "{classified:?}");
    }

    #[test]
    fn a_lost_outcome_put_is_told_apart_from_a_stream_condition() {
        const CHECK: Option<&str> = Some("ConditionalCheckFailed");
        const NONE: Option<&str> = Some("None");

        // marker, head update, event put, outcome put
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
            .transaction_stream_items(&commit, ValidatedMarker::Absent)
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
            .transaction_stream_items(&commit, ValidatedMarker::Absent)
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
                ("a".to_owned(), -1),
                ("a".to_owned(), 1),
                ("f".to_owned(), -1),
                ("command-outcome#cmd".to_owned(), 1),
            ]
        );
        let outcome_put = items.last().unwrap().put().unwrap();
        assert_eq!(outcome_put.item()["events"].as_s().unwrap(), r#"["a:1"]"#);
    }

    fn account_event(id: &str, sequence: i64) -> SerializedEvent {
        SerializedEvent {
            id: format!("{id}:{sequence}").into(),
            sequence: EventSequence(sequence),
            event_type: "account:credited".into(),
            version: crate::EventVersion(1),
            entity_id: id.into(),
            entity_type: "account".into(),
            payload: b"{}".to_vec(),
            ..Default::default()
        }
    }

    fn stream_commit(
        id: &str,
        sequences: std::ops::RangeInclusive<i64>,
    ) -> Commit<TransactWriteItem> {
        Commit::new(id, sequences.map(|n| account_event(id, n)).collect())
    }

    fn built(
        repository: &DynamoRepository,
        streams: Vec<Commit<TransactWriteItem>>,
        fences: Vec<(&str, i64)>,
    ) -> Vec<serde_json::Value> {
        let mut commit = TransactionCommit::new(streams);
        commit.fences = fences
            .into_iter()
            .map(|(id, head)| {
                crate::ReadFence::new(
                    id,
                    "account",
                    EventSequence(head),
                    crate::FenceProvenance::Engine,
                )
            })
            .collect();
        commit.normalize().unwrap();

        let (items, _) = repository
            .transaction_stream_items(&commit, ValidatedMarker::Absent)
            .unwrap();

        items.iter().map(action_summary).collect()
    }

    #[test]
    fn a_commit_sends_head_updates_events_snapshots_and_fences_in_the_pinned_order() {
        use serde_json::json;

        let repository = offline_repository();
        let snapshot = crate::NewSnapshot::new("account", EventSequence(2), "{}");

        // The spec's worked example: appends to `b` (a new stream) and `a` (a snapshot-bearing
        // append at head 7), fencing `z` (never written) and `m` (at head 3).
        let actual = built(
            &repository,
            vec![
                stream_commit("b", 1..=1),
                stream_commit("a", 8..=9).with_snapshot(snapshot),
            ],
            vec![("z", 0), ("m", 3)],
        );

        assert_eq!(
            actual,
            vec![
                json!({"operation": "ConditionCheck", "pk": "SYSTEM#FEATURES", "sk": 0,
                    "condition": "attribute_not_exists(pk)"}),
                json!({"operation": "Update", "pk": "a", "sk": -1,
                    "update": "SET head = :last", "condition": "head = :prev",
                    "values": {":last": {"N": "9"}, ":prev": {"N": "7"}}}),
                json!({"operation": "Put", "pk": "a", "sk": 8, "condition": "attribute_not_exists(sk)"}),
                json!({"operation": "Put", "pk": "a", "sk": 9, "condition": "attribute_not_exists(sk)"}),
                json!({"operation": "Put", "pk": "a", "sk": 0, "condition": "seq = :seq",
                    "values": {":seq": {"N": "1"}}}),
                json!({"operation": "Update", "pk": "b", "sk": -1,
                    "update": "SET head = :last", "condition": "attribute_not_exists(head)",
                    "values": {":last": {"N": "1"}}}),
                json!({"operation": "Put", "pk": "b", "sk": 1, "condition": "attribute_not_exists(sk)"}),
                json!({"operation": "ConditionCheck", "pk": "m", "sk": -1, "condition": "head = :head",
                    "values": {":head": {"N": "3"}}}),
                json!({"operation": "ConditionCheck", "pk": "z", "sk": -1,
                    "condition": "attribute_not_exists(head)"}),
            ],
            "marker; per written stream in byte order the head update, event puts, and snapshot \
             put; the fences in byte order; no continuation or floor check, no companion"
        );

        // A first snapshot in a commit carries the first-snapshot condition and nothing else.
        let first = built(
            &repository,
            vec![
                stream_commit("c", 1..=2).with_snapshot(crate::NewSnapshot::new(
                    "account",
                    EventSequence(1),
                    "{}",
                )),
            ],
            vec![],
        );
        assert_eq!(first[4]["condition"], "attribute_not_exists(seq)");
        assert!(first[4].get("values").is_none());
    }

    #[test]
    fn a_retained_streams_head_update_raises_its_ttl_and_never_lowers_it() {
        use serde_json::json;

        fn clock() -> SystemTime {
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_000)
        }

        let repository = offline_repository()
            .with_clock(clock)
            .with_retention("account", Duration::from_secs(100));
        let id = EntityId::new("a");

        // A create stamps the commit's own ttl on the head, under `attribute_not_exists(head)`.
        let created = built(&repository, vec![stream_commit("a", 1..=2)], vec![]);
        assert_eq!(
            created[1],
            json!({"operation": "Update", "pk": "a", "sk": -1,
                "update": "SET head = :last, #ttl = :t",
                "condition": "attribute_not_exists(head)",
                "names": {"#ttl": "ttl"},
                "values": {":last": {"N": "2"}, ":t": {"N": "1100"}}})
        );

        // An append guards the ttl by value.
        let appended = built(&repository, vec![stream_commit("a", 3..=3)], vec![]);
        assert_eq!(
            appended[1],
            json!({"operation": "Update", "pk": "a", "sk": -1,
                "update": "SET head = :last, #ttl = :t",
                "condition": "head = :prev AND (attribute_not_exists(#ttl) OR #ttl <= :t)",
                "names": {"#ttl": "ttl"},
                "values": {":last": {"N": "3"}, ":prev": {"N": "2"}, ":t": {"N": "1100"}}})
        );

        // The events keep the commit's own ttl, whatever the head takes.
        repository.note_ttl(&id, 5_000);
        let (items, layout) = repository
            .transaction_stream_items(
                &{
                    let mut commit = TransactionCommit::new(vec![stream_commit("a", 3..=3)]);
                    commit.normalize().unwrap();
                    commit
                },
                ValidatedMarker::Absent,
            )
            .unwrap();
        let head = items[layout.streams[0].head].update().unwrap();
        assert_eq!(head.expression_attribute_values().unwrap()[":t"], n(5_000));
        assert_eq!(layout.streams[0].head_ttl, Some(5_000));
        let event = items[layout.streams[0].events.start].put().unwrap();
        assert_eq!(event.item()["ttl"], n(1_100), "the event keeps its own ttl");

        // A smaller remembered ttl never lowers the head below the commit's own.
        repository.note_ttl(&EntityId::new("small"), 10);
        let small = built(&repository, vec![stream_commit("small", 2..=2)], vec![]);
        assert_eq!(small[1]["values"][":t"], json!({"N": "1100"}));

        // A stream without retention writes neither the name nor `:t`.
        let plain = built(
            &offline_repository().with_clock(clock),
            vec![stream_commit("p", 2..=2)],
            vec![],
        );
        assert!(plain[1].get("names").is_none() && plain[1]["values"].get(":t").is_none());
    }

    #[test]
    fn remembered_ttls_are_bounded_and_only_ever_grow() {
        let mut ttls = LoadedTtls::default();
        let first = EntityId::new("s0");

        ttls.note(&first, 50);
        ttls.note(&first, 20);
        assert_eq!(
            ttls.get(&first),
            Some(50),
            "a smaller ttl does not lower it"
        );
        ttls.note(&first, 70);
        assert_eq!(ttls.get(&first), Some(70));

        for n in 1..=LOADED_TTL_CAPACITY {
            ttls.note(&EntityId::new(format!("s{n}")), 1);
        }
        assert_eq!(ttls.ttls.len(), LOADED_TTL_CAPACITY);
        assert_eq!(
            ttls.get(&first),
            None,
            "the oldest stream is forgotten first"
        );
        assert_eq!(
            ttls.get(&EntityId::new(format!("s{LOADED_TTL_CAPACITY}"))),
            Some(1)
        );
    }

    #[test]
    fn out_of_band_snapshot_writes_guard_the_head_item() {
        use serde_json::json;

        let id = EntityId::new("a");
        let guard = |action: &HeadAction| action_summary(&head_action("events", action).unwrap());

        assert_eq!(
            guard(&HeadAction::snapshot_guard(&id, EventSequence(7), None)),
            json!({"operation": "ConditionCheck", "pk": "a", "sk": -1,
                "condition": "head >= :new", "values": {":new": {"N": "7"}}})
        );
        assert_eq!(
            guard(&HeadAction::snapshot_guard(&id, EventSequence(0), None)),
            json!({"operation": "ConditionCheck", "pk": "a", "sk": -1,
                "condition": "attribute_not_exists(head) OR head >= :new",
                "values": {":new": {"N": "0"}}})
        );
        assert_eq!(
            guard(&HeadAction::snapshot_guard(
                &id,
                EventSequence(7),
                Some(1_100)
            )),
            json!({"operation": "Update", "pk": "a", "sk": -1,
                "update": "SET #ttl = :t",
                "condition": "head >= :new AND (attribute_not_exists(#ttl) OR #ttl <= :t)",
                "names": {"#ttl": "ttl"},
                "values": {":new": {"N": "7"}, ":t": {"N": "1100"}}})
        );
        assert_eq!(
            guard(&HeadAction::snapshot_guard(
                &id,
                EventSequence(0),
                Some(1_100)
            )),
            json!({"operation": "ConditionCheck", "pk": "a", "sk": -1,
                "condition": "attribute_not_exists(head) OR head >= :new",
                "values": {":new": {"N": "0"}}}),
            "an Update would create a head item with no head attribute"
        );
        assert_eq!(
            guard(&HeadAction::snapshot_cover(&id, EventSequence(7))),
            guard(&HeadAction::snapshot_guard(&id, EventSequence(7), None)),
            "the fallback after a failed ttl condition is the plain check"
        );
    }

    /// The ADR's worked examples, counted from the actions this repository really builds: the
    /// marker check, the groups and fences, and the outcome put.
    #[test]
    fn the_built_actions_match_the_adr_action_counts_and_the_budget() {
        let repository = offline_repository();
        let outcome = Outcome::accepted("cmd", "digest");
        let snapshot = |sequence| crate::NewSnapshot::new("account", EventSequence(sequence), "{}");

        let count = |streams: Vec<Commit<TransactWriteItem>>, fences: Vec<(&str, i64)>| {
            let mut commit = TransactionCommit::new(streams).with_outcome(outcome.clone());
            commit.fences = fences
                .into_iter()
                .map(|(id, head)| {
                    crate::ReadFence::new(
                        id,
                        "account",
                        EventSequence(head),
                        crate::FenceProvenance::Engine,
                    )
                })
                .collect();
            commit.normalize().unwrap();

            let (items, _) = repository
                .transaction_stream_items(&commit, ValidatedMarker::Absent)
                .unwrap();
            let sizes = commit.sizes(|_| None).unwrap();
            assert_eq!(
                sizes.len(),
                items.len() + 1,
                "the estimator counts the outcome put"
            );

            // The estimator budgets every action by the size of the action it builds.
            for (item, size) in items.iter().zip(&sizes).skip(1) {
                if item.put().is_none() {
                    assert_eq!(sdk_item_size(item), *size, "{item:?}");
                }
            }

            items.len() + 1
        };

        assert_eq!(count(vec![stream_commit("a", 1..=1)], vec![]), 4, "create");
        assert_eq!(count(vec![stream_commit("a", 2..=2)], vec![]), 4, "append");
        assert_eq!(
            count(
                vec![stream_commit("a", 2..=2).with_snapshot(snapshot(2))],
                vec![]
            ),
            5,
            "append with a snapshot"
        );
        assert_eq!(
            count(
                vec![stream_commit("a", 1..=5).with_snapshot(snapshot(1))],
                vec![]
            ),
            9,
            "create five events with a snapshot"
        );

        let appends = |count: usize| -> Vec<Commit<TransactWriteItem>> {
            (0..count)
                .map(|n| stream_commit(&format!("w{n:02}"), 2..=2))
                .collect()
        };
        let reads = |count: usize| -> Vec<(String, i64)> {
            (0..count).map(|n| (format!("r{n:02}"), 3)).collect()
        };
        let fenced = |count: usize| -> Vec<(&'static str, i64)> {
            reads(count)
                .into_iter()
                .map(|(id, head)| (&*Box::leak(id.into_boxed_str()), head))
                .collect()
        };
        assert_eq!(count(appends(10), fenced(5)), 27);
        assert_eq!(count(appends(12), fenced(9)), 35);
        assert_eq!(count(appends(1), fenced(30)), 34);
    }

    #[test]
    fn single_stream_capacity_holds_the_documented_numbers() {
        // Marker counted, outcome not: create 98, append 98, create with a snapshot 97, append
        // with a snapshot 97 event and projector operations.
        let fits = |first: i64, events: i64, snapshot: bool| {
            let mut commit = stream_commit("a", first..=first + events - 1);
            if snapshot {
                commit = commit.with_snapshot(crate::NewSnapshot::new(
                    "account",
                    EventSequence(if first == 1 { 1 } else { 2 }),
                    "{}",
                ));
            }
            let sizes = commit.sizes(|_| None).unwrap();

            crate::repository::check_budget(sizes).is_ok()
        };

        for (first, snapshot, capacity) in
            [(1, false, 98), (2, false, 98), (1, true, 97), (2, true, 97)]
        {
            assert!(
                fits(first, capacity, snapshot),
                "first {first}, snapshot {snapshot}"
            );
            assert!(
                !fits(first, capacity + 1, snapshot),
                "first {first}, snapshot {snapshot}: one more is too large"
            );
        }
    }

    #[test]
    fn the_layout_maps_every_index_back_to_its_stream_or_fence() {
        let repository = offline_repository();
        let mut commit = TransactionCommit::new(vec![
            stream_commit("b", 1..=2),
            stream_commit("a", 4..=4).with_snapshot(crate::NewSnapshot::new(
                "account",
                EventSequence(1),
                "{}",
            )),
        ]);
        commit.fences = vec![crate::ReadFence::new(
            "f",
            "account",
            EventSequence(0),
            crate::FenceProvenance::Caller,
        )];
        commit.normalize().unwrap();

        let (items, layout) = repository
            .transaction_stream_items(&commit, ValidatedMarker::Absent)
            .unwrap();

        // marker, a: head + event + snapshot, b: head + 2 events, fence f
        assert_eq!(items.len(), 1 + 3 + 3 + 1);
        let [a, b] = &layout.streams[..] else {
            panic!("two written streams")
        };
        assert_eq!((a.head, a.events.clone(), a.snapshot), (1, 2..3, Some(3)));
        assert_eq!((b.head, b.events.clone(), b.snapshot), (4, 5..7, None));
        assert_eq!(a.first, EventSequence(4));
        assert_eq!(layout.fences[0].0, 7);

        for (index, want) in [(1, "a"), (3, "a"), (4, "b"), (6, "b"), (7, "f")] {
            assert_eq!(
                layout.entity_at(index),
                Some(EntityId::new(want)),
                "{index}"
            );
        }
        assert_eq!(
            layout.entity_at(0),
            None,
            "the marker check belongs to no stream"
        );
    }

    #[test]
    fn a_negative_sort_key_is_never_an_event() {
        let head = wire::head_item(&EntityId::new("a"), EventSequence(3), Some(1_100));

        assert_eq!(wire::sort_key(&head), Some(-1));
        assert!(!wire::is_snapshot_item(&head));
        assert_eq!(
            wire::head_from_item(&head).unwrap(),
            (EventSequence(3), Some(1_100))
        );
    }
    // ----- commit-wide view operations (v0.9, C7) -----

    #[derive(Debug)]
    struct Taken;

    impl std::fmt::Display for Taken {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("name taken")
        }
    }

    impl std::error::Error for Taken {}

    fn view(projector: (&str, usize), operation: usize, op: crate::ViewOperation) -> ProjectedView {
        ProjectedView::new(projector.0, projector.1, operation, op)
    }

    fn legacy_put(pk: &str) -> TransactWriteItem {
        put(
            "legacy",
            Item::from([
                ("pk".into(), AttributeValue::S(pk.into())),
                ("sk".into(), AttributeValue::N("0".into())),
            ]),
            None,
            &[],
        )
        .unwrap()
    }

    fn views_repository() -> DynamoRepository {
        offline_repository().with_views_table(VIEWS_TABLE)
    }

    /// The commit the layout and classification tests share: a legacy operation, two projectors'
    /// view operations (the second projector's second operation maps its failure), and two
    /// written streams, `a` with a snapshot.
    fn view_commit() -> TransactionCommit<TransactWriteItem> {
        use crate::ViewOperation as Op;

        let mut commit = TransactionCommit::new(vec![
            stream_commit("b", 1..=1),
            stream_commit("a", 1..=1).with_snapshot(crate::NewSnapshot::new(
                "account",
                EventSequence(1),
                b"{}".to_vec(),
            )),
        ])
        .with_operations([legacy_put("legacy-1")])
        .with_views([
            view(("first", 0), 0, Op::put("p", "one", "o", "", "{}")),
            view(("first", 0), 1, Op::delete("p", "two", "o").if_present()),
            view(
                ("second", 1),
                0,
                Op::put("q", "one", "id", "type", "{}").if_absent(),
            ),
            view(
                ("second", 1),
                1,
                Op::put("q", "two", "o", "", "{}")
                    .if_absent()
                    .on_conflict(Taken),
            ),
        ]);
        commit.normalize().unwrap();

        commit
    }

    #[test]
    fn view_actions_follow_the_marker_and_legacy_operations_and_precede_every_stream() {
        use serde_json::json;

        let repository = views_repository();
        let commit = view_commit();
        let (items, layout) = repository
            .transaction_stream_items(&commit, ValidatedMarker::Version(3))
            .unwrap();
        let actions: Vec<_> = items.iter().map(action_summary).collect();

        let shape: Vec<_> = actions
            .iter()
            .map(|action| {
                (
                    action["operation"].as_str().unwrap().to_owned(),
                    action["pk"].as_str().unwrap().to_owned(),
                    action["sk"].as_i64().unwrap(),
                    action["skText"].as_str().map(str::to_owned),
                    action["condition"].as_str().map(str::to_owned),
                )
            })
            .collect();
        let owned = |text: &str| Some(text.to_owned());
        let expected = [
            (
                "ConditionCheck",
                "SYSTEM#FEATURES",
                0,
                None,
                owned("attribute_not_exists(pk) OR version = :v"),
            ),
            // The legacy backend operation, then the views in declaration then emission order.
            ("Put", "legacy-1", 0, None, None),
            (
                "Put",
                "p",
                0,
                owned("one"),
                owned("attribute_not_exists(pk) OR entityID = :owner"),
            ),
            (
                "Delete",
                "p",
                0,
                owned("two"),
                owned("attribute_exists(pk) AND entityID = :owner"),
            ),
            (
                "Put",
                "q",
                0,
                owned("one"),
                owned("attribute_not_exists(pk)"),
            ),
            (
                "Put",
                "q",
                0,
                owned("two"),
                owned("attribute_not_exists(pk)"),
            ),
            // Then each written stream's group in ascending stream ID bytes: `a` with its snapshot
            // put last, then `b`.
            ("Update", "a", -1, None, owned("attribute_not_exists(head)")),
            ("Put", "a", 1, None, owned("attribute_not_exists(sk)")),
            ("Put", "a", 0, None, owned("attribute_not_exists(seq)")),
            ("Update", "b", -1, None, owned("attribute_not_exists(head)")),
            ("Put", "b", 1, None, owned("attribute_not_exists(sk)")),
        ]
        .map(|(operation, pk, sk, sk_text, condition)| {
            (operation.to_owned(), pk.to_owned(), sk, sk_text, condition)
        });
        assert_eq!(shape, expected);

        // Tables: the legacy operation names its own, the views the views table, the rest the log.
        let tables: Vec<_> = items
            .iter()
            .map(|item| {
                item.put()
                    .map(|put| put.table_name())
                    .or_else(|| item.delete().map(|delete| delete.table_name()))
                    .or_else(|| item.update().map(|update| update.table_name()))
                    .or_else(|| item.condition_check().map(|check| check.table_name()))
                    .unwrap()
            })
            .collect();
        assert_eq!(
            tables,
            [
                "events", "legacy", "views", "views", "views", "views", "events", "events",
                "events", "events", "events"
            ]
        );

        // The item shapes: a put writes all five attributes as strings; a delete is a key.
        let put = items[4].put().unwrap();
        assert_eq!(
            json!(from_sdk_item(put.item())),
            json!({"pk": {"S": "q"}, "sk": {"S": "one"}, "entityID": {"S": "id"},
                   "entityType": {"S": "type"}, "payload": {"S": "{}"}})
        );
        assert_eq!(items[3].delete().unwrap().key().len(), 2);

        // Every write binds its owner, except a put that requires the row to be absent. The
        // owner is the stored `entityID`, and the stream the operation is owned by.
        let owner = |item: &TransactWriteItem| {
            let values = item
                .put()
                .map(|put| put.expression_attribute_values())
                .or_else(|| {
                    item.delete()
                        .map(|delete| delete.expression_attribute_values())
                })
                .unwrap();

            values.map(|values| values.keys().cloned().collect::<Vec<_>>())
        };
        assert_eq!(
            owner(&items[2]),
            Some(vec![":owner".to_owned()]),
            "an unconditional put"
        );
        assert_eq!(
            owner(&items[3]),
            Some(vec![":owner".to_owned()]),
            "a present delete"
        );
        assert_eq!(owner(&items[4]), None, "an absent put binds no owner");
        assert_eq!(
            items[3]
                .delete()
                .unwrap()
                .expression_attribute_values()
                .unwrap()[":owner"],
            s("o")
        );

        // The layout maps each view to its index, with its provenance.
        assert_eq!(layout.operations, 1);
        let indexes: Vec<_> = layout
            .views
            .iter()
            .map(|(index, view)| (*index, view.projector.clone(), view.operation))
            .collect();
        assert_eq!(
            indexes,
            [
                (2, "first".to_owned(), 0),
                (3, "first".to_owned(), 1),
                (4, "second".to_owned(), 0),
                (5, "second".to_owned(), 1)
            ]
        );
        assert_eq!(layout.projector_items(), 5);
        assert_eq!(
            layout.streams[0].head, 6,
            "the first stream group follows the views"
        );
    }

    #[test]
    fn a_single_stream_commit_places_views_after_the_legacy_operations() {
        let repository = views_repository();
        let commit = stream_commit("a", 1..=1)
            .with_operations([legacy_put("legacy-1")])
            .with_views([view(
                ("only", 0),
                0,
                crate::ViewOperation::put("p", "s", "o", "", "{}"),
            )]);
        commit.validate().unwrap();

        let (items, layout) = repository
            .transaction_items(&commit, ValidatedMarker::Version(1))
            .unwrap();
        let actions: Vec<_> = items.iter().map(action_summary).collect();

        let operations: Vec<_> = actions
            .iter()
            .map(|action| {
                (
                    action["operation"].as_str().unwrap(),
                    action["pk"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            operations,
            [
                ("ConditionCheck", "SYSTEM#FEATURES"),
                ("Put", "legacy-1"),
                ("Put", "p"),
                ("Update", "a"),
                ("Put", "a")
            ]
        );
        assert_eq!((layout.operations, layout.views.len()), (1, 1));
    }

    #[test]
    fn view_operation_sizes_equal_the_size_of_the_action_built() {
        use crate::ViewOperation as Op;

        let repository = views_repository();
        let operations = [
            Op::put("p", "s", "o", "", ""),
            Op::put("pk", "sk", "entity", "type", "{\"a\":\"é\"}").if_absent(),
            Op::put("é", "ß", "é", "ß", "é").on_conflict(Taken),
            Op::delete("p", "s", "o"),
            Op::delete("pk", "sk", "o").if_present(),
        ];
        let views: Vec<_> = operations
            .iter()
            .enumerate()
            .map(|(index, op)| view(("p", 0), index, op.clone()))
            .collect();

        let built = repository.view_items(&views).unwrap();

        for (op, item) in operations.iter().zip(&built) {
            assert_eq!(
                sdk_item_size(item),
                crate::view_operation_size(op),
                "{op:?}"
            );
        }
    }

    #[tokio::test]
    async fn views_without_a_views_table_are_refused_before_any_request() {
        let repository = offline_repository();
        let views = || {
            [view(
                ("p", 0),
                0,
                crate::ViewOperation::put("p", "s", "o", "", "{}"),
            )]
        };

        let single = stream_commit("a", 1..=1).with_views(views());
        let multi = TransactionCommit::new(vec![stream_commit("a", 1..=1)]).with_views(views());

        for (entry_point, result) in [
            ("commit", repository.commit(single).await),
            (
                "commit_transaction",
                repository.commit_transaction(multi).await,
            ),
        ] {
            assert!(
                matches!(
                    result,
                    Err(Error::ViewsNotConfigured {
                        backend: "dynamodb"
                    })
                ),
                "{entry_point}: {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn view_operations_are_checked_before_any_request() {
        use crate::ViewOperation as Op;

        let repository = views_repository();
        let bad = |op: Op| [view(("p", 0), 0, op)];

        for views in [
            bad(Op::put("", "s", "o", "", "{}")),
            bad(Op::put("p".repeat(2049), "s", "o", "", "{}")),
            bad(Op::delete("p", "s".repeat(1025), "o")),
        ] {
            let single = stream_commit("a", 1..=1).with_views(views.clone());
            let multi = TransactionCommit::new(vec![stream_commit("a", 1..=1)]).with_views(views);

            let (one, two) = (
                repository.commit(single).await,
                repository.commit_transaction(multi).await,
            );
            assert!(
                matches!(
                    one,
                    Err(Error::InvalidViewOperation(_) | Error::ViewKeyTooLong { .. })
                ),
                "{one:?}"
            );
            assert!(
                matches!(
                    two,
                    Err(Error::InvalidViewOperation(_) | Error::ViewKeyTooLong { .. })
                ),
                "{two:?}"
            );
        }
    }

    /// Views are budgeted before the first request: the offline client could not have served one.
    #[tokio::test]
    async fn view_operations_count_against_the_budget_before_any_request() {
        use crate::ViewOperation as Op;

        let repository = views_repository();
        let bulk = |count: usize, payload: usize| -> Vec<ProjectedView> {
            (0..count)
                .map(|index| {
                    view(
                        ("bulk", 0),
                        index,
                        Op::put("p", format!("row-{index:03}"), "o", "", "x".repeat(payload)),
                    )
                })
                .collect()
        };
        let too_large = |result: Result<(), Error>| match result {
            Err(Error::CommitTooLarge {
                actions,
                bytes,
                largest_item,
            }) => (actions, bytes, largest_item),
            other => panic!("expected CommitTooLarge, got {other:?}"),
        };
        let commit = |views| stream_commit("a", 1..=1).with_views(views);
        let transaction = |views| {
            TransactionCommit::new(vec![stream_commit("a", 1..=1), stream_commit("b", 1..=1)])
                .with_views(views)
        };

        // Marker + head update + one event leave 97 actions for views.
        assert_eq!(
            too_large(repository.commit(commit(bulk(98, 0))).await).0,
            101
        );
        assert!(!matches!(
            repository.commit(commit(bulk(97, 0))).await,
            Err(Error::CommitTooLarge { .. })
        ));
        // Two streams cost two head updates and two events: 95 fit, 96 do not.
        assert_eq!(
            too_large(
                repository
                    .commit_transaction(transaction(bulk(96, 0)))
                    .await
            )
            .0,
            101
        );
        assert!(!matches!(
            repository
                .commit_transaction(transaction(bulk(95, 0)))
                .await,
            Err(Error::CommitTooLarge { .. })
        ));

        // Bytes: twelve 330,000-byte puts are each under 380,000 and together over 3,800,000.
        let (actions, bytes, largest) =
            too_large(repository.commit(commit(bulk(12, 330_000))).await);
        assert!(
            actions == 15 && bytes > 3_800_000 && largest < 380_000,
            "{actions} {bytes} {largest}"
        );
        let (_, _, largest) = too_large(repository.commit(commit(bulk(1, 400_000))).await);
        assert!(largest > 380_000);
        // The same bytes in a delete's condition would be negligible: deletes carry only keys.
        let deletes: Vec<_> = (0..97)
            .map(|index| {
                view(
                    ("d", 0),
                    index,
                    Op::delete("p", format!("{index:03}"), "o").if_present(),
                )
            })
            .collect();
        assert!(!matches!(
            repository.commit(commit(deletes)).await,
            Err(Error::CommitTooLarge { .. })
        ));
    }

    // ----- failure classification of projector operations (rank 6) -----

    fn cancelled(codes: &[Option<&str>]) -> SdkError<TransactWriteItemsError, ()> {
        use aws_sdk_dynamodb::types::CancellationReason;
        use aws_sdk_dynamodb::types::error::TransactionCanceledException;

        let mut exception = TransactionCanceledException::builder().message("cancelled");
        for code in codes {
            exception = exception.cancellation_reasons(
                CancellationReason::builder()
                    .set_code(code.map(str::to_owned))
                    .build(),
            );
        }

        SdkError::service_error(
            TransactWriteItemsError::TransactionCanceledException(exception.build()),
            (),
        )
    }

    /// The reason codes of `view_commit()`'s transaction: `None` everywhere except the given
    /// `(index, code)` entries. Items: 0 marker, 1 legacy, 2 to 5 views, 6 to 8 stream `a`'s head,
    /// event, and snapshot, 9 and 10 stream `b`'s head and event.
    fn reasons(failed: &[(usize, &'static str)]) -> Vec<Option<&'static str>> {
        let mut codes = vec![Some("None"); 11];
        for (index, code) in failed {
            codes[*index] = Some(code);
        }

        codes
    }

    async fn classified(failed: &[(usize, &'static str)]) -> Error {
        let repository = views_repository();
        let commit = view_commit();
        let (_, layout) = repository
            .transaction_stream_items(&commit, ValidatedMarker::Version(3))
            .unwrap();
        assert_eq!(layout.streams.len(), 2);

        repository
            .classify_commit_failure(&layout, false, cancelled(&reasons(failed)))
            .await
    }

    fn operation_failure(error: Error) -> Box<ProjectorOperationError> {
        match error {
            Error::ProjectorOperation(failed) => failed,
            other => panic!("expected a projector operation failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_first_failed_view_operation_in_transaction_order_is_named() {
        // Operation 1 of the second projector (item 5) carries a mapped error.
        let failed = operation_failure(classified(&[(5, "ConditionalCheckFailed")]).await);
        assert_eq!(
            (
                failed.projector.as_str(),
                failed.projector_index,
                failed.operation
            ),
            ("second", 1, 1)
        );
        assert_eq!(failed.reason, "ConditionalCheckFailed");
        assert_eq!(failed.pk, "q");
        assert_eq!(failed.sk, "two");
        assert_eq!(failed.mapped().unwrap().to_string(), "name taken");
        assert!(
            failed.cause().is_none(),
            "a condition failure has no backend cause"
        );

        // Several failures: the earliest item wins, whatever its reason.
        let failed = operation_failure(
            classified(&[
                (5, "ConditionalCheckFailed"),
                (3, "ValidationError"),
                (4, "ConditionalCheckFailed"),
            ])
            .await,
        );
        assert_eq!(
            (failed.projector.as_str(), failed.operation, failed.kind),
            ("first", 1, crate::ViewOperationKind::Delete)
        );
        assert_eq!(failed.reason, "ValidationError");
        assert!(failed.mapped().is_none());
        assert!(
            failed.cause().is_some(),
            "the SDK error is the cause of a validation failure"
        );

        let failed = operation_failure(classified(&[(2, "ItemCollectionSizeLimitExceeded")]).await);
        assert_eq!(failed.reason, "ItemCollectionSizeLimitExceeded");

        for error in [
            classified(&[(5, "ConditionalCheckFailed")]).await,
            classified(&[(2, "ValidationError")]).await,
        ] {
            assert!(!error.is_retryable() && !error.is_conflict(), "{error:?}");
        }
    }

    #[tokio::test]
    async fn a_legacy_operation_failure_keeps_its_legacy_error_and_precedes_the_views() {
        let error =
            classified(&[(1, "ConditionalCheckFailed"), (2, "ConditionalCheckFailed")]).await;

        assert!(matches!(error, Error::Projector(_)), "{error:?}");
    }

    #[tokio::test]
    async fn a_projector_failure_is_never_reported_when_a_higher_ranked_item_failed() {
        // Stream `a`'s snapshot put (item 8) failing is an ordinary conflict, however many view
        // operations failed with it.
        let error = classified(&[
            (8, "ConditionalCheckFailed"),
            (2, "ConditionalCheckFailed"),
            (5, "ConditionalCheckFailed"),
        ])
        .await;

        assert!(error.is_conflict(), "{error:?}");
        assert!(!matches!(error, Error::ProjectorOperation(_)));
    }

    #[tokio::test]
    async fn a_transaction_conflict_or_throttling_on_a_view_item_keeps_its_classification() {
        let error = classified(&[(3, "TransactionConflict")]).await;
        assert!(error.is_conflict() && error.is_retryable(), "{error:?}");

        let error = classified(&[(3, "ThrottlingError")]).await;
        assert!(matches!(error, Error::Transient(_)), "{error:?}");

        // A condition failure elsewhere in the same transaction still names that operation.
        let failed = operation_failure(
            classified(&[(3, "TransactionConflict"), (4, "ConditionalCheckFailed")]).await,
        );
        assert_eq!((failed.projector.as_str(), failed.operation), ("second", 0));
    }

    #[test]
    fn the_cancellation_classifier_counts_legacy_and_view_items_as_projector_items() {
        const CHECK: Option<&str> = Some("ConditionalCheckFailed");
        const NONE: Option<&str> = Some("None");

        // Marker, one legacy operation, two views: four projector items after the marker would be
        // indexes 1 to 3; item 4 is the first stream item.
        let cases = [
            (
                vec![NONE, NONE, NONE, CHECK, NONE],
                Some(Cancellation::ProjectorCondition),
            ),
            (
                vec![NONE, NONE, NONE, NONE, CHECK],
                Some(Cancellation::StreamCondition),
            ),
            (
                vec![CHECK, NONE, NONE, CHECK, CHECK],
                Some(Cancellation::Marker),
            ),
        ];

        for (codes, want) in cases {
            assert_eq!(classify_cancellation(3, &codes), want, "{codes:?}");
        }
    }

    #[tokio::test]
    async fn every_write_entry_point_refuses_views_with_no_event() {
        let repository = views_repository();
        let views = || {
            [view(
                ("orphan", 0),
                0,
                crate::ViewOperation::put("p", "s", "o", "", "{}"),
            )]
        };
        let fence = || {
            crate::ReadFence::new(
                "a",
                "account",
                EventSequence(0),
                crate::FenceProvenance::Engine,
            )
        };

        let results = [
            (
                "commit",
                repository
                    .commit(Commit::new("a", Vec::new()).with_views(views()))
                    .await,
            ),
            (
                "commit_transaction",
                repository
                    .commit_transaction(TransactionCommit::new(Vec::new()).with_views(views()))
                    .await,
            ),
            (
                "commit_transaction with a fence",
                repository
                    .commit_transaction(
                        TransactionCommit::new(Vec::new())
                            .with_fences([fence()])
                            .with_views(views()),
                    )
                    .await,
            ),
            (
                "an eventless stream's own views",
                repository
                    .commit_transaction(TransactionCommit::new(vec![
                        Commit::new("a", Vec::new()).with_views(views()),
                    ]))
                    .await,
            ),
        ];

        for (entry_point, result) in results {
            assert!(
                matches!(result, Err(Error::ProjectorWithoutEvents { operations: 1 })),
                "{entry_point}: {result:?}"
            );
        }
    }
}
