//! [`RebuildFence`] for DynamoDB: the lock and generation items of the rebuild fence (C10), with
//! the exact requests the storage spec pins ("Rebuild Fence", "Request shapes", "Rebuild view
//! writes"). Every expression reaches `owner` and `value`, which are reserved words, and the lock's
//! `generation` (its fencing token), through the
//! `#owner` and `#value` aliases.

use std::time::Duration;

use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::{ConditionCheck, Delete, Put, TransactWriteItem, Update};

use super::{
    DynamoRepository, cancellation_reasons, failed_items, from_sdk_item, n, s, sdk, sdk_error,
    to_sdk_item,
};
use crate::error::{BoxError, Error};
use crate::projection::{ProjectedView, ProjectorOperationError};
use crate::rebuild::{
    self, MAX_REBUILD_BEGIN_ATTEMPTS, Plan, PlanKind, RebuildFence, RebuildGeneration,
    RebuildLease, RebuildLock, RebuildState,
};
use crate::wire::dynamo::{self as wire, Item};

/// The SDK's error for a fence transaction.
type TransactError =
    SdkError<TransactWriteItemsError, aws_sdk_dynamodb::config::http::HttpResponse>;

/// Which release a release transaction is.
#[derive(Clone, Copy)]
pub(super) enum Release {
    /// `end_rebuild`: the generation settles and the force-release flag is cleared.
    End,
    /// `force_release_rebuild`: the generation stays odd and is flagged.
    Force,
}

impl DynamoRepository {
    /// Reads one fence item with a strongly consistent read, whatever the repository's
    /// read-consistency setting: two eventually consistent reads can return the same stale settled
    /// generation while a rebuild holds the lock.
    async fn rebuild_item(&self, key: Item) -> Result<Option<Item>, Error> {
        let output = self
            .client
            .get_item()
            .table_name(&self.table)
            .consistent_read(true)
            .set_key(Some(to_sdk_item(&key)))
            .send()
            .await
            .map_err(sdk_error)?;

        Ok(output
            .item
            .filter(|item| !item.is_empty())
            .map(|item| from_sdk_item(&item)))
    }

    /// The acquire's or the takeover's two actions: the lock put and the generation update.
    /// The lock item stores `lease`'s generation, the one the transaction produces, as its fencing
    /// token.
    pub(super) fn rebuild_begin_items(
        &self,
        plan: &Plan,
        lease: &RebuildLease,
        now: i64,
    ) -> Result<Vec<TransactWriteItem>, Error> {
        let lock = Put::builder()
            .table_name(&self.table)
            .set_item(Some(to_sdk_item(&wire::rebuild_lock_item(&RebuildLock {
                owner: lease.owner.clone(),
                generation: lease.generation,
                lease_expires_at: lease.lease_expires_at,
            }))));
        let generation = Update::builder()
            .table_name(&self.table)
            .set_key(Some(to_sdk_item(&wire::rebuild_generation_key())))
            .expression_attribute_names(wire::REBUILD_VALUE_NAME, "value");

        let (lock, generation) = if plan.kind == PlanKind::Takeover {
            let observed = plan.observed_owner.as_deref().unwrap_or_default();

            (
                lock.condition_expression(wire::REBUILD_TAKEOVER_LOCK_CONDITION)
                    .expression_attribute_names(wire::REBUILD_OWNER_NAME, "owner")
                    .expression_attribute_values(":now", n(now))
                    .expression_attribute_values(":observedOwner", s(observed)),
                generation
                    .update_expression(wire::REBUILD_TAKEOVER_UPDATE)
                    .condition_expression(wire::REBUILD_SEEN_ODD_CONDITION)
                    .expression_attribute_values(":two", n(2))
                    .expression_attribute_values(":seenOdd", n(plan.seen_odd)),
            )
        } else {
            (
                lock.condition_expression(wire::REBUILD_ACQUIRE_LOCK_CONDITION),
                generation
                    .update_expression(wire::REBUILD_ACQUIRE_UPDATE)
                    .condition_expression(wire::REBUILD_ACQUIRE_CONDITION)
                    .expression_attribute_values(":zero", n(0))
                    .expression_attribute_values(":increment", n(plan.increment))
                    .expression_attribute_values(":seenEven", n(plan.seen_even))
                    .expression_attribute_values(":seenOdd", n(plan.seen_odd))
                    .expression_attribute_values(":true", sdk::AttributeValue::Bool(true)),
            )
        };

        Ok(vec![
            TransactWriteItem::builder()
                .put(lock.build().map_err(Error::backend)?)
                .build(),
            TransactWriteItem::builder()
                .update(generation.build().map_err(Error::backend)?)
                .build(),
        ])
    }

    /// The renew's one `UpdateItem` on the lock item, as an [`Update`] (its fields are sent as an
    /// `UpdateItem` request).
    pub(super) fn rebuild_renew_update(
        &self,
        lease: &RebuildLease,
        lease_expires_at: i64,
    ) -> Result<Update, Error> {
        Update::builder()
            .table_name(&self.table)
            .set_key(Some(to_sdk_item(&wire::rebuild_lock_key())))
            .update_expression(wire::REBUILD_RENEW_UPDATE)
            .condition_expression(wire::REBUILD_RENEW_CONDITION)
            .expression_attribute_names(wire::REBUILD_OWNER_NAME, "owner")
            .expression_attribute_names(wire::REBUILD_GENERATION_NAME, "generation")
            .expression_attribute_values(":expiry", n(lease_expires_at))
            .expression_attribute_values(":owner", s(&lease.owner))
            .expression_attribute_values(":generation", n(lease.generation))
            .build()
            .map_err(Error::backend)
    }

    /// A release's or a force-release's two actions: the lock delete (the lease's owner and
    /// fencing token for a release, the named owner for a force-release) and the bump of the odd
    /// generation, `seen_odd` (the lease's for a release, the one read for a force-release).
    pub(super) fn rebuild_release_items(
        &self,
        release: Release,
        owner: &str,
        seen_odd: i64,
    ) -> Result<Vec<TransactWriteItem>, Error> {
        let (condition, placeholder, update) = match release {
            Release::End => (
                wire::REBUILD_RELEASE_CONDITION,
                ":me",
                wire::REBUILD_RELEASE_UPDATE,
            ),
            Release::Force => (
                wire::REBUILD_FORCE_RELEASE_CONDITION,
                ":requestedOwner",
                wire::REBUILD_FORCE_RELEASE_UPDATE,
            ),
        };

        let mut delete = Delete::builder()
            .table_name(&self.table)
            .set_key(Some(to_sdk_item(&wire::rebuild_lock_key())))
            .condition_expression(condition)
            .expression_attribute_names(wire::REBUILD_OWNER_NAME, "owner")
            .expression_attribute_values(placeholder, s(owner));
        if let Release::End = release {
            // The lease's fencing token is the odd generation it holds the lock at.
            delete = delete
                .expression_attribute_names(wire::REBUILD_GENERATION_NAME, "generation")
                .expression_attribute_values(":generation", n(seen_odd));
        }
        let delete = delete.build().map_err(Error::backend)?;

        let mut generation = Update::builder()
            .table_name(&self.table)
            .set_key(Some(to_sdk_item(&wire::rebuild_generation_key())))
            .update_expression(update)
            .condition_expression(wire::REBUILD_SEEN_ODD_CONDITION)
            .expression_attribute_names(wire::REBUILD_VALUE_NAME, "value")
            .expression_attribute_values(":seenOdd", n(seen_odd));
        generation = match release {
            Release::End => generation.expression_attribute_values(":one", n(1)),
            Release::Force => generation
                .expression_attribute_values(":two", n(2))
                .expression_attribute_values(":true", sdk::AttributeValue::Bool(true)),
        };

        Ok(vec![
            TransactWriteItem::builder().delete(delete).build(),
            TransactWriteItem::builder()
                .update(generation.build().map_err(Error::backend)?)
                .build(),
        ])
    }

    /// The guard of a rebuild view write, its last action: the caller's lease, by owner and
    /// fencing token, still holds the lock unexpired.
    pub(super) fn rebuild_guard(
        &self,
        lease: &RebuildLease,
        now: i64,
    ) -> Result<TransactWriteItem, Error> {
        let check = ConditionCheck::builder()
            .table_name(&self.table)
            .set_key(Some(to_sdk_item(&wire::rebuild_lock_key())))
            .condition_expression(wire::REBUILD_GUARD_CONDITION)
            .expression_attribute_names(wire::REBUILD_OWNER_NAME, "owner")
            .expression_attribute_names(wire::REBUILD_GENERATION_NAME, "generation")
            .expression_attribute_values(":me", s(&lease.owner))
            .expression_attribute_values(":generation", n(lease.generation))
            .expression_attribute_values(":now", n(now))
            .build()
            .map_err(Error::backend)?;

        Ok(TransactWriteItem::builder().condition_check(check).build())
    }

    /// The items of one guarded rebuild write: the view operations, then the guard.
    pub(super) fn rebuild_view_items(
        &self,
        lease: &RebuildLease,
        views: &[ProjectedView],
        now: i64,
    ) -> Result<Vec<TransactWriteItem>, Error> {
        let mut items = self.view_items(views)?;
        items.push(self.rebuild_guard(lease, now)?);

        Ok(items)
    }

    /// Sends one fence transaction. The SDK's error is boxed: it is large, and only the failure
    /// path looks at it.
    async fn transact(&self, items: Vec<TransactWriteItem>) -> Result<(), Box<TransactError>> {
        self.client
            .transact_write_items()
            .set_transact_items(Some(items))
            .send()
            .await
            .map(|_| ())
            .map_err(Box::new)
    }

    /// A release or a force-release of the odd generation `seen`: deletes the lock row and bumps
    /// the generation in one transaction.
    async fn release_rebuild(
        &self,
        owner: &str,
        release: Release,
        seen: i64,
    ) -> Result<RebuildGeneration, Error> {
        let items = self.rebuild_release_items(release, owner, seen)?;
        if let Err(err) = self.transact(items).await {
            // Only a takeover or a force-release changes the generation of a held lock, and both
            // replace or remove the lock row: a failed condition means the caller lost it.
            if failed_items(&cancellation_reasons(&*err), "ConditionalCheckFailed")
                .next()
                .is_some()
            {
                return Err(lease_lost(owner));
            }

            return Err(fence_error(*err));
        }

        Ok(match release {
            Release::End => RebuildGeneration {
                value: seen + 1,
                force_released: false,
            },
            Release::Force => RebuildGeneration {
                value: seen + 2,
                force_released: true,
            },
        })
    }
}

impl RebuildFence for DynamoRepository {
    async fn rebuild_generation(&self) -> Result<RebuildGeneration, Error> {
        self.rebuild_item(wire::rebuild_generation_key())
            .await?
            .map(|item| wire::rebuild_generation_from_item(&item))
            .transpose()
            .map(Option::unwrap_or_default)
    }

    async fn rebuild_state(&self) -> Result<RebuildState, Error> {
        let generation = self.rebuild_generation().await?;
        let lock = self
            .rebuild_item(wire::rebuild_lock_key())
            .await?
            .map(|item| wire::rebuild_lock_from_item(&item))
            .transpose()?;

        Ok(RebuildState { generation, lock })
    }

    async fn begin_rebuild(&self, owner: &str, lease: Duration) -> Result<RebuildLease, Error> {
        let seconds = rebuild::check_lease(owner, lease)?;

        let mut invalid = None;

        for _ in 0..MAX_REBUILD_BEGIN_ATTEMPTS {
            let state = self.rebuild_state().await?;
            let now = self.now();
            let plan = match Plan::decide(&state, now) {
                Ok(plan) => plan,
                // The two items are read one after the other, so a transition that lands between
                // the reads shows a state no transaction produced. Read again; only a state that
                // persists through every attempt is reported.
                Err(error @ Error::RebuildStateInvalid(_)) => {
                    invalid = Some(error);

                    continue;
                }
                Err(error) => return Err(error),
            };
            invalid = None;
            let lease = RebuildLease {
                owner: owner.to_owned(),
                generation: state.generation.value + plan.increment,
                lease_expires_at: now.saturating_add(seconds),
            };

            let items = self.rebuild_begin_items(&plan, &lease, now)?;
            match self.transact(items).await {
                Ok(()) => return Ok(lease),
                // Another operation raced the read: read the state again and decide again.
                Err(err) if race_lost(&*err) => {}
                Err(err) => return Err(fence_error(*err)),
            }
        }

        Err(invalid.unwrap_or(Error::RebuildConflict {
            attempts: MAX_REBUILD_BEGIN_ATTEMPTS,
        }))
    }

    async fn renew_rebuild(
        &self,
        lease: &RebuildLease,
        extend: Duration,
    ) -> Result<RebuildLease, Error> {
        let seconds = rebuild::check_lease(&lease.owner, extend)?;
        let lease_expires_at = self.now().saturating_add(seconds);
        let update = self.rebuild_renew_update(lease, lease_expires_at)?;

        let result = self
            .client
            .update_item()
            .table_name(update.table_name())
            .set_key(Some(update.key().clone()))
            .update_expression(update.update_expression())
            .set_condition_expression(update.condition_expression().map(str::to_owned))
            .set_expression_attribute_names(update.expression_attribute_names().cloned())
            .set_expression_attribute_values(update.expression_attribute_values().cloned())
            .send()
            .await;

        match result {
            Ok(_) => Ok(RebuildLease {
                lease_expires_at,
                ..lease.clone()
            }),
            Err(SdkError::ServiceError(err))
                if matches!(
                    err.err(),
                    UpdateItemError::ConditionalCheckFailedException(_)
                ) =>
            {
                Err(lease_lost(&lease.owner))
            }
            Err(err) => Err(sdk_error(err)),
        }
    }

    /// One transaction, with no read first: the lease names the generation its lock holds.
    async fn end_rebuild(&self, lease: &RebuildLease) -> Result<RebuildGeneration, Error> {
        rebuild::check_owner(&lease.owner)?;

        self.release_rebuild(&lease.owner, Release::End, lease.generation)
            .await
    }

    /// Reads the generation, then runs the force-release transaction against it.
    async fn force_release_rebuild(&self, owner: &str) -> Result<RebuildGeneration, Error> {
        rebuild::check_owner(owner)?;

        // A settled generation means no rebuild holds the fence, so there is no lock to release.
        let seen = self.rebuild_generation().await?;
        if seen.is_settled() {
            return Err(lease_lost(owner));
        }

        self.release_rebuild(owner, Release::Force, seen.value)
            .await
    }

    async fn write_rebuild_views(
        &self,
        lease: &RebuildLease,
        views: &[ProjectedView],
    ) -> Result<(), Error> {
        let owner = lease.owner.as_str();
        rebuild::check_owner(owner)?;
        rebuild::check_view_batch(views)?;

        let items = self.rebuild_view_items(lease, views, self.now())?;
        let guard = items.len() - 1;

        let Err(err) = self.transact(items).await else {
            return Ok(());
        };

        let codes = cancellation_reasons(&*err);

        // The guard is the last action. A lost lease outranks a view condition: the worker must
        // stop whatever else failed.
        if codes.get(guard).copied().flatten() == Some("ConditionalCheckFailed") {
            return Err(lease_lost(owner));
        }

        // A view operation's own failure names it, as in a commit.
        let failed = views.iter().zip(&codes).find_map(|(view, code)| {
            let code = (*code)?;

            matches!(
                code,
                "ConditionalCheckFailed" | "ValidationError" | "ItemCollectionSizeLimitExceeded"
            )
            .then(|| (view, code.to_owned()))
        });
        if let Some((view, code)) = failed {
            let cause: Option<BoxError> =
                (code != crate::projection::REASON_CONDITION_FAILED).then_some(err as BoxError);

            return Err(Error::ProjectorOperation(Box::new(
                ProjectorOperationError::for_view(&view.provenance(), code, cause),
            )));
        }

        Err(fence_error(*err))
    }
}

fn lease_lost(owner: &str) -> Error {
    Error::RebuildLeaseLost {
        owner: owner.to_owned(),
    }
}

/// Reports whether a cancelled fence transaction lost to another operation: a failed condition,
/// or another transaction writing the same items.
fn race_lost<R>(err: &SdkError<TransactWriteItemsError, R>) -> bool
where
    R: std::fmt::Debug + Send + Sync + 'static,
{
    cancellation_reasons(err)
        .iter()
        .any(|code| matches!(code, Some("ConditionalCheckFailed" | "TransactionConflict")))
}

/// Maps a failed fence transaction: a concurrent transaction or throttling is transient (the
/// fence's transactions are conditional, so sending one again is safe), anything else as a read.
fn fence_error<R>(err: SdkError<TransactWriteItemsError, R>) -> Error
where
    R: std::fmt::Debug + Send + Sync + 'static,
{
    let transient = cancellation_reasons(&err).iter().any(|code| {
        matches!(
            code,
            Some("TransactionConflict" | "ThrottlingError" | "ProvisionedThroughputExceeded")
        )
    });

    if transient {
        return Error::Transient(Box::new(err));
    }

    sdk_error(err)
}
