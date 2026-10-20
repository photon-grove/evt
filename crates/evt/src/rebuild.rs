//! The durable rebuild fence (v0.9, C10).
//!
//! Two engine rows in the reserved `SYSTEM#` namespace fence projection rebuilds against live
//! writers and readers: a **lock row** ([`REBUILD_LOCK_ID`]) naming the rebuild that holds the
//! fence and when its lease lapses, and a **generation** counter ([`REBUILD_GENERATION_ID`]) that
//! is odd while a rebuild is in progress. Every commit carries a check that the generation is
//! settled, so no commit lands while any rebuild holds the lock, an expired one included, or after
//! a force-release until a rebuild ends ([`Error::RebuildInProgress`]); readers of derived state
//! fence their reads against the generation ([`RebuildEpoch`]). On DynamoDB the check is the
//! absence of the lock item, which a force-release keeps in place, so it costs no extra action.
//!
//! Every backend implements the lifecycle ([`RebuildFence`]): [`begin`](RebuildFence::begin_rebuild)
//! acquires the lock (or takes over an expired lease) and returns a [`RebuildLease`],
//! [`renew`](RebuildFence::renew_rebuild) extends it, [`end`](RebuildFence::end_rebuild) releases
//! it and settles the generation, and [`force_release`](RebuildFence::force_release_rebuild) is the
//! operator's escape hatch that ends a wedged worker's lease at once, leaving readers fenced and
//! commits refused until a new rebuild ends. A rebuild
//! worker writes its view rows through [`write_rebuild_views`](RebuildFence::write_rebuild_views),
//! whose every batch asserts the worker still holds an unexpired lease.
//!
//! A lease is bound to its owner token **and** its fencing token: the odd generation its begin
//! produced, which the lock row stores. Renew, end, and a guarded write name the lease, so a stale
//! incarnation of a worker that reuses its owner token after a takeover (or a later begin) can
//! never act on the new holder's lock: its generation no longer matches.
//!
//! ```
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! use std::time::Duration;
//!
//! use photon_evt::{MemRepository, RebuildFence, observe_rebuild_epoch};
//!
//! let repository = MemRepository::new();
//!
//! let lease = repository.begin_rebuild("rebuild-1", Duration::from_secs(60)).await.unwrap();
//! assert_eq!(lease.generation, 1, "odd while the rebuild holds the lock");
//! assert!(observe_rebuild_epoch(&repository).await.is_err(), "readers are fenced");
//!
//! let settled = repository.end_rebuild(&lease).await.unwrap();
//! assert!(settled.is_settled());
//! assert_eq!(observe_rebuild_epoch(&repository).await.unwrap().generation(), 2);
//! # });
//! ```
//!
//! See `docs/spec/storage-format.md` ("Rebuild Fence", "Rebuild lifecycle API", "Rebuild view
//! writes", "Rebuild epochs") for the stored shapes and the exact requests.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::error::Error;
use crate::projection::{ProjectedView, check_views, view_operation_size};

/// The partition key of the rebuild-lock row (sort key [`REBUILD_SORT_KEY`]).
pub const REBUILD_LOCK_ID: &str = "SYSTEM#REBUILD";

/// The partition key of the rebuild generation row (sort key [`REBUILD_SORT_KEY`]).
pub const REBUILD_GENERATION_ID: &str = "SYSTEM#REBUILD#GENERATION";

/// The sort key of both rebuild rows.
pub const REBUILD_SORT_KEY: i64 = 0;

/// The longest rebuild owner token, in UTF-8 bytes.
pub const MAX_REBUILD_OWNER_BYTES: usize = 512;

/// The most view operations one guarded rebuild write carries: on DynamoDB the 100th action of
/// the transaction is the lease guard.
pub const MAX_REBUILD_VIEW_BATCH: usize = 99;

/// How often [`RebuildFence::begin_rebuild`] reads the state and decides again after its
/// transaction lost a race, before it fails with [`Error::RebuildConflict`].
pub const MAX_REBUILD_BEGIN_ATTEMPTS: usize = 8;

/// The value an acquire binds for the branch of its generation condition that does not apply to
/// the observed state: no generation equals it.
pub(crate) const NO_MATCH: i64 = -1;

/// The rebuild generation: the monotonic counter's value (0 while the item does not exist) and
/// whether the last release was a force-release.
///
/// The value is odd if and only if a rebuild is in progress, with one exception: a force-release
/// releases the lock but leaves the value odd and sets `force_released`, which means "derived
/// state untrustworthy", not "rebuild in progress". Readers and commits treat every odd value
/// alike: a reader gets [`Error::RebuildUnavailable`] and a commit [`Error::RebuildInProgress`]
/// until a rebuild ends.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RebuildGeneration {
    /// The counter, bumped by every acquire, takeover, release, and force-release.
    pub value: i64,
    /// Set by a force-release, cleared by the next completed release.
    pub force_released: bool,
}

impl RebuildGeneration {
    /// Reports whether derived state can be trusted: the generation is even.
    pub fn is_settled(&self) -> bool {
        self.value % 2 == 0
    }
}

/// The rebuild-lock row: the holder's owner token, its fencing token, and the second its lease
/// lapses, in whole Unix seconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RebuildLock {
    /// The owner token of the rebuild that holds the lock.
    pub owner: String,
    /// The fencing token: the odd generation the acquire or takeover that wrote the lock produced.
    /// While the lock exists it equals the generation row's value.
    pub generation: i64,
    /// The lease expiry, in whole Unix seconds.
    pub lease_expires_at: i64,
}

impl RebuildLock {
    /// Reports whether the lease lapsed before `now` (whole Unix seconds). A lease is still held at
    /// the second it expires.
    pub fn is_expired(&self, now: i64) -> bool {
        self.lease_expires_at < now
    }
}

/// The generation and the lock row, read in that order. The two reads are not one snapshot: a
/// caller that must decide on the generation uses [`RebuildFence::rebuild_generation`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RebuildState {
    /// The generation.
    pub generation: RebuildGeneration,
    /// The lock row, `None` when no lock is held (none exists, or only the item a force-release
    /// leaves on DynamoDB).
    pub lock: Option<RebuildLock>,
}

impl RebuildState {
    /// Reports whether a rebuild holds the fence: the lock row exists, even when its lease expired.
    pub fn in_progress(&self) -> bool {
        self.lock.is_some()
    }
}

/// What [`RebuildFence::begin_rebuild`] returns: the owner, the odd generation the transaction
/// produced (the lease's fencing token), and the second the lease lapses. Renew, end, and a guarded
/// write take it, and act only while the lock still names both its owner and its generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RebuildLease {
    /// The owner token that holds the lock.
    pub owner: String,
    /// The fencing token: the generation after the acquire or takeover (odd).
    pub generation: i64,
    /// The lease expiry, in whole Unix seconds.
    pub lease_expires_at: i64,
}

impl RebuildLease {
    /// Reports whether `lock` is this lease's: it names the same owner and fencing token.
    pub fn holds(&self, lock: &RebuildLock) -> bool {
        lock.owner == self.owner && lock.generation == self.generation
    }
}

/// The lifecycle of the durable rebuild fence over one event log (v0.9, C10). The in-memory,
/// DynamoDB, and PostgreSQL repositories implement it over the same two rows, with the same
/// semantics, errors, and stored bytes as Go's `evt.RebuildFence`.
///
/// Leases are whole seconds read from the repository's clock (`with_clock` on each repository, for
/// tests). An owner token is a non-empty string of at most [`MAX_REBUILD_OWNER_BYTES`] bytes, and
/// a lease is at least one second, rounded up; either is refused with [`Error::InvalidArgument`]
/// before any request. The generation and lock reads are strongly consistent regardless of a
/// repository's read-consistency setting.
pub trait RebuildFence: Send + Sync {
    /// Reads the generation item alone (absent reads as 0): the reader primitive.
    fn rebuild_generation(&self) -> impl Future<Output = Result<RebuildGeneration, Error>> + Send;

    /// Reads the generation, then the lock row.
    fn rebuild_state(&self) -> impl Future<Output = Result<RebuildState, Error>> + Send;

    /// Acquires the lock from a settled or force-released generation, or takes over an expired
    /// lease, in one transaction that writes both rows. An unexpired lease is refused with
    /// [`Error::RebuildLockHeld`], and a state this protocol cannot produce with
    /// [`Error::RebuildStateInvalid`]. When the transaction loses a race it reads the state and
    /// decides again, [`MAX_REBUILD_BEGIN_ATTEMPTS`] times in all, and then fails with the
    /// transient [`Error::RebuildConflict`]. Where the two rows are read one after the other
    /// (DynamoDB), an inconsistent pair is read again the same way, and reported only when it
    /// persists through every attempt. The returned lease carries the odd generation the
    /// transaction produced: one more than a settled generation, two more after a force-release or
    /// for a takeover.
    fn begin_rebuild(
        &self,
        owner: &str,
        lease: Duration,
    ) -> impl Future<Output = Result<RebuildLease, Error>> + Send;

    /// Extends `lease` to `extend` from now and returns it with the new expiry. Fails with
    /// [`Error::RebuildLeaseLost`] when the lock row is gone or names another owner or fencing
    /// token; it does not require the lease to be unexpired.
    fn renew_rebuild(
        &self,
        lease: &RebuildLease,
        extend: Duration,
    ) -> impl Future<Output = Result<RebuildLease, Error>> + Send;

    /// Releases `lease`'s lock and restores the settled generation (one more than the lease's),
    /// clearing the force-release flag. Fails with [`Error::RebuildLeaseLost`] when the lock row
    /// is gone or names another owner or fencing token.
    fn end_rebuild(
        &self,
        lease: &RebuildLease,
    ) -> impl Future<Output = Result<RebuildGeneration, Error>> + Send;

    /// The operator's release of a named owner's lock, for a lost or wedged worker (it names the
    /// owner alone, not a lease, because the operator holds none): ends the lease at once, so a
    /// new rebuild can begin without waiting for it to lapse, but leaves the generation odd (two
    /// more) and flagged. The released rebuild may have left derived state half-written (wiped
    /// rows, cleared journal counters), so readers stay fenced and every commit is still refused
    /// with [`Error::RebuildInProgress`] until a later rebuild begins and ends. Fails with
    /// [`Error::RebuildLeaseLost`] when no lock is held or it names another owner. It never runs
    /// the rebuild's cleanup.
    fn force_release_rebuild(
        &self,
        owner: &str,
    ) -> impl Future<Output = Result<RebuildGeneration, Error>> + Send;

    /// Writes one batch of view operations (the same operations a commit's projectors produce)
    /// atomically with a guard that `lease` still holds the lock, by owner and fencing token, and
    /// has not lapsed. A failed guard writes nothing and is [`Error::RebuildLeaseLost`]: the worker
    /// lost its lock to a takeover or a force-release, or let its lease lapse, and must stop. An empty batch is
    /// [`Error::InvalidArgument`] and one over [`MAX_REBUILD_VIEW_BATCH`] operations (or the
    /// commit byte budget) [`Error::CommitTooLarge`], both before any write; a repository with
    /// nowhere to write views refuses with [`Error::ViewsNotConfigured`].
    fn write_rebuild_views(
        &self,
        lease: &RebuildLease,
        views: &[ProjectedView],
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

macro_rules! forward_rebuild_fence {
    ($($target:ty),+) => {$(
        impl<F: RebuildFence + ?Sized> RebuildFence for $target {
            fn rebuild_generation(
                &self,
            ) -> impl Future<Output = Result<RebuildGeneration, Error>> + Send {
                (**self).rebuild_generation()
            }

            fn rebuild_state(&self) -> impl Future<Output = Result<RebuildState, Error>> + Send {
                (**self).rebuild_state()
            }

            fn begin_rebuild(
                &self,
                owner: &str,
                lease: Duration,
            ) -> impl Future<Output = Result<RebuildLease, Error>> + Send {
                (**self).begin_rebuild(owner, lease)
            }

            fn renew_rebuild(
                &self,
                lease: &RebuildLease,
                extend: Duration,
            ) -> impl Future<Output = Result<RebuildLease, Error>> + Send {
                (**self).renew_rebuild(lease, extend)
            }

            fn end_rebuild(
                &self,
                lease: &RebuildLease,
            ) -> impl Future<Output = Result<RebuildGeneration, Error>> + Send {
                (**self).end_rebuild(lease)
            }

            fn force_release_rebuild(
                &self,
                owner: &str,
            ) -> impl Future<Output = Result<RebuildGeneration, Error>> + Send {
                (**self).force_release_rebuild(owner)
            }

            fn write_rebuild_views(
                &self,
                lease: &RebuildLease,
                views: &[ProjectedView],
            ) -> impl Future<Output = Result<(), Error>> + Send {
                (**self).write_rebuild_views(lease, views)
            }
        }
    )+};
}

forward_rebuild_fence!(&F, Arc<F>);

/// A settled generation that a read of derived state is made under (spec, "Rebuild epochs").
///
/// [`observe`](RebuildEpoch::observe) turns a generation into an epoch, or fails with
/// [`Error::RebuildUnavailable`] for an odd one; [`verify`](RebuildEpoch::verify) checks a later
/// generation against it. The async helpers [`observe_rebuild_epoch`],
/// [`verify_rebuild_epoch`], and [`read_at_rebuild_epoch`] read the generation through a
/// [`RebuildFence`].
///
/// A command handler that reads a derived view decides under an epoch and returns the error of a
/// failed observe or verify as its own: the engine returns it at once and records no outcome, so
/// a decision made on a half-rebuilt view is never recorded. Both errors are transient: wait and
/// read again once the rebuild is over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RebuildEpoch {
    generation: i64,
}

impl RebuildEpoch {
    /// The epoch of an observed generation: an odd generation (a rebuild in progress, or a
    /// force-release that left derived state untrustworthy) is [`Error::RebuildUnavailable`].
    pub fn observe(generation: RebuildGeneration) -> Result<Self, Error> {
        unavailable(generation)?;

        Ok(Self {
            generation: generation.value,
        })
    }

    /// Checks a generation read after the read of derived state: an odd one is
    /// [`Error::RebuildUnavailable`] and a settled one that differs from the epoch
    /// [`Error::RebuildEpochChanged`]. A settled generation equal to the epoch means no rebuild
    /// started, finished, or was taken over while the read ran.
    pub fn verify(&self, generation: RebuildGeneration) -> Result<(), Error> {
        unavailable(generation)?;

        if generation.value != self.generation {
            return Err(Error::RebuildEpochChanged {
                observed: self.generation,
                current: generation.value,
            });
        }

        Ok(())
    }

    /// The settled generation the epoch was observed at.
    pub fn generation(&self) -> i64 {
        self.generation
    }
}

fn unavailable(generation: RebuildGeneration) -> Result<(), Error> {
    if generation.is_settled() {
        return Ok(());
    }

    Err(Error::RebuildUnavailable {
        generation: generation.value,
        force_released: generation.force_released,
    })
}

/// Reads the generation and returns the epoch a read is made under, or
/// [`Error::RebuildUnavailable`] for an odd generation.
pub async fn observe_rebuild_epoch<F: RebuildFence + ?Sized>(
    fence: &F,
) -> Result<RebuildEpoch, Error> {
    RebuildEpoch::observe(fence.rebuild_generation().await?)
}

/// Reads the generation again and checks it against `epoch` ([`RebuildEpoch::verify`]).
pub async fn verify_rebuild_epoch<F: RebuildFence + ?Sized>(
    fence: &F,
    epoch: RebuildEpoch,
) -> Result<(), Error> {
    epoch.verify(fence.rebuild_generation().await?)
}

/// Runs `read` under the reader epoch check: observes the generation, runs the read, and verifies
/// the generation again. The read's result counts only when the verify passes. A failed observe
/// skips the read, and a failed read is returned as it is, without verifying.
pub async fn read_at_rebuild_epoch<F, T, E, Fut>(
    fence: &F,
    read: impl FnOnce(RebuildEpoch) -> Fut,
) -> Result<T, E>
where
    F: RebuildFence + ?Sized,
    E: From<Error>,
    Fut: Future<Output = Result<T, E>>,
{
    let epoch = observe_rebuild_epoch(fence).await?;
    let value = read(epoch).await?;

    verify_rebuild_epoch(fence, epoch).await?;

    Ok(value)
}

/// Refuses an owner token that is empty, longer than [`MAX_REBUILD_OWNER_BYTES`], or containing
/// U+0000, before any request. A `&str` is always valid UTF-8; PostgreSQL text cannot hold U+0000,
/// so one backend would refuse what another stores.
pub(crate) fn check_owner(owner: &str) -> Result<(), Error> {
    if owner.is_empty() {
        return Err(Error::InvalidArgument(
            "a rebuild owner must not be empty".into(),
        ));
    }

    if owner.len() > MAX_REBUILD_OWNER_BYTES {
        return Err(Error::InvalidArgument(format!(
            "a rebuild owner is at most {MAX_REBUILD_OWNER_BYTES} bytes, got {}",
            owner.len()
        )));
    }

    if owner.contains('\0') {
        return Err(Error::InvalidArgument(
            "a rebuild owner must not contain U+0000".into(),
        ));
    }

    Ok(())
}

/// A lease in whole seconds, rounded up. A lease below one second is refused.
pub(crate) fn lease_seconds(lease: Duration) -> Result<i64, Error> {
    if lease < Duration::from_secs(1) {
        return Err(Error::InvalidArgument(
            "a rebuild lease is at least one second".into(),
        ));
    }

    lease
        .as_secs()
        .checked_add(u64::from(lease.subsec_nanos() > 0))
        .and_then(|seconds| i64::try_from(seconds).ok())
        .ok_or_else(|| Error::InvalidArgument("the rebuild lease is too long".into()))
}

/// Checks an owner and a lease together, for `begin` and `renew`.
pub(crate) fn check_lease(owner: &str, lease: Duration) -> Result<i64, Error> {
    check_owner(owner)?;

    lease_seconds(lease)
}

/// What `begin` does for an observed state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlanKind {
    /// Take a fresh lock from a settled generation (absent or even).
    Acquire,
    /// Take a fresh lock from the odd generation a force-release left.
    AcquireForceReleased,
    /// Replace the owner of an expired lease.
    Takeover,
}

/// `begin`'s decision, shared by every backend so they bind identical values (spec, "Request
/// shapes").
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    pub(crate) kind: PlanKind,
    /// What the generation grows by: 1 from a settled generation, 2 from a force-released one or
    /// for a takeover.
    pub(crate) increment: i64,
    /// The acquire's observed even generation, [`NO_MATCH`] when it does not apply.
    pub(crate) seen_even: i64,
    /// The acquire's observed force-released odd generation, or the takeover's observed odd
    /// generation; [`NO_MATCH`] when it does not apply.
    pub(crate) seen_odd: i64,
    /// The owner of the lock row a takeover replaces.
    pub(crate) observed_owner: Option<String>,
}

impl Plan {
    /// Decides what `begin` does for `state` at `now` (whole Unix seconds): acquire, acquire from a
    /// force-released generation, take over an expired lease, refuse an unexpired one with
    /// [`Error::RebuildLockHeld`], or report a state the protocol cannot produce with
    /// [`Error::RebuildStateInvalid`].
    pub(crate) fn decide(state: &RebuildState, now: i64) -> Result<Self, Error> {
        let generation = state.generation;

        let Some(lock) = &state.lock else {
            if generation.is_settled() {
                return Ok(Self {
                    kind: PlanKind::Acquire,
                    increment: 1,
                    seen_even: generation.value,
                    seen_odd: NO_MATCH,
                    observed_owner: None,
                });
            }

            if generation.force_released {
                return Ok(Self {
                    kind: PlanKind::AcquireForceReleased,
                    increment: 2,
                    seen_even: NO_MATCH,
                    seen_odd: generation.value,
                    observed_owner: None,
                });
            }

            return Err(Error::RebuildStateInvalid(format!(
                "generation {} is odd with no lock row and no force-release flag",
                generation.value
            )));
        };

        if generation.is_settled() {
            return Err(Error::RebuildStateInvalid(format!(
                "a lock row exists for {:?} but generation {} is even",
                lock.owner, generation.value
            )));
        }

        if !lock.is_expired(now) {
            return Err(Error::RebuildLockHeld {
                owner: lock.owner.clone(),
                lease_expires_at: lock.lease_expires_at,
            });
        }

        Ok(Self {
            kind: PlanKind::Takeover,
            increment: 2,
            seen_even: NO_MATCH,
            seen_odd: generation.value,
            observed_owner: Some(lock.owner.clone()),
        })
    }
}

/// The budgeted size of a control check on a rebuild row: its two key attributes, like the
/// feature-marker check.
pub(crate) fn lock_check_size() -> usize {
    "pk".len() + REBUILD_LOCK_ID.len() + "sk".len() + REBUILD_SORT_KEY.to_string().len()
}

/// Validates one guarded rebuild write before any request: at least one view operation, the
/// operations pass [`check_views`], and the batch plus its guard fits the commit budget (at most
/// [`MAX_REBUILD_VIEW_BATCH`] operations, and the byte limits of a commit, with every operation
/// sized by the backend-neutral rule and the guard by its keys, as Go sizes it).
pub(crate) fn check_view_batch(views: &[ProjectedView]) -> Result<(), Error> {
    if views.is_empty() {
        return Err(Error::InvalidArgument(
            "a rebuild write carries at least one view operation".into(),
        ));
    }

    check_views(views)?;

    crate::repository::check_budget(
        views
            .iter()
            .map(|view| view_operation_size(&view.op))
            .chain([lock_check_size()]),
    )
}

/// A repository's clock: the system time by default, injectable for tests (`with_clock`). The
/// rebuild fence reads it once per operation, in whole Unix seconds, and DynamoDB's retention
/// computes `ttl` expiries from it.
#[derive(Clone)]
pub(crate) struct Clock(Arc<dyn Fn() -> SystemTime + Send + Sync>);

impl Clock {
    pub(crate) fn new(clock: impl Fn() -> SystemTime + Send + Sync + 'static) -> Self {
        Self(Arc::new(clock))
    }

    /// The current time.
    pub(crate) fn now(&self) -> SystemTime {
        (self.0)()
    }

    /// The current time in whole Unix seconds (0 before the epoch).
    pub(crate) fn unix_seconds(&self) -> i64 {
        self.now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX)
            })
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new(SystemTime::now)
    }
}

impl fmt::Debug for Clock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Clock")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generation(value: i64, force_released: bool) -> RebuildGeneration {
        RebuildGeneration {
            value,
            force_released,
        }
    }

    fn lock(owner: &str, lease_expires_at: i64) -> Option<RebuildLock> {
        Some(RebuildLock {
            owner: owner.to_owned(),
            generation: 3,
            lease_expires_at,
        })
    }

    #[test]
    fn a_lease_holds_only_the_lock_of_its_owner_and_fencing_token() {
        let lease = RebuildLease {
            owner: "w".into(),
            generation: 3,
            lease_expires_at: 100,
        };
        let lock = |owner: &str, generation| RebuildLock {
            owner: owner.into(),
            generation,
            lease_expires_at: 50,
        };

        assert!(lease.holds(&lock("w", 3)), "the expiry is not part of it");
        assert!(
            !lease.holds(&lock("w", 5)),
            "a later incarnation of the owner"
        );
        assert!(!lease.holds(&lock("v", 3)));
    }

    #[test]
    fn begin_follows_the_decision_table() {
        let state = |generation, lock| RebuildState { generation, lock };

        // No lock row, generation absent or even: acquire by one.
        let plan = Plan::decide(&state(generation(0, false), None), 100).unwrap();
        assert_eq!(
            (plan.kind, plan.increment, plan.seen_even, plan.seen_odd),
            (PlanKind::Acquire, 1, 0, NO_MATCH)
        );
        let plan = Plan::decide(&state(generation(6, false), None), 100).unwrap();
        assert_eq!((plan.seen_even, plan.seen_odd), (6, NO_MATCH));

        // No lock row, odd and force-released: acquire by two.
        let plan = Plan::decide(&state(generation(7, true), None), 100).unwrap();
        assert_eq!(
            (plan.kind, plan.increment, plan.seen_even, plan.seen_odd),
            (PlanKind::AcquireForceReleased, 2, NO_MATCH, 7)
        );

        // An expired lease is taken over, observing its owner; one that expires this second is
        // still held.
        let plan = Plan::decide(&state(generation(3, false), lock("w1", 99)), 100).unwrap();
        assert_eq!(
            (plan.kind, plan.increment, plan.seen_odd),
            (PlanKind::Takeover, 2, 3)
        );
        assert_eq!(plan.observed_owner.as_deref(), Some("w1"));
        assert!(matches!(
            Plan::decide(&state(generation(3, false), lock("w1", 100)), 100),
            Err(Error::RebuildLockHeld { owner, lease_expires_at: 100 }) if owner == "w1"
        ));

        // The two states the protocol cannot produce.
        assert!(matches!(
            Plan::decide(&state(generation(5, false), None), 100),
            Err(Error::RebuildStateInvalid(_))
        ));
        assert!(matches!(
            Plan::decide(&state(generation(4, false), lock("w1", 99)), 100),
            Err(Error::RebuildStateInvalid(_))
        ));
    }

    #[test]
    fn owners_and_leases_are_checked_before_any_request() {
        assert!(matches!(check_owner(""), Err(Error::InvalidArgument(_))));
        assert!(check_owner(&"o".repeat(MAX_REBUILD_OWNER_BYTES)).is_ok());
        assert!(matches!(
            check_owner(&"o".repeat(MAX_REBUILD_OWNER_BYTES + 1)),
            Err(Error::InvalidArgument(_))
        ));
        // 171 three-byte characters are 513 bytes: the bound is bytes, not characters.
        assert!(check_owner(&"€".repeat(170)).is_ok());
        assert!(check_owner(&"€".repeat(171)).is_err());
        assert!(matches!(
            check_owner("job\0"),
            Err(Error::InvalidArgument(_))
        ));

        assert!(matches!(
            lease_seconds(Duration::from_millis(999)),
            Err(Error::InvalidArgument(_))
        ));
        assert_eq!(lease_seconds(Duration::from_secs(1)).unwrap(), 1);
        assert_eq!(lease_seconds(Duration::from_millis(1_001)).unwrap(), 2);
        assert_eq!(lease_seconds(Duration::from_secs(600)).unwrap(), 600);
        assert!(lease_seconds(Duration::MAX).is_err());
    }

    #[test]
    fn epochs_refuse_odd_and_changed_generations() {
        fn unavailable<T>(result: Result<T, Error>, value: i64, flagged: bool) -> bool {
            matches!(
                result,
                Err(Error::RebuildUnavailable { generation, force_released })
                    if generation == value && force_released == flagged
            )
        }

        assert!(unavailable(
            RebuildEpoch::observe(generation(3, false)),
            3,
            false
        ));
        assert!(unavailable(
            RebuildEpoch::observe(generation(5, true)),
            5,
            true
        ));

        let epoch = RebuildEpoch::observe(generation(4, false)).unwrap();
        assert_eq!(epoch.generation(), 4);
        assert!(epoch.verify(generation(4, false)).is_ok());
        assert!(unavailable(epoch.verify(generation(5, false)), 5, false));
        assert!(matches!(
            epoch.verify(generation(6, false)),
            Err(Error::RebuildEpochChanged {
                observed: 4,
                current: 6
            })
        ));

        // An absent generation is settled at 0.
        assert_eq!(
            RebuildEpoch::observe(RebuildGeneration::default())
                .unwrap()
                .generation(),
            0
        );
    }

    #[test]
    fn the_reader_errors_are_transient() {
        for error in [
            Error::RebuildInProgress,
            Error::RebuildConflict { attempts: 8 },
            Error::RebuildUnavailable {
                generation: 1,
                force_released: false,
            },
            Error::RebuildEpochChanged {
                observed: 2,
                current: 4,
            },
        ] {
            assert!(error.is_retryable(), "{error}");
            assert!(!error.is_conflict(), "{error}");
        }

        for error in [
            Error::RebuildLockHeld {
                owner: "w".into(),
                lease_expires_at: 1,
            },
            Error::RebuildLeaseLost { owner: "w".into() },
            Error::RebuildStateInvalid("x".into()),
        ] {
            assert!(!error.is_retryable(), "{error}");
            assert!(!error.is_conflict(), "{error}");
        }
    }

    #[test]
    fn a_view_batch_is_bounded_and_the_guard_is_sized_by_its_keys() {
        let view = |sk: usize| {
            ProjectedView::new(
                "rebuild",
                0,
                sk,
                crate::ViewOperation::put("p", format!("{sk:03}"), "o", "", "{}"),
            )
        };
        let batch = |count: usize| (0..count).map(view).collect::<Vec<_>>();

        assert_eq!(lock_check_size(), 2 + 14 + 2 + 1);
        assert!(matches!(
            check_view_batch(&[]),
            Err(Error::InvalidArgument(_))
        ));
        check_view_batch(&batch(MAX_REBUILD_VIEW_BATCH)).unwrap();
        assert!(matches!(
            check_view_batch(&batch(MAX_REBUILD_VIEW_BATCH + 1)),
            Err(Error::CommitTooLarge { actions: 101, .. })
        ));
        assert!(matches!(
            check_view_batch(&[view(1), view(1)]),
            Err(Error::DuplicateViewKey { .. })
        ));
    }
}
