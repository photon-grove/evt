//! Retry policy: the bounded attempts and backoff of fence-aware retry (C5) on a store with
//! outcomes ([`OutcomeConfig::with_retry`](crate::OutcomeConfig::with_retry)), and of
//! [`Store::execute_with_retry`](crate::Store::execute_with_retry) on one without.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

/// How many times to run a command and how long to wait between attempts.
///
/// On a store with outcomes this bounds fence-aware retry (C5): a command is attempted at most
/// [`max_attempts`](RetryPolicy::max_attempts) times, and each retry re-prepares it (the aggregate
/// is loaded again and the handler runs again against fresh state) after a backoff. When the last
/// attempt also conflicts, the engine records a stale-fence rejection
/// ([`Rejection::retries_exhausted`](crate::Rejection::retries_exhausted)) as the command's
/// outcome instead of returning the conflict. A transient backend failure is retried the same way
/// but is returned as an error when attempts run out.
///
/// Delays grow exponentially from `base_delay` up to `max_delay`, with jitter in the upper half of
/// each delay (`[delay / 2, delay]`), like Go's `policy.ExponentialJitterDelay`. The jitter keeps
/// writers that conflicted together from retrying together.
///
/// Worst case, a command takes `max_attempts` attempts, the waits between them (at most 25, 50,
/// 100, and 200 ms with the defaults, about 0.4 s in total), and, for an ambiguous write whose row
/// never appears, a full recovery window
/// ([`OutcomeConfig::with_recovery_wait`](crate::OutcomeConfig::with_recovery_wait), 10 s, plus up
/// to a second of grace) per attempt: about a minute with the defaults. Set a
/// [deadline](RetryPolicy::with_deadline) to bound the total.
///
/// The crate does not depend on an async runtime. [`Store::execute_with_retry`] takes the sleep
/// function from the caller, for example `tokio::time::sleep`; fence-aware retry waits on the
/// repository's own timer ([`Repository::backoff`](crate::Repository::backoff)).
///
/// [`Store::execute_with_retry`]: crate::Store::execute_with_retry
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RetryPolicy {
    /// Attempts in total, including the first. Values below 1 behave as 1.
    pub max_attempts: u32,
    /// The delay before the first retry, before jitter.
    pub base_delay: Duration,
    /// The upper bound for any delay.
    pub max_delay: Duration,
    /// When set, bounds the whole command: no attempt starts after the deadline has elapsed since
    /// the first began, and no wait is taken that would end past it (see
    /// [`with_deadline`](RetryPolicy::with_deadline)).
    pub deadline: Option<Duration>,
}

impl Default for RetryPolicy {
    /// [`DEFAULT_MAX_ATTEMPTS`] attempts, starting at 25 ms and capped at 1 s, like Go's
    /// `snapshots.DefaultRetryPolicy`.
    fn default() -> Self {
        Self::new(DEFAULT_MAX_ATTEMPTS)
    }
}

/// The default number of attempts in total, the first one included.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// The default delay before the first retry, before jitter, in milliseconds.
pub const DEFAULT_BASE_DELAY_MS: u64 = 25;

/// The default upper bound for any delay, in milliseconds.
pub const DEFAULT_MAX_DELAY_MS: u64 = 1000;

impl RetryPolicy {
    /// A policy with `max_attempts` attempts and the default delays.
    pub const fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            base_delay: Duration::from_millis(DEFAULT_BASE_DELAY_MS),
            max_delay: Duration::from_millis(DEFAULT_MAX_DELAY_MS),
            deadline: None,
        }
    }

    /// A policy with `max_attempts` attempts and no delay between them.
    pub const fn immediate(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            deadline: None,
        }
    }

    /// Sets the delay before the first retry and the upper bound for any delay.
    #[must_use]
    pub const fn with_backoff(mut self, base_delay: Duration, max_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self.max_delay = max_delay;
        self
    }

    /// Bounds the whole command: no attempt starts after `deadline` has elapsed since the first
    /// began, and no wait is taken that would end past it. The attempts made are then treated as
    /// exhausted, so a conflict is recorded as a stale-fence rejection and a transient failure is
    /// returned. An attempt already running is not interrupted. The check uses the system clock
    /// between attempts, so it needs no runtime timer.
    #[must_use]
    pub const fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// The jittered delay before retry `retry` (0 for the first retry).
    pub fn delay(&self, retry: u32) -> Duration {
        let ceiling = self
            .base_delay
            .saturating_mul(2u32.saturating_pow(retry))
            .min(self.max_delay);
        let floor = ceiling / 2;
        let spread = (ceiling - floor).as_nanos() as u64;

        if spread == 0 {
            return ceiling;
        }

        floor + Duration::from_nanos(random() % (spread + 1))
    }
}

/// The attempts one command has left: counts them and applies the policy's bounds.
pub(crate) struct Budget {
    policy: RetryPolicy,
    started: std::time::Instant,
    attempt: u32,
}

impl Budget {
    /// Starts the clock for a command's first attempt.
    pub(crate) fn new(policy: RetryPolicy) -> Self {
        Self {
            policy,
            started: std::time::Instant::now(),
            attempt: 1,
        }
    }

    /// The number of attempts made so far, the one that just failed included.
    pub(crate) fn attempts(&self) -> u32 {
        self.attempt
    }

    /// Spends one more attempt and returns the delay to wait before it, or `None` when the
    /// attempts or the deadline are used up.
    pub(crate) fn next(&mut self) -> Option<Duration> {
        let delay = self.policy.delay(self.attempt - 1);
        let past_deadline = self
            .policy
            .deadline
            .is_some_and(|deadline| self.started.elapsed().saturating_add(delay) >= deadline);

        if self.attempt >= self.policy.max_attempts.max(1) || past_deadline {
            return None;
        }

        self.attempt += 1;

        Some(delay)
    }
}

/// What a failed attempt of a command means for fence-aware retry (C5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Retry {
    /// Return the error.
    Never,
    /// Run the command again after a delay, but return the error when attempts run out: a
    /// transient backend failure or an ambiguous write says nothing about the state going stale.
    Transient,
    /// Another writer moved state the attempt read or wrote (a head update, an event put, a
    /// snapshot condition, an engine-filled fence). Run the command again after a delay, and
    /// record a stale-fence rejection when attempts run out. Stream head drift is not one of
    /// these: it is `Never` retried and never recorded.
    Conflict,
}

impl Retry {
    /// Classifies an error from committing. A caller-stamped fence conflict never reaches the
    /// retry loop: a transaction records its stale-fence rejection at once.
    pub(crate) fn of(error: &crate::Error) -> Self {
        use crate::Error;
        use crate::repository::FenceProvenance;

        match error {
            Error::Conflict { .. }
            | Error::SequenceGap { .. }
            | Error::FenceConflict {
                provenance: FenceProvenance::Engine,
                ..
            } => Self::Conflict,
            Error::Transient(_) | Error::AmbiguousWrite { .. } => Self::Transient,
            // A rebuild holds the lock (C10): the store-level control row says nothing about the
            // decision, so it is never a conflict and never exhausts into a rejection, but the
            // lock may be released, taken over, or force-released by the next attempt. The
            // reader errors are only returned by a lost `begin_rebuild` race or a handler's
            // own epoch check (which is returned at once, as the handler's error).
            Error::RebuildInProgress
            | Error::RebuildConflict { .. }
            | Error::RebuildUnavailable { .. }
            | Error::RebuildEpochChanged { .. } => Self::Transient,
            // Stream head drift says the log is damaged, not that a decision went stale: it is
            // returned at once, never retried and never exhausted into a stale-fence rejection.
            // A table that is not migrated stays that way until an operator migrates it.
            Error::StreamHeadDrift { .. } | Error::StreamHeadsNotMigrated => Self::Never,
            _ => Self::Never,
        }
    }

    /// Classifies an error from the decision phase (reading the outcome row, loading state, or the
    /// caller's own prepare step). A load that raced a compaction finds a sequence gap, which
    /// reloading resolves, so it is retried; but it says the read was inconsistent (or, when it
    /// persists, that events expired), not that a decision went stale, so it is only ever
    /// transient and never exhausts into a stale-fence rejection.
    pub(crate) fn of_decision(error: &crate::Error) -> Self {
        match Self::of(error) {
            Self::Conflict => Self::Transient,
            other => other,
        }
    }
}

/// Where in an attempt an error arose, which decides what the retry loop makes of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// The command's own failure: a handler's or prepare step's error, an encoding error. Never
    /// retried, whatever variant it is.
    Internal,
    /// Reading the outcome row or loading state (including a load a prepare step passed on).
    Deciding,
    /// The commit.
    Committing,
}

/// A failed attempt: the error, and the phase it arose in.
pub(crate) struct Failed {
    pub(crate) error: crate::Error,
    pub(crate) phase: Phase,
}

impl Failed {
    /// An error the command itself produced.
    pub(crate) fn internal(error: crate::Error) -> Self {
        Self {
            error,
            phase: Phase::Internal,
        }
    }

    /// An error from the decision phase.
    pub(crate) fn deciding(error: crate::Error) -> Self {
        Self {
            error,
            phase: Phase::Deciding,
        }
    }

    /// An error from the commit.
    pub(crate) fn committing(error: crate::Error) -> Self {
        Self {
            error,
            phase: Phase::Committing,
        }
    }

    /// What the retry loop does with it.
    pub(crate) fn retry(&self) -> Retry {
        match self.phase {
            Phase::Internal => Retry::Never,
            Phase::Deciding => Retry::of_decision(&self.error),
            Phase::Committing => Retry::of(&self.error),
        }
    }
}

/// A random value from the standard library's per-process hasher keys, which is enough for jitter
/// without a dependency on `rand`.
fn random() -> u64 {
    RandomState::new().build_hasher().finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_conflict_exhausts_only_when_it_came_from_the_commit() {
        use crate::Error;
        use crate::ids::{EntityId, EventSequence};
        use crate::repository::FenceProvenance;

        let conflict = || Error::Conflict {
            entity_id: EntityId::new("e"),
            message: "collision".into(),
        };
        let gap = || Error::SequenceGap {
            entity_id: EntityId::new("e"),
            after: EventSequence(0),
            found: EventSequence(2),
        };
        let fence = |provenance| Error::FenceConflict {
            entity_id: EntityId::new("e"),
            head: EventSequence(1),
            provenance,
        };

        assert_eq!(Failed::committing(conflict()).retry(), Retry::Conflict);
        assert_eq!(Failed::committing(gap()).retry(), Retry::Conflict);
        assert_eq!(
            Failed::committing(fence(FenceProvenance::Engine)).retry(),
            Retry::Conflict
        );
        assert_eq!(
            Failed::committing(fence(FenceProvenance::Caller)).retry(),
            Retry::Never,
            "a caller-stamped fence is recorded at once, never retried"
        );

        // The same errors while deciding (loading, or passed on by a prepare step) are retried but
        // never record a rejection.
        assert_eq!(Failed::deciding(conflict()).retry(), Retry::Transient);
        assert_eq!(Failed::deciding(gap()).retry(), Retry::Transient);
        assert_eq!(
            Failed::deciding(Error::Transient("throttled".into())).retry(),
            Retry::Transient
        );
        assert_eq!(
            Failed::committing(Error::Handler("boom".into())).retry(),
            Retry::Never
        );
    }

    #[test]
    fn a_commit_refused_by_a_rebuild_is_retried_but_never_exhausted_into_a_rejection() {
        use crate::Error;

        let refused = || Error::RebuildInProgress;

        assert_eq!(Failed::committing(refused()).retry(), Retry::Transient);
        assert_eq!(Failed::deciding(refused()).retry(), Retry::Transient);

        // A handler or prepare step that returns a reader's epoch error is the command's own
        // failure: returned at once, whatever the variant.
        for error in [
            Error::RebuildUnavailable {
                generation: 3,
                force_released: false,
            },
            Error::RebuildEpochChanged {
                observed: 2,
                current: 4,
            },
            refused(),
        ] {
            assert_eq!(Failed::internal(error).retry(), Retry::Never);
        }
    }

    #[test]
    fn drift_and_a_table_that_is_not_migrated_are_never_retried() {
        use crate::Error;
        use crate::ids::{EntityId, EventSequence};

        let drift = || Error::StreamHeadDrift {
            entity_id: EntityId::new("e"),
            stored: Some(EventSequence(2)),
            derived: EventSequence(3),
        };

        for failed in [
            Failed::committing(drift()),
            Failed::deciding(drift()),
            Failed::internal(drift()),
            Failed::committing(Error::StreamHeadsNotMigrated),
            Failed::deciding(Error::StreamHeadsNotMigrated),
        ] {
            assert_eq!(failed.retry(), Retry::Never, "{}", failed.error);
            assert!(!failed.error.is_retryable() && !failed.error.is_conflict());
        }
    }

    #[test]
    fn the_budget_bounds_attempts_and_honors_the_deadline() {
        let mut budget = Budget::new(RetryPolicy::immediate(3));
        assert!(budget.next().is_some());
        assert!(budget.next().is_some());
        assert!(budget.next().is_none(), "three attempts in total");
        assert_eq!(budget.attempts(), 3);

        let mut budget = Budget::new(RetryPolicy::immediate(0));
        assert!(budget.next().is_none(), "below one means one");
        assert_eq!(budget.attempts(), 1);

        let mut budget =
            Budget::new(RetryPolicy::immediate(5).with_deadline(Duration::from_nanos(1)));
        std::thread::sleep(Duration::from_millis(1));
        assert!(
            budget.next().is_none(),
            "no attempt starts past the deadline"
        );
        assert_eq!(budget.attempts(), 1);

        let mut budget = Budget::new(
            RetryPolicy::new(5)
                .with_backoff(Duration::from_secs(10), Duration::from_secs(10))
                .with_deadline(Duration::from_secs(1)),
        );
        assert!(
            budget.next().is_none(),
            "a wait that would end past the deadline is not taken"
        );
    }

    #[test]
    fn delays_grow_with_jitter_and_stay_capped() {
        let policy = RetryPolicy::new(10)
            .with_backoff(Duration::from_millis(10), Duration::from_millis(100));

        for (retry, ceiling) in [(0, 10), (1, 20), (2, 40), (3, 80), (4, 100), (30, 100)] {
            let ceiling = Duration::from_millis(ceiling);

            for _ in 0..100 {
                let delay = policy.delay(retry);
                assert!(
                    delay >= ceiling / 2 && delay <= ceiling,
                    "retry {retry}: {delay:?} outside [{:?}, {ceiling:?}]",
                    ceiling / 2
                );
            }
        }

        assert_eq!(RetryPolicy::immediate(3).delay(5), Duration::ZERO);
    }
}
