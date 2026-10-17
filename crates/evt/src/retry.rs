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
        }
    }

    /// A policy with `max_attempts` attempts and no delay between them.
    pub const fn immediate(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }

    /// Sets the delay before the first retry and the upper bound for any delay.
    #[must_use]
    pub const fn with_backoff(mut self, base_delay: Duration, max_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self.max_delay = max_delay;
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

/// What a failed attempt of a command means for fence-aware retry (C5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Retry {
    /// Return the error.
    Never,
    /// Run the command again after a delay, but return the error when attempts run out: a
    /// transient backend failure or an ambiguous write says nothing about the state going stale.
    Transient,
    /// Another writer moved state the attempt read or wrote (an event put, a floor or snapshot
    /// condition, a continuity check, an engine-filled fence). Run the command again after a
    /// delay, and record a stale-fence rejection when attempts run out.
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

/// A failed attempt: the error, and whether it came from the commit (where a conflict means
/// another writer moved state the attempt used) or from the decision phase before it.
pub(crate) struct Failed {
    pub(crate) error: crate::Error,
    pub(crate) committing: bool,
}

impl Failed {
    /// An error from the decision phase.
    pub(crate) fn deciding(error: crate::Error) -> Self {
        Self {
            error,
            committing: false,
        }
    }

    /// An error from the commit.
    pub(crate) fn committing(error: crate::Error) -> Self {
        Self {
            error,
            committing: true,
        }
    }

    /// What the retry loop does with it.
    pub(crate) fn retry(&self) -> Retry {
        if self.committing {
            Retry::of(&self.error)
        } else {
            Retry::of_decision(&self.error)
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
