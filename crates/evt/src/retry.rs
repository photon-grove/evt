//! Retry policy for [`Store::execute_with_retry`](crate::Store::execute_with_retry).

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

/// How many times to run a command and how long to wait between attempts.
///
/// Delays grow exponentially from `base_delay` up to `max_delay`, with jitter in the upper half of
/// each delay (`[delay / 2, delay]`), like Go's `policy.ExponentialJitterDelay`. The jitter keeps
/// writers that conflicted together from retrying together.
///
/// The crate does not depend on an async runtime, so the caller supplies the sleep function, for
/// example `tokio::time::sleep`.
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
    /// Three attempts, starting at 25 ms and capped at 1 s.
    fn default() -> Self {
        Self::new(3)
    }
}

impl RetryPolicy {
    /// A policy with `max_attempts` attempts and the default delays.
    pub const fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            base_delay: Duration::from_millis(25),
            max_delay: Duration::from_secs(1),
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
