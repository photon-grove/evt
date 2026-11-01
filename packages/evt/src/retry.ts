// Retry policy: how many times a command runs and how long to wait between attempts, with the
// defaults the compat fixtures pin (outcomes.json, retry).

import {EvtError} from './errors.js'

/**
 * How many times to run a command and how long to wait between attempts. Delays grow
 * exponentially, starting at `baseDelayMs` and capped at `maxDelayMs`, with jitter in the upper
 * half of each (`[delay / 2, delay]`), so writers that conflicted together don't retry together.
 */
export interface RetryPolicy {
  /** Attempts in total, the first included. */
  readonly maxAttempts: number
  /** The delay before the first retry, before jitter. */
  readonly baseDelayMs: number
  /** The upper bound of any delay. */
  readonly maxDelayMs: number
}

/** Five attempts, waiting from 25 ms up to at most 1 s, as Go and Rust do. */
export const DEFAULT_RETRY_POLICY: RetryPolicy = Object.freeze({
  maxAttempts: 5,
  baseDelayMs: 25,
  maxDelayMs: 1000,
})

/** Refuses a policy with fewer than one attempt or a delay that isn't a whole, finite number. */
export function checkRetryPolicy(policy: RetryPolicy): void {
  const valid =
    Number.isSafeInteger(policy.maxAttempts) &&
    policy.maxAttempts >= 1 &&
    Number.isSafeInteger(policy.baseDelayMs) &&
    policy.baseDelayMs >= 0 &&
    Number.isSafeInteger(policy.maxDelayMs) &&
    policy.maxDelayMs >= 0
  if (!valid) {
    throw new EvtError(
      'invalidArgument',
      'a retry policy needs at least one attempt and whole, non-negative delays: ' +
        `${policy.maxAttempts}, ${policy.baseDelayMs}, ${policy.maxDelayMs}`,
    )
  }
}

/**
 * The jittered delay in milliseconds before retry `retry` (0 for the first retry). `random`
 * returns a number in [0, 1), as `Math.random` does.
 */
export function retryDelay(policy: RetryPolicy, retry: number, random = Math.random): number {
  const ceiling = Math.min(policy.baseDelayMs * 2 ** retry, policy.maxDelayMs)
  const floor = Math.floor(ceiling / 2)

  return floor + Math.floor(random() * (ceiling - floor + 1))
}

/** Waits `ms` milliseconds, or rejects with the signal's reason when it aborts first. */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason)
      return
    }

    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', aborted)
      resolve()
    }, ms)
    const aborted = () => {
      clearTimeout(timer)
      reject(signal?.reason)
    }
    signal?.addEventListener('abort', aborted, {once: true})
  })
}
