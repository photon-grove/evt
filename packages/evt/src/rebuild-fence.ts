// The rebuild fence (ADR 0004, C10; storage format, "Rebuild fence"): the store-wide lock a
// projection rebuild holds while it rewrites derived state, and the generation counter readers
// pin. The generation is even while the store is quiescent and odd while a rebuild holds it, or
// after one was force-released and no rebuild has finished since; every commit is refused while
// it's odd. The lock names its owner, the odd generation that is its fencing token, and when its
// lease expires, so an expired one can be taken over.

import {
  EvtError,
  RebuildEpochChangedError,
  RebuildLockHeldError,
  RebuildStateInvalidError,
  RebuildUnavailableError,
} from './errors.js'
import {type ProjectedView, checkViews, viewOperationSize} from './projection.js'
import {type CallOptions, checkBudget} from './repository.js'
import {isHighSurrogate, isLowSurrogate, utf8Length} from './utf8.js'

/** The lock row's partition: `SYSTEM#REBUILD`, `sk = 0`. */
export const REBUILD_LOCK_ID = 'SYSTEM#REBUILD'

/** The generation row's partition: `SYSTEM#REBUILD#GENERATION`, `sk = 0`. */
export const REBUILD_GENERATION_ID = 'SYSTEM#REBUILD#GENERATION'

/** The longest rebuild owner, in UTF-8 bytes. */
export const MAX_REBUILD_OWNER_BYTES = 512

/** The most view operations one guarded write carries: the 100th action is the lease guard. */
export const MAX_REBUILD_VIEW_BATCH = 99

/** How often taking the lock reads and tries again after losing a race. */
export const MAX_REBUILD_BEGIN_ATTEMPTS = 8

/** The generation counter, and whether the last rebuild was force-released. */
export interface RebuildGeneration {
  readonly value: bigint
  readonly forceReleased: boolean
}

/** A rebuild's hold on the lock: its owner, its fencing generation, and its lease's expiry. */
export interface RebuildLock {
  readonly owner: string
  /** The odd generation that took the lock, which equals the generation while it's held. */
  readonly generation: bigint
  /** When the lease expires, in whole Unix seconds. It's still held during that second. */
  readonly leaseExpiresAt: number
}

/** The fence's two rows. A force-released lock is no lock: the odd generation records it. */
export interface RebuildState {
  readonly generation: RebuildGeneration
  readonly lock: RebuildLock | null
}

/** What a rebuild presents to renew, end or write under the lock it took. */
export type RebuildLease = RebuildLock

/**
 * A repository that runs the rebuild fence's lifecycle: every repository this package ships. A
 * rebuild takes the lock with {@link beginRebuild}, renews its lease while it runs, writes its view
 * rows with {@link writeRebuildViews}, each batch guarded by the lease, and ends with
 * {@link endRebuild}, which settles the generation. {@link forceReleaseRebuild} is the operator's
 * escape hatch for a crashed rebuild: it ends the lease and leaves the generation odd, so commits
 * stay refused until another rebuild finishes.
 */
export interface RebuildFence {
  /** The generation, then the lock: two strongly consistent reads, not one snapshot. */
  rebuildState(options?: CallOptions): Promise<RebuildState>
  /** The generation alone. */
  rebuildGeneration(options?: CallOptions): Promise<RebuildGeneration>
  /**
   * Takes the lock for `owner` with a lease of `leaseMs` milliseconds, rounded up to whole
   * seconds: from a settled generation, from a force-released one, or by taking over an expired
   * lease. Fails with a {@link RebuildLockHeldError} while another holds an unexpired lease.
   */
  beginRebuild(owner: string, leaseMs: number, options?: CallOptions): Promise<RebuildLease>
  /**
   * Extends the lease to `leaseMs` from now. The lease may have expired, as long as nobody took
   * it over; otherwise a {@link RebuildLeaseLostError}.
   */
  renewRebuild(lease: RebuildLease, leaseMs: number, options?: CallOptions): Promise<RebuildLease>
  /** Releases the lock and settles the generation, which it returns. */
  endRebuild(lease: RebuildLease, options?: CallOptions): Promise<RebuildGeneration>
  /**
   * Ends `owner`'s lease at once, leaving the generation odd and flagged force-released, which it
   * returns. Commits stay refused until a rebuild begins and ends.
   */
  forceReleaseRebuild(owner: string, options?: CallOptions): Promise<RebuildGeneration>
  /**
   * Writes a batch of view operations, at most {@link MAX_REBUILD_VIEW_BATCH}, atomically and only
   * while the lease holds the lock unexpired, or not at all.
   */
  writeRebuildViews(
    lease: RebuildLease,
    views: readonly ProjectedView[],
    options?: CallOptions,
  ): Promise<void>
}

/** Whether a repository runs the rebuild fence. */
export function isRebuildFence(repository: object): repository is RebuildFence {
  const fence = repository as Partial<RebuildFence>

  return (
    typeof fence.rebuildState === 'function' &&
    typeof fence.beginRebuild === 'function' &&
    typeof fence.writeRebuildViews === 'function'
  )
}

/**
 * The epoch a reader of derived state pins: the settled generation, 0 before any rebuild. An odd
 * one is a {@link RebuildUnavailableError}.
 */
export async function observeRebuildEpoch(
  fence: RebuildFence,
  options?: CallOptions,
): Promise<bigint> {
  const generation = await fence.rebuildGeneration(options)
  if (generation.value % 2n !== 0n) {
    throw new RebuildUnavailableError(generation.value, generation.forceReleased)
  }

  return generation.value
}

/**
 * Checks that the generation still is the epoch a read observed: an odd one is a
 * {@link RebuildUnavailableError}, another one a {@link RebuildEpochChangedError}.
 */
export async function verifyRebuildEpoch(
  fence: RebuildFence,
  observed: bigint,
  options?: CallOptions,
): Promise<void> {
  const current = await observeRebuildEpoch(fence, options)
  if (current !== observed) {
    throw new RebuildEpochChangedError(observed, current)
  }
}

/**
 * Runs a read of derived state between an observed epoch and its verification, so a read that
 * straddles a rebuild fails instead of mixing two generations. The read's own error is returned
 * as it is.
 */
export async function readAtRebuildEpoch<T>(
  fence: RebuildFence,
  read: (epoch: bigint) => Promise<T>,
  options?: CallOptions,
): Promise<T> {
  const epoch = await observeRebuildEpoch(fence, options)
  const result = await read(epoch)
  await verifyRebuildEpoch(fence, epoch, options)

  return result
}

/** Refuses a rebuild owner that is empty, over 512 UTF-8 bytes, not well formed, or has U+0000. */
export function checkRebuildOwner(owner: string): void {
  if (typeof owner !== 'string' || owner === '') {
    throw invalid('a rebuild owner must not be empty')
  }
  const bytes = utf8Length(owner)
  if (bytes > MAX_REBUILD_OWNER_BYTES) {
    throw invalid(`a rebuild owner is at most ${MAX_REBUILD_OWNER_BYTES} bytes, got ${bytes}`)
  }
  if (!wellFormed(owner)) {
    throw invalid('a rebuild owner must be valid UTF-8')
  }
  if (owner.includes('\0')) {
    throw invalid('a rebuild owner must not contain U+0000')
  }
}

/** A lease of `ms` milliseconds in whole seconds, rounded up. It must be at least a second. */
export function rebuildLeaseSeconds(ms: number): number {
  if (!Number.isSafeInteger(ms) || ms < 1000) {
    throw invalid('a rebuild lease is at least one second, in whole milliseconds')
  }

  return Math.ceil(ms / 1000)
}

/** Whether `lease` holds `lock`: the same owner and fencing generation, whatever the expiry. */
export function holdsRebuildLock(lease: RebuildLease, lock: RebuildLock | null): boolean {
  return lock !== null && lock.owner === lease.owner && lock.generation === lease.generation
}

/**
 * Settles a begin whose result is unknown (a `RebuildBeginIndeterminateError`) from a state
 * read after it failed, given the error's `lease` and `observed` generation: true when the lock
 * holds `lease`, so the begin applied; false when it doesn't and the begin can no longer take it,
 * because the generation moved from the `observed` value its write was conditioned on (the
 * generation only grows); null while the generation is still `observed`, because the begin may
 * still land.
 *
 * False means only that `lease` doesn't hold the lock: a begin that applied and was then
 * force-released or taken over settles the same way. True is exact only when the owner token is
 * the caller's alone, since a concurrent begin under the same token from the same generation
 * writes the same lease.
 */
export function holdsRebuildBegun(
  lease: RebuildLease,
  observed: bigint,
  state: RebuildState,
): boolean | null {
  if (holdsRebuildLock(lease, state.lock)) {
    return true
  }

  return state.generation.value === observed ? null : false
}

/**
 * Whether `lease` was acquired from the settled generation `before`, read with `rebuildGeneration`
 * just before the `beginRebuild` that returned it, rather than taken over from an expired lease or
 * acquired from a force-released generation (either may have left derived rows half-written). A
 * rebuild under it that fails before any write may release it only when this is true.
 */
export function acquiredFromSettled(lease: RebuildLease, before: RebuildGeneration): boolean {
  return before.value % 2n === 0n && lease.generation === before.value + 1n
}

/** How a rebuild takes the lock from the state it read. */
export interface RebuildBeginPlan {
  readonly kind: 'acquire' | 'acquireForceReleased' | 'takeover'
  /** How far the generation moves: 1 to acquire, 2 past a force-release or a takeover. */
  readonly increment: bigint
  /** The even generation an acquire conditions on, or -1 when none applies. */
  readonly seenEven: bigint
  /** The odd generation a force-released acquire or a takeover conditions on, or -1. */
  readonly seenOdd: bigint
  /** The expired lock's owner a takeover conditions on, or ''. */
  readonly observedOwner: string
}

/**
 * Decides how to take the lock at `now` (whole Unix seconds), as Go's `PlanRebuildBegin` and
 * Rust's `Plan::decide` do: a held unexpired lease is a {@link RebuildLockHeldError}, and rows that
 * disagree a {@link RebuildStateInvalidError}.
 */
export function planRebuildBegin(state: RebuildState, now: number): RebuildBeginPlan {
  const {value, forceReleased} = state.generation
  const settled = value % 2n === 0n
  const lock = state.lock
  if (lock === null) {
    if (settled) {
      return {kind: 'acquire', increment: 1n, seenEven: value, seenOdd: -1n, observedOwner: ''}
    }
    if (forceReleased) {
      return {
        kind: 'acquireForceReleased',
        increment: 2n,
        seenEven: -1n,
        seenOdd: value,
        observedOwner: '',
      }
    }
    throw new RebuildStateInvalidError(
      `generation ${value} is odd with no lock row and no force-release flag`,
    )
  }
  if (settled) {
    throw new RebuildStateInvalidError(
      `a lock row exists for ${JSON.stringify(lock.owner)} but generation ${value} is even`,
    )
  }
  if (lock.leaseExpiresAt >= now) {
    throw new RebuildLockHeldError(lock.owner, lock.leaseExpiresAt)
  }

  return {
    kind: 'takeover',
    increment: 2n,
    seenEven: -1n,
    seenOdd: value,
    observedOwner: lock.owner,
  }
}

/**
 * Refuses a guarded write's batch before any request: empty, malformed view operations, or over
 * the commit budget with the lease guard (19 bytes) as its last action.
 */
export function checkRebuildViewBatch(views: readonly ProjectedView[]): void {
  if (views.length === 0) {
    throw invalid('a rebuild write carries at least one view operation')
  }
  checkViews(views)
  checkBudget([...views.map((view) => viewOperationSize(view.op)), REBUILD_GUARD_SIZE])
}

/** The fence's clock reading: whole Unix seconds, 0 before the epoch. */
export function fenceNow(clock: () => number): number {
  return Math.max(0, Math.floor(clock() / 1000))
}

/** The lease guard's size in the budget: its key, `pk` and `sk = 0`. Internal. */
export const REBUILD_GUARD_SIZE = 'pk'.length + 'sk'.length + REBUILD_LOCK_ID.length + '0'.length

// Whether a string has no lone surrogate, so it's valid UTF-8.
function wellFormed(text: string): boolean {
  for (let i = 0; i < text.length; i++) {
    const unit = text.charCodeAt(i)
    if (isHighSurrogate(unit)) {
      if (!isLowSurrogate(text.charCodeAt(i + 1))) {
        return false
      }
      i++
    } else if (isLowSurrogate(unit)) {
      return false
    }
  }

  return true
}

function invalid(message: string): EvtError {
  return new EvtError('invalidArgument', message)
}
