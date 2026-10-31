// The snapshot policy and the per-item budget a snapshot must fit, the same on every backend.

import {snapshotItem, itemSize} from './dynamodb/item.js'
import type {SerializedSnapshot} from './event.js'
import {safeIntegerArgument} from './fields.js'

/**
 * The most bytes one action of a commit may carry: 95% of DynamoDB's 400 KB item limit. A commit
 * with a larger item fails on every backend.
 */
export const MAX_COMMIT_ITEM_BYTES = 380_000

// A ttl attribute at its widest: the name and the 19 digits of the largest int64.
const WIDEST_TTL = {N: (2n ** 63n - 1n).toString()}

/**
 * Whether a commit takes a snapshot, given the stream's current sequence, the events in the
 * commit and the snapshot interval: 0 for no snapshot, otherwise the events it folds in, which is
 * always the whole commit. A commit snapshots when it reaches or crosses the next multiple of the
 * interval, and an interval of 0 (or less, as in Go) disables snapshots.
 */
export function snapshotEventsDue(current: bigint, events: number, interval: number): number {
  safeIntegerArgument(events, 'events')
  if (safeIntegerArgument(interval, 'interval') <= 0) {
    return 0
  }

  const divisor = BigInt(interval)
  const nextSnapshotAt = interval - Number(((current % divisor) + divisor) % divisor)

  return events < nextSnapshotAt ? 0 : events
}

/**
 * The bytes the commit budget counts for a snapshot's item, with a `ttl` at its widest whether or
 * not the entity type is retained. A store leaves out a snapshot over
 * {@link MAX_COMMIT_ITEM_BYTES}, so an aggregate whose state outgrows the budget doesn't make
 * every commit that reaches the interval fail.
 */
export function snapshotCommitSize(snapshot: SerializedSnapshot): number {
  return itemSize({...snapshotItem(snapshot), ttl: WIDEST_TTL})
}
