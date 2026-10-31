// The storage contract every backend implements, and the commit rules every backend applies the
// same way: validation, the reserved namespaces, and the transaction budget.

import {headActionSize, headUpdate} from './dynamodb/head.js'
import {eventItem, itemSize, snapshotItem} from './dynamodb/item.js'
import {CommitTooLargeError, EvtError, ReservedEntityIdError} from './errors.js'
import {type SerializedEvent, type SerializedSnapshot, eventId} from './event.js'
import {MAX_COMMIT_ITEM_BYTES} from './snapshots.js'

/** A snapshot to write with a commit. Its event sequence is the commit's last event. */
export interface NewSnapshot {
  entityType: string
  /** The stored snapshot's sequence plus one, or 1 for the stream's first snapshot. */
  sequence: bigint
  /** The aggregate state after the commit's events. */
  payload: Uint8Array
}

/**
 * Everything a repository writes atomically for one command. A commit with no events is a no-op,
 * and its snapshot is ignored.
 */
export interface Commit {
  /** The stream the events are appended to. */
  entityId: string
  /** The new events, in ascending sequence order. */
  events: SerializedEvent[]
  /** The snapshot to write, when the commit reaches a snapshot boundary. */
  snapshot?: NewSnapshot | null | undefined
}

/** Options every repository call takes. */
export interface CallOptions {
  /** Aborts the call. An abort before the call starts rejects with the signal's reason. */
  signal?: AbortSignal | undefined
}

/**
 * An append-only event log with one optional snapshot and one head per stream (ADR 0007). Every
 * backend must:
 *
 * - refuse a commit built wrong ({@link validateCommit}) with `invalidCommit`;
 * - keep a head per stream, the highest event sequence ever committed to it, which a commit
 *   raises to its last event and compaction never touches;
 * - refuse a commit whose first event doesn't follow the stored head with a
 *   {@link ConflictError}, or with a {@link SequenceGapError} when it skips past the head;
 * - report a stream whose head disagrees with its events or snapshot as a
 *   {@link StreamHeadDriftError}, ranked above a gap and any other conflict;
 * - refuse a commit's snapshot whose sequence isn't the stored snapshot's plus one (1 with none
 *   stored) with a {@link ConflictError};
 * - write a commit's events, head and snapshot atomically, or nothing;
 * - refuse an entity ID in a reserved namespace on every write path, before any request;
 * - return events in ascending sequence order, and `latest` in descending order.
 */
export interface Repository {
  /** Appends a commit's events, and its snapshot if it has one, atomically. */
  commit(commit: Commit, options?: CallOptions): Promise<void>

  /** Every stored event of the stream, ascending. */
  events(entityId: string, options?: CallOptions): Promise<SerializedEvent[]>

  /** The stream's events after sequence `after`, ascending. */
  eventsAfter(entityId: string, after: bigint, options?: CallOptions): Promise<SerializedEvent[]>

  /**
   * The stream's last `n` events, newest first, never the snapshot. `n` must be a safe integer of
   * at least 1, or the call fails with `invalidArgument` before any request.
   */
  latest(entityId: string, n: number, options?: CallOptions): Promise<SerializedEvent[]>

  /** The stream's snapshot, or null when it has none. */
  snapshot(entityId: string, options?: CallOptions): Promise<SerializedSnapshot | null>

  /**
   * Writes a snapshot outside a commit, such as a catch-up snapshot. It never lowers the stored
   * snapshot's event sequence: it replaces a snapshot at or below it and resolves `true`, or
   * leaves a newer one in place and resolves `false`. The stream's head must cover the snapshot,
   * or the write fails with a {@link StreamHeadDriftError}. A snapshot at event sequence 0 needs no
   * head.
   */
  putSnapshot(snapshot: SerializedSnapshot, options?: CallOptions): Promise<boolean>

  /**
   * Deletes the stream's events with sequences 1 to `through`, but only when the stored snapshot
   * covers them (its event sequence is at least `through`), and resolves to how many it deleted.
   * It never deletes the snapshot or touches the head. A `through` below 1 deletes nothing.
   */
  compactBelow(entityId: string, through: bigint, options?: CallOptions): Promise<number>
}

/** The most actions one commit may carry, DynamoDB's transaction limit. */
export const MAX_COMMIT_ACTIONS = 100

/** The most bytes one commit may carry, summed over its actions: 95% of DynamoDB's 4 MB. */
export const MAX_COMMIT_BYTES = 3_800_000

/** The prefixes of the entity IDs only the engine writes. */
export const RESERVED_PREFIXES: readonly string[] = Object.freeze([
  'SYSTEM#',
  'command-outcome#',
])

// The control rows every commit checks: the feature marker and the rebuild lock.
const FEATURES_ID = 'SYSTEM#FEATURES'
const REBUILD_LOCK_ID = 'SYSTEM#REBUILD'

/** Whether an entity ID is in a reserved namespace. */
export function isReserved(entityId: string): boolean {
  return RESERVED_PREFIXES.some((prefix) => entityId.startsWith(prefix))
}

/** Refuses an entity ID in a reserved namespace, on a write path, before any request. */
export function checkNotReserved(entityId: string): void {
  if (isReserved(entityId)) {
    throw new ReservedEntityIdError(entityId)
  }
}

/**
 * Checks that a commit's events can be stored as written: each belongs to the commit's entity and
 * has the ID `{entityId}:{sequence}`, they share one entity type, the first sequence is at least
 * 1, and the sequences are contiguous. A snapshot must have the events' entity type and a
 * sequence of at least 1. A commit with no events is valid, and its snapshot isn't checked.
 */
export function validateCommit(commit: Commit): void {
  const [first] = commit.events
  if (first === undefined) {
    return
  }

  for (const event of commit.events) {
    if (event.entityType !== first.entityType) {
      throw invalidCommit(
        `event ${event.id} has entity type ${event.entityType}, but the commit's first event ` +
          `has ${first.entityType}`,
      )
    }
  }

  const snapshot = commit.snapshot
  if (snapshot && snapshot.entityType !== first.entityType) {
    throw invalidCommit(
      `the snapshot has entity type ${snapshot.entityType}, but the events have ` +
        first.entityType,
    )
  }
  if (snapshot && snapshot.sequence < 1n) {
    throw invalidCommit(
      `snapshot sequence ${snapshot.sequence} is below 1; snapshot sequences start at 1`,
    )
  }

  for (const event of commit.events) {
    if (event.entityId !== commit.entityId) {
      throw invalidCommit(
        `event ${event.id} belongs to entity ${event.entityId}, not ${commit.entityId}`,
      )
    }

    const id = eventId(event.entityId, event.sequence)
    if (event.id !== id) {
      throw invalidCommit(`event ${event.id} should have the ID ${id}`)
    }
  }

  if (first.sequence < 1n) {
    throw invalidCommit(`event sequence ${first.sequence} is below 1; sequences start at 1`)
  }

  for (let i = 1; i < commit.events.length; i++) {
    const previous = commit.events[i - 1]!.sequence
    const next = commit.events[i]!.sequence
    if (next !== previous + 1n) {
      throw invalidCommit(
        `event sequence ${next} follows ${previous}; sequences must be contiguous`,
      )
    }
  }
}

/**
 * The size of each action a commit's transaction carries, in order: the feature-marker and
 * rebuild-lock checks, then the stream's head update, its event puts, and its snapshot put when
 * it writes one. `ttl` gives the expiry an entity type's items carry, if it's retained. Every
 * backend budgets a commit by these sizes, so an oversized commit fails the same way on each.
 */
export function commitSizes(
  commit: Commit,
  ttl: (entityType: string) => number | undefined,
): number[] {
  const sizes = [controlCheckSize(FEATURES_ID), controlCheckSize(REBUILD_LOCK_ID)]
  const first = commit.events[0]
  const last = commit.events.at(-1)
  if (first === undefined || last === undefined) {
    return sizes
  }

  const head = headUpdate(commit.entityId, first.sequence, last.sequence, ttl(first.entityType))
  sizes.push(headActionSize(head))
  for (const event of commit.events) {
    sizes.push(itemSize(eventItem(event, ttl(event.entityType))))
  }

  const snapshot = commit.snapshot
  if (snapshot) {
    const stored: SerializedSnapshot = {
      entityType: snapshot.entityType,
      entityId: commit.entityId,
      sequence: snapshot.sequence,
      eventSequence: last.sequence,
      payload: snapshot.payload,
    }
    sizes.push(itemSize(snapshotItem(stored, ttl(snapshot.entityType))))
  }

  return sizes
}

/**
 * Refuses a commit whose actions are over the budget: more than {@link MAX_COMMIT_ACTIONS}, more
 * than {@link MAX_COMMIT_BYTES} in all, or one over {@link MAX_COMMIT_ITEM_BYTES}.
 */
export function checkBudget(sizes: readonly number[]): void {
  let bytes = 0
  let largestItem = 0
  for (const size of sizes) {
    bytes += size
    largestItem = Math.max(largestItem, size)
  }

  const tooLarge =
    sizes.length > MAX_COMMIT_ACTIONS ||
    bytes > MAX_COMMIT_BYTES ||
    largestItem > MAX_COMMIT_ITEM_BYTES
  if (tooLarge) {
    throw new CommitTooLargeError(sizes.length, bytes, largestItem)
  }
}

// A control check is sized by its key: `pk` and `sk = 0`.
function controlCheckSize(pk: string): number {
  return itemSize({pk: {S: pk}, sk: {N: '0'}})
}

function invalidCommit(message: string): EvtError {
  return new EvtError('invalidCommit', `invalid commit: ${message}`)
}
