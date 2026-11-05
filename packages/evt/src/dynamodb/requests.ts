// The `TransactWriteItems` actions of a commit and of a snapshot write, built from the storage
// format alone (storage format, "Writes"). Every commit opens with the two control checks, the
// feature marker's and the rebuild lock's, then its projector operations, then writes each stream:
// its head update, its event puts, and its snapshot put when it has one. A transaction then checks
// the head of every stream it fenced, and a commit that records a command outcome puts it last.

import type {SerializedEvent, SerializedSnapshot} from '../event.js'
import {int64Argument, safeIntegerArgument} from '../fields.js'
import {EvtError} from '../errors.js'
import type {Commit, NormalizedTransaction, ProjectorGroup, ReadFence} from '../repository.js'
import type {TransactItem} from './client.js'
import {type HeadAction, headFence, headUpdate} from './head.js'
import {HEAD_SORT_KEY, type Item, SNAPSHOT_SORT_KEY, eventItem, snapshotItem} from './item.js'
import {FEATURES_PK, REBUILD_LOCK_PK} from './marker.js'
import {OUTCOME_CONDITION, outcomeItem} from './outcome.js'
import {DYNAMODB_BACKEND} from './views.js'

/** The marker check of a writer that validated the manifest at version `:v`. */
export const MARKER_CHECK_CONDITION = 'attribute_not_exists(pk) OR version = :v'

/** The rebuild-lock check: no lock row, so the rebuild generation is settled. */
export const REBUILD_LOCK_CONDITION = 'attribute_not_exists(pk)'

/** The condition of an event put: the sequence is new. */
export const EVENT_PUT_CONDITION = 'attribute_not_exists(sk)'

/** The condition of a commit's first snapshot of a stream. */
export const FIRST_SNAPSHOT_CONDITION = 'attribute_not_exists(seq)'

/** The condition of a commit's later snapshot: the stored one is the one before it. */
export const NEXT_SNAPSHOT_CONDITION = 'seq = :seq'

/** A snapshot write's head guard: the head covers the snapshot. */
export const SNAPSHOT_GUARD_CONDITION = 'head >= :new'

/** The guard of a snapshot of an empty stream (event sequence 0), which needs no head. */
export const SNAPSHOT_GUARD_EMPTY_CONDITION = 'attribute_not_exists(head) OR head >= :new'

/** The update expression of a retained stream's snapshot guard: it refreshes the head's `ttl`. */
export const SNAPSHOT_GUARD_RETAINED_UPDATE = 'SET #ttl = :t'

/** {@link SNAPSHOT_GUARD_CONDITION} with retention: the head's `ttl` is raised, never lowered. */
export const SNAPSHOT_GUARD_RETAINED_CONDITION =
  'head >= :new AND (attribute_not_exists(#ttl) OR #ttl <= :t)'

/** A snapshot write's put: it never lowers the stored snapshot's event sequence. */
export const SNAPSHOT_WRITE_CONDITION = 'attribute_not_exists(eventSeq) OR eventSeq <= :new'

/** The key of a control row, or of a stream's snapshot. */
export function controlKey(pk: string): Item {
  return {pk: {S: pk}, sk: {N: SNAPSHOT_SORT_KEY.toString()}}
}

/** The key of a stream's head item. */
export function headKey(entityId: string): Item {
  return {pk: {S: entityId}, sk: {N: HEAD_SORT_KEY.toString()}}
}

/** The marker check of a writer that validated the manifest at `version`. */
export function markerCheck(table: string, version: bigint): TransactItem {
  return {
    ConditionCheck: {
      TableName: table,
      Key: controlKey(FEATURES_PK),
      ConditionExpression: MARKER_CHECK_CONDITION,
      ExpressionAttributeValues: {':v': number(version)},
    },
  }
}

/** The rebuild-lock check every commit carries second. */
export function rebuildLockCheck(table: string): TransactItem {
  return {
    ConditionCheck: {
      TableName: table,
      Key: controlKey(REBUILD_LOCK_PK),
      ConditionExpression: REBUILD_LOCK_CONDITION,
    },
  }
}

/** The `ttl`s of a commit to a stream with retention, in Unix seconds. */
export interface CommitTtl {
  /** The commit's own `ttl`, which its event and snapshot items carry. */
  items: number
  /**
   * The head item's: the commit's own `ttl` on a create, and on an append the larger of it and
   * the largest `ttl` the writer loaded for the stream, because the head's is never lowered.
   */
  head: number
}

/** The `ttl` of a snapshot written outside a commit, to a stream with retention. */
export interface SnapshotTtl {
  /** The snapshot's `ttl`, in Unix seconds, stamped when it's written. */
  ttl: number
  /**
   * Whether the head guard refreshes the head's `ttl` to at least the snapshot's. True unless the
   * stored head's `ttl` is known to cover it already.
   */
  refresh: boolean
}

/**
 * The actions of a commit with at least one event, in order, by a writer that validated the
 * feature manifest at `markerVersion`. `ttl` is given only for a stream with retention. A projector
 * group for any backend but DynamoDB is refused. Exported for the compatibility fixtures.
 */
export function commitTransaction(
  table: string,
  commit: Commit,
  markerVersion: bigint,
  ttl?: CommitTtl,
): TransactItem[] {
  if (commit.events.length === 0) {
    throw new RangeError('a commit transaction needs at least one event')
  }

  const transaction = {
    streams: [commit],
    fences: [],
    projections: commit.projections ?? [],
    outcome: null,
  }

  return transactionItems(table, transaction, markerVersion, () => ttl)
}

/** Where each stream's and fence's actions sit in a transaction, to classify a cancellation. */
export interface TransactionLayout {
  /** The index after the control checks and the projector operations. */
  readonly projectorEnd: number
  readonly streams: readonly StreamLayout[]
  readonly fences: readonly {readonly index: number; readonly fence: ReadFence}[]
  /** The outcome put's index, when the transaction records one. */
  readonly outcome: number | null
}

/** A written stream's actions in a transaction. */
export interface StreamLayout {
  readonly commit: Commit
  /** The head update's index. Its event puts follow it. */
  readonly head: number
  /** The index after its last event put. */
  readonly eventsEnd: number
  /** The snapshot put's index, when it writes one. */
  readonly snapshot: number | null
}

/**
 * The actions of a normalized transaction that writes, fences or records something, in order: the
 * control checks, the projector operations in declaration order, every written stream's head
 * update, event puts and snapshot put, a check on every fenced stream's head, then the outcome
 * put. `ttl` gives a written stream's `ttl`s when it has retention. Exported for the compatibility
 * fixtures.
 */
export function transactionItems(
  table: string,
  transaction: NormalizedTransaction,
  markerVersion: bigint,
  ttl: (commit: Commit) => CommitTtl | undefined,
): TransactItem[] {
  return buildTransaction(table, transaction, markerVersion, ttl).items
}

/** {@link transactionItems}, with where each stream's and fence's actions sit. */
export function buildTransaction(
  table: string,
  transaction: NormalizedTransaction,
  markerVersion: bigint,
  ttl: (commit: Commit) => CommitTtl | undefined,
): {items: TransactItem[]; layout: TransactionLayout} {
  const items = [markerCheck(table, markerVersion), rebuildLockCheck(table)]
  items.push(...projectorItems(transaction.projections))
  const projectorEnd = items.length

  const streams: StreamLayout[] = []
  for (const commit of transaction.streams) {
    const head = items.length
    items.push(...streamItems(table, commit, ttl(commit)))
    const eventsEnd = head + 1 + commit.events.length
    streams.push({commit, head, eventsEnd, snapshot: items.length > eventsEnd ? eventsEnd : null})
  }

  const fences = transaction.fences.map((fence) => {
    items.push(fenceCheck(table, fence))

    return {index: items.length - 1, fence}
  })

  let outcome: number | null = null
  if (transaction.outcome) {
    items.push({
      Put: {
        TableName: table,
        Item: outcomeItem(transaction.outcome),
        ConditionExpression: OUTCOME_CONDITION,
      },
    })
    outcome = items.length - 1
  }

  return {items, layout: {projectorEnd, streams, fences, outcome}}
}

// A written stream's actions: its head update, its event puts and its snapshot put.
function streamItems(table: string, commit: Commit, ttl: CommitTtl | undefined): TransactItem[] {
  const first = commit.events[0]!
  const last = commit.events.at(-1)!
  const head = headUpdate(commit.entityId, first.sequence, last.sequence, ttl?.head)
  const items = [headUpdateItem(table, head)]
  for (const event of commit.events) {
    items.push(eventPut(table, event, ttl?.items))
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
    items.push(commitSnapshotPut(table, stored, ttl?.items))
  }

  return items
}

// The check on a fenced stream's head item: it still has the head the transaction read.
function fenceCheck(table: string, fence: ReadFence): TransactItem {
  const action = headFence(fence.entityId, fence.head)
  const check: NonNullable<TransactItem['ConditionCheck']> = {
    TableName: table,
    Key: headKey(fence.entityId),
    ConditionExpression: action.conditionExpression,
  }
  if (action.values.length > 0) {
    check.ExpressionAttributeValues = Object.fromEntries(
      action.values.map(([placeholder, value]) => [placeholder, number(value)]),
    )
  }

  return {ConditionCheck: check}
}

/**
 * The actions of a snapshot written outside a commit: the marker check, the head guard, and the
 * put, which never lowers the stored snapshot. It carries no rebuild-lock check. `ttl` is given
 * only for a stream with retention. Exported for the compatibility fixtures.
 */
export function snapshotTransaction(
  table: string,
  snapshot: SerializedSnapshot,
  markerVersion: bigint,
  ttl?: SnapshotTtl,
): TransactItem[] {
  const eventSequence = int64Argument(snapshot.eventSequence, 'snapshot.eventSequence')

  return [
    markerCheck(table, markerVersion),
    snapshotGuard(table, snapshot.entityId, eventSequence, ttl),
    {
      Put: {
        TableName: table,
        Item: snapshotItem(snapshot, ttl?.ttl),
        ConditionExpression: SNAPSHOT_WRITE_CONDITION,
        ExpressionAttributeValues: {':new': number(eventSequence)},
      },
    },
  ]
}

// The head item's action in a snapshot write: a check that the head covers the snapshot, or, for a
// stream with retention, an update that also keeps the head's `ttl` at least the snapshot's. A
// snapshot of an empty stream may have no head item, so it's always a check: an update would
// create a head item without a head.
function snapshotGuard(
  table: string,
  entityId: string,
  eventSequence: bigint,
  ttl: SnapshotTtl | undefined,
): TransactItem {
  const values: Record<string, {N: string}> = {':new': number(eventSequence)}
  if (eventSequence === 0n || ttl === undefined || !ttl.refresh) {
    return {
      ConditionCheck: {
        TableName: table,
        Key: headKey(entityId),
        ConditionExpression:
          eventSequence === 0n ? SNAPSHOT_GUARD_EMPTY_CONDITION : SNAPSHOT_GUARD_CONDITION,
        ExpressionAttributeValues: values,
      },
    }
  }

  values[':t'] = number(BigInt(safeIntegerArgument(ttl.ttl, 'ttl')))

  return {
    Update: {
      TableName: table,
      Key: headKey(entityId),
      UpdateExpression: SNAPSHOT_GUARD_RETAINED_UPDATE,
      ConditionExpression: SNAPSHOT_GUARD_RETAINED_CONDITION,
      ExpressionAttributeNames: {'#ttl': 'ttl'},
      ExpressionAttributeValues: values,
    },
  }
}

/** The DynamoDB actions of projector groups, in order. */
export function projectorItems(groups: readonly ProjectorGroup[]): TransactItem[] {
  const items: TransactItem[] = []
  for (const group of groups) {
    if (group.backend !== DYNAMODB_BACKEND) {
      throw new EvtError(
        'invalidCommit',
        `invalid commit: a DynamoDB repository can't write a ${group.backend} projector group`,
      )
    }
    items.push(...(group.operations as TransactItem[]))
  }

  return items
}

function headUpdateItem(table: string, action: HeadAction): TransactItem {
  const update: NonNullable<TransactItem['Update']> = {
    TableName: table,
    Key: headKey(action.entityId),
    UpdateExpression: action.updateExpression ?? '',
    ConditionExpression: action.conditionExpression,
    ExpressionAttributeValues: Object.fromEntries(
      action.values.map(([placeholder, value]) => [placeholder, number(value)]),
    ),
  }
  if (action.names.length > 0) {
    update.ExpressionAttributeNames = Object.fromEntries(action.names)
  }

  return {Update: update}
}

function eventPut(table: string, event: SerializedEvent, ttl: number | undefined): TransactItem {
  return {
    Put: {TableName: table, Item: eventItem(event, ttl), ConditionExpression: EVENT_PUT_CONDITION},
  }
}

// A commit's snapshot follows the stored one: sequence 1 needs none, and a later one needs the
// one before it.
function commitSnapshotPut(
  table: string,
  snapshot: SerializedSnapshot,
  ttl: number | undefined,
): TransactItem {
  const sequence = int64Argument(snapshot.sequence, 'snapshot.sequence')
  if (sequence <= 1n) {
    return {
      Put: {
        TableName: table,
        Item: snapshotItem(snapshot, ttl),
        ConditionExpression: FIRST_SNAPSHOT_CONDITION,
      },
    }
  }

  return {
    Put: {
      TableName: table,
      Item: snapshotItem(snapshot, ttl),
      ConditionExpression: NEXT_SNAPSHOT_CONDITION,
      ExpressionAttributeValues: {':seq': number(sequence - 1n)},
    },
  }
}

function number(value: bigint): {N: string} {
  return {N: value.toString()}
}
