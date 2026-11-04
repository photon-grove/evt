// The `TransactWriteItems` actions of a commit and of a snapshot write, built from the storage
// format alone (storage format, "Writes"). Every commit opens with the two control checks, the
// feature marker's and the rebuild lock's, then writes the stream: its head update, its event
// puts, and its snapshot put when it has one.

import type {SerializedEvent, SerializedSnapshot} from '../event.js'
import {int64Argument} from '../fields.js'
import type {Commit} from '../repository.js'
import type {TransactItem} from './client.js'
import {type HeadAction, headUpdate} from './head.js'
import {HEAD_SORT_KEY, type Item, SNAPSHOT_SORT_KEY, eventItem, snapshotItem} from './item.js'
import {FEATURES_PK, REBUILD_LOCK_PK} from './marker.js'

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

/**
 * The actions of a commit with at least one event, in order, by a writer that validated the
 * feature manifest at `markerVersion`. Exported for the compatibility fixtures: its signature
 * changes when commits carry retention and span streams.
 */
export function commitTransaction(
  table: string,
  commit: Commit,
  markerVersion: bigint,
): TransactItem[] {
  const first = commit.events[0]
  const last = commit.events.at(-1)
  if (first === undefined || last === undefined) {
    throw new RangeError('a commit transaction needs at least one event')
  }

  const items = [markerCheck(table, markerVersion), rebuildLockCheck(table)]
  items.push(headUpdateItem(table, headUpdate(commit.entityId, first.sequence, last.sequence)))
  for (const event of commit.events) {
    items.push(eventPut(table, event))
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
    items.push(commitSnapshotPut(table, stored))
  }

  return items
}

/**
 * The actions of a snapshot written outside a commit: the marker check, the head guard, and the
 * put, which never lowers the stored snapshot. It carries no rebuild-lock check. Exported for the
 * compatibility fixtures: its signature changes when snapshot writes carry retention.
 */
export function snapshotTransaction(
  table: string,
  snapshot: SerializedSnapshot,
  markerVersion: bigint,
): TransactItem[] {
  const eventSequence = int64Argument(snapshot.eventSequence, 'snapshot.eventSequence')
  const values = {':new': number(eventSequence)}

  return [
    markerCheck(table, markerVersion),
    {
      ConditionCheck: {
        TableName: table,
        Key: headKey(snapshot.entityId),
        ConditionExpression:
          eventSequence === 0n ? SNAPSHOT_GUARD_EMPTY_CONDITION : SNAPSHOT_GUARD_CONDITION,
        ExpressionAttributeValues: values,
      },
    },
    {
      Put: {
        TableName: table,
        Item: snapshotItem(snapshot),
        ConditionExpression: SNAPSHOT_WRITE_CONDITION,
        ExpressionAttributeValues: values,
      },
    },
  ]
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

function eventPut(table: string, event: SerializedEvent): TransactItem {
  return {
    Put: {TableName: table, Item: eventItem(event), ConditionExpression: EVENT_PUT_CONDITION},
  }
}

// A commit's snapshot follows the stored one: sequence 1 needs none, and a later one needs the
// one before it.
function commitSnapshotPut(table: string, snapshot: SerializedSnapshot): TransactItem {
  const sequence = int64Argument(snapshot.sequence, 'snapshot.sequence')
  if (sequence <= 1n) {
    return {
      Put: {
        TableName: table,
        Item: snapshotItem(snapshot),
        ConditionExpression: FIRST_SNAPSHOT_CONDITION,
      },
    }
  }

  return {
    Put: {
      TableName: table,
      Item: snapshotItem(snapshot),
      ConditionExpression: NEXT_SNAPSHOT_CONDITION,
      ExpressionAttributeValues: {':seq': number(sequence - 1n)},
    },
  }
}

function number(value: bigint): {N: string} {
  return {N: value.toString()}
}
