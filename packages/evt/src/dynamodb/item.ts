// The DynamoDB item layout of the event log. It's pure: it shapes items the way the AWS SDK's
// low-level client takes them, and imports nothing from the SDK.
//
// | Attribute    | Type | Event item     | Snapshot item (sk 0) | Head item (sk -1) |
// | ------------ | ---- | -------------- | -------------------- | ----------------- |
// | `pk`         | S    | entity ID      | entity ID            | entity ID         |
// | `sk`         | N    | event sequence | `0`                  | `-1`              |
// | `type`       | S    | event type     |                      |                   |
// | `version`    | N    | event version  |                      |                   |
// | `entityType` | S    | entity type    | entity type          |                   |
// | `payload`    | S    | payload JSON   | state JSON           |                   |
// | `metadata`   | S    | metadata JSON  |                      |                   |
// | `seq`        | N    |                | snapshot sequence    |                   |
// | `eventSeq`   | N    |                | last event folded in |                   |
// | `head`       | N    |                |                      | highest sequence  |
// | `ttl`        | N    | optional       | optional             | optional          |

import {type SerializedEvent, type SerializedSnapshot, eventId} from '../event.js'
import {EvtError} from '../errors.js'
import {
  DOCUMENT,
  checkInt64,
  int64Argument,
  invalid,
  safeIntegerArgument,
} from '../fields.js'
import {parseJson} from '../json.js'
import {encodeMetadata, metadataFromJson} from '../metadata.js'
import {utf8Decode, utf8Encode, utf8Length} from '../utf8.js'

/** A DynamoDB attribute value, shaped like the SDK's `AttributeValue`. */
export type AttributeValue =
  | {S: string}
  | {N: string}
  | {B: Uint8Array}
  | {BOOL: boolean}
  | {NULL: boolean}
  | {M: Item}
  | {L: AttributeValue[]}
  | {SS: string[]}
  | {NS: string[]}
  | {BS: Uint8Array[]}

/** A DynamoDB item. */
export type Item = Record<string, AttributeValue>

/** The sort key of a stream's inline snapshot. */
export const SNAPSHOT_SORT_KEY = 0n

/** The sort key of a stream's head item. */
export const HEAD_SORT_KEY = -1n

/** The sort key of a single-item view row. */
export const DEFAULT_VIEW_SORT_KEY = 'VIEW'

/** Encodes an event as an event-log item. `ttl` (Unix seconds) is written only when positive. */
export function eventItem(event: SerializedEvent, ttl?: number): Item {
  return withTtl(
    {
      pk: {S: event.entityId},
      sk: {N: int64Argument(event.sequence, 'event.sequence').toString()},
      type: {S: event.type},
      version: {N: safeIntegerArgument(event.version, 'event.version').toString()},
      entityType: {S: event.entityType},
      payload: {S: payloadText(event.payload, 'event payload')},
      metadata: {S: encodeMetadata(event.metadata)},
    },
    ttl,
  )
}

/** Encodes a stream's inline snapshot item. `ttl` is written only when positive. */
export function snapshotItem(snapshot: SerializedSnapshot, ttl?: number): Item {
  return withTtl(
    {
      pk: {S: snapshot.entityId},
      sk: {N: SNAPSHOT_SORT_KEY.toString()},
      seq: {N: int64Argument(snapshot.sequence, 'snapshot.sequence').toString()},
      eventSeq: {N: int64Argument(snapshot.eventSequence, 'snapshot.eventSequence').toString()},
      entityType: {S: snapshot.entityType},
      payload: {S: payloadText(snapshot.payload, 'snapshot payload')},
    },
    ttl,
  )
}

/** Encodes a stream's head item. `ttl` is written only when positive. */
export function headItem(entityId: string, head: bigint, ttl?: number): Item {
  const item: Item = {
    pk: {S: entityId},
    sk: {N: HEAD_SORT_KEY.toString()},
    head: {N: int64Argument(head, 'head').toString()},
  }

  return withTtl(item, ttl)
}

/** Encodes a single-item view row: `pk = {entityType}#{entityId}`, `sk = "VIEW"`. */
export function viewItem(entityType: string, entityId: string, payload: Uint8Array): Item {
  return {
    pk: {S: `${entityType}#${entityId}`},
    sk: {S: DEFAULT_VIEW_SORT_KEY},
    entityID: {S: entityId},
    entityType: {S: entityType},
    payload: {S: payloadText(payload, 'view payload')},
  }
}

/**
 * Decodes an event-log item. Missing attributes other than the key are empty, and `metadata` must
 * be present and not blank. A JSON `null` metadata is the zero value, as in Go.
 */
export function eventFromItem(item: Item): SerializedEvent {
  const entityId = requiredString(item, 'pk')
  const sequence = requiredNumber(item, 'sk')
  const id = eventId(entityId, sequence)
  const metadata = optionalString(item, 'metadata') ?? ''
  if (metadata.trim() === '') {
    throw invalid(`event ${id} has empty metadata`)
  }

  let decoded
  try {
    decoded = metadataFromJson(parseJson(metadata, DOCUMENT))
  } catch (error) {
    throw new EvtError('invalidRecord', `event ${id} metadata: ${message(error)}`, {cause: error})
  }

  return {
    id,
    sequence,
    type: optionalString(item, 'type') ?? '',
    version: safeInteger(optionalNumber(item, 'version') ?? 0n, 'version'),
    entityId,
    entityType: optionalString(item, 'entityType') ?? '',
    payload: utf8Encode(optionalString(item, 'payload') ?? ''),
    metadata: decoded,
  }
}

/** Decodes a stream's inline snapshot item. */
export function snapshotFromItem(item: Item): SerializedSnapshot {
  return {
    entityType: optionalString(item, 'entityType') ?? '',
    entityId: requiredString(item, 'pk'),
    sequence: optionalNumber(item, 'seq') ?? 0n,
    eventSequence: optionalNumber(item, 'eventSeq') ?? 0n,
    payload: utf8Encode(optionalString(item, 'payload') ?? ''),
  }
}

/** Decodes a head item into its head and its `ttl`, if it has one. */
export function headFromItem(item: Item): {head: bigint; ttl: number | undefined} {
  const ttl = optionalNumber(item, 'ttl')

  return {
    head: requiredNumber(item, 'head'),
    ttl: ttl === undefined ? undefined : safeInteger(ttl, 'ttl'),
  }
}

/** Whether an item is a stream's inline snapshot (`sk = 0`). */
export function isSnapshotItem(item: Item): boolean {
  return sortKey(item) === SNAPSHOT_SORT_KEY
}

/** An item's numeric sort key, if it has one. */
export function sortKey(item: Item): bigint | undefined {
  const sk = item['sk']
  if (sk === undefined || !('N' in sk)) {
    return undefined
  }

  return parseNumber(sk.N)
}

/**
 * The `ttl` of an item written at `nowMs` (Unix milliseconds) under a retention of `retentionMs`:
 * their sum in whole Unix seconds, rounded down after adding, as Go's `Time.Add(...).Unix()` is.
 * Undefined, so no `ttl` is written, for a zero retention. A retention with a fraction of a
 * millisecond is refused, because Go would count it.
 */
export function expiry(nowMs: number, retentionMs: number): number | undefined {
  if (!Number.isSafeInteger(nowMs) || !Number.isSafeInteger(retentionMs) || retentionMs < 0) {
    throw new EvtError(
      'invalidArgument',
      'the clock and the retention must be whole, non-negative milliseconds: ' +
        `${nowMs}, ${retentionMs}`,
    )
  }
  if (retentionMs === 0) {
    return undefined
  }

  return Math.floor((nowMs + retentionMs) / 1000)
}

/**
 * The size DynamoDB counts for an item: each attribute's name plus its value, in UTF-8 bytes. The
 * commit budget measures every action with it.
 */
export function itemSize(item: Item): number {
  let size = 0
  for (const [name, value] of Object.entries(item)) {
    size += utf8Length(name) + valueSize(value)
  }

  return size
}

function valueSize(value: AttributeValue): number {
  if ('S' in value) {
    return utf8Length(value.S)
  }
  if ('N' in value) {
    return utf8Length(value.N)
  }
  if ('B' in value) {
    return value.B.length
  }
  if ('BOOL' in value || 'NULL' in value) {
    return 1
  }
  if ('M' in value) {
    return 3 + itemSize(value.M) + Object.keys(value.M).length
  }
  if ('L' in value) {
    return value.L.reduce((size, entry) => size + 1 + valueSize(entry), 3)
  }
  if ('SS' in value) {
    return value.SS.reduce((size, entry) => size + utf8Length(entry), 0)
  }
  if ('NS' in value) {
    return value.NS.reduce((size, entry) => size + utf8Length(entry), 0)
  }

  return value.BS.reduce((size, entry) => size + entry.length, 0)
}

function withTtl(item: Item, ttl: number | undefined): Item {
  if (ttl !== undefined && safeIntegerArgument(ttl, 'ttl') > 0) {
    item['ttl'] = {N: ttl.toString()}
  }

  return item
}

// A payload is stored as a DynamoDB string, so it must be UTF-8. A null payload is stored empty.
function payloadText(payload: Uint8Array | null, what: string): string {
  if (payload === null) {
    return ''
  }

  try {
    return utf8Decode(payload, what)
  } catch (error) {
    const reason = `${what} is not valid UTF-8 and cannot be stored as a string`
    throw new EvtError('invalidArgument', reason, {cause: error})
  }
}

function requiredString(item: Item, name: string): string {
  const value = optionalString(item, name)
  if (value === undefined) {
    throw invalid(`item has no ${name} attribute`)
  }

  return value
}

function requiredNumber(item: Item, name: string): bigint {
  const value = optionalNumber(item, name)
  if (value === undefined) {
    throw invalid(`item has no ${name} attribute`)
  }

  return value
}

function optionalString(item: Item, name: string): string | undefined {
  const value = item[name]
  if (value === undefined || 'NULL' in value) {
    return undefined
  }
  if (!('S' in value)) {
    throw invalid(`attribute ${name} is not a string`)
  }

  return value.S
}

function optionalNumber(item: Item, name: string): bigint | undefined {
  const value = item[name]
  if (value === undefined || 'NULL' in value) {
    return undefined
  }
  if (!('N' in value)) {
    throw invalid(`attribute ${name} is not a number`)
  }

  const number = parseNumber(value.N)
  if (number === undefined) {
    throw invalid(`attribute ${name} is not an integer: ${value.N}`)
  }

  return number
}

// An int64 in decimal, as strconv.ParseInt reads it: an optional sign, then digits.
function parseNumber(text: string): bigint | undefined {
  if (!/^[+-]?\d+$/.test(text)) {
    return undefined
  }

  try {
    return checkInt64(BigInt(text), 'number')
  } catch {
    return undefined
  }
}

function safeInteger(value: bigint, name: string): number {
  if (value < BigInt(Number.MIN_SAFE_INTEGER) || value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw invalid(`attribute ${name} is ${value}, past the integers it holds`)
  }

  return Number(value)
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
