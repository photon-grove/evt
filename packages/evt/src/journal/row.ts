// Journal rows and counter rows: their stored payloads, and the view rows that hold them (storage
// format, "Rows and counters").

import {EvtError} from '../errors.js'
import {type SerializedEvent, encodeEvent, eventFromJson} from '../event.js'
import {type JsonValue, parseJson, quote} from '../json.js'
import {utf8Encode} from '../utf8.js'
import type {ViewRow} from '../views.js'
import {
  COUNTER_ENTITY_TYPE,
  COUNTER_OWNER,
  COUNTER_SORT_KEY,
  ROW_ENTITY_TYPE,
  counterPartitionKey,
  parseRowSortKey,
  rowPartitionKey,
  rowSortKey,
  structFields,
  u64,
} from './scope.js'

const decoder = new TextDecoder('utf-8', {fatal: true, ignoreBOM: true})

/** The default row-size cap, in bytes of a row's payload. */
export const DEFAULT_MAX_ROW_BYTES = 30_000

/** The largest row-size cap: a row fits inside DynamoDB's 380,000-byte single-item budget. */
export const MAX_ROW_BYTES = 370_000

/**
 * One row of a scope's journal: an event at the position it holds in the scope. The stored payload
 * is `{"scope":…,"position":…,"event":<the stored event>}`, the event encoded exactly as a stream
 * envelope's `detail`, and its length in bytes is the row's size, which the row-size guard
 * measures.
 */
export interface JournalRow {
  readonly scope: string
  /** The position in the scope, from 1. */
  readonly position: bigint
  /** The stream that wrote the event, which owns the row. */
  readonly owner: string
  /** The event as it was stored, allocations included. */
  readonly event: SerializedEvent
}

/** A journal row's payload text. */
export function encodeRowText(scope: string, position: bigint, event: SerializedEvent): string {
  return `{"scope":${quote(scope)},"position":${position},"event":${encodeEvent(event)}}`
}

/** A journal row's payload. */
export function encodeRow(scope: string, position: bigint, event: SerializedEvent): Uint8Array {
  return utf8Encode(encodeRowText(scope, position, event))
}

/** The size in bytes of the row an event would hold at `position` in `scope`. */
export function rowSize(scope: string, position: bigint, event: SerializedEvent): number {
  return encodeRow(scope, position, event).length
}

/** The view row that holds an event's journal row, owned by the event's stream. */
export function rowView(scope: string, position: bigint, event: SerializedEvent): ViewRow {
  return {
    pk: rowPartitionKey(scope),
    sk: rowSortKey(position),
    entityId: event.entityId,
    entityType: ROW_ENTITY_TYPE,
    payload: encodeRow(scope, position, event),
  }
}

/** The payload of a scope's counter row at `position`. */
export function counterPayload(scope: string, position: bigint): Uint8Array {
  return utf8Encode(`{"scope":${quote(scope)},"position":${position}}`)
}

/** The view row that holds a scope's counter: the highest position allocated, engine-owned. */
export function counterView(scope: string, position: bigint): ViewRow {
  return {
    pk: counterPartitionKey(scope),
    sk: COUNTER_SORT_KEY,
    entityId: COUNTER_OWNER,
    entityType: COUNTER_ENTITY_TYPE,
    payload: counterPayload(scope, position),
  }
}

/** A counter row's position, checking it is the counter of `scope`, or `invalidRecord`. */
export function readCounter(scope: string, row: ViewRow): bigint {
  const corrupt = (why: string) =>
    new EvtError('invalidRecord', `journal counter of ${scope}: ${why}`)
  if (row.pk !== counterPartitionKey(scope) || row.sk !== COUNTER_SORT_KEY) {
    throw corrupt(`found the row ${quote(row.pk)}/${quote(row.sk)}`)
  }

  const fields = structFields(parsePayload(row.payload, corrupt), ['scope', 'position'])
  const [named, position] = fields ?? []
  const value = position === undefined ? null : u64(position)
  if (typeof named !== 'string' || value === null) {
    throw corrupt('the payload does not decode')
  }
  if (named !== scope) {
    throw corrupt(`it names the scope ${quote(named)}`)
  }

  return value
}

/**
 * Decodes a stored journal row, checking that its key, owner and payload agree. A mismatch is
 * `invalidRecord`: the row isn't one this journal wrote.
 */
export function journalRowFromView(scope: string, row: ViewRow): JournalRow {
  const corrupt = (why: string) =>
    new EvtError(
      'invalidRecord',
      `journal row ${quote(row.pk)}/${quote(row.sk)} of ${scope}: ${why}`,
    )
  if (row.pk !== rowPartitionKey(scope)) {
    throw corrupt('it is in another partition')
  }
  const position = parseRowSortKey(row.sk)
  if (position === null) {
    throw corrupt('the sort key is not a position')
  }

  const decoded = decodeRow(parsePayload(row.payload, corrupt), corrupt)
  if (decoded.scope !== scope || decoded.position !== position) {
    throw corrupt(`the payload says ${quote(decoded.scope)} at ${decoded.position}`)
  }
  if (decoded.event.entityId !== row.entityId) {
    throw corrupt(
      `the owner ${quote(row.entityId)} is not the event's stream ${decoded.event.entityId}`,
    )
  }

  return {scope, position, owner: row.entityId, event: decoded.event}
}

/**
 * Decodes a journal row's stored payload, as a compaction chunk embeds it, checking it is a row of
 * `scope`. Its owner is its event's stream.
 */
export function decodeRowPayload(scope: string, payload: Uint8Array): JournalRow {
  const corrupt = (why: string) =>
    new EvtError('invalidRecord', `a compacted journal row of ${scope}: ${why}`)
  const decoded = decodeRow(parsePayload(payload, corrupt), corrupt)
  if (decoded.scope !== scope || decoded.position === 0n) {
    throw corrupt(`it says ${quote(decoded.scope)} at ${decoded.position}`)
  }

  return {scope, position: decoded.position, owner: decoded.event.entityId, event: decoded.event}
}

function decodeRow(
  value: JsonValue,
  corrupt: (why: string) => EvtError,
): {scope: string; position: bigint; event: SerializedEvent} {
  const [scope, position, event] = structFields(value, ['scope', 'position', 'event']) ?? []
  const at = position === undefined ? null : u64(position)
  if (typeof scope !== 'string' || at === null || !(event instanceof Map)) {
    throw corrupt('the payload does not decode')
  }

  try {
    return {scope, position: at, event: eventFromJson(event)}
  } catch (error) {
    throw corrupt(`the event does not decode: ${error instanceof Error ? error.message : error}`)
  }
}

function parsePayload(payload: Uint8Array, corrupt: (why: string) => EvtError): JsonValue {
  try {
    return parseJson(decoder.decode(payload), {duplicateKeys: 'refuse'})
  } catch (error) {
    throw corrupt(
      `the payload does not decode: ${error instanceof Error ? error.message : String(error)}`,
    )
  }
}
