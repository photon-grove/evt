// NEW_IMAGE decode: the committed events in an event-log table's stream.

import {type SerializedEvent, isReserved} from '@photon-grove/evt'
import {eventFromItem, sortKey} from '@photon-grove/evt/dynamodb'

import {DecodeError} from './errors.js'
import {
  type StreamItem,
  type StreamRecord,
  eventName,
  itemFromStream,
  ownAttribute,
  sequenceNumber,
  streamViewType,
  stringOf,
} from './record.js'

/** A committed event, as one stream record delivered it. */
export interface StreamedEvent {
  /**
   * The event as it was stored. Upcasting is the consumer's: apply its upcasters before it decodes
   * the payload.
   */
  readonly event: SerializedEvent
  /** The record's sequence number in its shard. */
  readonly sequenceNumber: string
  /** When the change was made, in Unix seconds (approximate). */
  readonly approximateCreationDateTime: number | undefined
  /**
   * The key a consumer deduplicates on: the event ID, `{entityId}:{sequence}`. A redelivered record
   * carries the same event, so the key is stable across retries, and no two events share it.
   */
  readonly idempotencyKey: string
}

/**
 * Whether an item key is an event item of the event log: a string `pk` outside the reserved
 * namespaces (`SYSTEM#`, `command-outcome#`) and a number `sk` of at least 1.
 *
 * Every other item of the table is engine metadata: the stream head (`sk = -1`), the inline
 * snapshot (`sk = 0`), the feature marker and the rebuild fence (`SYSTEM#…`), and command outcomes
 * (`command-outcome#…`, at `sk = 1`).
 */
export function isEventKey(keys: StreamItem): boolean {
  const pk = stringOf(ownAttribute(keys, 'pk'))
  if (pk === undefined || isReserved(pk)) {
    return false
  }

  const sk = ownAttribute(keys, 'sk')?.N
  if (typeof sk !== 'string') {
    return false
  }

  const sequence = sortKey({sk: {N: sk}})

  return sequence !== undefined && sequence >= 1n
}

/**
 * Decodes the event a record inserted.
 *
 * Undefined for every record that is not the insert of an event item: a `MODIFY` or `REMOVE`
 * (events are append-only, so neither is a new event; a compaction or a time-to-live expiry removes
 * them), and the engine's metadata items ({@link isEventKey}). An event insert without a new image,
 * or whose item does not decode, throws a {@link DecodeError}.
 */
export function decodeEvent(record: StreamRecord): StreamedEvent | undefined {
  if (eventName(record) !== 'INSERT') {
    return undefined
  }

  const change = record.dynamodb
  const keys = isEmpty(change?.Keys) ? change?.NewImage : change?.Keys
  if (keys == null || !isEventKey(keys)) {
    return undefined
  }

  const sequence = sequenceNumber(record)
  // A null image, which Lambda never sends, reads as none, as the Rust crate reads it.
  const image = change?.NewImage
  if (image == null) {
    throw DecodeError.missingNewImage(sequence, streamViewType(record) ?? 'unset')
  }

  let event
  try {
    event = eventFromItem(itemFromStream(image))
  } catch (error) {
    throw DecodeError.invalidRecord(sequence, error)
  }

  return {
    event,
    sequenceNumber: sequence,
    approximateCreationDateTime: change?.ApproximateCreationDateTime,
    idempotencyKey: event.id,
  }
}

function isEmpty(item: StreamItem | null | undefined): boolean {
  return item == null || Object.keys(item).length === 0
}
