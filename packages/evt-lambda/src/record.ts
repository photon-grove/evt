// The DynamoDB Streams event a Lambda function receives from its event source mapping.
//
// The shapes follow the JSON Lambda delivers, so the parsed payload passes in as it is, and so does
// a `DynamoDBStreamEvent` typed by the aws-lambda typings package: every field is optional, and
// nothing here depends on an AWS package. Item images stay DynamoDB JSON (binary values are base64 text) until a
// decoder reads them, and only the fields a decoder reads are checked.

import {base64Decode, base64Encode} from '@photon-grove/evt'
import {type AttributeValue, type Item} from '@photon-grove/evt/dynamodb'

/** The principal DynamoDB names on a record of a deletion its time-to-live process made. */
export const TTL_PRINCIPAL = 'dynamodb.amazonaws.com'

/** The kind of change a record reports. Any other name reads as {@link UNKNOWN}. */
export type EventName = 'INSERT' | 'MODIFY' | 'REMOVE'

/** What a stream writes into each record, configured per table. */
export type StreamViewType = 'KEYS_ONLY' | 'NEW_IMAGE' | 'OLD_IMAGE' | 'NEW_AND_OLD_IMAGES'

/** The name an event name or view type this package does not know reads as. */
export const UNKNOWN = 'UNKNOWN'

/**
 * A DynamoDB JSON attribute value, as a stream record carries it. A well-formed value has exactly
 * one of the fields. Binary values (`B`, `BS`) are base64 text.
 */
export interface StreamAttributeValue {
  S?: string | undefined
  N?: string | undefined
  B?: string | undefined
  BOOL?: boolean | undefined
  NULL?: boolean | undefined
  M?: {[name: string]: StreamAttributeValue} | undefined
  L?: StreamAttributeValue[] | undefined
  SS?: string[] | undefined
  NS?: string[] | undefined
  BS?: string[] | undefined
}

/** An item image or key set, as a stream record carries it. */
export type StreamItem = {[name: string]: StreamAttributeValue}

/** The change itself: the item's keys, its images, and its place in the shard. */
export interface StreamChange {
  /** When the change was made, in Unix seconds (approximate). */
  ApproximateCreationDateTime?: number | undefined
  /** The item's key attributes. Present on every record, whatever the view type. */
  Keys?: StreamItem | undefined
  /** The item after the change (`NEW_IMAGE`, `NEW_AND_OLD_IMAGES`; absent on a `REMOVE`). */
  NewImage?: StreamItem | undefined
  /** The item before the change (`OLD_IMAGE`, `NEW_AND_OLD_IMAGES`; absent on an `INSERT`). */
  OldImage?: StreamItem | undefined
  /** The record's sequence number in its shard: the identifier a partial batch failure reports. */
  SequenceNumber?: string | undefined
  /** The record's size in bytes. */
  SizeBytes?: number | undefined
  /** The table's stream view type when the record was written. */
  StreamViewType?: string | undefined
}

/** Who made a change. DynamoDB sets it only on deletions its time-to-live process made. */
export interface UserIdentity {
  /** `Service` for a time-to-live deletion. */
  type?: string | undefined
  /** {@link TTL_PRINCIPAL} for a time-to-live deletion. */
  principalId?: string | undefined
}

/** One record of a batch. */
export interface StreamRecord {
  /** A unique identifier of the record. */
  eventID?: string | undefined
  /** The kind of change: an {@link EventName}, or a name this package reads as unknown. */
  eventName?: string | undefined
  /** The record format version. */
  eventVersion?: string | undefined
  /** `aws:dynamodb`. */
  eventSource?: string | undefined
  /** The table's region. */
  awsRegion?: string | undefined
  /** The change. */
  dynamodb?: StreamChange | undefined
  /** The ARN of the stream the record came from; it names the table. */
  eventSourceARN?: string | undefined
  /** Set on deletions made by the table's time-to-live process. */
  userIdentity?: UserIdentity | undefined
}

/** One invocation's batch: the records of one shard, in stream order, oldest first. */
export interface StreamEvent {
  Records?: readonly StreamRecord[] | undefined
}

/** The record's event name, or {@link UNKNOWN} for a name this package does not know. */
export function eventName(record: StreamRecord): EventName | typeof UNKNOWN {
  const name = record.eventName

  return name === 'INSERT' || name === 'MODIFY' || name === 'REMOVE' ? name : UNKNOWN
}

/**
 * The record's stream view type, {@link UNKNOWN} for a view type this package does not know, or
 * undefined when the record has none.
 */
export function streamViewType(record: StreamRecord): StreamViewType | typeof UNKNOWN | undefined {
  const viewType = record.dynamodb?.StreamViewType
  if (viewType === undefined) {
    return undefined
  }

  return viewType === 'KEYS_ONLY' ||
    viewType === 'NEW_IMAGE' ||
    viewType === 'OLD_IMAGE' ||
    viewType === 'NEW_AND_OLD_IMAGES'
    ? viewType
    : UNKNOWN
}

/** The record's sequence number in its shard, empty when it has none. */
export function sequenceNumber(record: StreamRecord): string {
  return record.dynamodb?.SequenceNumber ?? ''
}

/** The item's key attribute `name`, from the keys or, if the keys lack it, the new image. */
export function recordKey(record: StreamRecord, name: string): StreamAttributeValue | undefined {
  return ownAttribute(record.dynamodb?.Keys, name) ?? ownAttribute(record.dynamodb?.NewImage, name)
}

/** The partition key `pk`, if it is a string. */
export function recordPk(record: StreamRecord): string | undefined {
  return stringOf(recordKey(record, 'pk'))
}

/** Whether DynamoDB's time-to-live process made this change. */
export function isTtlDeletion(record: StreamRecord): boolean {
  return eventName(record) === 'REMOVE' && record.userIdentity?.principalId === TTL_PRINCIPAL
}

/**
 * A record of `name` for an item with these keys at `sequenceNumber`, written by a stream of view
 * type `viewType` (the default `NEW_IMAGE` when the record has a new image, otherwise `KEYS_ONLY`).
 * For tests: build items with `@photon-grove/evt/dynamodb`'s encoders and {@link streamItem}.
 */
export function streamRecord(
  name: EventName,
  keys: StreamItem,
  sequence: string,
  options: {newImage?: StreamItem; oldImage?: StreamItem; viewType?: StreamViewType} = {},
): StreamRecord {
  const change: StreamChange = {Keys: keys, SequenceNumber: sequence}
  if (options.newImage !== undefined) {
    change.NewImage = options.newImage
  }
  if (options.oldImage !== undefined) {
    change.OldImage = options.oldImage
  }
  change.StreamViewType =
    options.viewType ?? (options.newImage === undefined ? 'KEYS_ONLY' : 'NEW_IMAGE')

  return {eventName: name, eventVersion: '1.1', eventSource: 'aws:dynamodb', dynamodb: change}
}

/** An item as a stream record carries it: DynamoDB JSON, with binary values as base64 text. */
export function streamItem(item: Item): StreamItem {
  const out: StreamItem = {}
  for (const [name, value] of Object.entries(item)) {
    setAttribute(out, name, streamValue(value))
  }

  return out
}

function streamValue(value: AttributeValue): StreamAttributeValue {
  if ('B' in value) {
    return {B: base64Encode(value.B)}
  }
  if ('BS' in value) {
    return {BS: value.BS.map(base64Encode)}
  }
  if ('M' in value) {
    return {M: streamItem(value.M)}
  }
  if ('L' in value) {
    return {L: value.L.map(streamValue)}
  }

  return value
}

/**
 * Reads a stream record's image as an `@photon-grove/evt/dynamodb` item, the shape the item codecs
 * decode, refusing a value that is not well-formed DynamoDB JSON: not exactly one known field of
 * the right type, or binary data that is not standard padded base64.
 */
export function itemFromStream(image: StreamItem): Item {
  const out: Item = {}
  for (const [name, value] of Object.entries(image)) {
    setAttribute(out, name, valueFromStream(value, name))
  }

  return out
}

function valueFromStream(value: StreamAttributeValue, path: string): AttributeValue {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new TypeError(`attribute ${path} is not a DynamoDB JSON value`)
  }

  const fields = Object.keys(value)
  const field = fields[0]
  if (fields.length !== 1 || field === undefined) {
    throw new TypeError(`attribute ${path} has ${fields.length} type fields; it needs one`)
  }

  const inner: unknown = (value as Record<string, unknown>)[field]
  switch (field) {
    case 'S':
    case 'N':
      return {[field]: text(inner, path)} as AttributeValue
    case 'BOOL':
    case 'NULL':
      if (typeof inner !== 'boolean') {
        throw new TypeError(`attribute ${path} is not a boolean`)
      }

      return {[field]: inner} as AttributeValue
    case 'B':
      return {B: binary(inner, path)}
    case 'SS':
    case 'NS':
      return {[field]: list(inner, path).map((entry) => text(entry, path))} as AttributeValue
    case 'BS':
      return {BS: list(inner, path).map((entry) => binary(entry, path))}
    case 'L':
      return {
        L: list(inner, path).map((entry, index) =>
          valueFromStream(entry as StreamAttributeValue, `${path}[${index}]`),
        ),
      }
    case 'M': {
      if (typeof inner !== 'object' || inner === null || Array.isArray(inner)) {
        throw new TypeError(`attribute ${path} is not a map`)
      }

      const map: Item = {}
      for (const [name, entry] of Object.entries(inner)) {
        setAttribute(map, name, valueFromStream(entry as StreamAttributeValue, `${path}.${name}`))
      }

      return {M: map}
    }
    default:
      throw new TypeError(`attribute ${path} has the unknown type ${field}`)
  }
}

function text(value: unknown, path: string): string {
  if (typeof value !== 'string') {
    throw new TypeError(`attribute ${path} is not a string`)
  }

  return value
}

function list(value: unknown, path: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new TypeError(`attribute ${path} is not a list`)
  }

  return value
}

function binary(value: unknown, path: string): Uint8Array {
  return base64Decode(text(value, path))
}

/** An item's own attribute `name`: undefined for a missing item (or a null one) or attribute. */
export function ownAttribute(
  item: StreamItem | null | undefined,
  name: string,
): StreamAttributeValue | undefined {
  return item != null && Object.hasOwn(item, name) ? item[name] : undefined
}

// Sets an attribute as an own property, so one named `__proto__` is kept as an attribute, as the
// Rust crate's map keeps it, instead of replacing the object's prototype.
function setAttribute<T>(item: Record<string, T>, name: string, value: T): void {
  Object.defineProperty(item, name, {value, enumerable: true, writable: true, configurable: true})
}

/** The string an attribute holds, if it is a string attribute. */
export function stringOf(value: StreamAttributeValue | undefined): string | undefined {
  return typeof value?.S === 'string' ? value.S : undefined
}
