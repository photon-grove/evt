// Serialized events and snapshots: the stored form every repository reads and writes, and their
// JSON encodings, which match Go's evt.SerializedEvent and evt.SerializedSnapshot.

import {
  DOCUMENT,
  bytesField,
  int64Argument,
  int64Field,
  objectOrNull,
  safeIntegerArgument,
  safeIntegerField,
  stringField,
  type JsonObject,
} from './fields.js'
import {parseJson, stringifyJson} from './json.js'
import {type Metadata, metadataFromJson, metadataJson} from './metadata.js'

/** An event as it is stored. Its JSON is the `detail` of a stream envelope. */
export interface SerializedEvent {
  /** `{entityId}:{sequence}`. */
  id: string
  /** The 1-based position of the event in its stream. */
  sequence: bigint
  /** The event kind, such as `account:opened`. */
  type: string
  /** The payload's schema version. */
  version: number
  /** The stream the event belongs to. */
  entityId: string
  /** The aggregate type of the entity. */
  entityType: string
  /**
   * The payload bytes, stored verbatim. Null only when decoded from a JSON `null`, which is
   * written back as `null`, as Go does.
   */
  payload: Uint8Array | null
  metadata: Metadata
}

/** A snapshot of aggregate state. A stream keeps at most one, its latest. */
export interface SerializedSnapshot {
  entityType: string
  entityId: string
  /** 1 for an entity's first snapshot, then one more for each. */
  sequence: bigint
  /** The last event sequence folded into the snapshot. */
  eventSequence: bigint
  /** The state's bytes, stored verbatim. Null only when decoded from a JSON `null`. */
  payload: Uint8Array | null
}

/** The event ID for a sequence of a stream: `{entityId}:{sequence}`. */
export function eventId(entityId: string, sequence: bigint): string {
  return `${entityId}:${sequence}`
}

/** Encodes an event as the JSON Go writes for it. */
export function encodeEvent(event: SerializedEvent): string {
  return stringifyJson(eventJson(event))
}

/** Decodes an event's JSON leniently, as Go does. */
export function decodeEvent(json: string): SerializedEvent {
  return eventFromJson(objectOrNull(parseJson(json, DOCUMENT), 'event'))
}

/** Encodes a snapshot as the JSON Go writes for it. Its entity ID key is `entityID`. */
export function encodeSnapshot(snapshot: SerializedSnapshot): string {
  return stringifyJson({
    entityType: snapshot.entityType,
    entityID: snapshot.entityId,
    sequence: int64Argument(snapshot.sequence, 'snapshot.sequence'),
    eventSequence: int64Argument(snapshot.eventSequence, 'snapshot.eventSequence'),
    payload: snapshot.payload,
  })
}

/** Decodes a snapshot's JSON leniently, as Go does. */
export function decodeSnapshot(json: string): SerializedSnapshot {
  const object = objectOrNull(parseJson(json, DOCUMENT), 'snapshot') ?? new Map()

  return {
    entityType: stringField(object, 'entityType', 'snapshot'),
    entityId: stringField(object, 'entityID', 'snapshot'),
    sequence: int64Field(object, 'sequence', 'snapshot'),
    eventSequence: int64Field(object, 'eventSequence', 'snapshot'),
    payload: bytesField(object, 'payload', 'snapshot'),
  }
}

export function eventJson(event: SerializedEvent): object {
  return {
    id: event.id,
    sequence: int64Argument(event.sequence, 'event.sequence'),
    type: event.type,
    version: safeIntegerArgument(event.version, 'event.version'),
    entityId: event.entityId,
    entityType: event.entityType,
    payload: event.payload,
    metadata: metadataJson(event.metadata),
  }
}

export function eventFromJson(value: JsonObject | null): SerializedEvent {
  const object = value ?? new Map()

  return {
    id: stringField(object, 'id', 'event'),
    sequence: int64Field(object, 'sequence', 'event'),
    type: stringField(object, 'type', 'event'),
    version: safeIntegerField(object, 'version', 'event'),
    entityId: stringField(object, 'entityId', 'event'),
    entityType: stringField(object, 'entityType', 'event'),
    payload: bytesField(object, 'payload', 'event'),
    metadata: metadataFromJson(object.get('metadata')),
  }
}
