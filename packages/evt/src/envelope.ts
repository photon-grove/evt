// The stream envelope: the EventBridge-style message around each committed event, which consumers
// in every language unwrap. Go decodes it with projectors.StreamRecordFromEnvelope.

import {type SerializedEvent, eventFromJson, encodeEvent} from './event.js'
import {DOCUMENT, invalid, objectOrNull} from './fields.js'
import {RawJson, parseJson, stringifyJson} from './json.js'

// Go's zero time.Time, which the publisher left in the time field.
const ZERO_TIME = '0001-01-01T00:00:00Z'

/** Encodes the envelope for an event, byte for byte as the Go publisher wrote it. */
export function encodeEnvelope(event: SerializedEvent, source: string): string {
  return stringifyJson({
    version: '',
    id: event.id,
    'detail-type': event.type,
    source,
    account: '',
    time: ZERO_TIME,
    region: '',
    resources: null,
    detail: new RawJson(encodeEvent(event)),
  })
}

/**
 * Decodes the event from an envelope body: an SNS message, or an SQS body delivered raw. An event
 * with no entity ID or event ID is refused.
 */
export function decodeEnvelope(body: string): SerializedEvent {
  const envelope = objectOrNull(parseJson(body, DOCUMENT), 'envelope') ?? new Map()
  const event = eventFromJson(objectOrNull(envelope.get('detail'), 'envelope.detail'))
  if (event.entityId === '') {
    throw invalid('event envelope has no entity id')
  }
  if (event.id === '') {
    throw invalid('event envelope has no event id')
  }

  return event
}

/**
 * The message attributes the publisher set, all strings: `entityType` and `eventType`, then
 * `commandId` and `traceparent` when they aren't empty, and `correlationId`, the command ID or
 * else the traceparent.
 */
export function messageAttributes(event: SerializedEvent): Record<string, string> {
  const attributes: Record<string, string> = {
    entityType: event.entityType,
    eventType: event.type,
  }

  const commandId = event.metadata.commandId
  if (commandId !== null && commandId !== '') {
    attributes['commandId'] = commandId
    attributes['correlationId'] = commandId
  }

  const traceparent = event.metadata.trace?.get('traceparent')
  if (traceparent !== undefined && traceparent !== '') {
    attributes['traceparent'] = traceparent
    attributes['correlationId'] ??= traceparent
  }

  return attributes
}
