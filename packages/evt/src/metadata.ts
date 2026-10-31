// Event metadata: the envelope fields stored with every event, mirroring Go's evt.Metadata.

import {DOCUMENT, nullableString, objectOrNull, stringField, stringMap} from './fields.js'
import {type JsonValue, parseJson, stringifyJson} from './json.js'

/** The system and entry point that initiated a command. */
export interface Origin {
  source: string
  endpoint: string
}

/**
 * Metadata stored with each event. Every key is always written, in this order, with absent values
 * as `null` (or `""` for `region`, `timestamp` and `commitId`, and `{}` for `extensions`).
 */
export interface Metadata {
  /** The command that produced the event. Used to reject duplicate commands. */
  commandId: string | null
  /** The W3C trace-context carrier. Its keys are written sorted. */
  trace: Map<string, string> | null
  origin: Origin | null
  /** An optional caller address. Decoding keeps a stored `""`. */
  address: string | null
  /** The region the command ran in. */
  region: string
  /** RFC 3339 UTC with second precision, such as `2026-01-02T03:04:05Z`. */
  timestamp: string
  /** Who issued the command. */
  principalId: string | null
  /**
   * The command that caused this one. The encoder writes the `commandId` when it is null, and
   * decoding leaves a missing value null.
   */
  causationId: string | null
  /** String-keyed application metadata. Its keys are written sorted. */
  extensions: Map<string, string>
  /** The commit that wrote the event, set by the engine. */
  commitId: string
}

/** Metadata with every field at its zero value: what `{}` or `null` decodes to. */
export function emptyMetadata(): Metadata {
  return {
    commandId: null,
    trace: null,
    origin: null,
    address: null,
    region: '',
    timestamp: '',
    principalId: null,
    causationId: null,
    extensions: new Map(),
    commitId: '',
  }
}

/** The causation ID the encoder writes: the one set, otherwise the command ID. */
export function effectiveCausationId(metadata: Metadata): string | null {
  return metadata.causationId ?? metadata.commandId
}

/** Encodes metadata as Go's canonical JSON. */
export function encodeMetadata(metadata: Metadata): string {
  return stringifyJson(metadataJson(metadata))
}

/** Decodes metadata JSON leniently, as Go does. */
export function decodeMetadata(json: string): Metadata {
  return metadataFromJson(parseJson(json, DOCUMENT))
}

/** The metadata's JSON shape, for documents that embed it. */
export function metadataJson(metadata: Metadata): object {
  return {
    commandId: metadata.commandId,
    trace: metadata.trace,
    origin: metadata.origin && {source: metadata.origin.source, endpoint: metadata.origin.endpoint},
    address: metadata.address,
    region: metadata.region,
    timestamp: metadata.timestamp,
    principalId: metadata.principalId,
    causationId: effectiveCausationId(metadata),
    extensions: metadata.extensions,
    commitId: metadata.commitId,
  }
}

/** Decodes metadata from a parsed value. Null is the zero value. */
export function metadataFromJson(value: JsonValue | undefined): Metadata {
  const object = objectOrNull(value, 'metadata')
  const metadata = emptyMetadata()
  if (object === null) {
    return metadata
  }

  metadata.commandId = nullableString(object, 'commandId', 'metadata')
  metadata.trace = stringMap(object.get('trace'), 'metadata.trace')
  metadata.origin = origin(object.get('origin'))
  metadata.address = nullableString(object, 'address', 'metadata')
  metadata.region = stringField(object, 'region', 'metadata')
  metadata.timestamp = stringField(object, 'timestamp', 'metadata')
  metadata.principalId = nullableString(object, 'principalId', 'metadata')
  metadata.causationId = nullableString(object, 'causationId', 'metadata')
  metadata.extensions = stringMap(object.get('extensions'), 'metadata.extensions') ?? new Map()
  metadata.commitId = stringField(object, 'commitId', 'metadata')

  return metadata
}

function origin(value: JsonValue | undefined): Origin | null {
  const object = objectOrNull(value, 'metadata.origin')
  if (object === null) {
    return null
  }

  return {
    source: stringField(object, 'source', 'metadata.origin'),
    endpoint: stringField(object, 'endpoint', 'metadata.origin'),
  }
}
