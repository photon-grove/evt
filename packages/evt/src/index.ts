// @photon-grove/evt: event sourcing for TypeScript, wire-compatible with the Go and Rust evt
// implementations (docs/adr/0009-typescript-implementation.md). The storage contract is
// docs/reference/storage-format.md, and compat/fixtures holds its executable form.

export {base64Decode, base64Encode} from './base64.js'
export {encodeEnvelope, decodeEnvelope, messageAttributes} from './envelope.js'
export {EvtError, type EvtErrorCode} from './errors.js'
export {
  type SerializedEvent,
  type SerializedSnapshot,
  decodeEvent,
  decodeSnapshot,
  encodeEvent,
  encodeSnapshot,
  eventId,
} from './event.js'
export {JsonNumber, type JsonValue, parseJson, stringifyJson} from './json.js'
export {
  type Metadata,
  type Origin,
  decodeMetadata,
  effectiveCausationId,
  emptyMetadata,
  encodeMetadata,
} from './metadata.js'
export {MAX_COMMIT_ITEM_BYTES, snapshotCommitSize, snapshotEventsDue} from './snapshots.js'
export {compareUtf8, utf8Length} from './utf8.js'
