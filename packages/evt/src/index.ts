// @photon-grove/evt: event sourcing for TypeScript, wire-compatible with the Go and Rust evt
// implementations (docs/adr/0009-typescript-implementation.md). The storage contract is
// docs/reference/storage-format.md, and compat/fixtures holds its executable form.

export {
  type Aggregate,
  type AggregateDefinition,
  type Decision,
  type EventDefinition,
  type EventDefinitions,
  type EventOf,
  type Rejection,
  type Upcaster,
  defineAggregate,
  defineEvents,
  jsonUpcaster,
} from './aggregate.js'
export {base64Decode, base64Encode} from './base64.js'
export {
  type Codec,
  type JsonOptions,
  type StandardResult,
  type StandardSchemaV1,
  json,
} from './codec.js'
export {encodeEnvelope, decodeEnvelope, messageAttributes} from './envelope.js'
export {
  CommitTooLargeError,
  CompactionUncoveredError,
  ConflictError,
  EvtError,
  type EvtErrorCode,
  ReplayError,
  ReservedEntityIdError,
  SequenceGapError,
  SnapshotError,
  StreamHeadDriftError,
  isConflict,
  isRetryable,
} from './errors.js'
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
export type {CallOptions, Commit, NewSnapshot, Repository} from './repository.js'
export {
  DEFAULT_RETRY_POLICY,
  type RetryPolicy,
  retryDelay,
} from './retry.js'
export {MAX_COMMIT_ITEM_BYTES, snapshotCommitSize, snapshotEventsDue} from './snapshots.js'
export {compareUtf8, utf8Length} from './utf8.js'
export {
  DEFAULT_SNAPSHOT_INTERVAL,
  type ExecuteOptions,
  type ExecuteResult,
  type Loaded,
  type RetryOptions,
  Store,
  type StoreOptions,
} from './store.js'
