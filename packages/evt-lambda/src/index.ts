// @photon-grove/evt-lambda: AWS Lambda helpers for @photon-grove/evt deployments on DynamoDB, the
// consumer side of an event log (ADR 0005, X3), ported from the Rust crate photon-evt-lambda and
// held to its fixture, compat/fixtures/lambda.json.
//
// - Event decode (decodeEvent): the committed events in an event-log table's stream (NEW_IMAGE),
//   with the engine's own items (stream heads, snapshots, the feature marker, the rebuild fence,
//   command outcomes) skipped.
// - Filter patterns (EventFilter, JournalFilter): event source mapping filter patterns by event
//   type, entity type, or journal scope, and the same test for the handler.
// - Journal wake-ups (decodeWakeup): a views-table stream record (KEYS_ONLY is enough) that tells a
//   journal consumer which scope to read.
// - Partial batch failures (processEvents, processWakeups, processRecords): in-order processing
//   that stops at the first failure and returns the batchItemFailures response Lambda resumes from.
//
// The package depends on no Lambda runtime and no AWS package: StreamEvent is the payload Lambda
// delivers, and BatchResponse is the response it expects.

export {
  type BatchHandler,
  type BatchItemFailure,
  BatchOutcome,
  type BatchResponse,
  type RecordFailure,
  failureResponse,
  processEvents,
  processRecords,
  processWakeups,
  successResponse,
} from './batch.js'
export {DecodeError, type DecodeErrorKind, FilterError, type FilterErrorKind} from './errors.js'
export {type StreamedEvent, decodeEvent, isEventKey} from './events.js'
export {EventFilter, MAX_PATTERN_BYTES} from './filter.js'
export {JournalFilter, type JournalWakeup, coalesceWakeups, decodeWakeup} from './journal.js'
export {
  type EventName,
  type StreamAttributeValue,
  type StreamChange,
  type StreamEvent,
  type StreamItem,
  type StreamRecord,
  type StreamViewType,
  TTL_PRINCIPAL,
  UNKNOWN,
  type UserIdentity,
  eventName,
  isTtlDeletion,
  itemFromStream,
  recordKey,
  recordPk,
  sequenceNumber,
  streamItem,
  streamRecord,
  streamViewType,
} from './record.js'
