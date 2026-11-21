// Partial batch failures: process a batch in order, stop at the first failure, and tell Lambda
// where to resume.
//
// With `FunctionResponseTypes: ["ReportBatchItemFailures"]` on the event source mapping, a function
// returns `{"batchItemFailures":[{"itemIdentifier":"<SequenceNumber>"}]}`. For a stream, Lambda
// checkpoints before the lowest reported sequence number: the records ahead of it are done, and it
// delivers that record and every later one again. So the helpers here hand records to the handler
// in stream order, stop at the first record that fails (a decode error or a handler error), and
// report exactly that record. The records after it are not attempted: they come back in the retry,
// behind the one that failed, so per-item order holds.
//
// Every record from the failed one on may be delivered more than once, and so may the failed
// record's own side effects if it failed halfway: handlers must be idempotent. Key them on the
// event ID (`StreamedEvent.idempotencyKey`), or, for a projector, on the last sequence it applied
// per stream. Journal consumers are idempotent by construction: they read from their own cursor.
//
// Without `ReportBatchItemFailures`, a returned response is ignored, and only an error fails (and
// retries) the whole batch. An empty `itemIdentifier` also fails the whole batch, which is what a
// record with no sequence number gets. Bound the retries on the mapping (`MaximumRetryAttempts`,
// `MaximumRecordAgeInSeconds`) and give it an on-failure destination, or one poison record stops
// its shard until the record leaves the stream.

// Each record is handled only after the one before it succeeded, so the helpers await in loops on
// purpose.
/* oxlint-disable no-await-in-loop */

import {type StreamedEvent, decodeEvent} from './events.js'
import {type EventFilter} from './filter.js'
import {type JournalFilter, type JournalWakeup, coalesceIndexed, decodeWakeup} from './journal.js'
import {type StreamEvent, type StreamRecord, sequenceNumber} from './record.js'

/** The response a function returns for a partial batch failure. */
export interface BatchResponse {
  /** The failed records; empty when the whole batch succeeded. */
  batchItemFailures: BatchItemFailure[]
}

/** One failed record. */
export interface BatchItemFailure {
  /** The record's sequence number. */
  itemIdentifier: string
}

/** The response for a batch that succeeded whole. */
export function successResponse(): BatchResponse {
  return {batchItemFailures: []}
}

/** The response that makes Lambda resume from the record with this sequence number. */
export function failureResponse(sequence: string): BatchResponse {
  return {batchItemFailures: [{itemIdentifier: sequence}]}
}

/** The record a batch stopped at, and why. */
export interface RecordFailure {
  /** Its index in the batch. */
  readonly index: number
  /** Its sequence number, which the response reports. */
  readonly sequenceNumber: string
  /** The `DecodeError` or what the handler threw. */
  readonly error: unknown
}

/**
 * How a batch went: what to return to Lambda, and what to log. Return `outcome.response()` from
 * the function, or the failure is lost.
 */
export class BatchOutcome {
  /**
   * How many records of the batch, from the start, are done: every record when it succeeded,
   * otherwise the index of the record that failed.
   */
  readonly completed: number
  /** How many times the handler ran and succeeded. */
  readonly handled: number
  /** The record the batch stopped at, if it stopped. */
  readonly failure: RecordFailure | undefined

  constructor(completed: number, handled: number, failure?: RecordFailure) {
    this.completed = completed
    this.handled = handled
    this.failure = failure
  }

  /** The response to return from the function. */
  response(): BatchResponse {
    return this.failure === undefined
      ? successResponse()
      : failureResponse(this.failure.sequenceNumber)
  }

  /** Whether every record succeeded. */
  isSuccess(): boolean {
    return this.failure === undefined
  }
}

/** A handler of one item of a batch: it fails by throwing or by returning a rejected promise. */
export type BatchHandler<T> = (item: T) => void | Promise<void>

// The batch's records, a missing Records field read as an empty batch.
function recordsOf(event: StreamEvent): readonly StreamRecord[] {
  return event.Records ?? []
}

function failed(
  index: number,
  handled: number,
  record: StreamRecord,
  error: unknown,
): BatchOutcome {
  return new BatchOutcome(index, handled, {index, sequenceNumber: sequenceNumber(record), error})
}

/** Hands every record to `handler`, in order, stopping at the first error. */
export async function processRecords(
  event: StreamEvent,
  handler: BatchHandler<StreamRecord>,
): Promise<BatchOutcome> {
  const records = recordsOf(event)
  let handled = 0

  for (const [index, record] of records.entries()) {
    try {
      await handler(record)
    } catch (error) {
      return failed(index, handled, record, error)
    }

    handled++
  }

  return new BatchOutcome(records.length, handled)
}

/**
 * Hands every committed event of an event-log table's batch that `filter` matches to `handler`, in
 * order, stopping at the first record that does not decode or whose handler fails.
 *
 * Records that are not committed events (see `decodeEvent`) and events the filter does not match
 * are acknowledged without calling the handler. The filter is applied again here so the function
 * stays correct whatever its mapping's filter criteria say; pass `new EventFilter()` for every
 * event.
 */
export async function processEvents(
  event: StreamEvent,
  filter: EventFilter,
  handler: BatchHandler<StreamedEvent>,
): Promise<BatchOutcome> {
  const records = recordsOf(event)
  let handled = 0

  for (const [index, record] of records.entries()) {
    let streamed
    try {
      streamed = decodeEvent(record)
    } catch (error) {
      return failed(index, handled, record, error)
    }

    if (streamed === undefined || !filter.matchesEvent(streamed.event)) {
      continue
    }

    try {
      await handler(streamed)
    } catch (error) {
      return failed(index, handled, record, error)
    }

    handled++
  }

  return new BatchOutcome(records.length, handled)
}

/**
 * Wakes `handler` once per scope the batch's matching records name, coalesced (`coalesceWakeups`),
 * in the order the scopes first appear, stopping at the first failure.
 *
 * The handler reads the scope from its own cursor until it is caught up, and stores the new cursor.
 * Records `filter` does not match, and records that carry no wake-up (`decodeWakeup`), are
 * acknowledged without one.
 *
 * A handler error for a scope reports that scope's first record: the scopes woken before it are
 * done, and the failed scope and every later one wake again on the retry. A read that finds nothing
 * new is the only cost of a repeated wake-up. A wake-up whose retries run out is lost (see the
 * README, "Journal wake-ups"): the scope is read again on its next write.
 */
export async function processWakeups(
  event: StreamEvent,
  filter: JournalFilter,
  handler: BatchHandler<JournalWakeup>,
): Promise<BatchOutcome> {
  const records = recordsOf(event)
  const decoded: [number, JournalWakeup][] = []
  for (const [index, record] of records.entries()) {
    const wakeup = filter.matches(record) ? decodeWakeup(record) : undefined
    if (wakeup !== undefined) {
      decoded.push([index, wakeup])
    }
  }

  let handled = 0
  for (const [index, wakeup] of coalesceIndexed(decoded)) {
    try {
      await handler(wakeup)
    } catch (error) {
      return failed(index, handled, records[index]!, error)
    }

    handled++
  }

  return new BatchOutcome(records.length, handled)
}
