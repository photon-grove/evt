// lambda.json: the Lambda wire formats (ADR 0005, X3) through @photon-grove/evt-lambda, checked as
// the Rust crate checks them in crates/evt-lambda/tests/fixtures.rs. The fixture is Rust-generated;
// this runner reads its records as Lambda would deliver them and generates nothing.
import {type JsonValue, encodeEvent, parseJson} from '@photon-grove/evt'
import {eventItem} from '@photon-grove/evt/dynamodb'
import {
  DecodeError,
  EventFilter,
  JournalFilter,
  type JournalWakeup,
  type StreamEvent,
  type StreamRecord,
  type StreamedEvent,
  decodeEvent,
  decodeWakeup,
  processEvents,
  processWakeups,
  streamItem,
  streamRecord,
  successResponse,
} from '@photon-grove/evt-lambda'

import {Checks, canonical, field, list, loadFixture, str} from './harness.js'

const checks = new Checks()

// The published entry point resolves through the exports map on this runtime.
const lambda: object = await import('@photon-grove/evt-lambda')
checks.equal('@photon-grove/evt-lambda loads', typeof lambda, 'object')

const fixture = await loadFixture('lambda.json')
const events = list(field(fixture, 'events'))
const wakeups = list(field(fixture, 'wakeups'))

filterVectors()
eventVectors()
wakeupVectors()
await batchVectors()

checks.run('emptyResponse', () => {
  checks.equal(
    'emptyResponse',
    canonical(successResponse()),
    canonical(field(fixture, 'emptyResponse')),
  )
})

checks.run('rowSortKey', () => {
  const sk = str(field(fixture, 'rowSortKey'))
  const row = streamRecord('INSERT', {pk: {S: 'journal#world:7'}, sk: {S: sk}}, '1')
  checks.equal('rowSortKey decodes to position 4', decodeWakeup(row)?.position, 4n)
})

checks.report()

function filterVectors(): void {
  const filters = field(fixture, 'filters')
  const patterns: [string, () => string][] = [
    ['everyEvent', () => new EventFilter().pattern()],
    [
      'accountEvents',
      () =>
        new EventFilter()
          .entityType('account')
          .eventType('account:opened')
          .eventTypePrefix('account:closed')
          .pattern(),
    ],
    ['journalRows', () => JournalFilter.rows().pattern()],
    ['journalCounters', () => JournalFilter.counters().pattern()],
    [
      'journalScopes',
      () => JournalFilter.counters().scope('world:7').scopePrefix('world:7/').pattern(),
    ],
  ]

  for (const [name, pattern] of patterns) {
    checks.run(`filters ${name}`, () => {
      checks.equal(`filters ${name}`, pattern(), str(field(filters, name)))
    })
  }
}

function eventVectors(): void {
  for (const entry of events) {
    const name = str(field(entry, 'name'))
    checks.run(`events ${name}`, () => {
      const record = recordOf(entry)
      checks.equal(
        `events ${name}: decoded`,
        decodedEvent(record),
        canonical(field(entry, 'decoded')),
      )

      const everyEvent = field(entry, 'everyEventFilter')
      checks.equal(
        `events ${name}: everyEventFilter`,
        new EventFilter().matches(record),
        everyEvent,
      )
    })
  }
}

function wakeupVectors(): void {
  for (const entry of wakeups) {
    const name = str(field(entry, 'name'))
    checks.run(`wakeups ${name}`, () => {
      const record = recordOf(entry)
      checks.equal(
        `wakeups ${name}: decoded`,
        decodedWakeup(record),
        canonical(field(entry, 'decoded')),
      )

      const rows = field(entry, 'rowsFilter')
      checks.equal(`wakeups ${name}: rowsFilter`, JournalFilter.rows().matches(record), rows)

      const counters = field(entry, 'countersFilter')
      checks.equal(
        `wakeups ${name}: countersFilter`,
        JournalFilter.counters().matches(record),
        counters,
      )
    })
  }
}

async function batchVectors(): Promise<void> {
  const batches = new Map(
    list(field(fixture, 'batches')).map((entry) => [str(field(entry, 'name')), entry]),
  )
  const take = (cases: JsonValue[], indexes: number[]): StreamEvent => ({
    Records: indexes.map((index) => recordOf(cases[index]!)),
  })

  // Events: the first succeeds, the head is skipped, the third fails in the handler. The third is
  // the first case's record carrying the stream's next event, as the Rust generator builds it.
  const opened = take(events, [0, 4])
  const deposit = depositRecord(recordOf(events[0]!))
  const batch: StreamEvent = {Records: [...(opened.Records ?? []), deposit]}

  const allSucceed = await processEvents(batch, new EventFilter(), () => {})
  checkBatch(batches, 'events: every record succeeds', batch, allSucceed.response())

  const handlerFails = await processEvents(batch, new EventFilter(), (streamed: StreamedEvent) => {
    if (streamed.event.type === 'account:deposited') {
      throw new Error('unavailable')
    }
  })
  checkBatch(
    batches,
    'events: the handler fails on the third record',
    batch,
    handlerFails.response(),
  )

  const corrupt = take(events, [0, 8, 1])
  const corruptOutcome = await processEvents(corrupt, new EventFilter(), () => {})
  checkBatch(
    batches,
    'events: the second record does not decode',
    corrupt,
    corruptOutcome.response(),
  )

  const woken: unknown[] = []
  const rows = take(wakeups, [0, 5, 8, 2, 7, 3])
  const coalesced = await processWakeups(rows, JournalFilter.rows(), (wakeup: JournalWakeup) => {
    woken.push(wakeupJson(wakeup))
  })
  const name = 'journal rows: one wake-up per scope, the highest position'
  checkBatch(batches, name, rows, coalesced.response())
  checks.run(`batches ${name}: wakeups`, () => {
    checks.equal(
      `batches ${name}: wakeups`,
      canonical(woken),
      canonical(field(batches.get(name)!, 'wakeups')),
    )
  })
}

function checkBatch(
  batches: Map<string, JsonValue>,
  name: string,
  batch: StreamEvent,
  response: unknown,
): void {
  checks.run(`batches ${name}`, () => {
    const entry = batches.get(name)
    if (entry === undefined) {
      throw new Error('the fixture has no such batch')
    }

    const sequenceNumbers = (batch.Records ?? []).map((record) => record.dynamodb?.SequenceNumber)
    checks.equal(
      `batches ${name}: sequenceNumbers`,
      canonical(sequenceNumbers),
      canonical(field(entry, 'sequenceNumbers')),
    )
    checks.equal(
      `batches ${name}: response`,
      canonical(response),
      canonical(field(entry, 'response')),
    )
  })
}

// The stream's next event, acct-1:2, deposited by the same command, at sequence number 110.
function depositRecord(first: StreamRecord): StreamRecord {
  const opened = decodeEvent(first)
  if (opened === undefined) {
    throw new Error('the first event case does not decode')
  }

  const item = streamItem(
    eventItem({...opened.event, id: 'acct-1:2', sequence: 2n, type: 'account:deposited'}),
  )

  return {
    ...first,
    dynamodb: {
      ...first.dynamodb,
      Keys: {pk: item['pk']!, sk: item['sk']!},
      NewImage: item,
      SequenceNumber: '110',
    },
  }
}

// A fixture record as the Lambda runtime hands it over: the JSON text parsed by JSON.parse.
function recordOf(entry: JsonValue): StreamRecord {
  return JSON.parse(canonical(field(entry, 'record'))) as StreamRecord
}

function decodedEvent(record: StreamRecord): string {
  let streamed
  try {
    streamed = decodeEvent(record)
  } catch (error) {
    return canonical({error: error instanceof DecodeError ? error.kind : 'other'})
  }

  if (streamed === undefined) {
    return canonical({skip: true})
  }

  return canonical({
    event: parseJson(encodeEvent(streamed.event)),
    idempotencyKey: streamed.idempotencyKey,
  })
}

function wakeupJson(wakeup: JournalWakeup): object {
  return {
    scope: wakeup.scope,
    position: wakeup.position === undefined ? null : wakeup.position.toString(),
    sequenceNumber: wakeup.sequenceNumber,
  }
}

function decodedWakeup(record: StreamRecord): string {
  const wakeup = decodeWakeup(record)

  return canonical(wakeup === undefined ? {skip: true} : {wakeup: wakeupJson(wakeup)})
}
