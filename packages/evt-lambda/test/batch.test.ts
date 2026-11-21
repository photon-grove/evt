// Partial batch failures: in order, stop at the first failure, report exactly that record.
import {describe, expect, test} from 'vitest'

import {
  BatchOutcome,
  failureResponse,
  processEvents,
  processRecords,
  successResponse,
} from '../src/batch.ts'
import {DecodeError} from '../src/errors.ts'
import {EventFilter} from '../src/filter.ts'
import {type StreamEvent, type StreamRecord, eventName, streamRecord} from '../src/record.ts'
import {event, eventRecord} from './helpers.ts'

function batch(...records: StreamRecord[]): StreamEvent {
  return {Records: records}
}

describe('processEvents', () => {
  test('a batch that succeeds reports no failure', async () => {
    const head = eventRecord(event('account', 'account:opened'), '2')
    head.dynamodb!.Keys = {...head.dynamodb!.Keys, sk: {N: '-1'}}
    const records = batch(
      eventRecord(event('account', 'account:opened'), '1'),
      head,
      eventRecord(event('order', 'order:placed'), '3'),
      eventRecord(event('account', 'account:closed'), '4'),
    )

    const seen: string[] = []
    const outcome = await processEvents(
      records,
      new EventFilter().entityType('account'),
      (streamed) => {
        seen.push(streamed.sequenceNumber)
      },
    )

    expect(outcome.isSuccess()).toBe(true)
    expect(outcome.completed).toBe(4)
    expect(outcome.handled).toBe(2)
    expect(seen).toEqual(['1', '4'])
    expect(JSON.stringify(outcome.response())).toBe('{"batchItemFailures":[]}')
  })

  test('the first failure is reported and later records are not attempted', async () => {
    const records = batch(
      eventRecord(event('account', 'account:opened'), '11'),
      eventRecord(event('account', 'account:failing'), '12'),
      eventRecord(event('account', 'account:closed'), '13'),
    )

    const seen: string[] = []
    const failure = new Error('downstream unavailable')
    const outcome = await processEvents(records, new EventFilter(), async (streamed) => {
      seen.push(streamed.sequenceNumber)
      await Promise.resolve()

      if (streamed.event.type === 'account:failing') {
        throw failure
      }
    })

    expect(seen).toEqual(['11', '12'])
    expect(outcome.completed).toBe(1)
    expect(outcome.handled).toBe(1)
    expect(outcome.failure).toEqual({index: 1, sequenceNumber: '12', error: failure})
    expect(JSON.stringify(outcome.response())).toBe(
      '{"batchItemFailures":[{"itemIdentifier":"12"}]}',
    )
  })

  test('a record that does not decode fails the batch there', async () => {
    const corrupt = eventRecord(event('account', 'account:opened'), '22')
    delete corrupt.dynamodb!.NewImage!['metadata']
    const records = batch(
      eventRecord(event('account', 'account:opened'), '21'),
      corrupt,
      eventRecord(event('account', 'account:opened'), '23'),
    )

    let handled = 0
    const outcome = await processEvents(records, new EventFilter(), () => {
      handled++
    })

    expect(handled).toBe(1)
    expect(outcome.response()).toEqual(failureResponse('22'))
    expect(outcome.failure?.error).toBeInstanceOf(DecodeError)
  })

  test('a handler that returns a rejected promise fails like one that throws', async () => {
    const records = batch(eventRecord(event('account', 'account:opened'), '31'))

    const outcome = await processEvents(records, new EventFilter(), () =>
      Promise.reject(new Error('no')),
    )

    expect(outcome.response()).toEqual(failureResponse('31'))
  })
})

describe('processRecords', () => {
  test('it hands over every record', async () => {
    const records = batch(
      eventRecord(event('account', 'account:opened'), '1'),
      streamRecord('REMOVE', {}, '2'),
    )

    const outcome = await processRecords(records, (record) => {
      if (eventName(record) === 'REMOVE') {
        throw new Error('no removals')
      }
    })

    expect(outcome.completed).toBe(1)
    expect(outcome.response()).toEqual(failureResponse('2'))
  })

  test('an empty batch, or one with no Records, succeeds', async () => {
    const outcomes = await Promise.all(
      [{Records: []}, {}].map((empty) =>
        processRecords(empty, () => {
          throw new Error('never called')
        }),
      ),
    )

    for (const outcome of outcomes) {
      expect(outcome.isSuccess()).toBe(true)
      expect(outcome.completed).toBe(0)
    }
  })

  test('a record without a sequence number reports an empty identifier', async () => {
    const outcome = await processRecords(batch({eventName: 'INSERT'}), () => {
      throw new Error('fails')
    })

    expect(outcome.response()).toEqual({batchItemFailures: [{itemIdentifier: ''}]})
  })
})

test('the responses are the shapes Lambda reads', () => {
  expect(successResponse()).toEqual({batchItemFailures: []})
  expect(new BatchOutcome(2, 2).response()).toEqual(successResponse())
  expect(failureResponse('7')).toEqual({batchItemFailures: [{itemIdentifier: '7'}]})
})
