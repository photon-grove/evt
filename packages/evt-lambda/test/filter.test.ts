// EventFilter: the mapping pattern, and the handler test that must agree with it.
import {describe, expect, test} from 'vitest'

import {FilterError} from '../src/errors.ts'
import {EventFilter, MAX_PATTERN_BYTES} from '../src/filter.ts'
import {type StreamRecord, streamRecord} from '../src/record.ts'
import {event, eventRecord, lambdaMatches} from './helpers.ts'

function refusal(run: () => unknown): FilterError {
  try {
    run()
  } catch (error) {
    if (error instanceof FilterError) {
      return error
    }

    throw error
  }

  throw new Error('nothing was thrown')
}

describe('EventFilter', () => {
  test('the handler test agrees with the pattern', () => {
    const filters = [
      new EventFilter(),
      new EventFilter().entityType('account'),
      new EventFilter().eventType('account:opened', 'order:placed'),
      new EventFilter()
        .entityType('account', 'order')
        .eventTypePrefix('account:')
        .eventType('order:placed'),
    ]

    const head = streamRecord('INSERT', {pk: {S: 'e-1'}, sk: {N: '-1'}}, '1', {
      newImage: {pk: {S: 'e-1'}, sk: {N: '-1'}, head: {N: '1'}},
    })
    const eventRecords = (
      [
        ['account', 'account:opened'],
        ['account', 'account:closed'],
        ['order', 'order:placed'],
        ['order', 'order:shipped'],
        ['ledger', 'ledger:added'],
      ] as const
    ).map(([entityType, eventType]) => eventRecord(event(entityType, eventType)))
    const modify: StreamRecord = {...eventRecords[0]!, eventName: 'MODIFY'}
    const keysOnly: StreamRecord = {
      ...eventRecords[0]!,
      dynamodb: {...eventRecords[0]!.dynamodb, NewImage: undefined},
    }
    const records = [...eventRecords, head, modify, keysOnly]

    for (const filter of filters) {
      const pattern = filter.pattern()
      for (const record of records) {
        expect(filter.matches(record), `${pattern} on ${JSON.stringify(record)}`).toBe(
          lambdaMatches(pattern, record),
        )
      }
    }

    const all = new EventFilter()
    expect(records.filter((record) => all.matches(record))).toHaveLength(5)

    const accounts = filters[3]!
    expect(accounts.matchesEvent(event('account', 'account:closed'))).toBe(true)
    expect(accounts.matchesEvent(event('order', 'order:placed'))).toBe(true)
    expect(accounts.matchesEvent(event('order', 'order:shipped'))).toBe(false)
    expect(accounts.matchesEvent(event('ledger', 'account:opened'))).toBe(false)
  })

  test('empty values and oversized patterns are refused', () => {
    const emptyPrefix = refusal(() => new EventFilter().eventTypePrefix('').pattern())
    expect(emptyPrefix.kind).toBe('empty')
    expect(emptyPrefix.message).toBe('a filter event type cannot be empty')

    const emptyEntity = refusal(() => new EventFilter().entityType('').pattern())
    expect(emptyEntity.what).toBe('entity type')

    const types = Array.from({length: 400}, (_, n) => `account:event-${n}`)
    const tooLong = refusal(() => new EventFilter().eventType(...types).pattern())
    expect(tooLong.kind).toBe('tooLong')
    expect(tooLong.max).toBe(MAX_PATTERN_BYTES)
    expect(tooLong.length).toBeGreaterThan(MAX_PATTERN_BYTES)
  })

  test('the length limit counts UTF-8 bytes', () => {
    // 2000 UTF-16 units, but 5000 bytes: é is two bytes and € three.
    const wide = 'é'.repeat(1000) + '€'.repeat(1000)
    expect(refusal(() => new EventFilter().eventType(wide).pattern()).kind).toBe('tooLong')
  })

  test('the patterns are stable, with values in byte order and nothing escaped beyond JSON', () => {
    expect(new EventFilter().pattern()).toBe(
      '{"eventName":["INSERT"],"dynamodb":{"NewImage":{"metadata":{"S":[{"exists":true}]},"type":{"S":[{"exists":true}]}}}}',
    )
    expect(
      new EventFilter()
        .entityType('order', 'account')
        .eventType('😀', 'ü<b>', '！', 'Z', 'a')
        .pattern(),
    ).toBe(
      '{"eventName":["INSERT"],"dynamodb":{"NewImage":{"entityType":{"S":["account","order"]},"metadata":{"S":[{"exists":true}]},"type":{"S":["Z","a","ü<b>","！","😀"]}}}}',
    )
  })

  test('a filter is immutable', () => {
    const base = new EventFilter()
    const accounts = base.entityType('account')

    expect(base.matchesEvent(event('order', 'order:placed'))).toBe(true)
    expect(accounts.matchesEvent(event('order', 'order:placed'))).toBe(false)
  })
})

test('a record with a null new image matches no filter', () => {
  const record = {...eventRecord(event('account', 'account:opened')), dynamodb: {NewImage: null}}

  expect(new EventFilter().matches(record as never)).toBe(false)
})
