// Journal wake-ups: rows and counters of a views-table stream, filtered and coalesced by scope.
import {stringifyJson} from '@photon-grove/evt'
import {parseRowSortKey, rowSortKey} from '@photon-grove/evt/journal'
import {describe, expect, test} from 'vitest'

import {failureResponse, processWakeups} from '../src/batch.ts'
import {
  JournalFilter,
  type JournalWakeup,
  coalesceWakeups,
  decodeWakeup,
  readCounter,
} from '../src/journal.ts'
import {type StreamEvent, itemFromStream, streamRecord} from '../src/record.ts'
import {counterItem, keysOf, keysOnly, lambdaMatches, rowSk} from './helpers.ts'

// A counter item whose payload is this text.
function payload(text: string) {
  return itemFromStream({...counterItem('world:7', 0n), payload: {S: text}})
}

// The update of a scope's counter, as a NEW_IMAGE stream writes it.
function counter(scope: string, position: bigint, sequence: string) {
  const image = counterItem(scope, position)

  return streamRecord('MODIFY', keysOf(image), sequence, {newImage: image})
}

function wake(scope: string, position: bigint | undefined, sequenceNumber: string): JournalWakeup {
  return {scope, position, sequenceNumber}
}

describe('decodeWakeup', () => {
  test('rows and counters wake their scope', () => {
    const row = keysOnly('INSERT', 'journal#world:7', rowSk(4), '1')
    expect(decodeWakeup(row)).toEqual(wake('world:7', 4n, '1'))

    const bare = keysOnly('MODIFY', 'journal-counter#world:7', 'COUNTER', '2')
    expect(decodeWakeup(bare)).toEqual(wake('world:7', undefined, '2'))

    bare.dynamodb!.NewImage = counterItem('world:7', 12n)
    expect(decodeWakeup(bare)?.position).toBe(12n)

    // Keys missing from the record are read from the new image, as for events.
    const imageOnly = streamRecord('MODIFY', {}, '3', {newImage: counterItem('world:7', 2n)})
    expect(decodeWakeup(imageOnly)).toEqual(wake('world:7', 2n, '3'))
  })

  test('other rows and removals are not wake-ups', () => {
    const records = [
      keysOnly('REMOVE', 'journal#world:7', rowSk(4), '1'),
      keysOnly('INSERT', 'account#acct-1', 'VIEW', '1'),
      keysOnly('INSERT', 'journalist#x', 'VIEW', '1'),
      streamRecord('INSERT', {}, '1'),
      {...keysOnly('INSERT', 'journal#world:7', rowSk(4), '1'), eventName: 'LATER'},
    ]
    for (const record of records) {
      expect(decodeWakeup(record)).toBeUndefined()
    }
  })

  test('keys the journal never writes are not wake-ups', () => {
    const records = [
      keysOnly('INSERT', 'journal#world:7', '4', '1'),
      keysOnly('INSERT', 'journal#world:7', 'VIEW', '1'),
      keysOnly('INSERT', 'journal#world:7', '99999999999999999999', '1'),
      keysOnly('INSERT', 'journal#', rowSk(1), '1'),
      keysOnly('INSERT', `journal#${'x'.repeat(513)}`, rowSk(1), '1'),
      keysOnly('INSERT', 'journal#a\0b', rowSk(1), '1'),
      keysOnly('INSERT', 'journal-counter#world:7', 'VIEW', '1'),
      keysOnly('INSERT', 'journal-counter#', 'COUNTER', '1'),
      streamRecord('INSERT', {pk: {S: 'journal#world:7'}, sk: {N: '4'}}, '1'),
    ]
    for (const record of records) {
      expect(decodeWakeup(record)).toBeUndefined()
    }

    // 512 bytes is the longest scope, counted in UTF-8: 256 two-byte characters.
    expect(
      decodeWakeup(keysOnly('INSERT', `journal#${'é'.repeat(256)}`, rowSk(1), '1')),
    ).toBeDefined()
    expect(
      decodeWakeup(keysOnly('INSERT', `journal#${'é'.repeat(257)}`, rowSk(1), '1')),
    ).toBeUndefined()
  })

  test('a counter image that does not decode still wakes the scope, with no position', () => {
    const images = [
      counterItem('world:8', 1n),
      {...counterItem('world:7', 1n), sk: {S: 'VIEW'}},
      {...counterItem('world:7', 1n), payload: {S: 'not json'}},
      {...counterItem('world:7', 1n), payload: {B: 'AAE='}},
      {...counterItem('world:7', 1n), entityID: {BOOL: true}},
      {...counterItem('world:7', 1n), ttl: {N: '1.5'}},
      {...counterItem('world:7', 1n), extra: {B: 'not base64!'}},
    ]

    for (const image of images) {
      const record = streamRecord(
        'MODIFY',
        {pk: {S: 'journal-counter#world:7'}, sk: {S: 'COUNTER'}},
        '1',
        {
          newImage: image,
        },
      )
      expect(decodeWakeup(record)).toEqual(wake('world:7', undefined, '1'))
    }
  })
})

describe('the journal keys', () => {
  test('a row sort key is 20 digits of a u64', () => {
    expect(rowSortKey(4n)).toBe('00000000000000000004')
    expect(parseRowSortKey('00000000000000000004')).toBe(4n)
    expect(parseRowSortKey('18446744073709551615')).toBe(18_446_744_073_709_551_615n)
    expect(parseRowSortKey('18446744073709551616')).toBeNull()
    expect(parseRowSortKey('0000000000000000004')).toBeNull()
    expect(parseRowSortKey('+0000000000000000004')).toBeNull()
  })

  test('a counter payload reads as the Rust journal reads it', () => {
    expect(readCounter('world:7', payload('{"scope":"world:7","position":12}'))).toBe(12n)
    expect(readCounter('world:7', payload('{"position":3,"scope":"world:7","extra":1}'))).toBe(3n)
    expect(readCounter('world:7', payload('["world:7",5]'))).toBe(5n)
    expect(readCounter('world:7', payload(' {"scope":"world:7","position":0} '))).toBe(0n)
    expect(
      readCounter('world:7', payload('{"scope":"world:7","position":18446744073709551615}')),
    ).toBe(18_446_744_073_709_551_615n)
    expect(
      readCounter('world:7', {...counterItem('world:7', 7n), payload: {N: '7'}} as never),
    ).toBe(undefined)

    for (const text of [
      '{"scope":"world:7","position":18446744073709551616}',
      '{"scope":"world:7","position":-1}',
      '{"scope":"world:7","position":-0}',
      '{"scope":"world:7","position":1.0}',
      '{"scope":"world:7","position":1e3}',
      '{"scope":"world:7","position":"1"}',
      '{"scope":"world:7","position":1,"position":2}',
      '{"scope":"world:7"}',
      '{"scope":"world:8","position":1}',
      '["world:7",5,6]',
      '{"scope":"world:7","position":1} x',
    ]) {
      expect({text, position: readCounter('world:7', payload(text))}).toEqual({
        text,
        position: undefined,
      })
    }
  })
})

test('wake-ups coalesce by scope in first-appearance order', () => {
  expect(
    coalesceWakeups([
      wake('b', 3n, '1'),
      wake('a', undefined, '2'),
      wake('b', 5n, '3'),
      wake('b', 4n, '4'),
      wake('a', 1n, '5'),
    ]),
  ).toEqual([wake('b', 5n, '1'), wake('a', 1n, '2')])
})

describe('processWakeups', () => {
  const rows: StreamEvent = {
    Records: [
      keysOnly('INSERT', 'journal#a', rowSk(1), '1'),
      keysOnly('INSERT', 'account#acct-1', 'VIEW', '2'),
      keysOnly('INSERT', 'journal#b', rowSk(1), '3'),
      keysOnly('INSERT', 'journal#a', rowSk(2), '4'),
      keysOnly('INSERT', 'journal#c', rowSk(9), '5'),
    ],
  }

  test('a batch wakes each scope once and reports the first record not done', async () => {
    const woken: [string, bigint | undefined][] = []
    const outcome = await processWakeups(rows, JournalFilter.rows(), (wakeup) => {
      woken.push([wakeup.scope, wakeup.position])
    })

    expect(outcome.isSuccess()).toBe(true)
    expect(outcome.completed).toBe(5)
    expect(woken).toEqual([
      ['a', 2n],
      ['b', 1n],
      ['c', 9n],
    ])

    const failed = await processWakeups(rows, JournalFilter.rows(), (wakeup) => {
      if (wakeup.scope === 'b') {
        throw new Error('rebuild in progress')
      }
    })
    expect(failed.handled).toBe(1)
    expect(failed.completed).toBe(2)
    expect(failed.response()).toEqual(failureResponse('3'))
  })

  test('a stray key in the journal partitions is acknowledged, never a poison record', async () => {
    const stray: StreamEvent = {Records: [...rows.Records!]}
    ;(stray.Records as unknown[])[3] = keysOnly('INSERT', 'journal#a', 'VIEW', '4')

    const woken: [string, bigint | undefined][] = []
    const outcome = await processWakeups(stray, JournalFilter.rows(), (wakeup) => {
      woken.push([wakeup.scope, wakeup.position])
    })

    expect(woken).toEqual([
      ['a', 1n],
      ['b', 1n],
      ['c', 9n],
    ])
    expect(outcome.isSuccess()).toBe(true)
  })

  test('counter wake-ups carry the counter, and a failed scope wakes again from its first record', async () => {
    const counters: StreamEvent = {
      Records: [
        counter('a', 3n, '1'),
        counter('b', 7n, '2'),
        counter('a', 4n, '3'),
        counter('c', 1n, '4'),
        counter('b', 8n, '5'),
      ],
    }

    const woken: [string, bigint | undefined][] = []
    const outcome = await processWakeups(counters, JournalFilter.counters(), (wakeup) => {
      woken.push([wakeup.scope, wakeup.position])
      if (wakeup.scope === 'b') {
        throw new Error('rebuild in progress')
      }
    })

    expect(woken, 'c is not attempted').toEqual([
      ['a', 4n],
      ['b', 8n],
    ])
    expect(outcome.response()).toEqual(failureResponse('2'))
    expect(outcome.completed).toBe(1)

    const ignored = await processWakeups(counters, JournalFilter.rows(), () => {
      throw new Error('never called')
    })
    expect(ignored.isSuccess()).toBe(true)
    expect(ignored.handled).toBe(0)
  })
})

describe('JournalFilter', () => {
  test('the patterns are stable', () => {
    expect(JournalFilter.counters().pattern()).toBe(
      '{"eventName":["INSERT","MODIFY"],"dynamodb":{"Keys":{"pk":{"S":[{"prefix":"journal-counter#"}]}}}}',
    )
    expect(JournalFilter.rows().scope('world:7').scopePrefix('world:7/').pattern()).toBe(
      '{"eventName":["INSERT","MODIFY"],"dynamodb":{"Keys":{"pk":{"S":["journal#world:7",{"prefix":"journal#world:7/"}]}}}}',
    )
  })

  test('a scope that is not valid is refused', () => {
    expect(() => JournalFilter.rows().scope('')).toThrow(RangeError)
    expect(() => JournalFilter.rows().scope('a\0b')).toThrow(RangeError)
    expect(() => JournalFilter.rows().scope('x'.repeat(513))).toThrow(RangeError)
  })

  test('the handler test agrees with the pattern', () => {
    const filters = [
      JournalFilter.rows(),
      JournalFilter.counters(),
      JournalFilter.rows().scope('world:7'),
      JournalFilter.counters().scopePrefix('world:7/').scope('world:8'),
    ]
    const records = [
      keysOnly('INSERT', 'journal#world:7', rowSk(1), '1'),
      keysOnly('MODIFY', 'journal#world:7/place:3', rowSk(1), '1'),
      keysOnly('REMOVE', 'journal#world:7', rowSk(1), '1'),
      keysOnly('MODIFY', 'journal-counter#world:7', 'COUNTER', '1'),
      keysOnly('INSERT', 'journal-counter#world:7/place:3', 'COUNTER', '1'),
      keysOnly('INSERT', 'journal-counter#world:8', 'COUNTER', '1'),
      keysOnly('INSERT', 'account#acct-1', 'VIEW', '1'),
    ]

    for (const filter of filters) {
      const pattern = filter.pattern()
      for (const record of records) {
        expect(filter.matches(record), `${pattern} on ${stringifyJson(record)}`).toBe(
          lambdaMatches(pattern, record),
        )
      }
    }

    const hits = (filter: JournalFilter) =>
      records.filter((record) => filter.matches(record)).length
    expect(filters.map(hits)).toEqual([2, 3, 1, 2])
  })
})

test('a record with null keys matches no journal filter', () => {
  const record = {eventName: 'INSERT', dynamodb: {Keys: null}}

  expect(JournalFilter.rows().matches(record as never)).toBe(false)
  expect(decodeWakeup(record as never)).toBeUndefined()
})
