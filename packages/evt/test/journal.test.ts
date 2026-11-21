// The journal's pure pieces: scopes, sort keys, allocations in metadata, rows, counters and the
// order-independent replay, ported from the Rust crate's unit tests. compat/fixtures/journal.json
// pins the bytes, through packages/evt-compat.
import {describe, expect, test} from 'vitest'

import {EvtError} from '../src/errors.ts'
import type {SerializedEvent} from '../src/event.ts'
import {replay} from '../src/journal/replay.ts'
import {counterView, journalRowFromView, readCounter, rowSize, rowView} from '../src/journal/row.ts'
import {
  ALLOCATION_KEY,
  MAX_POSITION,
  MAX_SCOPE_BYTES,
  allocations,
  checkScope,
  encodeAllocations,
  parseRowSortKey,
  rowSortKey,
  setAllocations,
} from '../src/journal/scope.ts'
import {emptyMetadata} from '../src/metadata.ts'
import {MAX_VIEW_PK_BYTES} from '../src/projection.ts'
import {utf8Length} from '../src/utf8.ts'

function code(run: () => unknown): string | undefined {
  try {
    run()
  } catch (error) {
    return error instanceof EvtError ? error.code : 'not an EvtError'
  }

  return undefined
}

function event(entityId = 'ledger-1', sequence = 2n): SerializedEvent {
  const stored: SerializedEvent = {
    id: `${entityId}:${sequence}`,
    sequence,
    type: 'ledger:added',
    version: 1,
    entityId,
    entityType: 'ledger',
    payload: new TextEncoder().encode('{"amount":5}'),
    metadata: emptyMetadata(),
  }
  setAllocations(stored, [{scope: 'world:1', position: 4n}])

  return stored
}

describe('scopes', () => {
  test('are 1 to 512 bytes of UTF-8 without U+0000', () => {
    for (const good of ['world:7', 'é/\u{10000}', 'a'.repeat(MAX_SCOPE_BYTES)]) {
      expect(checkScope(good)).toBe(good)
    }
    for (const bad of ['', 'a'.repeat(MAX_SCOPE_BYTES + 1), 'nul\0', 'lone \ud800']) {
      expect({bad, code: code(() => checkScope(bad))}).toEqual({bad, code: 'invalidJournal'})
    }
  })

  test("at the cap, its keys fit a view's partition key", () => {
    const scope = 'é'.repeat(MAX_SCOPE_BYTES / 2)
    expect(utf8Length(`journal-counter#${scope}`)).toBeLessThanOrEqual(MAX_VIEW_PK_BYTES)
  })

  test('sort keys order numerically as bytes, and parse back', () => {
    const positions = [1n, 2n, 9n, 10n, 99n, 100n, MAX_POSITION - 1n, MAX_POSITION]
    const keys = positions.map(rowSortKey)
    expect(keys.every((key, index) => index === 0 || keys[index - 1]! < key)).toBe(true)
    expect(rowSortKey(5n)).toBe('00000000000000000005')
    expect(rowSortKey(MAX_POSITION)).toBe('18446744073709551615')
    for (const position of positions) {
      expect(parseRowSortKey(rowSortKey(position))).toBe(position)
    }
    for (const bad of [
      '',
      '5',
      '0000000000000000000x',
      '000000000000000000050',
      '99999999999999999999',
    ]) {
      expect({bad, position: parseRowSortKey(bad)}).toEqual({bad, position: null})
    }
  })
})

describe('allocations', () => {
  test('round-trip through metadata in canonical form', () => {
    const list = [
      {scope: 'world:1', position: 17n},
      {scope: 'world:1/place:9', position: 3n},
    ]
    expect(encodeAllocations(list)).toBe(
      '[{"scope":"world:1","position":17},{"scope":"world:1/place:9","position":3}]',
    )

    const stored = event()
    setAllocations(stored, list)
    expect(allocations(stored.metadata)).toEqual(list)
    setAllocations(stored, [])
    expect(stored.metadata.extensions.has(ALLOCATION_KEY)).toBe(false)
    expect(allocations(stored.metadata)).toEqual([])
  })

  test('a malformed or inconsistent allocation is refused', () => {
    for (const [name, value] of [
      ['not json', '{'],
      ['not an array', '{"scope":"a","position":1}'],
      ['position zero', '[{"scope":"a","position":0}]'],
      ['negative', '[{"scope":"a","position":-1}]'],
      ['fractional', '[{"scope":"a","position":1.5}]'],
      ['past a u64', '[{"scope":"a","position":18446744073709551616}]'],
      ['empty scope', '[{"scope":"","position":1}]'],
      ['unsorted', '[{"scope":"b","position":1},{"scope":"a","position":1}]'],
      ['repeated', '[{"scope":"a","position":1},{"scope":"a","position":2}]'],
      ['a repeated field', '[{"scope":"a","scope":"b","position":1}]'],
    ] as const) {
      const metadata = {...emptyMetadata(), extensions: new Map([[ALLOCATION_KEY, value]])}
      expect({name, code: code(() => allocations(metadata))}).toEqual({
        name,
        code: 'invalidJournal',
      })
    }
  })
})

describe('rows and counters', () => {
  test("a row round-trips, and its size is its payload's length", () => {
    const stored = event()
    const view = rowView('world:1', 4n, stored)
    expect([view.pk, view.sk, view.entityId, view.entityType]).toEqual([
      'journal#world:1',
      '00000000000000000004',
      'ledger-1',
      'journal',
    ])
    expect(new TextDecoder().decode(view.payload)).toMatch(
      /^\{"scope":"world:1","position":4,"event":\{"id":"ledger-1:2"/,
    )
    expect(rowSize('world:1', 4n, stored)).toBe(view.payload.length)

    const row = journalRowFromView('world:1', view)
    expect([row.position, row.owner]).toEqual([4n, 'ledger-1'])
    expect(row.event.metadata.extensions.get(ALLOCATION_KEY)).toBe(
      '[{"scope":"world:1","position":4}]',
    )
  })

  test('a row that disagrees with its key is refused', () => {
    const good = rowView('world:1', 4n, event())
    for (const [name, row] of [
      ['position', {...good, sk: rowSortKey(5n)}],
      ['owner', {...good, entityId: 'ledger-2'}],
      ['scope', {...good, pk: 'journal#world:2'}],
      ['payload', {...good, payload: new TextEncoder().encode('{')}],
      ['sort key', {...good, sk: '4'}],
    ] as const) {
      expect({name, code: code(() => journalRowFromView('world:1', row))}).toEqual({
        name,
        code: 'invalidRecord',
      })
    }
  })

  test("a counter round-trips and is the engine's", () => {
    const view = counterView('world:1', 12n)
    expect([view.pk, view.sk, view.entityId]).toEqual([
      'journal-counter#world:1',
      'COUNTER',
      'SYSTEM#JOURNAL',
    ])
    expect(new TextDecoder().decode(view.payload)).toBe('{"scope":"world:1","position":12}')
    expect(readCounter('world:1', view)).toBe(12n)
    expect(code(() => readCounter('world:2', view))).toBe('invalidRecord')
  })
})

describe('replay', () => {
  test('derives the same rows and counters from events in any order', () => {
    const first = event('a', 1n)
    setAllocations(first, [
      {scope: 'x', position: 1n},
      {scope: 'y', position: 1n},
    ])
    const second = event('b', 1n)
    setAllocations(second, [{scope: 'x', position: 2n}])
    const plain = event('c', 1n)
    setAllocations(plain, [])

    const forward = replay([first, second, plain, first])
    expect(forward).toEqual(replay([plain, second, first]))
    expect(forward.rows.map(({scope, position}) => `${scope}@${position}`)).toEqual([
      'x@1',
      'x@2',
      'y@1',
    ])
    expect(forward.counters).toEqual([
      {scope: 'x', position: 2n},
      {scope: 'y', position: 1n},
    ])
  })

  test('refuses two different records at one position, whatever their order', () => {
    const one = event('a', 1n)
    setAllocations(one, [{scope: 'x', position: 1n}])
    const other = event('b', 1n)
    setAllocations(other, [{scope: 'x', position: 1n}])
    expect(code(() => replay([one, other]))).toBe('invalidJournal')
    expect(code(() => replay([other, one]))).toBe('invalidJournal')
  })
})
