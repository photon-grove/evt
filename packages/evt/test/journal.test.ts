// The journal's pure pieces: scopes, sort keys, allocations in metadata, rows, counters and the
// order-independent replay, ported from the Rust crate's unit tests. compat/fixtures/journal.json
// pins the bytes, through packages/evt-compat.
import {describe, expect, test} from 'vitest'

import {defineAggregate, defineEvents} from '../src/aggregate.ts'
import {json} from '../src/codec.ts'
import {CommitTooLargeError, EvtError} from '../src/errors.ts'
import type {SerializedEvent} from '../src/event.ts'
import {Journal, JournalRowTooLargeError, allocate} from '../src/journal/config.ts'
import {PositionTaken, journalProjector} from '../src/journal/projector.ts'
import {type JournalPage, assemble} from '../src/journal/read.ts'
import {replay} from '../src/journal/replay.ts'
import {JournalRepository} from '../src/journal/repository.ts'
import {
  type JournalRow,
  counterView,
  journalRowFromView,
  readCounter,
  rowSize,
  rowView,
} from '../src/journal/row.ts'
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
import {JOURNAL_MARKER} from '../src/features.ts'
import {MemRepository, controlFixtures} from '../src/memory/repository.ts'
import {emptyMetadata} from '../src/metadata.ts'
import {
  type CommitProjector,
  MAX_VIEW_PK_BYTES,
  commitStream,
  projectViews,
  putView,
} from '../src/projection.ts'
import {Store} from '../src/store.ts'
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

const rowsAt = (...positions: number[]): JournalRow[] =>
  positions.map((position) => ({
    scope: 'world:1',
    position: BigInt(position),
    owner: 'e',
    event: event('e', BigInt(position)),
  }))
const read = (after: number, limit: number, counter: number, found: JournalRow[]) =>
  assemble('world:1', BigInt(after), limit, BigInt(counter), found)
const summary = (page: JournalPage) => ({
  positions: page.rows.map((row) => Number(row.position)),
  gap: page.gap && [Number(page.gap.from), Number(page.gap.to)],
  watermark: Number(page.highWatermark),
  next: Number(page.nextAfter),
  more: page.hasMore,
  caughtUp: page.caughtUp,
  ahead: page.cursorAhead,
})

describe('pages', () => {
  test('a contiguous page has no gap, and a full one short of the mark has more', () => {
    expect(summary(read(0, 10, 3, rowsAt(1, 2, 3)))).toEqual({
      positions: [1, 2, 3],
      gap: null,
      watermark: 3,
      next: 3,
      more: false,
      caughtUp: true,
      ahead: false,
    })
    expect(summary(read(0, 2, 5, rowsAt(1, 2)))).toMatchObject({gap: null, more: true, next: 2})
    // A full page is not checked against the watermark: the counter far ahead is more, not a gap.
    expect(summary(read(0, 3, 100, rowsAt(1, 2, 3)))).toMatchObject({gap: null, more: true})
  })

  test('a jump in the rows is a gap, and the page stops before it', () => {
    expect(summary(read(0, 10, 6, rowsAt(1, 2, 5, 6)))).toMatchObject({
      positions: [1, 2],
      gap: [3, 4],
      next: 2,
      more: false,
      caughtUp: false,
    })
    expect(summary(read(4, 10, 8, rowsAt(7, 8)))).toMatchObject({
      positions: [],
      gap: [5, 6],
      next: 4,
    })
  })

  test('rows that end short of the counter are a gap at the tail', () => {
    expect(summary(read(0, 10, 5, rowsAt(1, 2, 3)))).toMatchObject({gap: [4, 5]})
    expect(summary(read(0, 10, 2, []))).toMatchObject({gap: [1, 2]})
  })

  test('a row past the counter raises the watermark without a gap', () => {
    expect(summary(read(0, 10, 2, rowsAt(1, 2, 3)))).toMatchObject({
      gap: null,
      watermark: 3,
      caughtUp: true,
    })
    // The extra row a full page reads shows more when the counter is behind.
    expect(summary(read(0, 2, 1, rowsAt(1, 2, 3)))).toMatchObject({
      positions: [1, 2],
      watermark: 3,
      gap: null,
      more: true,
      caughtUp: false,
    })
  })

  test('an empty scope and a caught-up reader are not gaps; a cursor ahead is flagged', () => {
    expect(summary(read(0, 10, 0, []))).toMatchObject({gap: null, watermark: 0, caughtUp: true})
    expect(summary(read(7, 10, 7, []))).toMatchObject({gap: null, caughtUp: true})
    expect(summary(read(9, 10, 4, []))).toMatchObject({
      ahead: true,
      positions: [],
      gap: null,
      caughtUp: false,
      more: false,
    })
    expect(summary(read(4, 10, 4, [])).caughtUp).toBe(true)
    expect(summary(read(5, 10, 4, [])).ahead).toBe(true)
  })
})

function ledger(id = 'ledger-1:1'): SerializedEvent {
  return {...event(), id, entityType: 'ledger'}
}

function twoScopes(): string[] {
  return ['a', 'a-much-longer-scope-name']
}

describe('the journal configuration', () => {
  test('positions continue each scope from its counter', () => {
    const allocated = allocate(new Map([['world', 10n]]), [
      ['place', 'world'],
      ['world'],
      [],
      ['place', 'world'],
    ])
    expect(
      allocated.allocations.map((list) => list.map(({scope, position}) => `${scope}@${position}`)),
    ).toEqual([['place@1', 'world@11'], ['world@12'], [], ['place@2', 'world@13']])
    expect(allocated.counters).toEqual(
      new Map([
        ['place', 2n],
        ['world', 13n],
      ]),
    )
    expect(code(() => allocate(new Map([['world', MAX_POSITION]]), [['world']]))).toBe(
      'invalidJournal',
    )
  })

  test('refuses an empty journal and a cap past the limit', () => {
    expect(code(() => new Journal([], () => []))).toBe('invalidJournal')
    expect(code(() => new Journal(['ledger'], () => [], {maxRowBytes: 370_001}))).toBe(
      'invalidJournal',
    )
  })

  test('sorts and deduplicates scopes, only for covered types, never showing an allocation', () => {
    const journal = new Journal(['ledger'], (seen) =>
      seen.metadata.extensions.has(ALLOCATION_KEY) ? ['saw-allocation'] : ['b', 'a', 'b'],
    )
    expect(journal.scopesOf(ledger())).toEqual(['a', 'b'])
    expect(journal.scopesOf({...ledger(), entityType: 'other'})).toEqual([])

    const failing = new Journal(['ledger'], () => {
      throw new Error('no scope')
    })
    expect(() => failing.scopesOf(ledger())).toThrow(/no scope/)
  })

  test('the pre-check measures the widest row at the widest position', () => {
    const resolver = twoScopes
    const measured = ledger()
    setAllocations(measured, [
      {scope: 'a', position: MAX_POSITION},
      {scope: 'a-much-longer-scope-name', position: MAX_POSITION},
    ])
    const widest = rowSize('a-much-longer-scope-name', MAX_POSITION, measured)

    new Journal(['ledger'], resolver, {maxRowBytes: widest}).preCheck([ledger()])
    let refused: unknown
    try {
      new Journal(['ledger'], resolver, {maxRowBytes: widest - 1}).preCheck([ledger()])
    } catch (error) {
      refused = error
    }
    expect(refused).toBeInstanceOf(JournalRowTooLargeError)
    const row = refused as JournalRowTooLargeError
    expect([row.scope, row.position, row.size, row.limit]).toEqual([
      'a-much-longer-scope-name',
      MAX_POSITION,
      widest,
      widest - 1,
    ])
    expect(row.toRejection()).toMatchObject({
      code: 'journal-row-too-large',
      details: {scope: 'a-much-longer-scope-name', size: widest, limit: widest - 1},
    })
    // The headroom widens only the pre-check.
    const roomy = new Journal(['ledger'], resolver, {maxRowBytes: widest, preCheckHeadroom: 1})
    expect(code(() => roomy.preCheck([ledger()]))).toBe('journalRowTooLarge')
  })
})

describe("the journal's projector", () => {
  test('writes the rows the allocations name; no other projector sees them', () => {
    const stored = event('a', 1n)
    setAllocations(stored, [{scope: 'world:1', position: 7n}])
    const view = {
      commitId: 'c',
      streams: [commitStream('a', 'ledger', [stored], new Uint8Array())],
    }

    const seen: (string | undefined)[] = []
    const watcher: CommitProjector = {
      name: 'watcher',
      history: ['ledger'],
      project(commit) {
        seen.push(commit.streams[0]!.events[0]!.metadata.extensions.get(ALLOCATION_KEY))
        return []
      },
    }
    const journal = new Journal(['ledger'], () => ['world:1'])
    const projected = projectViews([watcher, journalProjector(journal)], view)

    expect(seen).toEqual([undefined])
    expect(projected.map(({op}) => [op.pk, op.sk, op.owner, op.condition])).toEqual([
      ['journal#world:1', '00000000000000000007', 'a', 'absent'],
    ])
    expect(projected[0]!.op.conflict).toBeInstanceOf(PositionTaken)

    // An application projector can't write the journal's partitions.
    const forger: CommitProjector = {
      name: 'forger',
      project: () => [putView('journal#world:1', '00000000000000000008', 'a', 'journal', '{}')],
    }
    expect(code(() => projectViews([forger], view))).toBe('reservedViewKey')
  })
})

describe('a store over the journal', () => {
  const account = defineAggregate({
    type: 'account',
    events: defineEvents({'account:noted': {version: 1, codec: json<{note: string}>()}}),
    state: json<{notes: number}>(),
    initial: () => ({notes: 0}),
    handle: (_state, note: string) => ({
      kind: 'accept',
      events: [{type: 'account:noted', data: {note}}],
    }),
    apply: (state) => ({notes: state.notes + 1}),
    identify: (note: string) => ({
      commandType: 'account:note',
      payload: json<string>().encode(note),
    }),
  })
  const journal = new Journal(['account'], (stored) => [`account:${stored.entityId}`], {
    maxRowBytes: 2_000,
  })

  test('hands back the allocations the commit stored, from every entry point', async () => {
    const repository = new JournalRepository(new MemRepository(), journal)
    const store = new Store(repository, {snapshotInterval: 0})

    const executed = await store.execute(account, 'a', 'one')
    expect(executed.kind === 'committed' && allocations(executed.events[0]!.metadata)).toEqual([
      {scope: 'account:a', position: 1n},
    ])

    const tx = store.transaction()
    tx.append(await tx.read(account, 'b'), [{type: 'account:noted', data: {note: 'two'}}])
    const committed = await tx.commit()
    expect(committed.kind === 'committed' && allocations(committed.events[0]!.metadata)).toEqual([
      {scope: 'account:b', position: 1n},
    ])
    expect(await repository.highWatermark('account:a')).toBe(1n)
  })

  test("records a row over the cap as the command's rejection, which a replay returns", async () => {
    const repository = new JournalRepository(new MemRepository(), journal)
    const store = new Store(repository, {snapshotInterval: 0, outcomes: {}})
    const options = {metadata: {commandId: 'big-1'}}

    const refused = await store.executeWithOutcome(account, 'a', 'x'.repeat(3_000), options)
    expect(refused.outcome.status).toBe('rejected')
    expect(refused.outcome.rejection?.code).toBe('journal-row-too-large')
    expect(await repository.events('a')).toEqual([])

    const replayed = await store.executeWithOutcome(account, 'a', 'x'.repeat(3_000), options)
    expect(replayed.replayed).toBe(true)
    expect(replayed.outcome.rejection?.code).toBe('journal-row-too-large')
  })

  test("refuses the journal's projector as an application projector", async () => {
    const store = new Store(new MemRepository(), {commitProjectors: [journalProjector(journal)]})
    await expect(store.execute(account, 'a', 'one')).rejects.toThrow(/engine metadata/)
  })
})

// The first event of an account stream.
function opened(entityId: string): SerializedEvent {
  return {...ledger(`${entityId}:1`), entityId, entityType: 'account', sequence: 1n}
}

describe('journal acceptance', () => {
  const journal = new Journal(['account'], (stored) => [`account:${stored.entityId}`])

  test('only the handle a journal wraps declares or retracts it', async () => {
    const memory = new MemRepository()
    await expect(memory[JOURNAL_MARKER].declareJournal(['account'])).rejects.toThrow(
      /unsupported features \(journal@1\)/,
    )
    await expect(
      memory[JOURNAL_MARKER].retractJournal({journal: true, history: ['account']}),
    ).rejects.toThrow(/journal@1/)
  })

  test('a journal writer still refuses a store that declares journal compaction', async () => {
    const memory = new MemRepository()
    const repository = new JournalRepository(memory, journal)
    await repository.commit({entityId: 'a', events: [opened('a')]})
    controlFixtures(memory).declare((draft) => draft.features.set('journal-compaction', 1))
    await expect(repository.commit({entityId: 'b', events: [opened('b')]})).rejects.toThrow(
      /journal-compaction@1/,
    )
  })

  test('an allocation that escapes a lone surrogate is refused', () => {
    for (const value of [
      String.raw`[{"scope":"a\ud800","position":1}]`,
      String.raw`[{"scope":"a\udc00","position":1}]`,
    ]) {
      const metadata = {...emptyMetadata(), extensions: new Map([[ALLOCATION_KEY, value]])}
      expect(code(() => allocations(metadata))).toBe('invalidJournal')
    }
    const pair = String.raw`[{"scope":"a😀","position":1}]`
    const metadata = {...emptyMetadata(), extensions: new Map([[ALLOCATION_KEY, pair]])}
    expect(allocations(metadata)).toEqual([{scope: 'a\u{1F600}', position: 1n}])
  })
})

describe('the journal and the commit budget', () => {
  test('a commit over the budget with its rows and counters declares nothing', async () => {
    const memory = new MemRepository()
    await memory.initialize()
    const repository = new JournalRepository(memory, new Journal(['account'], () => ['world:7']))
    // 49 events, 49 rows and a counter, with the two control checks: 101 actions.
    const events = Array.from({length: 49}, (_, index) => {
      const sequence = BigInt(index + 1)
      return {...opened('a'), id: `a:${sequence}`, sequence}
    })
    await expect(repository.commit({entityId: 'a', events})).rejects.toBeInstanceOf(
      CommitTooLargeError,
    )
    const manifest = controlFixtures(memory).manifest()
    expect(manifest?.features.has('journal')).toBe(false)
    expect(manifest?.historyDependent).toEqual([])
  })
})
