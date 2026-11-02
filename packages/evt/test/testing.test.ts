import {describe, expect, test} from 'vitest'

import {defineAggregate, defineEvents} from '../src/aggregate.ts'
import {json} from '../src/codec.ts'
import {AssertionError, deepEqual, show} from '../src/testing/equal.ts'
import {DEFAULT_ENTITY_ID, Fixture, Outcome, scenario} from '../src/testing/index.ts'

const ledgerEvents = defineEvents({
  'ledger:added': {version: 1, codec: json<{amount: number}>()},
})

type LedgerCommand = {type: 'add'; amount: number} | {type: 'check'} | {type: 'explode'}

const ledger = defineAggregate({
  type: 'ledger',
  events: ledgerEvents,
  state: json<{total: number; entries: number}>(),
  initial: () => ({total: 0, entries: 0}),
  handle(_state, command: LedgerCommand) {
    switch (command.type) {
      case 'add':
        if (command.amount <= 0) {
          return {kind: 'reject', rejection: {code: 'invalid-amount', message: 'must be positive'}}
        }
        return {kind: 'accept', events: [added(command.amount)]}
      case 'check':
        return {kind: 'accept', events: []}
      case 'explode':
        throw new Error('the handler failed')
    }
  },
  apply: (state, event) => ({
    total: state.total + event.data.amount,
    entries: state.entries + 1,
  }),
})

// A second aggregate type, whose events hold a Map.
const claimEvents = defineEvents({
  'claim:taken': {
    version: 1,
    codec: {
      encode: (data: {holders: Map<string, number>}) =>
        new TextEncoder().encode(JSON.stringify([...data.holders])),
      decode: (bytes: Uint8Array) => ({
        holders: new Map<string, number>(JSON.parse(new TextDecoder().decode(bytes))),
      }),
    },
  },
})

const claim = defineAggregate({
  type: 'claim',
  events: claimEvents,
  state: json<{held: boolean}>(),
  initial: () => ({held: false}),
  handle: (_state, _command: never) => ({kind: 'accept', events: []}),
  apply: () => ({held: true}),
})

function added(amount: number) {
  return {type: 'ledger:added', data: {amount}} as const
}

async function thrown(call: Promise<unknown>): Promise<unknown> {
  try {
    await call
  } catch (error) {
    return error
  }

  throw new Error('expected the call to throw')
}

describe('scenario', () => {
  test('emits events after the given history', async () => {
    await scenario(ledger).given(added(2), added(3)).when({type: 'add', amount: 5}).thenEvents(
      added(5),
    )
  })

  test('the state folds the given and the new events', async () => {
    await scenario(ledger)
      .given(added(2))
      .when({type: 'add', amount: 5})
      .thenState((state) => {
        expect(state).toEqual({total: 7, entries: 2})
      })
  })

  test('a command accepted with no events commits nothing', async () => {
    await scenario(ledger).given(added(1)).when({type: 'check'}).thenNoEvents()
  })

  test('rejections match by code or predicate and write nothing', async () => {
    const when = scenario(ledger).when({type: 'add', amount: -1})
    await when.thenRejected('invalid-amount')
    await when.thenRejected((rejection) => rejection.message === 'must be positive')

    const outcome = await when.run()
    expect(outcome.assertRejected()).toEqual({code: 'invalid-amount', message: 'must be positive'})
    expect(outcome.storedEvents).toBe(null)
    expect(await outcome.fixture.events(ledger, DEFAULT_ENTITY_ID)).toEqual([])
  })

  test('a repeated command ID is a duplicate', async () => {
    const fixture = new Fixture()
    const base = scenario(ledger).on(fixture).metadata({commandId: 'cmd-1'})

    await base.when({type: 'add', amount: 1}).thenEvents(added(1))
    await base.when({type: 'add', amount: 1}).thenDuplicate()
    await fixture.assertEvents(ledger, DEFAULT_ENTITY_ID, [added(1)])
  })

  test('a store error is the outcome, not a throw', async () => {
    const when = scenario(ledger).when({type: 'explode'})
    await when.thenError((error) => (error as Error).message === 'the handler failed')

    const error = await thrown(when.thenEvents())
    expect(error).toBeInstanceOf(AssertionError)
    expect((error as Error).message).toContain('got a store error: Error: the handler failed')
  })

  test('mismatched events fail with both sides', async () => {
    const error = await thrown(scenario(ledger).when({type: 'add', amount: 1}).thenEvents(added(2)))

    expect(error).toBeInstanceOf(AssertionError)
    expect(error).toMatchObject({
      message: expect.stringContaining('events emitted for ledger entity-1'),
      actual: [added(1)],
      expected: [added(2)],
    })
  })

  test('expecting an outcome that does not happen fails', async () => {
    const commits = scenario(ledger).when({type: 'add', amount: 1})
    const rejects = scenario(ledger).when({type: 'add', amount: 0})

    expect(await thrown(commits.thenRejected())).toMatchObject({
      message: 'expected a rejection, got committed events: [{"type": "ledger:added", ' +
        '"data": {"amount": 1}}]',
    })
    expect(await thrown(commits.thenDuplicate())).toBeInstanceOf(AssertionError)
    expect(await thrown(commits.thenError())).toBeInstanceOf(AssertionError)
    expect(await thrown(rejects.thenRejected('other'))).toMatchObject({
      actual: 'invalid-amount',
      expected: 'other',
    })
    expect(await thrown(rejects.thenRejected(() => false))).toBeInstanceOf(AssertionError)
    expect(await thrown(rejects.thenState(() => {}))).toBeInstanceOf(AssertionError)
  })

  test('invalid store options throw from the run', async () => {
    const run = scenario(ledger).options({snapshotInterval: -1}).when({type: 'check'}).run()

    expect(await thrown(run)).toMatchObject({code: 'invalidArgument'})
  })

  test('a failure message shows events that do not decode as stored', async () => {
    const outcome = await scenario(ledger).when({type: 'add', amount: 1}).run()
    // The same result read with an aggregate that knows none of its event types.
    const blind = {...ledger, events: defineEvents({})} as unknown as typeof ledger
    const misread = new Outcome(blind, outcome.fixture, outcome.entityId, outcome.settled)

    expect(() => misread.assertDuplicate()).toThrow(
      'expected a duplicate command, got committed events: ["ledger:added v1"] (undecodable)',
    )
  })

  test('the entity ID and store options are configurable', async () => {
    const outcome = await scenario(ledger)
      .given(added(1))
      .entity('ledger-9')
      .options({snapshotInterval: 2, region: 'us-west-2', clock: () => Date.UTC(2026, 0, 2)})
      .when({type: 'add', amount: 1})
      .run()

    expect(outcome.state).toMatchObject({entityId: 'ledger-9', sequence: 2n, snapshotSequence: 1n})
    expect(outcome.storedEvents?.[0]?.metadata).toMatchObject({
      region: 'us-west-2',
      timestamp: '2026-01-02T00:00:00Z',
    })
    expect(await outcome.fixture.events(ledger, 'ledger-9')).toHaveLength(2)
    expect(await outcome.fixture.repository.snapshot('ledger-9')).toMatchObject({eventSequence: 2n})
  })

  test('each method returns a new scenario', async () => {
    const base = scenario(ledger).given(added(1))
    const more = base.given(added(2))

    await base.when({type: 'add', amount: 1}).thenState((state) => {
      expect(state.entries).toBe(2)
    })
    await more.when({type: 'add', amount: 1}).thenState((state) => {
      expect(state.entries).toBe(3)
    })
  })
})

describe('Fixture', () => {
  test('seeds and reads streams of several aggregate types', async () => {
    const fixture = new Fixture()
    const holders = new Map([['ada', 1]])
    await fixture.given(ledger, 'ledger-1', [added(1), added(2)])
    await fixture.given(claim, 'claim-1', [{type: 'claim:taken', data: {holders}}])

    await fixture.assertEvents(ledger, 'ledger-1', [added(1), added(2)])
    await fixture.assertEvents(claim, 'claim-1', [
      {type: 'claim:taken', data: {holders: new Map([['ada', 1]])}},
    ])
    expect((await fixture.load(ledger, 'ledger-1')).state).toEqual({total: 3, entries: 2})

    const stored = await fixture.repository.events('ledger-1')
    expect(stored.map((event) => event.metadata.commitId)).toEqual([
      'given-ledger-1-1',
      'given-ledger-1-1',
    ])
    expect(stored[0]?.entityType).toBe('ledger')

    const error = await thrown(fixture.assertEvents(ledger, 'ledger-1', [added(1)]))
    expect(error).toBeInstanceOf(AssertionError)
    expect((error as Error).message).toContain('events of stream ledger-1')
  })

  test('given continues after a snapshot and compaction', async () => {
    const fixture = new Fixture()
    const store = fixture.store({snapshotInterval: 2})
    for (const amount of [1, 1]) {
      // Each command continues the one before it.
      // oxlint-disable-next-line no-await-in-loop
      await store.execute(ledger, 'ledger-1', {type: 'add', amount})
    }
    expect(await fixture.repository.compactBelow('ledger-1', 2n)).toBe(2)

    await fixture.given(ledger, 'ledger-1', [added(5)])

    const stored = await fixture.repository.events('ledger-1')
    expect(stored.map((event) => event.sequence)).toEqual([3n])
    expect(stored[0]?.metadata.commitId).toBe('given-ledger-1-3')
    expect(await fixture.load(ledger, 'ledger-1')).toMatchObject({
      sequence: 3n,
      state: {total: 7, entries: 3},
    })
  })

  test('given with no events writes nothing', async () => {
    const fixture = new Fixture()
    await fixture.given(ledger, 'ledger-1', [])

    expect(await fixture.repository.events('ledger-1')).toEqual([])
  })
})

describe('deepEqual', () => {
  test('compares structure', () => {
    expect(deepEqual({a: 1, b: [1n, 'x']}, {b: [1n, 'x'], a: 1})).toBe(true)
    expect(deepEqual({a: 1, b: undefined}, {a: 1})).toBe(true)
    expect(deepEqual(new Map([['k', {v: 1}]]), new Map([['k', {v: 1}]]))).toBe(true)
    expect(deepEqual(new Set([1, 2]), new Set([2, 1]))).toBe(true)
    expect(deepEqual(new Uint8Array([1, 2]), new Uint8Array([1, 2]))).toBe(true)
    expect(deepEqual(new Date(0), new Date(0))).toBe(true)
    expect(deepEqual(Number.NaN, Number.NaN)).toBe(true)

    expect(deepEqual({a: 1}, {a: 1, b: 2})).toBe(false)
    expect(deepEqual([1, 2], [2, 1])).toBe(false)
    expect(deepEqual(new Map([['k', 1]]), new Map([['k', 2]]))).toBe(false)
    expect(deepEqual(new Map([['k', 1]]), {k: 1})).toBe(false)
    expect(deepEqual(new Uint8Array([1]), [1])).toBe(false)
    expect(deepEqual(1n, 1)).toBe(false)
    expect(deepEqual(null, {})).toBe(false)
    expect(deepEqual(0, -0)).toBe(false)
  })

  test('sees holes, buffers, patterns and symbol keys', () => {
    const key = Symbol('k')

    // oxlint-disable-next-line no-sparse-arrays
    expect(deepEqual([1, , 3], [1, undefined, 3])).toBe(false)
    expect(deepEqual(new Uint8Array([1, 2]).buffer, new Uint8Array([1, 3]).buffer)).toBe(false)
    const tail = new DataView(new Uint8Array([1, 2]).buffer, 1)
    expect(deepEqual(tail, new DataView(new Uint8Array([2]).buffer))).toBe(true)
    expect(deepEqual(/a/g, /a/g)).toBe(true)
    expect(deepEqual(/a/, /b/)).toBe(false)
    expect(deepEqual(/a/g, /a/i)).toBe(false)
    expect(deepEqual({[key]: 1}, {[key]: 2})).toBe(false)
    expect(deepEqual({[key]: 1}, {[key]: 1})).toBe(true)
  })

  test('show prints what JSON would lose', () => {
    expect(show({n: 1n, m: new Map([['k', new Uint8Array([7])]]), u: undefined})).toBe(
      '{"n": 1n, "m": Map {"k" => Uint8Array [7]}, "u": undefined}',
    )
    expect(show([new Set(['a']), new Date(0), new Date(Number.NaN)])).toBe(
      '[Set {"a"}, Date 1970-01-01T00:00:00.000Z, Invalid Date]',
    )
    expect(show([-0, 0])).toBe('[-0, 0]')
  })
})
