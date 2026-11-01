// Each test's steps depend on the ones before them, so they await in loops on purpose.
/* oxlint-disable no-await-in-loop */
import {describe, expect, test} from 'vitest'

import {
  type Aggregate,
  type EventDefinitions,
  type EventOf,
  defineAggregate,
  defineEvents,
  jsonUpcaster,
} from '../src/aggregate.ts'
import {type StandardSchemaV1, json} from '../src/codec.ts'
import {commitIds} from '../src/commit-id.ts'
import {
  ConflictError,
  EvtError,
  ReplayError,
  SequenceGapError,
  SnapshotError,
} from '../src/errors.ts'
import {type SerializedEvent, encodeEvent, eventId} from '../src/event.ts'
import type {JsonValue} from '../src/json.ts'
import {MemRepository} from '../src/memory/repository.ts'
import {type Metadata, emptyMetadata} from '../src/metadata.ts'
import type {Commit, Repository} from '../src/repository.ts'
import {DEFAULT_RETRY_POLICY, checkRetryPolicy, retryDelay, sleep} from '../src/retry.ts'
import {Store} from '../src/store.ts'

const encoder = new TextEncoder()
const decoder = new TextDecoder()

// The aggregate from ADR 0009's sketch.
const events = defineEvents({
  'account:deposited': {version: 1, codec: json<{amountCents: number}>()},
})

type Deposit = {type: 'deposit'; amountCents: number}

const account = defineAggregate({
  type: 'account',
  events,
  state: json<{balanceCents: number}>(),
  initial: () => ({balanceCents: 0}),
  handle(_state, command: Deposit) {
    if (command.amountCents <= 0) {
      return {kind: 'reject', rejection: {code: 'invalid', message: 'amount must be positive'}}
    }

    return {
      kind: 'accept',
      events: [{type: 'account:deposited', data: {amountCents: command.amountCents}}],
    }
  },
  apply: (state, event) => ({balanceCents: state.balanceCents + event.data.amountCents}),
})

// 2026-10-31T12:34:56.789Z
const NOW = Date.UTC(2026, 9, 31, 12, 34, 56, 789)

function store(repository: Repository = new MemRepository(), snapshotInterval = 5): Store {
  return new Store(repository, {region: 'us-west-2', clock: () => NOW, snapshotInterval})
}

async function failure(call: Promise<unknown>): Promise<unknown> {
  try {
    await call
  } catch (error) {
    return error
  }

  return undefined
}

function storedEvent(sequence: number, type: string, version: number, payload: string) {
  const entityId = 'a'
  const event: SerializedEvent = {
    id: eventId(entityId, BigInt(sequence)),
    sequence: BigInt(sequence),
    type,
    version,
    entityId,
    entityType: 'account',
    payload: encoder.encode(payload),
    metadata: emptyMetadata(),
  }

  return event
}

// A repository holding events of stream `a`, written as given.
async function holding(...stored: SerializedEvent[]): Promise<MemRepository> {
  const repository = new MemRepository()
  await repository.commit({entityId: 'a', events: stored})

  return repository
}

async function snapshotAfter(
  aggregate: Aggregate<{balanceCents: number}, Deposit, typeof events>,
  commits: number,
  interval: number,
) {
  const repository = new MemRepository()
  const s = store(repository, interval)
  for (let i = 0; i < commits; i++) {
    await s.execute(aggregate, 'a', {type: 'deposit', amountCents: 10})
  }

  return repository.snapshot('a')
}

async function load<D extends EventDefinitions>(
  aggregate: Aggregate<{owner: string}, unknown, D>,
  version: number,
  payload: string,
) {
  const repository = await holding(storedEvent(1, 'account:renamed', version, payload))

  return store(repository).load(aggregate, 'a')
}

// A repository whose first `conflicts` commits fail as if another writer won.
function contended(conflicts: number): {repository: Repository; commits: () => number} {
  const repository = new MemRepository()
  let commits = 0

  return {
    repository: {
      ...delegate(repository),
      async commit(commit: Commit, options) {
        commits++
        if (commits <= conflicts) {
          throw new ConflictError(commit.entityId, 'another writer won')
        }
        await repository.commit(commit, options)
      },
    },
    commits: () => commits,
  }
}

// A repository that forwards every call to `repository`.
function delegate(repository: Repository): Repository {
  return {
    commit: (commit, options) => repository.commit(commit, options),
    events: (id, options) => repository.events(id, options),
    eventsAfter: (id, after, options) => repository.eventsAfter(id, after, options),
    latest: (id, n, options) => repository.latest(id, n, options),
    snapshot: (id, options) => repository.snapshot(id, options),
    putSnapshot: (snapshot, options) => repository.putSnapshot(snapshot, options),
    compactBelow: (id, through, options) => repository.compactBelow(id, through, options),
  }
}

function deposited(amountCents: number): EventOf<typeof events> {
  return {type: 'account:deposited', data: {amountCents}}
}

// A field of a JSON object, for upcasters.
function field(payload: JsonValue, name: string): JsonValue {
  if (!(payload instanceof Map) || !payload.has(name)) {
    throw new Error(`the payload has no ${name}`)
  }

  return payload.get(name)!
}

function delays(random: () => number): number[] {
  return [0, 1, 2, 3, 10].map((retry) => retryDelay(DEFAULT_RETRY_POLICY, retry, random))
}

// A schema that validates asynchronously, which a codec refuses.
async function validateLater(): Promise<{value: {n: number}}> {
  return {value: {n: 1}}
}

describe('Store.execute', () => {
  test('commits the events and fills in the region, timestamp and commit ID', async () => {
    const repository = new MemRepository()
    const result = await store(repository).execute(
      account,
      'acct-1',
      {type: 'deposit', amountCents: 100},
      {metadata: {commandId: 'cmd-1'}},
    )

    expect(result.kind).toBe('committed')
    const [stored] = await repository.events('acct-1')
    expect(stored).toMatchObject({id: 'acct-1:1', sequence: 1n, type: 'account:deposited'})
    expect(decoder.decode(stored!.payload!)).toBe('{"amountCents":100}')
    expect(stored!.metadata).toMatchObject({
      commandId: 'cmd-1',
      region: 'us-west-2',
      timestamp: '2026-10-31T12:34:56Z',
      commitId: 'cmd-1',
    })
  })

  test('keeps the metadata a command sets, and generates a missing commit ID', async () => {
    const repository = new MemRepository()
    const ids = new Store(repository, {commitId: () => 'commit-1', clock: () => NOW})
    await ids.execute(account, 'a', {type: 'deposit', amountCents: 1}, {
      metadata: {region: 'eu-west-1', timestamp: '2026-01-01T00:00:00Z'},
    })

    const [stored] = await repository.events('a')
    expect(stored!.metadata).toMatchObject({
      region: 'eu-west-1',
      timestamp: '2026-01-01T00:00:00Z',
      commitId: 'commit-1',
    })
  })

  test('treats a field an untyped caller left undefined as empty', async () => {
    const repository = new MemRepository()
    const loose = {commandId: undefined, region: undefined} as unknown as Partial<Metadata>
    const s = new Store(repository, {region: 'us-west-2', clock: () => NOW, commitId: () => 'g'})
    await s.execute(account, 'a', {type: 'deposit', amountCents: 1}, {metadata: loose})

    const [stored] = await repository.events('a')
    expect(stored!.metadata).toMatchObject({commandId: null, region: 'us-west-2', commitId: 'g'})
    expect(encodeEvent(stored!)).toContain('"metadata":{"commandId":null,')
  })

  test('gives every event of a commit its own metadata', async () => {
    const extensions = new Map([['k', 'v']])
    const twice = defineAggregate({
      ...account,
      handle: () => ({kind: 'accept', events: [deposited(1), deposited(2)]}),
    })
    const command: Deposit = {type: 'deposit', amountCents: 1}
    const result = await store().execute(twice, 'a', command, {metadata: {extensions}})
    if (result.kind !== 'committed') {
      throw new Error('the command was not committed')
    }

    result.events[0]!.metadata.extensions.set('k', 'changed')
    expect(result.events[1]!.metadata.extensions.get('k')).toBe('v')
    expect(extensions.get('k')).toBe('v')
  })

  test('a rejection writes nothing', async () => {
    const repository = new MemRepository()
    const result = await store(repository).execute(account, 'a', {type: 'deposit', amountCents: 0})

    expect(result).toMatchObject({kind: 'rejected', rejection: {code: 'invalid'}})
    expect(await repository.events('a')).toEqual([])
  })

  test('an accepted command with no events writes nothing', async () => {
    const quiet = defineAggregate({...account, handle: () => ({kind: 'accept', events: []})})
    const repository = new MemRepository()
    const result = await store(repository).execute(quiet, 'a', {type: 'deposit', amountCents: 1})

    expect(result).toMatchObject({kind: 'committed', events: []})
    expect(await repository.events('a')).toEqual([])
  })

  test('a command ID seen since the latest snapshot is a duplicate', async () => {
    const repository = new MemRepository()
    const s = store(repository, 2)
    const deposit = (commandId: string) =>
      s.execute(account, 'a', {type: 'deposit', amountCents: 1}, {metadata: {commandId}})

    await deposit('c1')
    expect(await deposit('c1')).toEqual({kind: 'duplicate', commandId: 'c1'})

    // The second commit snapshots, so c1 and c2 fold out of the window.
    await deposit('c2')
    expect((await deposit('c1')).kind).toBe('committed')
    expect((await deposit('')).kind).toBe('committed')
    expect((await deposit('')).kind).toBe('committed')
  })
})

describe('the snapshot policy', () => {

  test('snapshots when a commit reaches the interval', async () => {
    const snapshot = await snapshotAfter(account, 5, 2)

    expect(snapshot).toMatchObject({sequence: 2n, eventSequence: 4n})
    expect(decoder.decode(snapshot!.payload!)).toBe('{"balanceCents":40}')
  })

  test("an aggregate's own interval overrides the store's, and 0 disables snapshots", async () => {
    const every3 = defineAggregate({...account, snapshotInterval: 3})
    expect(await snapshotAfter(every3, 4, 2)).toMatchObject({sequence: 1n, eventSequence: 3n})

    const never = defineAggregate({...account, snapshotInterval: 0})
    expect(await snapshotAfter(never, 4, 2)).toBe(null)
    expect(await snapshotAfter(account, 4, 0)).toBe(null)
  })

  test('a snapshot over the per-item budget is left out and the commit succeeds', async () => {
    const hoard = defineAggregate({
      ...account,
      state: {
        encode: (state: {balanceCents: number}) =>
          encoder.encode(JSON.stringify({...state, padding: 'x'.repeat(400_000)})),
        decode: (bytes: Uint8Array) => JSON.parse(decoder.decode(bytes)),
      },
    })
    const repository = new MemRepository()
    const s = store(repository, 1)
    await s.execute(hoard, 'a', {type: 'deposit', amountCents: 1})

    expect(await repository.snapshot('a')).toBe(null)
    expect((await s.load(hoard, 'a')).state).toEqual({balanceCents: 1})
  })
})

describe('Store.load', () => {
  test('starts from the snapshot and folds the events after it', async () => {
    const repository = new MemRepository()
    const s = store(repository, 2)
    for (let i = 0; i < 3; i++) {
      const metadata = {commandId: `c${i}`}
      await s.execute(account, 'a', {type: 'deposit', amountCents: 5}, {metadata})
    }

    const loaded = await s.load(account, 'a')
    expect(loaded).toMatchObject({sequence: 3n, snapshotSequence: 1n, state: {balanceCents: 15}})
    expect([...loaded.seenCommandIds]).toEqual(['c2'])
  })

  test('a snapshot the state codec refuses fails the load', async () => {
    const repository = await holding(storedEvent(1, 'account:deposited', 1, '{"amountCents":1}'))
    await repository.putSnapshot({
      entityType: 'account',
      entityId: 'a',
      sequence: 1n,
      eventSequence: 1n,
      payload: encoder.encode('not json'),
    })

    expect(await failure(store(repository).load(account, 'a'))).toBeInstanceOf(SnapshotError)
  })

  test('a gap that clears reloads', async () => {
    const repository = await holding(storedEvent(1, 'account:deposited', 1, '{"amountCents":4}'))
    let reads = 0
    const healing: Repository = {
      ...delegate(repository),
      async events(id, options) {
        reads++
        return reads === 1
          ? [storedEvent(2, 'account:deposited', 1, '{"amountCents":1}')]
          : repository.events(id, options)
      },
    }

    expect((await store(healing).load(account, 'a')).state).toEqual({balanceCents: 4})
    expect(reads).toBe(2)
  })

  test('a gap that persists fails after three reads', async () => {
    const repository = await holding(storedEvent(1, 'account:deposited', 1, '{"amountCents":1}'))
    let reads = 0
    const gapped: Repository = {
      ...delegate(repository),
      async events() {
        reads++
        return [storedEvent(2, 'account:deposited', 1, '{"amountCents":1}')]
      },
    }

    const error = await failure(store(gapped).load(account, 'a'))
    expect(error).toBeInstanceOf(SequenceGapError)
    expect(error).toMatchObject({after: 0n, found: 2n})
    expect(reads).toBe(3)
  })
})

describe('upcasting', () => {
  const renamed = defineEvents({
    'account:renamed': {
      version: 3,
      codec: json<{owner: string; upcast?: number}>(),
      upcasters: [
        jsonUpcaster(1, 2, (payload) => new Map([['owner', field(payload, 'name')]])),
        jsonUpcaster(2, 3, (payload) => {
          const upcast = new Map<string, unknown>([['owner', field(payload, 'owner')]])
          upcast.set('upcast', 3)

          return upcast
        }),
      ],
    },
  })
  const named = defineAggregate({
    type: 'account',
    events: renamed,
    state: json<{owner: string}>(),
    initial: () => ({owner: ''}),
    handle: () => ({kind: 'accept', events: []}),
    apply: (_state, event) => ({owner: `${event.data.owner}/${event.data.upcast}`}),
  })


  test('runs the upcasters in order, each seeing the output of the ones before', async () => {
    expect((await load(named, 1, '{"name":"Ada"}')).state).toEqual({owner: 'Ada/3'})
    expect((await load(named, 2, '{"owner":"Ada"}')).state).toEqual({owner: 'Ada/3'})
  })

  test('fails closed on an unreached version, a failed upcast, or an unknown type', async () => {
    const tooNew = await failure(load(named, 4, '{}'))
    expect(tooNew).toBeInstanceOf(ReplayError)
    expect(tooNew).toMatchObject({phase: 'decode', version: 4})

    const broken = await failure(load(named, 1, 'not json'))
    expect(broken).toMatchObject({code: 'replay', phase: 'upcast', version: 1})

    const repository = await holding(storedEvent(1, 'account:unknown', 1, '{}'))
    expect(await failure(store(repository).load(named, 'a'))).toMatchObject({
      code: 'replay',
      eventType: 'account:unknown',
    })
  })

  test('writes back what a migration leaves alone exactly, however large or keyed', () => {
    const keep = jsonUpcaster(1, 2, (payload) => payload)
    const stored = '{"big":9007199254740993,"byId":{"10":"x","2":"y"},"f":1.50}'

    expect(decoder.decode(keep.upcast(encoder.encode(stored)))).toBe(stored)
  })

  test('refuses definitions with versions below 1', () => {
    expect(() => defineEvents({e: {version: 0, codec: json()}})).toThrow(EvtError)
    expect(() => defineAggregate({...account, snapshotInterval: -1})).toThrow(EvtError)
  })
})

describe('Store.executeWithRetry', () => {

  test('runs the command again after a conflict, waiting between attempts', async () => {
    const {repository, commits} = contended(2)
    const waits: number[] = []
    const result = await store(repository).executeWithRetry(
      account,
      'a',
      {type: 'deposit', amountCents: 1},
      {sleep: async (ms) => void waits.push(ms)},
    )

    expect(result.kind).toBe('committed')
    expect(commits()).toBe(3)
    expect(waits).toHaveLength(2)
    expect(waits[0]).toBeGreaterThanOrEqual(12)
    expect(waits[0]).toBeLessThanOrEqual(25)
    expect(waits[1]).toBeGreaterThanOrEqual(25)
    expect(waits[1]).toBeLessThanOrEqual(50)
  })

  test('throws the last conflict when the attempts run out', async () => {
    const {repository, commits} = contended(10)
    const error = await failure(
      store(repository).executeWithRetry(account, 'a', {type: 'deposit', amountCents: 1}, {
        policy: {maxAttempts: 3, baseDelayMs: 0, maxDelayMs: 0},
      }),
    )

    expect(error).toBeInstanceOf(ConflictError)
    expect(commits()).toBe(3)
  })

  test('an abort during the wait rejects with its reason', async () => {
    const {repository, commits} = contended(10)
    const controller = new AbortController()
    const reason = new Error('the caller gave up')
    const error = await failure(
      store(repository).executeWithRetry(account, 'a', {type: 'deposit', amountCents: 1}, {
        policy: {maxAttempts: 5, baseDelayMs: 60_000, maxDelayMs: 60_000},
        signal: controller.signal,
        sleep: (ms, signal) => {
          const waited = sleep(ms, signal)
          controller.abort(reason)
          return waited
        },
      }),
    )

    expect(error).toBe(reason)
    expect(commits()).toBe(1)
  })

  test('a rejection is a result, never retried', async () => {
    const {repository, commits} = contended(10)
    const command: Deposit = {type: 'deposit', amountCents: 0}
    const result = await store(repository).executeWithRetry(account, 'a', command)

    expect(result.kind).toBe('rejected')
    expect(commits()).toBe(0)
  })
})

describe('the retry policy', () => {
  test('has the defaults the fixtures pin', () => {
    expect(DEFAULT_RETRY_POLICY).toEqual({maxAttempts: 5, baseDelayMs: 25, maxDelayMs: 1000})
  })

  test('jitters each delay in the upper half, capped at the maximum', () => {
    expect(delays(() => 0)).toEqual([12, 25, 50, 100, 500])
    expect(delays(() => 0.999_999)).toEqual([25, 50, 100, 200, 1000])
  })

  test('refuses a policy without an attempt', () => {
    const none = {maxAttempts: 0, baseDelayMs: 1, maxDelayMs: 1}
    expect(() => checkRetryPolicy(none)).toThrow(EvtError)
  })
})

describe('json()', () => {
  test('refuses an integer a number would round', () => {
    const codec = json<{n: number}>()

    expect(codec.decode(encoder.encode('{"n":9007199254740991}'))).toEqual({n: 9007199254740991})
    expect(() => codec.decode(encoder.encode('{"n":9007199254740993}'))).toThrow(EvtError)
  })

  test('keeps a __proto__ key as a plain property', () => {
    const decoded = json<Record<string, unknown>>().decode(encoder.encode('{"__proto__":{"x":1}}'))

    expect(Object.getPrototypeOf(decoded)).toBe(Object.prototype)
    expect(Object.keys(decoded)).toEqual(['__proto__'])
  })

  test('validates with a Standard Schema', () => {
    const positive: StandardSchemaV1<unknown, {n: number}> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: (value) =>
          (value as {n: number}).n > 0
            ? {value: value as {n: number}}
            : {issues: [{message: 'n must be positive'}]},
      },
    }
    const codec = json({schema: positive})

    expect(codec.decode(encoder.encode('{"n":1}'))).toEqual({n: 1})
    expect(() => codec.decode(encoder.encode('{"n":0}'))).toThrow('n must be positive')
    // A value the schema refuses can't be written either, so it never fails a later load.
    expect(() => codec.encode({n: 0})).toThrow('n must be positive')

    const later = json({schema: {'~standard': {...positive['~standard'], validate: validateLater}}})
    expect(() => later.decode(encoder.encode('1'))).toThrow('synchronously')
  })
})

describe('commit IDs', () => {
  test('are 26 Crockford base32 characters that increase within a millisecond', () => {
    const next = commitIds(() => NOW)
    const ids = Array.from({length: 50}, next)

    for (const id of ids) {
      expect(id).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/)
    }
    const sorted = [...ids]
    sorted.sort()
    expect(sorted).toEqual(ids)
    expect(new Set(ids).size).toBe(ids.length)
    // The first ten characters are the timestamp.
    expect(ids[0]!.slice(0, 10)).toBe(commitIds(() => NOW)().slice(0, 10))
  })
})
