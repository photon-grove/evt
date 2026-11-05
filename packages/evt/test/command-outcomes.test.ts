// Commands with durable outcomes at the store level (ADR 0004, C3 and C4): the outcome check
// every attempt starts with, replays and reused IDs, rejections with and without events, the
// single-aggregate retry, ambiguous-write recovery, the lookups, and a transaction's outcome
// methods. The rows' bytes are pinned by the outcomes scenarios in packages/evt-compat.

// Each test's steps depend on the ones before them, so they await in loops on purpose.
/* oxlint-disable no-await-in-loop */
import {afterEach, describe, expect, test, vi} from 'vitest'

import {defineAggregate, defineEvents} from '../src/aggregate.ts'
import {json} from '../src/codec.ts'
import {
  AmbiguousWriteError,
  CommandIdTooLongError,
  ConflictError,
  DuplicateCommandError,
  EvtError,
  FenceConflictError,
  IdempotencyKeyReuseError,
  IndeterminateError,
  OutcomeConflictError,
  OutcomePrincipalMismatchError,
  StreamHeadDriftError,
  TransientError,
  isRetryable,
} from '../src/errors.ts'
import {eventId} from '../src/event.ts'
import {MemRepository, headFixtures} from '../src/memory/repository.ts'
import {emptyMetadata} from '../src/metadata.ts'
import {type Outcome, encodeRejection} from '../src/outcome.ts'
import type {CallOptions, Commit, OutcomeRepository, TransactionCommit} from '../src/repository.ts'
import {Store, type StoreOptions} from '../src/store.ts'
import {FaultRepository} from '../src/testing/fault.ts'

const encoder = new TextEncoder()
const decoder = new TextDecoder()

const events = defineEvents({
  'ledger:added': {version: 1, codec: json<{amount: number}>()},
  'ledger:refused': {version: 1, codec: json<{amount: number}>()},
})

type Command =
  | {type: 'add'; amount: number}
  | {type: 'take'; amount: number}
  | {type: 'takeAudited'; amount: number}
  | {type: 'peek'}
  | {type: 'explode'}

// A response carrying the balance.
function balance(amount: number): Uint8Array {
  return encoder.encode(`{"balance":${amount}}`)
}

// Counts the handler's runs, so a test can tell a replay from a rerun.
let handled = 0

const ledger = defineAggregate({
  type: 'ledger',
  events,
  state: json<{balance: number}>(),
  initial: () => ({balance: 0}),
  handle(state, command: Command) {
    handled++
    switch (command.type) {
      case 'add':
        return {
          kind: 'accept',
          events: [{type: 'ledger:added', data: {amount: command.amount}}],
          response: balance(state.balance + command.amount),
        }
      case 'take':
      case 'takeAudited':
        if (command.amount > state.balance) {
          return {
            kind: 'reject',
            rejection: {
              code: 'insufficient',
              message: 'not enough',
              details: {balance: state.balance, requested: command.amount},
            },
            events:
              command.type === 'takeAudited'
                ? [{type: 'ledger:refused', data: {amount: command.amount}}]
                : [],
          }
        }
        return {kind: 'accept', events: [{type: 'ledger:added', data: {amount: -command.amount}}]}
      case 'peek':
        return {kind: 'accept', events: [], response: balance(state.balance)}
      case 'explode':
        throw new Error('the handler failed')
    }
  },
  apply: (state, event) => ({
    balance: event.type === 'ledger:added' ? state.balance + event.data.amount : state.balance,
  }),
  identify: (command: Command) => ({
    commandType: `ledger:${command.type}`,
    payload: encoder.encode(JSON.stringify(command)),
  }),
})

// Instant waits, recorded.
function waits(): {sleep: (ms: number) => Promise<void>; slept: number[]} {
  const slept: number[] = []

  return {
    slept,
    sleep: async (ms) => {
      slept.push(ms)
    },
  }
}

function outcomeStore(
  repository: OutcomeRepository = new MemRepository(),
  outcomes: StoreOptions['outcomes'] = {},
): Store {
  const options = outcomes === true ? {} : (outcomes ?? {})

  return new Store(repository, {
    snapshotInterval: 0,
    outcomes: {sleep: waits().sleep, recoveryWaitMs: 1000, ...options},
  })
}

function as(commandId: string, principalId: string | null = null) {
  return {metadata: {commandId, principalId}}
}

async function failure(call: Promise<unknown>): Promise<unknown> {
  try {
    await call
  } catch (error) {
    return error
  }

  return undefined
}

// A repository that counts its reads and passes every call on.
class Counting extends FaultRepository {
  reads = 0

  override outcome(commandId: string, options?: CallOptions): Promise<Outcome | null> {
    this.reads++
    return super.outcome(commandId, options)
  }

  override events(entityId: string, options?: CallOptions) {
    this.reads++
    return super.events(entityId, options)
  }
}

// A repository whose commits fail with `fail` until it returns null.
class Failing extends FaultRepository {
  commits = 0
  readonly #fail: (commit: number) => unknown

  constructor(inner: OutcomeRepository, fail: (commit: number) => unknown) {
    super(inner)
    this.#fail = fail
  }

  override async commit(commit: Commit, options?: CallOptions): Promise<void> {
    const error = this.#fail(++this.commits)
    if (error !== null) {
      throw error
    }
    return super.commit(commit, options)
  }

  override async commitTransaction(
    transaction: TransactionCommit,
    options?: CallOptions,
  ): Promise<void> {
    return super.commitTransaction(transaction, options)
  }
}

describe('Store.executeWithOutcome', () => {
  test('records the outcome with the events and the response, and replays it', async () => {
    const repository = new MemRepository()
    const store = outcomeStore(repository)

    const first = await store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 5}, as('c1'))
    expect(first.replayed).toBe(false)
    expect(first.outcome).toMatchObject({
      commandId: 'c1',
      status: 'accepted',
      eventIds: [eventId('l', 1n)],
      identity: {commandType: 'ledger:add', targets: ['l'], principal: null},
    })
    expect(decoder.decode(first.outcome.response!)).toBe('{"balance":5}')
    expect(first.events.map((event) => event.metadata.commitId)).toEqual(['c1'])

    const runs = handled
    const again = await store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 5}, as('c1'))
    expect(handled).toBe(runs)
    expect(again).toEqual({outcome: first.outcome, events: [], replayed: true})
    expect(await repository.events('l')).toHaveLength(1)
    expect(await repository.outcome('c1')).toEqual(first.outcome)
  })

  test('a different payload or principal under the same command ID is a reused key', async () => {
    const store = outcomeStore()
    await store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 5}, as('c1', 'user:1'))

    const payload = await failure(
      store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 6}, as('c1', 'user:1')),
    )
    expect(payload).toBeInstanceOf(IdempotencyKeyReuseError)
    expect((payload as IdempotencyKeyReuseError).prior.commandId).toBe('c1')

    const principal = await failure(
      store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 5}, as('c1', 'user:2')),
    )
    expect(principal).toBeInstanceOf(IdempotencyKeyReuseError)

    const target = await failure(
      store.executeWithOutcome(ledger, 'm', {type: 'add', amount: 5}, as('c1', 'user:1')),
    )
    expect(target).toBeInstanceOf(IdempotencyKeyReuseError)
  })

  test('a command with no events is an accepted no-op that still stores its response', async () => {
    const repository = new MemRepository()
    const store = outcomeStore(repository)
    await store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 3}, as('c1'))

    const peek = await store.executeWithOutcome(ledger, 'l', {type: 'peek'}, as('c2'))
    expect(peek.outcome.status).toBe('accepted')
    expect(peek.outcome.eventIds).toEqual([])
    expect(decoder.decode(peek.outcome.response!)).toBe('{"balance":3}')
    expect(await repository.events('l')).toHaveLength(1)
    expect((await repository.outcome('c2'))?.response).toEqual(peek.outcome.response)
  })

  test('a rejection is recorded once and replayed verbatim after the state changes', async () => {
    const repository = new MemRepository()
    const store = outcomeStore(repository)

    const rejected = await store.executeWithOutcome(
      ledger,
      'l',
      {type: 'take', amount: 9},
      as('c1'),
    )
    expect(rejected.outcome.status).toBe('rejected')
    expect(rejected.outcome.rejection?.code).toBe('insufficient')
    expect(rejected.events).toEqual([])
    expect(await repository.events('l')).toEqual([])

    await store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 100}, as('c2'))
    const replay = await store.executeWithOutcome(ledger, 'l', {type: 'take', amount: 9}, as('c1'))
    expect(replay.replayed).toBe(true)
    expect(replay.outcome).toEqual(rejected.outcome)
  })

  test("a rejection's events commit with the rejected outcome, and a replay writes none", async () => {
    const repository = new MemRepository()
    const store = outcomeStore(repository)

    const rejected = await store.executeWithOutcome(
      ledger,
      'l',
      {type: 'takeAudited', amount: 9},
      as('c1'),
    )
    expect(rejected.outcome.status).toBe('rejected')
    expect(rejected.outcome.eventIds).toEqual([eventId('l', 1n)])
    expect(rejected.events.map((event) => event.type)).toEqual(['ledger:refused'])
    expect(await repository.events('l')).toHaveLength(1)

    const replay = await store.executeWithOutcome(
      ledger,
      'l',
      {type: 'takeAudited', amount: 9},
      as('c1'),
    )
    expect(replay.replayed).toBe(true)
    expect(await repository.events('l')).toHaveLength(1)
  })

  test('a thrown handler error records nothing', async () => {
    const repository = new MemRepository()
    const store = outcomeStore(repository)

    const error = await failure(store.executeWithOutcome(ledger, 'l', {type: 'explode'}, as('c1')))
    expect((error as Error).message).toBe('the handler failed')
    expect(await repository.outcome('c1')).toBeNull()
  })

  test('the command ID and the digest are checked before anything is read', async () => {
    const repository = new Counting(new MemRepository())
    const store = outcomeStore(repository)

    const missing = await failure(
      store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, {metadata: {}}),
    )
    expect(missing).toMatchObject({code: 'invalidArgument'})
    const long = await failure(
      store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('x'.repeat(513))),
    )
    expect(long).toBeInstanceOf(CommandIdTooLongError)
    const reserved = await failure(
      store.executeWithOutcome(ledger, 'SYSTEM#FEATURES', {type: 'add', amount: 1}, as('c1')),
    )
    expect(reserved).toMatchObject({code: 'reservedEntityId'})

    const empty = outcomeStore(repository, {digest: () => ''})
    const digest = await failure(
      empty.executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c1')),
    )
    expect(digest).toMatchObject({code: 'invalidArgument'})
    expect(repository.reads).toBe(0)
  })

  test('a custom digester decides identity', async () => {
    const store = outcomeStore(new MemRepository(), {digest: (input) => input.commandType})
    const first = await store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c1'))
    expect(first.outcome.digest).toBe('ledger:add')

    // Another amount digests the same, so it's the same command.
    const again = await store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 2}, as('c1'))
    expect(again.replayed).toBe(true)
  })

  test('an event that predates outcomes and carries the command ID is a duplicate', async () => {
    const repository = new MemRepository()
    await new Store(repository).execute(
      ledger,
      'l',
      {type: 'add', amount: 1},
      {metadata: {commandId: 'c1'}},
    )

    const error = await failure(
      outcomeStore(repository).executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c1')),
    )
    expect(error).toBeInstanceOf(DuplicateCommandError)
    expect(await repository.outcome('c1')).toBeNull()
  })

  test('a conflict re-prepares the command from a fresh load', async () => {
    const inner = new MemRepository()
    const repository = new Failing(inner, (commit) =>
      commit === 1 ? new ConflictError('l', 'another writer won') : null,
    )
    const store = outcomeStore(repository)

    const result = await store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c1'))
    expect(result.outcome.status).toBe('accepted')
    expect(repository.commits).toBe(2)
  })

  test('a conflict on the last attempt records a stale-fence rejection', async () => {
    const inner = new MemRepository()
    const repository = new Failing(inner, () => new ConflictError('l', 'another writer won'))
    const store = outcomeStore(repository, {
      retry: {maxAttempts: 3, baseDelayMs: 0, maxDelayMs: 0},
      staleFenceCode: 'stale',
    })

    const result = await store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c1'))
    expect(repository.commits).toBe(3)
    expect(result.outcome.status).toBe('rejected')
    expect(encodeRejection(result.outcome.rejection!)).toBe(
      '{"code":"stale","message":"the state this command was decided on kept changing; ' +
        'retries were exhausted","details":{"attempts":3}}',
    )
    expect(await inner.outcome('c1')).toEqual(result.outcome)
  })

  test('a throttled backend is thrown when the attempts run out, recording nothing', async () => {
    const inner = new MemRepository()
    const repository = new Failing(inner, () => new TransientError('throttled'))
    const store = outcomeStore(repository, {retry: {maxAttempts: 2, baseDelayMs: 0, maxDelayMs: 0}})

    const error = await failure(
      store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c1')),
    )
    expect(error).toBeInstanceOf(TransientError)
    expect(repository.commits).toBe(2)
    expect(await inner.outcome('c1')).toBeNull()
  })

  test('stream head drift is thrown at once, never retried or recorded', async () => {
    const repository = new MemRepository()
    const store = outcomeStore(repository)
    await store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c1'))
    headFixtures(repository).appendBypassingHead({
      ...(await repository.events('l'))[0]!,
      id: eventId('l', 2n),
      sequence: 2n,
    })

    const error = await failure(
      store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c2')),
    )
    expect(error).toBeInstanceOf(StreamHeadDriftError)
    expect(await repository.outcome('c2')).toBeNull()
  })

  test('a lost race for the outcome row resolves through the winner', async () => {
    const inner = new MemRepository()
    const winner = outcomeStore(inner)
    let raced = false
    const repository = new Failing(inner, () => null)
    // Another writer records the same command between this one's check and its commit.
    repository.commit = async (commit, options) => {
      if (!raced) {
        raced = true
        await winner.executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c1'))
      }
      return inner.commit(commit, options)
    }

    const result = await outcomeStore(repository).executeWithOutcome(
      ledger,
      'l',
      {type: 'add', amount: 1},
      as('c1'),
    )
    expect(result.replayed).toBe(true)
    expect(await inner.events('l')).toHaveLength(1)
  })

  test("a lost race to another command's row is a reused key", async () => {
    const inner = new MemRepository()
    const repository = new Failing(inner, () => null)
    repository.commit = async (commit, options) => {
      repository.commit = (next, again) => inner.commit(next, again)
      await outcomeStore(inner).executeWithOutcome(ledger, 'm', {type: 'add', amount: 9}, as('c1'))
      return inner.commit(commit, options)
    }

    const error = await failure(
      outcomeStore(repository).executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c1')),
    )
    expect(error).toBeInstanceOf(IdempotencyKeyReuseError)
    expect(error).not.toBeInstanceOf(OutcomeConflictError)
  })
})

describe('ambiguous-write recovery', () => {
  test('a commit that applied returns its stored outcome without running again', async () => {
    const inner = new MemRepository()
    const repository = new FaultRepository(inner).applyThenFail()

    const runs = handled
    const result = await outcomeStore(repository).executeWithOutcome(
      ledger,
      'l',
      {type: 'add', amount: 2},
      as('c1'),
    )
    expect(handled).toBe(runs + 1)
    expect(result.replayed).toBe(true)
    expect(result.outcome.eventIds).toEqual([eventId('l', 1n)])
    expect(await inner.events('l')).toHaveLength(1)
  })

  test("a commit that didn't apply is a retryable ambiguous write, and the retry is safe", async () => {
    const inner = new MemRepository()
    const repository = new FaultRepository(inner).failWithoutApplying(new Error('lost'))
    const {sleep, slept} = waits()
    const store = outcomeStore(repository, {
      sleep,
      retry: {maxAttempts: 1, baseDelayMs: 0, maxDelayMs: 0},
    })

    const error = await failure(
      store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 2}, as('c1')),
    )
    expect(error).toBeInstanceOf(AmbiguousWriteError)
    expect(isRetryable(error)).toBe(true)
    expect(((error as AmbiguousWriteError).cause as Error).message).toBe('lost')
    // 50, 100, 200 and 400 ms, then what's left of the 1 s window.
    expect(slept).toEqual([50, 100, 200, 400, 250])
    expect(await inner.outcome('c1')).toBeNull()

    const retry = await store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 2}, as('c1'))
    expect(retry.replayed).toBe(false)
    expect(await inner.events('l')).toHaveLength(1)
  })

  test('the default policy runs an ambiguous write again, and it commits once', async () => {
    const inner = new MemRepository()
    const repository = new FaultRepository(inner).failWithoutApplying()

    const result = await outcomeStore(repository).executeWithOutcome(
      ledger,
      'l',
      {type: 'add', amount: 2},
      as('c1'),
    )
    expect(result.replayed).toBe(false)
    expect(await inner.events('l')).toHaveLength(1)
  })

  test('a recovered rejection is the stored rejection', async () => {
    const inner = new MemRepository()
    const repository = new FaultRepository(inner).applyThenFail()

    const result = await outcomeStore(repository).executeWithOutcome(
      ledger,
      'l',
      {type: 'take', amount: 9},
      as('c1'),
    )
    expect(result.replayed).toBe(true)
    expect(result.outcome.rejection?.code).toBe('insufficient')
  })
})

// Waits, in real time, until the call under test has set a timer, then lets `ms` pass.
async function pass(ms: number): Promise<void> {
  while (vi.getTimerCount() === 0) {
    await new Promise((resolve) => setImmediate(resolve))
  }
  await vi.advanceTimersByTimeAsync(ms)
}

// A repository whose outcome reads after its first commit behave as `read` says.
function readingAfterCommit(
  inner: OutcomeRepository,
  commitError: unknown,
  read: () => Promise<Outcome | null>,
  apply = false,
): FaultRepository {
  let committed = false
  const repository = new FaultRepository(inner)
  repository.commit = async (commit, options) => {
    committed = true
    if (apply) {
      await inner.commit(commit, options)
    }
    throw commitError
  }
  repository.outcome = (commandId, options) =>
    committed ? read() : inner.outcome(commandId, options)

  return repository
}

describe('recovery reads', () => {
  afterEach(() => {
    vi.useRealTimers()
  })

  const once = {retry: {maxAttempts: 1, baseDelayMs: 0, maxDelayMs: 0}}

  test("a recovery read that fails keeps both failures in the ambiguous write's cause", async () => {
    const lost = new Error('lost')
    const repository = readingAfterCommit(
      new MemRepository(),
      new IndeterminateError({cause: lost}),
      async () => {
        throw new Error('access denied')
      },
    )

    const error = await failure(
      outcomeStore(repository, once).executeWithOutcome(
        ledger,
        'l',
        {type: 'add', amount: 1},
        as('c1'),
      ),
    )
    expect(error).toBeInstanceOf(AmbiguousWriteError)
    const cause = (error as AmbiguousWriteError).cause as AggregateError
    expect(cause).toBeInstanceOf(AggregateError)
    expect(cause.errors).toEqual([lost, new Error('access denied')])
  })

  test('a malformed row found while recovering is corruption, not an ambiguous write', async () => {
    const corrupt = new EvtError('invalidRecord', 'bad row')
    const repository = readingAfterCommit(
      new MemRepository(),
      new IndeterminateError(),
      async () => {
        throw corrupt
      },
    )

    const error = await failure(
      outcomeStore(repository, once).executeWithOutcome(
        ledger,
        'l',
        {type: 'add', amount: 1},
        as('c1'),
      ),
    )
    expect(error).toBe(corrupt)
  })

  test('a read that never answers ends recovery at the window and its grace period', async () => {
    vi.useFakeTimers({toFake: ['setTimeout', 'clearTimeout']})
    const repository = readingAfterCommit(
      new MemRepository(),
      new IndeterminateError(),
      () => new Promise(() => {}),
    )

    const call = failure(
      outcomeStore(repository, {...once, recoveryWaitMs: 0}).executeWithOutcome(
        ledger,
        'l',
        {type: 'add', amount: 1},
        as('c1'),
      ),
    )
    await pass(1000)
    expect(await call).toBeInstanceOf(AmbiguousWriteError)
    expect(vi.getTimerCount()).toBe(0)
  })

  test("a lost outcome put whose winner's row can't be read in time is transient", async () => {
    vi.useFakeTimers({toFake: ['setTimeout', 'clearTimeout']})
    const repository = readingAfterCommit(
      new MemRepository(),
      new OutcomeConflictError('c1'),
      () => new Promise(() => {}),
    )

    const call = failure(
      outcomeStore(repository, {...once, recoveryWaitMs: 0}).executeWithOutcome(
        ledger,
        'l',
        {type: 'add', amount: 1},
        as('c1'),
      ),
    )
    await pass(1000)
    const error = await call
    expect(error).toBeInstanceOf(TransientError)
    expect((error as TransientError).cause).toBeInstanceOf(OutcomeConflictError)
  })

  test("a DynamoDB transaction conflict is answered by the winner's row", async () => {
    const inner = new MemRepository()
    const repository = readingAfterCommit(
      inner,
      new TransientError('transaction conflict', {transactionConflict: true}),
      () => inner.outcome('c1'),
      true,
    )

    const result = await outcomeStore(repository, once).executeWithOutcome(
      ledger,
      'l',
      {type: 'add', amount: 1},
      as('c1'),
    )
    expect(result.replayed).toBe(true)
  })

  test("recovery doesn't depend on the caller's signal", async () => {
    const inner = new MemRepository()
    const controller = new AbortController()
    const repository = new FaultRepository(inner)
    repository.commit = async (commit, options) => {
      await inner.commit(commit, options)
      controller.abort()
      throw new IndeterminateError()
    }

    const result = await outcomeStore(repository).executeWithOutcome(
      ledger,
      'l',
      {type: 'add', amount: 1},
      {...as('c1'), signal: controller.signal},
    )
    expect(result.replayed).toBe(true)
  })
})

describe('outcome lookups', () => {
  test('resolveOutcome reads the row without running anything', async () => {
    const store = outcomeStore()
    expect(await store.resolveOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c1'))).toBeNull()

    const recorded = await store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c1'))
    const runs = handled
    const resolved = await store.resolveOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c1'))
    expect(handled).toBe(runs)
    expect(resolved).toEqual({outcome: recorded.outcome, events: [], replayed: true})

    const other = await failure(
      store.resolveOutcome(ledger, 'l', {type: 'add', amount: 2}, as('c1')),
    )
    expect(other).toBeInstanceOf(IdempotencyKeyReuseError)
  })

  test('resolveCommand answers a single-aggregate command by its identity', async () => {
    const store = outcomeStore()
    const command: Command = {type: 'add', amount: 1}
    await store.executeWithOutcome(ledger, 'l', command, as('c1'))

    const identity = {
      commandType: 'ledger:add',
      targets: ['l'],
      payload: encoder.encode(JSON.stringify(command)),
    }
    expect((await store.resolveCommand(identity, as('c1')))?.replayed).toBe(true)
  })

  test('resolveCommandForPrincipal answers only the principal that recorded the command', async () => {
    const repository = new MemRepository()
    const store = outcomeStore(repository)
    await store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c1', 'user:1'))
    await store.executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c2'))

    expect((await store.resolveCommandForPrincipal('c1', 'user:1'))?.replayed).toBe(true)
    expect(await store.resolveCommandForPrincipal('c9', 'user:1')).toBeNull()
    const other = await failure(store.resolveCommandForPrincipal('c1', 'user:2'))
    expect(other).toMatchObject({code: 'outcomePrincipalMismatch', recorded: true})
    const none = await failure(store.resolveCommandForPrincipal('c1', null))
    expect(none).toBeInstanceOf(OutcomePrincipalMismatchError)

    // An empty principal is no principal.
    expect((await store.resolveCommandForPrincipal('c2', ''))?.replayed).toBe(true)

    // A row that records no identity can't be matched, so the lookup fails closed.
    await repository.commitTransaction({
      streams: [],
      outcome: {
        commandId: 'c3',
        status: 'accepted',
        digest: 'd',
        response: null,
        eventIds: [],
        rejection: null,
        identity: null,
      },
    })
    const unrecorded = await failure(store.resolveCommandForPrincipal('c3', null))
    expect(unrecorded).toMatchObject({code: 'outcomePrincipalMismatch', recorded: false})
  })
})

describe('a store with outcomes', () => {
  test('refuses the plain commit paths, and a store without them the outcome ones', async () => {
    const withOutcomes = outcomeStore()
    expect(
      await failure(withOutcomes.execute(ledger, 'l', {type: 'add', amount: 1}, as('c1'))),
    ).toMatchObject({code: 'invalidArgument'})
    expect(await failure(withOutcomes.transaction().commit())).toMatchObject({
      code: 'invalidArgument',
    })

    const without = new Store(new MemRepository())
    expect(without.recordsOutcomes).toBe(false)
    expect(
      await failure(without.executeWithOutcome(ledger, 'l', {type: 'add', amount: 1}, as('c1'))),
    ).toMatchObject({code: 'invalidArgument'})
    expect(await failure(without.resolveCommandForPrincipal('c1', null))).toMatchObject({
      code: 'invalidArgument',
    })
    expect(
      await failure(without.transaction().beginCommand({commandType: 't'}, as('c1'))),
    ).toMatchObject({code: 'invalidArgument'})
  })

  test('needs an outcome repository, and an aggregate that identifies its commands', async () => {
    const {outcome: _, ...plain} = new MemRepository()
    expect(() => new Store(plain as never, {outcomes: true})).toThrow(EvtError)

    const {identify: _identify, ...unidentified} = ledger
    const anonymous = defineAggregate(unidentified)
    const error = await failure(
      outcomeStore().executeWithOutcome(anonymous, 'l', {type: 'add', amount: 1}, as('c1')),
    )
    expect(error).toMatchObject({code: 'invalidArgument'})
  })
})

describe('Transaction with outcomes', () => {
  test('beginCommand replays a recorded command, which ends the transaction', async () => {
    const store = outcomeStore()
    const first = store.transaction()
    expect(await first.beginCommand({commandType: 'audit'}, as('c1'))).toEqual({kind: 'proceed'})
    const recorded = await first.commitWithOutcome({response: encoder.encode('ok')})
    expect(recorded.outcome.identity).toEqual({commandType: 'audit', targets: [], principal: null})

    const second = store.transaction()
    const begun = await second.beginCommand({commandType: 'audit'}, as('c1'))
    expect(begun).toEqual({kind: 'replayed', outcome: {...recorded, replayed: true}})
    expect(await failure(second.read(ledger, 'l'))).toMatchObject({code: 'invalidCommit'})

    const reused = await failure(store.transaction().beginCommand({commandType: 'other'}, as('c1')))
    expect(reused).toBeInstanceOf(IdempotencyKeyReuseError)
  })

  test('beginCommand comes first, and an outcome needs it', async () => {
    const store = outcomeStore()
    const late = store.transaction()
    await late.read(ledger, 'l')
    expect(await failure(late.beginCommand({commandType: 'audit'}, as('c1')))).toMatchObject({
      code: 'invalidCommit',
    })

    const unbegun = store.transaction()
    expect(await failure(unbegun.commitWithOutcome())).toMatchObject({code: 'invalidArgument'})
  })

  test('a failed beginCommand can be called again', async () => {
    const inner = new MemRepository()
    const repository = new FaultRepository(inner)
    let reads = 0
    repository.outcome = async (commandId, options) => {
      if (++reads === 1) {
        throw new TransientError('throttled')
      }
      return inner.outcome(commandId, options)
    }

    const transaction = outcomeStore(repository).transaction()
    expect(
      await failure(transaction.beginCommand({commandType: 'audit'}, as('c1'))),
    ).toBeInstanceOf(TransientError)
    expect(
      await failure(transaction.beginCommand({commandType: 'audit'}, {metadata: {}})),
    ).toMatchObject({code: 'invalidArgument'})
    expect(await transaction.beginCommand({commandType: 'audit'}, as('c1'))).toEqual({
      kind: 'proceed',
    })
    expect((await transaction.commitWithOutcome()).outcome.status).toBe('accepted')
  })

  test("a transaction's outcome commit is recovered like a command's", async () => {
    const inner = new MemRepository()
    const applied = outcomeStore(new FaultRepository(inner).applyThenFail()).transaction()
    await applied.beginCommand({commandType: 'audit'}, as('c1'))
    expect((await applied.commitWithOutcome()).replayed).toBe(true)

    const dropped = outcomeStore(new FaultRepository(inner).failWithoutApplying()).transaction()
    await dropped.beginCommand({commandType: 'audit'}, as('c2'))
    await dropped.read(ledger, 'a')
    expect(await failure(dropped.reject({code: 'no', message: 'x'}))).toBeInstanceOf(
      AmbiguousWriteError,
    )
    expect(await inner.outcome('c2')).toBeNull()

    const refused = outcomeStore(new FaultRepository(inner).applyThenFail()).transaction()
    await refused.beginCommand({commandType: 'audit'}, as('c3'))
    const a = await refused.read(ledger, 'a')
    refused.append(a, [{type: 'ledger:refused', data: {amount: 1}}])
    const result = await refused.rejectWithEvents({code: 'no', message: 'x'})
    expect(result).toMatchObject({replayed: true, outcome: {eventIds: [eventId('a', 1n)]}})
  })

  test('a lost race for the row outranks a stale fence the caller stamped', async () => {
    const repository = new MemRepository()
    const store = outcomeStore(repository)
    const transaction = store.transaction()
    await transaction.beginCommand({commandType: 'audit'}, as('c1'))
    transaction.fence({entityId: 'a', entityType: 'ledger', head: 0n})

    // Another attempt of the command records it, and moves the fenced stream.
    const winner = store.transaction()
    await winner.beginCommand({commandType: 'audit'}, as('c1'))
    winner.append(await winner.read(ledger, 'a'), [{type: 'ledger:added', data: {amount: 1}}])
    const won = await winner.commitWithOutcome()

    expect(await transaction.commitWithOutcome()).toEqual({...won, events: [], replayed: true})
  })

  test('commitWithOutcome writes the events and the outcome atomically', async () => {
    const repository = new MemRepository()
    const store = outcomeStore(repository)
    const transaction = store.transaction()
    await transaction.beginCommand({commandType: 'pay', targets: ['b', 'a']}, as('c1'))
    const a = await transaction.read(ledger, 'a')
    const b = await transaction.read(ledger, 'b')
    transaction.append(b, [{type: 'ledger:added', data: {amount: 2}}])
    transaction.append(a, [{type: 'ledger:added', data: {amount: 1}}])

    const result = await transaction.commitWithOutcome()
    expect(result.outcome.eventIds).toEqual([eventId('a', 1n), eventId('b', 1n)])
    expect(result.events.map((event) => event.entityId)).toEqual(['a', 'b'])
    expect(result.outcome.identity?.targets).toEqual(['a', 'b'])
    expect(await repository.outcome('c1')).toEqual(result.outcome)
  })

  test('reject records a fenced rejection and discards the staged events', async () => {
    const repository = new MemRepository()
    const store = outcomeStore(repository)
    const transaction = store.transaction()
    await transaction.beginCommand({commandType: 'pay'}, as('c1'))
    const a = await transaction.read(ledger, 'a')
    transaction.append(a, [{type: 'ledger:added', data: {amount: 1}}])

    const result = await transaction.reject({code: 'no', message: 'refused'})
    expect(result.outcome).toMatchObject({status: 'rejected', eventIds: []})
    expect(await repository.events('a')).toEqual([])
  })

  test('a rejection decided on stale state is not recorded', async () => {
    const repository = new MemRepository()
    const store = outcomeStore(repository)
    const transaction = store.transaction()
    await transaction.beginCommand({commandType: 'pay'}, as('c1'))
    await transaction.read(ledger, 'a')
    await outcomeStore(repository).executeWithOutcome(
      ledger,
      'a',
      {type: 'add', amount: 1},
      as('x'),
    )

    const error = await failure(transaction.reject({code: 'no', message: 'refused'}))
    expect(error).toBeInstanceOf(FenceConflictError)
    expect(await repository.outcome('c1')).toBeNull()
  })

  test('a stale fence the caller stamped records the stale-fence rejection', async () => {
    const repository = new MemRepository()
    const store = outcomeStore(repository)
    await store.executeWithOutcome(ledger, 'a', {type: 'add', amount: 1}, as('x'))

    const transaction = store.transaction()
    await transaction.beginCommand({commandType: 'pay'}, as('c1'))
    const b = await transaction.read(ledger, 'b')
    transaction.append(b, [{type: 'ledger:added', data: {amount: 1}}])
    transaction.fence({entityId: 'a', entityType: 'ledger', head: 0n})

    const result = await transaction.commitWithOutcome()
    expect(result.outcome.rejection?.code).toBe('stale-fence')
    expect(await repository.events('b')).toEqual([])
    expect(await repository.outcome('c1')).toEqual(result.outcome)
  })

  test('rejectWithEvents commits the staged events with the rejection', async () => {
    const repository = new MemRepository()
    const transaction = outcomeStore(repository).transaction()
    await transaction.beginCommand({commandType: 'pay'}, as('c1'))
    const a = await transaction.read(ledger, 'a')
    transaction.append(a, [{type: 'ledger:refused', data: {amount: 1}}])

    const result = await transaction.rejectWithEvents({code: 'no', message: 'refused'})
    expect(result.outcome).toMatchObject({status: 'rejected', eventIds: [eventId('a', 1n)]})
    expect((await repository.events('a'))[0]?.metadata.commandId).toBe('c1')
  })

  test('a command ID an older event carries is a duplicate', async () => {
    const repository = new MemRepository()
    await repository.commit({
      entityId: 'a',
      events: [
        {
          id: eventId('a', 1n),
          sequence: 1n,
          type: 'ledger:added',
          version: 1,
          entityId: 'a',
          entityType: 'ledger',
          payload: encoder.encode('{"amount":1}'),
          metadata: {...emptyMetadata(), commandId: 'c1'},
        },
      ],
    })

    const transaction = outcomeStore(repository).transaction()
    await transaction.beginCommand({commandType: 'pay'}, as('c1'))
    await transaction.read(ledger, 'a')
    expect(await failure(transaction.reject({code: 'no', message: 'x'}))).toBeInstanceOf(
      DuplicateCommandError,
    )
  })
})
