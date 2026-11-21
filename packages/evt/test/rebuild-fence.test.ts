import {describe, expect, test} from 'vitest'

import type {DynamoClient, TransactItem} from '../src/dynamodb/client.ts'
import type {Item} from '../src/dynamodb/item.ts'
import {DynamoRepository} from '../src/dynamodb/repository.ts'
import {
  RebuildBeginIndeterminateError,
  RebuildConflictError,
  RebuildEpochChangedError,
  RebuildLockHeldError,
  RebuildStateInvalidError,
  RebuildUnavailableError,
  TransientError,
  isRetryable,
} from '../src/errors.ts'
import {MemRepository} from '../src/memory/repository.ts'
import {type ProjectedView, putView} from '../src/projection.ts'
import {
  type RebuildState,
  checkRebuildOwner,
  checkRebuildViewBatch,
  observeRebuildEpoch,
  planRebuildBegin,
  readAtRebuildEpoch,
  rebuildLeaseSeconds,
} from '../src/rebuild-fence.ts'

const NOW = 1_790_000_000

function state(value: bigint, lock: RebuildState['lock'] = null, forceReleased = false) {
  return {generation: {value, forceReleased}, lock}
}

// A view operation of a rebuild batch, the `n`th.
function view(n: number): ProjectedView {
  return {
    projector: 'rebuild',
    projectorIndex: 0,
    operation: n,
    op: putView('pk', String(n), 'a', 't', '{}'),
  }
}

// A client over a fresh fence whose transactions fail with `failure` for the first `times`.
function client(failure: () => unknown, times = Infinity) {
  const sent: TransactItem[][] = []
  const fake: DynamoClient = {
    getItem: async () => ({}),
    putItem: () => Promise.reject(new Error('unexpected putItem')),
    query: () => Promise.reject(new Error('unexpected query')),
    scan: () => Promise.reject(new Error('unexpected scan')),
    async transactWriteItems(input) {
      sent.push(input.TransactItems)
      if (sent.length <= times) {
        throw failure()
      }
      return {}
    },
  }

  return {fake, sent}
}
function cancelled(code: string): Error {
  return Object.assign(new Error('cancelled'), {
    name: 'TransactionCanceledException',
    CancellationReasons: [{Code: code}, {Code: 'None'}],
    $metadata: {httpStatusCode: 400},
  })
}

describe('rebuild fence planning', () => {
  test.each<[string, RebuildState, unknown]>([
    [
      'acquires a fresh store',
      state(0n),
      {kind: 'acquire', increment: 1n, seenEven: 0n, seenOdd: -1n},
    ],
    ['acquires a settled one', state(4n), {kind: 'acquire', increment: 1n, seenEven: 4n}],
    [
      'acquires past a force-release',
      state(5n, null, true),
      {kind: 'acquireForceReleased', increment: 2n, seenEven: -1n, seenOdd: 5n},
    ],
    [
      'takes over an expired lease',
      state(5n, {owner: 'a', generation: 5n, leaseExpiresAt: NOW - 1}),
      {kind: 'takeover', increment: 2n, seenOdd: 5n, observedOwner: 'a'},
    ],
  ])('%s', (_name, observed, plan) => {
    expect(planRebuildBegin(observed, NOW)).toMatchObject(plan as object)
  })

  test('refuses a held lease, even at the second it expires, and rows that disagree', () => {
    const held = state(5n, {owner: 'a', generation: 5n, leaseExpiresAt: NOW})
    expect(() => planRebuildBegin(held, NOW)).toThrow(RebuildLockHeldError)
    expect(() => planRebuildBegin(state(5n), NOW)).toThrow(RebuildStateInvalidError)
    const even = state(4n, {owner: 'a', generation: 3n, leaseExpiresAt: NOW + 9})
    expect(() => planRebuildBegin(even, NOW)).toThrow(RebuildStateInvalidError)
  })

  test('checks owners, leases and batches before any request', () => {
    expect(() => checkRebuildOwner('')).toThrow(/empty/)
    expect(() => checkRebuildOwner('€'.repeat(171))).toThrow(/512 bytes/)
    checkRebuildOwner('€'.repeat(170))
    expect(() => checkRebuildOwner('a\uD800')).toThrow(/UTF-8/)
    expect(() => checkRebuildOwner('a\0')).toThrow(/U\+0000/)
    expect(rebuildLeaseSeconds(1001)).toBe(2)
    expect(() => rebuildLeaseSeconds(999)).toThrow(/at least one second/)

    expect(() => checkRebuildViewBatch([])).toThrow(/at least one/)
    checkRebuildViewBatch(Array.from({length: 99}, (_, n) => view(n)))
    expect(() => checkRebuildViewBatch(Array.from({length: 100}, (_, n) => view(n)))).toThrow(
      expect.objectContaining({code: 'commitTooLarge'}),
    )
  })
})

describe('rebuild epochs', () => {
  test('pin the settled generation and refuse an odd or a moved one', async () => {
    let now = NOW * 1000
    const repo = new MemRepository({clock: () => now})
    expect(await observeRebuildEpoch(repo)).toBe(0n)
    const lease = await repo.beginRebuild('job', 60_000)
    const unavailable = await observeRebuildEpoch(repo).catch((error: unknown) => error)
    expect(unavailable).toBeInstanceOf(RebuildUnavailableError)
    expect(isRetryable(unavailable)).toBe(true)

    await repo.endRebuild(lease)
    const moved = await readAtRebuildEpoch(repo, async () => {
      now += 1000
      await repo.endRebuild(await repo.beginRebuild('job', 60_000))
    }).catch((error: unknown) => error)
    expect(moved).toBeInstanceOf(RebuildEpochChangedError)
    expect(moved).toMatchObject({observed: 2n, current: 4n})
  })
})

describe('DynamoRepository rebuild begin', () => {
  test('reads and tries again after a lost race, up to eight times', async () => {
    const {fake, sent} = client(() => cancelled('ConditionalCheckFailed'), 2)
    const repo = new DynamoRepository(fake, 'events', {clock: () => NOW * 1000})
    const lease = await repo.beginRebuild('job', 60_000)
    expect(lease).toEqual({owner: 'job', generation: 1n, leaseExpiresAt: NOW + 60})
    expect(sent).toHaveLength(3)

    const always = client(() => cancelled('TransactionConflict'))
    const refused = new DynamoRepository(always.fake, 'events').beginRebuild('job', 60_000)
    await expect(refused).rejects.toBeInstanceOf(RebuildConflictError)
    expect(always.sent).toHaveLength(8)
  })

  test('reports an unknown result as indeterminate, and a throttle as transient', async () => {
    const lost = client(() => Object.assign(new Error('socket hang up'), {name: 'TimeoutError'}))
    const indeterminate = await new DynamoRepository(lost.fake, 'events', {clock: () => NOW * 1000})
      .beginRebuild('job', 60_000)
      .catch((error: unknown) => error)
    expect(indeterminate).toBeInstanceOf(RebuildBeginIndeterminateError)
    expect(indeterminate).toMatchObject({observed: 0n, lease: {generation: 1n}})

    const throttled = client(() =>
      Object.assign(new Error('slow down'), {
        name: 'ThrottlingException',
        $metadata: {httpStatusCode: 400},
      }),
    )
    const transient = new DynamoRepository(throttled.fake, 'events').beginRebuild('job', 60_000)
    await expect(transient).rejects.toBeInstanceOf(TransientError)
  })

  test('reads the fence rows strongly consistent, whatever the repository reads', async () => {
    const reads: unknown[] = []
    const fake: DynamoClient = {
      getItem: async (input) => {
        reads.push(input)
        return {Item: {value: {N: '3'}, forceReleased: {BOOL: true}} as Item}
      },
      putItem: () => Promise.reject(new Error('unexpected')),
      query: () => Promise.reject(new Error('unexpected')),
      scan: () => Promise.reject(new Error('unexpected')),
      transactWriteItems: () => Promise.reject(new Error('unexpected')),
    }
    const repo = new DynamoRepository(fake, 'events', {consistentRead: false})
    expect(await repo.rebuildGeneration()).toEqual({value: 3n, forceReleased: true})
    expect(reads[0]).toMatchObject({ConsistentRead: true})
  })
})
