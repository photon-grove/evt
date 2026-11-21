import {describe, expect, test} from 'vitest'

import type {DynamoClient, TransactItem} from '../src/dynamodb/client.ts'
import type {Item} from '../src/dynamodb/item.ts'
import {DynamoRepository} from '../src/dynamodb/repository.ts'
import {
  RebuildBeginIndeterminateError,
  RebuildConflictError,
  RebuildEpochChangedError,
  RebuildLeaseLostError,
  RebuildLockHeldError,
  RebuildStateInvalidError,
  RebuildUnavailableError,
  TransientError,
  isRetryable,
} from '../src/errors.ts'
import {MemRepository} from '../src/memory/repository.ts'
import type {PgClient, PgResult} from '../src/postgres/client.ts'
import {PgRepository} from '../src/postgres/repository.ts'
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

// A DynamoDB client whose transactions are cancelled with `codes`, and whose generation is `value`.
function cancelling(codes: string[] | null, value = '5'): {fake: DynamoClient; sent: number[]} {
  const sent: number[] = []
  const fake: DynamoClient = {
    getItem: async () => ({Item: {value: {N: value}} as Item}),
    putItem: () => Promise.reject(new Error('unexpected putItem')),
    query: () => Promise.reject(new Error('unexpected query')),
    scan: () => Promise.reject(new Error('unexpected scan')),
    async transactWriteItems(input) {
      sent.push(input.TransactItems.length)
      if (codes !== null) {
        throw Object.assign(new Error('cancelled'), {
          name: 'TransactionCanceledException',
          CancellationReasons: codes.map((Code) => ({Code})),
          $metadata: {httpStatusCode: 400},
        })
      }
      return {}
    },
  }

  return {fake, sent}
}

const BATCH: ProjectedView[] = [view(0), view(1)]
const LEASE = {owner: 'job', generation: 5n, leaseExpiresAt: 0}

// What a guarded write of the batch rejects with when DynamoDB cancels it with `codes`.
function guarded(codes: string[]): Promise<unknown> {
  return new DynamoRepository(cancelling(codes).fake, 'events', {viewsTable: 'views'})
    .writeRebuildViews(LEASE, BATCH)
    .catch((error: unknown) => error)
}

describe('DynamoRepository rebuild failures', () => {
  test('a lost lease outranks every view failure of a guarded write', async () => {
    expect(
      await guarded(['ConditionalCheckFailed', 'None', 'ConditionalCheckFailed']),
    ).toBeInstanceOf(RebuildLeaseLostError)
    expect(await guarded(['None', 'ConditionalCheckFailed', 'None'])).toMatchObject({
      code: 'projectorOperation',
      operation: 1,
      reason: 'ConditionalCheckFailed',
    })
    expect(await guarded(['None', 'None', 'TransactionConflict'])).toBeInstanceOf(TransientError)
  })

  test('an end or a force-release that finds another lease reports it lost', async () => {
    const ended = cancelling(['ConditionalCheckFailed', 'None'])
    await expect(
      new DynamoRepository(ended.fake, 'events').endRebuild(LEASE),
    ).rejects.toBeInstanceOf(RebuildLeaseLostError)

    // A settled generation has no lease to release: nothing is sent.
    const settled = cancelling(null, '4')
    const released = new DynamoRepository(settled.fake, 'events').forceReleaseRebuild('job')
    await expect(released).rejects.toBeInstanceOf(RebuildLeaseLostError)
    expect(settled.sent).toEqual([])
  })
})

// A PostgreSQL client over the fence rows, which fails statements as `fail` decides and records
// every statement.
function pgFence(
  fail: (text: string, index: number) => unknown,
  rows: Record<string, PgRow[]> = {},
) {
  const log: string[] = []
  const query = async (text: string): Promise<PgResult> => {
    log.push(text)
    const failure = fail(text, log.length)
    if (failure !== undefined) {
      throw failure
    }
    for (const [prefix, result] of Object.entries(rows)) {
      if (text.startsWith(prefix)) {
        return {rows: result}
      }
    }
    return {rows: text.includes('RETURNING 1') ? [{}] : []}
  }
  const pg: PgClient = {query, connect: async () => ({query, release: () => undefined})}

  return {client: pg, log}
}

type PgRow = Record<string, unknown>

describe('PgRepository rebuild failures', () => {
  test('reads again when its rows moved, and reports an unknown COMMIT as indeterminate', async () => {
    let lost = true
    const moved = pgFence((text) => {
      if (text.startsWith('INSERT INTO evt_events__evt_rebuild_lock') && lost) {
        lost = false
        return Object.assign(new Error('duplicate key'), {code: '23505'})
      }
      return undefined
    })
    const repo = new PgRepository(moved.client, {clock: () => NOW * 1000})
    expect(await repo.beginRebuild('job', 60_000)).toEqual({
      owner: 'job',
      generation: 1n,
      leaseExpiresAt: NOW + 60,
    })
    expect(moved.log.filter((text) => text === 'ROLLBACK')).toHaveLength(1)
    expect(moved.log.at(-1)).toBe('COMMIT')

    const dropped = pgFence((text) =>
      text === 'COMMIT' ? new Error('connection reset') : undefined,
    )
    const indeterminate = await new PgRepository(dropped.client, {clock: () => NOW * 1000})
      .beginRebuild('job', 60_000)
      .catch((error: unknown) => error)
    expect(indeterminate).toBeInstanceOf(RebuildBeginIndeterminateError)
    expect(indeterminate).toMatchObject({observed: 0n, lease: {generation: 1n}})
  })

  test('a release or a guarded write without the lease is refused and rolled back', async () => {
    const settled = pgFence(() => undefined, {
      'SELECT value::text': [{value: '4', force_released: 'false'}],
    })
    const repo = new PgRepository(settled.client, {viewWriter: () => ({sql: 'SELECT 1'})})
    await expect(repo.endRebuild(LEASE)).rejects.toBeInstanceOf(RebuildLeaseLostError)
    await expect(repo.forceReleaseRebuild('job')).rejects.toBeInstanceOf(RebuildLeaseLostError)
    expect(settled.log.filter((text) => text.startsWith('DELETE'))).toEqual([])

    // No lock row: the batch never runs.
    await expect(repo.writeRebuildViews(LEASE, BATCH)).rejects.toBeInstanceOf(RebuildLeaseLostError)
    expect(settled.log).not.toContain('SELECT 1')
    expect(settled.log.at(-1)).toBe('ROLLBACK')
  })
})
