// The full projection rebuild: current-state projectors over snapshot-seeded entities, history
// projectors replayed commit by commit, the checks across the run, the guarded batches, and when
// a failed run releases the lock.
import {describe, expect, test} from 'vitest'

import {defineAggregate, defineEvents} from '../src/aggregate.ts'
import {json} from '../src/codec.ts'
import {
  CompactionUncoveredError,
  DuplicateViewKeyError,
  EvtError,
  ProjectorError,
  RebuildBeginIndeterminateError,
  RebuildConflictError,
  RebuildFailure,
  RebuildInProgressError,
  RebuildLeaseLostError,
  RebuildLockHeldError,
  SharedViewKeyError,
  TransientError,
  isConflict,
  isRetryable,
} from '../src/errors.ts'
import {type SerializedEvent, eventId} from '../src/event.ts'
import {MemRepository} from '../src/memory/repository.ts'
import {emptyMetadata} from '../src/metadata.ts'
import {
  type CommitProjector,
  type ProjectedView,
  ViewReplay,
  deleteView,
  engineProjector,
  putView,
} from '../src/projection.ts'
import {type RebuildLease, acquiredFromSettled, holdsRebuildBegun} from '../src/rebuild-fence.ts'
import {
  ProjectionRebuild,
  type ProjectionRebuildOptions,
  type RebuildProgress,
} from '../src/rebuild.ts'
import type {CallOptions} from '../src/repository.ts'

const encoder = new TextEncoder()
const decoder = new TextDecoder()
const START = 1_790_000_000_000

const ledger = defineAggregate({
  type: 'ledger',
  events: defineEvents({'ledger:added': {version: 1, codec: json<{amount: number}>()}}),
  state: json<{total: number}>(),
  initial: () => ({total: 0}),
  handle: () => ({kind: 'accept', events: []}),
  apply: (state, added) => ({total: state.total + added.data.amount}),
})

// A stored event adding `amount`, of commit `commitId`.
function event(
  entityId: string,
  sequence: number,
  amount: number,
  commitId = '',
  entityType = 'ledger',
): SerializedEvent {
  return {
    id: eventId(entityId, BigInt(sequence)),
    sequence: BigInt(sequence),
    type: 'ledger:added',
    version: 1,
    entityId,
    entityType,
    payload: encoder.encode(`{"amount":${amount}}`),
    metadata: {...emptyMetadata(), commitId},
  }
}

// A current-state projector: each stream's state at `totals`/<id>.
const totals: CommitProjector = {
  name: 'totals',
  project: (commit) =>
    commit.streams.map((stream) =>
      putView('totals', stream.entityId, stream.entityId, stream.entityType, stream.state),
    ),
}

// A history projector: the last commit of each ledger with its total at `last`/<id>, deleted by a
// commit that adds nothing.
const last: CommitProjector = {
  name: 'last',
  history: ['ledger'],
  project: (commit) =>
    commit.streams.flatMap((stream) => {
      if (stream.events.length === 0) {
        return []
      }
      const zero = stream.events.some((each) => decoder.decode(each.payload!) === '{"amount":0}')
      if (zero) {
        return [deleteView('last', stream.entityId, stream.entityId)]
      }
      const {total} = JSON.parse(decoder.decode(stream.state)) as {total: number}
      const payload = JSON.stringify({commit: commit.commitId, total})

      return [putView('last', stream.entityId, stream.entityId, 'ledger', payload)]
    }),
}

function rebuild(
  repository: MemRepository,
  options: Partial<ProjectionRebuildOptions> = {},
): ProjectionRebuild {
  return new ProjectionRebuild(repository, {
    projectors: [totals],
    aggregates: [ledger],
    ...options,
  })
}

function payloadOf(repository: MemRepository, pk: string, sk: string): string | null {
  const row = repository.view(pk, sk)

  return row === null ? null : decoder.decode(row.payload)
}

async function errorOf(promise: Promise<unknown>): Promise<unknown> {
  return promise.then(
    () => {
      throw new Error('expected a failure')
    },
    (error: unknown) => error,
  )
}

function clock(): {now: () => number; advance: (ms: number) => void} {
  let at = START

  return {now: () => at, advance: (ms) => (at += ms)}
}

describe('ProjectionRebuild configuration', () => {
  test('refuses what it cannot run, before it reads or locks anything', async () => {
    const repository = new MemRepository()
    const engine = engineProjector({name: 'engine', project: () => []})
    const cases: [Partial<ProjectionRebuildOptions>, string][] = [
      [{projectors: []}, 'at least one commit-wide projector'],
      [{projectors: [engine]}, 'reads engine metadata'],
      [{aggregates: []}, 'at least one aggregate type'],
      [{aggregates: [ledger, {aggregate: ledger, entityType: 'ledger'}]}, 'registered twice'],
      [
        {projectors: [{...last, history: ['ledger', 'team']}]},
        'history-dependent projector "last" reads entity type "team", which is not registered',
      ],
    ]
    // Each case runs after the one before it.
    /* oxlint-disable no-await-in-loop */
    for (const [options, message] of cases) {
      const run = rebuild(repository, options)
      for (const attempt of [run.run('rebuild-1', 60_000), run.plan()]) {
        const error = await errorOf(attempt)
        expect(error).toBeInstanceOf(EvtError)
        expect((error as EvtError).code).toBe('invalidArgument')
        expect((error as EvtError).message).toContain(message)
      }

      const failure = await errorOf(run.runWithLease(leaseOf('rebuild-1', 1n), 60_000))
      expect(failure).toBeInstanceOf(RebuildFailure)
      expect((failure as RebuildFailure).attempted).toBe(0)
      expect((failure as RebuildFailure).error).toBeInstanceOf(EvtError)
    }
    /* oxlint-enable no-await-in-loop */
    expect(await repository.rebuildState()).toEqual({
      generation: {value: 0n, forceReleased: false},
      lock: null,
    })

    // A lease renewed for less than a second is refused the same way.
    const short = await errorOf(rebuild(repository).runWithLease(leaseOf('rebuild-1', 1n), 999))
    expect((short as RebuildFailure).message).toContain('at least one second')
  })
})

function leaseOf(owner: string, generation: bigint, leaseExpiresAt = 0): RebuildLease {
  return {owner, generation, leaseExpiresAt}
}

describe('ProjectionRebuild current-state projectors', () => {
  test('rebuilds each entity from its snapshot, compacted ones included, and settles', async () => {
    const repository = new MemRepository()
    await repository.commit({
      entityId: 'z',
      events: [event('z', 1, 1), event('z', 2, 2)],
      snapshot: {entityType: 'ledger', sequence: 1n, payload: encoder.encode('{"total":30}')},
    })
    await repository.commit({entityId: 'z', events: [event('z', 3, 4)]})
    expect(await repository.compactBelow('z', 2n)).toBe(2)
    await repository.commit({entityId: 'a', events: [event('a', 1, 5)]})
    await repository.commit({entityId: 'r', events: [event('r', 1, 7, '', 'renamed')]})
    // A stale row the rebuild overwrites, and one no projector produces, which it leaves alone.
    await repository.commit({
      entityId: 'a',
      events: [event('a', 2, 0)],
      views: [
        {
          projector: 'p',
          projectorIndex: 0,
          operation: 0,
          op: putView('totals', 'a', 'a', 'x', '1'),
        },
        {projector: 'p', projectorIndex: 0, operation: 1, op: putView('old', 'a', 'a', 'x', '2')},
      ],
    })

    const report = await rebuild(repository, {
      aggregates: [ledger, {aggregate: ledger, entityType: 'renamed'}],
    }).run('rebuild-1', 60_000)

    expect(report).toEqual({entities: 3, commits: 0, puts: 3, deletes: 0, batches: 1, events: 0})
    expect(payloadOf(repository, 'totals', 'a')).toBe('{"total":5}')
    expect(payloadOf(repository, 'totals', 'z')).toBe('{"total":34}')
    expect(repository.view('totals', 'r')).toMatchObject({entityId: 'r', entityType: 'renamed'})
    expect(payloadOf(repository, 'old', 'a')).toBe('2')
    expect(await repository.rebuildState()).toEqual({
      generation: {value: 2n, forceReleased: false},
      lock: null,
    })
  })

  test("refuses a row another stream owns, and keeps each projector's index", async () => {
    const repository = new MemRepository()
    await repository.commit({entityId: 'a', events: [event('a', 1, 1)]})
    const foreign: CommitProjector = {
      name: 'foreign',
      project: () => [putView('team', 'x', 'team-1', 'team', '{}')],
    }

    const error = await errorOf(
      rebuild(repository, {projectors: [last, foreign]}).run('rebuild-1', 60_000),
    )
    expect((error as EvtError).code).toBe('invalidViewOperation')
    expect((error as EvtError).message).toContain('projector "foreign" (#1)')
    // Nothing was written, and the lock from a settled generation was released.
    expect(repository.views()).toEqual([])
    expect((await repository.rebuildGeneration()).value).toBe(2n)
  })
})

// A projector every stream writes one row with, as its owner.
function shared(history?: string[]): CommitProjector {
  return {
    name: 'shared',
    history,
    project: (commit) =>
      commit.streams.map((stream) => putView('shared', 'row', stream.entityId, 'ledger', '{}')),
  }
}

describe('ProjectionRebuild history projectors', () => {
  test('replays every commit across streams, writing the rows the replay leaves', async () => {
    const repository = new MemRepository()
    await repository.commitTransaction({
      streams: [
        {entityId: 'b', events: [event('b', 1, 2, 'c1')]},
        {entityId: 'a', events: [event('a', 1, 1, 'c1')]},
      ],
    })
    await repository.commit({entityId: 'a', events: [event('a', 2, 2, 'c2')]})
    const run = rebuild(repository, {projectors: [last]})

    expect(await run.run('rebuild-1', 60_000)).toEqual({
      entities: 0,
      commits: 2,
      puts: 2,
      deletes: 0,
      batches: 1,
      events: 3,
    })
    expect(payloadOf(repository, 'last', 'a')).toBe('{"commit":"c2","total":3}')
    expect(payloadOf(repository, 'last', 'b')).toBe('{"commit":"c1","total":2}')

    // A later commit deletes a row: the rebuild runs over the existing rows and writes the delete.
    await repository.commit({entityId: 'b', events: [event('b', 2, 0, 'c3')]})
    const plan = await run.plan()
    expect(plan.operations.map(({op}) => [op.kind, op.sk])).toEqual([
      ['put', 'a'],
      ['delete', 'b'],
    ])
    expect(await run.run('rebuild-2', 60_000)).toMatchObject({commits: 3, puts: 1, deletes: 1})
    expect(repository.view('last', 'b')).toBeNull()
    expect(payloadOf(repository, 'last', 'a')).toBe('{"commit":"c2","total":3}')
  })

  test('refuses a compacted history type with nothing written, and releases the lock', async () => {
    const repository = new MemRepository()
    await repository.commit({
      entityId: 'a',
      events: [event('a', 1, 1), event('a', 2, 1)],
      snapshot: {entityType: 'ledger', sequence: 1n, payload: encoder.encode('{"total":2}')},
    })
    await repository.commit({entityId: 'a', events: [event('a', 3, 1)]})
    await repository.compactBelow('a', 2n)
    await repository.commit({entityId: 'b', events: [event('b', 1, 1)]})

    const error = await errorOf(
      rebuild(repository, {projectors: [totals, last]}).run('rebuild-1', 60_000),
    )
    expect(error).toBeInstanceOf(CompactionUncoveredError)
    expect(error).toMatchObject({entityId: 'a', through: 2n, covered: 2n})
    expect(repository.views()).toEqual([])
    expect(await repository.rebuildState()).toEqual({
      generation: {value: 2n, forceReleased: false},
      lock: null,
    })

    // A current-state projector alone is seeded from the snapshot, so compaction is no matter.
    expect(await rebuild(repository).run('rebuild-2', 60_000)).toMatchObject({entities: 2})
    expect(payloadOf(repository, 'totals', 'a')).toBe('{"total":3}')
  })

  test('refuses a row two owners write, and a key one owner plans twice', async () => {
    const repository = new MemRepository()
    await repository.commit({entityId: 'a', events: [event('a', 1, 1, 'c1')]})
    await repository.commit({entityId: 'b', events: [event('b', 1, 1, 'c2')]})
    /* oxlint-disable no-await-in-loop */
    for (const history of [['ledger'], undefined]) {
      const error = await errorOf(
        rebuild(repository, {projectors: [shared(history)]}).run('rebuild', 60_000),
      )
      expect(error).toBeInstanceOf(SharedViewKeyError)
      expect(error).toMatchObject({pk: 'shared', sk: 'row', first: 'a', second: 'b'})
    }
    /* oxlint-enable no-await-in-loop */
    const twice: CommitProjector = {
      name: 'twice',
      project: (commit) =>
        commit.streams.map((stream) => putView('last', stream.entityId, stream.entityId, 'l', '1')),
    }
    const error = await errorOf(rebuild(repository, {projectors: [last, twice]}).plan())
    expect(error).toBeInstanceOf(DuplicateViewKeyError)
    expect(repository.views()).toEqual([])
    expect(await repository.rebuildState()).toMatchObject({lock: null})
  })
})

function item(op: ProjectedView['op']): ProjectedView {
  return {projector: 'p', projectorIndex: 0, operation: 0, op}
}

describe('ViewReplay', () => {
  test('keeps the last operation per key, by key bytes, and refuses a second owner', () => {
    const replay = new ViewReplay()
    replay.apply(item(putView('p', 'é', 'a', 't', '1')))
    replay.apply(item(putView('p', 'z', 'a', 't', '1')))
    replay.apply(item(putView('p', 'Z', 'a', 't', '1')))
    replay.apply(item(putView('p', 'z', 'a', 't', '2')))
    replay.apply(item(deleteView('p', 'Z', 'a')))
    expect(replay.intoLast().map(({op}) => [op.sk, op.kind, decoder.decode(op.payload)])).toEqual([
      ['Z', 'delete', ''],
      ['z', 'put', '2'],
      ['é', 'put', '1'],
    ])

    let error: unknown
    try {
      replay.apply(item(deleteView('p', 'z', 'b')))
    } catch (thrown) {
      error = thrown
    }
    expect(error).toBeInstanceOf(SharedViewKeyError)
    expect((error as SharedViewKeyError).code).toBe('sharedViewKey')
    expect((error as SharedViewKeyError).message).toBe(
      'view row "p"/"z" is written for streams "a" and "b", whose commits have no durable order',
    )
  })
})

// A memory repository whose fence calls a test can intercept.
class Intercepted extends MemRepository {
  renewals = 0
  writes = 0
  beforeWrite: (repository: Intercepted, n: number) => Promise<void> = async () => {}
  beforeEnd: (repository: Intercepted, held: RebuildLease) => Promise<void> = async () => {}
  begin: ((owner: string, leaseMs: number) => Promise<RebuildLease>) | null = null

  override async renewRebuild(lease: RebuildLease, leaseMs: number, options?: CallOptions) {
    this.renewals++
    return super.renewRebuild(lease, leaseMs, options)
  }

  override async writeRebuildViews(
    lease: RebuildLease,
    views: readonly ProjectedView[],
    options?: CallOptions,
  ) {
    this.writes++
    await this.beforeWrite(this, this.writes)
    return super.writeRebuildViews(lease, views, options)
  }

  override async endRebuild(lease: RebuildLease, options?: CallOptions) {
    await this.beforeEnd(this, lease)
    return super.endRebuild(lease, options)
  }

  override async beginRebuild(owner: string, leaseMs: number, options?: CallOptions) {
    return this.begin === null
      ? super.beginRebuild(owner, leaseMs, options)
      : this.begin(owner, leaseMs)
  }

  plainBegin(owner: string, leaseMs: number): Promise<RebuildLease> {
    return super.beginRebuild(owner, leaseMs)
  }
}

async function ledgers(repository: MemRepository, count: number): Promise<void> {
  await Promise.all(
    Array.from({length: count}, (_, n) => {
      const entityId = `e${String(n).padStart(3, '0')}`

      return repository.commit({entityId, events: [event(entityId, 1, n)]})
    }),
  )
}

describe('ProjectionRebuild writes', () => {
  test('writes guarded batches of at most 99 operations, reporting progress', async () => {
    const repository = new Intercepted()
    await ledgers(repository, 150)
    const progress: RebuildProgress[] = []
    const run = rebuild(repository, {onProgress: (each) => progress.push(each)})

    // The dry run takes no lock, writes nothing and reports nothing.
    const plan = await run.plan()
    expect(plan).toMatchObject({batches: 2, report: {entities: 150, puts: 150, batches: 0}})
    expect(plan.operations).toHaveLength(150)
    expect(repository.views()).toEqual([])
    expect(progress).toEqual([])
    expect((await repository.rebuildGeneration()).value).toBe(0n)

    expect(await run.run('rebuild-1', 60_000)).toMatchObject({puts: 150, batches: 2})
    expect(repository.writes).toBe(2)
    expect(repository.views()).toHaveLength(150)
    expect(progress).toEqual([
      {
        kind: 'planned',
        report: {entities: 150, commits: 0, puts: 150, deletes: 0, batches: 0, events: 0},
        batches: 2,
      },
      {kind: 'wrote', batch: 1, of: 2},
      {kind: 'wrote', batch: 2, of: 2},
    ])
  })

  test('a lease lost during the write fails the run and leaves the lock to its taker', async () => {
    const time = clock()
    const repository = new Intercepted({clock: time.now})
    await ledgers(repository, 150)
    repository.beforeWrite = async (repo, n) => {
      if (n === 2) {
        time.advance(120_000)
        await repo.plainBegin('other', 60_000)
      }
    }

    const error = await errorOf(rebuild(repository).run('rebuild-1', 60_000))
    expect(error).toBeInstanceOf(RebuildLeaseLostError)
    expect((await repository.rebuildState()).lock?.owner).toBe('other')
    // The first batch landed: the rows are half-written, which only a completed rebuild repairs.
    expect(repository.views()).toHaveLength(99)
  })

  test('a failed write keeps the lock, and only a completed rebuild settles it', async () => {
    const time = clock()
    const repository = new Intercepted({clock: time.now})
    await ledgers(repository, 150)
    repository.beforeWrite = async (_repo, n) => {
      if (n === 2) {
        throw new TransientError('throttled')
      }
    }

    expect(await errorOf(rebuild(repository).run('rebuild-1', 60_000))).toBeInstanceOf(
      TransientError,
    )
    expect((await repository.rebuildState()).lock?.owner).toBe('rebuild-1')
    expect(
      await errorOf(repository.commit({entityId: 'a', events: [event('a', 1, 1)]})),
    ).toBeInstanceOf(RebuildInProgressError)
    expect(await errorOf(rebuild(repository).run('rebuild-2', 60_000))).toBeInstanceOf(
      RebuildLockHeldError,
    )

    // A takeover of the lapsed lease that fails before any write keeps the lock too: an earlier
    // rebuild may have left the rows half-written.
    repository.beforeWrite = async () => {}
    time.advance(120_000)
    const failing: CommitProjector = {
      name: 'failing',
      project: () => {
        throw new Error('boom')
      },
    }
    expect(
      await errorOf(rebuild(repository, {projectors: [failing]}).run('rebuild-2', 60_000)),
    ).toBeInstanceOf(ProjectorError)
    expect((await repository.rebuildState()).lock?.owner).toBe('rebuild-2')

    time.advance(120_000)
    expect(await rebuild(repository).run('rebuild-3', 60_000)).toMatchObject({puts: 150})
    expect(await repository.rebuildState()).toEqual({
      generation: {value: 6n, forceReleased: false},
      lock: null,
    })
  })

  test('a run acquired after a force-release keeps the lock on an early failure', async () => {
    const repository = new MemRepository()
    await repository.commit({entityId: 'a', events: [event('a', 1, 1)]})
    await repository.beginRebuild('crashed', 60_000)
    await repository.forceReleaseRebuild('crashed')
    const failing: CommitProjector = {
      name: 'failing',
      project: () => {
        throw new Error('boom')
      },
    }

    expect(
      await errorOf(rebuild(repository, {projectors: [failing]}).run('rebuild-1', 60_000)),
    ).toBeInstanceOf(ProjectorError)
    expect(await repository.rebuildState()).toMatchObject({
      generation: {value: 5n},
      lock: {owner: 'rebuild-1', generation: 5n},
    })
  })

  test('an end that fails after every write is the run error', async () => {
    const repository = new Intercepted()
    await ledgers(repository, 2)
    repository.beforeEnd = async (repo, held) => {
      repository.beforeEnd = async () => {}
      await repo.forceReleaseRebuild(held.owner)
    }

    expect(await errorOf(rebuild(repository).run('rebuild-1', 60_000))).toBeInstanceOf(
      RebuildLeaseLostError,
    )
    expect(repository.views()).toHaveLength(2)
    expect(await repository.rebuildGeneration()).toEqual({value: 3n, forceReleased: true})
  })
})

describe('ProjectionRebuild locking', () => {
  test('begins again when an unknown begin can no longer apply, and gives up after eight', async () => {
    const repository = new Intercepted()
    await ledgers(repository, 1)
    let calls = 0
    repository.begin = async (owner, leaseMs) => {
      calls++
      if (calls === 1) {
        // Another rebuild came and went, so this begin, conditioned on generation 0, can't land.
        const other = await repository.plainBegin('other', 60_000)
        await repository.endRebuild(other)
        throw new RebuildBeginIndeterminateError(leaseOf(owner, 1n), 0n, new Error('timeout'))
      }
      return repository.plainBegin(owner, leaseMs)
    }
    expect(await rebuild(repository).run('rebuild-1', 60_000)).toMatchObject({entities: 1})
    expect(calls).toBe(2)
    expect(await repository.rebuildGeneration()).toEqual({value: 4n, forceReleased: false})

    // Always moved: the eighth round gives up with a retryable conflict.
    calls = 0
    repository.begin = async (owner) => {
      calls++
      const other = await repository.plainBegin('other', 60_000)
      await repository.endRebuild(other)
      throw new RebuildBeginIndeterminateError(leaseOf(owner, 1n), 0n, new Error('timeout'))
    }
    const conflict = await errorOf(rebuild(repository).run('rebuild-1', 60_000))
    expect(conflict).toBeInstanceOf(RebuildConflictError)
    expect(isRetryable(conflict)).toBe(true)
    expect(calls).toBe(8)
  })

  test('returns an unknown begin that applied, or may still apply, to the caller', async () => {
    const repository = new Intercepted()
    await ledgers(repository, 1)
    repository.begin = async (owner, leaseMs) => {
      const applied = await repository.plainBegin(owner, leaseMs)
      throw new RebuildBeginIndeterminateError(applied, applied.generation - 1n, new Error('lost'))
    }
    const applied = await errorOf(rebuild(repository).run('rebuild-1', 60_000))
    expect(applied).toBeInstanceOf(RebuildBeginIndeterminateError)
    const state = await repository.rebuildState()
    const {lease: begun, observed} = applied as RebuildBeginIndeterminateError
    expect(holdsRebuildBegun(begun, observed, state)).toBe(true)
    await repository.endRebuild(begun)

    repository.begin = async (owner) => {
      throw new RebuildBeginIndeterminateError(leaseOf(owner, 3n), 2n, new Error('lost'))
    }
    const pending = await errorOf(rebuild(repository).run('rebuild-2', 60_000))
    expect(pending).toBeInstanceOf(RebuildBeginIndeterminateError)
    expect(holdsRebuildBegun(leaseOf('rebuild-2', 3n), 2n, await repository.rebuildState())).toBe(
      null,
    )
  })
})

describe('ProjectionRebuild.runWithLease', () => {
  test("renews the caller's lease as it goes and returns it to end", async () => {
    const time = clock()
    const repository = new Intercepted({clock: time.now})
    await ledgers(repository, 150)
    const before = await repository.rebuildGeneration()
    const held = await repository.beginRebuild('caller', 60_000)
    expect(acquiredFromSettled(held, before)).toBe(true)
    time.advance(30_000)

    // A one-second renewal is due at every check: at the start, after each entity, before each
    // batch.
    const run = await rebuild(repository).runWithLease(held, 1000)
    expect(run.report).toMatchObject({entities: 150, batches: 2})
    expect(repository.renewals).toBe(1 + 150 + 2)
    expect(run.lease).toEqual({...held, leaseExpiresAt: held.leaseExpiresAt - 60 + 30 + 1})
    // The caller ends it.
    expect((await repository.rebuildState()).lock?.owner).toBe('caller')
    await repository.endRebuild(run.lease)
  })

  test('reports a failure with the batches attempted, the lease, and its retryability', async () => {
    const repository = new Intercepted()
    await ledgers(repository, 150)
    repository.beforeWrite = async (_repo, n) => {
      if (n === 2) {
        throw new TransientError('throttled')
      }
    }
    const held = await repository.beginRebuild('caller', 60_000)

    const failure = await errorOf(rebuild(repository).runWithLease(held, 60_000))
    expect(failure).toBeInstanceOf(RebuildFailure)
    expect(failure).toMatchObject({code: 'rebuildFailed', attempted: 2, batches: 2})
    expect((failure as RebuildFailure).error).toBeInstanceOf(TransientError)
    expect((failure as RebuildFailure).cause).toBe((failure as RebuildFailure).error)
    expect((failure as RebuildFailure).lease).toMatchObject({owner: 'caller'})
    expect(isRetryable(failure)).toBe(true)
    expect(isConflict(failure)).toBe(false)
    // The caller's lock is the caller's to end or keep.
    expect((await repository.rebuildState()).lock?.owner).toBe('caller')

    const lost = await errorOf(rebuild(repository).runWithLease(leaseOf('stale', 1n), 60_000))
    expect(lost).toMatchObject({attempted: 0, batches: 0})
    expect((lost as RebuildFailure).error).toBeInstanceOf(RebuildLeaseLostError)
  })
})
