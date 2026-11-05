// Each test's steps depend on the ones before them, so they await in loops on purpose.
/* oxlint-disable no-await-in-loop */
import {describe, expect, test} from 'vitest'

import {type Projection, defineAggregate, defineEvents} from '../src/aggregate.ts'
import {json} from '../src/codec.ts'
import {EvtError, FenceConflictError, ReservedEntityIdError, isRetryable} from '../src/errors.ts'
import {MemRepository} from '../src/memory/repository.ts'
import type {
  ProjectorGroup,
  Repository,
  TransactionCommit,
  TransactionalRepository,
} from '../src/repository.ts'
import {Store} from '../src/store.ts'

const decoder = new TextDecoder()

const account = defineAggregate({
  type: 'account',
  events: defineEvents({
    'account:deposited': {version: 1, codec: json<{amountCents: number}>()},
  }),
  state: json<{balanceCents: number}>(),
  initial: () => ({balanceCents: 0}),
  handle: () => ({kind: 'accept', events: []}),
  apply: (state, event) => ({
    balanceCents: state.balanceCents + event.data.amountCents,
  }),
})

const ledger = defineAggregate({
  type: 'ledger',
  events: defineEvents({
    'ledger:posted': {version: 1, codec: json<{entry: string}>()},
  }),
  state: json<{entries: string[]}>(),
  initial: () => ({entries: []}),
  handle: () => ({kind: 'accept', events: []}),
  apply: (state, event) => ({entries: [...state.entries, event.data.entry]}),
})

const deposit = (amountCents: number) => ({
  type: 'account:deposited' as const,
  data: {amountCents},
})
const post = (entry: string) => ({
  type: 'ledger:posted' as const,
  data: {entry},
})

// 2026-10-31T12:34:56.789Z
const NOW = Date.UTC(2026, 9, 31, 12, 34, 56, 789)

function store(repository: Repository = new MemRepository(), snapshotInterval = 5): Store {
  let next = 0
  return new Store(repository, {
    region: 'us-west-2',
    clock: () => NOW,
    snapshotInterval,
    commitId: () => `commit-${++next}`,
  })
}

// One operation naming the stream it was projected for.
function group(entityId: string): ProjectorGroup {
  return {backend: 'test', operations: [entityId], sizes: [1]}
}

async function failure(call: Promise<unknown>): Promise<unknown> {
  try {
    await call
  } catch (error) {
    return error
  }

  return undefined
}

function thrown(call: () => unknown): unknown {
  try {
    call()
  } catch (error) {
    return error
  }

  return undefined
}

function code(error: unknown): string | undefined {
  return error instanceof EvtError ? error.code : undefined
}

// Seeds a stream with one deposit through the single-stream path.
async function seed(s: Store, entityId: string): Promise<void> {
  const tx = s.transaction()
  tx.append(await tx.read(account, entityId), [deposit(1)])
  await tx.commit()
}

describe('Store.transaction', () => {
  test('commits several streams of different types atomically, with one commit ID', async () => {
    const repository = new MemRepository()
    const s = store(repository)
    const tx = s.transaction()

    const b = await tx.read(account, 'b')
    const a = await tx.read(ledger, 'a')
    tx.append(b, [deposit(5)]).append(a, [post('x')])
    tx.append(b, [deposit(7)])
    expect(b.state).toEqual({balanceCents: 12})
    expect(b.sequence).toBe(2n)
    expect(a.loaded.sequence).toBe(0n)

    const result = await tx.commit({metadata: {principalId: 'p'}})

    expect(result.kind).toBe('committed')
    const committed = result.kind === 'committed' ? result.events : []
    // In the order the commit writes them: by entity ID bytes, then sequence.
    expect(committed.map((event) => `${event.entityId}/${event.sequence}`)).toEqual([
      'a/1',
      'b/1',
      'b/2',
    ])
    for (const event of committed) {
      expect(event.metadata.commitId).toBe('commit-1')
      expect(event.metadata.region).toBe('us-west-2')
      expect(event.metadata.timestamp).toBe('2026-10-31T12:34:56Z')
      expect(event.metadata.principalId).toBe('p')
    }
    expect(await repository.events('b')).toEqual(committed.slice(1))
    expect((await s.load(ledger, 'a')).state).toEqual({entries: ['x']})
  })

  test('a command ID becomes the commit ID, and one already folded in is a duplicate', async () => {
    const repository = new MemRepository()
    const s = store(repository)

    const first = s.transaction()
    first.append(await first.read(account, 'a'), [deposit(1)])
    const result = await first.commit({metadata: {commandId: 'cmd-1'}})
    expect(result.kind === 'committed' && result.events[0]?.metadata.commitId).toBe('cmd-1')

    const again = s.transaction()
    again.append(await again.read(account, 'a'), [deposit(1)])
    await again.read(ledger, 'other')
    expect(await again.commit({metadata: {commandId: 'cmd-1'}})).toEqual({
      kind: 'duplicate',
      commandId: 'cmd-1',
    })
    expect(await repository.events('a')).toHaveLength(1)
  })

  test('fences a stream read but not written, at the head it was read at', async () => {
    const repository = new MemRepository()
    const s = store(repository)
    await seed(s, 'watched')

    const tx = s.transaction()
    await tx.read(account, 'watched')
    tx.append(await tx.read(account, 'writer'), [deposit(1)])

    // Another writer moves the watched stream after the read.
    const mover = store(repository).transaction()
    mover.append(await mover.read(account, 'watched'), [deposit(1)])
    await mover.commit()

    const error = await failure(tx.commit())
    expect(error).toBeInstanceOf(FenceConflictError)
    expect(error instanceof FenceConflictError && error.provenance).toBe('engine')
    expect(error instanceof FenceConflictError && error.head).toBe(1n)
    expect(isRetryable(error)).toBe(true)
    expect(await repository.events('writer')).toEqual([])
  })

  test('a fence that holds lets the commit through, and a read-only commit checks it', async () => {
    const repository = new MemRepository()
    const s = store(repository)
    await seed(s, 'watched')

    const tx = s.transaction()
    await tx.read(account, 'watched')
    await tx.read(account, 'absent')
    expect(await tx.commit()).toEqual({kind: 'committed', events: []})

    const stale = s.transaction()
    await stale.read(account, 'watched')
    const mover = s.transaction()
    mover.append(await mover.read(account, 'watched'), [deposit(1)])
    await mover.commit()
    expect(await failure(stale.commit())).toBeInstanceOf(FenceConflictError)
  })

  test("a caller's fence fails the commit once the stream moves, and isn't retryable", async () => {
    const repository = new MemRepository()
    const s = store(repository)
    await seed(s, 'watched')

    const held = s.transaction()
    held.append(await held.read(account, 'writer'), [deposit(1)])
    held.fence({entityId: 'watched', entityType: 'account', head: 1n})
    held.fence({entityId: 'absent', entityType: 'account', head: 0n})
    expect((await held.commit()).kind).toBe('committed')

    const stale = s.transaction()
    stale.append(await stale.read(account, 'writer'), [deposit(1)])
    stale.fence({entityId: 'watched', entityType: 'account', head: 0n})
    const error = await failure(stale.commit())
    expect(error).toBeInstanceOf(FenceConflictError)
    expect(error instanceof FenceConflictError && error.provenance).toBe('caller')
    expect(isRetryable(error)).toBe(false)
    expect(await repository.events('writer')).toHaveLength(1)
  })

  test("a caller's fence on a stream the transaction reads gives way to the read's", async () => {
    const s = store()
    await seed(s, 'before')
    await seed(s, 'after')

    // Stale fences, before and after the reads, which the reads' own fences replace.
    const tx = s.transaction()
    tx.fence({entityId: 'before', entityType: 'account', head: 7n})
    await tx.read(account, 'before')
    tx.append(await tx.read(account, 'after'), [deposit(1)])
    tx.fence({entityId: 'after', entityType: 'account', head: 7n})

    expect((await tx.commit()).kind).toBe('committed')
  })

  test('refuses a bad fence with nothing added', async () => {
    const s = store()
    const tx = s.transaction()
    tx.fence({entityId: 'a', entityType: 'account', head: 0n})

    const reserved = thrown(() => tx.fence({entityId: 'SYSTEM#x', entityType: 'account', head: 0n}))
    expect(reserved).toBeInstanceOf(ReservedEntityIdError)
    for (const head of [-1n, 2n ** 63n - 1n]) {
      const error = thrown(() => tx.fence({entityId: 'b', entityType: 'account', head}))
      expect(code(error)).toBe('invalidCommit')
    }
    const duplicate = thrown(() => tx.fence({entityId: 'a', entityType: 'account', head: 0n}))
    expect(code(duplicate)).toBe('invalidCommit')

    expect((await tx.commit()).kind).toBe('committed')
  })

  test('refuses a second read, a foreign handle and a bad event, changing nothing', async () => {
    const repository = new MemRepository()
    const s = store(repository)
    const tx = s.transaction()
    const a = await tx.read(account, 'a')

    expect(code(await failure(tx.read(account, 'a')))).toBe('invalidCommit')
    // A concurrent read of one stream is refused too.
    const reads = await Promise.allSettled([tx.read(ledger, 'c'), tx.read(ledger, 'c')])
    expect(reads.filter((read) => read.status === 'fulfilled')).toHaveLength(1)

    const other = s.transaction()
    const foreign = await other.read(account, 'b')
    expect(code(thrown(() => tx.append(foreign, [deposit(1)])))).toBe('invalidCommit')

    // An event the aggregate can't encode stages nothing.
    const bad = {
      type: 'account:withdrawn',
      data: {amountCents: 1},
    } as unknown as ReturnType<typeof deposit>
    expect(code(thrown(() => tx.append(a, [deposit(1), bad])))).toBe('invalidArgument')
    expect(a.sequence).toBe(0n)
    expect(a.state).toEqual({balanceCents: 0})

    tx.append(a, [deposit(2)])
    await tx.commit()
    expect((await s.load(account, 'a')).state).toEqual({balanceCents: 2})
    expect(await repository.events('b')).toEqual([])
  })

  test('commits once, whether the commit succeeds or fails', async () => {
    const s = store()
    const done = s.transaction()
    const a = await done.read(account, 'a')
    await done.commit()

    const failed = s.transaction()
    failed.fence({entityId: 'x', entityType: 'account', head: 3n})
    expect(await failure(failed.commit())).toBeInstanceOf(FenceConflictError)

    for (const tx of [done, failed]) {
      expect(code(await failure(tx.read(account, 'b')))).toBe('invalidCommit')
      expect(code(await failure(tx.commit()))).toBe('invalidCommit')
      expect(code(thrown(() => tx.fence({entityId: 'y', entityType: 'a', head: 0n})))).toBe(
        'invalidCommit',
      )
    }
    expect(code(thrown(() => done.append(a, [deposit(1)])))).toBe('invalidCommit')
  })

  test('snapshots each written stream by its own interval, covering its last event', async () => {
    const repository = new MemRepository()
    const s = store(repository, 2)
    const frequent = defineAggregate({...ledger, snapshotInterval: 1})

    const tx = s.transaction()
    const a = await tx.read(account, 'a')
    const l = await tx.read(frequent, 'l')
    tx.append(a, [deposit(1)]).append(l, [post('x')])
    tx.append(a, [deposit(2), deposit(3)])
    await tx.commit()

    const snapshot = await repository.snapshot('a')
    expect(snapshot?.eventSequence).toBe(3n)
    expect(snapshot?.sequence).toBe(1n)
    expect(decoder.decode(snapshot?.payload ?? new Uint8Array())).toBe('{"balanceCents":6}')
    expect((await repository.snapshot('l'))?.eventSequence).toBe(1n)

    // The next boundary is the fourth event: the stream's next snapshot covers it.
    const next = s.transaction()
    const again = await next.read(account, 'a')
    expect(again.loaded.snapshotSequence).toBe(1n)
    next.append(again, [deposit(4)])
    await next.commit()
    expect((await repository.snapshot('a'))?.eventSequence).toBe(4n)
    expect((await repository.snapshot('a'))?.sequence).toBe(2n)
  })

  test("runs each stream's projectors once, over every event appended to it", async () => {
    const seen: Projection<{balanceCents: number}>[] = []
    const projected = defineAggregate({
      ...account,
      projectors: [
        {
          project(projection) {
            seen.push(projection)
            return null
          },
        },
      ],
    })

    const s = store()
    const tx = s.transaction()
    const a = await tx.read(projected, 'a')
    await tx.read(projected, 'read-only')
    tx.append(a, [deposit(1)]).append(a, [deposit(2)])
    expect(seen).toHaveLength(0)
    await tx.commit({metadata: {commandId: 'cmd'}})

    expect(seen).toHaveLength(1)
    expect(seen[0]?.entityId).toBe('a')
    expect(seen[0]?.state).toEqual({balanceCents: 3})
    expect(seen[0]?.events.map((event) => [event.sequence, event.metadata.commitId])).toEqual([
      [1n, 'cmd'],
      [2n, 'cmd'],
    ])
  })

  test('refuses a commit while a read is loading, and stays open', async () => {
    const repository = new MemRepository()
    const s = store(repository)
    await seed(s, 'watched')
    await seed(s, 'watched-again')

    // A stale fence the read will supersede, once it has loaded.
    const tx = s.transaction()
    tx.fence({entityId: 'watched', entityType: 'account', head: 0n})
    const loading = tx.read(account, 'watched')
    expect(code(await failure(tx.commit()))).toBe('invalidCommit')

    tx.append(await loading, [deposit(1)])
    expect((await tx.commit()).kind).toBe('committed')
    expect(await repository.events('watched')).toHaveLength(2)
  })

  test('a command ID on a stream only read is a duplicate too', async () => {
    const repository = new MemRepository()
    const s = store(repository)
    const first = s.transaction()
    first.append(await first.read(account, 'read-only'), [deposit(1)])
    await first.commit({metadata: {commandId: 'cmd-1'}})

    const tx = s.transaction()
    await tx.read(account, 'read-only')
    tx.append(await tx.read(account, 'written'), [deposit(1)])
    expect((await tx.commit({metadata: {commandId: 'cmd-1'}})).kind).toBe('duplicate')
    expect(await repository.events('written')).toEqual([])
  })

  test('commits the projector operations of each stream in the order the reads were called', async () => {
    const projected = defineAggregate({
      ...account,
      projectors: [{project: (projection) => group(projection.entityId)}],
    })

    // The first read's load finishes last.
    const memory = new MemRepository()
    const committed: TransactionCommit[] = []
    const repository: TransactionalRepository = {
      commit: (commit) => memory.commit(commit),
      events: (id) => memory.events(id),
      eventsAfter: (id, after) => memory.eventsAfter(id, after),
      latest: (id, n) => memory.latest(id, n),
      async snapshot(id) {
        if (id === 'z-first') {
          await new Promise((resolve) => setTimeout(resolve, 20))
        }
        return memory.snapshot(id)
      },
      putSnapshot: (snapshot) => memory.putSnapshot(snapshot),
      compactBelow: (id, through) => memory.compactBelow(id, through),
      commitTransaction(transaction) {
        committed.push(transaction)
        return memory.commitTransaction(transaction)
      },
    }

    const tx = store(repository).transaction()
    const [first, second] = await Promise.all([
      tx.read(projected, 'z-first'),
      tx.read(projected, 'a-second'),
    ])
    tx.append(second, [deposit(1)]).append(first, [deposit(2)])
    await tx.commit()

    expect(committed[0]?.streams.map((stream) => stream.projections)).toEqual([
      [group('z-first')],
      [group('a-second')],
    ])
  })

  test("types each handle by its aggregate's events", async () => {
    const tx = store().transaction()
    const a = await tx.read(account, 'a')
    const l = await tx.read(ledger, 'l')

    // @ts-expect-error a ledger event on an account stream
    expect(() => tx.append(a, [post('x')])).toThrow(EvtError)
    tx.append(l, [post('x')])
  })

  test('needs a transactional repository', () => {
    const repository = new MemRepository()
    const plain: Repository = {
      commit: (commit, options) => repository.commit(commit, options),
      events: (id, options) => repository.events(id, options),
      eventsAfter: (id, after, options) => repository.eventsAfter(id, after, options),
      latest: (id, n, options) => repository.latest(id, n, options),
      snapshot: (id, options) => repository.snapshot(id, options),
      putSnapshot: (snapshot, options) => repository.putSnapshot(snapshot, options),
      compactBelow: (id, through, options) => repository.compactBelow(id, through, options),
    }

    expect(code(thrown(() => store(plain).transaction()))).toBe('invalidArgument')
  })
})
