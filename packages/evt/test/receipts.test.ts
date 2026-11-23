// Commit receipts: the store hands back the events as the repository stored them, which a
// repository that rewrites events as it commits (such as a journal's) makes differ from the events
// the commit was built with. The conformance suite in packages/evt-compat checks the default.
import {describe, expect, test} from 'vitest'

import {defineAggregate, defineEvents} from '../src/aggregate.ts'
import {json} from '../src/codec.ts'
import type {SerializedEvent, SerializedSnapshot} from '../src/event.ts'
import {MemRepository} from '../src/memory/repository.ts'
import type {Outcome} from '../src/outcome.ts'
import {
  type CallOptions,
  type Commit,
  type CommitReceipt,
  type OutcomeRepository,
  type TransactionCommit,
  commitTransactionWithReceipt,
  commitWithReceipt,
} from '../src/repository.ts'
import {Store} from '../src/store.ts'
import {FaultRepository} from '../src/testing/fault.ts'

// Stamps a trace entry on every event it commits, as a journal stamps its allocations, so only
// its receipts carry what it stored.
class StampingRepository implements OutcomeRepository {
  readonly inner = new MemRepository()

  commit(commit: Commit, options?: CallOptions): Promise<void> {
    return this.inner.commit(stamp(commit), options)
  }

  async commitWithReceipt(commit: Commit, options?: CallOptions): Promise<CommitReceipt> {
    const stamped = stamp(commit)
    await this.inner.commit(stamped, options)

    return {events: stamped.events}
  }

  commitTransaction(transaction: TransactionCommit, options?: CallOptions): Promise<void> {
    return this.inner.commitTransaction(stampAll(transaction), options)
  }

  commitTransactionWithReceipt(
    transaction: TransactionCommit,
    options?: CallOptions,
  ): Promise<CommitReceipt> {
    return commitTransactionWithReceipt(this.inner, stampAll(transaction), options)
  }

  outcome(commandId: string, options?: CallOptions): Promise<Outcome | null> {
    return this.inner.outcome(commandId, options)
  }

  events(entityId: string, options?: CallOptions): Promise<SerializedEvent[]> {
    return this.inner.events(entityId, options)
  }

  eventsAfter(entityId: string, after: bigint, options?: CallOptions): Promise<SerializedEvent[]> {
    return this.inner.eventsAfter(entityId, after, options)
  }

  latest(entityId: string, n: number, options?: CallOptions): Promise<SerializedEvent[]> {
    return this.inner.latest(entityId, n, options)
  }

  snapshot(entityId: string, options?: CallOptions): Promise<SerializedSnapshot | null> {
    return this.inner.snapshot(entityId, options)
  }

  putSnapshot(snapshot: SerializedSnapshot, options?: CallOptions): Promise<boolean> {
    return this.inner.putSnapshot(snapshot, options)
  }

  compactBelow(entityId: string, through: bigint, options?: CallOptions): Promise<number> {
    return this.inner.compactBelow(entityId, through, options)
  }
}

function stamp(commit: Commit): Commit {
  return {
    ...commit,
    events: commit.events.map((event) => ({
      ...event,
      metadata: {...event.metadata, trace: new Map([['stamp', String(event.sequence)]])},
    })),
  }
}

function stampAll(transaction: TransactionCommit): TransactionCommit {
  return {...transaction, streams: transaction.streams.map(stamp)}
}

const ledger = defineAggregate({
  type: 'ledger',
  events: defineEvents({'ledger:added': {version: 1, codec: json<{amount: number}>()}}),
  state: json<{total: number}>(),
  initial: () => ({total: 0}),
  handle: (_state, amount: number) => ({
    kind: 'accept',
    events: [{type: 'ledger:added', data: {amount}}],
  }),
  apply: (state, event) => ({total: state.total + event.data.amount}),
  identify: (amount: number) => ({
    commandType: 'ledger:add',
    payload: json<number>().encode(amount),
  }),
})

const added = (amount: number) => ({type: 'ledger:added' as const, data: {amount}})

function stamps(events: readonly SerializedEvent[]): (string | undefined)[] {
  return events.map((event) => event.metadata.trace?.get('stamp'))
}

describe('commit receipts', () => {
  test("the helpers return the commit's own events from a repository without receipts", async () => {
    const repo = new MemRepository()
    const store = new Store(repo, {snapshotInterval: 0})
    await store.execute(ledger, 'a', 1)
    const [event] = await repo.events('a')

    const next = {...event!, id: 'a:2', sequence: 2n}
    const receipt = await commitWithReceipt(repo, {entityId: 'a', events: [next]})
    expect(receipt.events).toEqual([next])
    const fenceOnly = await commitTransactionWithReceipt(repo, {
      streams: [{entityId: 'b', events: []}],
      fences: [{entityId: 'a', entityType: 'ledger', head: 2n, provenance: 'caller'}],
    })
    expect(fenceOnly.events).toEqual([])
  })

  test('the store returns the events as stored, not as built', async () => {
    const repo = new StampingRepository()
    const store = new Store(repo, {snapshotInterval: 0})

    const executed = await store.execute(ledger, 'a', 1)
    expect(executed.kind === 'committed' && stamps(executed.events)).toEqual(['1'])
    expect(executed.kind === 'committed' && executed.events).toEqual(await repo.events('a'))

    const tx = store.transaction()
    const b = await tx.read(ledger, 'b')
    const a = await tx.read(ledger, 'a')
    tx.append(b, [added(2)]).append(a, [added(3)])
    const committed = await tx.commit()
    expect(committed.kind === 'committed' && stamps(committed.events)).toEqual(['2', '1'])
    expect(committed.kind === 'committed' && committed.events.map((e) => e.entityId)).toEqual([
      'a',
      'b',
    ])
  })

  test('a command outcome returns the events as stored, through a wrapper too', async () => {
    const repo = new FaultRepository(new StampingRepository())
    const store = new Store(repo, {snapshotInterval: 0, outcomes: {}})

    const single = await store.executeWithOutcome(ledger, 'a', 4, {metadata: {commandId: 'c1'}})
    expect(stamps(single.events)).toEqual(['1'])
    expect(single.outcome.eventIds).toEqual(['a:1'])

    const multi = await store.executeTransaction(
      {commandType: 'ledger:bump', targets: ['a']},
      async (tx) => {
        tx.append(await tx.read(ledger, 'a'), [added(1)])
        return {kind: 'accept'}
      },
      {metadata: {commandId: 'c2'}},
    )
    expect(stamps(multi.events)).toEqual(['2'])
  })
})
