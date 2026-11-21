// Commit-wide projectors (ADR 0004, C7): what a projector sees, the checks on its operations,
// regrouping stored events into commits, the in-memory and DynamoDB repositories' view writes, and
// the store running projectors on the commits that write events. The DynamoDB actions' bytes are
// pinned by the commitViews vectors and the commit-wide scenarios in packages/evt-compat.

// Each test's steps depend on the ones before them, so they await in loops on purpose.
/* oxlint-disable no-await-in-loop */
import {describe, expect, test} from 'vitest'

import {defineAggregate, defineEvents} from '../src/aggregate.ts'
import {json} from '../src/codec.ts'
import type {DynamoClient, Page, TransactItem} from '../src/dynamodb/client.ts'
import type {Item} from '../src/dynamodb/item.ts'
import {markerItem} from '../src/dynamodb/marker.ts'
import {DynamoRepository} from '../src/dynamodb/repository.ts'
import {
  CommitOrderError,
  CommitTooLargeError,
  ConflictError,
  DuplicateViewKeyError,
  EvtError,
  ProjectorError,
  ProjectorOnlyCommitError,
  ProjectorOperationError,
  ReservedViewKeyError,
  ViewKeyTooLongError,
  ViewsNotConfiguredError,
} from '../src/errors.ts'
import {type SerializedEvent, eventId} from '../src/event.ts'
import {MemRepository, controlFixtures} from '../src/memory/repository.ts'
import {PgRepository} from '../src/postgres/repository.ts'
import {emptyMetadata} from '../src/metadata.ts'
import {
  type CommitProjector,
  type CommitView,
  type ProjectedView,
  type ViewOperation,
  checkViews,
  commitStream,
  deleteView,
  groupCommits,
  projectViews,
  putView,
  viewOperationSize,
} from '../src/projection.ts'
import type {Repository} from '../src/repository.ts'
import {Store} from '../src/store.ts'
import {FaultRepository} from '../src/testing/fault.ts'

const encoder = new TextEncoder()
const decoder = new TextDecoder()

function event(entityId: string, sequence: number, commitId = 'commit-1'): SerializedEvent {
  return {
    id: eventId(entityId, BigInt(sequence)),
    sequence: BigInt(sequence),
    type: 'ledger:added',
    version: 1,
    entityId,
    entityType: 'ledger',
    payload: encoder.encode('{"amount":1}'),
    metadata: {...emptyMetadata(), commandId: 'cmd', commitId},
  }
}

function projected(op: ViewOperation, operation = 0, projector = 'p'): ProjectedView {
  return {projector, projectorIndex: 0, operation, op}
}

async function failure(call: Promise<unknown> | (() => unknown)): Promise<unknown> {
  try {
    await (typeof call === 'function' ? call() : call)
  } catch (error) {
    return error
  }

  return undefined
}

function commitOf(...streams: [string, string, number][]): CommitView {
  return {
    commitId: 'commit-1',
    streams: streams.map(([entityId, entityType, events]) =>
      commitStream(
        entityId,
        entityType,
        Array.from({length: events}, (_, i) => ({...event(entityId, i + 1), entityType})),
        encoder.encode(`{"of":"${entityId}"}`),
      ),
    ),
  }
}

describe('projectViews', () => {
  test("a current-state projector sees each stream's state, and no commit ID or events", () => {
    const seen: CommitView[] = []
    const projector: CommitProjector = {
      name: 'directory',
      project(view) {
        seen.push(view)
        return [putView('directory', 'a', 'a', 'ledger', '{}')]
      },
    }

    const views = projectViews([projector], commitOf(['a', 'ledger', 2]))
    expect(views).toEqual([
      {
        projector: 'directory',
        projectorIndex: 0,
        operation: 0,
        op: putView('directory', 'a', 'a', 'ledger', '{}'),
      },
    ])
    expect(seen[0]!.commitId).toBe('')
    expect(seen[0]!.streams[0]!.events).toEqual([])
    expect(decoder.decode(seen[0]!.streams[0]!.state)).toBe('{"of":"a"}')
  })

  test('a history-dependent projector sees only the streams of the types it names', () => {
    let seen: CommitView | null = null
    const projector: CommitProjector = {
      name: 'audit',
      history: ['ledger'],
      project(view) {
        seen = view
        return []
      },
    }

    projectViews([projector], commitOf(['a', 'ledger', 1], ['b', 'claim', 1]))
    const [a, b] = (seen as unknown as CommitView).streams
    expect((seen as unknown as CommitView).commitId).toBe('commit-1')
    expect(a!.events).toHaveLength(1)
    expect(b!.events).toEqual([])
    expect(b!.state).toEqual(new Uint8Array())
  })

  test("a stored event's causation defaults to its command ID", () => {
    const stream = commitStream('a', 'ledger', [event('a', 1)], new Uint8Array())
    expect(stream.events[0]!.metadata.causationId).toBe('cmd')
  })

  test('runs no projector for a commit without events', () => {
    const projector: CommitProjector = {
      name: 'never',
      project: () => {
        throw new Error('ran')
      },
    }
    expect(projectViews([projector], commitOf(['a', 'ledger', 0]))).toEqual([])
  })

  test('refuses an owner the commit did not write, and one of an undeclared type', () => {
    const outsider: CommitProjector = {
      name: 'p',
      project: () => [putView('k', 'a', 'other', 'ledger', '{}')],
    }
    expect(() => projectViews([outsider], commitOf(['a', 'ledger', 1]))).toThrow(
      /invalid view operation: projector "p" \(#0\) operation 0: the owner stream "other"/,
    )

    const undeclared: CommitProjector = {
      name: 'p',
      history: ['claim'],
      project: () => [putView('k', 'a', 'a', 'ledger', '{}')],
    }
    expect(() => projectViews([undeclared], commitOf(['a', 'ledger', 1]))).toThrow(
      /does not declare in its history/,
    )
  })

  test("refuses the engine's partitions, and wraps a projector's own failure", async () => {
    const journal: CommitProjector = {
      name: 'p',
      project: () => [putView('journal#x', 'a', 'a', 'ledger', '{}')],
    }
    expect(
      await failure(() => projectViews([journal], commitOf(['a', 'ledger', 1]))),
    ).toBeInstanceOf(ReservedViewKeyError)

    const broken: CommitProjector = {
      name: 'broken',
      project: () => {
        throw new Error('boom')
      },
    }
    const error = await failure(() => projectViews([broken], commitOf(['a', 'ledger', 1])))
    expect(error).toBeInstanceOf(ProjectorError)
    expect((error as ProjectorError).message).toBe(
      'projector failed: commit projector "broken": boom',
    )
  })
})

describe('checkViews', () => {
  test.each([
    [putView('k', 'a', 'o', 't', '{}', {condition: 'present' as never}), 'a put cannot carry'],
    [deleteView('k', 'a', 'o', {condition: 'absent' as never}), 'a delete cannot carry'],
    [putView('', 'a', 'o', 't', '{}'), 'pk is empty'],
    [putView('k', '', 'o', 't', '{}'), 'sk is empty'],
    [putView('k', 'a', '', 't', '{}'), 'owner is empty'],
    [putView('k\0', 'a', 'o', 't', '{}'), 'pk contains U+0000'],
    [putView('k', 'a', 'o', 't', new Uint8Array([0xff])), 'payload is not valid UTF-8'],
  ])('refuses an invalid operation: %#', (op, reason) => {
    const error = (() => {
      try {
        checkViews([projected(op)])
      } catch (thrown) {
        return thrown
      }
      return undefined
    })()
    expect(error).toMatchObject({code: 'invalidViewOperation'})
    expect((error as EvtError).message).toContain(reason)
  })

  test('refuses an overlong key, and a key two operations name', () => {
    expect(() => checkViews([projected(putView('k'.repeat(2049), 'a', 'o', 't', '{}'))])).toThrow(
      ViewKeyTooLongError,
    )
    expect(() => checkViews([projected(putView('k', 's'.repeat(1025), 'o', 't', '{}'))])).toThrow(
      ViewKeyTooLongError,
    )
    checkViews([projected(putView('k'.repeat(2048), 's'.repeat(1024), 'o', 't', '{}'))])

    expect(() =>
      checkViews([
        projected(putView('k', 'a', 'o', 't', '{}')),
        projected(deleteView('k', 'a', 'o'), 1),
      ]),
    ).toThrow(DuplicateViewKeyError)
  })

  test('sizes an operation as DynamoDB counts its action', () => {
    expect(
      viewOperationSize(putView('ledger', 'acct-1', 'acct-1', 'account', '{"events":2}')),
    ).toBe(2 + 6 + 2 + 6 + 8 + 6 + 10 + 7 + 7 + 12)
    expect(viewOperationSize(deleteView('k', 'a', 'o', {condition: 'present'}))).toBe(
      2 + 1 + 2 + 1 + 42 + 6 + 1,
    )
    expect(viewOperationSize(deleteView('k', 'a', 'o'))).toBe(2 + 1 + 2 + 1 + 45 + 6 + 1)
  })
})

describe('groupCommits', () => {
  test('regroups events by commit ID, in an order every stream agrees with', () => {
    const views = groupCommits([
      event('b', 1, 'c2'),
      event('a', 2, 'c2'),
      event('a', 1, 'c1'),
      event('a', 3, ''),
      event('b', 2, 'c3'),
    ])
    expect(
      views.map((view) => [
        view.commitId,
        view.streams.map((stream) => `${stream.entityId}:${stream.events.map((e) => e.sequence)}`),
      ]),
    ).toEqual([
      ['c1', ['a:1']],
      ['c2', ['a:2', 'b:1']],
      ['', ['a:3']],
      ['c3', ['b:2']],
    ])
  })

  test('refuses commits no order satisfies', () => {
    const error = (() => {
      try {
        groupCommits([
          event('a', 1, 'x'),
          event('a', 2, 'y'),
          event('b', 1, 'y'),
          event('b', 2, 'x'),
        ])
      } catch (thrown) {
        return thrown
      }
      return undefined
    })()
    expect(error).toBeInstanceOf(CommitOrderError)
    expect((error as CommitOrderError).commits).toEqual(['x', 'y'])
  })
})

// Commits event `sequence` of stream `a` with the given view operations.
function write(repository: MemRepository, sequence: number, ...ops: ViewOperation[]) {
  return repository.commit({
    entityId: 'a',
    events: [event('a', sequence)],
    views: ops.map((op, i) => projected(op, i)),
  })
}

describe('MemRepository views', () => {
  test('writes and deletes rows, each conditioned on its owner and its condition', async () => {
    const repository = new MemRepository()
    await write(
      repository,
      1,
      putView('handles', 'ada', 'a', 'ledger', '{"n":1}', {condition: 'absent'}),
    )
    expect(repository.view('handles', 'ada')).toMatchObject({entityId: 'a', entityType: 'ledger'})

    const taken = await failure(
      write(
        repository,
        2,
        putView('handles', 'ada', 'a', 'ledger', '{}', {
          condition: 'absent',
          conflict: new Error('taken'),
        }),
      ),
    )
    expect(taken).toBeInstanceOf(ProjectorOperationError)
    expect(taken).toMatchObject({reason: 'ConditionalCheckFailed', mapped: new Error('taken')})
    expect(await repository.events('a')).toHaveLength(1)

    // Another stream can't overwrite or delete the row.
    const other = await failure(
      repository.commit({
        entityId: 'b',
        events: [event('b', 1)],
        views: [projected(putView('handles', 'ada', 'b', 'ledger', '{}'))],
      }),
    )
    expect(other).toBeInstanceOf(ProjectorOperationError)
    expect(await repository.events('b')).toEqual([])

    await write(repository, 2, deleteView('handles', 'ada', 'a', {condition: 'present'}))
    expect(repository.view('handles', 'ada')).toBeNull()
    expect(
      await failure(
        write(repository, 3, deleteView('handles', 'ada', 'a', {condition: 'present'})),
      ),
    ).toBeInstanceOf(ProjectorOperationError)
    await write(repository, 3, deleteView('handles', 'ada', 'a'))
    expect(repository.views()).toEqual([])
  })

  test('refuses views without events, and budgets them', async () => {
    const repository = new MemRepository()
    expect(
      await failure(
        repository.commit({
          entityId: 'a',
          events: [],
          views: [projected(putView('k', 'a', 'a', 't', '{}'))],
        }),
      ),
    ).toBeInstanceOf(ProjectorOnlyCommitError)

    const many = Array.from({length: 98}, (_, i) =>
      projected(putView('k', `s${i}`, 'a', 't', '{}'), i),
    )
    expect(
      await failure(repository.commit({entityId: 'a', events: [event('a', 1)], views: many})),
    ).toBeInstanceOf(CommitTooLargeError)
  })
})

function markerRow(): Item {
  return markerItem({
    version: 1n,
    features: new Map([['stream-heads', 1]]),
    historyDependent: [],
    retention: [],
  })
}

function cancelled(...codes: string[]): Error {
  return Object.assign(new Error('Transaction cancelled'), {
    name: 'TransactionCanceledException',
    CancellationReasons: codes.map((Code) => ({Code})),
  })
}

// A client with a marker row, and transactions that answer in turn.
class Scripted implements DynamoClient {
  readonly transactions: TransactItem[][] = []
  readonly #answers: (Error | null)[]

  constructor(...answers: (Error | null)[]) {
    this.#answers = answers
  }

  async getItem(input: {Key: Item}): Promise<{Item?: Item}> {
    const pk = input.Key['pk']
    return pk && 'S' in pk && pk.S === 'SYSTEM#FEATURES' ? {Item: markerRow()} : {}
  }

  async putItem(): Promise<unknown> {
    throw new Error('unexpected putItem')
  }

  async query(): Promise<Page> {
    return {Items: []}
  }

  async scan(): Promise<Page> {
    return {Items: []}
  }

  async transactWriteItems(input: {TransactItems: TransactItem[]}): Promise<unknown> {
    this.transactions.push(input.TransactItems)
    const answer = this.#answers.shift()
    if (answer) {
      throw answer
    }

    return {}
  }
}

describe('DynamoRepository views', () => {
  const views = [
    projected(
      putView('handles', 'ada', 'a', 'ledger', '{}', {condition: 'absent', conflict: 'taken'}),
    ),
    projected(deleteView('audit', 'x', 'a'), 1),
  ]

  test('writes the views to the views table after the control checks', async () => {
    const client = new Scripted()
    await new DynamoRepository(client, 'events', {viewsTable: 'views'}).commit({
      entityId: 'a',
      events: [event('a', 1)],
      views,
    })

    const items = client.transactions[0]!
    expect(items[2]).toEqual({
      Put: {
        TableName: 'views',
        Item: {
          pk: {S: 'handles'},
          sk: {S: 'ada'},
          entityID: {S: 'a'},
          entityType: {S: 'ledger'},
          payload: {S: '{}'},
        },
        ConditionExpression: 'attribute_not_exists(pk)',
      },
    })
    expect(items[3]).toEqual({
      Delete: {
        TableName: 'views',
        Key: {pk: {S: 'audit'}, sk: {S: 'x'}},
        ConditionExpression: 'attribute_not_exists(pk) OR entityID = :owner',
        ExpressionAttributeValues: {':owner': {S: 'a'}},
      },
    })
  })

  test("keeps a payload's leading byte order mark", async () => {
    const client = new Scripted()
    const payload = new Uint8Array([0xef, 0xbb, 0xbf, 0x7b, 0x7d])
    await new DynamoRepository(client, 'events', {viewsTable: 'views'}).commit({
      entityId: 'a',
      events: [event('a', 1)],
      views: [projected(putView('k', 'a', 'a', 'ledger', payload))],
    })

    const put = client.transactions[0]![2]!.Put!
    expect(put.Item['payload']).toEqual({S: '\uFEFF{}'})
  })

  test('refuses views without a views table, before any request', async () => {
    const client = new Scripted()
    const error = await failure(
      new DynamoRepository(client, 'events').commit({
        entityId: 'a',
        events: [event('a', 1)],
        views,
      }),
    )
    expect(error).toBeInstanceOf(ViewsNotConfiguredError)
    expect(client.transactions).toEqual([])
  })

  test("maps a view's failed condition or refusal to its projector operation", async () => {
    const condition = new Scripted(
      cancelled('None', 'None', 'ConditionalCheckFailed', 'None', 'None', 'None'),
    )
    const failed = await failure(
      new DynamoRepository(condition, 'events', {viewsTable: 'views'}).commit({
        entityId: 'a',
        events: [event('a', 1)],
        views,
      }),
    )
    expect(failed).toMatchObject({code: 'projectorOperation', operation: 0, mapped: 'taken'})

    const refused = new Scripted(
      cancelled('None', 'None', 'None', 'ValidationError', 'None', 'None'),
    )
    const validation = await failure(
      new DynamoRepository(refused, 'events', {viewsTable: 'views'}).commit({
        entityId: 'a',
        events: [event('a', 1)],
        views,
      }),
    )
    expect(validation).toMatchObject({reason: 'ValidationError', operation: 1, mapped: undefined})
    expect((validation as Error).cause).toBeInstanceOf(Error)
  })
})

// A view that claims key `k/one` for `owner`.
function claim(owner: string): ProjectedView {
  return projected(putView('k', 'one', owner, 'ledger', '{}', {condition: 'absent'}))
}

describe('view failures and the other repositories', () => {
  test('a view failure ranks below a recorded outcome and a stream conflict', async () => {
    const repository = new MemRepository()
    await repository.commit({entityId: 'a', events: [event('a', 1)], views: [claim('a')]})

    // A stale stream and a failed view: the conflict is reported.
    const stale = await failure(
      repository.commit({entityId: 'a', events: [event('a', 1)], views: [claim('a')]}),
    )
    expect(stale).toMatchObject({code: 'conflict'})

    // A recorded outcome and a failed view: the outcome conflict is reported.
    await repository.commitTransaction({
      streams: [{entityId: 'b', events: [event('b', 1)]}],
      outcome: {
        commandId: 'c1',
        status: 'accepted',
        digest: 'd',
        response: null,
        eventIds: [],
        rejection: null,
        identity: null,
      },
    })
    const recorded = await failure(
      repository.commitTransaction({
        streams: [{entityId: 'b', events: [event('b', 2)]}],
        views: [claim('b')],
        outcome: {
          commandId: 'c1',
          status: 'accepted',
          digest: 'd',
          response: null,
          eventIds: [],
          rejection: null,
          identity: null,
        },
      }),
    )
    expect(recorded).toMatchObject({code: 'outcomeConflict'})
  })

  test("DynamoDB reports a lost outcome put over a view's failed condition", async () => {
    // The control checks, the view, the head update and the event put, then the outcome.
    const client = new Scripted(
      cancelled('None', 'None', 'ConditionalCheckFailed', 'None', 'None', 'ConditionalCheckFailed'),
    )
    const error = await failure(
      new DynamoRepository(client, 'events', {viewsTable: 'views'}).commit({
        entityId: 'a',
        events: [event('a', 1)],
        views: [projected(putView('k', 'one', 'a', 'ledger', '{}'))],
        outcome: {
          commandId: 'c1',
          status: 'accepted',
          digest: 'd',
          response: null,
          eventIds: [],
          rejection: null,
          identity: null,
        },
      }),
    )
    expect(error).toMatchObject({code: 'outcomeConflict'})
  })

  test('PostgreSQL refuses view operations before any request', async () => {
    const queries: string[] = []
    const client = {
      query: async (text: string) => {
        queries.push(text)
        throw new Error('unexpected query')
      },
    }
    const error = await failure(
      new PgRepository(client as never).commit({
        entityId: 'a',
        events: [event('a', 1)],
        views: [projected(putView('k', 'one', 'a', 'ledger', '{}'))],
      }),
    )
    expect(error).toBeInstanceOf(ViewsNotConfiguredError)
    expect(queries).toEqual([])
  })
})

describe('Store commit projectors', () => {
  const events = defineEvents({'ledger:added': {version: 1, codec: json<{amount: number}>()}})
  const ledger = defineAggregate({
    type: 'ledger',
    events,
    state: json<{total: number}>(),
    initial: () => ({total: 0}),
    handle: (_state, amount: number) =>
      amount < 0
        ? {kind: 'reject', rejection: {code: 'negative', message: 'no'}}
        : {kind: 'accept', events: amount === 0 ? [] : [{type: 'ledger:added', data: {amount}}]},
    apply: (state, added) => ({total: state.total + added.data.amount}),
  })
  const totals: CommitProjector = {
    name: 'totals',
    project: (commit) =>
      commit.streams.map((stream) =>
        putView('totals', stream.entityId, stream.entityId, stream.entityType, stream.state),
      ),
  }

  test('commits the views of every commit that writes events, and only those', async () => {
    const repository = new MemRepository()
    const store = new Store(repository, {snapshotInterval: 0, commitProjectors: [totals]})
    await store.execute(ledger, 'a', 2)
    await store.execute(ledger, 'a', 3)
    expect(decoder.decode(repository.view('totals', 'a')!.payload)).toBe('{"total":5}')

    // A rejection and an accepted no-op write nothing, views included.
    await store.execute(ledger, 'a', -1)
    await store.execute(ledger, 'a', 0)
    expect(decoder.decode(repository.view('totals', 'a')!.payload)).toBe('{"total":5}')

    const transaction = store.transaction()
    transaction.append(await transaction.read(ledger, 'b'), [
      {type: 'ledger:added', data: {amount: 1}},
    ])
    transaction.append(await transaction.read(ledger, 'c'), [
      {type: 'ledger:added', data: {amount: 4}},
    ])
    await transaction.commit()
    expect(repository.views().map((row) => `${row.sk}=${decoder.decode(row.payload)}`)).toEqual([
      'a={"total":5}',
      'b={"total":1}',
      'c={"total":4}',
    ])
  })

  test('refuses a repository that writes no views, or declares no history for a projector', async () => {
    const plain = new MemRepository()
    const repository: Repository = {
      commit: (commit, options) => plain.commit(commit, options),
      events: (id, options) => plain.events(id, options),
      eventsAfter: (id, after, options) => plain.eventsAfter(id, after, options),
      latest: (id, n, options) => plain.latest(id, n, options),
      snapshot: (id, options) => plain.snapshot(id, options),
      putSnapshot: (snapshot, options) => plain.putSnapshot(snapshot, options),
      compactBelow: (id, through, options) => plain.compactBelow(id, through, options),
    }
    const unsupported = await failure(
      new Store(repository, {commitProjectors: [totals]}).execute(ledger, 'a', 1),
    )
    expect(unsupported).toMatchObject({code: 'viewsUnsupported'})
    expect(await plain.events('a')).toEqual([])

    const historical = await failure(
      new Store(Object.assign(repository, {writesViews: () => true}), {
        commitProjectors: [{...totals, history: ['ledger']}],
      }).execute(ledger, 'b', 1),
    )
    expect(historical).toMatchObject({code: 'historyDeclarationUnsupported'})
    expect(await plain.events('b')).toEqual([])
  })

  test('declares a history-dependent projector type once, before its first commit', async () => {
    const repository = new MemRepository()
    const declared: string[][] = []
    const declare = repository.declareHistoryDependent.bind(repository)
    repository.declareHistoryDependent = async (types, options) => {
      declared.push([...types])
      await declare(types, options)
    }
    const seen: string[] = []
    const audit: CommitProjector = {
      name: 'audit',
      history: ['ledger', 'other', 'ledger'],
      project: (commit) => {
        seen.push(`${commit.commitId}:${commit.streams.flatMap((s) => s.events).length}`)
        return commit.streams.map((stream) =>
          putView('audit', stream.entityId, stream.entityId, stream.entityType, '{}'),
        )
      },
    }
    const store = new Store(repository, {snapshotInterval: 1, commitProjectors: [audit]})
    await store.execute(ledger, 'a', 2, {metadata: {commandId: 'c1'}})
    const transaction = store.transaction()
    transaction.append(await transaction.read(ledger, 'b'), [
      {type: 'ledger:added', data: {amount: 1}},
    ])
    await transaction.commit({metadata: {commandId: 'c2'}})

    // Declared once for the store, before the first commit, which ran the projector with history.
    expect(declared).toEqual([['ledger', 'other']])
    expect(seen).toEqual(['c1:1', 'c2:1'])
    expect(repository.views().map((row) => row.sk)).toEqual(['a', 'b'])

    // A declared type is never compacted, in any process.
    const compaction = await failure(repository.compactBelow('a', 1n))
    expect(compaction).toMatchObject({code: 'historyDependentProjection'})

    // Another store declares again, and nothing changes: the types are already there.
    await new Store(repository, {commitProjectors: [audit]}).execute(ledger, 'a', 1)
    expect(declared).toHaveLength(2)
  })

  test('a failed declaration marks nothing, and the next commit declares again', async () => {
    const repository = new MemRepository()
    const declare = repository.declareHistoryDependent.bind(repository)
    let calls = 0
    repository.declareHistoryDependent = async (types, options) => {
      calls++
      if (calls === 1) {
        throw new Error('marker unavailable')
      }
      await declare(types, options)
    }
    const store = new Store(repository, {commitProjectors: [{...totals, history: ['ledger']}]})
    expect(await failure(store.execute(ledger, 'a', 1))).toMatchObject({
      message: 'marker unavailable',
    })
    expect(await repository.events('a')).toEqual([])

    await store.execute(ledger, 'a', 1)
    expect(calls).toBe(2)
    expect((await repository.events('a')).length).toBe(1)
  })

  test('concurrent first commits share one declaration', async () => {
    const repository = new MemRepository()
    const declare = repository.declareHistoryDependent.bind(repository)
    let calls = 0
    repository.declareHistoryDependent = async (types, options) => {
      calls++
      await new Promise((resolve) => setTimeout(resolve, 5))
      await declare(types, options)
    }
    const store = new Store(repository, {commitProjectors: [{...totals, history: ['ledger']}]})
    await Promise.all(['a', 'b', 'c'].map((id) => store.execute(ledger, id, 1)))
    expect(calls).toBe(1)
  })

  test('a store with outcomes declares before its commit, and through a FaultRepository', async () => {
    const repository = new MemRepository()
    const faulty = new FaultRepository(repository)
    expect(faulty.declareHistoryDependent).toBeTypeOf('function')
    const store = new Store(faulty, {
      outcomes: {},
      commitProjectors: [{...totals, history: ['ledger']}],
    })
    const identified = {
      ...ledger,
      identify: (amount: number) => ({
        commandType: 'ledger:add',
        payload: new TextEncoder().encode(`${amount}`),
      }),
    }
    const outcome = await store.executeWithOutcome(identified, 'a', 2, {
      metadata: {commandId: 'c1'},
    })
    expect(outcome.outcome.status).toBe('accepted')
    expect(controlFixtures(repository).manifest()?.historyDependent).toEqual(['ledger'])

    // A wrapped repository that can't declare leaves the wrapper without the method.
    const plain = Object.assign(Object.create(repository) as MemRepository, {
      declareHistoryDependent: undefined,
    })
    expect(new FaultRepository(plain).declareHistoryDependent).toBeUndefined()
  })

  test('each projector reads its own copy of the commit', async () => {
    const repository = new MemRepository()
    const vandal: CommitProjector = {
      name: 'vandal',
      project: (commit) => {
        commit.streams[0]!.state.fill(0x20)
        return []
      },
    }
    await new Store(repository, {commitProjectors: [vandal, totals]}).execute(ledger, 'a', 5)
    expect(decoder.decode(repository.view('totals', 'a')!.payload)).toBe('{"total":5}')
  })

  test('a store without projectors never encodes a state it does not snapshot', async () => {
    const unencodable = defineAggregate({
      ...ledger,
      state: {
        encode: () => {
          throw new Error('no snapshots for this type')
        },
        decode: () => ({total: 0}),
      },
    })
    const repository = new MemRepository()
    await new Store(repository, {snapshotInterval: 0}).execute(unencodable, 'a', 1)
    expect(await repository.events('a')).toHaveLength(1)
  })

  test("a projector's copy of a Buffer-backed state is its own", async () => {
    const buffered = defineAggregate({
      ...ledger,
      state: {
        encode: (state: {total: number}) => Buffer.from(JSON.stringify(state)),
        decode: (bytes: Uint8Array) => JSON.parse(decoder.decode(bytes)) as {total: number},
      },
    })
    const vandal: CommitProjector = {
      name: 'vandal',
      project: (commit) => {
        commit.streams[0]!.state.fill(0x20)
        return []
      },
    }
    const repository = new MemRepository()
    const store = new Store(repository, {snapshotInterval: 1, commitProjectors: [vandal, totals]})
    await store.execute(buffered, 'a', 5)

    expect(decoder.decode(repository.view('totals', 'a')!.payload)).toBe('{"total":5}')
    expect((await store.load(buffered, 'a')).state).toEqual({total: 5})
  })

  test('a retry projects again, and only the committing attempt writes views', async () => {
    const inner = new MemRepository()
    let commits = 0
    const contended = new FaultRepository(inner)
    contended.commit = async (commit, options) => {
      if (++commits === 1) {
        // Another writer gets there first.
        await inner.commit({entityId: 'a', events: [{...event('a', 1), entityType: 'ledger'}]})
        throw new ConflictError('a', 'another writer won')
      }
      return inner.commit(commit, options)
    }
    let runs = 0
    const counting: CommitProjector = {
      name: 'counting',
      project: (commit) => {
        runs++
        return totals.project(commit)
      },
    }

    await new Store(contended, {commitProjectors: [counting]}).executeWithRetry(ledger, 'a', 2, {
      sleep: async () => {},
    })
    expect(runs).toBe(2)
    expect(decoder.decode(inner.view('totals', 'a')!.payload)).toBe('{"total":3}')
  })

  test("a projector's failed condition fails the command, with nothing written", async () => {
    const repository = new MemRepository()
    const claims: CommitProjector = {
      name: 'claims',
      project: (commit) => [
        putView('claims', 'one', commit.streams[0]!.entityId, 'ledger', '{}', {
          condition: 'absent',
        }),
      ],
    }
    const store = new Store(repository, {commitProjectors: [claims]})
    await store.execute(ledger, 'a', 1)

    const error = await failure(store.execute(ledger, 'b', 1))
    expect(error).toBeInstanceOf(ProjectorOperationError)
    expect(await repository.events('b')).toEqual([])
  })
})
