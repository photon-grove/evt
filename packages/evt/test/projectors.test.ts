// Projectors: the Store hands each one the state after a commit's events, the commit carries their
// groups, and every backend budgets them and refuses them without events.
import {describe, expect, test} from 'vitest'

import {type Projection, type Projector, defineAggregate, defineEvents} from '../src/aggregate.ts'
import {json} from '../src/codec.ts'
import {headActionSize, headUpdate} from '../src/dynamodb/head.ts'
import {itemSize, viewItem} from '../src/dynamodb/item.ts'
import {commitTransaction} from '../src/dynamodb/requests.ts'
import {DYNAMODB_BACKEND, transactItemSize, viewProjector} from '../src/dynamodb/views.ts'
import {CommitTooLargeError, EvtError, ProjectorOnlyCommitError} from '../src/errors.ts'
import {type SerializedEvent, eventId} from '../src/event.ts'
import {MemRepository} from '../src/memory/repository.ts'
import {emptyMetadata} from '../src/metadata.ts'
import type {CallOptions, Commit, ProjectorGroup} from '../src/repository.ts'
import {Store} from '../src/store.ts'

const encoder = new TextEncoder()
const decoder = new TextDecoder()

const events = defineEvents({
  'counter:added': {version: 1, codec: json<{n: number}>()},
})

type State = {total: number}

// A projector that records what it saw and returns a group of one operation, its name.
function recording(name: string, seen: Projection<State>[]): Projector<State> {
  return {
    project(projection) {
      seen.push(projection)

      return {backend: 'test', operations: [name], sizes: [1]}
    },
  }
}

function counter(projectors: Projector<State>[]) {
  return defineAggregate({
    type: 'counter',
    events,
    state: json<State>(),
    initial: () => ({total: 0}),
    handle: (_state, command: {add: number[]}) => ({
      kind: 'accept',
      events: command.add.map((n) => ({type: 'counter:added' as const, data: {n}})),
    }),
    apply: (state, added) => ({total: state.total + added.data.n}),
    projectors,
  })
}

// A memory repository that keeps every commit it was asked for.
class Recorded extends MemRepository {
  readonly commits: Commit[] = []

  override async commit(commit: Commit, options?: CallOptions): Promise<void> {
    this.commits.push(commit)

    return super.commit(commit, options)
  }
}

function event(sequence: number): SerializedEvent {
  return {
    id: eventId('c', BigInt(sequence)),
    sequence: BigInt(sequence),
    type: 'counter:added',
    version: 1,
    entityId: 'c',
    entityType: 'counter',
    payload: encoder.encode('{"n":1}'),
    metadata: emptyMetadata(),
  }
}

function group(operations: number, size = 1): ProjectorGroup {
  return {
    backend: 'test',
    operations: Array.from({length: operations}, (_, i) => i),
    sizes: Array.from({length: operations}, () => size),
  }
}

describe('the store', () => {
  test("hands each projector the state after the commit's events", async () => {
    const seen: Projection<State>[] = []
    const repository = new Recorded()
    const store = new Store(repository, {snapshotInterval: 0})
    const skipped: Projector<State> = {project: () => null}
    const aggregate = counter([recording('first', seen), skipped, recording('last', seen)])

    await store.execute(aggregate, 'c', {add: [2]})
    await store.execute(aggregate, 'c', {add: [3, 4]})

    const second = seen.at(-1)!
    expect(second.entityType).toBe('counter')
    expect(second.entityId).toBe('c')
    expect(second.state).toEqual({total: 9})
    expect(decoder.decode(second.encodedState)).toBe('{"total":9}')
    expect(second.events.map((stored) => stored.sequence)).toEqual([2n, 3n])

    // In declaration order, and a projector that returns null adds nothing.
    expect(repository.commits.at(-1)?.projections?.map((g) => g.operations)).toEqual([
      ['first'],
      ['last'],
    ])
  })

  test('hands projectors the snapshot payload when the commit writes one, as a copy', async () => {
    const seen: Projection<State>[] = []
    const repository = new Recorded()
    await new Store(repository, {snapshotInterval: 1}).execute(
      counter([recording('p', seen)]),
      'c',
      {add: [5]},
    )

    const payload = repository.commits[0]?.snapshot?.payload
    expect(seen[0]?.encodedState).toEqual(payload)
    expect(seen[0]?.encodedState).not.toBe(payload)
    expect(Object.isFrozen(seen[0])).toBe(true)
  })

  test("can't change the events the commit stores, or another projector's view", async () => {
    const seen: Projection<State>[] = []
    const tamper: Projector<State> = {
      project({events: [first]}) {
        // The digit of {"n":1}.
        first!.payload![5] = 0x39
        first!.metadata.extensions.set('tampered', 'yes')

        return null
      },
    }
    const repository = new Recorded()
    const aggregate = counter([tamper, recording('after', seen)])
    await new Store(repository).execute(aggregate, 'c', {add: [1]})

    const [stored] = await repository.events('c')
    expect(decoder.decode(stored!.payload!)).toBe('{"n":1}')
    expect(stored!.metadata.extensions.has('tampered')).toBe(false)
    expect(decoder.decode(seen[0]!.events[0]!.payload!)).toBe('{"n":1}')
    expect(Object.isFrozen(seen[0]!.events[0])).toBe(true)
  })

  test('commits no projections without events', async () => {
    const seen: Projection<State>[] = []
    const repository = new Recorded()
    await new Store(repository).execute(counter([recording('p', seen)]), 'c', {add: []})

    expect(seen).toEqual([])
    expect(repository.commits).toEqual([])
  })
})

describe('a repository', () => {
  test('refuses projector operations without events', async () => {
    const commit = new MemRepository().commit({entityId: 'c', events: [], projections: [group(2)]})

    await expect(commit).rejects.toBeInstanceOf(ProjectorOnlyCommitError)
    await expect(commit).rejects.toMatchObject({operations: 2})
  })

  test('refuses a group without a size per operation', async () => {
    const projections = [{backend: 'test', operations: [1, 2], sizes: [1]}]
    const commit = new MemRepository().commit({entityId: 'c', events: [event(1)], projections})

    await expect(commit).rejects.toMatchObject({code: 'invalidCommit'})
  })

  test('budgets the operations, which the memory repository then drops', async () => {
    const repository = new MemRepository()
    await repository.commit({entityId: 'c', events: [event(1)], projections: [group(96)]})
    expect(await repository.events('c')).toHaveLength(1)

    // The 2 control checks, 97 operations, the head update and the event put are 101 actions.
    const over = repository.commit({entityId: 'c', events: [event(2)], projections: [group(97)]})
    await expect(over).rejects.toBeInstanceOf(CommitTooLargeError)

    const large = repository.commit({
      entityId: 'c',
      events: [event(2)],
      projections: [group(1, 3_800_000)],
    })
    await expect(large).rejects.toBeInstanceOf(CommitTooLargeError)
  })
})

describe('the DynamoDB view projector', () => {
  const projection = {
    entityType: 'counter',
    entityId: 'c',
    state: {total: 9},
    encodedState: encoder.encode('{"total":9}'),
    events: [],
  }

  test("puts the entity's view row", () => {
    const put = {Put: {TableName: 'views', Item: viewItem('counter', 'c', projection.encodedState)}}

    expect(viewProjector<State>('views').project(projection)).toEqual({
      backend: DYNAMODB_BACKEND,
      operations: [put],
      sizes: [itemSize(put.Put.Item)],
    })
  })

  test('projects nothing for an entity without an ID, and needs a table', () => {
    expect(viewProjector<State>('views').project({...projection, entityId: ''})).toBeNull()
    expect(() => viewProjector('')).toThrow(RangeError)
  })

  test('sizes an update as the commit budget sizes a head update', () => {
    for (const first of [1n, 4n]) {
      const commit = {entityId: 'c', events: [{...event(1), sequence: first}]}
      const [, , head] = commitTransaction('events', commit, 1n)
      const action = headUpdate('c', first, first)

      expect(transactItemSize(head!)).toBe(headActionSize(action))
    }
  })

  test('sizes a delete and a check by their keys and expressions', () => {
    const key = {pk: {S: 'k'}, sk: {S: 'VIEW'}}
    const expressions = {
      ConditionExpression: 'entityID = :owner',
      ExpressionAttributeNames: {'#t': 'ttl'},
      ExpressionAttributeValues: {':owner': {S: 'acct-1'}},
    }
    // pk + k, sk + VIEW, the condition, #t + ttl, and :owner + acct-1.
    const want = 3 + 6 + 'entityID = :owner'.length + 5 + 12

    expect(transactItemSize({Delete: {TableName: 'views', Key: key, ...expressions}})).toBe(want)
    expect(
      transactItemSize({ConditionCheck: {TableName: 'views', Key: key, ...expressions}}),
    ).toBe(want)
  })

  test('refuses a group for another backend in a DynamoDB commit', () => {
    const commit = {entityId: 'c', events: [event(1)], projections: [group(1)]}

    expect(() => commitTransaction('events', commit, 1n)).toThrow(EvtError)
  })
})
