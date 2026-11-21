import {describe, expect, test} from 'vitest'

import {defineAggregate, defineEvents} from '../src/aggregate.ts'
import {json} from '../src/codec.ts'
import type {DynamoClient, Page, ScanInput} from '../src/dynamodb/client.ts'
import {eventItem} from '../src/dynamodb/item.ts'
import {DynamoRepository} from '../src/dynamodb/repository.ts'
import {type EnumeratedEntity, isEventLog} from '../src/enumerate.ts'
import {type SerializedEvent, eventId} from '../src/event.ts'
import {MemRepository} from '../src/memory/repository.ts'
import {emptyMetadata} from '../src/metadata.ts'
import type {PgClient, PgResult} from '../src/postgres/client.ts'
import {PgRepository} from '../src/postgres/repository.ts'
import type {Repository} from '../src/repository.ts'
import {Store} from '../src/store.ts'

const encoder = new TextEncoder()

const ledger = defineAggregate({
  type: 'ledger',
  events: defineEvents({'ledger:added': {version: 1, codec: json<{amount: number}>()}}),
  state: json<{total: number}>(),
  initial: () => ({total: 0}),
  handle: (_state, amount: number) => ({
    kind: 'accept',
    events: [{type: 'ledger:added', data: {amount}}],
  }),
  apply: (state, added) => ({total: state.total + added.data.amount}),
})

function event(entityId: string, sequence: number, type = 'ledger:added', entityType = 'ledger') {
  return {
    id: eventId(entityId, BigInt(sequence)),
    sequence: BigInt(sequence),
    type,
    version: 1,
    entityId,
    entityType,
    payload: encoder.encode(`{"amount":${sequence}}`),
    metadata: emptyMetadata(),
  } satisfies SerializedEvent
}

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const items: T[] = []
  for await (const item of iterable) {
    items.push(item)
  }

  return items
}

function summary(entities: EnumeratedEntity<{total: number}>[]): unknown[] {
  return entities.map((entity) =>
    entity.ok
      ? [entity.entityId, entity.loaded.state.total, entity.loaded.sequence]
      : [entity.entityId, (entity.error as {code?: string}).code ?? 'error'],
  )
}

describe('Store enumeration', () => {
  test('folds each entity of a type from its events, and yields a failed one as an error', async () => {
    const repository = new MemRepository()
    await repository.commit({entityId: 'b', events: [event('b', 1), event('b', 2)]})
    await repository.commit({entityId: 'a', events: [event('a', 1)]})
    await repository.commit({entityId: 'bad', events: [event('bad', 1, 'ledger:unknown')]})
    await repository.commit({entityId: 'o', events: [event('o', 1, 'ledger:added', 'other')]})
    await repository.commitTransaction({
      streams: [],
      fences: [],
      outcome: {
        commandId: 'cmd',
        status: 'accepted',
        digest: 'd',
        response: null,
        eventIds: [],
        rejection: null,
        identity: null,
      },
    })
    const store = new Store(repository)

    expect(summary(await collect(store.streamEntities(ledger)))).toEqual([
      ['a', 1, 1n],
      ['b', 3, 2n],
      ['bad', 'replay'],
    ])
    // A named type folds into the aggregate, as after a rename.
    expect(summary(await collect(store.streamEntities(ledger, {entityType: 'other'})))).toEqual([
      ['o', 1, 1n],
    ])
  })

  test('loads each entity from its snapshot, in ID byte order', async () => {
    const repository = new MemRepository()
    const payload = encoder.encode('{"total":3}')
    await repository.commit({
      entityId: 'z',
      events: [event('z', 1), event('z', 2)],
      snapshot: {entityType: 'ledger', sequence: 1n, payload},
    })
    await repository.commit({entityId: 'z', events: [event('z', 3)]})
    expect(await repository.compactBelow('z', 2n)).toBe(2)
    await repository.commit({entityId: 'é', events: [event('é', 1)]})
    await repository.commit({entityId: 'Z', events: [event('Z', 1)]})
    const store = new Store(repository)

    expect(summary(await collect(store.streamEntitiesFromSnapshots(ledger)))).toEqual([
      ['Z', 1, 1n],
      ['z', 6, 3n],
      ['é', 1, 1n],
    ])
    expect(summary(await collect(store.streamEntities(ledger)))).toContainEqual([
      'z',
      'sequenceGap',
    ])
  })

  test('refuses a repository that does not enumerate its log', async () => {
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
    expect(isEventLog(repository)).toBe(false)
    await expect(collect(new Store(repository).streamEntities(ledger))).rejects.toMatchObject({
      code: 'invalidArgument',
    })
  })
})

// A table of items, scanned in pages of `size`, which records each request.
function table(items: Record<string, unknown>[], size: number) {
  const requests: ScanInput[] = []
  const client: DynamoClient = {
    getItem: () => Promise.reject(new Error('unexpected getItem')),
    putItem: () => Promise.reject(new Error('unexpected putItem')),
    query: () => Promise.reject(new Error('unexpected query')),
    transactWriteItems: () => Promise.reject(new Error('unexpected transactWriteItems')),
    async scan(input: ScanInput): Promise<Page> {
      requests.push(input)
      const start = input.ExclusiveStartKey === undefined ? 0 : Number(input.ExclusiveStartKey['i'])
      const page = items.slice(start, start + size)
      return {
        Items: page,
        ...(start + size < items.length && {LastEvaluatedKey: {i: String(start + size)}}),
      }
    },
  }

  return {client, requests}
}

describe('DynamoRepository enumeration', () => {
  test('yields the event rows of each Scan page, leaving out the rest', async () => {
    const {client, requests} = table(
      [
        eventItem(event('a', 1)),
        {pk: {S: 'a'}, sk: {N: '-1'}, head: {N: '2'}},
        {pk: {S: 'a'}, sk: {N: '0'}, eventSeq: {N: '1'}, entityType: {S: 'ledger'}},
        eventItem(event('a', 2)),
        // A page the filter emptied out, and an engine row the emulator didn't filter.
        {pk: {S: 'SYSTEM#FEATURES'}, sk: {N: '0'}},
        eventItem(event('b', 1)),
      ],
      2,
    )
    const repository = new DynamoRepository(client, 'events', {consistentRead: false})
    const batches = await collect(repository.streamAllEvents({entityType: 'ledger'}))

    expect(batches.map((batch) => batch.map((e) => `${e.entityId}:${e.sequence}`))).toEqual([
      ['a:1'],
      ['a:2'],
      ['b:1'],
    ])
    expect(requests[0]).toMatchObject({
      TableName: 'events',
      ConsistentRead: false,
      FilterExpression:
        'NOT begins_with(pk, :system) AND NOT begins_with(pk, :outcome) AND entityType = :et',
      ExpressionAttributeValues: {
        ':system': {S: 'SYSTEM#'},
        ':outcome': {S: 'command-outcome#'},
        ':et': {S: 'ledger'},
      },
    })
    expect(requests).toHaveLength(3)
  })

  test("folds each entity's head from events and snapshots, never head items", async () => {
    const {client, requests} = table(
      [
        {pk: {S: 'a'}, sk: {N: '3'}},
        {pk: {S: 'a'}, sk: {N: '-1'}, head: {N: '9'}},
        {pk: {S: 'b'}, sk: {N: '0'}, eventSeq: {N: '5'}},
        {pk: {S: 'b'}, sk: {N: '4'}},
        {pk: {S: 'c'}, sk: {N: '0'}, eventSeq: {N: '0'}},
      ],
      2,
    )
    const heads = await new DynamoRepository(client, 'events').entityHeads()

    expect([...heads]).toEqual([
      ['a', 3n],
      ['b', 5n],
    ])
    expect(requests[0]).toMatchObject({ProjectionExpression: 'pk, sk, eventSeq'})
    expect(requests[0]!.FilterExpression).not.toContain(':et')
  })
})

describe('PgRepository enumeration', () => {
  test('pages the log and yields one batch per entity, across pages', async () => {
    const rows = [
      ...Array.from({length: 499}, (_, i) => event('a', i + 1)),
      event('b', 1),
      event('b', 2),
    ].map((e) => ({
      event_id: e.id,
      sequence: e.sequence.toString(),
      event_type: e.type,
      version: '1',
      entity_id: e.entityId,
      entity_type: e.entityType,
      payload: Buffer.from(e.payload).toString('hex'),
      metadata: '{}',
    }))
    const sent: (unknown[] | undefined)[] = []
    const client: PgClient = {
      async query(text: string, values?: unknown[]): Promise<PgResult> {
        sent.push(values)
        const after = values?.[1] === null ? 0 : 500
        return {rows: text.includes('LIMIT 500') ? rows.slice(after, after + 500) : []}
      },
    }
    const batches = await collect(new PgRepository(client).streamAllEvents())

    expect(batches.map((batch) => [batch[0]!.entityId, batch.length])).toEqual([
      ['a', 499],
      ['b', 2],
    ])
    // The second page continues from the first page's last row.
    expect(sent).toEqual([
      [null, null, null],
      [null, 'b', '1'],
    ])
  })
})
