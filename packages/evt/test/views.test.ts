// The view repositories and the typed view codec. The views.json vectors and the conformance cases
// run in packages/evt-compat (in memory, and live on Moto); these pin what they don't: batching,
// retries, the consistency option, and the codec's API.
import {DynamoDB} from '@aws-sdk/client-dynamodb'
import {describe, expect, test} from 'vitest'

import {json} from '../src/codec.ts'
import {
  EvtError,
  ProjectorOperationError,
  TransientError,
  ViewsNotConfiguredError,
} from '../src/errors.ts'
import type {DynamoClient} from '../src/dynamodb/client.ts'
import {DynamoRepository} from '../src/dynamodb/repository.ts'
import {
  type BatchGetItemInput,
  type BatchWriteItemInput,
  type DynamoViewClient,
  DynamoViewRepository,
  type ViewQueryInput,
} from '../src/dynamodb/view-repository.ts'
import type {Item} from '../src/dynamodb/item.ts'
import {MemRepository} from '../src/memory/repository.ts'
import {MemViewRepository} from '../src/memory/views.ts'
import {eventId} from '../src/event.ts'
import {emptyMetadata} from '../src/metadata.ts'
import {putView} from '../src/projection.ts'
import {ViewCodec, ViewDecodeError, decodeJsonView} from '../src/viewcodec.ts'
import {type ViewRow, decodeViewCursor, encodeViewCursor} from '../src/views.ts'

const encoder = new TextEncoder()
const decoder = new TextDecoder()

function row(pk: string, sk: string, payload = '{}', entityType = 't'): ViewRow {
  return {pk, sk, entityId: 'owner', entityType, payload: encoder.encode(payload)}
}

/** A client that records requests and answers from scripts, one answer per call. */
class ScriptedClient implements DynamoViewClient {
  readonly writes: BatchWriteItemInput[] = []
  readonly batchGets: BatchGetItemInput[] = []
  readonly queries: ViewQueryInput[] = []
  writeAnswers: {UnprocessedItems?: Record<string, unknown[]>}[] = []
  getAnswers: {
    Responses?: Record<string, Record<string, unknown>[]>
    UnprocessedKeys?: Record<string, unknown>
  }[] = []

  getItem: DynamoViewClient['getItem'] = async () => ({})

  async query(input: ViewQueryInput) {
    this.queries.push(input)

    return {Items: []}
  }

  async batchWriteItem(input: BatchWriteItemInput) {
    this.writes.push(structuredClone(input))

    return this.writeAnswers.shift() ?? {}
  }

  async batchGetItem(input: BatchGetItemInput) {
    this.batchGets.push(structuredClone(input))

    return this.getAnswers.shift() ?? {}
  }
}

const noSleep = async (): Promise<void> => {}

function item(pk: string): Item {
  return {pk: {S: pk}, sk: {S: 'VIEW'}, payload: {S: '{}'}}
}

describe('DynamoViewRepository', () => {
  test('the SDK client is a DynamoViewClient', () => {
    const client: DynamoViewClient = new DynamoDB({region: 'us-west-2'})
    expect(new DynamoViewRepository(client, 'views').table).toBe('views')
    expect(() => new DynamoViewRepository(client, '')).toThrow(RangeError)
  })

  test('partition listings read consistently unless opted out; the index never does', async () => {
    const client = new ScriptedClient()
    await new DynamoViewRepository(client, 'views').listViewsByPkPaged('p')
    await new DynamoViewRepository(client, 'views', {consistentRead: false}).listViewsByPk('p')
    await new DynamoViewRepository(client, 'views').listViewsByEntityTypePaged('t', {limit: 5})

    expect(client.queries.map((query) => query.ConsistentRead)).toEqual([true, false, undefined])
    expect(client.queries[2]).toMatchObject({IndexName: 'entityType-index', Limit: 5})
  })

  test('putViews writes chunks of 25, a repeated key once with its last row', async () => {
    const client = new ScriptedClient()
    const rows = Array.from({length: 60}, (_, index) => row('p', `s${index}`))
    rows.push(row('p', 's0', '{"last":true}'), row('p', '', '{"a":1}'), row('p', 'VIEW', '{"b":2}'))
    await new DynamoViewRepository(client, 'views').putViews(rows)

    const requests = client.writes.map((write) => write.RequestItems['views'] ?? [])
    expect(requests.map((chunk) => chunk.length)).toEqual([25, 25, 11])
    const items = requests.flat().map((request) => request.PutRequest!.Item)
    const at = (sk: string): Item[] =>
      items.filter((written) => (written['sk'] as {S: string}).S === sk)
    expect(at('s0')).toEqual([expect.objectContaining({payload: {S: '{"last":true}'}})])
    expect(at('VIEW')).toEqual([expect.objectContaining({payload: {S: '{"b":2}'}})])
  })

  test('a bad row refuses the whole batch before any request', async () => {
    const client = new ScriptedClient()
    const bad = {...row('p', 'b'), payload: new Uint8Array([0xff])}
    const refused = new DynamoViewRepository(client, 'views').putViews([row('p', 'a'), bad])

    await expect(refused).rejects.toMatchObject({code: 'invalidRecord'})
    expect(client.writes).toEqual([])
  })

  test('unprocessed writes are sent again, and fail as transient after three attempts', async () => {
    const pending = {views: [{PutRequest: {Item: {pk: {S: 'p'}}}}]}
    const delays: number[] = []
    const sleep = async (ms: number): Promise<void> => {
      delays.push(ms)
    }

    const client = new ScriptedClient()
    client.writeAnswers = [{UnprocessedItems: pending}, {UnprocessedItems: {views: []}}]
    await new DynamoViewRepository(client, 'views', {sleep}).putView(row('p', 's'))
    expect(client.writes).toHaveLength(2)
    expect(client.writes[1]!.RequestItems).toEqual(pending)
    expect(delays).toEqual([25])

    const stuck = new ScriptedClient()
    stuck.writeAnswers = [1, 2, 3].map(() => ({UnprocessedItems: pending}))
    const failed = new DynamoViewRepository(stuck, 'views', {sleep}).deleteView('p', '')
    await expect(failed).rejects.toBeInstanceOf(TransientError)
    expect(stuck.writes).toHaveLength(3)
    expect(stuck.writes[0]!.RequestItems['views']).toEqual([
      {DeleteRequest: {Key: {pk: {S: 'p'}, sk: {S: 'VIEW'}}}},
    ])
  })

  test('batch reads are strongly consistent, chunked to 100, and retry unprocessed keys', async () => {
    const client = new ScriptedClient()
    client.getAnswers = [
      {
        Responses: {views: [item('p000')]},
        UnprocessedKeys: {views: {Keys: [{pk: {S: 'p001'}, sk: {S: 'VIEW'}}]}},
      },
      {Responses: {views: [item('p001')]}},
      {Responses: {views: [item('p100')]}, UnprocessedKeys: {views: {Keys: []}}},
    ]
    const pks = Array.from({length: 101}, (_, index) => `p${String(index).padStart(3, '0')}`)
    const repo = new DynamoViewRepository(client, 'views', {sleep: noSleep})
    const rows = await repo.batchGetViews([...pks, 'p000', ''])

    expect(rows.map((found) => found.pk)).toEqual(['p000', 'p001', 'p100'])
    expect(rows[0]).toMatchObject({entityId: '', ttl: 0})
    const sent = client.batchGets.map((get) => get.RequestItems['views']!)
    expect(sent.map((keys) => keys.Keys.length)).toEqual([100, 1, 1])
    expect(sent.every((keys) => keys.ConsistentRead === true)).toBe(true)
  })

  test('the abort signal goes with every request, and an aborted call sends none', async () => {
    const seen: unknown[] = []
    const client = new ScriptedClient()
    client.getItem = async (_input, options) => {
      seen.push(options)

      return {}
    }
    const controller = new AbortController()
    const repo = new DynamoViewRepository(client, 'views')
    await repo.getViewAt('p', 's', {signal: controller.signal})
    expect(seen).toEqual([{abortSignal: controller.signal}])

    controller.abort(new Error('stop'))
    await expect(repo.putView(row('p', 's'), {signal: controller.signal})).rejects.toThrow('stop')
    expect(client.writes).toEqual([])
  })
})

describe('MemViewRepository', () => {
  test('a page carries a cursor only when a row follows', async () => {
    const repo = new MemViewRepository()
    await repo.putViews(['a', 'b', 'c'].map((sk) => row('p', sk)))

    const first = await repo.listViewsByPkPaged('p', {limit: 2})
    expect(first.views.map((found) => found.sk)).toEqual(['a', 'b'])
    expect(decodeViewCursor(first.nextCursor!)).toEqual(
      new Map([
        ['pk', 'p'],
        ['sk', 'b'],
      ]),
    )
    const second = await repo.listViewsByPkPaged('p', {limit: 2, cursor: first.nextCursor})
    expect(second).toEqual({views: [expect.objectContaining({sk: 'c'})], nextCursor: null})
    expect((await repo.listViewsByPkPaged('p', {limit: 3})).nextCursor).toBeNull()
  })

  test("an entity type's cursor is DynamoDB's index key", async () => {
    const repo = new MemViewRepository()
    await repo.putViews([row('p1', 'VIEW'), row('p2', 'VIEW')])

    const page = await repo.listViewsByEntityTypePaged('t', {limit: 1})
    // {"entityType":"t","pk":"p1","sk":"VIEW"}, as Rust's in-memory repository pins it.
    expect(page.nextCursor).toBe('eyJlbnRpdHlUeXBlIjoidCIsInBrIjoicDEiLCJzayI6IlZJRVcifQ==')
  })

  test('rows share no bytes with the caller', async () => {
    const repo = new MemViewRepository()
    const written = row('p', 's', '{"a":1}')
    await repo.putView(written)
    written.payload[2] = 0x62

    const read = await repo.getViewAt('p', 's')
    read!.payload[2] = 0x63
    expect(decoder.decode((await repo.getViewAt('p', 's'))!.payload)).toBe('{"a":1}')
  })
})

// Commits one event to `owner`, putting the row handles/ada with `payload`.
function commit(repo: MemRepository, owner: string, payload: string): Promise<void> {
  return repo.commit({
    entityId: owner,
    events: [
      {
        id: eventId(owner, 1n),
        sequence: 1n,
        type: 'ledger:added',
        version: 1,
        entityId: owner,
        entityType: 'ledger',
        payload: encoder.encode('{}'),
        metadata: {...emptyMetadata(), timestamp: '2026-01-02T03:04:05Z'},
      },
    ],
    views: [
      {
        projector: 'p',
        projectorIndex: 0,
        operation: 0,
        op: putView('handles', 'ada', owner, 'ledger', payload),
      },
    ],
  })
}

describe('MemRepository as a view repository', () => {
  test('a commit and a view read see the same rows', async () => {
    const repo = new MemRepository()
    await commit(repo, 'a', '{"n":1}')

    expect(await repo.getViewAt('handles', 'ada')).toEqual({
      pk: 'handles',
      sk: 'ada',
      entityId: 'a',
      entityType: 'ledger',
      payload: encoder.encode('{"n":1}'),
      ttl: 0,
    })
    expect(repo.view('handles', 'ada')).toEqual(await repo.getViewAt('handles', 'ada'))
  })

  test("a row written outside a commit is the one a commit's owner check reads", async () => {
    const repo = new MemRepository()
    await repo.putView({...row('handles', 'ada'), entityId: 'b'})

    await expect(commit(repo, 'a', '{}')).rejects.toBeInstanceOf(ProjectorOperationError)
    await repo.deleteView('handles', 'ada')
    await commit(repo, 'a', '{}')
    expect((await repo.listViewsByEntityType('ledger')).map((found) => found.entityId)).toEqual([
      'a',
    ])
  })
})

describe('DynamoRepository.viewRepository', () => {
  test('reads its views table through its client', () => {
    const client = new DynamoDB({region: 'us-west-2'})
    const repo = new DynamoRepository(client, 'events', {viewsTable: 'views'})
    expect(repo.viewRepository().table).toBe('views')
  })

  test('is refused without a views table, or with a client that cannot batch', () => {
    const client = new DynamoDB({region: 'us-west-2'})
    expect(() => new DynamoRepository(client, 'events').viewRepository()).toThrow(
      ViewsNotConfiguredError,
    )
    expect(() => new DynamoRepository(client, 'events', {viewsTable: ''}).viewRepository()).toThrow(
      ViewsNotConfiguredError,
    )

    const unbatched: DynamoClient = {
      getItem: async () => ({}),
      putItem: async () => ({}),
      query: async () => ({Items: []}),
      scan: async () => ({Items: []}),
      transactWriteItems: async () => ({}),
    }
    expect(() =>
      new DynamoRepository(unbatched, 'events', {viewsTable: 'views'}).viewRepository(),
    ).toThrow(TypeError)
  })
})

describe('the view cursor', () => {
  test("is Go's padded base64url JSON, keys sorted", () => {
    const cursor = encodeViewCursor(
      new Map([
        ['sk', '~~~'],
        ['pk', '???>>>'],
      ]),
    )
    expect(cursor).toBe('eyJwayI6Ij8_P1x1MDAzZVx1MDAzZVx1MDAzZSIsInNrIjoifn5-In0=')
    expect(decodeViewCursor(cursor).get('pk')).toBe('???>>>')
  })

  test('refuses a standard-alphabet or keyless cursor', () => {
    for (const cursor of ['eyJwayI6Ij8/P1x1MDAzZVx1MDAzZVx1MDAzZSIsInNrIjoifn5+In0=', 'e30=']) {
      expect(() => decodeViewCursor(cursor)).toThrow(/invalid pagination cursor/)
    }
  })
})

interface Profile {
  name: string
  bio: string | null
  tags: string[]
  count: number
}

const ada: Profile = {name: 'Ada', bio: null, tags: ['b', 'a'], count: 3}

describe('ViewCodec', () => {
  test('a value round-trips, written as Go writes it', async () => {
    const repo = new MemViewRepository()
    const codec = new ViewCodec<Profile>(repo, 'profile')
    expect(await codec.get('profile#ada')).toBeNull()

    await codec.putAt('profile#ada', 'ada', {...ada, name: 'Ada <&>  '})
    const stored = await repo.getView('profile#ada')
    expect(stored).toMatchObject({sk: 'VIEW', entityId: 'ada', entityType: 'profile', ttl: 0})
    expect(decoder.decode(stored!.payload)).toBe(
      '{"name":"Ada \\u003c\\u0026\\u003e \\u2028","bio":null,"tags":["b","a"],"count":3}',
    )
    expect(await codec.get('profile#ada')).toEqual({...ada, name: 'Ada <&>  '})
    expect(codec.entityType).toBe('profile')
    expect(codec.repo).toBe(repo)
  })

  test('putRow keeps the template and its ttl, and leaves it unchanged', async () => {
    const repo = new MemViewRepository()
    const codec = new ViewCodec<Profile>(repo, 'profile')
    const template: ViewRow = {...row('p', 's', 'ignored', 'other-type'), ttl: 99}
    await codec.putRow(template, ada)

    expect(await repo.getViewAt('p', 's')).toMatchObject({entityType: 'other-type', ttl: 99})
    expect(await codec.getAt('p', 's')).toEqual(ada)
    expect(decoder.decode(template.payload)).toBe('ignored')
  })

  test('a value the codec refuses fails with invalidArgument, naming the row', async () => {
    const codec = new ViewCodec<unknown>(new MemViewRepository(), 'profile')
    const refused = codec.put('p', 's', 'o', {when: new Date(0)})

    await expect(refused).rejects.toMatchObject({code: 'invalidArgument'})
    await expect(refused).rejects.toThrow(/^encode JSON view pk="p" sk="s": /)
  })

  test('strict reads fail on a bad payload, and lenient lists skip and report it', async () => {
    const repo = new MemViewRepository()
    const codec = new ViewCodec<Profile>(repo, 'profile')
    await codec.put('p', 'a', 'o', {...ada, name: 'a'})
    await repo.putView(row('p', 'b', 'not json', 'profile'))
    await repo.putView(row('p', 'c', '{"n":9007199254740993}', 'profile'))
    await codec.put('p', 'd', 'o', {...ada, name: 'd'})

    const refused = codec.getAt('p', 'b')
    await expect(refused).rejects.toBeInstanceOf(ViewDecodeError)
    await expect(refused).rejects.toMatchObject({pk: 'p', sk: 'b', code: 'invalidRecord'})
    await expect(refused).rejects.toThrow(/^decode JSON view pk="p" sk="b": /)

    const skipped: string[] = []
    const listed = await codec.listByPk('p', {
      onDecodeError: (bad, error) => skipped.push(`${bad.sk}:${error.sk}`),
    })
    expect(listed.map((value) => value.name)).toEqual(['a', 'd'])
    expect(skipped).toEqual(['b:b', 'c:c'])
    expect(await codec.listAll()).toEqual(listed)

    await expect(codec.listByPkStrict('p')).rejects.toBeInstanceOf(ViewDecodeError)
    await expect(codec.listAllStrict()).rejects.toBeInstanceOf(ViewDecodeError)
    await expect(codec.listByPkPaged('p', {limit: 1})).resolves.toEqual({
      values: [{...ada, name: 'a'}],
      nextCursor: expect.any(String),
    })
  })

  test('a lenient list rethrows what is not a decode failure', async () => {
    const repo = new MemViewRepository()
    await repo.putView(row('p', 'a', '{}', 'profile'))
    const codec = new ViewCodec(repo, 'profile', {
      encode: () => new Uint8Array(),
      decode: () => {
        throw new Error('decoder bug')
      },
    })
    // The codec's own throw is a decode failure of the row, as Go's decoder error is.
    await expect(codec.listByPk('p')).resolves.toEqual([])

    const broken = new MemViewRepository()
    broken.listViewsByPk = async () => {
      throw new EvtError('transient', 'down')
    }
    await expect(new ViewCodec(broken, 'profile').listByPk('p')).rejects.toThrow('down')
  })

  test('lists are bounded to the entity type', async () => {
    const repo = new MemViewRepository()
    await new ViewCodec<Profile>(repo, 'profile').put('p1', 'VIEW', 'o', ada)
    await new ViewCodec<Profile>(repo, 'other').put('p2', 'VIEW', 'o', {...ada, name: 'b'})

    expect(await new ViewCodec<Profile>(repo, 'profile').listAllStrict()).toEqual([ada])
  })

  test('a custom codec builds the value, as a Go factory does', async () => {
    class Entity {
      constructor(
        readonly injected: number,
        readonly name: string,
      ) {}
    }
    const base = json<{name: string}>()
    const codec = new ViewCodec<Entity>(new MemViewRepository(), 'entity', {
      encode: (entity) => base.encode({name: entity.name}),
      decode: (bytes) => new Entity(7, base.decode(bytes).name),
    })
    await codec.putAt('e', 'o', new Entity(1, 'x'))

    expect(await codec.get('e')).toEqual(new Entity(7, 'x'))
  })

  test('a single view derives the pk and the owner', async () => {
    const repo = new MemViewRepository()
    const single = new ViewCodec<Profile>(repo, 'profile').single(
      (id: string) => `profile#${id}`,
      (value) => value.name.toLowerCase(),
    )
    expect(await single.get('ada')).toBeNull()

    await single.put('ada', ada)
    expect(await single.get('ada')).toEqual(ada)
    expect((await repo.getView('profile#ada'))?.entityId).toBe('ada')
    expect(await single.listAll()).toEqual([ada])
    expect(single.codec.entityType).toBe('profile')

    const ownerless = new ViewCodec<Profile>(repo, 'profile').single((id: string) => id)
    await ownerless.put('x', ada)
    expect((await repo.getView('x'))?.entityId).toBe('')
  })

  test('decodeJsonView names the row', () => {
    const viewRow = row('p', 's', '[1')
    expect(() => decodeJsonView(viewRow)).toThrow(ViewDecodeError)
    expect(decodeJsonView(row('p', 's', '[1]'))).toEqual([1])
  })
})
