// DynamoRepository against a scripted client: the calls it makes and how it classifies a failed
// one. The live behavior runs in packages/evt-compat against Moto.
import {DynamoDB} from '@aws-sdk/client-dynamodb'
import {readFileSync} from 'node:fs'
import {dirname, join} from 'node:path'
import {fileURLToPath} from 'node:url'
import {describe, expect, test} from 'vitest'

import type {DynamoClient, Page, TransactItem} from '../src/dynamodb/client.ts'
import {
  type Item,
  eventItem,
  expiry,
  headItem,
  snapshotItem,
  sortKey,
} from '../src/dynamodb/item.ts'
import {markerItem} from '../src/dynamodb/marker.ts'
import {DynamoRepository} from '../src/dynamodb/repository.ts'
import {commitTransaction, snapshotTransaction} from '../src/dynamodb/requests.ts'
import {viewProjector} from '../src/dynamodb/views.ts'
import {
  CommitTooLargeError,
  ConflictError,
  EvtError,
  FeatureManifestChangedError,
  HistoryDependentProjectionError,
  RebuildInProgressError,
  StoreNotInitializedError,
  StreamHeadDriftError,
  TransientError,
  UnsupportedFeatureError,
  isConflict,
  isRetryable,
} from '../src/errors.ts'
import {type SerializedEvent, eventId} from '../src/event.ts'
import {emptyMetadata} from '../src/metadata.ts'
import {
  type Commit,
  commitSizes,
  normalizeTransaction,
  transactionSizes,
} from '../src/repository.ts'

function event(sequence: number): SerializedEvent {
  return {
    id: eventId('a', BigInt(sequence)),
    sequence: BigInt(sequence),
    type: 'ledger:added',
    version: 1,
    entityId: 'a',
    entityType: 't',
    payload: new TextEncoder().encode('{}'),
    metadata: emptyMetadata(),
  }
}

function manifest(version: bigint, features: [string, number][] = [['stream-heads', 1]]): Item {
  return markerItem({version, features: new Map(features), historyDependent: [], retention: []})
}

function cancelled(...codes: string[]): Error {
  return Object.assign(new Error('Transaction cancelled'), {
    name: 'TransactionCanceledException',
    CancellationReasons: codes.map((Code) => ({Code})),
  })
}

// A client whose marker row is `marker` and whose transactions answer `answers` in turn. A
// failing one first moves the marker to `declared`, when it's set, as a racing writer would.
class Scripted implements DynamoClient {
  readonly transactions: TransactItem[][] = []
  marker: Item | undefined
  declared: Item | undefined
  readonly #answers: (Error | null)[]

  constructor(marker: Item | undefined, ...answers: (Error | null)[]) {
    this.marker = marker
    this.#answers = answers
  }

  // Stream a's snapshot, when set, and the event keys a query returns.
  snapshot: Item | undefined
  keys: Item[] = []

  async getItem(input: {Key: Item}): Promise<{Item?: Item}> {
    const pk = input.Key['pk']
    if (pk && 'S' in pk && pk.S === 'SYSTEM#FEATURES') {
      return this.marker ? {Item: this.marker} : {}
    }
    if (pk && 'S' in pk && pk.S === 'a' && this.snapshot) {
      return {Item: this.snapshot}
    }

    return {}
  }

  async putItem(): Promise<unknown> {
    throw new Error('unexpected putItem')
  }

  async query(): Promise<Page> {
    return {Items: this.keys}
  }

  async scan(): Promise<Page> {
    return {Items: []}
  }

  async transactWriteItems(input: {TransactItems: TransactItem[]}): Promise<unknown> {
    this.transactions.push(input.TransactItems)
    const answer = this.#answers.shift()
    if (answer) {
      this.marker = this.declared ?? this.marker
      throw answer
    }

    return {}
  }
}

// Commits event 1 of stream a through a fresh repository.
function commitOne(client: DynamoClient): Promise<void> {
  return new DynamoRepository(client, 'events').commit({entityId: 'a', events: [event(1)]})
}

describe('DynamoRepository', () => {
  test("takes the SDK's DynamoDB client", () => {
    const repo = new DynamoRepository(new DynamoDB({region: 'us-west-2'}), 'events')

    expect(repo.eventsTable).toBe('events')
  })

  test('commits one transaction on the version it validated', async () => {
    const client = new Scripted(manifest(3n))
    const commit = {entityId: 'a', events: [event(1), event(2)]}

    await new DynamoRepository(client, 'events').commit(commit)

    expect(client.transactions).toEqual([commitTransaction('events', commit, 3n)])
  })

  test('refuses an uninitialized store without a request', async () => {
    const client = new Scripted(undefined)
    const commit = commitOne(client)

    await expect(commit).rejects.toThrow(StoreNotInitializedError)
    expect(client.transactions).toEqual([])
  })

  test('a failed marker check validates again, then retries on the new version', async () => {
    const client = new Scripted(manifest(1n), cancelled('ConditionalCheckFailed', 'None', 'None'))
    const repo = new DynamoRepository(client, 'events')
    const commit = {entityId: 'a', events: [event(1)]}
    client.declared = manifest(2n)

    const error = await repo.commit(commit).catch((thrown: unknown) => thrown)
    expect(error).toBeInstanceOf(FeatureManifestChangedError)
    expect(isRetryable(error)).toBe(true)

    await repo.commit(commit)
    expect(client.transactions.at(-1)).toEqual(commitTransaction('events', commit, 2n))
  })

  test('a failed marker check on an unsupported declaration refuses', async () => {
    const client = new Scripted(manifest(1n), cancelled('ConditionalCheckFailed', 'None', 'None'))
    client.declared = manifest(2n, [
      ['stream-heads', 1],
      ['journal', 1],
    ])
    const commit = commitOne(client)

    await expect(commit).rejects.toThrow(UnsupportedFeatureError)
  })

  test('a failed lock check is a rebuild in progress', async () => {
    const client = new Scripted(manifest(1n), cancelled('None', 'ConditionalCheckFailed', 'None'))
    const commit = commitOne(client)

    await expect(commit).rejects.toThrow(RebuildInProgressError)
  })

  test('a transaction conflict is transient, never a conflict', async () => {
    // On the rebuild-lock check, an item no command changes: nothing the commit read went stale.
    const client = new Scripted(manifest(1n), cancelled('None', 'TransactionConflict', 'None'))
    const error = await commitOne(client).catch((thrown: unknown) => thrown)

    expect(error).toBeInstanceOf(TransientError)
    expect(isConflict(error)).toBe(false)
    expect(isRetryable(error)).toBe(true)
  })

  test('a throttled transaction is transient', async () => {
    const client = new Scripted(manifest(1n), cancelled('None', 'None', 'ThrottlingError', 'None'))
    const error = await commitOne(client).catch((thrown: unknown) => thrown)

    expect(error).toBeInstanceOf(TransientError)
    expect(isRetryable(error)).toBe(true)
  })

  test('compaction checks the marker per batch, and validates again when it moved', async () => {
    const client = new Scripted(manifest(1n), cancelled('ConditionalCheckFailed', 'None'))
    client.declared = manifest(2n)
    client.snapshot = snapshotItem({
      entityType: 't',
      entityId: 'a',
      sequence: 1n,
      eventSequence: 150n,
      payload: new TextEncoder().encode('{}'),
    })
    client.keys = Array.from({length: 150}, (_, i) => ({pk: {S: 'a'}, sk: {N: String(i + 1)}}))

    expect(await new DynamoRepository(client, 'events').compactBelow('a', 150n)).toBe(150)

    // The first batch failed on version 1 and ran again on version 2; the second batch followed.
    const checks = client.transactions.map((items) => items[0]!.ConditionCheck)
    const versions = checks.map((check) => check?.ExpressionAttributeValues?.[':observed'])
    expect(versions).toEqual([{N: '1'}, {N: '2'}, {N: '2'}])
    expect(client.transactions.map((items) => items.length)).toEqual([100, 100, 52])
  })

  test('compaction refuses a history-dependent declaration made between batches', async () => {
    const client = new Scripted(manifest(1n), cancelled('ConditionalCheckFailed', 'None'))
    client.declared = markerItem({
      version: 2n,
      features: new Map([['stream-heads', 1]]),
      historyDependent: ['t'],
      retention: [],
    })
    client.snapshot = snapshotItem({
      entityType: 't',
      entityId: 'a',
      sequence: 1n,
      eventSequence: 2n,
      payload: new TextEncoder().encode('{}'),
    })
    client.keys = [{pk: {S: 'a'}, sk: {N: '1'}}]

    const compacting = new DynamoRepository(client, 'events').compactBelow('a', 2n)

    await expect(compacting).rejects.toThrow(HistoryDependentProjectionError)
  })

  test('any other failure is thrown as it is', async () => {
    const throttled = Object.assign(new Error('slow down'), {name: 'ThrottlingException'})
    const client = new Scripted(manifest(1n), throttled)
    const commit = commitOne(client)

    await expect(commit).rejects.toBe(throttled)
  })
})

// A client over a table of items, whose transactions answer `answers` in turn without writing.
class Table implements DynamoClient {
  readonly transactions: TransactItem[][] = []
  readonly items = new Map<string, Item>()
  readonly #answers: (Error | null)[]

  constructor(...answers: (Error | null)[]) {
    this.put(manifest(1n))
    this.#answers = answers
  }

  put(item: Item): void {
    this.items.set(key(item), item)
  }

  async getItem(input: {Key: Item}): Promise<{Item?: Item}> {
    const item = this.items.get(key(input.Key))

    return item ? {Item: item} : {}
  }

  async putItem(): Promise<unknown> {
    throw new Error('unexpected putItem')
  }

  // Only the stream queries the repository makes: `pk = :pk AND sk > :sk`.
  async query(input: {
    ExpressionAttributeValues: Item
    ScanIndexForward?: boolean
    Limit?: number
  }): Promise<Page> {
    const pk = (input.ExpressionAttributeValues[':pk'] as {S: string}).S
    const after = sortKey({sk: input.ExpressionAttributeValues[':sk']!})!
    const events = [...this.items.values()].filter(
      (item) => (item['pk'] as {S: string}).S === pk && sortKey(item)! > after,
    )
    events.sort((a, b) => (sortKey(a)! < sortKey(b)! ? -1 : 1))
    if (input.ScanIndexForward === false) {
      events.reverse()
    }

    return {Items: events.slice(0, input.Limit ?? events.length)}
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

function key(item: Item): string {
  return `${(item['pk'] as {S: string}).S} ${(item['sk'] as {N: string}).N}`
}

// The failures of a snapshot write's head guard, and of a commit's head update or event put.
function guardFailed(): Error {
  return cancelled('None', 'ConditionalCheckFailed', 'None')
}

function headFailed(): Error {
  return cancelled('None', 'None', 'ConditionalCheckFailed', 'None')
}

function eventFailed(): Error {
  return cancelled('None', 'None', 'None', 'ConditionalCheckFailed')
}

// The kind of each snapshot write's head guard.
function guards(client: Table): string[] {
  return client.transactions.map((transaction) => Object.keys(transaction[1]!)[0]!)
}

describe('retention', () => {
  // 1,000 s past the epoch, with a minute's retention: items expire at 1,060 s.
  const NOW = 1_000_000
  const TTL = 1060
  const options = {retention: {t: 60_000}, clock: () => NOW}

  test('stamps one ttl on a commit, and on a create its head', async () => {
    const client = new Table()
    const commit = {
      entityId: 'a',
      events: [event(1), event(2)],
      snapshot: {entityType: 't', sequence: 1n, payload: new TextEncoder().encode('{}')},
    }
    await new DynamoRepository(client, 'events', options).commit(commit)

    const want = commitTransaction('events', commit, 1n, {items: TTL, head: TTL})
    expect(client.transactions).toEqual([want])
    expect(want[2]?.Update?.ExpressionAttributeValues?.[':t']).toEqual({N: '1060'})
    expect(want[3]?.Put?.Item['ttl']).toEqual({N: '1060'})
    expect(want.at(-1)?.Put?.Item['ttl']).toEqual({N: '1060'})
  })

  test("an append's head never lowers the ttl the writer loaded", async () => {
    const client = new Table()
    client.put(headItem('a', 1n, 5000))
    client.put(eventItem(event(1), 5000))
    const repo = new DynamoRepository(client, 'events', options)
    await repo.events('a')

    const commit = {entityId: 'a', events: [event(2)]}
    await repo.commit(commit)

    expect(client.transactions).toEqual([
      commitTransaction('events', commit, 1n, {items: TTL, head: 5000}),
    ])
  })

  test('stamps nothing on a type without retention', async () => {
    const client = new Table()
    const commit = {entityId: 'a', events: [event(1)]}
    await new DynamoRepository(client, 'events', {retention: {u: 60_000, t: 0}}).commit(commit)

    expect(client.transactions).toEqual([commitTransaction('events', commit, 1n)])
  })

  test('takes a Map, and covers the ttl of a snapshot it wrote', async () => {
    let now = NOW + 600_000
    const client = new Table()
    const repo = new DynamoRepository(client, 'events', {
      retention: new Map([['t', 60_000]]),
      clock: () => now,
    })
    const snapshot = {
      entityType: 't',
      entityId: 'a',
      sequence: 1n,
      eventSequence: 1n,
      payload: new TextEncoder().encode('{}'),
    }
    await repo.putSnapshot(snapshot)

    // The clock stepped back: the head still covers the snapshot's later ttl.
    now = NOW
    const commit = {entityId: 'a', events: [event(2)]}
    await repo.commit(commit)

    expect(client.transactions[1]).toEqual(
      commitTransaction('events', commit, 1n, {items: TTL, head: TTL + 600}),
    )
  })

  test('writes no ttl for an expiry at the epoch', async () => {
    const client = new Table()
    const commit = {entityId: 'a', events: [event(1)]}
    const repo = new DynamoRepository(client, 'events', {retention: {t: 1}, clock: () => 0})
    await repo.commit(commit)

    expect(client.transactions).toEqual([commitTransaction('events', commit, 1n)])
  })

  test.each([-1, 0.5, Number.NaN])('refuses a retention of %s ms', (retention) => {
    const construct = () => new DynamoRepository(new Table(), 'events', {retention: {t: retention}})

    expect(construct).toThrow(EvtError)
  })

  describe('a snapshot write', () => {
    const snapshot = {
      entityType: 't',
      entityId: 'a',
      sequence: 1n,
      eventSequence: 6n,
      payload: new TextEncoder().encode('{}'),
    }
    test("refreshes the head's ttl in its guard", async () => {
      const client = new Table()
      await new DynamoRepository(client, 'events', options).putSnapshot(snapshot)

      expect(client.transactions).toEqual([
        snapshotTransaction('events', snapshot, 1n, {ttl: TTL, refresh: true}),
      ])
    })

    test("sends a plain check when the moved head's ttl covers the snapshot", async () => {
      const client = new Table(guardFailed())
      client.put(headItem('a', 7n, 2000))
      await new DynamoRepository(client, 'events', options).putSnapshot(snapshot)

      expect(guards(client)).toEqual(['Update', 'ConditionCheck'])
      expect(client.transactions[1]).toEqual(
        snapshotTransaction('events', snapshot, 1n, {ttl: TTL, refresh: false}),
      )
    })

    test("refreshes again when the moved head's ttl is lower", async () => {
      const client = new Table(guardFailed())
      client.put(headItem('a', 7n, 1000))
      await new DynamoRepository(client, 'events', options).putSnapshot(snapshot)

      expect(guards(client)).toEqual(['Update', 'Update'])
    })

    test('is transient once the head keeps moving', async () => {
      const client = new Table(...Array.from({length: 5}, guardFailed))
      client.put(headItem('a', 7n, 1000))
      const write = new DynamoRepository(client, 'events', options).putSnapshot(snapshot)

      await expect(write).rejects.toBeInstanceOf(TransientError)
      expect(client.transactions).toHaveLength(5)
    })

    test('is drift when the head is below the snapshot', async () => {
      const client = new Table(guardFailed())
      client.put(headItem('a', 5n, 1000))
      const write = new DynamoRepository(client, 'events', options).putSnapshot(snapshot)

      await expect(write).rejects.toBeInstanceOf(StreamHeadDriftError)
      expect(client.transactions).toHaveLength(1)
    })
  })

  describe('a failed commit', () => {
    test.each([
      {retained: true, want: ConflictError},
      {retained: false, want: StreamHeadDriftError},
    ])('with its head above its log (retained: $retained)', async ({retained, want}) => {
      const client = new Table(headFailed())
      client.put(headItem('a', 5n, 5000))
      client.put(eventItem(event(3), 5000))
      const repo = new DynamoRepository(client, 'events', retained ? options : {})
      const commit = repo.commit({entityId: 'a', events: [event(6)]})

      await expect(commit).rejects.toBeInstanceOf(want)
    })

    test.each([
      {ttl: 900, want: ConflictError},
      {ttl: 5000, want: StreamHeadDriftError},
    ])('with no head over an event that expires at $ttl', async ({ttl, want}) => {
      const client = new Table(headFailed())
      client.put(eventItem(event(1), ttl))
      const repo = new DynamoRepository(client, 'events', options)
      const commit = repo.commit({entityId: 'a', events: [event(2)]})

      await expect(commit).rejects.toBeInstanceOf(want)
    })

    test.each([
      {ttl: 1000, want: ConflictError},
      {ttl: 1001, want: StreamHeadDriftError},
    ])('whose head passed over an event that expires at $ttl', async ({ttl, want}) => {
      const client = new Table(eventFailed())
      client.put(eventItem(event(1), ttl))
      const repo = new DynamoRepository(client, 'events', options)
      const commit = repo.commit({entityId: 'a', events: [event(1)]})

      await expect(commit).rejects.toBeInstanceOf(want)
    })

    // The probe reads the remaining items when the head item is absent; the classification reads
    // them itself only when the probe didn't, as with a head item below the events.
    test.each([
      {head: 0, ttl: 1000, want: ConflictError},
      {head: 0, ttl: 5000, want: StreamHeadDriftError},
      {head: 1, ttl: 1000, want: ConflictError},
      {head: 1, ttl: 5000, want: StreamHeadDriftError},
    ])(
      'whose head passed reads the remaining items once (head: $head, ttl: $ttl)',
      async ({head, ttl, want}) => {
        const client = new ExpiryReads(eventFailed())
        if (head > 0) {
          client.put(headItem('a', BigInt(head), ttl))
        }
        client.put(eventItem(event(1), ttl))
        client.put(eventItem(event(2), ttl))
        const repo = new DynamoRepository(client, 'events', options)
        const commit = repo.commit({entityId: 'a', events: [event(head + 1)]})

        await expect(commit).rejects.toBeInstanceOf(want)
        expect(client.snapshotReads).toBe(1)
        expect(client.eventQueries).toBe(1)
      },
    )
  })
})

// A table that counts the reads of a stream's remaining items: the snapshot GetItem (sk=0) and
// the event Query that project its ttl.
class ExpiryReads extends Table {
  snapshotReads = 0
  eventQueries = 0

  override async getItem(input: {
    Key: Item
    ExpressionAttributeNames?: Record<string, string>
  }): Promise<{Item?: Item}> {
    if (sortKey(input.Key) === 0n && input.ExpressionAttributeNames?.['#ttl'] === 'ttl') {
      this.snapshotReads++
    }

    return super.getItem(input)
  }

  override async query(input: {
    ExpressionAttributeValues: Item
    ExpressionAttributeNames?: Record<string, string>
    ScanIndexForward?: boolean
    Limit?: number
  }): Promise<Page> {
    if (input.ExpressionAttributeNames?.['#ttl'] === 'ttl') {
      this.eventQueries++
    }

    return super.query(input)
  }
}

describe('projector operations', () => {
  const view = viewProjector<unknown>('views').project({
    entityType: 't',
    entityId: 'a',
    state: null,
    encodedState: new TextEncoder().encode('{}'),
    events: [],
  })!
  const commit = {entityId: 'a', events: [event(1)], projections: [view]}

  test('follow the control checks, before the stream', async () => {
    const client = new Table()
    await new DynamoRepository(client, 'events').commit(commit)

    const [transaction] = client.transactions
    expect(transaction).toHaveLength(5)
    expect(transaction?.[2]).toEqual(view.operations[0])
    expect(transaction?.[3]?.Update?.Key).toEqual({pk: {S: 'a'}, sk: {N: '-1'}})
  })

  test("shift the stream's items when a failure is classified", async () => {
    // The head update, after the view put, failed on a stream with no head: a conflict. Read
    // without the shift, it would be a passing head update and a failed event put, which is drift.
    const client = new Table(cancelled('None', 'None', 'None', 'ConditionalCheckFailed', 'None'))
    const write = new DynamoRepository(client, 'events').commit(commit)

    await expect(write).rejects.toBeInstanceOf(ConflictError)
  })

  test('for another backend are refused before any request', async () => {
    const refusing: DynamoClient = {
      getItem: () => Promise.reject(new Error('unexpected getItem')),
      putItem: () => Promise.reject(new Error('unexpected putItem')),
      query: () => Promise.reject(new Error('unexpected query')),
      scan: () => Promise.reject(new Error('unexpected scan')),
      transactWriteItems: () => Promise.reject(new Error('unexpected transactWriteItems')),
    }
    const repo = new DynamoRepository(refusing, 'events', {lazyInitialization: true})
    const other = {backend: 'test', operations: [1], sizes: [1]}
    const write = repo.commit({...commit, projections: [other]})

    await expect(write).rejects.toMatchObject({code: 'invalidCommit'})
  })

  test('are budgeted by the actions sent, not the sizes their group reports', async () => {
    const large = viewProjector<unknown>('views').project({
      entityType: 't',
      entityId: 'a',
      state: null,
      encodedState: new TextEncoder().encode('x'.repeat(390_000)),
      events: [],
    })!
    const client = new Table()
    const projections = [{...large, sizes: [0]}]
    const write = new DynamoRepository(client, 'events').commit({...commit, projections})

    await expect(write).rejects.toBeInstanceOf(CommitTooLargeError)
    expect(client.transactions).toEqual([])
  })

  test('report a failure of their own as it is', async () => {
    const failure = cancelled('None', 'None', 'ConditionalCheckFailed', 'None', 'None')
    const write = new DynamoRepository(new Table(failure), 'events').commit(commit)

    await expect(write).rejects.toBe(failure)
  })
})

describe('the commit budget', () => {
  // The single-stream commits of feature-marker.json's streamHeads vectors, which the fixture
  // runner checks request by request; their sizes need the budget's internals.
  const fixture = join(dirname(fileURLToPath(import.meta.url)), '../../../compat/fixtures')
  const vectors = JSON.parse(readFileSync(join(fixture, 'feature-marker.json'), 'utf8'))
    .streamHeads as {name: string; request: Request; sizes?: number[]}[]

  interface Request {
    streams?: Stream[]
    fences?: {entityId: string; head: number}[]
    clockUnixNano?: number
    retentionNanos?: Record<string, number>
  }

  interface Stream {
    entityId: string
    entityType: string
    first: number
    count: number
    snapshotSequence?: number
  }

  const singleStream = vectors.filter(
    (vector) => vector.request.streams?.length === 1 && vector.sizes !== undefined,
  )

  test('covers the single-stream commits', () => {
    expect(singleStream.map((vector) => vector.name)).toEqual([
      'create',
      'append',
      'create-with-snapshot',
      'append-with-later-snapshot',
      'retained-create',
      'retained-append',
    ])
  })

  function commitOf(stream: Stream): Commit {
    const events: SerializedEvent[] = []
    for (let i = 0; i < stream.count; i++) {
      const sequence = BigInt(stream.first + i)
      events.push({
        id: eventId(stream.entityId, sequence),
        sequence,
        type: 'account:deposited',
        version: 1,
        entityId: stream.entityId,
        entityType: stream.entityType,
        payload: new TextEncoder().encode('{"amountCents":1}'),
        metadata: {
          ...emptyMetadata(),
          region: 'us-west-2',
          timestamp: '2026-10-12T00:00:00Z',
          commitId: 'compat-head-vector',
        },
      })
    }
    const snapshot =
      stream.snapshotSequence === undefined
        ? null
        : {
            entityType: stream.entityType,
            sequence: BigInt(stream.snapshotSequence),
            payload: new TextEncoder().encode('{}'),
          }

    return {entityId: stream.entityId, events, snapshot}
  }

  test.each(singleStream)('sizes $name as the vector does', ({request, sizes}) => {
    const stream = request.streams![0]!

    // The clock and the retention are whole milliseconds, exact as JSON numbers.
    const retention = request.retentionNanos?.[stream.entityType]
    const ttl =
      retention === undefined ? undefined : expiry(request.clockUnixNano! / 1e6, retention / 1e6)

    // Go lists the head update after the stream's puts, and the budget only sums them.
    const got = commitSizes(commitOf(stream), () => ttl)
    expect(ascending(got)).toEqual(ascending(sizes!))
  })

  test('sizes a transaction with fences as the vector does', () => {
    const vector = vectors.find(({name}) => name === 'streams-and-fences')!
    const {streams = [], fences = []} = vector.request
    const transaction = normalizeTransaction({
      streams: streams.map(commitOf),
      fences: fences.map(({entityId, head}) => ({
        entityId,
        entityType: 'account',
        head: BigInt(head),
        provenance: 'engine' as const,
      })),
    })

    const got = transactionSizes(transaction, () => undefined)
    expect(ascending(got)).toEqual(ascending(vector.sizes!))
  })
})

// A sorted copy: ES2022 has no toSorted.
function ascending(sizes: number[]): number[] {
  const sorted = [...sizes]
  sorted.sort((a, b) => a - b)

  return sorted
}
