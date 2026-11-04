// DynamoRepository against a scripted client: the calls it makes and how it classifies a failed
// one. The live behavior runs in packages/evt-compat against Moto.
import {DynamoDB} from '@aws-sdk/client-dynamodb'
import {readFileSync} from 'node:fs'
import {dirname, join} from 'node:path'
import {fileURLToPath} from 'node:url'
import {describe, expect, test} from 'vitest'

import type {DynamoClient, Page, TransactItem} from '../src/dynamodb/client.ts'
import {type Item, snapshotItem} from '../src/dynamodb/item.ts'
import {markerItem} from '../src/dynamodb/marker.ts'
import {DynamoRepository} from '../src/dynamodb/repository.ts'
import {commitTransaction} from '../src/dynamodb/requests.ts'
import {
  ConflictError,
  FeatureManifestChangedError,
  HistoryDependentProjectionError,
  RebuildInProgressError,
  StoreNotInitializedError,
  TransientError,
  UnsupportedFeatureError,
  isRetryable,
} from '../src/errors.ts'
import {type SerializedEvent, eventId} from '../src/event.ts'
import {emptyMetadata} from '../src/metadata.ts'
import {commitSizes} from '../src/repository.ts'

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

  test('a transaction conflict is a conflict', async () => {
    const client = new Scripted(manifest(1n), cancelled('None', 'None', 'TransactionConflict'))
    const commit = commitOne(client)

    await expect(commit).rejects.toThrow(ConflictError)
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

describe('the commit budget', () => {
  // The single-stream commits of feature-marker.json's streamHeads vectors, which the fixture
  // runner checks request by request; their sizes need the budget's internals.
  const fixture = join(dirname(fileURLToPath(import.meta.url)), '../../../compat/fixtures')
  const vectors = JSON.parse(readFileSync(join(fixture, 'feature-marker.json'), 'utf8'))
    .streamHeads as {name: string; request: {streams?: Stream[]}; sizes?: number[]}[]

  interface Stream {
    entityId: string
    entityType: string
    first: number
    count: number
    snapshotSequence?: number
  }

  const singleStream = vectors.filter(
    (vector) =>
      vector.request.streams?.length === 1 &&
      vector.sizes !== undefined &&
      !('retentionNanos' in vector.request),
  )

  test('covers the single-stream commits', () => {
    expect(singleStream.map((vector) => vector.name)).toEqual([
      'create',
      'append',
      'create-with-snapshot',
      'append-with-later-snapshot',
    ])
  })

  test.each(singleStream)('sizes $name as the vector does', ({request, sizes}) => {
    const stream = request.streams![0]!
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

    // Go lists the head update after the stream's puts, and the budget only sums them.
    const got = commitSizes({entityId: stream.entityId, events, snapshot}, () => undefined)
    expect(ascending(got)).toEqual(ascending(sizes!))
  })
})

// A sorted copy: ES2022 has no toSorted.
function ascending(sizes: number[]): number[] {
  const sorted = [...sizes]
  sorted.sort((a, b) => a - b)

  return sorted
}
