// Runs the conformance suites against DynamoDB: a local emulator such as Moto at
// AWS_ENDPOINT_URL, never real AWS. Each run creates its own tables and deletes them at the end,
// so it can share the emulator with the Go and Rust suites. Each step needs the one before it, so
// the steps await in loops on purpose.
/* oxlint-disable no-await-in-loop */
import {DynamoDB} from '@aws-sdk/client-dynamodb'
import process from 'node:process'

import {
  ConflictError,
  type FeatureManifest,
  FeatureManifestChangedError,
  type SerializedEvent,
  StoreNotInitializedError,
  StreamHeadDriftError,
  StreamHeadsNotMigratedError,
  emptyMetadata,
  eventId,
} from '@photon-grove/evt'
import {
  DynamoRepository,
  type Item,
  eventItem,
  headFromItem,
  manifestFromItem,
  markerItem,
  ttlFromItem,
} from '@photon-grove/evt/dynamodb'

import {
  type ControlFixtures,
  type HeadFixtures,
  type ManifestDraft,
  type RebuildState,
  conformance,
  markerConformance,
} from './conformance.js'
import {Checks} from './harness.js'

// Only a local emulator: a loopback address or a .localhost name.
const endpoint = process.env['AWS_ENDPOINT_URL'] ?? ''
const host = URL.canParse(endpoint) ? new URL(endpoint).hostname : ''
const local = ['localhost', '127.0.0.1', '[::1]'].includes(host) || host.endsWith('.localhost')
if (!local) {
  throw new Error(`AWS_ENDPOINT_URL must name a local DynamoDB emulator, such as Moto: ${endpoint}`)
}

const client = new DynamoDB({
  endpoint,
  region: process.env['AWS_REGION'] ?? 'us-west-2',
  credentials: {
    accessKeyId: process.env['AWS_ACCESS_KEY_ID'] ?? 'test',
    secretAccessKey: process.env['AWS_SECRET_ACCESS_KEY'] ?? 'test',
  },
})

const run = Date.now().toString(36)
const shared = `evt-ts-conformance-${run}`
const marked = `evt-ts-marker-${run}`
const tableCount = {value: 0}

const checks = new Checks()
const created: string[] = []
try {
  for (const table of [shared, marked]) {
    await createTable(table)
    created.push(table)
  }

  // The suite's store is initialized up front, as a deployment would, so its writers are strict.
  const repo = new DynamoRepository(client, shared)
  await repo.initialize()
  await conformance(checks, repo, headFixtures(shared))

  // The marker cases declare store-wide, so they get a table of their own, and they take
  // stream-heads away, which only a lazily initializing writer reports as unmigrated.
  const lazy = new DynamoRepository(client, marked, {lazyInitialization: true})
  await markerConformance(checks, lazy, controlFixtures(marked))

  await initialization(created)
  await retention(created)
} finally {
  await Promise.all(created.map(deleteTable))
}

checks.report()

// A fresh table for one case, deleted with the others at the end of the run.
async function freshTable(tables: string[]): Promise<string> {
  const table = `evt-ts-case-${run}-${++tableCount.value}`
  await createTable(table)
  tables.push(table)

  return table
}

// What DynamoDB alone does: refuse an uninitialized store, initialize one, and revalidate after
// another writer declares. Each case gets a fresh table.
async function initialization(tables: string[]): Promise<void> {
  const fresh = (): Promise<string> => freshTable(tables)

  await checks.case('dynamodb: a strict writer refuses an uninitialized store', async () => {
    const repo = new DynamoRepository(client, await fresh())
    const writes: [string, () => Promise<unknown>][] = [
      ['a commit', () => repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})],
      ['a snapshot write', () => repo.putSnapshot(snapshot('a', 0n))],
      ['compaction', () => repo.compactBelow('a', 1n)],
    ]
    for (const [what, write] of writes) {
      await refuses(write(), StoreNotInitializedError, what)
    }
    ensure((await repo.events('a')).length === 0, 'the refused writes wrote nothing')
  })

  await checks.case('dynamodb: initialize declares once, across concurrent calls', async () => {
    const table = await fresh()
    const repos = [0, 1, 2].map(() => new DynamoRepository(client, table))
    const declared = await Promise.all(repos.map((repo) => repo.initialize()))
    ensure(declared.filter(Boolean).length === 1, `one call declares: got ${declared}`)
    ensure(!(await repos[0]!.initialize()), 'initializing again declares nothing')
    ensure((await manifestOf(table))?.version === 1n, 'the manifest version bumps once')
    await repos[1]!.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})
  })

  await checks.case('dynamodb: a lazy writer declares stream-heads first', async () => {
    const table = await fresh()
    const repo = new DynamoRepository(client, table, {lazyInitialization: true})
    await repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})
    const manifest = await manifestOf(table)
    ensure(manifest?.version === 1n, 'the first write declares version 1')
    ensure(manifest.features.get('stream-heads') === 1, 'it declares stream-heads')
  })

  await checks.case('dynamodb: a store with a headless stream is not fresh', async () => {
    const table = await fresh()
    await client.putItem({TableName: table, Item: eventItem(ledgerEvent('a', 1))})
    const lazy = new DynamoRepository(client, table, {lazyInitialization: true})
    await refuses(lazy.initialize(), StreamHeadsNotMigratedError, 'initializing')
    await refuses(
      lazy.commit({entityId: 'b', events: [ledgerEvent('b', 1)]}),
      StreamHeadsNotMigratedError,
      'a lazy commit',
    )
    ensure((await manifestOf(table)) === null, 'nothing was declared')
  })

  await checks.case('dynamodb: a declaration fails a cached writer once', async () => {
    const table = await fresh()
    const repo = new DynamoRepository(client, table)
    await repo.initialize()
    await repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})

    await controlFixtures(table).declare((draft) => draft.retention.push('session'))
    await refuses(
      repo.commit({entityId: 'a', events: [ledgerEvent('a', 2)]}),
      FeatureManifestChangedError,
      'the first commit after the declaration',
    )
    await repo.commit({entityId: 'a', events: [ledgerEvent('a', 2)]})
    ensure((await repo.events('a')).length === 2, 'the retried commit wrote')
  })
}

// Retention: the ttl every item of a retained stream carries, the head's never-lower rule, and a
// stream whose items TTL deleted out of order. Moto stores ttl attributes but never deletes them,
// so the deletions are written by hand. Each case gets a fresh, initialized table.
async function retention(tables: string[]): Promise<void> {
  // 2027-01-15, and the ttl of an item written then and kept for a minute or an hour.
  let now = 1_800_000_000_000
  const clock = (): number => now
  const minute = {retention: {ledger: 60_000}, clock}
  const hour = {retention: {ledger: 3_600_000}, clock}
  const fresh = async (): Promise<string> => {
    const table = await freshTable(tables)
    await new DynamoRepository(client, table).initialize()

    return table
  }

  await checks.case('dynamodb: a retained commit stamps its ttl on every item', async () => {
    const table = await fresh()
    const repo = new DynamoRepository(client, table, minute)
    const payload = new TextEncoder().encode('{}')
    await repo.commit({
      entityId: 'a',
      events: [ledgerEvent('a', 1), ledgerEvent('a', 2)],
      snapshot: {entityType: 'ledger', sequence: 1n, payload},
    })

    for (const sk of [-1n, 0n, 1n, 2n]) {
      const ttl = await ttlOf(table, sk)
      ensure(ttl === 1_800_000_060, `item ${sk} expires at 1800000060: got ${ttl}`)
    }
  })

  await checks.case('dynamodb: a retained head never lowers its ttl', async () => {
    const table = await fresh()
    await new DynamoRepository(client, table, hour).commit({
      entityId: 'a',
      events: [ledgerEvent('a', 1)],
    })

    // A writer with a shorter retention that hasn't loaded the stream conflicts, rather than
    // lower the head's ttl. One that has loaded its events covers the ttl they carry.
    const append = {entityId: 'a', events: [ledgerEvent('a', 2)]}
    const unloaded = new DynamoRepository(client, table, minute)
    await refuses(unloaded.commit(append), ConflictError, 'an append that would lower the ttl')
    const loaded = new DynamoRepository(client, table, minute)
    await loaded.events('a')
    await loaded.commit(append)

    ensure((await ttlOf(table, -1n)) === 1_800_003_600, "the head keeps the hour's ttl")
    ensure((await ttlOf(table, 2n)) === 1_800_000_060, "the event carries the minute's")
  })

  await checks.case('dynamodb: a retained snapshot write refreshes its head', async () => {
    const table = await fresh()
    const repo = new DynamoRepository(client, table, minute)
    await repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})

    now += 600_000
    ensure(await repo.putSnapshot(snapshot('a', 1n)), 'the snapshot is written')
    ensure((await ttlOf(table, 0n)) === 1_800_000_660, 'the snapshot carries its own ttl')
    ensure((await ttlOf(table, -1n)) === 1_800_000_660, "the head's ttl covers it")
    now -= 600_000
  })

  await checks.case('dynamodb: a retained stream TTL emptied out of order conflicts', async () => {
    // TTL deleted the head first, leaving an event that has expired too: a create passes its head
    // condition and fails on the event, a conflict until TTL deletes it. Without retention, or
    // with the event still live, it's drift.
    const cases = [
      [minute, 1_799_999_000, ConflictError],
      [minute, 1_800_009_000, StreamHeadDriftError],
      [{}, 1_799_999_000, StreamHeadDriftError],
    ] as const
    for (const [options, ttl, want] of cases) {
      const table = await fresh()
      await client.putItem({TableName: table, Item: eventItem(ledgerEvent('a', 1), ttl)})
      const repo = new DynamoRepository(client, table, options)
      const commit = repo.commit({entityId: 'a', events: [ledgerEvent('a', 1)]})
      await refuses(commit, want, `a create over an item expiring at ${ttl}`)
    }
  })
}

// The ttl of stream a's item at `sk`, read back.
async function ttlOf(table: string, sk: bigint): Promise<number | undefined> {
  const {Item: item} = await client.getItem({
    TableName: table,
    Key: key('a', sk),
    ConsistentRead: true,
  })

  return item ? ttlFromItem(item as Item) : undefined
}

function ledgerEvent(entityId: string, sequence: number): SerializedEvent {
  return {
    id: eventId(entityId, BigInt(sequence)),
    sequence: BigInt(sequence),
    type: 'ledger:added',
    version: 1,
    entityId,
    entityType: 'ledger',
    payload: new TextEncoder().encode('{}'),
    metadata: {...emptyMetadata(), timestamp: '2026-01-02T03:04:05Z'},
  }
}

function snapshot(entityId: string, eventSequence: bigint) {
  return {
    entityType: 'ledger',
    entityId,
    sequence: 1n,
    eventSequence,
    payload: new TextEncoder().encode('{}'),
  }
}

async function manifestOf(table: string): Promise<FeatureManifest | null> {
  return controlFixtures(table).manifest()
}

function ensure(condition: boolean, what: string): asserts condition {
  if (!condition) {
    throw new Error(what)
  }
}

// Fails unless `call` rejects with an error of `type`.
async function refuses(
  call: Promise<unknown>,
  type: abstract new (...args: never[]) => Error,
  what: string,
): Promise<void> {
  try {
    await call
  } catch (error) {
    ensure(error instanceof type, `${what}: got ${String(error)}`)

    return
  }

  throw new Error(`${what}: resolved, want a ${type.name}`)
}

async function createTable(name: string): Promise<void> {
  await client.createTable({
    TableName: name,
    BillingMode: 'PAY_PER_REQUEST',
    AttributeDefinitions: [
      {AttributeName: 'pk', AttributeType: 'S'},
      {AttributeName: 'sk', AttributeType: 'N'},
    ],
    KeySchema: [
      {AttributeName: 'pk', KeyType: 'HASH'},
      {AttributeName: 'sk', KeyType: 'RANGE'},
    ],
  })
}

async function deleteTable(name: string): Promise<void> {
  await client.deleteTable({TableName: name})
}

function key(pk: string, sk: bigint): Item {
  return {pk: {S: pk}, sk: {N: sk.toString()}}
}

// Writes the rows no repository call can, straight to the table.
function headFixtures(table: string): HeadFixtures {
  return {
    async appendBypassingHead(event: SerializedEvent) {
      await client.putItem({TableName: table, Item: eventItem(event)})
    },
    async deleteHead(entityId: string) {
      await client.deleteItem({TableName: table, Key: key(entityId, -1n)})
    },
    async storedHead(entityId: string) {
      const {Item: item} = await client.getItem({
        TableName: table,
        Key: key(entityId, -1n),
        ConsistentRead: true,
      })

      return item ? headFromItem(item as Item).head : null
    },
  }
}

function controlFixtures(table: string): ControlFixtures {
  const manifest = async (): Promise<FeatureManifest | null> => {
    const {Item: item} = await client.getItem({
      TableName: table,
      Key: key('SYSTEM#FEATURES', 0n),
      ConsistentRead: true,
    })

    return item ? manifestFromItem(item as Item) : null
  }

  return {
    manifest,
    async declare(change: (draft: ManifestDraft) => void) {
      const stored = await manifest()
      const draft: ManifestDraft = {
        features: new Map(stored?.features),
        historyDependent: [...(stored?.historyDependent ?? [])],
        retention: [...(stored?.retention ?? [])],
      }
      change(draft)

      const next = {...draft, version: (stored?.version ?? 0n) + 1n}
      await client.putItem({
        TableName: table,
        Item: markerItem(next),
        ...(stored
          ? {
              ConditionExpression: 'version = :observed',
              ExpressionAttributeValues: {':observed': {N: stored.version.toString()}},
            }
          : {ConditionExpression: 'attribute_not_exists(pk)'}),
      })

      // Read back as stored: each set sorted and deduplicated.
      return (await manifest())!
    },
    async setRebuild(state: RebuildState) {
      await client.putItem({
        TableName: table,
        Item: {...key('SYSTEM#REBUILD#GENERATION', 0n), value: {N: state.generation.toString()}},
      })

      const lock = key('SYSTEM#REBUILD', 0n)
      if (state.lock === null) {
        await client.deleteItem({TableName: table, Key: lock})
      } else if (state.lock === 'held') {
        await client.putItem({
          TableName: table,
          Item: {
            ...lock,
            owner: {S: 'conformance'},
            generation: {N: state.generation.toString()},
            leaseExpiresAt: {N: String(Math.floor(Date.now() / 1000) + 300)},
          },
        })
      } else {
        await client.putItem({TableName: table, Item: {...lock, forceReleased: {BOOL: true}}})
      }
    },
  }
}
