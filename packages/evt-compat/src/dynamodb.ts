// Runs the conformance suites against DynamoDB: a local emulator such as Moto at
// AWS_ENDPOINT_URL, never real AWS. Each run creates its own tables and deletes them at the end,
// so it can share the emulator with the Go and Rust suites. Each step needs the one before it, so
// the steps await in loops on purpose.
/* oxlint-disable no-await-in-loop */
import {DynamoDB} from '@aws-sdk/client-dynamodb'
import process from 'node:process'

import {
  type FeatureManifest,
  FeatureManifestChangedError,
  type SerializedEvent,
  StoreNotInitializedError,
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
} finally {
  await Promise.all(created.map(deleteTable))
}

checks.report()

// What DynamoDB alone does: refuse an uninitialized store, initialize one, and revalidate after
// another writer declares. Each case gets a fresh table.
async function initialization(tables: string[]): Promise<void> {
  let count = 0
  const fresh = async (): Promise<string> => {
    const table = `evt-ts-init-${run}-${++count}`
    await createTable(table)
    tables.push(table)

    return table
  }

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
