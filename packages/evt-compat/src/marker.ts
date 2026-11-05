// feature-marker.json: the declarations (the manifest's stored JSON texts and its DynamoDB item,
// for each declaration of a chain) and the streamHeads requests of the DynamoDB writer. The view
// commits and the PostgreSQL rows and writes wait for the backends that write them. The
// postgresSchemas DDL is checked byte for byte, so every language creates the same schema.
import {
  type Commit,
  type FeatureManifest,
  type JsonValue,
  type SerializedEvent,
  type SerializedSnapshot,
  emptyMetadata,
  encodeManifest,
  eventId,
  normalizeTransaction,
} from '@photon-grove/evt'
import {
  type CommitTtl,
  type Item,
  type TransactItem,
  commitTransaction,
  expiry,
  manifestFromItem,
  markerItem,
  snapshotTransaction,
  transactionItems,
} from '@photon-grove/evt/dynamodb'
import {pgTables, schemaDdl} from '@photon-grove/evt/postgres'

import {milliseconds} from './codec.js'
import {type Checks, canonical, field, int, list, loadFixture, num, plain, str} from './harness.js'

export async function markerVectors(checks: Checks): Promise<void> {
  const declarations = list(field(await loadFixture('feature-marker.json'), 'declarations'))
  checks.equal('feature-marker declarations present', declarations.length > 0, true)

  for (const vector of declarations) {
    const name = `feature-marker declaration ${str(field(vector, 'name'))}`
    checks.run(name, () => {
      const input = field(vector, 'input')
      const output = field(vector, 'output')
      const features = field(input, 'features')
      if (!(features instanceof Map)) {
        throw new Error('the declaration has no features object')
      }

      // The PostgreSQL row records the version the chain reached, apart from the item.
      const manifest: FeatureManifest = {
        version: int(field(field(output, 'postgresRow'), 'version')),
        features: new Map(Array.from(features, ([key, value]) => [key, num(value)])),
        historyDependent: list(field(input, 'historyDependent')).map(str),
        retention: list(field(input, 'retention')).map(str),
      }

      const texts = encodeManifest(manifest)
      for (const [part, json] of Object.entries(texts)) {
        checks.equal(`${name} ${part}`, json, str(field(output, `${part}Json`)))
      }

      const item = plain(field(output, 'dynamoItem')) as Item
      const want = canonical(item)
      checks.equal(`${name} dynamoItem`, canonical(markerItem(manifest)), want)

      // Decoding the item gives back the manifest as stored: each set sorted and deduplicated.
      checks.equal(`${name} decoded`, canonical(markerItem(manifestFromItem(item))), want)
    })
  }
}

// The streamHeads vectors this package's writers produce: single-stream commits, a transaction
// that writes several streams and fences others, and snapshot writes, with and without retention.
const HEAD_VECTORS = new Set([
  'streams-and-fences',
  'create',
  'append',
  'create-with-snapshot',
  'append-with-later-snapshot',
  'retained-create',
  'retained-append',
  'put-snapshot',
  'put-snapshot-retained',
  'put-snapshot-empty-stream',
])

// The fixed parts of the events and snapshots the Go generator writes into these vectors.
const HEAD_EVENT_TYPE = 'account:deposited'
const HEAD_PAYLOAD = '{"amountCents":1}'
const HEAD_COMMIT_ID = 'compat-head-vector'
const HEAD_TABLE = 'evt-compat'

const encoder = new TextEncoder()

export async function headVectors(checks: Checks): Promise<void> {
  const vectors = list(field(await loadFixture('feature-marker.json'), 'streamHeads'))
  const covered = vectors.filter((vector) => HEAD_VECTORS.has(str(field(vector, 'name'))))
  checks.equal('feature-marker streamHeads covered', covered.length, HEAD_VECTORS.size)

  for (const vector of covered) {
    const name = `feature-marker streamHeads ${str(field(vector, 'name'))}`
    checks.run(name, () => {
      const request = field(vector, 'request')
      const want = canonical(list(field(vector, 'output')).map(plain))

      const put = request instanceof Map ? request.get('putSnapshot') : undefined
      if (put !== undefined) {
        const snapshot: SerializedSnapshot = {
          entityType: str(field(put, 'entityType')),
          entityId: str(field(put, 'entityId')),
          sequence: int(field(put, 'snapshotSequence')),
          eventSequence: int(field(put, 'eventSequence')),
          payload: encoder.encode('{}'),
        }
        const ttl = vectorTtl(request, snapshot.entityType)
        const guarded = ttl === undefined ? undefined : {ttl, refresh: true}
        const got = snapshotTransaction(HEAD_TABLE, snapshot, 1n, guarded).map(normalize)
        checks.equal(name, canonical(got), want)

        return
      }

      const streams = list(field(request, 'streams'))
      const fences = request instanceof Map ? request.get('fences') : undefined
      if (streams.length !== 1 || fences !== undefined) {
        // A transaction: the streams and fences in the order the request lists them, which
        // normalizing sorts.
        const transaction = normalizeTransaction({
          streams: streams.map(headCommit),
          fences: (fences === undefined ? [] : list(fences)).map((fence) => ({
            entityId: str(field(fence, 'entityId')),
            entityType: 'account',
            head: int(field(fence, 'head')),
            provenance: 'engine' as const,
          })),
        })
        const items = transactionItems(HEAD_TABLE, transaction, 1n, (commit) =>
          commitTtl(request, commit),
        )
        checks.equal(name, canonical(items.map(normalize)), want)

        return
      }

      // The vectors' sizes are the commit budget's, which packages/evt's tests check.
      const commit = headCommit(streams[0]!)
      const ttl = commitTtl(request, commit)
      const got = commitTransaction(HEAD_TABLE, commit, 1n, ttl).map(normalize)
      checks.equal(name, canonical(got), want)
    })
  }
}

// The `ttl` an item of the entity type gets from the vector's clock and retention, if it has one.
function vectorTtl(request: JsonValue, entityType: string): number | undefined {
  const retention = request instanceof Map ? request.get('retentionNanos') : undefined
  const nanos = retention instanceof Map ? retention.get(entityType) : undefined
  if (nanos === undefined) {
    return undefined
  }

  const clockMs = milliseconds(int(field(request, 'clockUnixNano')))

  return expiry(clockMs, milliseconds(int(nanos)))
}

// A retained commit's `ttl`s. An append's head covers the `ttl` the writer loaded, which the
// vector seeds on the stream's head item.
function commitTtl(request: JsonValue, commit: Commit): CommitTtl | undefined {
  const ttl = vectorTtl(request, commit.events[0]!.entityType)
  if (ttl === undefined) {
    return undefined
  }
  if (commit.events[0]!.sequence <= 1n) {
    return {items: ttl, head: ttl}
  }

  const seeded = request instanceof Map ? request.get('seeded') : undefined
  const loaded = (seeded === undefined ? [] : list(seeded))
    .filter((stream) => str(field(stream, 'entityId')) === commit.entityId)
    .map((stream) => num(field(stream, 'ttl')))

  return {items: ttl, head: Math.max(ttl, ...loaded)}
}

function headCommit(stream: JsonValue): Commit {
  const entityId = str(field(stream, 'entityId'))
  const entityType = str(field(stream, 'entityType'))
  const first = int(field(stream, 'first'))
  const count = num(field(stream, 'count'))

  const events: SerializedEvent[] = []
  for (let i = 0n; i < BigInt(count); i++) {
    events.push({
      id: eventId(entityId, first + i),
      sequence: first + i,
      type: HEAD_EVENT_TYPE,
      version: 1,
      entityId,
      entityType,
      payload: encoder.encode(HEAD_PAYLOAD),
      metadata: {
        ...emptyMetadata(),
        region: 'us-west-2',
        timestamp: '2026-10-12T00:00:00Z',
        commitId: HEAD_COMMIT_ID,
      },
    })
  }

  const snapshotSequence = stream instanceof Map ? stream.get('snapshotSequence') : undefined
  const snapshot = snapshotSequence === undefined
    ? null
    : {entityType, sequence: int(snapshotSequence), payload: encoder.encode('{}')}

  return {entityId, events, snapshot}
}

// An action in the vectors' shape: its kind and lower-camel fields.
function normalize(action: TransactItem): unknown {
  const [operation, body] = Object.entries(action)[0] as [string, Record<string, unknown>]
  const normalized: Record<string, unknown> = {operation}
  for (const [key, value] of Object.entries(body)) {
    normalized[key[0]!.toLowerCase() + key.slice(1)] = value
  }

  return normalized
}

export async function postgresSchemaVectors(checks: Checks): Promise<void> {
  const schemas = list(field(await loadFixture('feature-marker.json'), 'postgresSchemas'))
  checks.equal('postgres schemas present', schemas.length > 0, true)

  for (const vector of schemas) {
    const events = str(field(vector, 'eventsTable'))
    const snapshots = str(field(vector, 'snapshotsTable'))
    const name = `postgres schema ${events}/${snapshots}`
    checks.run(name, () => {
      checks.equal(name, schemaDdl(pgTables(events, snapshots)), str(field(vector, 'ddl')))
    })
  }
}
