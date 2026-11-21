// feature-marker.json: the declarations (the manifest's stored JSON texts, its DynamoDB item and
// its PostgreSQL row and write, for each declaration of a chain), the commits a writer sends
// against a declared and a fresh marker, and the streamHeads and commitViews requests of the
// DynamoDB writer. The postgresSchemas DDL is checked byte for byte, so every language creates the
// same schema.
import {
  type Commit,
  type FeatureManifest,
  type JsonValue,
  type SerializedEvent,
  type SerializedSnapshot,
  decodeMetadata,
  emptyMetadata,
  encodeManifest,
  eventId,
  normalizeTransaction,
  STREAM_HEADS_FEATURE,
  STREAM_HEADS_VERSION,
  viewOperationSize,
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
  transactItemSize,
  viewProjector,
} from '@photon-grove/evt/dynamodb'
import {manifestRow, manifestWrite, pgTables, schemaDdl} from '@photon-grove/evt/postgres'

import {milliseconds} from './codec.js'
import {stepViews} from './runner.js'
import {type Checks, canonical, field, int, list, loadFixture, num, plain, str} from './harness.js'

export async function markerVectors(checks: Checks): Promise<void> {
  const declarations = list(field(await loadFixture('feature-marker.json'), 'declarations'))
  checks.equal('feature-marker declarations present', declarations.length > 0, true)

  // The vectors are one chain of declarations: each declares over the one before.
  let stored: FeatureManifest | null = null
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

      // The PostgreSQL row, and the statement that writes it over the chain's previous one, as
      // Go's repository runs it: the SQL with its whitespace, casts and RETURNING aside.
      const row = field(output, 'postgresRow')
      checks.equal(
        `${name} postgresRow`,
        canonical(manifestRow(manifest)),
        canonical({
          id: num(field(row, 'id')),
          version: int(field(row, 'version')).toString(),
          features: str(field(row, 'features')),
          history_dependent: str(field(row, 'history_dependent')),
          retention: str(field(row, 'retention')),
        }),
      )
      const write = manifestWrite(pgTables(), stored, manifest)
      const wantWrite = field(output, 'postgresWrite')
      checks.equal(
        `${name} postgresWrite`,
        canonical({sql: statementShape(write.text), args: write.values}),
        canonical({
          sql: statementShape(str(field(wantWrite, 'sql'))),
          args: list(field(wantWrite, 'args')).map((arg) =>
            typeof arg === 'string' ? arg : int(arg).toString(),
          ),
        }),
      )
      stored = manifest
    })
  }
}

// A statement as Go's and this package's writers both shape it: whitespace collapsed, and without
// the casts and the RETURNING clause this package adds so that no driver parses a value.
function statementShape(sql: string): string {
  return sql
    .replaceAll('::bigint', '')
    .replace(/ RETURNING 1$/, '')
    .replace(/\s+/g, ' ')
    .trim()
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

// The commitViews vectors: a transaction's view operations go to the views table after the
// control checks, and before the written streams and the fences.
export async function commitViewVectors(checks: Checks): Promise<void> {
  const vectors = list(field(await loadFixture('feature-marker.json'), 'commitViews'))
  checks.equal('feature-marker commitViews present', vectors.length > 0, true)

  for (const vector of vectors) {
    const name = `feature-marker commitViews ${str(field(vector, 'name'))}`
    checks.run(name, () => {
      const request = field(vector, 'request')
      const fences = request instanceof Map ? request.get('fences') : undefined
      const views = stepViews('', request)
      const transaction = normalizeTransaction({
        streams: list(field(request, 'streams')).map(headCommit),
        fences: (fences === undefined ? [] : list(fences)).map((fence) => ({
          entityId: str(field(fence, 'entityId')),
          entityType: 'account',
          head: int(field(fence, 'head')),
          provenance: 'engine' as const,
        })),
        views,
      })
      const viewsTable = str(field(request, 'viewsTable'))
      const items = transactionItems(
        str(field(request, 'eventsTable')),
        transaction,
        1n,
        () => undefined,
        viewsTable,
      )
      checks.equal(
        name,
        canonical(items.map(normalize)),
        canonical(list(field(vector, 'output')).map(plain)),
      )

      // Go lists the budgeted sizes as the control checks' fixed ones, the streams' and fences' in
      // its own order, then the views' in theirs: the streams' and fences' are compared sorted.
      const want = list(field(vector, 'sizes')).map(num)
      const viewSizes = views.map((view) => viewOperationSize(view.op))
      const others = items
        .slice(2)
        .filter((item) => (item.Put ?? item.Delete)?.TableName !== viewsTable)
        .map(transactItemSize)
      checks.equal(`${name} control sizes`, canonical(want.slice(0, 2)), canonical([20, 19]))
      checks.equal(
        `${name} sizes`,
        canonical(ascending(others)),
        canonical(ascending(want.slice(2, 2 + others.length))),
      )
      checks.equal(
        `${name} view sizes`,
        canonical(viewSizes),
        canonical(want.slice(2 + others.length)),
      )
    })
  }
}

// Sizes in ascending order, in a copy.
function ascending(sizes: readonly number[]): number[] {
  const sorted = [...sizes]
  sorted.sort((a, b) => a - b)

  return sorted
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

// The commits vectors: a writer's commit with the per-entity view projector's row, against a store
// whose manifest a declaration of the chain left, or a fresh one the writer declares stream-heads
// on before its first commit, whose marker check then conditions on that version.
export async function commitVectors(checks: Checks): Promise<void> {
  const fixture = await loadFixture('feature-marker.json')
  const versions = new Map(
    list(field(fixture, 'declarations')).map((vector) => [
      str(field(vector, 'name')),
      int(field(field(field(vector, 'output'), 'postgresRow'), 'version')),
    ]),
  )
  const vectors = list(field(fixture, 'commits'))
  checks.equal('feature-marker commits present', vectors.length > 0, true)

  for (const vector of vectors) {
    const name = `feature-marker commit ${str(field(vector, 'name'))}`
    checks.run(name, () => {
      const input = field(vector, 'input')
      const declaration = str(field(input, 'markerDeclaration'))
      let version = versions.get(declaration)
      if (declaration === '') {
        const declared: FeatureManifest = {
          version: 1n,
          features: new Map([[STREAM_HEADS_FEATURE, STREAM_HEADS_VERSION]]),
          historyDependent: [],
          retention: [],
        }
        checks.equal(
          `${name} declared`,
          canonical(markerItem(declared)),
          canonical(plain(field(vector, 'declared'))),
        )
        version = declared.version
      }
      if (version === undefined) {
        throw new Error(`no declaration ${declaration}`)
      }

      const raw = field(input, 'event')
      const entityId = str(field(raw, 'entityId'))
      const entityType = str(field(raw, 'entityType'))
      const event: SerializedEvent = {
        id: str(field(raw, 'id')),
        sequence: int(field(raw, 'sequence')),
        type: str(field(raw, 'type')),
        version: num(field(raw, 'version')),
        entityId,
        entityType,
        payload: encoder.encode(str(field(raw, 'payload'))),
        metadata: decodeMetadata(str(field(raw, 'metadataJson'))),
      }
      const group = viewProjector(str(field(input, 'viewsTable'))).project({
        entityType,
        entityId,
        state: null,
        encodedState: encoder.encode(str(field(input, 'viewState'))),
        events: [event],
      })
      const snapshot = field(input, 'snapshot')
      const commit: Commit = {
        entityId,
        events: [event],
        projections: group === null ? [] : [group],
        ...(snapshot !== null && {
          snapshot: {
            entityType: str(field(snapshot, 'entityType')),
            sequence: int(field(snapshot, 'sequence')),
            payload: encoder.encode(str(field(snapshot, 'payload'))),
          },
        }),
      }
      const got = commitTransaction(str(field(input, 'eventsTable')), commit, version)
      checks.equal(
        name,
        canonical(got.map(normalize)),
        canonical(list(field(vector, 'output')).map(plain)),
      )
    })
  }
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
