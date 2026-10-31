// codec.json: the serialized event, metadata and snapshot JSON, the DynamoDB items, the stream
// envelope, TTL, the snapshot policy and the snapshot budget, checked as the Rust crate checks them
// in crates/evt-compat/tests/fixtures.rs.
import {
  type JsonValue,
  MAX_COMMIT_ITEM_BYTES,
  type SerializedEvent,
  type SerializedSnapshot,
  decodeEnvelope,
  decodeEvent,
  decodeMetadata,
  decodeSnapshot,
  encodeEnvelope,
  encodeEvent,
  encodeMetadata,
  encodeSnapshot,
  messageAttributes,
  parseJson,
  snapshotCommitSize,
  snapshotEventsDue,
  stringifyJson,
} from '@photon-grove/evt'
import {
  type Item,
  eventFromItem,
  eventItem,
  expiry,
  headItem,
  isSnapshotItem,
  snapshotFromItem,
  snapshotItem,
  viewItem,
} from '@photon-grove/evt/dynamodb'

import {type Checks, canonical, field, int, list, loadFixture, num, plain, str} from './harness.js'

// The source the Go generator stamps on every envelope.
const ORIGIN_SOURCE = 'evt-compat'

const encoder = new TextEncoder()

export async function codecVectors(checks: Checks): Promise<void> {
  const fixtures = await loadFixture('codec.json')

  // A section regenerated empty would otherwise pass without checking anything.
  for (const section of ['events', 'snapshots', 'metadataDecode', 'eventDecode', 'views']) {
    checks.equal(`codec ${section} present`, list(field(fixtures, section)).length > 0, true)
  }
  checks.equal('codec ttl streams', list(field(field(fixtures, 'ttl'), 'streams')).length, 3)

  for (const vector of list(field(fixtures, 'events'))) {
    const name = `codec event ${str(field(vector, 'name'))}`
    checks.run(name, () => eventVector(checks, name, vector))
  }
  for (const vector of list(field(fixtures, 'snapshots'))) {
    const name = `codec snapshot ${str(field(vector, 'name'))}`
    checks.run(name, () => snapshotVector(checks, name, vector))
  }
  for (const vector of list(field(fixtures, 'metadataDecode'))) {
    const name = `codec metadataDecode ${str(field(vector, 'name'))}`
    checks.run(name, () => {
      const metadata = decodeMetadata(str(field(vector, 'input')))
      checks.equal(name, encodeMetadata(metadata), str(field(vector, 'canonical')))
    })
  }
  for (const vector of list(field(fixtures, 'eventDecode'))) {
    const name = `codec eventDecode ${str(field(vector, 'name'))}`
    checks.run(name, () => {
      const event = decodeEvent(str(field(vector, 'input')))
      checks.equal(name, encodeEvent(event), str(field(vector, 'canonical')))
    })
  }

  checks.run('codec intKeyMap', () => {
    const keys = new Map([[-1, 'minus one'], [2, 'two'], [10, 'ten']])
    checks.equal('codec intKeyMap', stringifyJson(keys), str(field(fixtures, 'intKeyMap')))
  })
  checks.run('codec ttl', () => ttlVectors(checks, field(fixtures, 'ttl')))
  for (const vector of list(field(fixtures, 'views'))) {
    const name = `codec view ${str(field(vector, 'name'))}`
    checks.run(name, () => viewVector(checks, name, vector))
  }

  const policy = list(field(fixtures, 'snapshotPolicy'))
  checks.equal('codec snapshotPolicy present', policy.length > 0, true)
  for (const vector of policy) {
    const [current, events, interval, expected] = list(vector)
    const name = `codec snapshotPolicy ${stringifyJson(vector)}`
    checks.run(name, () => {
      const due = snapshotEventsDue(int(at(current)), num(at(events)), num(at(interval)))
      checks.equal(name, due, num(at(expected)))
    })
  }

  const budget = list(field(fixtures, 'snapshotBudget'))
  checks.equal('codec snapshotBudget has small, at-limit and over-limit', budget.length, 3)
  for (const vector of budget) {
    const name = `codec snapshotBudget ${str(field(vector, 'name'))}`
    checks.run(name, () => {
      const size = snapshotCommitSize({
        entityType: str(field(vector, 'entityType')),
        entityId: str(field(vector, 'entityId')),
        sequence: int(field(vector, 'sequence')),
        eventSequence: int(field(vector, 'eventSequence')),
        payload: encoder.encode('x'.repeat(num(field(vector, 'payloadBytes')))),
      })
      checks.equal(`${name}: size`, size, num(field(vector, 'size')))
      checks.equal(`${name}: fits`, size <= MAX_COMMIT_ITEM_BYTES, field(vector, 'fits'))
    })
  }
}

function eventVector(checks: Checks, name: string, vector: JsonValue): void {
  const input = field(vector, 'input')
  const metadataJson = str(field(input, 'metadataJson'))
  const metadata = decodeMetadata(metadataJson)
  checks.equal(`${name}: metadata`, encodeMetadata(metadata), metadataJson)

  const event: SerializedEvent = {
    id: str(field(input, 'id')),
    sequence: int(field(input, 'sequence')),
    type: str(field(input, 'type')),
    version: num(field(input, 'version')),
    entityId: str(field(input, 'entityId')),
    entityType: str(field(input, 'entityType')),
    payload: encoder.encode(str(field(input, 'payload'))),
    metadata,
  }
  const serialized = str(field(vector, 'serializedEventJson'))
  checks.equal(`${name}: serialized event`, encodeEvent(event), serialized)
  checks.equal(`${name}: decode serialized event`, encodeEvent(decodeEvent(serialized)), serialized)

  const goItem = field(vector, 'dynamoItem')
  checks.equal(`${name}: dynamo item`, canonical(eventItem(event)), canonical(goItem))
  checks.equal(`${name}: decode dynamo item`, encodeEvent(eventFromItem(item(goItem))), serialized)

  const message = str(field(vector, 'snsMessage'))
  checks.equal(`${name}: envelope`, encodeEnvelope(event, ORIGIN_SOURCE), message)
  checks.equal(`${name}: decode envelope`, encodeEvent(decodeEnvelope(message)), serialized)
  checks.equal(
    `${name}: attributes`,
    canonical(messageAttributes(event)),
    canonical(field(vector, 'snsAttributes')),
  )
}

function snapshotVector(checks: Checks, name: string, vector: JsonValue): void {
  const input = field(vector, 'input')
  const snapshot: SerializedSnapshot = {
    entityType: str(field(input, 'entityType')),
    entityId: str(field(input, 'entityId')),
    sequence: int(field(input, 'sequence')),
    eventSequence: int(field(input, 'eventSequence')),
    payload: encoder.encode(str(field(input, 'payload'))),
  }
  const serialized = str(field(vector, 'serializedSnapshotJson'))
  checks.equal(`${name}: json`, encodeSnapshot(snapshot), serialized)
  checks.equal(`${name}: decode json`, encodeSnapshot(decodeSnapshot(serialized)), serialized)

  const goItem = field(vector, 'dynamoItem')
  checks.equal(`${name}: dynamo item`, canonical(snapshotItem(snapshot)), canonical(goItem))
  checks.equal(`${name}: sk=0`, isSnapshotItem(item(goItem)), true)
  checks.equal(`${name}: decode item`, encodeSnapshot(snapshotFromItem(item(goItem))), serialized)
}

// The streams were committed with a fixed clock and retention, both with fractions of a second,
// so the expiry is rounded down after adding them.
function ttlVectors(checks: Checks, vector: JsonValue): void {
  const clockMs = milliseconds(int(field(vector, 'clockUnixNano')))

  for (const stream of list(field(vector, 'streams'))) {
    const name = `codec ttl ${str(field(stream, 'name'))}`
    checks.run(name, () => {
      const ttl = expiry(clockMs, milliseconds(int(field(stream, 'retentionNanos'))))
      const snapshot = decodeSnapshot(str(field(stream, 'snapshot')))

      // The head item comes first (sk -1) and carries the same ttl as the stream's other items.
      const items = [
        headItem(snapshot.entityId, snapshot.eventSequence, ttl),
        snapshotItem(snapshot, ttl),
        ...list(field(stream, 'events')).map((event) => eventItem(decodeEvent(str(event)), ttl)),
      ]
      checks.equal(`${name}: items`, canonical(items), canonical(field(stream, 'dynamoItems')))
    })
  }
}

// The view vector encodes the account aggregate's state: a struct whose labels are a Go map.
function viewVector(checks: Checks, name: string, vector: JsonValue): void {
  const state = str(field(vector, 'state'))
  const parsed = parseJson(state)
  const account = {
    owner: str(field(parsed, 'owner')),
    status: str(field(parsed, 'status')),
    balanceCents: num(field(parsed, 'balanceCents')),
    deposits: num(field(parsed, 'deposits')),
    openedAt: str(field(parsed, 'openedAt')),
    tags: list(field(parsed, 'tags')).map(str),
    labels: new Map(Object.entries(plain(field(parsed, 'labels')) as Record<string, string>)),
    lastMemo: field(parsed, 'lastMemo'),
  }
  const payload = stringifyJson(account)
  checks.equal(`${name}: state`, payload, state)

  const got = viewItem('account', str(field(vector, 'entityId')), encoder.encode(payload))
  checks.equal(`${name}: view item`, canonical(got), canonical(field(vector, 'dynamoItem')))
}

function item(value: JsonValue): Item {
  return plain(value) as Item
}

function at(value: JsonValue | undefined): JsonValue {
  if (value === undefined) {
    throw new Error('snapshotPolicy vector is short')
  }

  return value
}

// The fixture records nanoseconds. The TS clock and retention are whole milliseconds.
function milliseconds(nanos: bigint): number {
  if (nanos % 1_000_000n !== 0n) {
    throw new Error(`${nanos} ns is not a whole number of milliseconds`)
  }

  return Number(nanos / 1_000_000n)
}
