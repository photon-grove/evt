// views.json: the view repository's items, keys, requests and cursors, and the typed view codec's
// payload bytes, checked through DynamoViewRepository against a recording client, as the Rust crate
// checks them in crates/evt-compat/tests/views.rs.
import {
  type Codec,
  JsonNumber,
  type JsonValue,
  type ViewRow,
  ViewCodec,
  ViewDecodeError,
  decodeJsonView,
  decodeViewCursor,
  parseJson,
  stringifyJson,
} from '@photon-grove/evt'
import {
  type BatchGetItemInput,
  type BatchWriteItemInput,
  type DynamoViewClient,
  DynamoViewRepository,
  type GetItemInput,
  type Item,
  MAX_VIEW_BATCH_GET,
  MAX_VIEW_BATCH_WRITE,
  type ViewQueryInput,
  encodeItemCursor,
  viewRowFromItem,
} from '@photon-grove/evt/dynamodb'

import {type Checks, canonical, field, int, list, loadFixture, num, plain, str} from './harness.js'

const TABLE = 'views'

const encoder = new TextEncoder()
const decoder = new TextDecoder('utf-8', {fatal: true})

export async function viewVectors(checks: Checks): Promise<void> {
  const fixtures = await loadFixture('views.json')

  // A section regenerated empty would otherwise pass without checking anything.
  const sections = ['puts', 'deletes', 'gets', 'decodes', 'queries', 'cursors', 'startKeys']
  for (const section of [...sections, 'codec', 'codecDecode']) {
    checks.equal(`views ${section} present`, list(field(fixtures, section)).length > 0, true)
  }

  const limits = field(fixtures, 'limits')
  checks.equal('views limits write', MAX_VIEW_BATCH_WRITE, num(field(limits, 'write')))
  checks.equal('views limits get', MAX_VIEW_BATCH_GET, num(field(limits, 'get')))

  const each = async (
    section: string,
    check: (name: string, vector: JsonValue) => Promise<void>,
  ): Promise<void> => {
    for (const vector of list(field(fixtures, section))) {
      const name = `views ${section} ${str(field(vector, 'name'))}`
      // The vectors are independent; awaiting each keeps the report in fixture order.
      // oxlint-disable-next-line no-await-in-loop
      await checks.case(name, () => check(name, vector))
    }
  }

  await each('puts', async (name, vector) => {
    const client = new RecordingClient()
    await new DynamoViewRepository(client, TABLE).putView(row(field(vector, 'row')))
    same(name, client.onlyWrite('PutRequest'), field(vector, 'dynamoItem'))
  })

  await each('deletes', async (name, vector) => {
    const client = new RecordingClient()
    const repo = new DynamoViewRepository(client, TABLE)
    await repo.deleteView(str(field(vector, 'pk')), str(field(vector, 'sk')))
    same(name, client.onlyWrite('DeleteRequest'), field(vector, 'key'))
  })

  await each('gets', async (name, vector) => {
    const client = new RecordingClient()
    const found = await new DynamoViewRepository(client, TABLE).getView(str(field(vector, 'pk')))
    ensure(found === null, `${name}: an empty response is no row`)
    ensure(client.gets.length === 1, `${name}: one GetItem`)
    const [get] = client.gets
    same(`${name} key`, get!.Key, field(vector, 'key'))
    ensure(get!.TableName === TABLE, `${name}: the table`)
    ensure(
      (get!.ConsistentRead ?? false) === field(vector, 'consistentRead'),
      `${name}: ConsistentRead`,
    )
  })

  await each('decodes', async (name, vector) => {
    const item = plain(field(vector, 'dynamoItem')) as Item
    const error = field(vector, 'error') === true
    // Through the repository, as a GetItem response, and as the pure decoder.
    const client = new RecordingClient()
    client.getOutput = item
    const repo = new DynamoViewRepository(client, TABLE)
    for (const [how, decode] of [
      ['decoded', async () => viewRowFromItem(item)],
      ['read', () => repo.getView('ignored')],
    ] as const) {
      let decoded: ViewRow | null
      try {
        // oxlint-disable-next-line no-await-in-loop
        decoded = await decode()
      } catch (thrown) {
        ensure(error, `${name}: ${how} threw ${String(thrown)}`)
        ensure(code(thrown) === 'invalidRecord', `${name}: ${how} refuses with invalidRecord`)
        continue
      }
      ensure(!error, `${name}: ${how} should refuse`)
      ensure(decoded !== null, `${name}: ${how} found the row`)
      ensure(rowText(decoded) === rowText(row(field(vector, 'row'))), `${name}: ${how} row`)
    }
  })

  await each('queries', async (name, vector) => {
    const client = new RecordingClient()
    const repo = new DynamoViewRepository(client, TABLE)
    const key = str(field(vector, 'key'))
    const options = {
      limit: num(field(vector, 'limit')),
      cursor: str(field(vector, 'cursor')),
    }
    const kind = str(field(vector, 'kind'))
    const page =
      kind === 'pk'
        ? await repo.listViewsByPkPaged(key, options)
        : await repo.listViewsByEntityTypePaged(key, options)
    ensure(page.views.length === 0 && page.nextCursor === null, `${name}: an empty page`)
    ensure(client.queries.length === 1, `${name}: one Query`)

    const query = client.queries[0]!
    ensure(query.TableName === TABLE, `${name}: the table`)
    ensure((query.IndexName ?? '') === str(field(vector, 'index')), `${name}: index`)
    ensure(
      query.KeyConditionExpression === str(field(vector, 'keyCondition')),
      `${name}: condition`,
    )
    same(`${name} values`, query.ExpressionAttributeValues, field(vector, 'values'))
    const limit = field(vector, 'requestLimit')
    ensure((query.Limit ?? null) === (limit === null ? null : num(limit)), `${name}: Limit`)
    const startKey = field(vector, 'startKey')
    ensure(
      canonical(query.ExclusiveStartKey ?? null) === canonical(plain(startKey)),
      `${name}: ExclusiveStartKey`,
    )
    // Go sets ConsistentRead true on a partition query and leaves it unset on the index.
    ensure(
      ('ConsistentRead' in query && query.ConsistentRead !== undefined) ===
        field(vector, 'consistentSet'),
      `${name}: ConsistentRead set`,
    )
    ensure(
      (query.ConsistentRead ?? false) === field(vector, 'consistentRead'),
      `${name}: ConsistentRead`,
    )
  })

  await each('cursors', async (name, vector) => {
    const lastKey = plain(field(vector, 'lastEvaluatedKey')) as Item
    const want = str(field(vector, 'cursor'))
    ensure(encodeItemCursor(lastKey) === want, `${name}: encoded ${encodeItemCursor(lastKey)}`)

    // A page whose response carries the key returns the same cursor.
    const client = new RecordingClient()
    client.queryOutput = {Items: [], LastEvaluatedKey: lastKey}
    const repo = new DynamoViewRepository(client, TABLE)
    const page = await repo.listViewsByPkPaged('p')
    ensure(page.nextCursor === want, `${name}: the page's cursor`)
  })

  await each('startKeys', async (name, vector) => {
    const cursor = str(field(vector, 'cursor'))
    let key: Map<string, string>
    try {
      key = decodeViewCursor(cursor)
    } catch (thrown) {
      ensure(field(vector, 'error') === true, `${name}: threw ${String(thrown)}`)
      ensure(code(thrown) === 'invalidArgument', `${name}: refuses with invalidArgument`)
      return
    }
    ensure(field(vector, 'error') === false, `${name}: should refuse`)
    const item = Object.fromEntries(Array.from(key, ([attribute, held]) => [attribute, {S: held}]))
    same(name, item, field(vector, 'startKey'))
  })

  await each('codec', async (name, vector) => {
    const value = str(field(vector, 'value'))
    const encoded = decoder.decode(PROFILE.encode(PROFILE.decode(encoder.encode(value))))
    ensure(encoded === value, `${name}: payload ${encoded}`)

    // The whole path: the typed codec through the repository to the item BatchWriteItem sends.
    const client = new RecordingClient()
    const codec = new ViewCodec(new DynamoViewRepository(client, TABLE), 'profile', PROFILE)
    const pk = str(field(vector, 'pk'))
    const sk = str(field(vector, 'sk'))
    await codec.put(pk, sk, str(field(vector, 'entityId')), PROFILE.decode(encoder.encode(value)))
    same(name, client.onlyWrite('PutRequest'), field(vector, 'dynamoItem'))
  })

  await each('codecDecode', async (name, vector) => {
    const payload = str(field(vector, 'payload'))
    const viewRow = {
      pk: 'p',
      sk: 's',
      entityId: '',
      entityType: '',
      payload: encoder.encode(payload),
    }
    let profile: Profile
    try {
      profile = decodeJsonView(viewRow, PROFILE)
    } catch (thrown) {
      ensure(field(vector, 'valid') === false, `${name}: threw ${String(thrown)}`)
      ensure(thrown instanceof ViewDecodeError, `${name}: a ViewDecodeError`)
      ensure(thrown.pk === 'p' && thrown.sk === 's', `${name}: names the row`)
      return
    }
    ensure(field(vector, 'valid') === true, `${name}: should refuse`)
    const canonicalText = decoder.decode(PROFILE.encode(profile))
    ensure(canonicalText === str(field(vector, 'canonical')), `${name}: canonical ${canonicalText}`)
  })
}

/**
 * The typed view of the codec vectors: Go's `CodecProfile`, decoded by Go's rules. A missing
 * field or null is the zero value, unknown fields are ignored, a wrong type is refused, `count` is
 * an int64 and `big` a uint64, and `labels` is a map, written sorted.
 */
interface Profile {
  name: string
  note: string
  bio: string | null
  tags: string[] | null
  labels: Map<string, string> | null
  count: bigint
  big: bigint
}

const PROFILE: Codec<Profile> = {
  encode(profile) {
    const {name, note, bio, tags, labels, count, big} = profile

    return encoder.encode(stringifyJson({name, note, bio, tags, labels, count, big}))
  },
  decode(bytes) {
    const value = parseJson(decoder.decode(bytes))
    const profile: Profile = {
      name: '',
      note: '',
      bio: null,
      tags: null,
      labels: null,
      count: 0n,
      big: 0n,
    }
    if (value === null) {
      return profile
    }
    if (!(value instanceof Map)) {
      throw new Error('a profile is a JSON object')
    }

    const present = (key: string): JsonValue | undefined => {
      const entry = value.get(key)

      return entry === null ? undefined : entry
    }
    profile.name = text(present('name'), 'name') ?? ''
    profile.note = text(present('note'), 'note') ?? ''
    profile.bio = text(present('bio'), 'bio') ?? null
    const tags = present('tags')
    if (tags !== undefined) {
      if (!Array.isArray(tags)) {
        throw new Error('tags is not an array')
      }
      profile.tags = tags.map((tag) => text(tag ?? undefined, 'tag') ?? '')
    }
    const labels = present('labels')
    if (labels !== undefined) {
      if (!(labels instanceof Map)) {
        throw new Error('labels is not an object')
      }
      profile.labels = new Map(
        Array.from(labels, ([key, label]) => [key, text(label ?? undefined, 'label') ?? '']),
      )
    }
    profile.count = integer(present('count'), -(2n ** 63n), 2n ** 63n - 1n, 'count')
    profile.big = integer(present('big'), 0n, 2n ** 64n - 1n, 'big')

    return profile
  },
}

function text(value: JsonValue | undefined, what: string): string | undefined {
  if (value !== undefined && typeof value !== 'string') {
    throw new Error(`${what} is not a string`)
  }

  return value
}

function integer(value: JsonValue | undefined, min: bigint, max: bigint, what: string): bigint {
  if (value === undefined) {
    return 0n
  }
  if (!(value instanceof JsonNumber) || !value.isInteger) {
    throw new Error(`${what} is not an integer`)
  }

  const number = BigInt(value.text)
  if (number < min || number > max) {
    throw new Error(`${what} is out of range`)
  }

  return number
}

/** A DynamoViewClient that records every request and answers with scripted output. */
class RecordingClient implements DynamoViewClient {
  readonly writes: BatchWriteItemInput[] = []
  readonly gets: GetItemInput[] = []
  readonly queries: ViewQueryInput[] = []
  getOutput: Item | undefined
  queryOutput: {Items: Item[]; LastEvaluatedKey?: Item} = {Items: []}

  async getItem(input: GetItemInput) {
    this.gets.push(structuredClone(input))

    return {Item: this.getOutput}
  }

  async query(input: ViewQueryInput) {
    this.queries.push(structuredClone(input))

    return this.queryOutput
  }

  async batchWriteItem(input: BatchWriteItemInput) {
    this.writes.push(structuredClone(input))

    return {}
  }

  async batchGetItem(_input: BatchGetItemInput) {
    return {}
  }

  /** The one request of the one BatchWriteItem, as the item or key it carries. */
  onlyWrite(kind: 'PutRequest' | 'DeleteRequest'): Item {
    ensure(this.writes.length === 1, `want one BatchWriteItem, got ${this.writes.length}`)
    const requests = this.writes[0]!.RequestItems[TABLE] ?? []
    ensure(requests.length === 1, `want one write request, got ${requests.length}`)
    const request = requests[0]!
    const written = kind === 'PutRequest' ? request.PutRequest?.Item : request.DeleteRequest?.Key
    ensure(written !== undefined, `want a ${kind}`)

    return written
  }
}

function row(value: JsonValue): ViewRow {
  return {
    pk: str(field(value, 'pk')),
    sk: str(field(value, 'sk')),
    entityId: str(field(value, 'entityId')),
    entityType: str(field(value, 'entityType')),
    payload: encoder.encode(str(field(value, 'payload'))),
    ttl: Number(int(field(value, 'ttl'))),
  }
}

// A row as comparable text, its payload as a string.
function rowText(viewRow: ViewRow): string {
  return canonical({
    ...viewRow,
    payload: decoder.decode(viewRow.payload),
    ttl: viewRow.ttl ?? 0,
  })
}

function same(name: string, got: unknown, want: JsonValue): void {
  const [gotText, wantText] = [canonical(got), canonical(plain(want))]
  ensure(gotText === wantText, `${name}\n    got  ${gotText}\n    want ${wantText}`)
}

function code(error: unknown): unknown {
  return (error as {code?: unknown}).code
}

function ensure(condition: boolean, message: string): asserts condition {
  if (!condition) {
    throw new Error(message)
  }
}
