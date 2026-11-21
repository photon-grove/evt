// The view repository over a DynamoDB views table, item for item and request for request Go's
// `dynamo.ViewRepository` (compat/fixtures/views.json). The views table is the one the commit path
// writes view rows to: string `pk` and `sk`, and a global secondary index `entityType-index` with
// hash key `entityType`.
//
// | Attribute    | Type | Value                                  |
// | ------------ | ---- | -------------------------------------- |
// | `pk`         | S    | partition key                          |
// | `sk`         | S    | sort key (`VIEW` for a single item)    |
// | `entityID`   | S    | owner stream                           |
// | `entityType` | S    | entity type                            |
// | `payload`    | S    | view state JSON                        |
// | `ttl`        | N    | optional, written whenever it isn't 0  |

import {TransientError} from '../errors.js'
import {invalid} from '../fields.js'
import type {CallOptions} from '../repository.js'
import {sleep} from '../retry.js'
import {utf8Encode} from '../utf8.js'
import {
  type PageOptions,
  type ViewPage,
  type ViewRepository,
  type ViewRow,
  allPages,
  batchKeys,
  checkViewRow,
  cursorKey,
  encodeViewCursor,
  checkViewText,
  pageLimit,
  resolveViewKey,
  viewTtl,
} from '../views.js'
import type {GetItemInput, Page, QueryInput, RequestOptions} from './client.js'
import type {AttributeValue, Item} from './item.js'

/** The global secondary index that lists view rows by entity type. */
export const ENTITY_TYPE_INDEX = 'entityType-index'

/** DynamoDB's limit on requests per `BatchWriteItem` call, the chunk size of `putViews`. */
export const MAX_VIEW_BATCH_WRITE = 25

/** DynamoDB's limit on keys per `BatchGetItem` call, the chunk size of `batchGetViews`. */
export const MAX_VIEW_BATCH_GET = 100

// How many times a batch call sends what DynamoDB left unprocessed before it gives up, as in Go.
const MAX_BATCH_ATTEMPTS = 3

/** One request of a `BatchWriteItem` call. */
export interface WriteRequest {
  PutRequest?: {Item: Item}
  DeleteRequest?: {Key: Item}
}

/** A `BatchWriteItem` call. */
export interface BatchWriteItemInput {
  RequestItems: Record<string, WriteRequest[]>
}

/** A `BatchGetItem` call. */
export interface BatchGetItemInput {
  RequestItems: Record<string, {Keys: Item[]; ConsistentRead?: boolean}>
}

/** A `Query`, optionally of an index. */
export interface ViewQueryInput extends QueryInput {
  IndexName?: string
}

/**
 * The calls {@link DynamoViewRepository} makes, as a structural interface: the AWS SDK's
 * aggregated `DynamoDB` client satisfies it. Responses are read as the SDK returns them.
 */
export interface DynamoViewClient {
  getItem(
    input: GetItemInput,
    options?: RequestOptions,
  ): Promise<{Item?: Record<string, unknown> | undefined}>
  query(input: ViewQueryInput, options?: RequestOptions): Promise<Page>
  batchWriteItem(
    input: BatchWriteItemInput,
    options?: RequestOptions,
  ): Promise<{UnprocessedItems?: Record<string, unknown[]> | undefined}>
  batchGetItem(
    input: BatchGetItemInput,
    options?: RequestOptions,
  ): Promise<{
    Responses?: Record<string, Record<string, unknown>[]> | undefined
    UnprocessedKeys?: Record<string, unknown> | undefined
  }>
}

/** Options for {@link DynamoViewRepository}. */
export interface DynamoViewRepositoryOptions {
  /**
   * Whether partition listings read strongly consistently (default `true`). `false` halves their
   * read cost, and a listing may then miss a row a commit just wrote (Go `WithConsistentRead`).
   * Point reads stay strongly consistent, and entity-type listings read an index, which can only
   * be read eventually consistently, either way.
   */
  consistentRead?: boolean | undefined
  /** Waits between the attempts of a batch call; the default sleeps. For tests. */
  sleep?: ((ms: number, signal?: AbortSignal) => Promise<void>) | undefined
}

/**
 * A {@link ViewRepository} over a DynamoDB views table. Point reads are strongly consistent, as
 * are partition listings unless `consistentRead: false`, so a row a commit wrote is seen by the
 * next read of its key or partition. Entity-type listings read `entityType-index`, eventually
 * consistently. Writes go through `BatchWriteItem`, as Go's do, so a write is unconditional and
 * part of no commit; what a batch leaves unprocessed is sent again, up to three attempts, after
 * which the call fails with a `TransientError`. Failures of the client itself are rethrown as
 * they are.
 */
export class DynamoViewRepository implements ViewRepository {
  readonly #client: DynamoViewClient
  readonly #table: string
  readonly #consistentRead: boolean
  readonly #sleep: (ms: number, signal?: AbortSignal) => Promise<void>

  constructor(client: DynamoViewClient, table: string, options: DynamoViewRepositoryOptions = {}) {
    if (table === '') {
      throw new RangeError('a view repository needs a views table')
    }

    this.#client = client
    this.#table = table
    this.#consistentRead = options.consistentRead ?? true
    this.#sleep = options.sleep ?? sleep
  }

  /** The views table this repository reads and writes. */
  get table(): string {
    return this.#table
  }

  async getView(pk: string, options?: CallOptions): Promise<ViewRow | null> {
    return this.getViewAt(pk, '', options)
  }

  async getViewAt(pk: string, sk: string, options?: CallOptions): Promise<ViewRow | null> {
    options?.signal?.throwIfAborted()
    const output = await this.#client.getItem(
      {
        TableName: this.#table,
        Key: viewKey(pk, resolveViewKey(pk, sk)),
        ConsistentRead: true,
      },
      requestOptions(options),
    )
    const item = output.Item as Item | undefined

    return item === undefined || Object.keys(item).length === 0 ? null : viewRowFromItem(item)
  }

  async putView(row: ViewRow, options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    await this.#writeBatch([{PutRequest: {Item: viewRowItem(row)}}], options)
  }

  async putViews(rows: readonly ViewRow[], options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    // Every row is checked before the first write. BatchWriteItem refuses a call that repeats a
    // key, so a repeated key keeps its last row, as one put after another would.
    const items = new Map<string, Item>()
    for (const row of rows) {
      const item = viewRowItem(row)
      const key = JSON.stringify([row.pk, (item['sk'] as {S: string}).S])
      items.delete(key)
      items.set(key, item)
    }

    const writes = [...items.values()].map((item) => ({
      PutRequest: {Item: item},
    }))
    for (let start = 0; start < writes.length; start += MAX_VIEW_BATCH_WRITE) {
      // Each chunk is written after the one before it, as Go writes them.
      // oxlint-disable-next-line no-await-in-loop
      await this.#writeBatch(writes.slice(start, start + MAX_VIEW_BATCH_WRITE), options)
    }
  }

  async deleteView(pk: string, sk: string, options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    const key = viewKey(pk, resolveViewKey(pk, sk))
    await this.#writeBatch([{DeleteRequest: {Key: key}}], options)
  }

  async batchGetViews(pks: readonly string[], options?: CallOptions): Promise<ViewRow[]> {
    return this.batchGetViewsAt(pks, '', options)
  }

  async batchGetViewsAt(
    pks: readonly string[],
    sk: string,
    options?: CallOptions,
  ): Promise<ViewRow[]> {
    options?.signal?.throwIfAborted()
    const keys = batchKeys(pks, sk)
    const rows: ViewRow[] = []
    for (let start = 0; start < keys.pks.length; start += MAX_VIEW_BATCH_GET) {
      const chunk = keys.pks
        .slice(start, start + MAX_VIEW_BATCH_GET)
        .map((pk) => viewKey(pk, keys.sk))
      // Each chunk is read after the one before it, as Go reads them.
      // oxlint-disable-next-line no-await-in-loop
      const found = await this.#getChunk(chunk, options)
      rows.push(...found)
    }

    return rows
  }

  async listViewsByPk(pk: string, options?: CallOptions): Promise<ViewRow[]> {
    return allPages((cursor) => this.listViewsByPkPaged(pk, {...options, cursor}))
  }

  async listViewsByPkPaged(pk: string, options: PageOptions = {}): Promise<ViewPage> {
    options.signal?.throwIfAborted()
    checkViewText(pk, 'pk')
    const query = viewQueryByPk(this.#table, pk, options)
    query.ConsistentRead = this.#consistentRead

    return this.#page(query, options)
  }

  async listViewsByEntityType(entityType: string, options?: CallOptions): Promise<ViewRow[]> {
    return allPages((cursor) => this.listViewsByEntityTypePaged(entityType, {...options, cursor}))
  }

  async listViewsByEntityTypePaged(
    entityType: string,
    options: PageOptions = {},
  ): Promise<ViewPage> {
    options.signal?.throwIfAborted()
    checkViewText(entityType, 'entity type')

    return this.#page(viewQueryByEntityType(this.#table, entityType, options), options)
  }

  async #page(query: ViewQueryInput, options: CallOptions): Promise<ViewPage> {
    const output = await this.#client.query(query, requestOptions(options))
    const views = ((output.Items ?? []) as Item[]).map(viewRowFromItem)
    const last = output.LastEvaluatedKey as Item | undefined
    const nextCursor =
      last === undefined || Object.keys(last).length === 0 ? null : encodeItemCursor(last)

    return {views, nextCursor}
  }

  // Sends one BatchWriteItem of at most 25 requests, sending again what DynamoDB left unprocessed.
  async #writeBatch(writes: WriteRequest[], options: CallOptions | undefined): Promise<void> {
    let pending = writes
    for (let attempt = 1; pending.length > 0; attempt++) {
      if (attempt > 1) {
        // oxlint-disable-next-line no-await-in-loop
        await this.#sleep((attempt - 1) * 25, options?.signal)
      }

      // oxlint-disable-next-line no-await-in-loop
      const output = await this.#client.batchWriteItem(
        {RequestItems: {[this.#table]: pending}},
        requestOptions(options),
      )
      pending = (output.UnprocessedItems?.[this.#table] ?? []) as WriteRequest[]
      if (pending.length > 0 && attempt === MAX_BATCH_ATTEMPTS) {
        throw new TransientError(`batch write returned unprocessed items after ${attempt} attempts`)
      }
    }
  }

  // Sends one BatchGetItem of at most 100 keys, sending again what DynamoDB left unprocessed. Every
  // attempt reads strongly consistently, whether or not the response repeats ConsistentRead.
  async #getChunk(keys: Item[], options: CallOptions | undefined): Promise<ViewRow[]> {
    const rows: ViewRow[] = []
    let pending = keys
    for (let attempt = 1; pending.length > 0; attempt++) {
      if (attempt > 1) {
        // oxlint-disable-next-line no-await-in-loop
        await this.#sleep((attempt - 1) * 25, options?.signal)
      }

      // oxlint-disable-next-line no-await-in-loop
      const output = await this.#client.batchGetItem(
        {RequestItems: {[this.#table]: {Keys: pending, ConsistentRead: true}}},
        requestOptions(options),
      )
      for (const item of output.Responses?.[this.#table] ?? []) {
        rows.push(viewRowFromItem(item as Item))
      }

      const unprocessed = output.UnprocessedKeys?.[this.#table] as {Keys?: Item[]} | undefined
      pending = unprocessed?.Keys ?? []
      if (pending.length > 0 && attempt === MAX_BATCH_ATTEMPTS) {
        throw new TransientError(`batch get returned unprocessed keys after ${attempt} attempts`)
      }
    }

    return rows
  }
}

/**
 * Encodes a view row as Go's `PutView` writes it: the five attributes, an empty `sk` as `"VIEW"`,
 * and `ttl` whenever it isn't zero. The row is checked first (see `ViewRepository`).
 */
export function viewRowItem(row: ViewRow): Item {
  const payload = checkViewRow(row)
  const item: Item = {
    pk: {S: row.pk},
    sk: {S: resolveViewKey(row.pk, row.sk)},
    entityID: {S: row.entityId},
    entityType: {S: row.entityType},
    payload: {S: payload},
  }
  const ttl = viewTtl(row)
  if (ttl !== 0) {
    item['ttl'] = {N: ttl.toString()}
  }

  return item
}

/**
 * Decodes a views-table item as Go's reader does: an absent or NULL attribute is empty (or zero),
 * a text attribute may hold a number (its text), and `ttl` must be an integer, here a safe one.
 * Anything else is refused with `invalidRecord`.
 */
export function viewRowFromItem(item: Item): ViewRow {
  return {
    pk: text(item, 'pk'),
    sk: text(item, 'sk'),
    entityId: text(item, 'entityID'),
    entityType: text(item, 'entityType'),
    payload: utf8Encode(text(item, 'payload')),
    ttl: ttlAttribute(item),
  }
}

/** The primary key of a views-table item. */
export function viewKey(pk: string, sk: string): Item {
  return {pk: {S: pk}, sk: {S: sk}}
}

/**
 * The `Query` of one page of a partition. The caller sets `ConsistentRead`; the repository sets
 * it to its `consistentRead`, `true` by default.
 */
export function viewQueryByPk(
  table: string,
  pk: string,
  options: PageOptions = {},
): ViewQueryInput {
  return viewQuery(table, undefined, 'pk', pk, options)
}

/**
 * The `Query` of one page of an entity type, through `entityType-index`. It carries no
 * `ConsistentRead`, which DynamoDB refuses on a global secondary index.
 */
export function viewQueryByEntityType(
  table: string,
  entityType: string,
  options: PageOptions = {},
): ViewQueryInput {
  return viewQuery(table, ENTITY_TYPE_INDEX, 'entityType', entityType, options)
}

/** Encodes a `LastEvaluatedKey` as a cursor. Attributes that aren't strings are dropped. */
export function encodeItemCursor(key: Item): string {
  const strings = new Map<string, string>()
  for (const [name, value] of Object.entries(key)) {
    if ('S' in value) {
      strings.set(name, value.S)
    }
  }

  return encodeViewCursor(strings)
}

function viewQuery(
  table: string,
  index: string | undefined,
  name: 'pk' | 'entityType',
  value: string,
  options: PageOptions,
): ViewQueryInput {
  const limit = pageLimit(options.limit)
  const start = cursorKey(options.cursor, name, value)
  const query: ViewQueryInput = {
    TableName: table,
    KeyConditionExpression: `${name} = :${name}`,
    ExpressionAttributeValues: {[`:${name}`]: {S: value}},
  }
  if (index !== undefined) {
    query.IndexName = index
  }
  if (limit !== undefined) {
    query.Limit = limit
  }
  if (start !== null) {
    query.ExclusiveStartKey = Object.fromEntries(
      Array.from(start, ([attribute, held]): [string, AttributeValue] => [attribute, {S: held}]),
    )
  }

  return query
}

function text(item: Item, name: string): string {
  const value = item[name]
  if (value === undefined || 'NULL' in value) {
    return ''
  }
  if ('S' in value) {
    return value.S
  }
  if ('N' in value) {
    return value.N
  }

  throw invalid(`view attribute ${name} is not a string`)
}

function ttlAttribute(item: Item): number {
  const value = item['ttl']
  if (value === undefined || 'NULL' in value) {
    return 0
  }
  if (!('N' in value)) {
    throw invalid('view attribute ttl is not a number')
  }

  // An int64 in decimal, as strconv.ParseInt reads it, that a number holds exactly.
  const number = /^[+-]?\d+$/.test(value.N) ? Number(value.N) : Number.NaN
  if (!Number.isSafeInteger(number)) {
    throw invalid(`view attribute ttl is not a safe integer: ${value.N}`)
  }

  return number
}

function requestOptions(options: CallOptions | undefined): RequestOptions {
  return options?.signal ? {abortSignal: options.signal} : {}
}
