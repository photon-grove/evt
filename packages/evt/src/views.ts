// The view repository (ADR 0009 phase 5): reading and writing view rows outside the commit path,
// the counterpart of Go's `evt.ViewRepository` and Rust's `ViewRepository` trait. The in-memory
// and DynamoDB backends implement it, and `ViewCodec` (viewcodec.ts) layers typed values on top.
//
// Views are projections: every row must be reproducible by replaying events. The writes here take
// no owner check and no condition, as in Go and Rust. They are for the rebuild path and for rows
// no commit-wide projector owns: writing a row a projector owns changes its owner behind the
// projector's back, which a replay refuses.

import {base64Decode, base64Encode} from './base64.js'
import {EvtError, messageOf} from './errors.js'
import {parseJson, stringifyJson} from './json.js'
import type {CallOptions} from './repository.js'
import {utf8Decode, utf8Encode} from './utf8.js'

/** The sort key of a single-item view row: an empty `sk` means it, on every read and write. */
export const DEFAULT_VIEW_SK = 'VIEW'

/**
 * One view row: its keys, the stream that owns it (stored as the row's `entityID`), its entity
 * type, its payload, and an optional expiry.
 */
export interface ViewRow {
  /** The partition key. Never empty. */
  readonly pk: string
  /** The sort key. An empty one is stored as `"VIEW"`. */
  readonly sk: string
  /** The owner stream, stored as `entityID`. */
  readonly entityId: string
  readonly entityType: string
  /** The view state, valid UTF-8 (DynamoDB stores it as a string). */
  readonly payload: Uint8Array
  /**
   * When the row may expire, in Unix seconds, written as the DynamoDB `ttl` attribute whenever it
   * isn't zero (a negative one too, as Go writes it). Zero or absent is none. A row a repository
   * reads always has it, `0` when the item has none. The in-memory repository keeps it and never
   * expires a row.
   */
  readonly ttl?: number
}

/** One page of rows, and the cursor of the next page if there may be more. */
export interface ViewPage {
  /** The rows, in the order the backend lists them. */
  readonly views: ViewRow[]
  /**
   * An opaque cursor for the next page, or null when the listing is exhausted. DynamoDB returns
   * one whenever a bounded page was full, even when no row follows, so the next page can be
   * empty; the in-memory repository returns one only when a row follows. Either way, page until
   * it's null.
   */
  readonly nextCursor: string | null
}

/** The options of a paged listing. */
export interface PageOptions extends CallOptions {
  /**
   * The page size. Absent, zero or negative leaves it to the backend (DynamoDB: its 1 MB response
   * cap), as does one above 2^31 - 1, as in Go. It must be an integer.
   */
  limit?: number | undefined
  /**
   * The `nextCursor` of an earlier page of the same listing, or empty or absent for the first.
   * Base64url JSON of the last key, byte for byte Go's, so a cursor moves between a Go, a Rust
   * and a TypeScript process. A malformed cursor, or one from another listing, is refused with
   * `invalidArgument`.
   */
  cursor?: string | null | undefined
}

/**
 * Reads and writes view rows by key, partition and entity type.
 *
 * Consistency: a point read and a partition listing see every row a commit wrote once the commit
 * has returned, so a request handler can commit and then read its own write. DynamoDB reads its
 * table strongly consistently for them; an entity-type listing comes from a global secondary
 * index, which DynamoDB reads only eventually consistently, so it can lag behind a commit. The
 * in-memory repository is always consistent.
 *
 * U+0000 is refused with `invalidArgument` before any request: in a written row's keys, owner,
 * entity type or payload (a batch holding one writes nothing), and in a key or entity type a read,
 * delete or listing names. PostgreSQL `text` can't hold it, so every backend of every language
 * refuses it. An empty `pk` is refused with `invalidArgument`, and a payload that isn't valid
 * UTF-8 with `invalidRecord`.
 */
export interface ViewRepository {
  /** The single-item view at `pk` (its `sk` is `"VIEW"`), or null. */
  getView(pk: string, options?: CallOptions): Promise<ViewRow | null>

  /** The row at `pk` and `sk` (empty means `"VIEW"`), or null. */
  getViewAt(pk: string, sk: string, options?: CallOptions): Promise<ViewRow | null>

  /** Writes or replaces the row, with no owner check and no condition. */
  putView(row: ViewRow, options?: CallOptions): Promise<void>

  /**
   * Writes or replaces many rows, Go's `PutViews` (the writer behind a rebuild). Every row is
   * checked before the first write. DynamoDB writes them in `BatchWriteItem` calls of 25 and
   * retries what it leaves unprocessed. The batch isn't atomic: a failure can leave a prefix
   * written. A repeated key keeps its last row.
   */
  putViews(rows: readonly ViewRow[], options?: CallOptions): Promise<void>

  /** Deletes the row at `pk` and `sk` (empty means `"VIEW"`). Deleting no row succeeds. */
  deleteView(pk: string, sk: string, options?: CallOptions): Promise<void>

  /**
   * The rows at the given partition keys and the single-item sort key `"VIEW"`. Keys are
   * de-duplicated and an empty one is skipped. Only rows that exist come back, in no guaranteed
   * order, so index them by `pk`.
   */
  batchGetViews(pks: readonly string[], options?: CallOptions): Promise<ViewRow[]>

  /** {@link batchGetViews} at sort key `sk` (empty means `"VIEW"`). */
  batchGetViewsAt(pks: readonly string[], sk: string, options?: CallOptions): Promise<ViewRow[]>

  /** Every row of a partition, ascending by `sk` bytes. It buffers them all. */
  listViewsByPk(pk: string, options?: CallOptions): Promise<ViewRow[]>

  /** One page of a partition's rows, ascending by `sk` bytes. */
  listViewsByPkPaged(pk: string, options?: PageOptions): Promise<ViewPage>

  /** Every row of an entity type. It buffers them all. */
  listViewsByEntityType(entityType: string, options?: CallOptions): Promise<ViewRow[]>

  /** One page of an entity type's rows. DynamoDB lists them in no particular order. */
  listViewsByEntityTypePaged(entityType: string, options?: PageOptions): Promise<ViewPage>
}

/**
 * Checks what every backend refuses in a row before writing it: an empty `pk`, a payload that
 * isn't valid UTF-8, a `ttl` that isn't a safe integer, and U+0000 in any of its text. Returns the
 * payload as text.
 */
export function checkViewRow(row: ViewRow): string {
  if (row.pk === '') {
    throw new EvtError('invalidArgument', 'a view row needs a non-empty pk')
  }

  let payload: string
  try {
    payload = utf8Decode(row.payload, 'view payload')
  } catch (error) {
    throw new EvtError(
      'invalidRecord',
      'view payload is not valid UTF-8 and cannot be stored as a string',
      {cause: error},
    )
  }

  viewTtl(row)
  nulFree(row.pk, 'pk')
  nulFree(row.sk, 'sk')
  nulFree(row.entityId, 'owner')
  nulFree(row.entityType, 'entity type')
  nulFree(payload, 'payload')

  return payload
}

/** A row's `ttl`, `0` when it has none, refused when it isn't a safe integer. */
export function viewTtl(row: ViewRow): number {
  const ttl = row.ttl ?? 0
  if (!Number.isSafeInteger(ttl)) {
    throw new EvtError('invalidArgument', `a view ttl must be a safe integer: ${ttl}`)
  }

  return ttl
}

/** Refuses U+0000 in a view row's text or in a key or entity type a read names. */
export function nulFree(value: string, field: string): void {
  if (value.includes('\0')) {
    throw new EvtError('invalidArgument', `a view ${field} cannot contain U+0000`)
  }
}

/**
 * Resolves a point key: an empty `pk` and U+0000 in either part are refused, and an empty `sk`
 * is `"VIEW"`, so every operation reads the key a write stored.
 */
export function resolveViewKey(pk: string, sk: string): string {
  if (pk === '') {
    throw new EvtError('invalidArgument', 'a view key needs a non-empty pk')
  }
  nulFree(pk, 'pk')
  nulFree(sk, 'sk')

  return sk === '' ? DEFAULT_VIEW_SK : sk
}

/**
 * The sort key of a batch read, checked with every key before any read: U+0000 in it or in a
 * `pk` is refused. Returns the distinct, non-empty keys in their first order.
 */
export function batchKeys(pks: readonly string[], sk: string): {pks: string[]; sk: string} {
  const resolved = resolveViewKey('-', sk)
  for (const pk of pks) {
    nulFree(pk, 'pk')
  }

  return {pks: [...new Set(pks.filter((pk) => pk !== ''))], sk: resolved}
}

/** A listing's page size, or undefined for none: Go ignores one at or below 0 or past int32. */
export function pageLimit(limit: number | undefined): number | undefined {
  if (limit === undefined) {
    return undefined
  }
  if (!Number.isSafeInteger(limit)) {
    throw new EvtError('invalidArgument', `a page limit must be an integer: ${limit}`)
  }

  return limit > 0 && limit <= 2 ** 31 - 1 ? limit : undefined
}

/**
 * Encodes a listing's last key as a cursor: its string attributes as JSON, sorted by name with
 * Go's escaping, in padded base64url. This is Go's cursor byte for byte.
 */
export function encodeViewCursor(key: ReadonlyMap<string, string>): string {
  const text = stringifyJson(new Map(key))

  return base64Encode(utf8Encode(text)).replaceAll('+', '-').replaceAll('/', '_')
}

/**
 * Decodes a cursor into the key it resumes after. Padded base64url of a JSON object of strings is
 * required, as Go's decoder requires it, and the key must hold `pk` and `sk`, as Rust requires
 * (Go forwards whatever it holds and lets DynamoDB refuse it). Refused with `invalidArgument`.
 */
export function decodeViewCursor(cursor: string): Map<string, string> {
  if (/[+/]/.test(cursor)) {
    throw invalidCursor('illegal base64url data')
  }

  let key: Map<string, string>
  try {
    const bytes = base64Decode(cursor.replaceAll('-', '+').replaceAll('_', '/'))
    const value = parseJson(utf8Decode(bytes, 'cursor'))
    if (!(value instanceof Map)) {
      throw new EvtError('invalidRecord', 'the cursor is not a JSON object')
    }

    key = new Map()
    for (const [name, entry] of value) {
      if (typeof entry !== 'string') {
        throw new EvtError('invalidRecord', `the cursor's ${name} is not a string`)
      }
      key.set(name, entry)
    }
  } catch (error) {
    throw invalidCursor(messageOf(error), error)
  }

  if (!key.has('pk') || !key.has('sk')) {
    throw invalidCursor('the key has no pk and sk')
  }

  return key
}

/**
 * The key a listing's cursor resumes after, or null for the first page. A cursor whose `name`
 * attribute isn't `value` belongs to another listing, which DynamoDB would refuse, so every
 * backend refuses it.
 */
export function cursorKey(
  cursor: string | null | undefined,
  name: 'pk' | 'entityType',
  value: string,
): Map<string, string> | null {
  if (cursor === undefined || cursor === null || cursor === '') {
    return null
  }

  const key = decodeViewCursor(cursor)
  if (key.get(name) !== value) {
    throw invalidCursor('it belongs to another listing')
  }

  return key
}

/** Collects every page of a listing, following the cursors. */
export async function allPages(
  page: (cursor: string | null) => Promise<ViewPage>,
): Promise<ViewRow[]> {
  const rows: ViewRow[] = []
  let cursor: string | null = null
  do {
    // Each page needs the cursor of the one before it.
    // oxlint-disable-next-line no-await-in-loop
    const next: ViewPage = await page(cursor)
    rows.push(...next.views)
    cursor = next.nextCursor
  } while (cursor !== null)

  return rows
}

function invalidCursor(reason: string, cause?: unknown): EvtError {
  return new EvtError(
    'invalidArgument',
    `invalid pagination cursor: ${reason}`,
    cause === undefined ? undefined : {cause},
  )
}
