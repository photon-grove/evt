// The evt-owned PostgreSQL views table (ADR 0008): PgViewRepository reads and writes
// `{events}__evt_views` outside the commit path with the DynamoDB view repository's semantics, as
// Rust's `PgViewRepository` does (storage format, "PostgreSQL views table").
//
// The keys collate as "C", so a partition lists in sort-key byte order, as on DynamoDB, and the
// payload is `text`, kept byte for byte. Cursors are the DynamoDB repository's encoding of the last
// row's keys, so a cursor moves between languages. Every read sees every committed transaction,
// entity-type listings included.
//
// Each statement of a batch needs the one before it, so they await in loops on purpose.
/* oxlint-disable no-await-in-loop */

import {EvtError} from '../errors.js'
import type {CallOptions} from '../repository.js'
import {compareUtf8, utf8Encode, utf8Length} from '../utf8.js'
import {
  type PageOptions,
  type ViewPage,
  type ViewRepository,
  type ViewRow,
  allPages,
  batchKeys,
  checkViewRow,
  checkViewText,
  cursorKey,
  encodeViewCursor,
  pageLimit,
  resolveViewKey,
  viewTtl,
} from '../views.js'
import type {PgClient} from './client.js'
import {MAX_VIEW_INDEX_KEY_BYTES, MAX_VIEW_PAGE_ROWS, type PgTables, viewsTable} from './schema.js'
import {type Row, query, text, transaction} from './session.js'

// The columns every read selects, each as text, in `rowFrom`'s names.
const COLUMNS = 'pk, sk, entity_id, entity_type, payload, ttl::text AS ttl'

// How many expired rows `purgeExpired` deletes per statement.
const PURGE_BATCH = 1000

// Reaches a view repository's one-time encoding check, for the commit path.
let ensureEncoding: (views: PgViewRepository, options?: CallOptions) => Promise<void>

/**
 * A {@link ViewRepository} over a PostgreSQL store's views table, `{events}__evt_views`, which
 * {@link PgRepository.ensureSchema} creates. Get the one a repository writes with
 * {@link PgRepository.viewRepository}, or construct one over the same tables.
 *
 * Writes take no owner check and no condition, as every view repository's: they are for the
 * rebuild path and for rows no commit-wide projector owns. `putViews` writes its rows in one
 * transaction, so unlike DynamoDB's batches it is atomic. The database must be `UTF8`, which the
 * first call checks.
 */
export class PgViewRepository implements ViewRepository {
  readonly #client: PgClient
  readonly #table: string
  // Set once the database's encoding was found to be UTF8.
  #encodingChecked = false

  static {
    ensureEncoding = (views, options) => views.#ensureEncoding(options)
  }

  constructor(client: PgClient, tables: PgTables) {
    this.#client = client
    this.#table = viewsTable(tables)
  }

  /** The views table's name. */
  get table(): string {
    return this.#table
  }

  /**
   * Deletes the rows whose `ttl` has passed by `now` (whole Unix seconds), a batch at a time
   * through the partial `ttl` index, and resolves how many it deleted. Readers return expired rows
   * until it runs, as DynamoDB's do until its sweep, so an operator or a scheduled job runs it.
   * Rows without a `ttl` are never touched.
   *
   * The outer condition repeats the expiry because the subquery picks keys from its snapshot: a
   * row a concurrent write renewed while the delete waited on it is checked again against the
   * committed row and kept.
   */
  async purgeExpired(now: number, options?: CallOptions): Promise<number> {
    if (!Number.isSafeInteger(now)) {
      throw new EvtError('invalidArgument', `purgeExpired needs whole Unix seconds: ${now}`)
    }
    await this.#ensureEncoding(options)

    const table = this.#table
    const sql =
      `WITH gone AS (DELETE FROM ${table} WHERE (pk, sk) IN (SELECT pk, sk FROM ${table} ` +
      'WHERE ttl > 0 AND ttl <= $1::bigint ORDER BY ttl LIMIT $2::bigint) ' +
      'AND ttl > 0 AND ttl <= $1::bigint RETURNING 1) SELECT count(*)::text AS deleted FROM gone'
    let deleted = 0
    for (;;) {
      options?.signal?.throwIfAborted()
      const rows = await query(this.#client, sql, [now.toString(), PURGE_BATCH.toString()])
      const batch = Number(text(rows[0] ?? {}, 'deleted'))
      deleted += batch
      if (batch === 0) {
        return deleted
      }
    }
  }

  async getView(pk: string, options?: CallOptions): Promise<ViewRow | null> {
    return this.getViewAt(pk, '', options)
  }

  async getViewAt(pk: string, sk: string, options?: CallOptions): Promise<ViewRow | null> {
    const resolved = resolveViewKey(pk, sk)
    await this.#ensureEncoding(options)

    const rows = await query(
      this.#client,
      `SELECT ${COLUMNS} FROM ${this.#table} WHERE pk = $1 AND sk = $2`,
      [pk, resolved],
    )

    return rows[0] === undefined ? null : rowFrom(rows[0])
  }

  async putView(row: ViewRow, options?: CallOptions): Promise<void> {
    await this.putViews([row], options)
  }

  /**
   * Writes every row in one transaction. Every row is checked before anything is written, a
   * repeated key keeps its last row, and the rows are written in key byte order, so two batches
   * over the same keys lock them in the same order and can't deadlock.
   */
  async putViews(rows: readonly ViewRow[], options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    const byKey = new Map<string, {sk: string; payload: string; row: ViewRow}>()
    for (const row of rows) {
      const {sk, payload} = checked(row)
      byKey.set(`${row.pk}\0${sk}`, {sk, payload, row})
    }
    if (byKey.size === 0) {
      return
    }

    const ordered = [...byKey.values()]
    ordered.sort((a, b) => compareUtf8(a.row.pk, b.row.pk) || compareUtf8(a.sk, b.sk))
    await this.#ensureEncoding(options)
    await transaction(this.#client, options, false, async (run) => {
      for (const {sk, payload, row} of ordered) {
        await run(upsertSql(this.#table), [
          row.pk,
          sk,
          row.entityId,
          row.entityType,
          payload,
          viewTtl(row).toString(),
        ])
      }
    })
  }

  async deleteView(pk: string, sk: string, options?: CallOptions): Promise<void> {
    const resolved = resolveViewKey(pk, sk)
    await this.#ensureEncoding(options)
    await query(this.#client, `DELETE FROM ${this.#table} WHERE pk = $1 AND sk = $2`, [
      pk,
      resolved,
    ])
  }

  async batchGetViews(pks: readonly string[], options?: CallOptions): Promise<ViewRow[]> {
    return this.batchGetViewsAt(pks, '', options)
  }

  /** {@link batchGetViews} at sort key `sk`, in one statement. */
  async batchGetViewsAt(
    pks: readonly string[],
    sk: string,
    options?: CallOptions,
  ): Promise<ViewRow[]> {
    const keys = batchKeys(pks, sk)
    if (keys.pks.length === 0) {
      options?.signal?.throwIfAborted()
      return []
    }

    await this.#ensureEncoding(options)
    const rows = await query(
      this.#client,
      `SELECT ${COLUMNS} FROM ${this.#table} ` +
        'WHERE sk = $1 AND pk IN (SELECT jsonb_array_elements_text($2::jsonb))',
      [keys.sk, JSON.stringify(keys.pks)],
    )

    return rows.map(rowFrom)
  }

  async listViewsByPk(pk: string, options?: CallOptions): Promise<ViewRow[]> {
    return allPages((cursor) => this.listViewsByPkPaged(pk, {...options, cursor}))
  }

  async listViewsByPkPaged(pk: string, options: PageOptions = {}): Promise<ViewPage> {
    if (pk === '') {
      throw new EvtError('invalidArgument', 'a view listing needs a non-empty pk')
    }
    checkViewText(pk, 'pk')
    const after = cursorKey(options.cursor, 'pk', pk)
    const table = this.#table

    return after === null
      ? this.#page(`SELECT ${COLUMNS} FROM ${table} WHERE pk = $1 ORDER BY sk LIMIT $2`, [pk], {
          options,
        })
      : this.#page(
          `SELECT ${COLUMNS} FROM ${table} WHERE pk = $1 AND sk > $2 ORDER BY sk LIMIT $3`,
          [pk, after.get('sk')!],
          {options},
        )
  }

  async listViewsByEntityType(entityType: string, options?: CallOptions): Promise<ViewRow[]> {
    return allPages((cursor) => this.listViewsByEntityTypePaged(entityType, {...options, cursor}))
  }

  /** One page of an entity type's rows, ascending by `pk` and then `sk` bytes. */
  async listViewsByEntityTypePaged(
    entityType: string,
    options: PageOptions = {},
  ): Promise<ViewPage> {
    checkViewText(entityType, 'entity type')
    const after = cursorKey(options.cursor, 'entityType', entityType)
    const table = this.#table

    return after === null
      ? this.#page(
          `SELECT ${COLUMNS} FROM ${table} WHERE entity_type = $1 ORDER BY pk, sk LIMIT $2`,
          [entityType],
          {options, entityType: true},
        )
      : this.#page(
          `SELECT ${COLUMNS} FROM ${table} WHERE entity_type = $1 AND (pk, sk) > ($2, $3) ` +
            'ORDER BY pk, sk LIMIT $4',
          [entityType, after.get('pk')!, after.get('sk')!],
          {options, entityType: true},
        )
  }

  // One page: `values` are the statement's leading arguments, and the page size is its last. A
  // full page has a cursor, the keys DynamoDB's LastEvaluatedKey would carry.
  async #page(
    sql: string,
    values: string[],
    {options, entityType = false}: {options: PageOptions; entityType?: boolean},
  ): Promise<ViewPage> {
    const size = pageLimit(options.limit) ?? MAX_VIEW_PAGE_ROWS
    await this.#ensureEncoding(options)
    const views = (await query(this.#client, sql, [...values, size.toString()])).map(rowFrom)

    const last = views.at(-1)
    if (last === undefined || views.length < size) {
      return {views, nextCursor: null}
    }

    const key = new Map([
      ['pk', last.pk],
      ['sk', last.sk],
    ])
    if (entityType) {
      key.set('entityType', last.entityType)
    }

    return {views, nextCursor: encodeViewCursor(key)}
  }

  // Checks, once per repository, that the database is UTF8: the views table stores text.
  async #ensureEncoding(options: CallOptions | undefined): Promise<void> {
    options?.signal?.throwIfAborted()
    if (this.#encodingChecked) {
      return
    }

    const rows = await query(this.#client, "SELECT current_setting('server_encoding') AS encoding")
    const encoding = text(rows[0] ?? {}, 'encoding')
    if (encoding !== 'UTF8') {
      throw new EvtError(
        'invalidArgument',
        `the PostgreSQL views table needs a UTF8 database, and this one is ${encoding}`,
      )
    }

    this.#encodingChecked = true
  }
}

/** Checks, once per repository, that the database a views table lives in is UTF8. */
export function checkViewsEncoding(views: PgViewRepository, options?: CallOptions): Promise<void> {
  return ensureEncoding(views, options)
}

// Checks a row before writing it: the checks every backend makes, then the index key bound.
// Returns the sort key it is stored under (`VIEW` for an empty one) and the payload as text.
function checked(row: ViewRow): {sk: string; payload: string} {
  const payload = checkViewRow(row)
  const sk = resolveViewKey(row.pk, row.sk)
  const bytes = utf8Length(row.entityType) + utf8Length(row.pk) + utf8Length(sk)
  if (bytes > MAX_VIEW_INDEX_KEY_BYTES) {
    throw new EvtError(
      'invalidArgument',
      `view key too long for PostgreSQL: entity type, pk and sk are ${bytes} bytes together, ` +
        `the limit is ${MAX_VIEW_INDEX_KEY_BYTES}`,
    )
  }

  return {sk, payload}
}

// The upsert of one row: every column replaced, as DynamoDB's PutItem replaces the item.
function upsertSql(table: string): string {
  return (
    `INSERT INTO ${table} (pk, sk, entity_id, entity_type, payload, ttl) ` +
    'VALUES ($1, $2, $3, $4, $5, $6::bigint) ON CONFLICT (pk, sk) DO UPDATE SET ' +
    'entity_id = EXCLUDED.entity_id, entity_type = EXCLUDED.entity_type, ' +
    'payload = EXCLUDED.payload, ttl = EXCLUDED.ttl'
  )
}

// Decodes one row of COLUMNS. A ttl past the safe integers is refused rather than rounded.
function rowFrom(row: Row): ViewRow {
  const ttl = Number(text(row, 'ttl'))
  if (!Number.isSafeInteger(ttl)) {
    throw new EvtError(
      'invalidRecord',
      `a view row's ttl is not a safe integer: ${text(row, 'ttl')}`,
    )
  }

  return {
    pk: text(row, 'pk'),
    sk: text(row, 'sk'),
    entityId: text(row, 'entity_id'),
    entityType: text(row, 'entity_type'),
    payload: utf8Encode(text(row, 'payload')),
    ttl,
  }
}
