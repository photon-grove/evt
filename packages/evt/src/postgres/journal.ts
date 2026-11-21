// The journal's reads on PostgreSQL (ADR 0008, storage format, "Journal on PostgreSQL"): the rows,
// counters and compaction rows are rows of the views table, `{events}__evt_views`, column for
// attribute, which the commit path writes with `viewsTable: true`. Each read is one statement
// outside a transaction, so it sees every committed transaction, and a gap is never a row still in
// flight.

import {EvtError} from '../errors.js'
import {compactionPartitionKey} from '../journal/compaction.js'
import {type JournalRow, journalRowFromView, readCounter} from '../journal/row.js'
import {
  COUNTER_SORT_KEY,
  checkScope,
  counterPartitionKey,
  parseRowSortKey,
  rowPartitionKey,
  rowSortKey,
} from '../journal/scope.js'
import type {CallOptions} from '../repository.js'
import type {ViewRow} from '../views.js'
import type {PgClient} from './client.js'
import {query, text} from './session.js'
import {VIEW_COLUMNS, viewRowFrom} from './view-repository.js'

/** The counters of `scopes`, in the order given, in one statement: 0 for a scope with none. */
export async function readCounters(
  client: PgClient,
  table: string,
  scopes: readonly string[],
  options: CallOptions | undefined,
): Promise<bigint[]> {
  options?.signal?.throwIfAborted()
  if (scopes.length === 0) {
    return []
  }

  // The keys travel as one JSON array, since every value the repository sends is a string. A
  // valid scope holds no lone surrogate, so the array is valid JSON text.
  for (const scope of scopes) {
    checkScope(scope)
  }
  const rows = await query(
    client,
    `SELECT ${VIEW_COLUMNS} FROM ${table} ` +
      'WHERE sk = $1 AND pk IN (SELECT jsonb_array_elements_text($2::jsonb))',
    [COUNTER_SORT_KEY, JSON.stringify(scopes.map(counterPartitionKey))],
  )
  const found = new Map(rows.map(viewRowFrom).map((row) => [row.pk, row]))

  return scopes.map((scope) => {
    const row = found.get(counterPartitionKey(scope))

    return row === undefined ? 0n : readCounter(scope, row)
  })
}

/** The highest position with a row in the scope: 0 when it has none. */
export async function readLastPosition(
  client: PgClient,
  table: string,
  scope: string,
  options: CallOptions | undefined,
): Promise<bigint> {
  options?.signal?.throwIfAborted()
  const pk = rowPartitionKey(checkScope(scope))
  const rows = await query(
    client,
    `SELECT sk FROM ${table} WHERE pk = $1 ORDER BY sk DESC LIMIT 1`,
    [pk],
  )
  if (rows[0] === undefined) {
    return 0n
  }

  const sk = text(rows[0], 'sk')
  const position = parseRowSortKey(sk)
  if (position === null) {
    throw new EvtError(
      'invalidRecord',
      `journal row ${JSON.stringify(pk)}/${JSON.stringify(sk)} has no position`,
    )
  }

  return position
}

/** Up to `limit` rows of the scope above `after`, ascending by sort key bytes, in one statement. */
export async function readRows(
  client: PgClient,
  table: string,
  scope: string,
  after: bigint,
  limit: number,
  options: CallOptions | undefined,
): Promise<JournalRow[]> {
  options?.signal?.throwIfAborted()
  const rows = await query(
    client,
    `SELECT ${VIEW_COLUMNS} FROM ${table} WHERE pk = $1 AND sk > $2 ORDER BY sk LIMIT $3::bigint`,
    [rowPartitionKey(checkScope(scope)), rowSortKey(after), String(limit)],
  )

  return rows.map((row) => journalRowFromView(scope, viewRowFrom(row)))
}

/** A row of the scope's journal compaction partition at `sk`, or null, in one statement. */
export async function readCompactionRow(
  client: PgClient,
  table: string,
  scope: string,
  sk: string,
  options: CallOptions | undefined,
): Promise<ViewRow | null> {
  options?.signal?.throwIfAborted()
  const rows = await query(
    client,
    `SELECT ${VIEW_COLUMNS} FROM ${table} WHERE pk = $1 AND sk = $2`,
    [compactionPartitionKey(checkScope(scope)), sk],
  )

  return rows[0] === undefined ? null : viewRowFrom(rows[0])
}
