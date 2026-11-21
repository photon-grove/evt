// The application-owned view table the live PostgreSQL runs write commit-wide view rows to, and
// the view writer for it. evt defines no PostgreSQL view schema, so every driver creates this one,
// and every driver's writer issues these statements (Go's PostgresViewDDL and PostgresViewWriter
// in go/internal/compat/views.go), which the commit budget sizes by their text.

import type {ViewOperation} from '@photon-grove/evt'
import type {ViewStatement, ViewWriter} from '@photon-grove/evt/postgres'

/** The view table of the cross-language run. */
export const POSTGRES_VIEW_TABLE = 'evt_compat_views'

const decoder = new TextDecoder('utf-8', {fatal: true, ignoreBOM: true})

/** The DDL of a view table, `evt_compat_views` unless a suite names its own. */
export function postgresViewDdl(table = POSTGRES_VIEW_TABLE): string {
  return `CREATE TABLE IF NOT EXISTS ${table} (
    pk          text NOT NULL,
    sk          text NOT NULL,
    entity_id   text NOT NULL,
    entity_type text NOT NULL,
    payload     text NOT NULL,
    PRIMARY KEY (pk, sk)
)`
}

/**
 * The writer of a view table, whose `entity_id` column holds the row's owner, which every
 * statement enforces:
 *
 * - a put that requires the row to be absent is a plain INSERT, and the unique violation of the
 *   primary key (`<table>_pkey`) is its failed condition;
 * - any other put overwrites only the owner's row: the conflict update is guarded by the owner, and
 *   a row of another owner leaves it affecting nothing;
 * - a delete that requires the row deletes the owner's row, and affecting nothing fails it;
 * - any other delete deletes the owner's row if there is one, and reports the rows of another owner
 *   it found, which fail it.
 */
export function postgresViewWriter(table = POSTGRES_VIEW_TABLE): ViewWriter {
  return (op: ViewOperation): ViewStatement => {
    if (op.kind === 'delete') {
      const args = [op.pk, op.sk, op.owner]
      if (op.condition === 'present') {
        return {sql: `DELETE FROM ${table} WHERE pk = $1 AND sk = $2 AND entity_id = $3`, args}
      }

      return {
        sql:
          `WITH gone AS (DELETE FROM ${table} WHERE pk = $1 AND sk = $2 AND entity_id = $3) ` +
          `SELECT 1 FROM ${table} WHERE pk = $1 AND sk = $2 AND entity_id <> $3`,
        args,
      }
    }

    const args = [op.pk, op.sk, op.owner, op.entityType, decoder.decode(op.payload)]
    const insert =
      `INSERT INTO ${table} (pk, sk, entity_id, entity_type, payload) ` +
      'VALUES ($1, $2, $3, $4, $5)'
    if (op.condition === 'absent') {
      return {sql: insert, args, keyConstraint: `${table}_pkey`}
    }

    return {
      sql:
        `${insert} ON CONFLICT (pk, sk) DO UPDATE SET entity_type = EXCLUDED.entity_type, ` +
        `payload = EXCLUDED.payload WHERE ${table}.entity_id = EXCLUDED.entity_id`,
      args,
    }
  }
}
