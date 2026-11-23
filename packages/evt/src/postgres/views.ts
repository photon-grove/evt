// Commit-wide view operations on PostgreSQL (ADR 0004, C7). A repository writes them either to the
// evt-owned views table (ADR 0008), with statements evt builds, or to the application's own view
// tables, through a view writer that turns each operation into the statement that writes it. The
// repository runs the statements inside the commit's transaction, after the events and before the
// snapshots, and reads each condition's verdict from the statement's outcome, as Go's and Rust's
// repositories do.

import {EvtError, ProjectorError, ProjectorOperationError, ViewKeyTooLongError} from '../errors.js'
import {type ProjectedView, REASON_CONDITION_FAILED, type ViewOperation} from '../projection.js'
import {utf8Decode, utf8Length} from '../utf8.js'
import {checkViewText} from '../views.js'
import type {PgResult} from './client.js'
import {MAX_VIEW_INDEX_KEY_BYTES} from './schema.js'

/**
 * The statement that writes one view operation to the application's view table: the text, with
 * `$1`-style placeholders, and its arguments, which go to the driver as they are.
 */
export interface ViewStatement {
  readonly sql: string
  readonly args?: readonly unknown[]
  /**
   * The name of the table's unique or primary-key constraint on `(pk, sk)`. It is required for a
   * put with the `absent` condition: only a violation of this constraint means the row already
   * exists. A unique violation of any other constraint of the table (a unique payload field, say)
   * says nothing about the requested row, so it is an ordinary failure of the statement, with the
   * SQLSTATE as its reason.
   */
  readonly keyConstraint?: string
  /**
   * The statement returns one row for each row it affects (a `RETURNING` clause), so its rows are
   * its count, and none means it affected none, whatever the driver reports. Set it for a driver
   * that reports no affected-row count. A delete without a condition is counted by its rows
   * already, since it reports foreign rows with a `SELECT`.
   */
  readonly returning?: boolean
}

/**
 * Turns one commit-wide view operation into the statement that writes it.
 *
 * The writer must store the operation's `owner` with the row (the application's own column) and
 * enforce it, because the owner of a row must never change. The repository can't see the table,
 * so it decides whether a condition held from the statement's outcome, and the statement must have
 * these semantics:
 *
 * - a put with the `absent` condition raises a unique violation of the `(pk, sk)` key constraint
 *   when the row exists: write a plain `INSERT`, with no `ON CONFLICT` clause, and name the
 *   constraint in {@link ViewStatement.keyConstraint};
 * - any other put overwrites the owner's row and refuses another owner's:
 *   `INSERT ... ON CONFLICT (pk, sk) DO UPDATE ... WHERE <table>.owner = EXCLUDED.owner`. It
 *   affects one row, or none when the row belongs to another owner, which fails the condition;
 * - a delete with the `present` condition deletes the owner's row: `DELETE ... WHERE` pk, sk and
 *   owner. Affecting no row (the row is absent or another owner's) fails the condition;
 * - any other delete succeeds whether or not it deleted a row, and refuses another owner's row.
 *   Its statement reports the foreign rows it found instead of the rows it deleted:
 *   `WITH gone AS (DELETE ... WHERE pk, sk, and owner) SELECT 1 FROM <table> WHERE pk, sk, and
 *   owner <> $owner`. Reporting a row fails the condition.
 *
 * A statement's count is the rows it returns when it returns any, and otherwise the count the
 * driver reports (`pg` and Neon report `rowCount`, PGlite `affectedRows`). With a driver that
 * reports neither, add `RETURNING 1` to the puts and the `present` deletes and mark them
 * {@link ViewStatement.returning}. A failed condition is
 * a `ProjectorOperationError` with the reason `ConditionalCheckFailed`; any other error the
 * database raises is one too, with its SQLSTATE as the reason. An error the writer throws fails
 * the commit before any write.
 */
export type ViewWriter = (op: ViewOperation) => ViewStatement

/** A view operation and the statement its writer built. */
export interface BuiltView {
  readonly view: ProjectedView
  readonly statement: ViewStatement
}

/**
 * Builds a commit's view statements, in the order the commit executes them. It runs before the
 * budget is checked and before any write, so a writer that refuses an operation fails the whole
 * commit with nothing written.
 */
export function buildViews(views: readonly ProjectedView[], writer: ViewWriter): BuiltView[] {
  return views.map((view) => {
    const expected = view.op.expectedPayload
    if (expected !== null && expected !== undefined) {
      throw invalidView(view, 'a view writer cannot apply an expected payload')
    }
    const statement = writer(view.op)
    if (typeof statement?.sql !== 'string' || statement.sql.trim() === '') {
      throw invalidView(view, 'the view writer returned an empty statement')
    }
    if (statement.args !== undefined && !Array.isArray(statement.args)) {
      throw invalidView(view, "the view writer's arguments are not an array")
    }
    const absent = view.op.kind === 'put' && view.op.condition === 'absent'
    if (absent && !statement.keyConstraint) {
      throw invalidView(
        view,
        'the view writer must name the key constraint of a put that requires the row to be absent',
      )
    }

    return {view, statement}
  })
}

/**
 * Builds a commit's statements on the views table `table`, with the DynamoDB repository's owner
 * rules, as Rust's `with_views_table()` builds them (storage format, "PostgreSQL views table").
 * It refuses, before any write, what the table can't store: a payload that isn't UTF-8, U+0000 or
 * a lone surrogate in any text, and a key too long for the btree indexes
 * ({@link ViewKeyTooLongError}). Every counted statement returns its rows, so its count doesn't
 * depend on the driver.
 */
export function viewsTableStatements(table: string, views: readonly ProjectedView[]): BuiltView[] {
  return views.map((view) => ({view, statement: viewsTableStatement(table, view)}))
}

function viewsTableStatement(table: string, view: ProjectedView): ViewStatement {
  const {op} = view
  const expectedBytes = op.expectedPayload ?? null
  let payload: string
  let expected: string | null = null
  try {
    payload = utf8Decode(op.payload, 'view payload')
    if (expectedBytes !== null) {
      expected = utf8Decode(expectedBytes, 'expected view payload')
    }
    for (const [field, value] of [
      ['pk', op.pk],
      ['sk', op.sk],
      ['owner', op.owner],
      ['entity type', op.entityType],
      ['payload', payload],
      ['expected payload', expected ?? ''],
    ] as const) {
      checkViewText(value, field)
    }
  } catch (error) {
    throw invalidView(view, `the views table can't store it: ${(error as Error).message}`)
  }

  const bytes = utf8Length(op.entityType) + utf8Length(op.pk) + utf8Length(op.sk)
  if (bytes > MAX_VIEW_INDEX_KEY_BYTES) {
    throw new ViewKeyTooLongError(
      'entity type, pk and sk',
      bytes,
      MAX_VIEW_INDEX_KEY_BYTES,
      view.projector,
      view.operation,
    )
  }

  const key = [op.pk, op.sk, op.owner]
  const row = [...key, op.entityType, payload]
  const columns = '(pk, sk, entity_id, entity_type, payload, ttl)'
  if (op.kind === 'put') {
    // The engine's compare-and-set (the journal's counter): replace exactly the owner's row the
    // commit read. No row means the row moved, was another owner's, or is gone.
    if (expected !== null) {
      return {
        sql:
          `UPDATE ${table} SET entity_type = $4, payload = $5, ttl = 0 ` +
          'WHERE pk = $1 AND sk = $2 AND entity_id = $3 AND payload = $6 RETURNING 1',
        args: [...row, expected],
        returning: true,
      }
    }
    switch (op.condition) {
      // A plain insert: a violation of the key is the failed condition.
      case 'absent':
        return {
          sql: `INSERT INTO ${table} ${columns} VALUES ($1, $2, $3, $4, $5, 0)`,
          args: row,
          keyConstraint: `${table}_pkey`,
        }
      // An upsert that replaces only the owner's row: no row when another stream owns it.
      default:
        return {
          sql:
            `INSERT INTO ${table} ${columns} VALUES ($1, $2, $3, $4, $5, 0) ` +
            'ON CONFLICT (pk, sk) DO UPDATE SET entity_type = EXCLUDED.entity_type, ' +
            `payload = EXCLUDED.payload, ttl = 0 WHERE ${table}.entity_id = EXCLUDED.entity_id ` +
            'RETURNING 1',
          args: row,
          returning: true,
        }
    }
  }

  // Deletes the owner's row; no row (absent, or another owner's) fails the condition.
  if (op.condition === 'present') {
    return {
      sql: `DELETE FROM ${table} WHERE pk = $1 AND sk = $2 AND entity_id = $3 RETURNING 1`,
      args: key,
      returning: true,
    }
  }

  // Deletes the owner's row if there is one, and reports another owner's row instead of the rows
  // it deleted: any row it reports fails the condition.
  return {
    sql:
      `WITH gone AS (DELETE FROM ${table} WHERE pk = $1 AND sk = $2 AND entity_id = $3) ` +
      `SELECT 1 FROM ${table} WHERE pk = $1 AND sk = $2 AND entity_id <> $3`,
    args: key,
  }
}

/**
 * The number of rows a view statement affected: the rows it returned, or the count the driver
 * reports when it returned none. A statement marked `returning`, and a delete without a condition,
 * which reports foreign rows with a `SELECT`, are counted by their rows alone. A driver that
 * reports no count can't serve any other statement that returns no rows.
 */
export function affectedRows(result: PgResult, {view, statement}: BuiltView): number {
  const reports = view.op.kind === 'delete' && view.op.condition !== 'present'
  if (statement.returning === true || reports) {
    return result.rows.length
  }

  const counts = [result.rows.length, result.rowCount, result.affectedRows].filter(
    (count): count is number => typeof count === 'number' && Number.isFinite(count),
  )
  if (result.rows.length === 0 && counts.length === 1) {
    throw invalidView(
      view,
      'the PostgreSQL client reported no affected-row count for a statement that returned no ' +
        'rows: add RETURNING 1 to the statement and mark it returning',
    )
  }

  return Math.max(...counts)
}

/**
 * Whether a statement's count says its operation's condition failed: a put without the `absent`
 * condition and a delete that requires the row affect no row when the owner's condition fails; a
 * delete without a condition reports a row only when another owner's holds the key. A put that
 * requires the row to be absent fails with a unique violation instead.
 */
export function viewConditionFailed(op: ViewOperation, affected: number): boolean {
  if (op.kind === 'put' && op.condition === 'absent') {
    return false
  }
  if (op.kind === 'put' || op.condition === 'present') {
    return affected === 0
  }

  return affected > 0
}

/**
 * Maps an error a view statement raised. A violation of the `(pk, sk)` key constraint on a put
 * that requires the row to be absent is its failed condition; any other error the database raised
 * is the operation's failure with the SQLSTATE as its reason, except a transaction rollback
 * (serialization failure, deadlock: class 40), which isn't the projector's doing and passes through
 * like it does for every other statement of the commit. An error the database didn't raise, such
 * as a lost connection, passes through too.
 */
export function mapViewError(built: BuiltView, error: unknown, state: string | null): unknown {
  if (state === null || state.startsWith('40')) {
    return error
  }

  const {view, statement} = built
  const violated = constraintOf(error)
  const conditionFailed =
    state === '23505' &&
    view.op.kind === 'put' &&
    view.op.condition === 'absent' &&
    statement.keyConstraint !== undefined &&
    violated !== null &&
    violated.toLowerCase() === statement.keyConstraint.toLowerCase()

  return new ProjectorOperationError(view, conditionFailed ? REASON_CONDITION_FAILED : state, error)
}

/**
 * Maps a failed `COMMIT` of a transaction that carried view operations. An integrity violation
 * (class 23) can only come from a deferred constraint on a projector's table, and names no
 * operation, so it's the projector's failure rather than the event log's.
 */
export function viewCommitError(error: unknown, state: string | null): ProjectorError | null {
  return state?.startsWith('23') ? new ProjectorError('(at commit)', error) : null
}

// The constraint a driver error names: `pg` and PGlite call it `constraint`.
function constraintOf(error: unknown): string | null {
  const name =
    typeof error === 'object' && error !== null
      ? (error as {constraint?: unknown}).constraint
      : undefined

  return typeof name === 'string' && name !== '' ? name : null
}

function invalidView(view: ProjectedView, reason: string): EvtError {
  return new EvtError(
    'invalidViewOperation',
    `invalid view operation: projector ${JSON.stringify(view.projector)} ` +
      `(#${view.projectorIndex}) operation ${view.operation}: ${reason}`,
  )
}
