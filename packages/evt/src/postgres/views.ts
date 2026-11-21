// Commit-wide view operations on PostgreSQL (ADR 0004, C7). evt defines no PostgreSQL view schema:
// the application owns its view tables and their DDL, and a repository's view writer turns each
// operation into the statement that writes it. The repository runs the statements inside the
// commit's transaction, after the events and before the snapshots, and reads each condition's
// verdict from the statement's outcome, as Go's and Rust's repositories do.

import {EvtError, ProjectorError, ProjectorOperationError} from '../errors.js'
import {type ProjectedView, REASON_CONDITION_FAILED, type ViewOperation} from '../projection.js'
import type {PgResult} from './client.js'

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
