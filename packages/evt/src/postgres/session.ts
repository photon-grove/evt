// How the PostgreSQL repository and its view repository talk to a client: single statements, and
// READ COMMITTED transactions on a connection of their own, run one at a time on a client that is
// one connection. Each statement of a transaction needs the one before it.

import {EvtError, IndeterminateError, TransientError} from '../errors.js'
import type {CallOptions} from '../repository.js'
import type {PgClient, PgConnection, PgQueryable, PgResult} from './client.js'
import {viewCommitError} from './views.js'

/** A row as the driver returns it. Every column the repositories read is text. */
export type Row = Record<string, unknown>

/** One statement inside a transaction: it checks the signal first, so an abort rolls back. */
export type Statement = (text: string, values?: (string | null)[]) => Promise<Row[]>

/**
 * A transaction's statements. `exec` runs a statement with arguments of any type (a view writer's)
 * and resolves the driver's whole result, its counts included.
 */
export interface TxStatement extends Statement {
  exec(text: string, values: unknown[]): Promise<PgResult>
}

/**
 * How a transaction's failures read: whether it records an outcome (`indeterminate`) and whether
 * it carries view statements (`views`).
 */
export interface TransactionMode {
  readonly indeterminate: boolean
  readonly views: boolean
}

// SQLSTATEs for which PostgreSQL rolled the transaction back and a retry may not hit again: a
// serialization failure and a detected deadlock.
const TRANSIENT_STATES = new Set(['40001', '40P01'])

// The shape of a SQLSTATE: five digits or capital letters. No class starts with E, so a Node
// system error code such as EPIPE never matches.
const SQLSTATE = /^[0-9A-DF-Z][0-9A-Z]{4}$/

/** One statement outside a transaction. */
export async function query(
  client: PgClient,
  statement: string,
  values?: unknown[],
): Promise<Row[]> {
  if (client.connect !== undefined) {
    return rowsOf(await client.query(statement, values))
  }

  return serialize(client, async () => rowsOf(await client.query(statement, values)))
}

/**
 * Runs `work` in one READ COMMITTED transaction on a connection of its own, and commits it. A
 * failure, an abort included, rolls it back. A serialization failure or a deadlock is transient.
 * For a commit that records an outcome (`indeterminate`), a `COMMIT` whose result is unknown is
 * an IndeterminateError.
 */
export async function transaction<T>(
  client: PgClient,
  options: CallOptions | undefined,
  mode: TransactionMode | boolean,
  work: (run: TxStatement) => Promise<T>,
): Promise<T> {
  const flags = typeof mode === 'boolean' ? {indeterminate: mode, views: false} : mode
  const signal = options?.signal
  signal?.throwIfAborted()

  if (client.connect === undefined) {
    return serialize(client, () => inTransaction(client, signal, flags, work))
  }

  const connection = await client.connect()
  if (typeof connection?.query !== 'function' || typeof connection.release !== 'function') {
    if (typeof connection?.release === 'function') {
      connection.release()
    }
    throw new EvtError(
      'invalidArgument',
      "the PostgreSQL client's connect() lent no connection: pass a pool, such as pg's Pool",
    )
  }

  let broken: Error | undefined
  try {
    return await inTransaction(connection, signal, flags, work, (error) => {
      broken = error
    })
  } finally {
    connection.release(broken)
  }
}

// The operations queued on each single-connection client, shared by every repository on it: two
// transactions on one session would share its locks, and one's ROLLBACK would undo the other.
const queues = new WeakMap<PgQueryable, Promise<unknown>>()

// Runs `task` on a single-connection client after every task queued on it before.
function serialize<T>(client: PgQueryable, task: () => Promise<T>): Promise<T> {
  const result = (queues.get(client) ?? Promise.resolve()).then(task, task)
  queues.set(
    client,
    result.catch(() => undefined),
  )

  return result
}

// One transaction on one connection. `broken` hears of a connection that failed to roll back.
// With `indeterminate`, a COMMIT that fails without a database error (the connection dropped while
// it was in flight) and an abort while it was in flight are IndeterminateErrors: the transaction
// may have applied. With `views`, an integrity violation at COMMIT is a ProjectorError.
async function inTransaction<T>(
  connection: PgQueryable | PgConnection,
  signal: AbortSignal | undefined,
  {indeterminate, views}: TransactionMode,
  work: (run: TxStatement) => Promise<T>,
  broken?: (error: Error) => void,
): Promise<T> {
  const exec = async (statement: string, values: unknown[]): Promise<PgResult> => {
    signal?.throwIfAborted()
    const result = await connection.query(statement, values)
    rowsOf(result)

    return result
  }
  const run: TxStatement = Object.assign(
    async (statement: string, values?: (string | null)[]) => {
      signal?.throwIfAborted()

      return rowsOf(await connection.query(statement, values))
    },
    {exec},
  )

  let result: T
  let committing = false
  try {
    await connection.query('START TRANSACTION ISOLATION LEVEL READ COMMITTED')
    result = await work(run)
    signal?.throwIfAborted()
    committing = true
    await connection.query('COMMIT')
  } catch (error) {
    try {
      await connection.query('ROLLBACK')
    } catch (rollback) {
      broken?.(rollback instanceof Error ? rollback : new Error(String(rollback)))
    }

    const failedViews = committing && views ? viewCommitError(error, sqlState(error)) : null
    if (failedViews !== null) {
      throw failedViews
    }
    if (committing && indeterminate && sqlState(error) === null) {
      throw new IndeterminateError({cause: error})
    }

    throw transient(error)
  }

  // An abort while the COMMIT was in flight rejects too, though the commit may have applied, as
  // CallOptions promises: there is nothing left to roll back.
  if (indeterminate && signal?.aborted) {
    throw new IndeterminateError({cause: signal.reason})
  }
  signal?.throwIfAborted()

  return result
}

/** The SQLSTATE of an error PostgreSQL raised, or null for any other error. */
export function sqlState(error: unknown): string | null {
  const code = typeof error === 'object' && error !== null ? (error as {code?: unknown}).code : null

  return typeof code === 'string' && SQLSTATE.test(code) ? code : null
}

// A driver error PostgreSQL raised for a rollback a retry may not hit again, as a TransientError.
function transient(error: unknown): unknown {
  const code = sqlState(error)
  if (code !== null && TRANSIENT_STATES.has(code)) {
    return new TransientError(`PostgreSQL rolled the transaction back (${code})`, {cause: error})
  }

  return error
}

/** A result's rows, refused when the client returned none. */
export function rowsOf(result: {rows?: readonly unknown[]} | undefined): Row[] {
  const rows = result?.rows
  if (!Array.isArray(rows)) {
    throw new EvtError('invalidRecord', 'the PostgreSQL client returned a result without rows')
  }

  return rows as Row[]
}

/** A text column of a row that may be NULL. */
export function nullableText(row: Row, column: string): string | null {
  return row[column] === null ? null : text(row, column)
}

/** A text column of a row. */
export function text(row: Row, column: string): string {
  const value = row[column]
  if (typeof value !== 'string') {
    throw new EvtError('invalidRecord', `the PostgreSQL row has no text column ${column}`)
  }

  return value
}
