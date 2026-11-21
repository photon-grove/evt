// Journal wake-ups on PostgreSQL (ADR 0008, storage format, "Journal on PostgreSQL"): a journaled
// commit through the views table runs `pg_notify` on the journal channel once per scope it wrote,
// and a JournalListener turns those notifications into wake-ups.
//
// A wake-up is not a delivery: the consumer pages the scope from its own cursor, as with a DynamoDB
// stream record. Notifications sent while the listener is disconnected are lost, so the listener
// reports every connection (`connected`) and a quiet interval (`poll`), and the consumer pages
// every scope it follows then.

import {EvtError, TransientError} from '../errors.js'
import {checkScope} from '../journal/scope.js'
import type {CallOptions} from '../repository.js'
import type {PgQueryable} from './client.js'
import {journalChannel, pgTables} from './schema.js'

/** How long {@link JournalListener.next} waits before it returns a `poll` wake-up, by default. */
export const DEFAULT_POLL_INTERVAL_MS = 30_000

/** How long a connection attempt, and the health check after each `poll`, may take, by default. */
export const DEFAULT_CONNECT_TIMEOUT_MS = 10_000

/**
 * Why a consumer should read: a scope that has new rows (`scope`), every scope it follows after a
 * (re)connection, since notifications sent before it may be lost (`connected`), or every scope as
 * a backstop, at the poll interval (`poll`).
 */
export type Wakeup =
  | {readonly kind: 'scope'; readonly scope: string}
  | {readonly kind: 'connected'}
  | {readonly kind: 'poll'}

/** A notification as a driver reports it: `pg`'s `Notification` has this shape. */
export interface PgNotification {
  readonly channel: string
  readonly payload?: string | undefined
}

/**
 * The dedicated connection a {@link JournalListener} listens on, shaped like a connected `pg`
 * `Client`: it runs statements, reports notifications and the connection's end or failure as
 * events, and closes. `LISTEN` belongs to the session, so it must be a connection of its own, never
 * one a pool lends.
 */
export interface PgListenConnection extends PgQueryable {
  on(event: 'notification', listener: (message: PgNotification) => void): unknown
  on(event: 'error', listener: (error: Error) => void): unknown
  on(event: 'end', listener: () => void): unknown
  end(): Promise<void>
}

/** Options for a {@link JournalListener}. */
export interface JournalListenerOptions {
  /**
   * Opens a new dedicated connection, connected and ready for statements, such as
   * `async () => { const client = new pg.Client(url); await client.connect(); return client }`.
   * The listener calls it on its first {@link JournalListener.next} and after each lost connection.
   */
  readonly connect: () => Promise<PgListenConnection>
  /** The events table of the event log, as its repository has it: `evt_events` by default. */
  readonly eventsTable?: string | undefined
  /**
   * A `poll` wake-up comes this often, in milliseconds, from the last `poll` or `connected`,
   * however many notifications arrive in between. {@link DEFAULT_POLL_INTERVAL_MS} by default.
   */
  readonly pollIntervalMs?: number | undefined
  /**
   * Bounds a connection attempt with its `LISTEN`, and the health check after each `poll`, in
   * milliseconds. A health check that takes longer replaces the connection.
   * {@link DEFAULT_CONNECT_TIMEOUT_MS} by default.
   */
  readonly connectTimeoutMs?: number | undefined
}

/**
 * Listens on a PostgreSQL event log's journal channel, `{events}__evt_journal`, over one dedicated
 * connection it opens with the `connect` it was given, and turns its notifications into
 * {@link Wakeup}s, as Rust's `photon_evt::postgres::JournalListener` does.
 *
 * Call {@link next} in a loop. The first call connects and resolves `connected`, and so does the
 * call after a lost connection; a connection that can't be opened rejects, and the next call tries
 * again, so back off between failures. A `poll` comes at the poll interval whatever else arrives,
 * and the call after it first checks the connection with `SELECT 1`, replacing one that fails or
 * doesn't answer within the connect timeout (a half-open connection, after a failover).
 *
 * The driver reads notifications as they arrive, between calls too, and the listener keeps each
 * scope once until a call reports it: a wake-up says to page the scope, so a second one for a scope
 * not yet reported adds nothing. A consumer that stops calling holds at most one pending wake-up per
 * scope, and PostgreSQL's notification queue never waits on it. {@link close} ends the connection.
 */
export class JournalListener {
  /** The channel it listens on: `{events}__evt_journal`. */
  readonly channel: string
  readonly #connect: () => Promise<PgListenConnection>
  readonly #pollInterval: number
  readonly #connectTimeout: number

  // The open connection, or null: none yet, lost, or closed.
  #connection: PgListenConnection | null = null
  // When the next poll is due (milliseconds since the epoch): a fixed cadence from the last poll or
  // connected, so steady traffic on some scopes never starves the backstop for the others.
  #nextPoll = 0
  // Whether the health check the last poll asked for is still due.
  #checkDue = false
  // Scopes notified and not reported yet, in arrival order, each once.
  readonly #pending = new Set<string>()
  // Wakes a call waiting for a notification or for the connection's end.
  #wake: (() => void) | null = null
  #closed = false
  // Whether a call to next() is running: one at a time.
  #busy = false

  /**
   * Takes the connection factory and the event log's events table, which must be a name its
   * repository would accept, or the constructor fails with `invalidArgument`.
   */
  constructor(options: JournalListenerOptions) {
    if (typeof options?.connect !== 'function') {
      throw new EvtError('invalidArgument', 'a journal listener needs a connect function')
    }

    this.channel = journalChannel(pgTables(options.eventsTable))
    this.#connect = options.connect
    this.#pollInterval = duration(options.pollIntervalMs, DEFAULT_POLL_INTERVAL_MS, 'poll interval')
    this.#connectTimeout = duration(
      options.connectTimeoutMs,
      DEFAULT_CONNECT_TIMEOUT_MS,
      'connect timeout',
    )
  }

  /**
   * The next wake-up. When it isn't connected, it connects and resolves `connected`. A `poll` is
   * resolved the moment it is due, ahead of any pending scope, and the call after it first checks
   * the connection. An abort rejects with the signal's reason and loses nothing: a pending scope
   * stays pending, and an interrupted check runs again on the next call.
   */
  async next(options?: CallOptions): Promise<Wakeup> {
    if (this.#busy) {
      throw new EvtError('invalidArgument', 'a journal listener takes one call to next() at a time')
    }

    this.#busy = true
    try {
      return await this.#next(options?.signal)
    } finally {
      this.#busy = false
    }
  }

  async #next(signal: AbortSignal | undefined): Promise<Wakeup> {
    // Each step needs the one before it.
    /* oxlint-disable no-await-in-loop */
    for (;;) {
      signal?.throwIfAborted()
      if (this.#closed) {
        throw closedError()
      }

      // The backstop first: a backlog of pending scopes must not hold it back.
      if (this.#connection !== null && Date.now() >= this.#nextPoll) {
        return this.#poll()
      }

      const scope = this.#pending.values().next()
      if (!scope.done) {
        this.#pending.delete(scope.value)

        return {kind: 'scope', scope: scope.value}
      }

      const connection = this.#connection
      if (connection === null) {
        await this.#open(signal)
        this.#checkDue = false
        this.#nextPoll = Date.now() + this.#pollInterval

        return {kind: 'connected'}
      }

      // The health check the last poll asked for: a half-open connection is replaced rather than
      // polled forever.
      if (this.#checkDue) {
        const alive = await within(connection.query('SELECT 1'), this.#connectTimeout, signal).then(
          () => true,
          (error: unknown) => {
            signal?.throwIfAborted()
            void error

            return false
          },
        )
        this.#checkDue = false
        if (!alive) {
          this.#drop(connection)
        }

        continue
      }

      if (await this.#wait(signal)) {
        return this.#poll()
      }
    }
    /* oxlint-enable no-await-in-loop */
  }

  /** Ends the connection. A later {@link next} rejects. */
  async close(): Promise<void> {
    this.#closed = true
    const connection = this.#connection
    this.#connection = null
    this.#pending.clear()
    this.#wake?.()
    if (connection !== null) {
      await connection.end().catch(() => undefined)
    }
  }

  #poll(): Wakeup {
    this.#nextPoll = Date.now() + this.#pollInterval
    this.#checkDue = true

    return {kind: 'poll'}
  }

  // Waits for a notification, the connection's end, or the poll's deadline, and resolves whether
  // the deadline came first.
  async #wait(signal: AbortSignal | undefined): Promise<boolean> {
    let timer: ReturnType<typeof setTimeout> | undefined
    let onAbort: (() => void) | undefined
    try {
      return await new Promise<boolean>((resolve, reject) => {
        this.#wake = () => resolve(false)
        timer = setTimeout(() => resolve(true), Math.max(0, this.#nextPoll - Date.now()))
        if (signal !== undefined) {
          onAbort = () => reject(signal.reason)
          signal.addEventListener('abort', onAbort, {once: true})
        }
      })
    } finally {
      this.#wake = null
      clearTimeout(timer)
      if (onAbort !== undefined) {
        signal?.removeEventListener('abort', onAbort)
      }
    }
  }

  // Opens a connection and runs LISTEN on it, both within one connect timeout. A connection that
  // opens after the timeout, an abort or close() is ended as soon as it does.
  async #open(signal: AbortSignal | undefined): Promise<void> {
    const deadline = Date.now() + this.#connectTimeout
    const opening = this.#connect()
    let connection: PgListenConnection
    try {
      connection = await within(opening, this.#connectTimeout, signal)
    } catch (error) {
      opening
        .then((late) => {
          late.on('error', () => undefined)
          return late.end()
        })
        .catch(() => undefined)

      throw error
    }
    if (this.#closed) {
      connection.on('error', () => undefined)
      connection.end().catch(() => undefined)

      throw closedError()
    }

    // Events from a connection that is no longer the listener's are ignored.
    connection.on('notification', (message) => {
      if (this.#connection === connection && message.channel === this.channel) {
        this.#receive(message.payload)
      }
    })
    const lost = () => {
      if (this.#connection === connection) {
        this.#drop(connection)
      }
    }
    connection.on('error', lost)
    connection.on('end', lost)

    this.#connection = connection
    try {
      const left = Math.max(1, deadline - Date.now())
      await within(connection.query(`LISTEN ${this.channel}`), left, signal)
    } catch (error) {
      this.#drop(connection)

      throw this.#closed ? closedError() : error
    }
    if (this.#closed) {
      this.#drop(connection)

      throw closedError()
    }
  }

  // Queues the scope a notification names, once, when it is a valid scope.
  #receive(payload: string | undefined): void {
    try {
      this.#pending.add(checkScope(payload ?? ''))
    } catch {
      return
    }
    this.#wake?.()
  }

  // Forgets a connection that ended or failed, and ends it.
  #drop(connection: PgListenConnection): void {
    if (this.#connection === connection) {
      this.#connection = null
    }
    connection.end().catch(() => undefined)
    this.#wake?.()
  }
}

function closedError(): EvtError {
  return new EvtError('invalidArgument', 'the journal listener is closed')
}

// Resolves what `promise` resolves, or rejects when `ms` pass first or the signal aborts.
async function within<T>(promise: Promise<T>, ms: number, signal?: AbortSignal): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  let onAbort: (() => void) | undefined
  try {
    return await new Promise<T>((resolve, reject) => {
      timer = setTimeout(
        () => reject(new TransientError(`PostgreSQL did not answer within ${ms} ms`)),
        ms,
      )
      if (signal !== undefined) {
        onAbort = () => reject(signal.reason)
        signal.addEventListener('abort', onAbort, {once: true})
      }
      promise.then(resolve, reject)
    })
  } finally {
    clearTimeout(timer)
    if (onAbort !== undefined) {
      signal?.removeEventListener('abort', onAbort)
    }
  }
}

// The longest delay a timer takes: a longer one fires at once.
const MAX_TIMER_MS = 2_147_483_647

// A whole number of milliseconds from 1 to MAX_TIMER_MS, or the default.
function duration(value: number | undefined, fallback: number, what: string): number {
  if (value === undefined) {
    return fallback
  }
  if (!Number.isSafeInteger(value) || value < 1 || value > MAX_TIMER_MS) {
    throw new EvtError(
      'invalidArgument',
      `a journal listener's ${what} is 1 to ${MAX_TIMER_MS} ms; got ${value}`,
    )
  }

  return value
}
