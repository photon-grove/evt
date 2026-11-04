// The PostgreSQL client a PgRepository takes: a structural interface that `pg`'s Pool, Neon's
// serverless Pool and PGlite satisfy without an adapter, so this package imports no driver.

/** The result of one statement: its rows, as the driver returns them. */
export interface PgResult {
  readonly rows: readonly unknown[]
}

/** Something that runs one statement: `$1`, `$2` and so on in `text` take `values` in order. */
export interface PgQueryable {
  query(text: string, values?: unknown[]): Promise<PgResult>
}

/**
 * A connection held for one transaction, as a pool lends it. `release` returns it to the pool, and
 * gets the error when the connection may be unusable, so the pool can discard it.
 */
export interface PgConnection extends PgQueryable {
  release(error?: Error): void
}

/**
 * The client a {@link PgRepository} runs on.
 *
 * - A pool, such as `pg`'s `Pool` or Neon's serverless `Pool`, has `connect`, which lends one
 *   connection for each transaction. Single statements outside a transaction go to `query`.
 * - A single connection without `connect`, such as PGlite, runs everything itself, so every
 *   repository on it runs one operation at a time on it. Nothing else may use the connection
 *   meanwhile: a statement sent inside a repository's transaction would join it.
 *
 * Every value the repository sends is a string or null, with casts in the SQL, and every column it
 * reads is text, so no driver's type parsing touches a stored value. A `pg` `Client` has a
 * `connect` that opens the client rather than lending a connection, so pass a `Pool` instead.
 */
export interface PgClient extends PgQueryable {
  connect?(): Promise<PgConnection>
}
