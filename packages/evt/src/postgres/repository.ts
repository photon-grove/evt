// The PostgreSQL repository: events in one table keyed by (entity_id, sequence), snapshots in a
// second keyed by entity_id, and the engine rows (the feature marker, the rebuild fence, the
// stream heads) in tables derived from the events table's name (storage format, "PostgreSQL").
// Every commit is one READ COMMITTED transaction that runs the statements Go and Rust run, in
// their order: the entity lock, the feature-marker check under its shared lock, the rebuild-lock
// assertion under its own, the head statement, the event inserts, then the snapshot upsert.
//
// Each statement of a transaction needs the one before it, so they await in loops on purpose.
/* oxlint-disable no-await-in-loop */

import {
  CompactionUncoveredError,
  ConflictError,
  EvtError,
  FenceConflictError,
  HistoryDependentProjectionError,
  RebuildInProgressError,
  SequenceGapError,
  StreamHeadDriftError,
  StreamHeadsNotMigratedError,
  TransientError,
} from '../errors.js'
import type {SerializedEvent, SerializedSnapshot} from '../event.js'
import {
  type FeatureManifest,
  checkSupported,
  checkWritable,
  declaresStreamHeads,
  decodeManifest,
  encodeManifest,
  withStreamHeads,
} from '../features.js'
import {int64Argument} from '../fields.js'
import {decodeMetadata, emptyMetadata, encodeMetadata} from '../metadata.js'
import {
  type CallOptions,
  type Commit,
  type NormalizedTransaction,
  type StoreInitializer,
  type TransactionCommit,
  type TransactionalRepository,
  checkBudget,
  checkFenceRetention,
  checkNotReserved,
  normalizeTransaction,
  rankFailures,
  transactionSizes,
  validateCommit,
} from '../repository.js'
import {compareUtf8} from '../utf8.js'
import type {PgClient, PgConnection, PgQueryable} from './client.js'
import {
  ENGINE_LOCK_EXCLUSIVE,
  ENGINE_LOCK_SHARED,
  ENTITY_LOCK,
  type PgTables,
  SCHEMA_LOCK,
  featuresLockKey,
  featuresTable,
  pgTables,
  rebuildGenerationTable,
  rebuildLockKey,
  rebuildLockTable,
  schemaDdl,
  streamHeadsTable,
} from './schema.js'

/** The tables a {@link PgRepository} stores its event log in. */
export interface PgRepositoryOptions {
  /** The events table, `evt_events` by default. The engine tables derive their names from it. */
  eventsTable?: string
  /** The snapshots table, `evt_snapshots` by default. */
  snapshotsTable?: string
}

/** The backend name a projector group for the PostgreSQL repository would carry. */
export const POSTGRES_BACKEND = 'postgres'

// The event columns every read selects, each as text, so no driver parses a stored value. An
// ORDER BY names the table's column, since a bare `sequence` would sort the text alias: 10
// before 9.
const EVENT_COLUMNS =
  'event_id, sequence::text AS sequence, event_type, version::text AS version, entity_id, ' +
  "entity_type, encode(payload, 'hex') AS payload, metadata::text AS metadata"

// SQLSTATEs for which PostgreSQL rolled the transaction back and a retry may not hit again: a
// serialization failure and a detected deadlock.
const TRANSIENT_STATES = new Set(['40001', '40P01'])

// One statement inside a transaction: it checks the signal first, so an abort rolls back.
type Statement = (text: string, values?: (string | null)[]) => Promise<Row[]>

type Row = Record<string, unknown>

/**
 * A {@link Repository} on PostgreSQL, through any client shaped like `pg`'s `Pool`
 * ({@link PgClient}).
 *
 * Run {@link PgRepository.ensureSchema} once before the first write. A writer initializes a fresh
 * store itself on its first write, as Go's and Rust's do: the check is two `EXISTS` probes under
 * the lock it takes anyway. {@link PgRepository.initialize} is for a deploy step that treats every
 * backend alike.
 *
 * The repository validates the store's feature manifest at its first write and caches the version
 * it validated. Every commit and snapshot write reads the manifest again under a shared lock that
 * no declaration can take while the write is open, and validates it again when it changed.
 */
export class PgRepository implements TransactionalRepository, StoreInitializer {
  readonly #client: PgClient
  readonly #tables: PgTables

  // The manifest version this writer validated, or null until it validates.
  #validated: bigint | null = null
  // Whether the rebuild-lock table exists, which a commit's assertion probes for until it does.
  #rebuildReady = false

  /**
   * Takes the client and the table names, which must be plain identifiers that fit their limits
   * ({@link pgTables}), or the constructor fails with `invalidArgument`.
   */
  constructor(client: PgClient, options: PgRepositoryOptions = {}) {
    this.#client = client
    this.#tables = pgTables(options.eventsTable, options.snapshotsTable)
  }

  /** The tables this repository reads and writes, folded to lower case. */
  get tables(): PgTables {
    return this.#tables
  }

  /**
   * Creates the event log's tables if they don't exist, with the DDL Go and Rust run
   * ({@link schemaDdl}), under the schema lock they take. Safe to call on every start.
   */
  async ensureSchema(options?: CallOptions): Promise<void> {
    await this.#transaction(options, async (run) => {
      await run(SCHEMA_LOCK, [this.#tables.events])
      for (const statement of statements(schemaDdl(this.#tables))) {
        await run(statement)
      }
    })
  }

  /**
   * Declares stream-heads on a store whose events and snapshots tables are empty, under the
   * manifest's exclusive lock, and resolves `true` when this call declared it. A store that
   * already declares it is left alone. Run {@link PgRepository.ensureSchema} first.
   */
  async initialize(options?: CallOptions): Promise<boolean> {
    try {
      return await this.#transaction(options, async (run) => {
        await run(ENGINE_LOCK_EXCLUSIVE, [featuresLockKey(this.#tables)])
        const manifest = await this.#readManifest(run)
        checkSupported(manifest)
        if (declaresStreamHeads(manifest)) {
          return false
        }

        const {events, snapshots} = this.#tables
        const populated = await run(
          `SELECT 1 WHERE EXISTS (SELECT 1 FROM ${events}) OR EXISTS (SELECT 1 FROM ${snapshots})`,
        )
        if (populated.length > 0) {
          throw new StreamHeadsNotMigratedError()
        }

        await this.#writeManifest(run, manifest, withStreamHeads(manifest))

        return true
      })
    } finally {
      // A declaration moved the version: the next write reads it again.
      this.#validated = null
    }
  }

  async commit(commit: Commit, options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    validateCommit(commit)
    checkNotReserved(commit.entityId)
    if (commit.events.length > 0) {
      await this.#write(normalizeTransaction({streams: [commit]}), options)
    }
  }

  /**
   * Commits every stream and checks every fence in one transaction, in the order Go and Rust run
   * it: every stream's entity lock, written and fenced, in ascending entity-ID byte order; the
   * marker check, the fences' retention and the rebuild-lock assertion; every head statement and
   * fence read, ranked; the event inserts; then the snapshot upserts.
   */
  async commitTransaction(transaction: TransactionCommit, options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    const normalized = normalizeTransaction(transaction)
    if (normalized.streams.length > 0 || normalized.fences.length > 0) {
      await this.#write(normalized, options)
    }
  }

  async #write(transaction: NormalizedTransaction, options?: CallOptions): Promise<void> {
    for (const group of transaction.projections) {
      if (group.backend !== POSTGRES_BACKEND || group.operations.length > 0) {
        throw new EvtError(
          'invalidCommit',
          `invalid commit: a PostgreSQL repository can't write a ${group.backend} projector group`,
        )
      }
    }

    // Every backend budgets a commit the same way, so an oversized one fails before any request.
    checkBudget(transactionSizes(transaction, () => undefined))
    await this.#ensureStreamHeads(options)

    await this.#transaction(options, async (run) => {
      // Each stream once, in the bytewise order that also pins the transaction, so two
      // transactions that share streams lock them in the same sequence and can't deadlock.
      const ids = [...transaction.streams, ...transaction.fences].map(({entityId}) => entityId)
      ids.sort(compareUtf8)
      for (const [index, entityId] of ids.entries()) {
        if (ids[index - 1] !== entityId) {
          await run(ENTITY_LOCK, [entityId])
        }
      }

      const manifest = await this.#checkManifest(run)
      const retention = manifest.retention
      checkFenceRetention(transaction, (entityType) => retention.includes(entityType))
      await this.#assertNoRebuildLock(run)

      // Every head statement and fence read runs before any failure is reported, so the ranking
      // sees them all.
      const moved: boolean[] = []
      for (const stream of transaction.streams) {
        const {entityId, events} = stream
        const first = events[0]!.sequence
        const last = events.at(-1)!.sequence
        moved.push(await this.#moveHead(run, entityId, events[0]!.entityType, first, last))
      }
      const stale = []
      for (const fence of transaction.fences) {
        if ((await this.#storedHead(run, fence.entityId)) !== fence.head) {
          stale.push(fence)
        }
      }

      const failures: EvtError[] = []
      for (const [index, stream] of transaction.streams.entries()) {
        if (!moved[index]) {
          failures.push(await this.#probe(run, stream.entityId, stream.events[0]!.sequence))
        }
      }
      for (const fence of stale) {
        // No first sequence: a fence that didn't drift is a fence conflict.
        const verdict = await this.#probe(run, fence.entityId, 0n)
        failures.push(
          verdict instanceof StreamHeadDriftError
            ? verdict
            : new FenceConflictError(fence.entityId, fence.head, fence.provenance),
        )
      }
      const failure = rankFailures(failures)
      if (failure !== undefined) {
        throw failure
      }

      for (const stream of transaction.streams) {
        await this.#insertEvents(run, stream)
      }
      for (const stream of transaction.streams) {
        await this.#writeCommitSnapshot(run, stream)
      }
    })
  }

  // A stream's event inserts, after its head statement passed. A row that already exists is an
  // event above the head: the entity lock rules out a racing writer.
  async #insertEvents(run: Statement, stream: Commit): Promise<void> {
    const first = stream.events[0]!.sequence
    for (const event of stream.events) {
      if (!(await this.#insertEvent(run, event))) {
        throw drift(stream.entityId, first - 1n, await this.#derivedHead(run, stream.entityId))
      }
    }
  }

  // A stream's commit snapshot, which must follow the stored one, as the DynamoDB put's condition
  // requires.
  async #writeCommitSnapshot(run: Statement, stream: Commit): Promise<void> {
    const {entityId, snapshot} = stream
    if (!snapshot) {
      return
    }

    const rows = await run(
      `SELECT sequence::text AS sequence FROM ${this.#tables.snapshots} WHERE entity_id = $1`,
      [entityId],
    )
    const stored = rows[0] === undefined ? null : BigInt(text(rows[0], 'sequence'))
    if (stored === null && snapshot.sequence !== 1n) {
      throw new ConflictError(
        entityId,
        `snapshot sequence ${snapshot.sequence} has no stored predecessor`,
      )
    }
    if (stored !== null && snapshot.sequence !== stored + 1n) {
      throw new ConflictError(
        entityId,
        `snapshot sequence moved: stored ${stored}, commit writes ${snapshot.sequence}`,
      )
    }

    const eventSequence = stream.events.at(-1)!.sequence
    await this.#upsertSnapshot(run, {...snapshot, entityId, eventSequence})
  }

  async events(entityId: string, options?: CallOptions): Promise<SerializedEvent[]> {
    options?.signal?.throwIfAborted()
    const rows = await this.#read(
      `SELECT ${EVENT_COLUMNS} FROM ${this.#tables.events} WHERE entity_id = $1 ` +
        `ORDER BY ${this.#tables.events}.sequence ASC`,
      [entityId],
    )

    return rows.map(eventFromRow)
  }

  async eventsAfter(
    entityId: string,
    after: bigint,
    options?: CallOptions,
  ): Promise<SerializedEvent[]> {
    options?.signal?.throwIfAborted()
    int64Argument(after, 'after')
    const rows = await this.#read(
      `SELECT ${EVENT_COLUMNS} FROM ${this.#tables.events} ` +
        'WHERE entity_id = $1 AND sequence > $2::bigint ' +
        `ORDER BY ${this.#tables.events}.sequence ASC`,
      [entityId, after.toString()],
    )

    return rows.map(eventFromRow)
  }

  async latest(entityId: string, n: number, options?: CallOptions): Promise<SerializedEvent[]> {
    options?.signal?.throwIfAborted()
    if (!Number.isSafeInteger(n) || n < 1) {
      throw new EvtError(
        'invalidArgument',
        `latest(n) requires a whole number of at least one event: got n = ${n}`,
      )
    }

    const rows = await this.#read(
      `SELECT ${EVENT_COLUMNS} FROM ${this.#tables.events} WHERE entity_id = $1 ` +
        `ORDER BY ${this.#tables.events}.sequence DESC LIMIT $2::bigint`,
      [entityId, n.toString()],
    )

    return rows.map(eventFromRow)
  }

  async snapshot(entityId: string, options?: CallOptions): Promise<SerializedSnapshot | null> {
    options?.signal?.throwIfAborted()
    const rows = await this.#read(
      'SELECT entity_type, entity_id, sequence::text AS sequence, ' +
        "event_sequence::text AS event_sequence, encode(payload, 'hex') AS payload " +
        `FROM ${this.#tables.snapshots} WHERE entity_id = $1`,
      [entityId],
    )

    return rows[0] === undefined ? null : snapshotFromRow(rows[0])
  }

  async putSnapshot(snapshot: SerializedSnapshot, options?: CallOptions): Promise<boolean> {
    options?.signal?.throwIfAborted()
    checkNotReserved(snapshot.entityId)
    int64Argument(snapshot.sequence, 'snapshot.sequence')
    const eventSequence = int64Argument(snapshot.eventSequence, 'snapshot.eventSequence')
    await this.#ensureStreamHeads(options)

    return this.#transaction(options, async (run) => {
      const {entityId} = snapshot
      await run(ENTITY_LOCK, [entityId])
      await this.#checkManifest(run)

      // The head must cover the snapshot. A snapshot of an empty stream needs no head. The entity
      // lock holds the head steady.
      const head = await this.#storedHead(run, entityId)
      if (eventSequence > 0n && head < eventSequence) {
        const derived = await this.#derivedHead(run, entityId)
        throw drift(entityId, head, derived > eventSequence ? derived : eventSequence)
      }

      return this.#upsertSnapshot(run, snapshot)
    })
  }

  async compactBelow(entityId: string, through: bigint, options?: CallOptions): Promise<number> {
    options?.signal?.throwIfAborted()
    checkNotReserved(entityId)
    if (int64Argument(through, 'through') < 1n) {
      return 0
    }

    // One transaction under the manifest's shared lock, held through the delete, so no
    // declaration lands between the check and the delete it authorizes. It never declares
    // stream-heads, and never touches the head.
    return this.#transaction(options, async (run) => {
      await run(ENGINE_LOCK_SHARED, [featuresLockKey(this.#tables)])
      const manifest = await this.#readManifest(run)
      checkWritable(manifest)

      const rows = await run(
        'SELECT entity_type, event_sequence::text AS event_sequence ' +
          `FROM ${this.#tables.snapshots} WHERE entity_id = $1`,
        [entityId],
      )
      const covered = rows[0] === undefined ? null : BigInt(text(rows[0], 'event_sequence'))
      if (covered === null || covered < through) {
        throw new CompactionUncoveredError(entityId, through, covered)
      }

      const entityType = text(rows[0]!, 'entity_type')
      if (manifest!.historyDependent.includes(entityType)) {
        throw new HistoryDependentProjectionError(entityType, entityId)
      }

      const deleted = await run(
        `WITH deleted AS (DELETE FROM ${this.#tables.events} ` +
          'WHERE entity_id = $1 AND sequence >= 1 AND sequence <= $2::bigint RETURNING 1) ' +
          'SELECT count(*)::text AS deleted FROM deleted',
        [entityId, through.toString()],
      )

      return Number(text(deleted[0]!, 'deleted'))
    })
  }

  // Makes sure the store is ready for this writer's first write: it declares stream-heads. A
  // writer initializes a store whose manifest lacks it, which refuses one that holds data. It runs
  // before the write's transaction, which holds the shared lock a declaration would wait for.
  async #ensureStreamHeads(options: CallOptions | undefined): Promise<void> {
    if (this.#validated !== null) {
      return
    }

    const manifest = await this.#readManifest((statement, values) =>
      this.#read(statement, values),
    )
    checkSupported(manifest)
    if (!declaresStreamHeads(manifest)) {
      await this.initialize(options)
    }
  }

  // The commit-time marker check, under the manifest's shared lock held to COMMIT, so a
  // declaration can't slip between the check and the write. A manifest that changed since this
  // writer validated it is validated again.
  async #checkManifest(run: Statement): Promise<FeatureManifest> {
    await run(ENGINE_LOCK_SHARED, [featuresLockKey(this.#tables)])
    const manifest = await this.#readManifest(run)
    if (manifest !== null && manifest.version === this.#validated) {
      return manifest
    }

    this.#validated = null
    checkWritable(manifest)
    this.#validated = manifest!.version

    return manifest!
  }

  // The commit-time rebuild-lock assertion, under the rebuild fence's shared lock held to COMMIT:
  // a lock row, an expired one included, or an odd generation refuses the commit. A schema older
  // than the fence has no lock table, and nobody can have taken the lock.
  async #assertNoRebuildLock(run: Statement): Promise<void> {
    await run(ENGINE_LOCK_SHARED, [rebuildLockKey(this.#tables)])
    const lock = rebuildLockTable(this.#tables)
    if (!this.#rebuildReady) {
      if ((await run('SELECT 1 WHERE to_regclass($1) IS NOT NULL', [lock])).length === 0) {
        return
      }

      this.#rebuildReady = true
    }

    const generation = rebuildGenerationTable(this.#tables)
    const held = await run(
      `SELECT 1 WHERE EXISTS (SELECT 1 FROM ${lock} WHERE id = 0) ` +
        `OR EXISTS (SELECT 1 FROM ${generation} WHERE id = 0 AND value % 2 <> 0)`,
    )
    if (held.length > 0) {
      throw new RebuildInProgressError()
    }
  }

  // The stream's head statement: a create inserts its head row, an append moves it from the
  // sequence before the commit's first. It passes when it wrote one row.
  async #moveHead(
    run: Statement,
    entityId: string,
    entityType: string,
    first: bigint,
    last: bigint,
  ): Promise<boolean> {
    const heads = streamHeadsTable(this.#tables)
    const rows =
      first <= 1n
        ? await run(
            `INSERT INTO ${heads} (entity_id, entity_type, head) VALUES ($1, $2, $3::bigint) ` +
              'ON CONFLICT (entity_id) DO NOTHING RETURNING 1',
            [entityId, entityType, last.toString()],
          )
        : await run(
            `UPDATE ${heads} SET head = $3::bigint WHERE entity_id = $1 AND head = $2::bigint ` +
              'RETURNING 1',
            [entityId, (first - 1n).toString(), last.toString()],
          )

    return rows.length === 1
  }

  // Classifies a failed head statement under the stream's lock, as Go and Rust do: a stored head
  // below the head the log derives is drift, an equal one leaves a gap or a conflict, and one
  // above it is drift unless deriving again catches up with it.
  async #probe(run: Statement, entityId: string, first: bigint): Promise<EvtError> {
    const derived = await this.#derivedHead(run, entityId)
    const stored = await this.#storedHead(run, entityId)
    if (stored < derived) {
      return drift(entityId, stored, derived)
    }
    if (stored > derived) {
      const again = await this.#derivedHead(run, entityId)
      return again < stored ? drift(entityId, stored, again) : conflict(entityId)
    }
    if (stored < first - 1n) {
      return new SequenceGapError(entityId, stored, first)
    }

    return conflict(entityId)
  }

  // The stream's stored head, 0 when it has no head row.
  async #storedHead(run: Statement, entityId: string): Promise<bigint> {
    const rows = await run(
      `SELECT head::text AS head FROM ${streamHeadsTable(this.#tables)} WHERE entity_id = $1`,
      [entityId],
    )

    return rows[0] === undefined ? 0n : BigInt(text(rows[0], 'head'))
  }

  // The head the stream's log derives: the higher of its last event and its snapshot's event
  // sequence.
  async #derivedHead(run: Statement, entityId: string): Promise<bigint> {
    const {events, snapshots} = this.#tables
    const rows = await run(
      'SELECT GREATEST(' +
        `COALESCE((SELECT MAX(sequence) FROM ${events} WHERE entity_id = $1), 0), ` +
        `COALESCE((SELECT MAX(event_sequence) FROM ${snapshots} WHERE entity_id = $1), 0)` +
        ')::text AS head',
      [entityId],
    )

    return BigInt(text(rows[0]!, 'head'))
  }

  // Inserts one event row, and reports whether it was written: false when its sequence exists.
  async #insertEvent(run: Statement, event: SerializedEvent): Promise<boolean> {
    const rows = await run(
      `INSERT INTO ${this.#tables.events} ` +
        '(entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata) ' +
        "VALUES ($1, $2::bigint, $3, $4, $5::bigint, $6, decode($7, 'hex'), $8::text::jsonb) " +
        'ON CONFLICT (entity_id, sequence) DO NOTHING RETURNING 1',
      [
        event.entityId,
        event.sequence.toString(),
        event.id,
        event.type,
        event.version.toString(),
        event.entityType,
        hex(event.payload),
        encodeMetadata(event.metadata),
      ],
    )

    return rows.length === 1
  }

  // Writes a snapshot unless a newer one is stored, and reports whether it wrote.
  async #upsertSnapshot(run: Statement, snapshot: SerializedSnapshot): Promise<boolean> {
    const table = this.#tables.snapshots
    const rows = await run(
      `INSERT INTO ${table} ` +
        '(entity_id, entity_type, sequence, event_sequence, payload, updated_at) ' +
        "VALUES ($1, $2, $3::bigint, $4::bigint, decode($5, 'hex'), now()) " +
        'ON CONFLICT (entity_id) DO UPDATE SET entity_type = EXCLUDED.entity_type, ' +
        'sequence = EXCLUDED.sequence, event_sequence = EXCLUDED.event_sequence, ' +
        'payload = EXCLUDED.payload, updated_at = now() ' +
        `WHERE EXCLUDED.event_sequence >= ${table}.event_sequence RETURNING 1`,
      [
        snapshot.entityId,
        snapshot.entityType,
        snapshot.sequence.toString(),
        snapshot.eventSequence.toString(),
        hex(snapshot.payload),
      ],
    )

    return rows.length === 1
  }

  // The manifest row, or null when the store has none. A store older than the marker has no
  // marker table: the probe doesn't name it, since a failed statement would abort the transaction.
  async #readManifest(run: Statement): Promise<FeatureManifest | null> {
    const table = featuresTable(this.#tables)
    if ((await run('SELECT 1 WHERE to_regclass($1) IS NOT NULL', [table])).length === 0) {
      return null
    }

    const rows = await run(
      'SELECT version::text AS version, features, history_dependent, retention ' +
        `FROM ${table} WHERE id = 0`,
    )
    const row = rows[0]
    if (row === undefined) {
      return null
    }

    return decodeManifest(BigInt(text(row, 'version')), {
      features: text(row, 'features'),
      historyDependent: text(row, 'history_dependent'),
      retention: text(row, 'retention'),
    })
  }

  // Writes the next manifest over the stored one, which the caller read under the exclusive lock.
  async #writeManifest(
    run: Statement,
    stored: FeatureManifest | null,
    next: FeatureManifest,
  ): Promise<void> {
    const table = featuresTable(this.#tables)
    const texts = encodeManifest(next)
    const {features, historyDependent, retention} = texts
    const values = [next.version.toString(), features, historyDependent, retention]
    const rows =
      stored === null
        ? await run(
            `INSERT INTO ${table} (id, version, features, history_dependent, retention) ` +
              'VALUES (0, $1::bigint, $2, $3, $4) RETURNING 1',
            values,
          )
        : await run(
            `UPDATE ${table} SET version = $1::bigint, features = $2, history_dependent = $3, ` +
              'retention = $4 WHERE id = 0 AND version = $5::bigint RETURNING 1',
            [...values, stored.version.toString()],
          )
    if (rows.length !== 1) {
      // Impossible while the exclusive lock is held: refuse rather than guess.
      throw new EvtError(
        'invalidRecord',
        'the feature manifest moved during its declaration despite the exclusive lock',
      )
    }
  }

  // One statement outside a transaction.
  async #read(statement: string, values?: (string | null)[]): Promise<Row[]> {
    const client = this.#client
    if (client.connect !== undefined) {
      return rowsOf(await client.query(statement, values))
    }

    return serialize(client, async () => rowsOf(await client.query(statement, values)))
  }

  // Runs `work` in one READ COMMITTED transaction on a connection of its own, and commits it. A
  // failure, an abort included, rolls it back. A serialization failure or a deadlock is transient.
  async #transaction<T>(
    options: CallOptions | undefined,
    work: (run: Statement) => Promise<T>,
  ): Promise<T> {
    const signal = options?.signal
    signal?.throwIfAborted()

    const client = this.#client
    if (client.connect === undefined) {
      return serialize(client, () => inTransaction(client, signal, work))
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
      return await inTransaction(connection, signal, work, (error) => {
        broken = error
      })
    } finally {
      connection.release(broken)
    }
  }

}

// The operations queued on each single-connection client, shared by every repository on it: two
// transactions on one session would share its locks, and one's ROLLBACK would undo the other.
const queues = new WeakMap<PgQueryable, Promise<unknown>>()

// Runs `task` on a single-connection client after every task queued on it before.
function serialize<T>(client: PgQueryable, task: () => Promise<T>): Promise<T> {
  const result = (queues.get(client) ?? Promise.resolve()).then(task, task)
  queues.set(client, result.catch(() => undefined))

  return result
}

// One transaction on one connection. `broken` hears of a connection that failed to roll back.
async function inTransaction<T>(
  connection: PgQueryable | PgConnection,
  signal: AbortSignal | undefined,
  work: (run: Statement) => Promise<T>,
  broken?: (error: Error) => void,
): Promise<T> {
  const run: Statement = async (statement, values) => {
    signal?.throwIfAborted()

    return rowsOf(await connection.query(statement, values))
  }

  let result: T
  try {
    await connection.query('START TRANSACTION ISOLATION LEVEL READ COMMITTED')
    result = await work(run)
    signal?.throwIfAborted()
    await connection.query('COMMIT')
  } catch (error) {
    try {
      await connection.query('ROLLBACK')
    } catch (rollback) {
      broken?.(rollback instanceof Error ? rollback : new Error(String(rollback)))
    }

    throw transient(error)
  }

  // An abort while the COMMIT was in flight rejects too, though the commit may have applied, as
  // CallOptions promises: there is nothing left to roll back.
  signal?.throwIfAborted()

  return result
}

// A driver error PostgreSQL raised for a rollback a retry may not hit again, as a TransientError.
function transient(error: unknown): unknown {
  const code = typeof error === 'object' && error !== null ? (error as {code?: unknown}).code : null
  if (typeof code === 'string' && TRANSIENT_STATES.has(code)) {
    return new TransientError(`PostgreSQL rolled the transaction back (${code})`, {cause: error})
  }

  return error
}

// The statements of a DDL script, one at a time, since a driver's extended protocol runs one per
// call. Each ends at a semicolon that ends its line and comes before a blank line, a CREATE, an
// ALTER, a DO or the end, which no statement inside the DO block's body is followed by.
function statements(ddl: string): string[] {
  return ddl
    .split(/;\n(?=\n|CREATE |ALTER |DO |$)/)
    .map((statement) => statement.trim())
    .filter((statement) => statement !== '')
}

function rowsOf(result: {rows?: readonly unknown[]} | undefined): Row[] {
  const rows = result?.rows
  if (!Array.isArray(rows)) {
    throw new EvtError('invalidRecord', 'the PostgreSQL client returned a result without rows')
  }

  return rows as Row[]
}

// A text column of a row.
function text(row: Row, column: string): string {
  const value = row[column]
  if (typeof value !== 'string') {
    throw new EvtError('invalidRecord', `the PostgreSQL row has no text column ${column}`)
  }

  return value
}

function eventFromRow(row: Row): SerializedEvent {
  const metadata = text(row, 'metadata')

  return {
    id: text(row, 'event_id'),
    sequence: BigInt(text(row, 'sequence')),
    type: text(row, 'event_type'),
    version: Number(text(row, 'version')),
    entityId: text(row, 'entity_id'),
    entityType: text(row, 'entity_type'),
    payload: unhex(text(row, 'payload')),
    // A row Go wrote with null metadata reads as the default, as Go and Rust read it.
    metadata:
      metadata.trim() === '' || metadata === 'null' ? emptyMetadata() : decodeMetadata(metadata),
  }
}

function snapshotFromRow(row: Row): SerializedSnapshot {
  return {
    entityType: text(row, 'entity_type'),
    entityId: text(row, 'entity_id'),
    sequence: BigInt(text(row, 'sequence')),
    eventSequence: BigInt(text(row, 'event_sequence')),
    payload: unhex(text(row, 'payload')),
  }
}

// Bytes as lowercase hex, which decode(…, 'hex') reads. A null payload is stored empty.
function hex(bytes: Uint8Array | null): string {
  let out = ''
  for (const byte of bytes ?? []) {
    out += byte.toString(16).padStart(2, '0')
  }

  return out
}

function unhex(digits: string): Uint8Array {
  const bytes = new Uint8Array(digits.length / 2)
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = Number.parseInt(digits.slice(2 * i, 2 * i + 2), 16)
  }

  return bytes
}

function conflict(entityId: string): ConflictError {
  return new ConflictError(
    entityId,
    'conditional check failed: the sequence or snapshot changed since it was read',
  )
}

// A stored head of 0 is an absent head.
function drift(entityId: string, stored: bigint, derived: bigint): StreamHeadDriftError {
  return new StreamHeadDriftError(entityId, stored > 0n ? stored : null, derived)
}
