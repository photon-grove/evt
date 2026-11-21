// The PostgreSQL repository: events in one table keyed by (entity_id, sequence), snapshots in a
// second keyed by entity_id, and the engine rows (the feature marker, the rebuild fence, the
// stream heads) in tables derived from the events table's name (storage format, "PostgreSQL").
// Every commit is one READ COMMITTED transaction that runs the statements Go and Rust run, in
// their order: the entity lock, the feature-marker check under its shared lock, the rebuild-lock
// assertion under its own, the outcome probe, the head statement, the event inserts, the view
// statements, the snapshot upsert, then the outcome insert.
//
// Each statement of a transaction needs the one before it, so they await in loops on purpose.
/* oxlint-disable no-await-in-loop */

import {
  CompactionUncoveredError,
  ConflictError,
  EvtError,
  FenceConflictError,
  HistoryDependentProjectionError,
  IndeterminateError,
  OutcomeConflictError,
  ProjectorOperationError,
  RebuildBeginIndeterminateError,
  RebuildConflictError,
  RebuildInProgressError,
  RebuildLeaseLostError,
  SequenceGapError,
  StreamHeadDriftError,
  StreamHeadsNotMigratedError,
  TransientError,
  UnsupportedFeatureError,
  ViewsNotConfiguredError,
} from '../errors.js'
import type {EventLog, StreamFilter} from '../enumerate.js'
import {
  MAX_REBUILD_BEGIN_ATTEMPTS,
  type RebuildFence,
  type RebuildGeneration,
  type RebuildLease,
  type RebuildLock,
  type RebuildState,
  checkRebuildOwner,
  checkRebuildViewBatch,
  fenceNow,
  planRebuildBegin,
  rebuildLeaseSeconds,
} from '../rebuild-fence.js'
import type {SerializedEvent, SerializedSnapshot} from '../event.js'
import {
  type FeatureManifest,
  JOURNAL_COMPACTION_FEATURE,
  JOURNAL_COMPACTION_VERSION,
  JOURNAL_MARKER,
  type JournalDeclared,
  type JournalMarker,
  POSTGRES_VIEWS_FEATURE,
  POSTGRES_VIEWS_VERSION,
  checkJournalWriter,
  checkSupported,
  checkWritable,
  declaredAnything,
  declaresStreamHeads,
  decodeManifest,
  isJournalWriter,
  journalDeclaration,
  missingHistoryDependent,
  withHistoryDependent,
  withJournal,
  withStreamHeads,
  withoutJournal,
} from '../features.js'
import {int64Argument} from '../fields.js'
import type {JournalStore} from '../journal/read.js'
import type {JournalRow} from '../journal/row.js'
import {decodeMetadata, emptyMetadata, encodeMetadata} from '../metadata.js'
import {type Outcome, checkCommandId} from '../outcome.js'
import {type ProjectedView, REASON_CONDITION_FAILED} from '../projection.js'
import {
  type CallOptions,
  type Commit,
  type NormalizedTransaction,
  type OutcomeRepository,
  type StoreInitializer,
  type TransactionCommit,
  checkBudget,
  checkFenceRetention,
  checkNotReserved,
  commitAsTransaction,
  isEmptyTransaction,
  normalizeTransaction,
  rankFailures,
  transactionSizes,
} from '../repository.js'
import {compareUtf8, utf8Length} from '../utf8.js'
import type {ViewRow} from '../views.js'
import type {PgClient, PgResult} from './client.js'
import {readCompactionRow, readCounters, readLastPosition, readRows} from './journal.js'
import {manifestWrite} from './marker.js'
import {insertOutcomeSql, outcomeFromRow, outcomeRow, selectOutcomeSql} from './outcome.js'
import {
  ENGINE_LOCK_EXCLUSIVE,
  ENGINE_LOCK_SHARED,
  ENTITY_LOCK,
  type PgTables,
  SCHEMA_LOCK,
  featuresLockKey,
  featuresTable,
  outcomesTable,
  pgTables,
  rebuildGenerationTable,
  rebuildLockKey,
  rebuildLockTable,
  schemaDdl,
  streamHeadsTable,
  viewsDdl,
  viewsTable,
} from './schema.js'
import {
  type Row,
  type Statement,
  type TransactionMode,
  type TxStatement,
  nullableText,
  query,
  sqlState,
  text,
  transaction as runTransaction,
} from './session.js'
import {
  type BuiltView,
  type ViewWriter,
  affectedRows,
  buildViews,
  mapViewError,
  viewConditionFailed,
  viewsTableStatements,
} from './views.js'
import {PgViewRepository, checkViewsEncoding} from './view-repository.js'

/** The tables a {@link PgRepository} stores its event log in. */
export interface PgRepositoryOptions {
  /** The events table, `evt_events` by default. The engine tables derive their names from it. */
  eventsTable?: string
  /** The snapshots table, `evt_snapshots` by default. */
  snapshotsTable?: string
  /**
   * Turns each commit-wide view operation into the statement that writes it to the application's
   * own view table, run in the commit's transaction after the events and before the snapshots.
   * Without one, or {@link viewsTable}, a commit that carries view operations is refused with a
   * {@link ViewsNotConfiguredError} before any write.
   */
  viewWriter?: ViewWriter
  /**
   * Makes the evt-owned views table, `{events}__evt_views` ({@link PgViewRepository}), the target
   * of each commit's view operations, in place of a {@link viewWriter}: a repository has one or
   * the other, and setting both fails with `invalidArgument`. evt builds each statement with the
   * DynamoDB repository's owner rules, sizes it in the commit budget as its DynamoDB item, and
   * declares the `postgres-views` feature before the first write, so a writer that would put the
   * views elsewhere refuses the store. The database must be `UTF8`. Rust's `with_views_table()`.
   */
  viewsTable?: boolean
  /** The clock rebuild leases expire by, in Unix milliseconds as `Date.now` gives them. */
  clock?: () => number
}

/** The backend name a projector group for the PostgreSQL repository would carry. */
export const POSTGRES_BACKEND = 'postgres'

// The event columns every read selects, each as text, so no driver parses a stored value. An
// ORDER BY names the table's column, since a bare `sequence` would sort the text alias: 10
// before 9.
const EVENT_COLUMNS =
  'event_id, sequence::text AS sequence, event_type, version::text AS version, entity_id, ' +
  "entity_type, encode(payload, 'hex') AS payload, metadata::text AS metadata"

// A rebuild's begin found its rows moved by a writer outside the protocol: it reads and tries again.
const MOVED = Symbol('moved')

// The rows each page of an enumeration reads.
const ENUMERATION_PAGE_SIZE = 500

// A unique violation, which the outcome insert raises when another writer recorded the outcome.
const UNIQUE_VIOLATION = '23505'

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
export class PgRepository
  implements OutcomeRepository, StoreInitializer, EventLog, RebuildFence, JournalStore
{
  readonly #client: PgClient
  readonly #tables: PgTables
  readonly #viewWriter: ViewWriter | undefined
  readonly #clock: () => number
  // Whether commits write their views to the views table, which #viewRepository reads.
  readonly #viewsTable: boolean
  readonly #viewRepository: PgViewRepository

  // The manifest version this writer validated, or null until it validates.
  #validated: bigint | null = null
  // Whether this writer saw the manifest declare postgres-views, so it declares it only once.
  #viewsDeclared = false
  // Whether the rebuild-lock table exists, which a commit's assertion probes for until it does.
  #rebuildReady = false

  /**
   * Takes the client and the table names, which must be plain identifiers that fit their limits
   * ({@link pgTables}), or the constructor fails with `invalidArgument`.
   */
  constructor(client: PgClient, options: PgRepositoryOptions = {}) {
    this.#client = client
    this.#tables = pgTables(options.eventsTable, options.snapshotsTable)
    this.#viewWriter = options.viewWriter
    this.#clock = options.clock ?? Date.now
    this.#viewsTable = options.viewsTable === true
    if (this.#viewsTable && this.#viewWriter !== undefined) {
      throw new EvtError(
        'invalidArgument',
        'postgres: a repository writes its views to the views table or through a view writer, ' +
          'not both',
      )
    }
    this.#viewRepository = new PgViewRepository(client, this.#tables)
  }

  /**
   * It writes commit-wide view operations to its views table or through its view writer. With
   * neither, a commit that carries them is refused with a {@link ViewsNotConfiguredError}, as
   * Go's and Rust's are.
   */
  writesViews(): boolean {
    return true
  }

  /**
   * Whether it applies a view put's engine-only expected payload (the journal's counter): with the
   * views table, whose statements evt builds, as Rust's does. A view writer can't express it, so
   * a commit that carries one through a writer is refused before any write.
   */
  appliesExpectedPayload(): boolean {
    return this.#viewsTable
  }

  /**
   * The view repository over this event log's views table, `{events}__evt_views`, which
   * {@link PgRepository.ensureSchema} creates: the table commits write their views to with
   * `viewsTable: true`, so a handler can commit and then read its own rows. Every read sees every
   * committed transaction.
   */
  viewRepository(): PgViewRepository {
    return this.#viewRepository
  }

  /** The tables this repository reads and writes, folded to lower case. */
  get tables(): PgTables {
    return this.#tables
  }

  /**
   * Creates the event log's tables if they don't exist, with the DDL Go and Rust run
   * ({@link schemaDdl}), and the views table ({@link viewsDdl}), which Rust's `ensure_schema`
   * creates and Go's `EnsureSchema` doesn't, under the schema lock they take. Safe to call on
   * every start.
   */
  async ensureSchema(options?: CallOptions): Promise<void> {
    await this.#transaction(options, false, async (run) => {
      await run(SCHEMA_LOCK, [this.#tables.events])
      for (const statement of statements(schemaDdl(this.#tables) + viewsDdl(this.#tables))) {
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
      return await this.#transaction(options, false, async (run) => {
        await run(ENGINE_LOCK_EXCLUSIVE, [featuresLockKey(this.#tables)])
        const manifest = await this.#readManifest(run)
        checkSupported(manifest, isJournalWriter(this))
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

  /**
   * Adds the entity types to the feature manifest's historyDependent set in one declaration under
   * the manifest's exclusive lock, so compaction refuses them in every process. Types already
   * declared are left alone, and nothing is written when there are none to add. A store that
   * declares an unsupported feature is refused first.
   */
  async declareHistoryDependent(types: readonly string[], options?: CallOptions): Promise<void> {
    try {
      await this.#transaction(options, false, async (run) => {
        await run(ENGINE_LOCK_EXCLUSIVE, [featuresLockKey(this.#tables)])
        const manifest = await this.#readManifest(run)
        checkSupported(manifest, isJournalWriter(this))
        const missing = missingHistoryDependent(manifest, types)
        if (missing.length > 0) {
          await this.#writeManifest(run, manifest, withHistoryDependent(manifest, missing))
        }
      })
    } finally {
      // A declaration moved the version: the next write reads it again.
      this.#validated = null
    }
  }

  async commit(commit: Commit, options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    const normalized = commitAsTransaction(commit)
    if (!isEmptyTransaction(normalized)) {
      await this.#write(normalized, options)
    }
  }

  /**
   * Commits every stream and checks every fence in one transaction, in the order Go and Rust run
   * it: every stream's entity lock, written and fenced, in ascending entity-ID byte order; the
   * marker check, the fences' retention and the rebuild-lock assertion; the probe for a recorded
   * outcome; every head statement and fence read, ranked; the event inserts; the snapshot upserts;
   * then the outcome insert.
   */
  async commitTransaction(transaction: TransactionCommit, options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    const normalized = normalizeTransaction(transaction)
    if (!isEmptyTransaction(normalized)) {
      await this.#write(normalized, options)
    }
  }

  async outcome(commandId: string, options?: CallOptions): Promise<Outcome | null> {
    options?.signal?.throwIfAborted()
    checkCommandId(commandId)

    const rows = await this.#read(selectOutcomeSql(this.#tables), [commandId])
    const row = rows[0]
    if (row === undefined) {
      return null
    }

    const response = nullableText(row, 'response')

    return outcomeFromRow({
      command_id: text(row, 'command_id'),
      status: text(row, 'status'),
      digest: text(row, 'digest'),
      response: response === null ? null : unhex(response),
      events: text(row, 'events'),
      rejection: nullableText(row, 'rejection'),
      code: nullableText(row, 'code'),
      command_type: nullableText(row, 'command_type'),
      targets: nullableText(row, 'targets'),
      principal: nullableText(row, 'principal'),
    })
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

    // The view statements are built before the budget and before any write, so a writer that
    // refuses one fails the whole commit.
    const views = this.#buildViews(transaction.views)

    // Every backend budgets a commit the same way, so an oversized one fails before any request.
    // A views-table statement counts as the DynamoDB item it mirrors, so the budget refuses the
    // same commits it refuses on DynamoDB. A view writer's statement counts its SQL text: its
    // arguments are opaque, so its bytes are a lower bound, while its count is exact.
    const sizes = this.#viewsTable
      ? transactionSizes(transaction, () => undefined)
      : transactionSizes({...transaction, views: []}, () => undefined)
    if (!this.#viewsTable) {
      for (const {statement} of views) {
        sizes.push(utf8Length(statement.sql))
      }
    }
    checkBudget(sizes)
    await this.#ensureStreamHeads(options)
    await this.#ensureViewsDeclared(options)

    const {outcome} = transaction
    const mode = {indeterminate: outcome !== null, views: views.length > 0}
    await this.#transaction(options, mode, async (run) => {
      // Each stream once, in the bytewise order that also pins the transaction, so two
      // transactions that share streams lock them in the same sequence and can't deadlock.
      const ids = [...transaction.streams, ...transaction.fences].map(({entityId}) => entityId)
      ids.sort(compareUtf8)
      for (const [index, entityId] of ids.entries()) {
        if (ids[index - 1] !== entityId) {
          await run(ENTITY_LOCK, [entityId])
        }
      }

      const manifest = await this.#checkManifest(run, this.#viewsTable)
      const retention = manifest.retention
      checkFenceRetention(transaction, (entityType) => retention.includes(entityType))
      await this.#assertNoRebuildLock(run)

      // A recorded outcome outranks every stream failure, as the outcome put does on DynamoDB. The
      // insert's unique violation still settles a real race.
      if (outcome !== null) {
        const recorded = await run(
          `SELECT 1 AS recorded FROM ${outcomesTable(this.#tables)} WHERE command_id = $1`,
          [outcome.commandId],
        )
        if (recorded.length > 0) {
          throw new OutcomeConflictError(outcome.commandId)
        }
      }

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
      // After the events, so a writer that lost a race fails on its head (a conflict) before it
      // reaches a view row, and before the snapshots.
      await execViews(run, views)
      for (const stream of transaction.streams) {
        await this.#writeCommitSnapshot(run, stream)
      }
      if (outcome !== null) {
        await this.#insertOutcome(run, outcome)
      }
    })
  }

  // A commit's view statements, or a refusal when there are views and nowhere to write them.
  #buildViews(views: readonly ProjectedView[]): BuiltView[] {
    if (views.length === 0) {
      return []
    }
    if (this.#viewsTable) {
      return viewsTableStatements(viewsTable(this.#tables), views)
    }
    if (this.#viewWriter === undefined) {
      throw new ViewsNotConfiguredError(POSTGRES_BACKEND)
    }

    return buildViews(views, this.#viewWriter)
  }

  // The outcome insert, the commit's last statement. A unique violation is a lost race.
  async #insertOutcome(run: Statement, outcome: Outcome): Promise<void> {
    const row = outcomeRow(outcome)
    try {
      await run(insertOutcomeSql(this.#tables), [
        row.command_id,
        row.status,
        row.digest,
        row.response === null ? null : hex(row.response),
        row.events,
        row.rejection,
        row.code,
        row.command_type,
        row.targets,
        row.principal,
      ])
    } catch (error) {
      if (sqlState(error) === UNIQUE_VIOLATION) {
        throw new OutcomeConflictError(outcome.commandId)
      }

      throw error
    }
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

  async rebuildGeneration(options?: CallOptions): Promise<RebuildGeneration> {
    options?.signal?.throwIfAborted()

    return this.#readGeneration((statement, values) => this.#read(statement, values), false)
  }

  /** The generation, then the lock: two statements, not one transaction, as Go's and Rust's. */
  async rebuildState(options?: CallOptions): Promise<RebuildState> {
    const generation = await this.rebuildGeneration(options)
    const lock = await this.#readLock((statement, values) => this.#read(statement, values), false)

    return {generation, lock}
  }

  async beginRebuild(owner: string, leaseMs: number, options?: CallOptions): Promise<RebuildLease> {
    options?.signal?.throwIfAborted()
    checkRebuildOwner(owner)
    const seconds = rebuildLeaseSeconds(leaseMs)
    const lockTable = rebuildLockTable(this.#tables)
    const generationTable = rebuildGenerationTable(this.#tables)

    // Every lifecycle transaction holds the fence's exclusive lock, so rows move between attempts
    // only when a writer outside the protocol changed them.
    for (let attempt = 1; attempt <= MAX_REBUILD_BEGIN_ATTEMPTS; attempt++) {
      let begun: {lease: RebuildLease; observed: bigint} | null = null
      try {
        const lease = await this.#transaction(options, true, async (run) => {
          await run(ENGINE_LOCK_EXCLUSIVE, [rebuildLockKey(this.#tables)])
          const generation = await this.#readGeneration(run, true)
          const lock = await this.#readLock(run, true)
          const now = fenceNow(this.#clock)
          const plan = planRebuildBegin({generation, lock}, now)
          const taken = {
            owner,
            generation: generation.value + plan.increment,
            leaseExpiresAt: now + seconds,
          }
          begun = {lease: taken, observed: generation.value}
          const expiry = taken.leaseExpiresAt.toString()
          const token = taken.generation.toString()

          if (plan.kind === 'takeover') {
            const moved =
              (
                await run(
                  `UPDATE ${lockTable} SET owner = $1, generation = $5::bigint, ` +
                    'lease_expires_at = to_timestamp($2::bigint::double precision) ' +
                    'WHERE id = 0 AND owner = $3 ' +
                    'AND lease_expires_at < to_timestamp($4::bigint::double precision) RETURNING 1',
                  [owner, expiry, plan.observedOwner, now.toString(), token],
                )
              ).length === 0 ||
              (
                await run(
                  `UPDATE ${generationTable} SET value = value + 2 ` +
                    'WHERE id = 0 AND value = $1::bigint RETURNING 1',
                  [plan.seenOdd.toString()],
                )
              ).length === 0
            if (moved) {
              throw MOVED
            }

            return taken
          }

          try {
            await run(
              `INSERT INTO ${lockTable} (id, owner, generation, lease_expires_at) ` +
                'VALUES (0, $1, $3::bigint, to_timestamp($2::bigint::double precision))',
              [owner, expiry, token],
            )
          } catch (error) {
            throw sqlState(error) === '23505' ? MOVED : error
          }
          const updated = await run(
            `INSERT INTO ${generationTable} (id, value, force_released) ` +
              'VALUES (0, $1::bigint, false) ON CONFLICT (id) DO UPDATE ' +
              `SET value = ${generationTable}.value + $1::bigint ` +
              `WHERE ${generationTable}.value = $2::bigint ` +
              `OR (${generationTable}.value = $3::bigint AND ${generationTable}.force_released) ` +
              'RETURNING 1',
            [plan.increment.toString(), plan.seenEven.toString(), plan.seenOdd.toString()],
          )
          if (updated.length === 0) {
            throw MOVED
          }

          return taken
        })

        return lease
      } catch (error) {
        if (error === MOVED) {
          continue
        }
        if (error instanceof IndeterminateError) {
          const known = begun as {lease: RebuildLease; observed: bigint} | null
          if (known !== null) {
            throw new RebuildBeginIndeterminateError(known.lease, known.observed, error.cause)
          }
        }
        throw error
      }
    }

    throw new RebuildConflictError(MAX_REBUILD_BEGIN_ATTEMPTS)
  }

  async renewRebuild(
    lease: RebuildLease,
    leaseMs: number,
    options?: CallOptions,
  ): Promise<RebuildLease> {
    options?.signal?.throwIfAborted()
    checkRebuildOwner(lease.owner)
    const seconds = rebuildLeaseSeconds(leaseMs)
    const expiry = fenceNow(this.#clock) + seconds
    const renewed = await this.#read(
      `UPDATE ${rebuildLockTable(this.#tables)} ` +
        'SET lease_expires_at = to_timestamp($2::bigint::double precision) ' +
        'WHERE id = 0 AND owner = $1 AND generation = $3::bigint RETURNING 1',
      [lease.owner, expiry.toString(), lease.generation.toString()],
    )
    if (renewed.length === 0) {
      throw new RebuildLeaseLostError(lease.owner)
    }

    return {...lease, leaseExpiresAt: expiry}
  }

  async endRebuild(lease: RebuildLease, options?: CallOptions): Promise<RebuildGeneration> {
    options?.signal?.throwIfAborted()
    checkRebuildOwner(lease.owner)

    return this.#release(lease.owner, lease.generation, options)
  }

  async forceReleaseRebuild(owner: string, options?: CallOptions): Promise<RebuildGeneration> {
    options?.signal?.throwIfAborted()
    checkRebuildOwner(owner)

    return this.#release(owner, null, options)
  }

  /**
   * Writes the batch in one transaction that first locks the lock row and checks the lease holds
   * it unexpired, so a takeover waits for the batch. Its statements run as a commit's do.
   */
  async writeRebuildViews(
    lease: RebuildLease,
    views: readonly ProjectedView[],
    options?: CallOptions,
  ): Promise<void> {
    options?.signal?.throwIfAborted()
    checkRebuildOwner(lease.owner)
    checkRebuildViewBatch(views)
    const built = this.#buildViews(views)
    if (this.#viewsTable) {
      await this.#ensureStreamHeads(options)
      await this.#ensureViewsDeclared(options)
    }

    await this.#transaction(options, {indeterminate: false, views: true}, async (run) => {
      const lock = await this.#readLock(run, true)
      const held =
        lock !== null &&
        lock.owner === lease.owner &&
        lock.generation === lease.generation &&
        lock.leaseExpiresAt > fenceNow(this.#clock)
      if (!held) {
        throw new RebuildLeaseLostError(lease.owner)
      }
      this.#refuseForeignViews(await this.#readManifest(run))
      await execViews(run, built)
    })
  }

  // Ends a lease (with its fencing generation) or force-releases an owner's (without), in one
  // transaction under the fence's exclusive lock. A force-release keeps no lock row: the odd
  // generation it leaves refuses commits.
  async #release(
    owner: string,
    generation: bigint | null,
    options?: CallOptions,
  ): Promise<RebuildGeneration> {
    const lockTable = rebuildLockTable(this.#tables)
    const generationTable = rebuildGenerationTable(this.#tables)

    return this.#transaction(options, false, async (run) => {
      await run(ENGINE_LOCK_EXCLUSIVE, [rebuildLockKey(this.#tables)])
      const seen = await this.#readGeneration(run, true)
      if (seen.value % 2n === 0n || (generation !== null && seen.value !== generation)) {
        throw new RebuildLeaseLostError(owner)
      }

      const deleted =
        generation === null
          ? await run(`DELETE FROM ${lockTable} WHERE id = 0 AND owner = $1 RETURNING 1`, [owner])
          : await run(
              `DELETE FROM ${lockTable} WHERE id = 0 AND owner = $1 ` +
                'AND generation = $2::bigint RETURNING 1',
              [owner, generation.toString()],
            )
      if (deleted.length === 0) {
        throw new RebuildLeaseLostError(owner)
      }

      const increment = generation === null ? 2n : 1n
      const forceReleased = generation === null
      const updated = await run(
        `UPDATE ${generationTable} SET value = value + $1::bigint, ` +
          'force_released = $2::boolean WHERE id = 0 AND value = $3::bigint RETURNING 1',
        [increment.toString(), forceReleased.toString(), seen.value.toString()],
      )
      if (updated.length === 0) {
        throw new RebuildLeaseLostError(owner)
      }

      return {value: seen.value + increment, forceReleased}
    })
  }

  // The generation row, 0 never force-released when there is none, locked to COMMIT with `lock`.
  async #readGeneration(run: Statement, lock: boolean): Promise<RebuildGeneration> {
    const rows = await run(
      'SELECT value::text AS value, force_released::text AS force_released ' +
        `FROM ${rebuildGenerationTable(this.#tables)} WHERE id = 0${lock ? ' FOR UPDATE' : ''}`,
    )
    const row = rows[0]

    return row === undefined
      ? {value: 0n, forceReleased: false}
      : {value: BigInt(text(row, 'value')), forceReleased: text(row, 'force_released') === 'true'}
  }

  // The lock row, or null, locked to COMMIT with `lock`.
  async #readLock(run: Statement, lock: boolean): Promise<RebuildLock | null> {
    const rows = await run(
      'SELECT owner, generation::text AS generation, ' +
        'EXTRACT(EPOCH FROM lease_expires_at)::bigint::text AS lease_expires_at ' +
        `FROM ${rebuildLockTable(this.#tables)} WHERE id = 0${lock ? ' FOR UPDATE' : ''}`,
    )
    const row = rows[0]

    return row === undefined
      ? null
      : {
          owner: text(row, 'owner'),
          generation: BigInt(text(row, 'generation')),
          leaseExpiresAt: Number(text(row, 'lease_expires_at')),
        }
  }

  /** Each entity's events come whole, in one batch. */
  readonly batchesByEntity = true

  /**
   * The event log, one batch per entity, entities ascending by `entity_id` in the database's
   * collation and events by sequence. It reads a page of rows at a time, each page continuing
   * from the last row of the one before, so memory holds a page and the entity being collected,
   * and each page sees the log as its own statement does. The reserved namespaces are skipped.
   */
  async *streamAllEvents(
    filter?: StreamFilter,
    options?: CallOptions,
  ): AsyncGenerator<SerializedEvent[]> {
    const {events} = this.#tables
    const entityType = filter?.entityType ?? null
    let batch: SerializedEvent[] = []
    let after: [string, string] | null = null
    for (;;) {
      options?.signal?.throwIfAborted()
      const rows = await this.#read(
        `SELECT ${EVENT_COLUMNS} FROM ${events} WHERE entity_id NOT LIKE 'SYSTEM#%' ` +
          "AND entity_id NOT LIKE 'command-outcome#%' AND ($1::text IS NULL OR entity_type = $1) " +
          `AND ($2::text IS NULL OR (entity_id, ${events}.sequence) > ($2, $3::bigint)) ` +
          `ORDER BY entity_id ASC, ${events}.sequence ASC LIMIT ${ENUMERATION_PAGE_SIZE}`,
        [entityType, after?.[0] ?? null, after?.[1] ?? null],
      )
      for (const row of rows) {
        const event = eventFromRow(row)
        if (batch.length > 0 && batch[0]!.entityId !== event.entityId) {
          yield batch
          batch = []
        }
        batch.push(event)
      }
      if (rows.length < ENUMERATION_PAGE_SIZE) {
        break
      }
      const last = batch.at(-1)!
      after = [last.entityId, last.sequence.toString()]
    }
    if (batch.length > 0) {
      yield batch
    }
  }

  /**
   * Each entity's head. A store that declares stream-heads reads it from the heads table, and one
   * that doesn't yet, whose heads table may be empty, from the events and snapshots, as Rust does.
   */
  async entityHeads(entityType?: string, options?: CallOptions): Promise<Map<string, bigint>> {
    options?.signal?.throwIfAborted()
    const {events, snapshots} = this.#tables
    const manifest = await this.#readManifest((statement, values) => this.#read(statement, values))
    const reserved = "entity_id NOT LIKE 'SYSTEM#%' AND entity_id NOT LIKE 'command-outcome#%'"
    const sql = declaresStreamHeads(manifest)
      ? `SELECT entity_id, head::text AS head FROM ${streamHeadsTable(this.#tables)} ` +
        `WHERE ($1::text IS NULL OR entity_type = $1) AND ${reserved}`
      : 'SELECT entity_id, MAX(head)::text AS head FROM (' +
        `SELECT entity_id, ${events}.sequence AS head, entity_type FROM ${events} UNION ALL ` +
        `SELECT entity_id, event_sequence AS head, entity_type FROM ${snapshots}` +
        `) sources WHERE ($1::text IS NULL OR entity_type = $1) AND ${reserved} ` +
        'GROUP BY entity_id'
    const rows = await this.#read(sql, [entityType ?? null])
    const heads = new Map<string, bigint>()
    for (const row of rows) {
      const head = BigInt(text(row, 'head'))
      if (head > 0n) {
        heads.set(text(row, 'entity_id'), head)
      }
    }

    return heads
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

    return this.#transaction(options, false, async (run) => {
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
    return this.#transaction(options, false, async (run) => {
      await run(ENGINE_LOCK_SHARED, [featuresLockKey(this.#tables)])
      const manifest = await this.#readManifest(run)
      this.#refuseForeignViews(manifest)
      checkWritable(manifest, isJournalWriter(this))

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

  /**
   * A scope's journal counter, from the views table: 0 when it has none. A repository without
   * `viewsTable: true` holds no journal, and refuses with a {@link ViewsNotConfiguredError}.
   */
  async readCounter(scope: string, options?: CallOptions): Promise<bigint> {
    const [counter] = await this.readCounters([scope], options)

    return counter!
  }

  /** Several scopes' journal counters, in the order given, in one statement. */
  async readCounters(scopes: readonly string[], options?: CallOptions): Promise<bigint[]> {
    options?.signal?.throwIfAborted()

    return readCounters(this.#client, this.#journalTable(), scopes, options)
  }

  /** The highest position with a journal row in the scope: 0 when it has none. */
  async readLastPosition(scope: string, options?: CallOptions): Promise<bigint> {
    options?.signal?.throwIfAborted()

    return readLastPosition(this.#client, this.#journalTable(), scope, options)
  }

  /** Up to `limit` journal rows of the scope above `after`, ascending, in one statement. */
  async readRows(
    scope: string,
    after: bigint,
    limit: number,
    options?: CallOptions,
  ): Promise<JournalRow[]> {
    options?.signal?.throwIfAborted()

    return readRows(this.#client, this.#journalTable(), scope, after, limit, options)
  }

  /** A row of the scope's journal compaction partition at sort key `sk`, or null. */
  async readCompactionRow(
    scope: string,
    sk: string,
    options?: CallOptions,
  ): Promise<ViewRow | null> {
    options?.signal?.throwIfAborted()

    return readCompactionRow(this.#client, this.#journalTable(), scope, sk, options)
  }

  /** The journal's declarations on this store's manifest. Internal. */
  readonly [JOURNAL_MARKER]: JournalMarker & CompactionMarker = {
    declareJournal: async (types, options) => {
      options?.signal?.throwIfAborted()
      checkJournalWriter(this)
      this.#journalTable()
      // Validated as a commit validates it: a fresh store is initialized, and a populated one
      // that doesn't declare stream-heads is refused, before anything is declared.
      await this.#ensureStreamHeads(options)
      // The feature, then the history, a declaration each, as Rust declares them.
      const feature = await this.#declareJournal(types, true, options)
      const history = await this.#declareJournal(types, false, options)

      return {journal: feature.journal, history: history.history}
    },
    retractJournal: async (declared, options) => {
      checkJournalWriter(this)
      if (!declaredAnything(declared)) {
        return
      }

      try {
        await this.#transaction(options, false, async (run) => {
          await run(ENGINE_LOCK_EXCLUSIVE, [featuresLockKey(this.#tables)])
          const manifest = await this.#readManifest(run)
          checkSupported(manifest, isJournalWriter(this))
          if (manifest !== null) {
            await this.#writeManifest(run, manifest, withoutJournal(manifest, declared))
          }
        })
      } finally {
        // A declaration moved the version: the next write reads it again.
        this.#validated = null
      }
    },
    // Compaction is the journal's own, so any handle checks and declares it.
    checkCompactionMarker: async (options) => {
      options?.signal?.throwIfAborted()
      const manifest = await this.#readManifest((statement, values) =>
        this.#read(statement, values),
      )
      checkSupported(manifest, true)
    },
    // Until journal compaction is ported to PostgreSQL, nothing declares it here: a writer that
    // doesn't model it would read compacted rows as a gap, so it refuses a store that declares it.
    declareCompaction: async (options) => {
      options?.signal?.throwIfAborted()
      throw new UnsupportedFeatureError([
        {name: JOURNAL_COMPACTION_FEATURE, version: JOURNAL_COMPACTION_VERSION},
      ])
    },
  }

  // Declares the journal feature (`feature`) or the entity types history-dependent, and resolves
  // to what it wrote. The manifest is read first without a lock, so a store that declares it all
  // takes no exclusive lock, which would wait for every open commit.
  async #declareJournal(
    types: readonly string[],
    feature: boolean,
    options: CallOptions | undefined,
  ): Promise<JournalDeclared> {
    const part = (manifest: FeatureManifest | null): JournalDeclared => {
      checkSupported(manifest, isJournalWriter(this))
      const wanted = journalDeclaration(manifest, types)

      return feature
        ? {journal: wanted.journal, history: []}
        : {journal: false, history: wanted.history}
    }
    const observed = await this.#readManifest((statement, values) => this.#read(statement, values))
    if (!declaredAnything(part(observed))) {
      return {journal: false, history: []}
    }

    try {
      return await this.#transaction(options, false, async (run) => {
        await run(ENGINE_LOCK_EXCLUSIVE, [featuresLockKey(this.#tables)])
        const manifest = await this.#readManifest(run)
        const declared = part(manifest)
        if (declaredAnything(declared)) {
          await this.#writeManifest(run, manifest, withJournal(manifest, declared))
        }

        return declared
      })
    } finally {
      // A declaration moved the version: the next write reads it again.
      this.#validated = null
    }
  }

  // The views table the journal lives in: the journal is written only through it.
  #journalTable(): string {
    if (!this.#viewsTable) {
      throw new ViewsNotConfiguredError(POSTGRES_BACKEND)
    }

    return viewsTable(this.#tables)
  }

  // Makes sure the store is ready for this writer's first write: it declares stream-heads. A
  // writer initializes a store whose manifest lacks it, which refuses one that holds data. It runs
  // before the write's transaction, which holds the shared lock a declaration would wait for.
  async #ensureStreamHeads(options: CallOptions | undefined): Promise<void> {
    if (this.#validated !== null) {
      return
    }

    const manifest = await this.#readManifest((statement, values) => this.#read(statement, values))
    this.#refuseForeignViews(manifest)
    checkSupported(manifest, isJournalWriter(this))
    if (!declaresStreamHeads(manifest)) {
      await this.initialize(options)
    }
  }

  // Declares postgres-views on a store whose views live in the views table, once per writer, after
  // checking the database is UTF8. It runs before the write's transaction, which holds the shared
  // lock a declaration would wait for, and the commit checks the declaration again under that lock.
  async #ensureViewsDeclared(options: CallOptions | undefined): Promise<void> {
    if (!this.#viewsTable || this.#viewsDeclared) {
      return
    }

    await checkViewsEncoding(this.#viewRepository, options)
    // Read first, without a lock: a store that declares it needs no exclusive lock, which would
    // wait for every open commit and hold back new ones.
    const observed = await this.#readManifest((statement, values) => this.#read(statement, values))
    const version = observed?.features.get(POSTGRES_VIEWS_FEATURE)
    if (version !== undefined && version !== POSTGRES_VIEWS_VERSION) {
      throw new UnsupportedFeatureError([{name: POSTGRES_VIEWS_FEATURE, version}])
    }
    if (version !== undefined) {
      this.#viewsDeclared = true
      return
    }

    try {
      await this.#transaction(options, false, async (run) => {
        await run(ENGINE_LOCK_EXCLUSIVE, [featuresLockKey(this.#tables)])
        const manifest = await this.#readManifest(run)
        // A version of postgres-views other than this package's is unsupported, like any feature
        // this package doesn't model, and nothing is declared on such a store.
        checkSupported(manifest, isJournalWriter(this))
        if (!manifest?.features.has(POSTGRES_VIEWS_FEATURE)) {
          await this.#writeManifest(run, manifest, withPostgresViews(manifest))
        }
      })
    } finally {
      // A declaration moved the version: the next write reads it again.
      this.#validated = null
    }

    this.#viewsDeclared = true
  }

  // Refuses a store that declares postgres-views unless this writer writes its views to the views
  // table, so a store's commit-wide views never split between the views table and an
  // application's tables.
  #refuseForeignViews(manifest: FeatureManifest | null): void {
    const version = manifest?.features.get(POSTGRES_VIEWS_FEATURE)
    if (version !== undefined && !this.#viewsTable) {
      throw new UnsupportedFeatureError([{name: POSTGRES_VIEWS_FEATURE, version}])
    }
  }

  // The commit-time marker check, under the manifest's shared lock held to COMMIT, so a
  // declaration can't slip between the check and the write. A manifest that changed since this
  // writer validated it is validated again. A store that declares postgres-views is refused unless
  // this writer has the views table, and with `views` (a commit through the views table, which
  // #ensureViewsDeclared declared it for) a store that no longer declares it is a TransientError,
  // whose retry declares it again.
  async #checkManifest(run: Statement, views = false): Promise<FeatureManifest> {
    await run(ENGINE_LOCK_SHARED, [featuresLockKey(this.#tables)])
    const manifest = await this.#readManifest(run)
    this.#refuseForeignViews(manifest)
    if (views && !manifest?.features.has(POSTGRES_VIEWS_FEATURE)) {
      this.#viewsDeclared = false
      throw new TransientError('the store no longer declares postgres-views; declaring it again')
    }
    if (manifest !== null && manifest.version === this.#validated) {
      return manifest
    }

    this.#validated = null
    checkWritable(manifest, isJournalWriter(this))
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
    const write = manifestWrite(this.#tables, stored, next)
    const rows = await run(write.text, write.values)
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
    return query(this.#client, statement, values)
  }

  // Runs `work` in one READ COMMITTED transaction (see `transaction`).
  async #transaction<T>(
    options: CallOptions | undefined,
    mode: TransactionMode | boolean,
    work: (run: TxStatement) => Promise<T>,
  ): Promise<T> {
    return runTransaction(this.#client, options, mode, work)
  }
}

// Runs a commit's view statements in order. Each statement's outcome decides whether its
// operation's condition held, and its first failure is mapped to the operation.
async function execViews(run: TxStatement, views: readonly BuiltView[]): Promise<void> {
  for (const built of views) {
    let result: PgResult
    try {
      result = await run.exec(built.statement.sql, [...(built.statement.args ?? [])])
    } catch (error) {
      throw mapViewError(built, error, sqlState(error))
    }

    // A put that requires the row to be absent fails with a unique violation, never by its count.
    const {op} = built.view
    const counted = op.kind === 'delete' || op.condition !== 'absent'
    if (counted && viewConditionFailed(op, affectedRows(result, built))) {
      throw new ProjectorOperationError(built.view, REASON_CONDITION_FAILED)
    }
  }
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

// The manifest with postgres-views declared and its version bumped.
function withPostgresViews(manifest: FeatureManifest | null): FeatureManifest {
  const features = new Map(manifest?.features)
  features.set(POSTGRES_VIEWS_FEATURE, POSTGRES_VIEWS_VERSION)

  return {
    version: (manifest?.version ?? 0n) + 1n,
    features,
    historyDependent: manifest?.historyDependent ?? [],
    retention: manifest?.retention ?? [],
  }
}

// The journal compaction hooks of the journal's marker capability. They come with journal
// compaction's port in the journal module, which will declare them on JournalMarker itself.
interface CompactionMarker {
  checkCompactionMarker(options?: CallOptions): Promise<void>
  declareCompaction(options?: CallOptions): Promise<void>
}

// A stored head of 0 is an absent head.
function drift(entityId: string, stored: bigint, derived: bigint): StreamHeadDriftError {
  return new StreamHeadDriftError(entityId, stored > 0n ? stored : null, derived)
}
