// The DynamoDB repository: one event-log table keyed by `pk` (the entity ID) and `sk` (the event
// sequence, 0 for the inline snapshot and -1 for the head item), as the storage format lays it
// out. Every commit is one TransactWriteItems call that opens with the feature-marker and
// rebuild-lock checks, and a failed one is classified by its cancellation reasons, then by reading
// the stream, the way the Go and Rust repositories classify it. Entity types with retention carry a
// `ttl` on every item, and the head item's is never lowered (storage format, "Retention").
//
// Pages, retries and the probe's reads each depend on the call before them, so they await in loops
// on purpose.
/* oxlint-disable no-await-in-loop */

import {
  CompactionUncoveredError,
  ConflictError,
  EvtError,
  FeatureManifestChangedError,
  FenceConflictError,
  HistoryDependentProjectionError,
  IndeterminateError,
  OutcomeConflictError,
  RebuildBeginIndeterminateError,
  RebuildConflictError,
  RebuildInProgressError,
  RebuildLeaseLostError,
  RebuildStateInvalidError,
  SequenceGapError,
  StoreNotInitializedError,
  StreamHeadDriftError,
  StreamHeadsNotMigratedError,
  TransientError,
  ProjectorOperationError,
  ViewsNotConfiguredError,
} from '../errors.js'
import type {EventLog, StreamFilter} from '../enumerate.js'
import type {SerializedEvent, SerializedSnapshot} from '../event.js'
import {
  type FeatureManifest,
  STREAM_HEADS_FEATURE,
  STREAM_HEADS_VERSION,
  checkSupported,
  declaresStreamHeads,
  JOURNAL_COMPACTION_FEATURE,
  JOURNAL_COMPACTION_VERSION,
  JOURNAL_MARKER,
  checkJournalWriter,
  type JournalDeclared,
  type JournalMarker,
  declaredAnything,
  isJournalWriter,
  journalDeclaration,
  withJournal,
  withoutJournal,
  missingHistoryDependent,
} from '../features.js'
import {int64Argument} from '../fields.js'
import {
  type CompactionWrite,
  type CompactionWritten,
  type Expect,
  checkCompactionWrites,
  compactionPartitionKey,
} from '../journal/compaction.js'
import {type JournalRow, journalRowFromView, readCounter} from '../journal/row.js'
import {
  COUNTER_OWNER,
  COUNTER_SORT_KEY,
  counterPartitionKey,
  parseRowSortKey,
  rowPartitionKey,
  rowSortKey,
} from '../journal/scope.js'
import {type Outcome, checkCommandId} from '../outcome.js'
import {type ProjectedView, REASON_CONDITION_FAILED} from '../projection.js'
import {
  MAX_REBUILD_BEGIN_ATTEMPTS,
  type RebuildBeginPlan,
  type RebuildFence,
  type RebuildGeneration,
  type RebuildLease,
  type RebuildState,
  checkRebuildOwner,
  checkRebuildViewBatch,
  fenceNow,
  observeRebuildEpoch,
  planRebuildBegin,
  rebuildLeaseSeconds,
} from '../rebuild-fence.js'
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
  isReserved,
  normalizeTransaction,
  rankFailures,
  transactionSizes,
} from '../repository.js'
import type {DynamoClient, Page, RequestOptions, TransactItem} from './client.js'
import {
  HEAD_SORT_KEY,
  type Item,
  eventFromItem,
  expiry,
  headFromItem,
  snapshotFromItem,
  sortKey,
  ttlFromItem,
} from './item.js'
import {FEATURES_PK, manifestFromItem, markerItem} from './marker.js'
import {OUTCOME_SORT_KEY, outcomeFromItem, outcomeKey} from './outcome.js'
import {
  beginRebuildItems,
  endRebuildItems,
  forceReleaseRebuildItems,
  generationFromItem,
  lockFromItem,
  rebuildGenerationKey,
  rebuildGuardCheck,
  rebuildLockKey,
  renewRebuildUpdate,
} from './rebuild.js'
import type {ViewRow} from '../views.js'
import {
  type DynamoViewClient,
  DynamoViewRepository,
  type DynamoViewRepositoryOptions,
  viewKey,
  viewRowFromItem,
  viewRowItem,
} from './view-repository.js'
import {DYNAMODB_BACKEND, transactItemSize} from './views.js'
import {
  type CommitTtl,
  type StreamLayout,
  type TransactionLayout,
  controlKey,
  headKey,
  projectorItems,
  buildTransaction,
  viewActions,
  snapshotTransaction,
} from './requests.js'

/** How a {@link DynamoRepository} reads and initializes its store. */
export interface DynamoRepositoryOptions {
  /**
   * Whether a write to a store that doesn't declare stream-heads initializes it, scanning the
   * table to make sure it holds no data, instead of refusing with a
   * {@link StoreNotInitializedError}. Off by default, so writers never need `dynamodb:Scan`:
   * initialize a fresh store once, at deploy time, with {@link DynamoRepository.initialize}.
   */
  lazyInitialization?: boolean
  /**
   * Whether event and snapshot reads are strongly consistent. On by default. Head reads, the
   * failure-path probe, compaction and the feature marker always read consistently.
   */
  consistentRead?: boolean
  /**
   * How long the items of each entity type live, in whole milliseconds, keyed by entity type.
   * Every event, snapshot and head item of a type with a positive retention carries a DynamoDB
   * `ttl`, the write time plus the retention in Unix seconds, for the table's TTL to delete. A
   * type that isn't listed, or has 0, is kept forever. TTL deletes items in no order, so retain
   * only short-lived streams that nothing replays. Declare the retained types in the store's
   * feature manifest too, from Go or Rust, so that their writers refuse read fences on them.
   */
  retention?: Readonly<Record<string, number>> | ReadonlyMap<string, number>
  /**
   * The clock that stamps `ttl`s and decides which have passed, and that rebuild leases expire by,
   * in Unix milliseconds as `Date.now` gives them, never seconds. `Date.now` by default.
   */
  clock?: () => number
  /**
   * The table commit-wide projectors' view operations are written to, in each commit's
   * transaction, keyed by string `pk` and `sk`, and the table
   * {@link DynamoRepository.viewRepository} reads. Without one, a commit that carries view
   * operations, and `viewRepository`, are refused with a {@link ViewsNotConfiguredError}.
   */
  viewsTable?: string
}

// The cancellation reasons of a view action the views table refused, beside a failed condition.
const VIEW_REFUSALS = new Set(['ValidationError', 'ItemCollectionSizeLimitExceeded'])

// A declaration is a read-modify-write; racing declarations retry this many times.
const MAX_DECLARE_ATTEMPTS = 8

// Each page of a scan that only has to find one item.
const SCAN_PROBE_PAGE_SIZE = 100

// The most events one latest(n) page reads.
const LATEST_PAGE_SIZE = 100

// The most deletes in one compaction transaction, beside its marker check.
const COMPACTION_BATCH_SIZE = 99

// How many streams' loaded `ttl`s a repository remembers before it forgets them all.
const MAX_TTL_FLOORS = 4096

// How often a snapshot write sends its head guard again after another writer moved the head.
const MAX_SNAPSHOT_GUARD_RETRIES = 4

/**
 * A {@link Repository} on a DynamoDB event-log table, through the AWS SDK's low-level `DynamoDB`
 * client or anything shaped like it ({@link DynamoClient}).
 *
 * The repository validates the store's feature manifest at its first write and caches the version
 * it validated. Every commit and snapshot write checks that version, so when another writer
 * declares a feature, the next write fails, validates the manifest again, and either refuses an
 * unsupported feature or fails once with a retryable {@link FeatureManifestChangedError}.
 */
export class DynamoRepository
  implements OutcomeRepository, StoreInitializer, EventLog, RebuildFence
{
  readonly #client: DynamoClient
  readonly #table: string
  readonly #lazy: boolean
  readonly #viewsTable: string | undefined
  readonly #consistentRead: boolean
  readonly #retention: ReadonlyMap<string, number>
  readonly #clock: () => number

  // The largest `ttl` this repository loaded for each stream, which a commit's head `ttl` covers.
  readonly #ttlFloors = new Map<string, number>()

  // The manifest version this writer validated, or null until it validates.
  #validated: bigint | null = null
  // Serializes validation, so concurrent writes read and declare the manifest once.
  #lock: Promise<unknown> = Promise.resolve()

  constructor(client: DynamoClient, eventsTable: string, options: DynamoRepositoryOptions = {}) {
    this.#client = client
    this.#table = eventsTable
    this.#lazy = options.lazyInitialization ?? false
    this.#consistentRead = options.consistentRead ?? true
    this.#retention = retentionMap(options.retention)
    this.#clock = options.clock ?? Date.now
    this.#viewsTable = options.viewsTable
  }

  /** It writes commit-wide view operations, to its views table. */
  writesViews(): boolean {
    return true
  }

  /** It applies a view put's expected-payload condition, in its views table. */
  appliesExpectedPayload(): boolean {
    return true
  }

  /** The events table this repository writes. */
  get eventsTable(): string {
    return this.#table
  }

  /**
   * A {@link DynamoViewRepository} over this repository's views table and client, so a handler
   * can commit and then read its own rows: point reads and partition listings are strongly
   * consistent (unless `consistentRead: false`), so they see every row a returned commit wrote,
   * as with Rust's `DynamoRepository::view_repository`. Refused with a
   * {@link ViewsNotConfiguredError} when no views table is configured, and with a `TypeError` when
   * the client has no `batchWriteItem` or `batchGetItem` (the AWS SDK's aggregated `DynamoDB`
   * client has both).
   */
  viewRepository(options?: DynamoViewRepositoryOptions): DynamoViewRepository {
    const table = this.#journalTable()
    if (!isViewClient(this.#client)) {
      throw new TypeError('a view repository needs a client with batchWriteItem and batchGetItem')
    }

    return new DynamoViewRepository(this.#client, table, options)
  }

  /**
   * Declares stream-heads on a store that holds no data, and resolves `true` when this call
   * declared it. It scans the table with strongly consistent reads, so run it once, at deploy
   * time, with a role that may Scan. Concurrent initializations let exactly one declare.
   */
  async initialize(options?: CallOptions): Promise<boolean> {
    options?.signal?.throwIfAborted()
    const request = requestOptions(options)

    return this.#exclusive(async () => {
      try {
        const manifest = await this.#readManifest(request)
        checkSupported(manifest, isJournalWriter(this))
        if (declaresStreamHeads(manifest)) {
          return false
        }
        if (!(await this.#storeIsEmpty(request))) {
          throw new StreamHeadsNotMigratedError()
        }

        // Another initializer may have declared a feature this package doesn't model.
        const {manifest: declared, wrote} = await this.#declareOnEmptyStore(request)
        checkSupported(declared, isJournalWriter(this))

        return wrote
      } finally {
        // A declaration, or its retraction, moved the version: the next write reads it again.
        this.#validated = null
      }
    })
  }

  /**
   * Adds the entity types to the feature manifest's historyDependent set in one conditional
   * declaration, so compaction refuses them in every process. The store is validated first, as a
   * write validates it: an uninitialized store is refused (or, initializing lazily, declared) before
   * anything is declared. Types already declared are left alone, and nothing is written when there
   * are none to add.
   */
  async declareHistoryDependent(types: readonly string[], options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    const request = requestOptions(options)

    await this.#exclusive(async () => {
      await this.#validate(true, request)
      const declared = await this.#declare((draft) => {
        // Another writer may have declared a feature this package doesn't model since.
        const stored = {...draft, version: 0n}
        checkSupported(stored, isJournalWriter(this))
        const missing = missingHistoryDependent(stored, types)
        draft.historyDependent.push(...missing)

        return missing.length > 0
      }, request)
      if (declared.wrote) {
        // The declaration moved the version: the next write reads it again.
        this.#validated = null
      }
    })
  }

  async rebuildGeneration(options?: CallOptions): Promise<RebuildGeneration> {
    options?.signal?.throwIfAborted()

    return generationFromItem(await this.#controlItem(rebuildGenerationKey(), options))
  }

  /** The generation, then the lock: two strongly consistent reads, as Go's and Rust's are. */
  async rebuildState(options?: CallOptions): Promise<RebuildState> {
    const generation = await this.rebuildGeneration(options)
    const lock = lockFromItem(await this.#controlItem(rebuildLockKey(), options))

    return {generation, lock}
  }

  async beginRebuild(owner: string, leaseMs: number, options?: CallOptions): Promise<RebuildLease> {
    options?.signal?.throwIfAborted()
    checkRebuildOwner(owner)
    const seconds = rebuildLeaseSeconds(leaseMs)

    // Each attempt reads what the one before it lost to.
    let invalid: RebuildStateInvalidError | null = null
    for (let attempt = 1; attempt <= MAX_REBUILD_BEGIN_ATTEMPTS; attempt++) {
      const state = await this.rebuildState(options)
      const now = fenceNow(this.#clock)
      let plan: RebuildBeginPlan
      try {
        plan = planRebuildBegin(state, now)
      } catch (error) {
        // The two reads may straddle another rebuild's transition: read again.
        if (error instanceof RebuildStateInvalidError) {
          invalid = error
          continue
        }
        throw error
      }
      invalid = null

      const lease = {
        owner,
        generation: state.generation.value + plan.increment,
        leaseExpiresAt: now + seconds,
      }
      try {
        await this.#client.transactWriteItems(
          {TransactItems: beginRebuildItems(this.#table, plan, lease, now)},
          requestOptions(options),
        )

        return lease
      } catch (error) {
        const codes = cancellationCodes(error)
        if (
          codes?.some((code) => code === 'ConditionalCheckFailed' || code === 'TransactionConflict')
        ) {
          continue
        }
        if (codes === null && !throttled(error) && resultUnknown(error)) {
          throw new RebuildBeginIndeterminateError(lease, state.generation.value, error)
        }
        throw fenceError(error, codes)
      }
    }

    throw invalid ?? new RebuildConflictError(MAX_REBUILD_BEGIN_ATTEMPTS)
  }

  async renewRebuild(
    lease: RebuildLease,
    leaseMs: number,
    options?: CallOptions,
  ): Promise<RebuildLease> {
    options?.signal?.throwIfAborted()
    checkRebuildOwner(lease.owner)
    const seconds = rebuildLeaseSeconds(leaseMs)
    const update = this.#client.updateItem
    if (update === undefined) {
      throw new EvtError('invalidArgument', 'the DynamoDB client has no updateItem to renew with')
    }

    const expiresAt = fenceNow(this.#clock) + seconds
    try {
      await update.call(
        this.#client,
        renewRebuildUpdate(this.#table, lease, expiresAt),
        requestOptions(options),
      )
    } catch (error) {
      if (isConditionalCheckFailed(error)) {
        throw new RebuildLeaseLostError(lease.owner)
      }
      throw fenceError(error, null)
    }

    return {...lease, leaseExpiresAt: expiresAt}
  }

  async endRebuild(lease: RebuildLease, options?: CallOptions): Promise<RebuildGeneration> {
    options?.signal?.throwIfAborted()
    checkRebuildOwner(lease.owner)
    await this.#fenceTransaction(endRebuildItems(this.#table, lease), lease.owner, options)

    return {value: lease.generation + 1n, forceReleased: false}
  }

  async forceReleaseRebuild(owner: string, options?: CallOptions): Promise<RebuildGeneration> {
    options?.signal?.throwIfAborted()
    checkRebuildOwner(owner)
    const seen = await this.rebuildGeneration(options)
    if (seen.value % 2n === 0n) {
      throw new RebuildLeaseLostError(owner)
    }
    await this.#fenceTransaction(
      forceReleaseRebuildItems(this.#table, owner, seen.value),
      owner,
      options,
    )

    return {value: seen.value + 2n, forceReleased: true}
  }

  /**
   * Writes the batch to the views table in one transaction whose last action guards the lease. A
   * lost or expired lease outranks every view's failure.
   */
  async writeRebuildViews(
    lease: RebuildLease,
    views: readonly ProjectedView[],
    options?: CallOptions,
  ): Promise<void> {
    options?.signal?.throwIfAborted()
    checkRebuildOwner(lease.owner)
    checkRebuildViewBatch(views)
    const actions = viewActions(views, this.#viewsTable)
    const guard = rebuildGuardCheck(this.#table, lease, fenceNow(this.#clock))
    try {
      await this.#client.transactWriteItems(
        {TransactItems: [...actions.map(({action}) => action), guard]},
        requestOptions(options),
      )
    } catch (error) {
      const codes = cancellationCodes(error)
      if (codes !== null) {
        if (codes[actions.length] === 'ConditionalCheckFailed') {
          throw new RebuildLeaseLostError(lease.owner)
        }
        for (const [index, {view}] of actions.entries()) {
          const code = codes[index]
          if (code === REASON_CONDITION_FAILED) {
            throw new ProjectorOperationError(view, code)
          }
          if (code !== undefined && VIEW_REFUSALS.has(code)) {
            throw new ProjectorOperationError(view, code, error)
          }
        }
      }
      throw fenceError(error, codes)
    }
  }

  // A control row of the fence, read strongly consistent whatever the repository's reads are.
  async #controlItem(key: Item, options?: CallOptions): Promise<Item | null> {
    const output = await this.#client.getItem(
      {TableName: this.#table, Key: key, ConsistentRead: true},
      requestOptions(options),
    )
    const item = output.Item as Item | undefined

    return item && Object.keys(item).length > 0 ? item : null
  }

  // Ends or force-releases a lease: any failed condition means the caller no longer holds it.
  async #fenceTransaction(
    items: TransactItem[],
    owner: string,
    options?: CallOptions,
  ): Promise<void> {
    try {
      await this.#client.transactWriteItems({TransactItems: items}, requestOptions(options))
    } catch (error) {
      const codes = cancellationCodes(error)
      if (codes?.includes('ConditionalCheckFailed')) {
        throw new RebuildLeaseLostError(owner)
      }
      throw fenceError(error, codes)
    }
  }

  /** A Scan page holds any mix of entities, in no order. */
  readonly batchesByEntity = false

  /**
   * The event log, one batch per Scan page of events, in the table's order and not grouped by
   * entity: an entity's events can span pages. Rows below `sk = 1` (snapshots and head items)
   * and the reserved namespaces are skipped, and pages the filter empties out aren't yielded.
   * Reads are strongly consistent unless the repository says otherwise.
   */
  async *streamAllEvents(
    filter?: StreamFilter,
    options?: CallOptions,
  ): AsyncGenerator<SerializedEvent[]> {
    options?.signal?.throwIfAborted()
    const request = requestOptions(options)
    for await (const items of this.#enumerate(undefined, filter?.entityType, request)) {
      const events = items.filter((item) => (sortKey(item) ?? 0n) >= 1n).map(eventFromItem)
      if (events.length > 0) {
        yield events
      }
    }
  }

  /**
   * Each entity's head, folded from a key-only Scan of the whole log: the larger of its highest
   * event sort key and its snapshot's `eventSeq`. Head items are derived from the log, not part of
   * it, so they don't count.
   */
  async entityHeads(entityType?: string, options?: CallOptions): Promise<Map<string, bigint>> {
    options?.signal?.throwIfAborted()
    const request = requestOptions(options)
    const heads = new Map<string, bigint>()
    for await (const items of this.#enumerate('pk, sk, eventSeq', entityType, request)) {
      for (const item of items) {
        const pk = item['pk']
        const sk = sortKey(item)
        if (pk === undefined || !('S' in pk) || sk === undefined || sk < 0n) {
          continue
        }

        const seq = item['eventSeq']
        const head = sk > 0n ? sk : seq !== undefined && 'N' in seq ? BigInt(seq.N) : 0n
        if (head > (heads.get(pk.S) ?? 0n)) {
          heads.set(pk.S, head)
        }
      }
    }

    return heads
  }

  async commit(commit: Commit, options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    const normalized = commitAsTransaction(commit)
    if (!isEmptyTransaction(normalized)) {
      await this.#write(normalized, options)
    }
  }

  /**
   * Commits every stream and checks every fence in one `TransactWriteItems` call: the control
   * checks, the projector operations, each written stream's actions, a check on each fenced
   * stream's head, then the outcome put. A fence on a type this repository retains is refused
   * before any request, and one on a type the store's manifest retains after the marker's
   * validation.
   */
  async commitTransaction(transaction: TransactionCommit, options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    const normalized = normalizeTransaction(transaction)
    if (!isEmptyTransaction(normalized)) {
      await this.#write(normalized, options)
    }
  }

  /**
   * The command's outcome row, read strongly consistent whatever {@link consistentRead} says: an
   * eventually consistent miss would run a committed command again.
   */
  async outcome(commandId: string, options?: CallOptions): Promise<Outcome | null> {
    options?.signal?.throwIfAborted()
    checkCommandId(commandId)

    const output = await this.#client.getItem(
      {
        TableName: this.#table,
        Key: {pk: {S: outcomeKey(commandId)}, sk: {N: OUTCOME_SORT_KEY.toString()}},
        ConsistentRead: true,
      },
      requestOptions(options),
    )
    const item = output.Item as Item | undefined

    return item && Object.keys(item).length > 0 ? outcomeFromItem(item) : null
  }

  async #write(transaction: NormalizedTransaction, options?: CallOptions): Promise<void> {
    // One `ttl` per entity type for the whole transaction, so a stream's head's is never below one
    // of its own items.
    const ttls = new Map<string, number | undefined>()
    const ttlFor = (entityType: string): number | undefined => {
      if (!ttls.has(entityType)) {
        ttls.set(entityType, this.#ttlFor(entityType))
      }

      return ttls.get(entityType)
    }

    // Every backend budgets a commit the same way, so an oversized one fails before any request.
    // The projector operations are sized from the actions this repository sends, which refuses a
    // group for another backend here too.
    // View operations need the views table, refused without one before anything else.
    viewActions(transaction.views, this.#viewsTable)
    const sizes = projectorItems(transaction.projections).map(transactItemSize)
    const projections = [{backend: DYNAMODB_BACKEND, operations: sizes, sizes}]
    checkBudget(transactionSizes({...transaction, projections}, ttlFor))

    // A fence on a type this repository retains needs no request to refuse.
    checkFenceRetention(transaction, (entityType) => this.#retention.has(entityType))

    const request = requestOptions(options)
    const version = await this.#markerVersion(request)
    if (transaction.fences.length > 0) {
      // The store's manifest may retain a type this repository doesn't. The marker check holds it
      // at the version read here, or fails the transaction.
      const manifest = await this.#readManifest(request)
      const retention = manifest?.retention ?? []
      checkFenceRetention(transaction, (entityType) => retention.includes(entityType))
    }

    const {items, layout} = buildTransaction(
      this.#table,
      transaction,
      version,
      (commit) => this.#commitTtl(commit, ttlFor(commit.events[0]!.entityType)),
      this.#viewsTable,
    )
    try {
      await this.#client.transactWriteItems({TransactItems: items}, request)
    } catch (error) {
      throw await this.#classifyCommit(transaction, layout, error, request)
    }
  }

  async events(entityId: string, options?: CallOptions): Promise<SerializedEvent[]> {
    return this.eventsAfter(entityId, 0n, options)
  }

  async eventsAfter(
    entityId: string,
    after: bigint,
    options?: CallOptions,
  ): Promise<SerializedEvent[]> {
    options?.signal?.throwIfAborted()
    int64Argument(after, 'after')

    // A negative `after` would reach the head item (sk -1) and the snapshot (sk 0).
    const from = after > 0n ? after : 0n
    const events: SerializedEvent[] = []
    const pages = this.#query(entityId, from, true, undefined, requestOptions(options))
    for await (const page of pages) {
      events.push(...this.#decodeEvents(page))
    }

    return events
  }

  async latest(entityId: string, n: number, options?: CallOptions): Promise<SerializedEvent[]> {
    options?.signal?.throwIfAborted()
    if (!Number.isSafeInteger(n) || n < 1) {
      throw new EvtError(
        'invalidArgument',
        `latest(n) requires a whole number of at least one event: got n = ${n}`,
      )
    }

    const limit = Math.min(n, LATEST_PAGE_SIZE)
    const events: SerializedEvent[] = []
    for await (const page of this.#query(entityId, 0n, false, limit, requestOptions(options))) {
      events.push(...this.#decodeEvents(page))
      if (events.length >= n) {
        break
      }
    }

    return events.slice(0, n)
  }

  async snapshot(entityId: string, options?: CallOptions): Promise<SerializedSnapshot | null> {
    options?.signal?.throwIfAborted()

    return this.#readSnapshot(entityId, this.#consistentRead, requestOptions(options))
  }

  async putSnapshot(snapshot: SerializedSnapshot, options?: CallOptions): Promise<boolean> {
    options?.signal?.throwIfAborted()
    checkNotReserved(snapshot.entityId)
    int64Argument(snapshot.sequence, 'snapshot.sequence')
    const eventSequence = int64Argument(snapshot.eventSequence, 'snapshot.eventSequence')

    // Stamped now, as the snapshot's own items are, so it expires with its stream.
    const ttl = this.#ttlFor(snapshot.entityType)
    const request = requestOptions(options)
    const version = await this.#markerVersion(request)
    for (let retries = 0, refresh = true; ; retries++) {
      const guarded = ttl === undefined ? undefined : {ttl, refresh}
      try {
        await this.#client.transactWriteItems(
          {TransactItems: snapshotTransaction(this.#table, snapshot, version, guarded)},
          request,
        )
        this.#observeTtl(snapshot.entityId, ttl)

        return true
      } catch (error) {
        const codes = cancellationCodes(error)
        const failed = codes?.indexOf(CONDITION_FAILED) ?? -1
        if (failed === 0) {
          throw await this.#markerCheckFailed(error, request)
        }
        if (failed === 2) {
          // A newer snapshot is stored, and stays.
          return false
        }
        if (failed !== 1) {
          if (codes?.some((code) => THROTTLED.has(code))) {
            throw new TransientError('the snapshot write was throttled', {cause: error})
          }
          if (codes?.includes('TransactionConflict')) {
            throw new TransientError(
              `transaction conflict on ${snapshot.entityId}: another writer was committing at the same time`,
              {cause: error},
            )
          }

          throw error
        }

        // The head guard failed. A refreshing guard can fail on its ttl clause alone, when another
        // writer moved the head: the head covers the snapshot, so send it again, refreshing only
        // when the stored ttl is still below the snapshot's.
        const head = await this.#readHead(snapshot.entityId, request)
        const refreshed = guarded?.refresh === true && eventSequence > 0n
        if (refreshed && head !== null && head.head >= eventSequence) {
          if (retries >= MAX_SNAPSHOT_GUARD_RETRIES) {
            throw new TransientError(
              `the head item of ${snapshot.entityId} kept moving under its snapshot write`,
              {cause: error},
            )
          }

          refresh = (head.ttl ?? 0) < ttl!
          continue
        }

        // The head doesn't cover the snapshot: a stream whose log is ahead of its head.
        const derived = await this.#deriveHead(snapshot.entityId, request)
        throw drift(
          snapshot.entityId,
          head?.head ?? null,
          derived > eventSequence ? derived : eventSequence,
        )
      }
    }
  }

  /** A scope's journal counter, read strongly consistently from the views table: 0 when none. */
  async readCounter(scope: string, options?: CallOptions): Promise<bigint> {
    options?.signal?.throwIfAborted()
    const output = await this.#client.getItem(
      {
        TableName: this.#journalTable(),
        Key: viewKey(counterPartitionKey(scope), COUNTER_SORT_KEY),
        ConsistentRead: true,
      },
      requestOptions(options),
    )
    const item = output.Item as Item | undefined

    return item === undefined || Object.keys(item).length === 0
      ? 0n
      : readCounter(scope, viewRowFromItem(item))
  }

  /** Several scopes' journal counters, in the order given. */
  async readCounters(scopes: readonly string[], options?: CallOptions): Promise<bigint[]> {
    return Promise.all(scopes.map((scope) => this.readCounter(scope, options)))
  }

  /** The highest position with a journal row in the scope: 0 when it has none. */
  async readLastPosition(scope: string, options?: CallOptions): Promise<bigint> {
    options?.signal?.throwIfAborted()
    const pk = rowPartitionKey(scope)
    const page = await this.#client.query(
      {
        TableName: this.#journalTable(),
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: {':pk': {S: pk}},
        ConsistentRead: true,
        ScanIndexForward: false,
        Limit: 1,
      },
      requestOptions(options),
    )
    const item = page.Items?.[0] as Item | undefined
    if (item === undefined) {
      return 0n
    }
    const row = viewRowFromItem(item)
    const position = parseRowSortKey(row.sk)
    if (position === null) {
      throw new EvtError('invalidRecord', `journal row ${pk}/${row.sk} has no position`)
    }

    return position
  }

  /**
   * Up to `limit` journal rows of the scope above `after`, ascending, with strongly consistent
   * queries that follow the last evaluated key until they have the rows or reach the end.
   */
  async readRows(
    scope: string,
    after: bigint,
    limit: number,
    options?: CallOptions,
  ): Promise<JournalRow[]> {
    options?.signal?.throwIfAborted()
    const table = this.#journalTable()
    const request = requestOptions(options)
    const rows: JournalRow[] = []
    let start: Item | undefined
    // Each page continues from the last.
    while (rows.length < limit) {
      const page: Page = await this.#client.query(
        {
          TableName: table,
          KeyConditionExpression: 'pk = :pk AND sk > :after',
          ExpressionAttributeValues: {
            ':pk': {S: rowPartitionKey(scope)},
            ':after': {S: rowSortKey(after)},
          },
          ConsistentRead: true,
          Limit: limit - rows.length,
          ...(start && {ExclusiveStartKey: start}),
        },
        request,
      )
      for (const item of page.Items ?? []) {
        rows.push(journalRowFromView(scope, viewRowFromItem(item as Item)))
      }
      start = page.LastEvaluatedKey as Item | undefined
      if (start === undefined) {
        break
      }
    }

    return rows.slice(0, limit)
  }

  /** The journal's declarations on this store's manifest. Internal. */
  readonly [JOURNAL_MARKER]: JournalMarker = {
    declareJournal: (types, options) => {
      options?.signal?.throwIfAborted()
      checkJournalWriter(this)
      const request = requestOptions(options)

      return this.#exclusive(async () => {
        // Validated as a commit validates it: an uninitialized store is refused before anything is
        // declared, or, initializing lazily, declared.
        await this.#validate(true, request)
        // The feature, then the history, a declaration each.
        const feature = await this.#declareJournal(types, true, request)
        const history = await this.#declareJournal(types, false, request)

        return {journal: feature.journal, history: history.history}
      })
    },
    retractJournal: async (declared, options) => {
      checkJournalWriter(this)
      if (!declaredAnything(declared)) {
        return
      }
      const request = requestOptions(options)
      await this.#exclusive(async () => {
        try {
          await this.#declare((draft) => {
            checkSupported({...draft, version: 0n}, isJournalWriter(this))
            const next = withoutJournal({...draft, version: 0n}, declared)
            draft.features = new Map(next.features)
            draft.historyDependent = [...next.historyDependent]

            return true
          }, request)
        } finally {
          this.#validated = null
        }
      })
    },
    checkCompactionMarker: async (options) => {
      checkSupported(await this.#readManifest(requestOptions(options)), true)
    },
    declareCompaction: async (options) => {
      const request = requestOptions(options)
      await this.#exclusive(async () => {
        const result = await this.#declare((draft) => {
          const stored = {...draft, version: 0n}
          checkSupported(stored, true)
          if (draft.features.get(JOURNAL_COMPACTION_FEATURE) === JOURNAL_COMPACTION_VERSION) {
            return false
          }
          draft.features.set(JOURNAL_COMPACTION_FEATURE, JOURNAL_COMPACTION_VERSION)

          return true
        }, request)
        if (result.wrote) {
          this.#validated = null
        }
      })
    },
  }

  // Declares the journal feature (`feature`) or the entity types history-dependent, and resolves
  // to what it wrote. Only a journal writer declares. The caller holds #lock.
  async #declareJournal(
    types: readonly string[],
    feature: boolean,
    request: RequestOptions,
  ): Promise<JournalDeclared> {
    let declared: JournalDeclared = {journal: false, history: []}
    const result = await this.#declare((draft) => {
      // Another writer may have declared a feature this package doesn't model since.
      const stored = {...draft, version: 0n}
      checkSupported(stored, isJournalWriter(this))
      const wanted = journalDeclaration(stored, types)
      declared = feature
        ? {journal: wanted.journal, history: []}
        : {journal: false, history: wanted.history}
      if (!declaredAnything(declared)) {
        return false
      }
      const next = withJournal(stored, declared)
      draft.features = new Map(next.features)
      draft.historyDependent = [...next.historyDependent]

      return true
    }, request)
    if (!result.wrote) {
      return {journal: false, history: []}
    }
    // The declaration moved the version: the next write reads it again.
    this.#validated = null

    return declared
  }

  /** A row of a scope's journal compaction partition, strongly consistent, or null. */
  async readCompactionRow(
    scope: string,
    sk: string,
    options?: CallOptions,
  ): Promise<ViewRow | null> {
    options?.signal?.throwIfAborted()
    const output = await this.#client.getItem(
      {
        TableName: this.#journalTable(),
        Key: viewKey(compactionPartitionKey(scope), sk),
        ConsistentRead: true,
      },
      requestOptions(options),
    )
    const item = output.Item as Item | undefined

    return item === undefined || Object.keys(item).length === 0 ? null : viewRowFromItem(item)
  }

  /**
   * One journal compaction transaction: the writes in order, then, with an epoch, a check on the
   * events table that the rebuild generation still holds it. A failed condition names the write,
   * or the epoch check; a concurrent transaction or throttling is transient.
   */
  async writeCompaction(
    epoch: bigint | null,
    writes: readonly CompactionWrite[],
    options?: CallOptions,
  ): Promise<CompactionWritten> {
    options?.signal?.throwIfAborted()
    checkCompactionWrites(writes)
    const table = this.#journalTable()
    const items = writes.map((write) => compactionItem(table, write))
    if (epoch !== null) {
      items.push(this.#epochCheck(epoch))
    }

    try {
      await this.#client.transactWriteItems({TransactItems: items}, requestOptions(options))
    } catch (error) {
      const codes = cancellationCodes(error)
      if (codes === null) {
        throw error
      }
      if (epoch !== null && codes[writes.length] === CONDITION_FAILED) {
        return {kind: 'epochChanged'}
      }
      const at = codes.slice(0, writes.length).indexOf(CONDITION_FAILED)
      if (at !== -1) {
        return {kind: 'refused', at}
      }
      if (codes.some((code) => code === 'TransactionConflict' || THROTTLED.has(code))) {
        throw new TransientError('a journal compaction write met another writer', {cause: error})
      }
      throw error
    }

    return {kind: 'written'}
  }

  /** The clock journal compaction reads, in whole Unix seconds. */
  compactionNow(): number {
    return Math.floor(this.#clock() / 1000)
  }

  /**
   * The settled rebuild generation, read strongly consistently from the events table: even, 0
   * with no generation item. An odd one is a `RebuildUnavailableError`.
   */
  observeRebuildEpoch(options?: CallOptions): Promise<bigint> {
    return observeRebuildEpoch(this, options)
  }


  // The check of a compaction write that the rebuild generation is still `epoch`'s.
  #epochCheck(epoch: bigint): TransactItem {
    return {
      ConditionCheck: {
        TableName: this.#table,
        Key: controlKey(REBUILD_GENERATION_PK),
        ExpressionAttributeNames: {'#value': 'value'},
        ...(epoch === 0n
          ? {ConditionExpression: 'attribute_not_exists(#value)'}
          : {
              ConditionExpression: '#value = :epoch',
              ExpressionAttributeValues: {':epoch': {N: epoch.toString()}},
            }),
      },
    }
  }

  // The views table, which the journal and the view repository need.
  #journalTable(): string {
    if (this.#viewsTable === undefined || this.#viewsTable === '') {
      throw new ViewsNotConfiguredError(DYNAMODB_BACKEND)
    }

    return this.#viewsTable
  }

  async compactBelow(entityId: string, through: bigint, options?: CallOptions): Promise<number> {
    options?.signal?.throwIfAborted()
    checkNotReserved(entityId)
    if (int64Argument(through, 'through') < 1n) {
      return 0
    }

    const request = requestOptions(options)
    const snapshot = await this.#readSnapshot(entityId, true, request)
    let check = this.#compactionCheck(await this.#readManifest(request), entityId, snapshot)
    if (snapshot === null || snapshot.eventSequence < through) {
      throw new CompactionUncoveredError(entityId, through, snapshot?.eventSequence ?? null)
    }

    const keys: Item[] = []
    for await (const page of this.#rangeKeys(entityId, through, request)) {
      keys.push(...page)
    }

    // Each batch of deletes checks the manifest the writer validated. A declaration between
    // batches stops the later ones, but can't roll back those already deleted.
    let deleted = 0
    for (let start = 0; start < keys.length; start += COMPACTION_BATCH_SIZE) {
      const batch = keys.slice(start, start + COMPACTION_BATCH_SIZE)
      for (let attempt = 0; ; attempt++) {
        const deletes: TransactItem[] = batch.map((key) => ({
          Delete: {TableName: this.#table, Key: key},
        }))
        try {
          await this.#client.transactWriteItems({TransactItems: [check, ...deletes]}, request)
          deleted += batch.length
          break
        } catch (error) {
          if (cancellationCodes(error)?.indexOf(CONDITION_FAILED) !== 0) {
            throw error
          }

          // Validate the new manifest first, so a declaration that refuses compaction says so.
          check = this.#compactionCheck(await this.#readManifest(request), entityId, snapshot)
          if (attempt >= MAX_DECLARE_ATTEMPTS) {
            throw new FeatureManifestChangedError({cause: error})
          }
        }
      }
    }

    return deleted
  }

  // Validates the manifest for compaction, which never declares stream-heads, and builds the check
  // each batch carries on the version it read.
  #compactionCheck(
    manifest: FeatureManifest | null,
    entityId: string,
    snapshot: SerializedSnapshot | null,
  ): TransactItem {
    checkSupported(manifest, isJournalWriter(this))
    if (manifest === null || !declaresStreamHeads(manifest)) {
      throw this.#lazy ? new StreamHeadsNotMigratedError() : new StoreNotInitializedError()
    }
    if (snapshot && manifest.historyDependent.includes(snapshot.entityType)) {
      throw new HistoryDependentProjectionError(snapshot.entityType, entityId)
    }

    return {
      ConditionCheck: {
        TableName: this.#table,
        Key: controlKey(FEATURES_PK),
        ConditionExpression: 'version = :observed',
        ExpressionAttributeValues: {':observed': {N: manifest.version.toString()}},
      },
    }
  }

  // The `ttl` an item of the entity type written now carries, or undefined without retention.
  #ttlFor(entityType: string): number | undefined {
    const retention = this.#retention.get(entityType)
    const ttl = retention === undefined ? undefined : expiry(this.#clock(), retention)

    // An expiry at or before the epoch is no ttl at all, as Go writes none for 0.
    return ttl !== undefined && ttl > 0 ? ttl : undefined
  }

  // The commit's `ttl`s: its own on its items, and on its head the larger of its own and what this
  // writer loaded for the stream, except on a create, which has nothing to cover.
  #commitTtl(commit: Commit, ttl: number | undefined): CommitTtl | undefined {
    if (ttl === undefined) {
      return undefined
    }

    const create = commit.events[0]!.sequence <= 1n
    const floor = create ? 0 : (this.#ttlFloors.get(commit.entityId) ?? 0)

    return {items: ttl, head: Math.max(ttl, floor)}
  }

  // Remembers a loaded item's `ttl` for its stream. The memory is bounded: a full one is cleared,
  // which costs at most a conflict and a retry, whose load observes the `ttl` again.
  #observeTtl(entityId: string, ttl: number | undefined): void {
    if (ttl === undefined || ttl <= 0) {
      return
    }

    const current = this.#ttlFloors.get(entityId)
    if (current === undefined && this.#ttlFloors.size >= MAX_TTL_FLOORS) {
      this.#ttlFloors.clear()
    }
    if (current === undefined || ttl > current) {
      this.#ttlFloors.set(entityId, ttl)
    }
  }

  #decodeEvents(items: Item[]): SerializedEvent[] {
    const events: SerializedEvent[] = []
    for (const item of items) {
      const event = eventFromItem(item)
      if (event.sequence >= 1n) {
        this.#observeTtl(event.entityId, ttlFromItem(item))
        events.push(event)
      }
    }

    return events
  }

  // The manifest version the next write checks, validating it first when this writer hasn't.
  #markerVersion(request: RequestOptions): Promise<bigint> {
    return this.#exclusive(async () => {
      const version = await this.#validate(true, request)
      if (version === null) {
        throw new RangeError('a declaring validation always caches a version')
      }

      return version
    })
  }

  // Reads and validates the manifest, unless this writer cached a validation. A store that doesn't
  // declare stream-heads is refused as uninitialized, unless the writer initializes lazily: then
  // one that holds data is refused as unmigrated, and an empty one is declared when `declare` is
  // set. Resolves to the cached version, or null when nothing was cached. The caller holds #lock.
  async #validate(declare: boolean, request: RequestOptions): Promise<bigint | null> {
    if (this.#validated !== null) {
      return this.#validated
    }

    let manifest = await this.#readManifest(request)
    checkSupported(manifest, isJournalWriter(this))
    if (!declaresStreamHeads(manifest)) {
      if (!this.#lazy) {
        throw new StoreNotInitializedError()
      }
      if (!(await this.#storeIsEmpty(request))) {
        throw new StreamHeadsNotMigratedError()
      }
      if (!declare) {
        return null
      }

      // Another writer may have declared a feature this package doesn't model.
      const declared = await this.#declareOnEmptyStore(request)
      checkSupported(declared.manifest, isJournalWriter(this))
      manifest = declared.manifest
    }

    this.#validated = manifest!.version

    return this.#validated
  }

  // A failed marker check: the manifest changed under this writer. It validates the new one at
  // once, so an unsupported feature refuses the write, and anything else is retryable.
  async #markerCheckFailed(cause: unknown, request: RequestOptions): Promise<EvtError> {
    try {
      // Dropped under the lock, so a validation already running can't cache the old version again.
      await this.#exclusive(() => {
        this.#validated = null

        return this.#validate(false, request)
      })
    } catch (error) {
      if (error instanceof EvtError) {
        return error
      }

      throw error
    }

    return new FeatureManifestChangedError({cause})
  }

  // Maps a cancelled transaction to the error it reports. The control checks outrank everything.
  // Then every failed stream and fence is classified, and the ranking reports the first drift, else
  // the first gap, else the first other failure, in transaction order.
  async #classifyCommit(
    transaction: NormalizedTransaction,
    layout: TransactionLayout,
    error: unknown,
    request: RequestOptions,
  ): Promise<unknown> {
    const codes = cancellationCodes(error)
    if (codes === null) {
      // A commit that records an outcome, and whose result is unknown, may still land: the store
      // settles it by reading the outcome row.
      return transaction.outcome !== null && resultUnknown(error)
        ? new IndeterminateError({cause: error})
        : error
    }

    const failed = codes.indexOf(CONDITION_FAILED)
    if (failed === -1) {
      // A view the backend refused: an item the views table can't hold.
      const refused = layout.views.find(({index}) => VIEW_REFUSALS.has(codes[index] ?? ''))
      if (refused !== undefined) {
        return new ProjectorOperationError(refused.view, codes[refused.index]!, error)
      }

      // Another transaction was writing one of the items, perhaps one this commit never changes
      // (a control check): nothing was written and nothing is known to be stale, so it is
      // transient, like throttling, and never a conflict, as in Go and Rust.
      const colliding = codes.indexOf('TransactionConflict')
      if (colliding !== -1) {
        // A conflict on the outcome put names another attempt of the same command, still
        // committing: the store polls its row briefly instead of reading it once.
        const outcomeRow =
          layout.outcome !== null && codes[layout.outcome] === 'TransactionConflict'
        return new TransientError(
          `transaction conflict on ${entityAt(layout, colliding)}: another writer was committing at the same time`,
          {cause: error, transactionConflict: true, outcomeRow},
        )
      }
      if (codes.some((code) => THROTTLED.has(code))) {
        return new TransientError('the commit was throttled', {cause: error})
      }

      return error
    }

    // The control checks outrank every stream failure: the marker, then the rebuild lock.
    if (failed === 0) {
      return this.#markerCheckFailed(error, request)
    }
    if (codes[1] === CONDITION_FAILED) {
      return new RebuildInProgressError()
    }

    // A lost outcome put outranks every stream and fence failure: the command already ran, and
    // the store resolves it through the winner's row. It's never a conflict.
    if (layout.outcome !== null && codes[layout.outcome] === CONDITION_FAILED) {
      return new OutcomeConflictError(transaction.outcome!.commandId)
    }

    const failures: EvtError[] = []
    for (const stream of layout.streams) {
      const failure = await this.#streamFailure(stream, codes, request)
      if (failure !== null) {
        failures.push(failure)
      }
    }
    for (const {index, fence} of layout.fences) {
      if (codes[index] !== CONDITION_FAILED) {
        continue
      }

      const verdict = await this.#probe(fence.entityId, false, request)
      failures.push(
        verdict.drift ?? new FenceConflictError(fence.entityId, fence.head, fence.provenance),
      )
    }

    const ranked = rankFailures(failures)
    if (ranked !== undefined) {
      return ranked
    }

    // Only a projector group's operation failed: its own error, as Go reports it.
    if (failed < layout.projectorEnd) {
      return error
    }

    // A view whose condition failed, or that the views table refused.
    for (const {index, view} of layout.views) {
      const code = codes[index] ?? ''
      if (code === CONDITION_FAILED) {
        return new ProjectorOperationError(view, code)
      }
      if (VIEW_REFUSALS.has(code)) {
        return new ProjectorOperationError(view, code, error)
      }
    }

    return new ConflictError(
      entityAt(layout, failed),
      'conditional check failed: the sequence or snapshot changed since it was read',
    )
  }

  // What a written stream contributes to a failed transaction, or null when none of its actions
  // failed. A failed head update is classified by the probe. A head update that passed while an
  // event put failed is an event above the head, unless retention explains it. A failed snapshot
  // put alone is an ordinary conflict.
  async #streamFailure(
    stream: StreamLayout,
    codes: string[],
    request: RequestOptions,
  ): Promise<EvtError | null> {
    const {commit} = stream
    const {entityId} = commit
    const first = commit.events[0]!.sequence
    const retained = this.#retention.has(commit.events[0]!.entityType)
    const eventFailed = codes.slice(stream.head + 1, stream.eventsEnd).includes(CONDITION_FAILED)
    const snapshotFailed = stream.snapshot !== null && codes[stream.snapshot] === CONDITION_FAILED

    if (codes[stream.head] === CONDITION_FAILED) {
      const verdict = await this.#probe(entityId, retained, request)
      if (verdict.drift) {
        return verdict.drift
      }
      if (verdict.healthy && first > 1n && verdict.stored < first - 1n) {
        return new SequenceGapError(entityId, verdict.stored, first)
      }

      return conflict(entityId)
    }

    if (eventFailed && codes[stream.head] === 'None') {
      // With retention, TTL can delete the head before an expired event, so a create passes its
      // head condition and fails on the leftover item: a conflict until TTL deletes the rest. The
      // probe already read the remaining items when it found the head item absent below the head
      // its log derives; read them only if it didn't.
      const verdict = await this.#probe(entityId, retained, request)
      const expired =
        retained && (verdict.expired ?? (await this.#remainingItemsExpired(entityId, request)))
      if (!expired) {
        return verdict.drift ?? drift(entityId, verdict.head, verdict.derived)
      }

      return conflict(entityId)
    }

    return eventFailed || snapshotFailed ? conflict(entityId) : null
  }

  // Classifies a stream whose head update failed, as the failure-path probe does: a stored head
  // below the head its log derives is drift, an equal one is healthy, and one above it is either a
  // writer that committed in between (derive again) or drift. With retention, TTL deletes items in
  // no order, so a head above its log, or a missing head over items that have all expired, is a
  // conflict that heals once TTL deletes the rest.
  async #probe(entityId: string, retained: boolean, request: RequestOptions): Promise<Verdict> {
    const derived = await this.#deriveHead(entityId, request)
    const head = (await this.#readHead(entityId, request))?.head ?? null
    const stored = head ?? 0n
    if (stored < derived) {
      if (retained && head === null) {
        const expired = await this.#remainingItemsExpired(entityId, request)
        const drifted = expired ? null : drift(entityId, head, derived)

        return {healthy: false, drift: drifted, stored, head, derived, expired}
      }

      return {healthy: false, drift: drift(entityId, head, derived), stored, head, derived}
    }
    if (stored === derived) {
      return {healthy: true, drift: null, stored, head, derived}
    }
    if (retained) {
      // The head outlived part of the log it counted: TTL deleted the tail first.
      return {healthy: false, drift: null, stored, head, derived}
    }

    const second = await this.#deriveHead(entityId, request)
    const drifted = second < stored ? drift(entityId, head, second) : null

    return {healthy: false, drift: drifted, stored, head, derived: second}
  }

  // A stream's head as its log derives it, with strongly consistent reads: the higher of its
  // snapshot's event sequence and its highest event sequence.
  async #deriveHead(entityId: string, request: RequestOptions): Promise<bigint> {
    const snapshot = await this.#client.getItem(
      {
        TableName: this.#table,
        Key: controlKey(entityId),
        ConsistentRead: true,
        ProjectionExpression: 'eventSeq',
      },
      request,
    )
    const eventSeq = (snapshot.Item as Item | undefined)?.['eventSeq']
    let derived = eventSeq && 'N' in eventSeq ? BigInt(eventSeq.N) : 0n

    const latest = await this.#client.query(
      {
        TableName: this.#table,
        KeyConditionExpression: 'pk = :pk AND sk > :sk',
        ExpressionAttributeValues: {':pk': {S: entityId}, ':sk': {N: '0'}},
        ConsistentRead: true,
        ProjectionExpression: 'sk',
        ScanIndexForward: false,
        Limit: 1,
      },
      request,
    )
    const [item] = (latest.Items ?? []) as Item[]
    const sequence = item && sortKey(item)
    if (sequence !== undefined && sequence > derived) {
      derived = sequence
    }

    return derived
  }

  // Whether every item still stored for the stream, its snapshot and its events, has a `ttl` that
  // has passed, so TTL will delete it. An item without a `ttl` never expires.
  async #remainingItemsExpired(entityId: string, request: RequestOptions): Promise<boolean> {
    const now = Math.floor(this.#clock() / 1000)
    const expired = (item: Item): boolean => {
      const ttl = ttlFromItem(item)

      return ttl !== undefined && ttl <= now
    }

    const snapshot = await this.#client.getItem(
      {
        TableName: this.#table,
        Key: controlKey(entityId),
        ConsistentRead: true,
        // The key too, so a snapshot without a ttl doesn't read back empty, as an absent one does.
        ProjectionExpression: 'sk, #ttl',
        ExpressionAttributeNames: {'#ttl': 'ttl'},
      },
      request,
    )
    const item = snapshot.Item as Item | undefined
    if (item && Object.keys(item).length > 0 && !expired(item)) {
      return false
    }

    let start: Item | undefined
    do {
      const page = await this.#client.query(
        {
          TableName: this.#table,
          KeyConditionExpression: 'pk = :pk AND sk > :sk',
          ExpressionAttributeNames: {'#ttl': 'ttl'},
          ExpressionAttributeValues: {':pk': {S: entityId}, ':sk': {N: '0'}},
          ConsistentRead: true,
          ProjectionExpression: 'sk, #ttl',
          ...(start && {ExclusiveStartKey: start}),
        },
        request,
      )
      if (((page.Items ?? []) as Item[]).some((event) => !expired(event))) {
        return false
      }
      start = page.LastEvaluatedKey as Item | undefined
    } while (start !== undefined)

    return true
  }

  // The stored head and its `ttl`, or null when the stream has no head item.
  async #readHead(
    entityId: string,
    request: RequestOptions,
  ): Promise<{head: bigint; ttl: number | undefined} | null> {
    const output = await this.#client.getItem(
      {TableName: this.#table, Key: headKey(entityId), ConsistentRead: true},
      request,
    )
    const item = output.Item as Item | undefined
    if (!item || Object.keys(item).length === 0) {
      return null
    }

    // A head item without its attribute reads as head 0, as Go reads it, so it classifies as drift.
    const head =
      item['head'] === undefined ? {head: 0n, ttl: ttlFromItem(item)} : headFromItem(item)
    this.#observeTtl(entityId, head.ttl)

    return head
  }

  async #readSnapshot(
    entityId: string,
    consistent: boolean,
    request: RequestOptions,
  ): Promise<SerializedSnapshot | null> {
    const output = await this.#client.getItem(
      {TableName: this.#table, Key: controlKey(entityId), ConsistentRead: consistent},
      request,
    )
    const item = output.Item as Item | undefined
    if (!item || Object.keys(item).length === 0) {
      return null
    }

    this.#observeTtl(entityId, ttlFromItem(item))

    return snapshotFromItem(item)
  }

  // The marker row, with a strongly consistent read, or null when the store has none.
  async #readManifest(request: RequestOptions): Promise<FeatureManifest | null> {
    const output = await this.#client.getItem(
      {TableName: this.#table, Key: controlKey(FEATURES_PK), ConsistentRead: true},
      request,
    )
    const item = output.Item as Item | undefined

    return item && Object.keys(item).length > 0 ? manifestFromItem(item) : null
  }

  // Applies a change to the manifest in one conditional write that bumps its version by one, and
  // retries against a racing declaration. A change that reports false needs no write: the
  // manifest already says what it would.
  async #declare(
    change: (draft: Draft) => boolean,
    request: RequestOptions,
  ): Promise<{manifest: FeatureManifest; wrote: boolean}> {
    for (let attempt = 0; ; attempt++) {
      const stored = await this.#readManifest(request)
      const draft: Draft = {
        features: new Map(stored?.features),
        historyDependent: [...(stored?.historyDependent ?? [])],
        retention: [...(stored?.retention ?? [])],
      }
      if (!change(draft)) {
        return {manifest: stored ?? {...draft, version: 0n}, wrote: false}
      }

      const manifest: FeatureManifest = {...draft, version: (stored?.version ?? 0n) + 1n}
      const condition =
        stored === null
          ? {ConditionExpression: 'attribute_not_exists(pk)'}
          : {
              ConditionExpression: 'version = :observed',
              ExpressionAttributeValues: {':observed': {N: stored.version.toString()}},
            }
      try {
        await this.#client.putItem(
          {TableName: this.#table, Item: markerItem(manifest), ...condition},
          request,
        )

        return {manifest, wrote: true}
      } catch (error) {
        if (!isConditionalCheckFailed(error) || attempt >= MAX_DECLARE_ATTEMPTS) {
          throw error
        }
      }
    }
  }

  // Declares stream-heads on a store that held no data when it was scanned, then closes the window
  // since: the declaration commits first, so every marker-reading writer sees it, and only then is
  // the table scanned for a stream without a head item, which retracts it. When another
  // initializer declared first, its manifest is the one reported, as it stands now.
  async #declareOnEmptyStore(
    request: RequestOptions,
  ): Promise<{manifest: FeatureManifest; wrote: boolean}> {
    const declared = await this.#declare((draft) => {
      if (draft.features.get(STREAM_HEADS_FEATURE) === STREAM_HEADS_VERSION) {
        return false
      }

      draft.features.set(STREAM_HEADS_FEATURE, STREAM_HEADS_VERSION)

      return true
    }, request)

    if (!declared.wrote) {
      // The winner ran its own verification, which may have retracted its declaration.
      const current = await this.#readManifest(request)
      if (!declaresStreamHeads(current)) {
        throw new StreamHeadsNotMigratedError()
      }

      return {manifest: current!, wrote: false}
    }

    if (await this.#hasHeadlessStream(request)) {
      await this.#declare((draft) => draft.features.delete(STREAM_HEADS_FEATURE), request)
      throw new StreamHeadsNotMigratedError()
    }

    return declared
  }

  // Whether the table holds no item outside the reserved namespaces, by a strongly consistent scan
  // that stops at the first one it finds.
  async #storeIsEmpty(request: RequestOptions): Promise<boolean> {
    for await (const items of this.#scan('pk', request)) {
      if (items.some((item) => !reservedItem(item))) {
        return false
      }
    }

    return true
  }

  // Whether any stream has items but no head item. A scan isn't atomic across items, so a stream
  // that looks headless is confirmed by reading its head item.
  async #hasHeadlessStream(request: RequestOptions): Promise<boolean> {
    const streams = new Set<string>()
    const headed = new Set<string>()
    for await (const items of this.#scan('pk, sk', request)) {
      for (const item of items) {
        const pk = item['pk']
        if (pk === undefined || !('S' in pk) || isReserved(pk.S)) {
          continue
        }

        streams.add(pk.S)
        if (sortKey(item) === HEAD_SORT_KEY) {
          headed.add(pk.S)
        }
      }
    }

    for (const entityId of streams) {
      if (!headed.has(entityId) && (await this.#readHead(entityId, request)) === null) {
        return true
      }
    }

    return false
  }

  // The pages of a Scan of the log outside the reserved namespaces, narrowed to an entity type.
  async *#enumerate(
    projection: string | undefined,
    entityType: string | undefined,
    request: RequestOptions,
  ): AsyncGenerator<Item[]> {
    const values: Item = {':system': {S: 'SYSTEM#'}, ':outcome': {S: 'command-outcome#'}}
    let filter = 'NOT begins_with(pk, :system) AND NOT begins_with(pk, :outcome)'
    if (entityType !== undefined) {
      filter += ' AND entityType = :et'
      values[':et'] = {S: entityType}
    }

    let start: Item | undefined
    do {
      // Checked between pages too, so a client that ignores the signal still stops.
      request.abortSignal?.throwIfAborted()
      const page: Page = await this.#client.scan(
        {
          TableName: this.#table,
          ConsistentRead: this.#consistentRead,
          FilterExpression: filter,
          ExpressionAttributeValues: values,
          ...(projection !== undefined && {ProjectionExpression: projection}),
          ...(start && {ExclusiveStartKey: start}),
        },
        request,
      )
      yield ((page.Items ?? []) as Item[]).filter((item) => !reservedItem(item))
      start = page.LastEvaluatedKey as Item | undefined
    } while (start !== undefined)
  }

  async *#scan(projection: string, request: RequestOptions): AsyncGenerator<Item[]> {
    let start: Item | undefined
    do {
      const page: Page = await this.#client.scan(
        {
          TableName: this.#table,
          ConsistentRead: true,
          ProjectionExpression: projection,
          Limit: SCAN_PROBE_PAGE_SIZE,
          ...(start && {ExclusiveStartKey: start}),
        },
        request,
      )
      yield (page.Items ?? []) as Item[]
      start = page.LastEvaluatedKey as Item | undefined
    } while (start !== undefined)
  }

  // The pages of a stream's events after `after`, in either order.
  async *#query(
    entityId: string,
    after: bigint,
    ascending: boolean,
    limit: number | undefined,
    request: RequestOptions,
  ): AsyncGenerator<Item[]> {
    let start: Item | undefined
    do {
      const page = await this.#client.query(
        {
          TableName: this.#table,
          KeyConditionExpression: 'pk = :pk AND sk > :sk',
          ExpressionAttributeValues: {':pk': {S: entityId}, ':sk': {N: after.toString()}},
          ConsistentRead: this.#consistentRead,
          ScanIndexForward: ascending,
          ...(limit !== undefined && {Limit: limit}),
          ...(start && {ExclusiveStartKey: start}),
        },
        request,
      )
      yield (page.Items ?? []) as Item[]
      start = page.LastEvaluatedKey as Item | undefined
    } while (start !== undefined)
  }

  // The keys of the stream's events 1 to `through`, read consistently.
  async *#rangeKeys(
    entityId: string,
    through: bigint,
    request: RequestOptions,
  ): AsyncGenerator<Item[]> {
    let start: Item | undefined
    do {
      const page = await this.#client.query(
        {
          TableName: this.#table,
          KeyConditionExpression: 'pk = :pk AND sk BETWEEN :lo AND :hi',
          ExpressionAttributeValues: {
            ':pk': {S: entityId},
            ':lo': {N: '1'},
            ':hi': {N: through.toString()},
          },
          ConsistentRead: true,
          ProjectionExpression: 'pk, sk',
          ...(start && {ExclusiveStartKey: start}),
        },
        request,
      )
      const items = (page.Items ?? []) as Item[]
      yield items.flatMap(({pk, sk}) => (pk && sk ? [{pk, sk}] : []))
      start = page.LastEvaluatedKey as Item | undefined
    } while (start !== undefined)
  }

  // Runs `task` after every task queued before it, so validation reads and declares once.
  #exclusive<T>(task: () => Promise<T>): Promise<T> {
    const result = this.#lock.then(task, task)
    this.#lock = result.catch(() => undefined)

    return result
  }
}

const CONDITION_FAILED = 'ConditionalCheckFailed'

// The cancellation reasons of a transaction DynamoDB throttled, which Go and Rust retry too.
const THROTTLED = new Set(['ThrottlingError', 'ProvisionedThroughputExceeded'])

interface Draft {
  features: Map<string, number>
  historyDependent: string[]
  retention: string[]
}

interface Verdict {
  healthy: boolean
  drift: StreamHeadDriftError | null
  /** The stored head, 0 when the stream has none. */
  stored: bigint
  head: bigint | null
  derived: bigint
  /** What `#remainingItemsExpired` found, when the probe read the remaining items. */
  expired?: boolean
}

function conflict(entityId: string): ConflictError {
  return new ConflictError(
    entityId,
    'conditional check failed: the sequence or snapshot changed since it was read',
  )
}

// The stream an action of a transaction belongs to, for a failure no stream check explains: the
// written or fenced stream at that index, or else the transaction's first.
function entityAt(layout: TransactionLayout, index: number): string {
  for (const stream of layout.streams) {
    if (index >= stream.head && (index < stream.eventsEnd || index === stream.snapshot)) {
      return stream.commit.entityId
    }
  }

  const fenced = layout.fences.find((each) => each.index === index)
  if (fenced !== undefined) {
    return fenced.fence.entityId
  }

  return layout.streams[0]?.commit.entityId ?? layout.fences[0]?.fence.entityId ?? ''
}

function drift(entityId: string, head: bigint | null, derived: bigint): StreamHeadDriftError {
  return new StreamHeadDriftError(entityId, head, derived)
}

// The retention option as a map of the entity types with a positive retention, each checked to be
// whole milliseconds.
function retentionMap(
  retention: Readonly<Record<string, number>> | ReadonlyMap<string, number> | undefined,
): ReadonlyMap<string, number> {
  const entries = isMap(retention) ? [...retention.entries()] : Object.entries(retention ?? {})
  const retained = new Map<string, number>()
  for (const [entityType, milliseconds] of entries) {
    if (!Number.isSafeInteger(milliseconds) || milliseconds < 0) {
      throw new EvtError(
        'invalidArgument',
        `the retention of ${entityType} must be whole, non-negative milliseconds: ${milliseconds}`,
      )
    }
    if (milliseconds > 0) {
      retained.set(entityType, milliseconds)
    }
  }

  return retained
}

// A map by its methods, so one from another realm counts too.
function isMap<K, V>(value: object | undefined): value is ReadonlyMap<K, V> {
  const map = value as Partial<ReadonlyMap<K, V>> | undefined

  return typeof map?.get === 'function' && typeof map.entries === 'function'
}

function reservedItem(item: Item): boolean {
  const pk = item['pk']

  return pk !== undefined && 'S' in pk && isReserved(pk.S)
}

function requestOptions(options: CallOptions | undefined): RequestOptions {
  return options?.signal ? {abortSignal: options.signal} : {}
}

// The reason codes of a cancelled transaction, one per action, or null for any other error.
function cancellationCodes(error: unknown): string[] | null {
  if (!isNamed(error, 'TransactionCanceledException')) {
    return null
  }

  const reasons = (error as {CancellationReasons?: unknown}).CancellationReasons
  if (!Array.isArray(reasons)) {
    return null
  }

  return reasons.map((reason) => {
    const code = (reason as {Code?: unknown} | null)?.Code

    return typeof code === 'string' ? code : ''
  })
}

// The SDK errors that leave a commit's result unknown, whatever their HTTP status.
const UNKNOWN_RESULT = new Set([
  'InternalServerError',
  'ServiceUnavailable',
  'TransactionInProgressException',
  'IdempotentParameterMismatchException',
])

// Whether a failed commit may have applied: anything but a refusal DynamoDB answered with a 4xx
// status. A timeout, a lost connection, an unreadable response, an abort while the call was in
// flight and any 5xx may follow a transaction that applied.
function resultUnknown(error: unknown): boolean {
  if (typeof error !== 'object' || error === null) {
    return true
  }

  const {name, $metadata} = error as {name?: unknown; $metadata?: {httpStatusCode?: unknown}}
  if (typeof name === 'string' && UNKNOWN_RESULT.has(name)) {
    return true
  }

  const status = $metadata?.httpStatusCode

  // A 2xx whose body couldn't be read carries its real status: the transaction may have applied.
  return typeof status !== 'number' || status < 400 || status >= 500
}

// The SDK errors of a throttled request: a refusal that didn't apply, which a retry may pass.
const THROTTLED_NAMES = new Set([
  'ThrottlingException',
  'ProvisionedThroughputExceededException',
  'RequestLimitExceeded',
])

function throttled(error: unknown): boolean {
  const name = typeof error === 'object' && error !== null ? (error as {name?: unknown}).name : null

  return typeof name === 'string' && THROTTLED_NAMES.has(name)
}

// A fence request's failure that isn't the lease's: transient when DynamoDB throttled it or a
// transaction conflicted, as Go and Rust classify it, and otherwise the error itself.
function fenceError(error: unknown, codes: string[] | null): unknown {
  const transient =
    throttled(error) ||
    (codes?.some((code) => code === 'TransactionConflict' || THROTTLED.has(code)) ?? false)

  return transient
    ? new TransientError('the rebuild fence request was throttled or conflicted', {cause: error})
    : error
}

function isConditionalCheckFailed(error: unknown): boolean {
  return isNamed(error, 'ConditionalCheckFailedException')
}

function isNamed(error: unknown, name: string): boolean {
  return typeof error === 'object' && error !== null && (error as {name?: unknown}).name === name
}

// The rebuild fence's generation item, on the events table.
const REBUILD_GENERATION_PK = 'SYSTEM#REBUILD#GENERATION'

// The conditions of a compaction write that expects a row: absent, or the engine's exact payload.
const COMPACTION_ABSENT = 'attribute_not_exists(pk)'
const COMPACTION_PAYLOAD = 'entityID = :engine AND payload = :expected'
const COMPACTION_DELETE = 'attribute_not_exists(pk) OR entityID = :owner'
const compactionDecoder = new TextDecoder('utf-8', {fatal: true, ignoreBOM: true})

// A compaction write's condition on what it expects at its key.
function expected(
  expect: Expect,
): {ConditionExpression?: string; ExpressionAttributeValues?: Item} {
  switch (expect.kind) {
    case 'any':
      return {}
    case 'absent':
      return {ConditionExpression: COMPACTION_ABSENT}
    case 'payload':
      return {
        ConditionExpression: COMPACTION_PAYLOAD,
        ExpressionAttributeValues: {
          ':expected': {S: payloadText(expect.payload)},
          ':engine': {S: COUNTER_OWNER},
        },
      }
  }
}

// The transaction action of one compaction write.
function compactionItem(table: string, write: CompactionWrite): TransactItem {
  switch (write.kind) {
    case 'put':
      return {Put: {TableName: table, Item: viewRowItem(write.row), ...expected(write.expect)}}
    case 'delete':
      return {
        Delete: {
          TableName: table,
          Key: viewKey(write.pk, write.sk),
          ConditionExpression: COMPACTION_DELETE,
          ExpressionAttributeValues: {':owner': {S: write.owner}},
        },
      }
    case 'check': {
      const condition = expected(write.expect)

      return {
        ConditionCheck: {
          TableName: table,
          Key: viewKey(write.pk, write.sk),
          // A check needs a condition; `any` checks that the key is a key.
          ConditionExpression:
            condition.ConditionExpression ?? 'attribute_not_exists(pk) OR attribute_exists(pk)',
          ...(condition.ExpressionAttributeValues && {
            ExpressionAttributeValues: condition.ExpressionAttributeValues,
          }),
        },
      }
    }
  }
}

function payloadText(payload: Uint8Array): string {
  try {
    return compactionDecoder.decode(payload)
  } catch {
    throw new EvtError('invalidArgument', 'a compaction payload must be UTF-8')
  }
}

// Whether a repository's client also makes the batch calls a view repository needs.
function isViewClient(client: DynamoClient): client is DynamoClient & DynamoViewClient {
  const view = client as Partial<DynamoViewClient>

  return typeof view.batchWriteItem === 'function' && typeof view.batchGetItem === 'function'
}
