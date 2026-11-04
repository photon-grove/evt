// The DynamoDB repository: one event-log table keyed by `pk` (the entity ID) and `sk` (the event
// sequence, 0 for the inline snapshot and -1 for the head item), as the storage format lays it
// out. Every commit is one TransactWriteItems call that opens with the feature-marker and
// rebuild-lock checks, and a failed one is classified by its cancellation reasons, then by reading
// the stream, the way the Go and Rust repositories classify it.
//
// Pages, retries and the probe's reads each depend on the call before them, so they await in loops
// on purpose.
/* oxlint-disable no-await-in-loop */

import {
  CompactionUncoveredError,
  ConflictError,
  EvtError,
  FeatureManifestChangedError,
  HistoryDependentProjectionError,
  RebuildInProgressError,
  SequenceGapError,
  StoreNotInitializedError,
  StreamHeadDriftError,
  StreamHeadsNotMigratedError,
  TransientError,
} from '../errors.js'
import type {SerializedEvent, SerializedSnapshot} from '../event.js'
import {
  type FeatureManifest,
  STREAM_HEADS_FEATURE,
  STREAM_HEADS_VERSION,
  checkSupported,
  declaresStreamHeads,
} from '../features.js'
import {int64Argument} from '../fields.js'
import {
  type CallOptions,
  type Commit,
  type Repository,
  type StoreInitializer,
  checkBudget,
  checkNotReserved,
  commitSizes,
  isReserved,
  validateCommit,
} from '../repository.js'
import type {DynamoClient, Page, RequestOptions, TransactItem} from './client.js'
import {
  HEAD_SORT_KEY,
  type Item,
  eventFromItem,
  headFromItem,
  snapshotFromItem,
  sortKey,
} from './item.js'
import {FEATURES_PK, manifestFromItem, markerItem} from './marker.js'
import {commitTransaction, controlKey, headKey, snapshotTransaction} from './requests.js'

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
}

// A declaration is a read-modify-write; racing declarations retry this many times.
const MAX_DECLARE_ATTEMPTS = 8

// Each page of a scan that only has to find one item.
const SCAN_PROBE_PAGE_SIZE = 100

// The most events one latest(n) page reads.
const LATEST_PAGE_SIZE = 100

// The most deletes in one compaction transaction, beside its marker check.
const COMPACTION_BATCH_SIZE = 99

/**
 * A {@link Repository} on a DynamoDB event-log table, through the AWS SDK's low-level `DynamoDB`
 * client or anything shaped like it ({@link DynamoClient}).
 *
 * The repository validates the store's feature manifest at its first write and caches the version
 * it validated. Every commit and snapshot write checks that version, so when another writer
 * declares a feature, the next write fails, validates the manifest again, and either refuses an
 * unsupported feature or fails once with a retryable {@link FeatureManifestChangedError}.
 */
export class DynamoRepository implements Repository, StoreInitializer {
  readonly #client: DynamoClient
  readonly #table: string
  readonly #lazy: boolean
  readonly #consistentRead: boolean

  // The manifest version this writer validated, or null until it validates.
  #validated: bigint | null = null
  // Serializes validation, so concurrent writes read and declare the manifest once.
  #lock: Promise<unknown> = Promise.resolve()

  constructor(client: DynamoClient, eventsTable: string, options: DynamoRepositoryOptions = {}) {
    this.#client = client
    this.#table = eventsTable
    this.#lazy = options.lazyInitialization ?? false
    this.#consistentRead = options.consistentRead ?? true
  }

  /** The events table this repository writes. */
  get eventsTable(): string {
    return this.#table
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
        checkSupported(manifest)
        if (declaresStreamHeads(manifest)) {
          return false
        }
        if (!(await this.#storeIsEmpty(request))) {
          throw new StreamHeadsNotMigratedError()
        }

        // Another initializer may have declared a feature this package doesn't model.
        const {manifest: declared, wrote} = await this.#declareOnEmptyStore(request)
        checkSupported(declared)

        return wrote
      } finally {
        // A declaration, or its retraction, moved the version: the next write reads it again.
        this.#validated = null
      }
    })
  }

  async commit(commit: Commit, options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    validateCommit(commit)
    checkNotReserved(commit.entityId)
    if (commit.events.length === 0) {
      return
    }

    // Every backend budgets a commit the same way, so an oversized one fails before any request.
    checkBudget(commitSizes(commit, () => undefined))

    const request = requestOptions(options)
    const version = await this.#markerVersion(request)
    try {
      await this.#client.transactWriteItems(
        {TransactItems: commitTransaction(this.#table, commit, version)},
        request,
      )
    } catch (error) {
      throw await this.#classifyCommit(commit, error, request)
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
      events.push(...decodeEvents(page))
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
      events.push(...decodeEvents(page))
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

    const request = requestOptions(options)
    const version = await this.#markerVersion(request)
    try {
      await this.#client.transactWriteItems(
        {TransactItems: snapshotTransaction(this.#table, snapshot, version)},
        request,
      )

      return true
    } catch (error) {
      const codes = cancellationCodes(error)
      const failed = codes?.indexOf(CONDITION_FAILED) ?? -1
      if (failed === 0) {
        throw await this.#markerCheckFailed(error, request)
      }
      if (failed === 1) {
        // The head doesn't cover the snapshot: a stream whose log is ahead of its head.
        const head = await this.#readHead(snapshot.entityId, request)
        const derived = await this.#deriveHead(snapshot.entityId, request)
        throw drift(
          snapshot.entityId,
          head,
          derived > eventSequence ? derived : eventSequence,
        )
      }
      if (failed === 2) {
        // A newer snapshot is stored, and stays.
        return false
      }
      if (codes?.some((code) => THROTTLED.has(code))) {
        throw new TransientError('the snapshot write was throttled', {cause: error})
      }

      throw error
    }
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
    checkSupported(manifest)
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
    checkSupported(manifest)
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
      checkSupported(declared.manifest)
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

  async #classifyCommit(commit: Commit, error: unknown, request: RequestOptions): Promise<unknown> {
    const codes = cancellationCodes(error)
    if (codes === null) {
      return error
    }

    const failed = codes.indexOf(CONDITION_FAILED)
    if (failed === -1) {
      if (codes.includes('TransactionConflict')) {
        return new ConflictError(
          commit.entityId,
          'transaction conflict: another writer was committing at the same time',
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

    // The stream's head update, then its event puts and its snapshot put.
    const head = 2
    const eventsEnd = head + 1 + commit.events.length
    const eventFailed = codes.slice(head + 1, eventsEnd).includes(CONDITION_FAILED)
    const {entityId} = commit
    const first = commit.events[0]!.sequence

    if (codes[head] === CONDITION_FAILED) {
      const verdict = await this.#probe(entityId, request)
      if (verdict.drift) {
        return verdict.drift
      }
      if (verdict.healthy && first > 1n && verdict.stored < first - 1n) {
        return new SequenceGapError(entityId, verdict.stored, first)
      }
    } else if (codes[head] === 'None' && eventFailed) {
      // The head update passed and an event put failed: an event sits above the head.
      const verdict = await this.#probe(entityId, request)

      return verdict.drift ?? drift(entityId, verdict.head, verdict.derived)
    }

    return new ConflictError(
      entityId,
      'conditional check failed: the sequence or snapshot changed since it was read',
    )
  }

  // Classifies a stream whose head update failed, as the failure-path probe does: a stored head
  // below the head its log derives is drift, an equal one is healthy, and one above it is either a
  // writer that committed in between (derive again) or drift.
  async #probe(entityId: string, request: RequestOptions): Promise<Verdict> {
    const derived = await this.#deriveHead(entityId, request)
    const head = await this.#readHead(entityId, request)
    const stored = head ?? 0n
    if (stored < derived) {
      return {healthy: false, drift: drift(entityId, head, derived), stored, head, derived}
    }
    if (stored === derived) {
      return {healthy: true, drift: null, stored, head, derived}
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

  // The stored head, or null when the stream has no head item.
  async #readHead(entityId: string, request: RequestOptions): Promise<bigint | null> {
    const output = await this.#client.getItem(
      {TableName: this.#table, Key: headKey(entityId), ConsistentRead: true},
      request,
    )
    const item = output.Item as Item | undefined
    if (!item || Object.keys(item).length === 0) {
      return null
    }

    // A head item without its attribute reads as head 0, as Go reads it, so it classifies as drift.
    return item['head'] === undefined ? 0n : headFromItem(item).head
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

    return item && Object.keys(item).length > 0 ? snapshotFromItem(item) : null
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
}

function drift(entityId: string, head: bigint | null, derived: bigint): StreamHeadDriftError {
  return new StreamHeadDriftError(entityId, head, derived)
}

function decodeEvents(items: Item[]): SerializedEvent[] {
  return items.map(eventFromItem).filter((event) => event.sequence >= 1n)
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

function isConditionalCheckFailed(error: unknown): boolean {
  return isNamed(error, 'ConditionalCheckFailedException')
}

function isNamed(error: unknown, name: string): boolean {
  return typeof error === 'object' && error !== null && (error as {name?: unknown}).name === name
}
