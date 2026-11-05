// The storage contract every backend implements, and the commit rules every backend applies the
// same way: validation, the reserved namespaces, and the transaction budget.

import {headActionSize, headFence, headUpdate} from './dynamodb/head.js'
import {eventItem, itemSize, snapshotItem} from './dynamodb/item.js'
import {FEATURES_PK, REBUILD_LOCK_PK} from './dynamodb/marker.js'
import {outcomeItemSize} from './dynamodb/outcome.js'
import {
  CommitTooLargeError,
  EvtError,
  type FenceProvenance,
  ProjectorOnlyCommitError,
  ReservedEntityIdError,
  RetentionConflictError,
  SequenceGapError,
  StreamHeadDriftError,
} from './errors.js'
import {type SerializedEvent, type SerializedSnapshot, eventId} from './event.js'
import {type Outcome, checkCommitText, checkOutcome, storedOutcome} from './outcome.js'
import {MAX_COMMIT_ITEM_BYTES} from './snapshots.js'
import {compareUtf8} from './utf8.js'

/** A snapshot to write with a commit. Its event sequence is the commit's last event. */
export interface NewSnapshot {
  entityType: string
  /** The stored snapshot's sequence plus one, or 1 for the stream's first snapshot. */
  sequence: bigint
  /** The aggregate state after the commit's events. */
  payload: Uint8Array
}

/**
 * Operations a projector adds to a commit, for the backend it names to write in the commit's own
 * transaction (Go's transaction groups). The DynamoDB repository writes `dynamodb` groups, whose
 * operations are `TransactWriteItems` actions. The in-memory repository budgets every group and
 * writes none, as Go's does, so code with projectors runs on it unchanged.
 */
export interface ProjectorGroup {
  /** The backend whose transaction carries the operations, such as `dynamodb`. */
  readonly backend: string
  /** The operations, in the order they're written. */
  readonly operations: readonly unknown[]
  /** Each operation's size in the commit budget, as DynamoDB counts it. */
  readonly sizes: readonly number[]
}

/**
 * Everything a repository writes atomically for one command. A commit with no events is a no-op,
 * and its snapshot is ignored, unless it carries projector operations, which are refused, or an
 * outcome, which is written alone (an outcome-only commit).
 */
export interface Commit {
  /** The stream the events are appended to. */
  entityId: string
  /** The new events, in ascending sequence order. */
  events: SerializedEvent[]
  /** The snapshot to write, when the commit reaches a snapshot boundary. */
  snapshot?: NewSnapshot | null | undefined
  /** The operations the aggregate's projectors add, written first, in order. */
  projections?: readonly ProjectorGroup[] | null | undefined
  /**
   * The command outcome to record in the same transaction, as its last write, on an
   * {@link OutcomeRepository}. Its event IDs are replaced by the commit's own.
   */
  outcome?: Outcome | null | undefined
}

/** Options every repository call takes. */
export interface CallOptions {
  /**
   * Aborts the call. An abort before the call starts rejects with the signal's reason and writes
   * nothing. An abort while a live backend's write is in flight rejects too, but the write may
   * have been applied: load the stream again before deciding it wasn't.
   */
  signal?: AbortSignal | undefined
}

/**
 * An append-only event log with one optional snapshot and one head per stream (ADR 0007). Every
 * backend must:
 *
 * - refuse a commit built wrong ({@link validateCommit}) with `invalidCommit`;
 * - keep a head per stream, the highest event sequence ever committed to it, which a commit
 *   raises to its last event and compaction never touches;
 * - refuse a commit whose first event doesn't follow the stored head with a
 *   {@link ConflictError}, or with a {@link SequenceGapError} when it skips past the head;
 * - report a stream whose head disagrees with its events or snapshot as a
 *   {@link StreamHeadDriftError}, ranked above a gap and any other conflict;
 * - refuse a commit's snapshot whose sequence isn't the stored snapshot's plus one (1 with none
 *   stored) with a {@link ConflictError};
 * - write a commit's events, head and snapshot atomically, or nothing, with the projector
 *   operations it writes (a backend writes only its own groups), and refuse projector operations
 *   without events with a {@link ProjectorOnlyCommitError};
 * - refuse an entity ID in a reserved namespace on every write path, before any request;
 * - refuse every write (a commit, a snapshot write, compaction) to a store whose feature manifest
 *   declares a pair this package doesn't model, with an {@link UnsupportedFeatureError}, and to
 *   one that doesn't declare stream-heads: with a {@link StreamHeadsNotMigratedError} when it
 *   holds data, and otherwise either by declaring stream-heads on the first commit or snapshot
 *   write or with a {@link StoreNotInitializedError}, as the backend initializes;
 * - refuse a commit, but not a snapshot write or compaction, while a projection rebuild holds the
 *   store, with a {@link RebuildInProgressError}, after the manifest's refusals and before every
 *   stream's;
 * - refuse compaction of an entity whose type the manifest declares history-dependent, with a
 *   {@link HistoryDependentProjectionError};
 * - return events in ascending sequence order, and `latest` in descending order.
 */
export interface Repository {
  /** Appends a commit's events, and its snapshot if it has one, atomically. */
  commit(commit: Commit, options?: CallOptions): Promise<void>

  /** Every stored event of the stream, ascending. */
  events(entityId: string, options?: CallOptions): Promise<SerializedEvent[]>

  /** The stream's events after sequence `after`, ascending. */
  eventsAfter(entityId: string, after: bigint, options?: CallOptions): Promise<SerializedEvent[]>

  /**
   * The stream's last `n` events, newest first, never the snapshot. `n` must be a safe integer of
   * at least 1, or the call fails with `invalidArgument` before any request.
   */
  latest(entityId: string, n: number, options?: CallOptions): Promise<SerializedEvent[]>

  /** The stream's snapshot, or null when it has none. */
  snapshot(entityId: string, options?: CallOptions): Promise<SerializedSnapshot | null>

  /**
   * Writes a snapshot outside a commit, such as a catch-up snapshot. It never lowers the stored
   * snapshot's event sequence: it replaces a snapshot at or below it and resolves `true`, or
   * leaves a newer one in place and resolves `false`. The stream's head must cover the snapshot,
   * or the write fails with a {@link StreamHeadDriftError}. A snapshot at event sequence 0 needs no
   * head.
   */
  putSnapshot(snapshot: SerializedSnapshot, options?: CallOptions): Promise<boolean>

  /**
   * Deletes the stream's events with sequences 1 to `through`, but only when the stored snapshot
   * covers them (its event sequence is at least `through`), and resolves to how many it deleted.
   * It never deletes the snapshot or touches the head. A `through` below 1 deletes nothing.
   */
  compactBelow(entityId: string, through: bigint, options?: CallOptions): Promise<number>
}

/**
 * A repository whose store can be initialized explicitly: its feature manifest declares
 * stream-heads before any writer writes. A strict backend, such as DynamoDB by default, refuses
 * every write until then. One whose writers initialize a fresh store on their first write, such
 * as the in-memory one, needs it only where code treats every backend alike.
 */
export interface StoreInitializer {
  /**
   * Declares stream-heads on a store that holds no data, and resolves `true` when this call
   * declared it. A store that already declares it is left alone and resolves `false`. One that
   * holds data without it is refused with a {@link StreamHeadsNotMigratedError}, and one that
   * declares an unsupported feature with an {@link UnsupportedFeatureError}.
   */
  initialize(options?: CallOptions): Promise<boolean>
}

/**
 * A stream a transaction read but doesn't write, whose head must not move before it commits (ADR
 * 0007): the fence passes while the stream's stored head still equals `head`.
 */
export interface ReadFence {
  entityId: string
  /** The stream's entity type, which a fence on a retained type is refused for. */
  entityType: string
  /** The head the transaction observed: 0 for a stream with no events. */
  head: bigint
  /** Whether the engine read the head itself, or the caller stamped it. */
  provenance: FenceProvenance
}

/**
 * Everything one transaction writes atomically: the streams it appends to, and fences on the
 * streams it read. A fence on a stream the transaction also writes is dropped, since that stream's
 * head update already conditions on the head it read. A transaction that writes nothing and fences
 * nothing is a no-op.
 */
export interface TransactionCommit {
  /** The streams written, each a {@link Commit}, at most one per stream. */
  streams: readonly Commit[]
  /** The streams read but not written, at most one fence per stream. */
  fences?: readonly ReadFence[] | null | undefined
  /**
   * The command outcome to record in the same transaction, after the fences, on an
   * {@link OutcomeRepository}. Its event IDs are replaced by the ones the transaction writes. With
   * no events it's an outcome-only commit: a rejection or an accepted no-op, still fenced.
   */
  outcome?: Outcome | null | undefined
}

/**
 * A repository that commits several streams atomically, with read fences (ADR 0004). A
 * transaction is refused as a commit is, and also:
 *
 * - with `invalidCommit` when it writes a stream twice, fences a stream twice, fences at a head
 *   below 0 or at the largest int64, or carries a snapshot for a stream it writes no events to;
 * - with a {@link RetentionConflictError} when it fences an entity type with retention: by the
 *   repository's own configuration before any request, and by the store's manifest after the
 *   manifest's other refusals, but always before the rebuild lock's;
 * - with a {@link FenceConflictError} when a fenced stream's head moved, unless the stream
 *   drifted, ranked after every drift and gap of the transaction, in transaction order.
 */
export interface TransactionalRepository extends Repository {
  /** Appends to every stream and checks every fence atomically. */
  commitTransaction(transaction: TransactionCommit, options?: CallOptions): Promise<void>
}

/**
 * A repository that records command outcomes (ADR 0004, C3): it writes a commit's or a
 * transaction's `outcome` as the transaction's last write, conditioned on no row existing for the
 * command ID, and reads it back. It must:
 *
 * - refuse an outcome that fails {@link checkOutcome}, and a commit that carries one with a string
 *   U+0000 or a lone surrogate would corrupt, before any request;
 * - count the outcome item in the commit budget;
 * - report a lost outcome condition as an {@link OutcomeConflictError}, which outranks every
 *   stream and fence failure but not the control checks, and never as a conflict;
 * - fail a commit that carries an outcome, and whose result is genuinely unknown (a timeout, a
 *   lost connection or response, a server fault, an abort while the write was in flight, a
 *   connection lost during `COMMIT`), with an {@link IndeterminateError}, and keep every
 *   definitive refusal as it is;
 * - read the row with a strongly consistent read, whatever its read-consistency setting, and
 *   refuse a malformed one with `invalidRecord`.
 */
export interface OutcomeRepository extends TransactionalRepository {
  /**
   * The stored outcome of a command, or null when none is recorded. Null is never proof the
   * command didn't apply: a commit may still land. The command ID is checked as a write checks it.
   */
  outcome(commandId: string, options?: CallOptions): Promise<Outcome | null>
}

/** A transaction as the backends write it. */
export interface NormalizedTransaction {
  /** The streams it writes events to, sorted by entity ID bytes, without their projections. */
  readonly streams: readonly Commit[]
  /** Its fences on the streams it doesn't write, sorted the same way. */
  readonly fences: readonly ReadFence[]
  /** The written streams' projector groups, in the order the streams were declared. */
  readonly projections: readonly ProjectorGroup[]
  /** The outcome to record, with the event IDs the transaction writes, or null. */
  readonly outcome: Outcome | null
}

/**
 * Validates a transaction and puts it in the order every backend writes it: the written streams,
 * then the fences, each sorted by entity ID bytes, the order that pins a DynamoDB transaction and
 * PostgreSQL's locks. The projector groups keep the order the streams were declared in. Streams
 * without events are dropped, and so are the fences on streams that write events, whose head
 * updates condition on the head they read. A fence on a stream listed without events stays.
 */
export function normalizeTransaction(transaction: TransactionCommit): NormalizedTransaction {
  const listed = new Set<string>()
  for (const stream of transaction.streams) {
    validateCommit(stream)
    checkNotReserved(stream.entityId)
    if (listed.has(stream.entityId)) {
      throw invalidCommit(`duplicate stream ${stream.entityId}`)
    }
    listed.add(stream.entityId)
  }

  const fenced = new Set<string>()
  for (const fence of transaction.fences ?? []) {
    checkNotReserved(fence.entityId)
    if (fence.head < 0n || fence.head >= MAX_INT64 || fenced.has(fence.entityId)) {
      throw invalidCommit(`invalid or duplicate fence ${fence.entityId}`)
    }
    fenced.add(fence.entityId)
  }

  for (const stream of transaction.streams) {
    if (stream.snapshot && stream.events.length === 0) {
      throw invalidCommit(`the snapshot of ${stream.entityId} must cover this commit's last event`)
    }
  }

  const written = transaction.streams.filter((stream) => stream.events.length > 0)
  const projections = written.flatMap((stream) => stream.projections ?? [])
  const streams = written.map((stream) => Object.assign({}, stream, {projections: undefined}))
  streams.sort(byId)
  const writes = new Set(streams.map((stream) => stream.entityId))
  const fences = (transaction.fences ?? []).filter((fence) => !writes.has(fence.entityId))
  fences.sort(byId)

  const outcome = transaction.outcome ?? null
  if (outcome === null) {
    return {streams, fences, projections, outcome}
  }

  // A commit that records an outcome stores every string it names on every backend alike, so
  // every stream and fence it lists is checked, written or not.
  checkOutcome(outcome)
  const listedFences = transaction.fences ?? []
  checkCommitText(
    transaction.streams.flatMap((stream) => stream.events),
    [
      ...transaction.streams.flatMap((stream) =>
        stream.snapshot ? [stream.snapshot.entityType] : [],
      ),
      ...listedFences.map((fence) => fence.entityType),
    ],
    [...transaction.streams, ...listedFences].map((stream) => stream.entityId),
  )
  const events = streams.flatMap((stream) => stream.events)

  return {
    streams,
    fences,
    projections,
    outcome: storedOutcome(
      outcome,
      events.map((event) => event.id),
    ),
  }
}

/** Whether a normalized transaction writes nothing at all: no stream, no fence and no outcome. */
export function isEmptyTransaction(transaction: NormalizedTransaction): boolean {
  return (
    transaction.streams.length === 0 &&
    transaction.fences.length === 0 &&
    transaction.outcome === null
  )
}

/**
 * The transaction a single-stream commit writes. Its snapshot is dropped when it has no events,
 * since a commit with no events ignores it.
 */
export function commitAsTransaction(commit: Commit): NormalizedTransaction {
  validateCommit(commit)
  checkNotReserved(commit.entityId)

  const normalized = normalizeTransaction({
    streams: commit.events.length > 0 ? [commit] : [],
    outcome: commit.outcome,
  })
  // An outcome-only commit still names its stream, which is checked as a written one's is.
  if (normalized.outcome !== null && commit.events.length === 0) {
    checkCommitText([], commit.snapshot ? [commit.snapshot.entityType] : [], [commit.entityId])
  }

  return normalized
}

/**
 * Refuses a transaction that fences a retained entity type, by the store's manifest or the
 * repository's own configuration.
 */
export function checkFenceRetention(
  transaction: NormalizedTransaction,
  retained: (entityType: string) => boolean,
): void {
  for (const fence of transaction.fences) {
    if (retained(fence.entityType)) {
      throw new RetentionConflictError(fence.entityType)
    }
  }
}

/**
 * The error a failed transaction reports from the failures of its streams and fences, given in
 * transaction order: the first drift, else the first gap, else the first of the rest.
 */
export function rankFailures(failures: readonly EvtError[]): EvtError | undefined {
  return (
    failures.find((failure) => failure instanceof StreamHeadDriftError) ??
    failures.find((failure) => failure instanceof SequenceGapError) ??
    failures[0]
  )
}

/** The most actions one commit may carry, DynamoDB's transaction limit. */
export const MAX_COMMIT_ACTIONS = 100

/** The most bytes one commit may carry, summed over its actions: 95% of DynamoDB's 4 MB. */
export const MAX_COMMIT_BYTES = 3_800_000

/** The prefixes of the entity IDs only the engine writes. */
export const RESERVED_PREFIXES: readonly string[] = Object.freeze([
  'SYSTEM#',
  'command-outcome#',
])

/** Whether an entity ID is in a reserved namespace. */
export function isReserved(entityId: string): boolean {
  return RESERVED_PREFIXES.some((prefix) => entityId.startsWith(prefix))
}

/** Refuses an entity ID in a reserved namespace, on a write path, before any request. */
export function checkNotReserved(entityId: string): void {
  if (isReserved(entityId)) {
    throw new ReservedEntityIdError(entityId)
  }
}

/**
 * Checks that a commit's events can be stored as written: each belongs to the commit's entity and
 * has the ID `{entityId}:{sequence}`, they share one entity type, the first sequence is at least
 * 1, and the sequences are contiguous. A snapshot must have the events' entity type and a
 * sequence of at least 1, and each projector group a size per operation. A commit with no events
 * is valid, and its snapshot isn't checked, but its projector operations are refused.
 */
export function validateCommit(commit: Commit): void {
  let operations = 0
  for (const group of commit.projections ?? []) {
    if (group.sizes.length !== group.operations.length) {
      throw invalidCommit(
        `a ${group.backend} projector group has ${group.operations.length} operations but ` +
          `${group.sizes.length} sizes`,
      )
    }
    operations += group.operations.length
  }

  const [first] = commit.events
  if (first === undefined) {
    if (operations > 0) {
      throw new ProjectorOnlyCommitError(operations)
    }

    return
  }

  for (const event of commit.events) {
    if (event.entityType !== first.entityType) {
      throw invalidCommit(
        `event ${event.id} has entity type ${event.entityType}, but the commit's first event ` +
          `has ${first.entityType}`,
      )
    }
  }

  const snapshot = commit.snapshot
  if (snapshot && snapshot.entityType !== first.entityType) {
    throw invalidCommit(
      `the snapshot has entity type ${snapshot.entityType}, but the events have ` +
        first.entityType,
    )
  }
  if (snapshot && snapshot.sequence < 1n) {
    throw invalidCommit(
      `snapshot sequence ${snapshot.sequence} is below 1; snapshot sequences start at 1`,
    )
  }

  for (const event of commit.events) {
    if (event.entityId !== commit.entityId) {
      throw invalidCommit(
        `event ${event.id} belongs to entity ${event.entityId}, not ${commit.entityId}`,
      )
    }

    const id = eventId(event.entityId, event.sequence)
    if (event.id !== id) {
      throw invalidCommit(`event ${event.id} should have the ID ${id}`)
    }
  }

  if (first.sequence < 1n) {
    throw invalidCommit(`event sequence ${first.sequence} is below 1; sequences start at 1`)
  }

  for (let i = 1; i < commit.events.length; i++) {
    const previous = commit.events[i - 1]!.sequence
    const next = commit.events[i]!.sequence
    if (next !== previous + 1n) {
      throw invalidCommit(
        `event sequence ${next} follows ${previous}; sequences must be contiguous`,
      )
    }
  }
}

/**
 * The size of each action a commit's transaction carries, in order: the feature-marker and
 * rebuild-lock checks, the projector operations, then the stream's head update, its event puts,
 * and its snapshot put when it writes one, without an outcome. `ttl` gives the expiry an entity
 * type's items carry, if it's retained. The backends budget through {@link transactionSizes},
 * which also counts a commit's outcome put.
 */
export function commitSizes(
  commit: Commit,
  ttl: (entityType: string) => number | undefined,
): number[] {
  const transaction = {
    streams: [commit],
    fences: [],
    projections: commit.projections ?? [],
    outcome: null,
  }

  return transactionSizes(transaction, ttl)
}

/**
 * The size of each action a transaction carries: the two control checks, every stream's projector
 * operations, every written stream's head update, event puts and snapshot put, a check on every
 * fenced stream's head, then the outcome put. Every backend budgets a transaction by these sizes.
 */
export function transactionSizes(
  transaction: NormalizedTransaction,
  ttl: (entityType: string) => number | undefined,
): number[] {
  // The control rows every commit checks: the feature marker and the rebuild lock.
  const sizes = [controlCheckSize(FEATURES_PK), controlCheckSize(REBUILD_LOCK_PK)]
  for (const group of transaction.projections) {
    sizes.push(...group.sizes)
  }
  for (const stream of transaction.streams) {
    sizes.push(...streamSizes(stream, ttl))
  }
  for (const fence of transaction.fences) {
    sizes.push(headActionSize(headFence(fence.entityId, fence.head)))
  }
  if (transaction.outcome) {
    sizes.push(outcomeItemSize(transaction.outcome))
  }

  return sizes
}

// A written stream's actions: its head update, its event puts and its snapshot put.
function streamSizes(
  commit: Commit,
  ttl: (entityType: string) => number | undefined,
): number[] {
  const sizes: number[] = []
  const first = commit.events[0]
  const last = commit.events.at(-1)
  if (first === undefined || last === undefined) {
    return sizes
  }

  const head = headUpdate(commit.entityId, first.sequence, last.sequence, ttl(first.entityType))
  sizes.push(headActionSize(head))
  for (const event of commit.events) {
    sizes.push(itemSize(eventItem(event, ttl(event.entityType))))
  }

  const snapshot = commit.snapshot
  if (snapshot) {
    const stored: SerializedSnapshot = {
      entityType: snapshot.entityType,
      entityId: commit.entityId,
      sequence: snapshot.sequence,
      eventSequence: last.sequence,
      payload: snapshot.payload,
    }
    sizes.push(itemSize(snapshotItem(stored, ttl(snapshot.entityType))))
  }

  return sizes
}

/**
 * Refuses a commit whose actions are over the budget: more than {@link MAX_COMMIT_ACTIONS}, more
 * than {@link MAX_COMMIT_BYTES} in all, or one over {@link MAX_COMMIT_ITEM_BYTES}.
 */
export function checkBudget(sizes: readonly number[]): void {
  let bytes = 0
  let largestItem = 0
  for (const size of sizes) {
    bytes += size
    largestItem = Math.max(largestItem, size)
  }

  const tooLarge =
    sizes.length > MAX_COMMIT_ACTIONS ||
    bytes > MAX_COMMIT_BYTES ||
    largestItem > MAX_COMMIT_ITEM_BYTES
  if (tooLarge) {
    throw new CommitTooLargeError(sizes.length, bytes, largestItem)
  }
}

// Orders streams and fences by their entity IDs' UTF-8 bytes.
function byId(a: {entityId: string}, b: {entityId: string}): number {
  return compareUtf8(a.entityId, b.entityId)
}

// The largest int64, whose successor no head can reach.
const MAX_INT64 = 2n ** 63n - 1n

// A control check is sized by its key: `pk` and `sk = 0`.
function controlCheckSize(pk: string): number {
  return itemSize({pk: {S: pk}, sk: {N: '0'}})
}

function invalidCommit(message: string): EvtError {
  return new EvtError('invalidCommit', `invalid commit: ${message}`)
}
