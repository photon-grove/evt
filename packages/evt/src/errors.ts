// Every failure evt throws is an EvtError with a code, as the Rust crate returns its Error enum.
// Domain outcomes (a rejected command, a duplicate) are values, never errors. A failure with
// details has its own subclass, so `instanceof` narrows to its fields.

import type {Outcome} from './outcome.js'

/**
 * What went wrong:
 *
 * - `invalidJson`: text that isn't JSON.
 * - `invalidRecord`: a stored record (an event, a snapshot, an item) with the wrong shape.
 * - `invalidArgument`: a value evt can't write, such as a float that isn't finite or an object
 *   whose key order the engine has already changed, or a read with a bad argument, such as
 *   `latest(0)`.
 * - `invalidCommit`: a commit built wrong, such as one with another entity's event. Nothing was
 *   written.
 * - `conflict`: optimistic concurrency failed. Another writer committed since the stream was
 *   read. Reload and run the command again.
 * - `sequenceGap`: a commit or a load skips a sequence. A conflict.
 * - `fenceConflict`: another writer moved a stream the transaction read but didn't write. A
 *   conflict, retryable when the engine observed the fenced head.
 * - `retentionConflict`: a transaction fenced a stream whose entity type has retention, which a
 *   fence can't protect. Nothing was written.
 * - `streamHeadDrift`: a stream's head disagrees with its events or snapshot. Never retried.
 * - `commitTooLarge`: the commit is over the transaction budget. Nothing was written.
 * - `compactionUncovered`: no snapshot covers the events compaction was asked to delete.
 * - `reservedEntityId`: a write to an entity ID in a namespace only the engine writes.
 * - `unsupportedFeature`: the store declares a feature this package doesn't model. Nothing was
 *   written.
 * - `streamHeadsNotMigrated`: the store holds data but doesn't declare stream-heads. Nothing was
 *   written.
 * - `storeNotInitialized`: the store doesn't declare stream-heads, and the writer doesn't
 *   initialize a store itself. Nothing was written.
 * - `featureManifestChanged`: another writer changed the store's feature manifest since this one
 *   validated it. Retryable: the writer validates it again.
 * - `transient`: the backend throttled the write. Retryable. Nothing was written.
 * - `rebuildInProgress`: a projection rebuild holds the store, so commits wait. Retryable.
 * - `historyDependentProjection`: compaction was refused for a history-dependent entity type.
 * - `projectorOnlyCommit`: a commit carries projector operations but no events. Nothing was
 *   written.
 * - `replay`: a stored event failed to upcast or decode while an aggregate loaded.
 * - `snapshot`: a stored snapshot failed to decode while an aggregate loaded.
 * - `outcomeConflict`: a commit's command outcome lost its condition: another writer already
 *   recorded an outcome for the command ID. Nothing was written.
 * - `outcomeTooLarge`: a command outcome's response, rejection or recorded identity is over
 *   65536 bytes. Nothing was written.
 * - `commandIdTooLong`: a command ID is over 512 UTF-8 bytes. Nothing was written.
 * - `indeterminate`: a commit that carries a command outcome failed in a way that leaves it
 *   unknown whether it applied, such as a timeout or a connection lost during `COMMIT`.
 * - `idempotencyKeyReuse`: the command ID already has an outcome, recorded for a command with
 *   another identity (another type, target, payload or principal). Nothing was written.
 * - `ambiguousWrite`: a command's commit failed indeterminately, and its outcome row didn't
 *   appear within the recovery window. It may still land. Retryable with the same command ID.
 * - `outcomePrincipalMismatch`: a lookup by command ID found an outcome recorded for another
 *   principal, or one that records no identity to match.
 * - `duplicateCommand`: an event written before outcomes existed already carries the command ID,
 *   so the command ran, but no outcome row says how. Nothing was written.
 */
export type EvtErrorCode =
  | 'invalidJson'
  | 'invalidRecord'
  | 'invalidArgument'
  | 'invalidCommit'
  | 'conflict'
  | 'sequenceGap'
  | 'fenceConflict'
  | 'retentionConflict'
  | 'streamHeadDrift'
  | 'commitTooLarge'
  | 'compactionUncovered'
  | 'reservedEntityId'
  | 'unsupportedFeature'
  | 'streamHeadsNotMigrated'
  | 'storeNotInitialized'
  | 'featureManifestChanged'
  | 'transient'
  | 'rebuildInProgress'
  | 'historyDependentProjection'
  | 'projectorOnlyCommit'
  | 'replay'
  | 'snapshot'
  | 'outcomeConflict'
  | 'outcomeTooLarge'
  | 'commandIdTooLong'
  | 'indeterminate'
  | 'idempotencyKeyReuse'
  | 'ambiguousWrite'
  | 'outcomePrincipalMismatch'
  | 'duplicateCommand'

export class EvtError extends Error {
  override readonly name: string = 'EvtError'
  readonly code: EvtErrorCode

  constructor(code: EvtErrorCode, message: string, options?: {cause?: unknown}) {
    super(message, options)
    this.code = code
  }
}

/** Optimistic concurrency failed on a written stream: its sequence or snapshot moved. */
export class ConflictError extends EvtError {
  override readonly name = 'ConflictError'
  override readonly code = 'conflict'
  readonly entityId: string

  constructor(entityId: string, detail: string) {
    super('conflict', `conflict on entity ${entityId}: ${detail}`)
    this.entityId = entityId
  }
}

/** A commit's first event skips past the stream's head, or a load found a hole. A conflict. */
export class SequenceGapError extends EvtError {
  override readonly name = 'SequenceGapError'
  override readonly code = 'sequenceGap'
  readonly entityId: string
  /** The last sequence before the gap. */
  readonly after: bigint
  /** The sequence found next, which isn't `after + 1`. */
  readonly found: bigint

  constructor(entityId: string, after: bigint, found: bigint) {
    super(
      'sequenceGap',
      `conflict on entity ${entityId}: the stream skips from sequence ${after} to ${found}: it ` +
        'was compacted during the load, or events expired',
    )
    this.entityId = entityId
    this.after = after
    this.found = found
  }
}

/**
 * A stream's head disagrees with its events or snapshot (ADR 0007): a writer appended without
 * raising the head, events were deleted, a snapshot is ahead of the head, or a stream has data and
 * no head. It isn't retryable: running the command again fails the same way. Repair is offline.
 */
export class StreamHeadDriftError extends EvtError {
  override readonly name = 'StreamHeadDriftError'
  override readonly code = 'streamHeadDrift'
  readonly entityId: string
  /** The stored head, or null when the stream has no head. */
  readonly stored: bigint | null
  /** The head its events and snapshot imply: the higher of the two. */
  readonly derived: bigint

  constructor(entityId: string, stored: bigint | null, derived: bigint) {
    super(
      'streamHeadDrift',
      `stream head drift on entity ${entityId}: stored head ${stored ?? 'absent'}, ` +
        `derived head ${derived}`,
    )
    this.entityId = entityId
    this.stored = stored
    this.derived = derived
  }
}

/** The commit is over the transaction budget: too many actions, bytes, or bytes in one item. */
export class CommitTooLargeError extends EvtError {
  override readonly name = 'CommitTooLargeError'
  override readonly code = 'commitTooLarge'
  readonly actions: number
  readonly bytes: number
  readonly largestItem: number

  constructor(actions: number, bytes: number, largestItem: number) {
    super(
      'commitTooLarge',
      `commit exceeds transaction budget: ${actions} actions, ${bytes} bytes, ` +
        `largest item ${largestItem}`,
    )
    this.actions = actions
    this.bytes = bytes
    this.largestItem = largestItem
  }
}

/** Compaction was refused because no snapshot covers the events it would delete. */
export class CompactionUncoveredError extends EvtError {
  override readonly name = 'CompactionUncoveredError'
  override readonly code = 'compactionUncovered'
  readonly entityId: string
  readonly through: bigint
  /** The event sequence the stored snapshot covers, or null with no snapshot. */
  readonly covered: bigint | null

  constructor(entityId: string, through: bigint, covered: bigint | null) {
    super(
      'compactionUncovered',
      'compaction refused: no durable snapshot covers the requested range ' +
        `(entity ${entityId}, through ${through}, snapshot covers ${covered ?? 'nothing'})`,
    )
    this.entityId = entityId
    this.through = through
    this.covered = covered
  }
}

/** A write to an entity ID in a reserved namespace (`SYSTEM#`, `command-outcome#`). */
export class ReservedEntityIdError extends EvtError {
  override readonly name = 'ReservedEntityIdError'
  override readonly code = 'reservedEntityId'
  readonly entityId: string

  constructor(entityId: string) {
    super(
      'reservedEntityId',
      `entity ID ${entityId} is in a reserved namespace (SYSTEM#, command-outcome#)`,
    )
    this.entityId = entityId
  }
}

/** A feature and one of its stored-format versions. */
export interface FeaturePair {
  readonly name: string
  readonly version: number
}

/**
 * The store's feature manifest declares pairs this package doesn't model, so it refuses to write:
 * a commit, a snapshot write, compaction or initialization. Nothing was written.
 */
export class UnsupportedFeatureError extends EvtError {
  override readonly name = 'UnsupportedFeatureError'
  override readonly code = 'unsupportedFeature'
  /** Every unsupported pair, sorted by name. */
  readonly unsupported: readonly FeaturePair[]

  constructor(unsupported: readonly FeaturePair[]) {
    const pairs = unsupported.map((pair) => `${pair.name}@${pair.version}`).join(', ')
    super('unsupportedFeature', `store declares unsupported features (${pairs}); refusing to write`)
    this.unsupported = unsupported
  }
}

/**
 * The store holds data but its feature manifest doesn't declare stream-heads: its streams may have
 * no head items, so a head-aware write could fail every stream. Migrate the store with every older
 * writer stopped. Nothing was written.
 */
export class StreamHeadsNotMigratedError extends EvtError {
  override readonly name = 'StreamHeadsNotMigratedError'
  override readonly code = 'streamHeadsNotMigrated'

  constructor() {
    super(
      'streamHeadsNotMigrated',
      'store does not declare the stream-heads feature and holds data without per-stream head ' +
        'items; migrate it before writing',
    )
  }
}

/**
 * The store's feature manifest doesn't declare stream-heads, and this writer doesn't initialize a
 * store itself (the DynamoDB default), so it refuses rather than scan the table to learn whether
 * it's fresh. Initialize a fresh store once, at deploy time, or migrate one written before stream
 * heads. Nothing was written.
 */
export class StoreNotInitializedError extends EvtError {
  override readonly name = 'StoreNotInitializedError'
  override readonly code = 'storeNotInitialized'

  constructor() {
    super(
      'storeNotInitialized',
      'store is not initialized: the feature manifest does not declare stream-heads; initialize ' +
        'a fresh store, or migrate one written before stream heads, before writing',
    )
  }
}

/**
 * Another writer changed the store's feature manifest after this one validated it, so the commit's
 * marker check failed. The writer has validated the new manifest, which it supports. Retryable.
 * Nothing was written.
 */
export class FeatureManifestChangedError extends EvtError {
  override readonly name = 'FeatureManifestChangedError'
  override readonly code = 'featureManifestChanged'

  constructor(options?: {cause?: unknown}) {
    super('featureManifestChanged', 'feature manifest changed under this writer', options)
  }
}

/**
 * The backend refused the write for now, such as a DynamoDB transaction cancelled for throttling.
 * Retryable, after a backoff. Nothing was written. The backend's error is its cause.
 */
export class TransientError extends EvtError {
  override readonly name = 'TransientError'
  override readonly code = 'transient'
  /**
   * Another transaction was writing one of the commit's items at the same time (DynamoDB's
   * `TransactionConflict`), so the winner may be another attempt of the same command.
   */
  readonly transactionConflict: boolean

  constructor(detail: string, options?: {cause?: unknown; transactionConflict?: boolean}) {
    super(
      'transient',
      `transient failure: ${detail}`,
      options && 'cause' in options ? {cause: options.cause} : undefined,
    )
    this.transactionConflict = options?.transactionConflict ?? false
  }
}

/**
 * A projection rebuild holds the store, or one was force-released and no rebuild has finished
 * since, so every commit is refused. Retryable: the command can run again once the rebuild ends.
 */
export class RebuildInProgressError extends EvtError {
  override readonly name = 'RebuildInProgressError'
  override readonly code = 'rebuildInProgress'

  constructor() {
    super('rebuildInProgress', 'a projection rebuild is in progress')
  }
}

/** Where a read fence's head came from (ADR 0004): the engine's own read, or the caller. */
export type FenceProvenance = 'engine' | 'caller'

/**
 * A read fence failed: another writer moved a stream the transaction read but didn't write past
 * the head it observed. A fence the engine filled is retryable, since loading again observes the
 * new head. A caller-stamped one isn't: the caller's input is stale. Nothing was written.
 */
export class FenceConflictError extends EvtError {
  override readonly name = 'FenceConflictError'
  override readonly code = 'fenceConflict'
  readonly entityId: string
  /** The head the transaction fenced the stream at. */
  readonly head: bigint
  readonly provenance: FenceProvenance

  constructor(entityId: string, head: bigint, provenance: FenceProvenance) {
    super('fenceConflict', `read fence failed for ${entityId} at head ${head} (${provenance})`)
    this.entityId = entityId
    this.head = head
    this.provenance = provenance
  }
}

/**
 * A transaction fenced a stream whose entity type has retention: TTL deletes items in no order, so
 * a fence on a retained stream protects nothing. Nothing was written.
 */
export class RetentionConflictError extends EvtError {
  override readonly name = 'RetentionConflictError'
  override readonly code = 'retentionConflict'
  readonly entityType: string

  constructor(entityType: string) {
    super(
      'retentionConflict',
      `read fences refused for retention-enabled entity type ${entityType}`,
    )
    this.entityType = entityType
  }
}

/**
 * Compaction was refused because the store declares the entity's type history-dependent: its
 * projectors' rebuild replays the events compaction would delete. Nothing was deleted.
 */
export class HistoryDependentProjectionError extends EvtError {
  override readonly name = 'HistoryDependentProjectionError'
  override readonly code = 'historyDependentProjection'
  readonly entityType: string
  readonly entityId: string

  constructor(entityType: string, entityId: string) {
    super(
      'historyDependentProjection',
      `entity type ${entityType} is declared history-dependent; compaction would delete the ` +
        `commits its rebuild replays (entity ${entityId})`,
    )
    this.entityType = entityType
    this.entityId = entityId
  }
}

/**
 * A commit carried projector operations but no events. Views are projections of committed events,
 * so operations with no event behind them are refused on every backend. Nothing was written.
 */
export class ProjectorOnlyCommitError extends EvtError {
  override readonly name = 'ProjectorOnlyCommitError'
  override readonly code = 'projectorOnlyCommit'
  /** How many projector operations the commit carried. */
  readonly operations: number

  constructor(operations: number) {
    super(
      'projectorOnlyCommit',
      `a commit carries ${operations} projector operations but no events; views need a ` +
        'backing event in the same commit',
    )
    this.operations = operations
  }
}

/** A load failed closed on a stored event: an upcaster threw, or the payload didn't decode. */
export class ReplayError extends EvtError {
  override readonly name = 'ReplayError'
  override readonly code = 'replay'
  readonly eventType: string
  /** The stored version, or the version an earlier upcaster produced. */
  readonly version: number
  readonly phase: 'upcast' | 'decode'

  constructor(
    eventType: string,
    version: number,
    phase: 'upcast' | 'decode',
    detail: string,
    cause?: unknown,
  ) {
    const doing = phase === 'upcast' ? 'upcasting' : 'decoding'
    super('replay', `replay failed while ${doing} ${eventType} v${version}: ${detail}`, {cause})
    this.eventType = eventType
    this.version = version
    this.phase = phase
  }
}

/** A load found a stored snapshot whose payload the aggregate's state codec doesn't decode. */
export class SnapshotError extends EvtError {
  override readonly name = 'SnapshotError'
  override readonly code = 'snapshot'
  readonly entityId: string

  constructor(entityId: string, cause: unknown) {
    super('snapshot', `decoding snapshot for entity ${entityId}: ${messageOf(cause)}`, {cause})
    this.entityId = entityId
  }
}

/**
 * A commit's outcome put lost its condition: another writer already recorded an outcome for the
 * command ID. It's never a generic conflict, and running the command again can't resolve it: read
 * the stored outcome instead. Nothing was written.
 */
export class OutcomeConflictError extends EvtError {
  override readonly name = 'OutcomeConflictError'
  override readonly code = 'outcomeConflict'
  readonly commandId: string

  constructor(commandId: string) {
    super('outcomeConflict', `an outcome is already recorded for command ${commandId}`)
    this.commandId = commandId
  }
}

/** Which part of a command outcome is over its bound. */
export type OutcomePart = 'response' | 'rejection' | 'identity'

/**
 * A command outcome's response, encoded rejection or recorded identity is over 65536 bytes.
 * Refused before any write, on every backend. Nothing was written.
 */
export class OutcomeTooLargeError extends EvtError {
  override readonly name = 'OutcomeTooLargeError'
  override readonly code = 'outcomeTooLarge'
  readonly what: OutcomePart
  readonly size: number

  constructor(what: OutcomePart, size: number) {
    super('outcomeTooLarge', `the outcome's ${what} is ${size} bytes, over the 65536-byte bound`)
    this.what = what
    this.size = size
  }
}

/** A command ID is over 512 UTF-8 bytes. Refused before any read or write. */
export class CommandIdTooLongError extends EvtError {
  override readonly name = 'CommandIdTooLongError'
  override readonly code = 'commandIdTooLong'
  readonly length: number

  constructor(length: number) {
    super('commandIdTooLong', `the command ID is ${length} bytes, over the 512-byte bound`)
    this.length = length
  }
}

/**
 * A commit that carries a command outcome failed in a way that leaves it unknown whether it
 * applied: a timeout, a lost connection or response, a server fault, an abort while the write was
 * in flight, or a PostgreSQL connection lost during `COMMIT`. The commit may still land, so read
 * the outcome row before deciding it didn't. A definitive refusal is never reported this way. The
 * backend's error is its cause.
 */
export class IndeterminateError extends EvtError {
  override readonly name = 'IndeterminateError'
  override readonly code = 'indeterminate'

  constructor(options?: {cause?: unknown}) {
    super('indeterminate', `commit result unknown: ${messageOf(options?.cause)}`, options)
  }
}

/**
 * The command ID already has an outcome, recorded for a command with another identity digest:
 * another type, target, payload or principal. `prior` is the stored outcome. Nothing was written.
 */
export class IdempotencyKeyReuseError extends EvtError {
  override readonly name = 'IdempotencyKeyReuseError'
  override readonly code = 'idempotencyKeyReuse'
  readonly commandId: string
  readonly prior: Outcome

  constructor(commandId: string, prior: Outcome) {
    super(
      'idempotencyKeyReuse',
      `command ${commandId} already has an outcome for a command with another identity`,
    )
    this.commandId = commandId
    this.prior = prior
  }
}

/**
 * A command's commit failed in a way that leaves it unknown whether it applied, and its outcome
 * row didn't appear within the recovery window, or couldn't be read. The commit may still land, so
 * this never means "not applied". Run the command again with the same command ID: its outcome
 * check answers it if the commit landed. The commit's failure is the `cause`.
 */
export class AmbiguousWriteError extends EvtError {
  override readonly name = 'AmbiguousWriteError'
  override readonly code = 'ambiguousWrite'
  readonly commandId: string

  constructor(commandId: string, options?: {cause?: unknown}) {
    super(
      'ambiguousWrite',
      `command ${commandId} may or may not have been applied: ${messageOf(options?.cause)}`,
      options,
    )
    this.commandId = commandId
  }
}

/**
 * A lookup by command ID alone found an outcome it can't answer for the asking principal:
 * `recorded` when the row belongs to another principal, not `recorded` when the row records no
 * identity to match. It never carries the stored outcome.
 */
export class OutcomePrincipalMismatchError extends EvtError {
  override readonly name = 'OutcomePrincipalMismatchError'
  override readonly code = 'outcomePrincipalMismatch'
  readonly commandId: string
  readonly recorded: boolean

  constructor(commandId: string, recorded: boolean) {
    super(
      'outcomePrincipalMismatch',
      recorded
        ? `the outcome of command ${commandId} belongs to another principal`
        : `the outcome of command ${commandId} records no principal to match`,
    )
    this.commandId = commandId
    this.recorded = recorded
  }
}

/**
 * An event written before outcomes existed already carries the command ID: the command ran, but
 * no outcome row records how, so the store can't replay it. Nothing was written.
 */
export class DuplicateCommandError extends EvtError {
  override readonly name = 'DuplicateCommandError'
  override readonly code = 'duplicateCommand'
  readonly commandId: string

  constructor(commandId: string) {
    super('duplicateCommand', `command ${commandId} already ran, and records no outcome`)
    this.commandId = commandId
  }
}

/**
 * Whether an error is an optimistic-concurrency conflict: a `conflict`, a `sequenceGap` or a
 * `fenceConflict`.
 */
export function isConflict(error: unknown): boolean {
  return (
    error instanceof EvtError &&
    (error.code === 'conflict' || error.code === 'sequenceGap' || error.code === 'fenceConflict')
  )
}

/**
 * Whether reloading and running the command again may succeed: after a conflict (a fence the
 * caller stamped excepted), while a rebuild holds the store, after the feature manifest changed
 * under the writer, after throttling, or after an ambiguous write, whose outcome check answers a
 * command that landed.
 */
export function isRetryable(error: unknown): boolean {
  // A caller-stamped fence stays stale however often the command runs again.
  if (error instanceof FenceConflictError) {
    return error.provenance === 'engine'
  }
  if (isConflict(error)) {
    return true
  }

  return (
    error instanceof EvtError &&
    (error.code === 'rebuildInProgress' ||
      error.code === 'featureManifestChanged' ||
      error.code === 'transient' ||
      error.code === 'ambiguousWrite')
  )
}

/** An error's message, or the value itself as text. */
export function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
