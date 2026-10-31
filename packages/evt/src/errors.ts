// Every failure evt throws is an EvtError with a code, as the Rust crate returns its Error enum.
// Domain outcomes (a rejected command, a duplicate) are values, never errors. A failure with
// details has its own subclass, so `instanceof` narrows to its fields.

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
 * - `streamHeadDrift`: a stream's head disagrees with its events or snapshot. Never retried.
 * - `commitTooLarge`: the commit is over the transaction budget. Nothing was written.
 * - `compactionUncovered`: no snapshot covers the events compaction was asked to delete.
 * - `reservedEntityId`: a write to an entity ID in a namespace only the engine writes.
 */
export type EvtErrorCode =
  | 'invalidJson'
  | 'invalidRecord'
  | 'invalidArgument'
  | 'invalidCommit'
  | 'conflict'
  | 'sequenceGap'
  | 'streamHeadDrift'
  | 'commitTooLarge'
  | 'compactionUncovered'
  | 'reservedEntityId'

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

/** Whether an error is an optimistic-concurrency conflict: a `conflict` or a `sequenceGap`. */
export function isConflict(error: unknown): boolean {
  return error instanceof EvtError && (error.code === 'conflict' || error.code === 'sequenceGap')
}

/** Whether reloading and running the command again may succeed. So far, only after a conflict. */
export function isRetryable(error: unknown): boolean {
  return isConflict(error)
}
