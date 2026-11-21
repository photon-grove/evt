// The journal's configuration: the entity types it covers, how an event maps to scopes, the
// row-size cap and the contention policy. Allocation is a pure function of the counters it's given.

import {
  type Aggregate,
  type EventDefinitions,
  type EventOf,
  type Rejection,
  encodeEventData,
} from '../aggregate.js'
import {EvtError} from '../errors.js'
import {type SerializedEvent, eventId} from '../event.js'
import type {Metadata} from '../metadata.js'
import {DEFAULT_RETRY_POLICY, type RetryPolicy, checkRetryPolicy} from '../retry.js'
import {copyMetadata} from '../staging.js'
import {compareUtf8} from '../utf8.js'
import {DEFAULT_MAX_ROW_BYTES, MAX_ROW_BYTES, rowSize} from './row.js'
import {
  ALLOCATION_KEY,
  type Allocation,
  MAX_POSITION,
  checkScope,
  invalidJournal,
  setAllocations,
} from './scope.js'

/**
 * Maps an event to the scopes it's delivered through, such as a world scope and a place scope for
 * an event that happened in a place. It must be a pure function of the event (its type, payload
 * and metadata other than the journal's own allocation): no clock, no randomness, no lookup. Its
 * answer is written into the event's metadata, and nothing calls it for that event again, so
 * changing it changes only where future events go. Return none to leave an event out. Duplicates
 * collapse. A thrown error fails the commit with `invalidJournal` before anything is written.
 */
export type ScopeResolver = (event: SerializedEvent) => readonly string[]

/** The journal's tunables. */
export interface JournalOptions {
  /**
   * The row-size cap in bytes of a row's payload, {@link DEFAULT_MAX_ROW_BYTES} by default and at
   * most {@link MAX_ROW_BYTES}. A cap of 0 refuses every row.
   */
  readonly maxRowBytes?: number | undefined
  /**
   * How much wider than the row it computes {@link Journal.preCheck} measures, as a margin for
   * what a handler can't see of the commit's metadata. The commit-time guard ignores it. 0 by
   * default.
   */
  readonly preCheckHeadroom?: number | undefined
  /**
   * How many times a journaled commit is attempted when another writer takes its positions, and
   * how long it waits between attempts. {@link DEFAULT_RETRY_POLICY} by default.
   */
  readonly retry?: RetryPolicy | undefined
}

/**
 * A journal row over the configured size cap: which row, how large, and the cap. The pre-check and
 * the commit-time guard both throw it before any write, naming the widest row.
 */
export class JournalRowTooLargeError extends EvtError {
  override readonly name = 'JournalRowTooLargeError'
  override readonly code = 'journalRowTooLarge'
  readonly scope: string
  /** The row's position; for a pre-check, which allocates nothing, the widest one (a u64's max). */
  readonly position: bigint
  readonly eventId: string
  /** The row's size in bytes. */
  readonly size: number
  /** The configured cap in bytes. */
  readonly limit: number

  constructor(scope: string, position: bigint, event: string, size: number, limit: number) {
    super(
      'journalRowTooLarge',
      `the journal row for event ${event} in scope ${JSON.stringify(scope)} is ${size} bytes, ` +
        `over the ${limit} byte cap`,
    )
    this.scope = scope
    this.position = position
    this.eventId = event
    this.size = size
    this.limit = limit
  }

  /**
   * The rejection an application records for the refused command: code `journal-row-too-large`,
   * with the scope, event, size and cap in its details.
   */
  toRejection(): Rejection {
    return {
      code: 'journal-row-too-large',
      message: this.message,
      details: {scope: this.scope, eventId: this.eventId, size: this.size, limit: this.limit},
    }
  }
}

/**
 * The journal's configuration, shared by the repository that writes it and the projector that
 * derives its rows. Its entity types are its history: its rows are rebuilt from their events, so
 * the first journaled commit declares them history-dependent, and compaction refuses them.
 */
export class Journal {
  /** The journaled entity types, sorted by bytes. */
  readonly entityTypes: readonly string[]
  readonly maxRowBytes: number
  readonly preCheckHeadroom: number
  readonly retry: RetryPolicy
  readonly #resolver: ScopeResolver

  constructor(
    entityTypes: readonly string[],
    resolver: ScopeResolver,
    options: JournalOptions = {},
  ) {
    const types = [...new Set(entityTypes)]
    types.sort(compareUtf8)
    if (types.length === 0) {
      throw invalidJournal('a journal covers at least one entity type')
    }
    if (typeof resolver !== 'function') {
      throw invalidJournal('a journal needs a scope resolver')
    }

    const maxRowBytes = options.maxRowBytes ?? DEFAULT_MAX_ROW_BYTES
    if (!Number.isSafeInteger(maxRowBytes) || maxRowBytes < 0 || maxRowBytes > MAX_ROW_BYTES) {
      throw invalidJournal(`a row cap is 0 to ${MAX_ROW_BYTES} bytes; got ${maxRowBytes}`)
    }
    const headroom = options.preCheckHeadroom ?? 0
    if (!Number.isSafeInteger(headroom) || headroom < 0) {
      throw invalidJournal(`a pre-check headroom is a whole number of bytes; got ${headroom}`)
    }
    const retry = options.retry ?? DEFAULT_RETRY_POLICY
    checkRetryPolicy(retry)

    this.entityTypes = Object.freeze(types)
    this.maxRowBytes = maxRowBytes
    this.preCheckHeadroom = headroom
    this.retry = retry
    this.#resolver = resolver
  }

  /** Whether the journal covers events of this entity type. */
  covers(entityType: string): boolean {
    return this.entityTypes.includes(entityType)
  }

  /**
   * The scopes an event is delivered through, ascending by bytes and without repeats; none for an
   * event of an uncovered type. The resolver sees the event without a journal allocation, so its
   * answer can't depend on one.
   */
  scopesOf(event: SerializedEvent): string[] {
    if (!this.covers(event.entityType)) {
      return []
    }

    let bare = event
    if (event.metadata.extensions.has(ALLOCATION_KEY)) {
      bare = {...event}
      setAllocations(bare, [])
    }
    let scopes: readonly string[]
    try {
      scopes = this.#resolver(bare)
      if (!Array.isArray(scopes)) {
        throw new TypeError('the resolver returned no array of scopes')
      }
    } catch (error) {
      throw invalidJournal(
        `the scope resolver failed for ${event.id}: ${error instanceof Error ? error.message : error}`,
      )
    }

    const unique = [...new Set(scopes.map(checkScope))]
    unique.sort(compareUtf8)

    return unique
  }

  /** The widest row over the cap among an event's allocations, or null. */
  widestRow(
    event: SerializedEvent,
    list: readonly Allocation[],
    headroom = 0,
  ): JournalRowTooLargeError | null {
    let widest: JournalRowTooLargeError | null = null
    for (const {scope, position} of list) {
      const size = rowSize(scope, position, event) + headroom
      if (size > this.maxRowBytes && (widest === null || size > widest.size)) {
        widest = new JournalRowTooLargeError(scope, position, event.id, size, this.maxRowBytes)
      }
    }

    return widest
  }

  /**
   * Checks events against the row-size cap as the commit will guard them, without reading a
   * counter: each row is measured at the widest position (a u64's max, 20 digits). For events
   * exactly as the commit will store them, a pre-check that passes means the guard passes. It's
   * advisory otherwise: a handler can't see the commit's metadata, so pass the same metadata, or
   * widen the measure with `preCheckHeadroom`. Events of uncovered types are skipped. It throws
   * the widest refused row of the first event that has one, a {@link JournalRowTooLargeError}:
   * call it in a command handler and reject with its `toRejection()`.
   */
  preCheck(events: readonly SerializedEvent[]): void {
    for (const event of events) {
      const scopes = this.scopesOf(event)
      if (scopes.length === 0) {
        continue
      }

      const widest = scopes.map((scope) => ({scope, position: MAX_POSITION}))
      const measured = {...event}
      setAllocations(measured, widest)
      const refused = this.widestRow(measured, widest, this.preCheckHeadroom)
      if (refused !== null) {
        throw refused
      }
    }
  }

  /**
   * {@link preCheck} for the events a handler just decided: serialized as stream `entityId` would
   * store them after `sequence` events, with `metadata` stamped as the commit stamps it (the
   * command ID, else the caller's commit ID, else a generated 26-character ID).
   */
  preCheckEvents<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    entityId: string,
    sequence: bigint,
    events: readonly EventOf<D>[],
    metadata: Metadata,
  ): void {
    const stamped = {...metadata}
    if (stamped.commandId !== null && stamped.commandId !== '') {
      stamped.commitId = stamped.commandId
    } else if (stamped.commitId === '') {
      stamped.commitId = '0'.repeat(26)
    }
    // Serialized as the store serializes them, without applying them to any state.
    const serialized = events.map((event, index): SerializedEvent => {
      const at = sequence + BigInt(index + 1)
      const {version, payload} = encodeEventData(aggregate.events, event)

      return {
        id: eventId(entityId, at),
        sequence: at,
        type: event.type,
        version,
        entityId,
        entityType: aggregate.type,
        payload,
        metadata: copyMetadata(stamped),
      }
    })

    this.preCheck(serialized)
  }
}

/** The result of allocating a commit: each event's allocations, and the counters after it. */
export interface Allocated {
  /** Each event's allocations, ascending by scope, in the order the events were given. */
  readonly allocations: Allocation[][]
  /** The highest position now allocated in each scope the commit touched, in first-use order. */
  readonly counters: Map<string, bigint>
}

/**
 * Allocates positions: each event, in order, takes the next position of every scope in its plan,
 * continuing past the given `counters` (a scope with none starts at 1). Positions are contiguous in each
 * scope, and a scope's counter after the commit is the last position taken.
 */
export function allocate(
  counters: ReadonlyMap<string, bigint>,
  plan: readonly (readonly string[])[],
): Allocated {
  const next = new Map<string, bigint>()
  const allocations = plan.map((scopes) =>
    scopes.map((scope) => {
      const current = next.get(scope) ?? counters.get(scope) ?? 0n
      if (current >= MAX_POSITION) {
        throw invalidJournal(`scope ${scope} is out of positions`)
      }
      const position = current + 1n
      next.set(scope, position)

      return {scope, position}
    }),
  )

  return {allocations, counters: next}
}
