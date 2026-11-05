// The Store: loads aggregates, runs commands, and commits their events with the snapshots the
// snapshot policy calls for.

import {
  type Aggregate,
  type EventDefinitions,
  type EventOf,
  type Rejection,
  replayEvent,
} from './aggregate.js'
import {commitIds} from './commit-id.js'
import {EvtError, SequenceGapError, SnapshotError, isConflict, isRetryable} from './errors.js'
import type {SerializedEvent} from './event.js'
import {type Metadata, emptyMetadata} from './metadata.js'
import type {
  CallOptions,
  NewSnapshot,
  ProjectorGroup,
  Repository,
  TransactionalRepository,
} from './repository.js'
import {
  DEFAULT_RETRY_POLICY,
  type RetryPolicy,
  checkRetryPolicy,
  retryDelay,
  sleep,
} from './retry.js'
import {commitSnapshot, projectCommit, serializeEvents} from './staging.js'
import {Transaction} from './transaction.js'

/** The default number of events between snapshots, as in Go and Rust. */
export const DEFAULT_SNAPSHOT_INTERVAL = 5

// How many times a load reads the stream when it finds a sequence gap.
const LOAD_ATTEMPTS = 3

/** How a {@link Store} fills in what commands leave out. */
export interface StoreOptions {
  /** The region metadata gets when a command's metadata has none. */
  region?: string | undefined
  /** The clock, in Unix milliseconds, that stamps metadata timestamps. `Date.now` by default. */
  clock?: (() => number) | undefined
  /** The number of events between snapshots (default 5). 0 disables them. */
  snapshotInterval?: number | undefined
  /**
   * Makes the commit ID of a commit whose metadata has neither a command ID nor a commit ID. ULID
   * format by default.
   */
  commitId?: (() => string) | undefined
}

/** An aggregate loaded from its stream, with the position it was loaded at. */
export interface Loaded<S> {
  readonly entityId: string
  readonly state: S
  /** The sequence of the last event folded into the state, 0 for a new entity. */
  readonly sequence: bigint
  /** The sequence of the snapshot the load started from, or the commit wrote, 0 with none. */
  readonly snapshotSequence: bigint
  /** The command IDs of the events folded in since the snapshot: the dedupe window. */
  readonly seenCommandIds: ReadonlySet<string>
}

/** Options for {@link Store.execute}. */
export interface ExecuteOptions extends CallOptions {
  /**
   * The command's metadata. What it leaves out is empty, except the region and the timestamp,
   * which the store fills in.
   */
  metadata?: Partial<Metadata> | undefined
}

/** Options for {@link Store.executeWithRetry}. */
export interface RetryOptions extends ExecuteOptions {
  /** {@link DEFAULT_RETRY_POLICY} by default. */
  policy?: RetryPolicy | undefined
  /** Waits between attempts. A timer by default; inject one in tests. */
  sleep?: ((ms: number, signal?: AbortSignal) => Promise<void>) | undefined
}

/**
 * What a command did:
 *
 * - `committed`: it was accepted, and its events (possibly none) are stored.
 * - `duplicate`: an event since the latest snapshot already carries its command ID, so it didn't
 *   run.
 * - `rejected`: the handler refused it. Nothing was written.
 */
export type ExecuteResult<S> =
  | {kind: 'committed'; state: Loaded<S>; events: SerializedEvent[]}
  | {kind: 'duplicate'; commandId: string}
  | {kind: 'rejected'; rejection: Rejection; state: Loaded<S>}

export class Store {
  readonly repository: Repository
  readonly #region: string
  readonly #clock: () => number
  readonly #snapshotInterval: number
  readonly #commitId: () => string

  constructor(repository: Repository, options: StoreOptions = {}) {
    const interval = options.snapshotInterval ?? DEFAULT_SNAPSHOT_INTERVAL
    if (!Number.isSafeInteger(interval) || interval < 0) {
      throw new EvtError(
        'invalidArgument',
        `the snapshot interval must be a whole number of events: ${interval}`,
      )
    }

    this.repository = repository
    this.#region = options.region ?? ''
    this.#clock = options.clock ?? Date.now
    this.#snapshotInterval = interval
    this.#commitId = options.commitId ?? commitIds(this.#clock)
  }

  /** The number of events between snapshots of `aggregate`: its own interval, or the store's. */
  snapshotInterval(aggregate: {readonly snapshotInterval?: number | undefined}): number {
    return aggregate.snapshotInterval ?? this.#snapshotInterval
  }

  /**
   * Loads an aggregate from its snapshot and the events after it, or from every event when it has
   * no snapshot. An entity with no events loads as the initial state at sequence 0.
   *
   * The events must continue the sequence. A gap means another writer snapshotted and compacted
   * the stream between the two reads, so the load starts over, up to three times, and then fails
   * with a {@link SequenceGapError}. When no events follow, the snapshot is read again to rule out
   * a compaction that left no gap to see.
   */
  async load<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    entityId: string,
    options: CallOptions = {},
  ): Promise<Loaded<S>> {
    // Each attempt waits for the one before it.
    /* oxlint-disable no-await-in-loop */
    for (let attempt = 1; ; attempt++) {
      try {
        return await this.#loadOnce(aggregate, entityId, options)
      } catch (error) {
        if (!isConflict(error) || attempt >= LOAD_ATTEMPTS) {
          throw error
        }
      }
    }
    /* oxlint-enable no-await-in-loop */
  }

  /**
   * Loads the aggregate, runs the command, and commits its events, with a snapshot when the
   * commit reaches the snapshot interval, and the aggregate's projector operations. A snapshot
   * whose item is over the per-item commit budget is left out, so the commit still succeeds and
   * later loads replay from the one before, but a projector's own items are budgeted as they are:
   * a view row of a state that large fails the commit.
   *
   * A command ID already carried by an event folded in since the latest snapshot makes the
   * command a `duplicate`, and the handler doesn't run. A conflict with another writer throws;
   * {@link executeWithRetry} runs the command again.
   */
  async execute<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    entityId: string,
    command: C,
    options: ExecuteOptions = {},
  ): Promise<ExecuteResult<S>> {
    const metadata = this.#metadata(options.metadata)
    const loaded = await this.load(aggregate, entityId, options)

    const commandId = metadata.commandId
    if (commandId !== null && commandId !== '' && loaded.seenCommandIds.has(commandId)) {
      return {kind: 'duplicate', commandId}
    }

    const decision = await aggregate.handle(loaded.state, command)
    if (decision.kind === 'reject') {
      return {kind: 'rejected', rejection: decision.rejection, state: loaded}
    }
    if (decision.events.length === 0) {
      return {kind: 'committed', state: loaded, events: []}
    }

    const {state, events, snapshot, projections} = this.#stage(
      aggregate,
      loaded,
      decision.events,
      metadata,
    )
    await this.repository.commit({entityId, events, snapshot, projections}, options)

    return {kind: 'committed', state, events}
  }

  /**
   * Like {@link execute}, but loads and runs the command again after a retryable error (see
   * {@link isRetryable}), waiting {@link retryDelay} between attempts. When the attempts run out,
   * the last error is thrown. A rejection or a duplicate is a result, never retried.
   */
  async executeWithRetry<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    entityId: string,
    command: C,
    options: RetryOptions = {},
  ): Promise<ExecuteResult<S>> {
    const policy = options.policy ?? DEFAULT_RETRY_POLICY
    checkRetryPolicy(policy)
    const wait = options.sleep ?? sleep

    // Each attempt waits for the one before it.
    /* oxlint-disable no-await-in-loop */
    for (let retry = 0; ; retry++) {
      try {
        return await this.execute(aggregate, entityId, command, options)
      } catch (error) {
        if (!isRetryable(error) || retry + 1 >= policy.maxAttempts) {
          throw error
        }
      }

      await wait(retryDelay(policy, retry), options.signal)
    }
    /* oxlint-enable no-await-in-loop */
  }

  /**
   * Begins a transaction that reads streams of any aggregate types, appends to some of them, and
   * commits them all at once, with a read fence on every stream it reads but doesn't write. The
   * store's region, clock, commit IDs and snapshot interval apply to it, and each stream's
   * aggregate projectors run in its commit. The repository must be a
   * {@link TransactionalRepository}, as every one this package ships is, or this throws
   * `invalidArgument`. {@link execute} stays the shortcut for one stream.
   */
  transaction(): Transaction {
    const repository = this.repository
    if (!isTransactional(repository)) {
      throw new EvtError('invalidArgument', 'the repository has no commitTransaction')
    }

    return new Transaction({
      repository,
      load: (aggregate, entityId, options) => this.load(aggregate, entityId, options),
      snapshotInterval: (aggregate) => this.snapshotInterval(aggregate),
      metadata: (partial) => this.#metadata(partial),
    })
  }

  async #loadOnce<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    entityId: string,
    options: CallOptions,
  ): Promise<Loaded<S>> {
    const snapshot = await this.repository.snapshot(entityId, options)

    let state = aggregate.initial()
    let sequence = 0n
    let snapshotSequence = 0n
    if (snapshot) {
      try {
        state = aggregate.state.decode(snapshot.payload ?? new Uint8Array())
      } catch (error) {
        throw new SnapshotError(entityId, error)
      }
      sequence = snapshot.eventSequence
      snapshotSequence = snapshot.sequence
    }

    const events = snapshot
      ? await this.repository.eventsAfter(entityId, sequence, options)
      : await this.repository.events(entityId, options)

    // With no events after it, a snapshot and a compaction between the two reads would leave no
    // gap to see, so the snapshot is read again: a newer one means the stream moved past it.
    if (events.length === 0) {
      const covered = (await this.repository.snapshot(entityId, options))?.eventSequence ?? 0n
      if (covered > sequence) {
        throw new SequenceGapError(entityId, sequence, covered + 1n)
      }
    }

    const seenCommandIds = new Set<string>()
    for (const stored of events) {
      if (stored.sequence !== sequence + 1n) {
        throw new SequenceGapError(entityId, sequence, stored.sequence)
      }

      state = aggregate.apply(state, replayEvent(aggregate.events, stored))
      sequence = stored.sequence
      const commandId = stored.metadata.commandId
      if (commandId !== null && commandId !== '') {
        seenCommandIds.add(commandId)
      }
    }

    return {entityId, state, sequence, snapshotSequence, seenCommandIds}
  }

  // Applies the decided events, and builds the stored events, the snapshot the policy calls for,
  // and the projectors' operations. The state is only used once the commit succeeds.
  #stage<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    loaded: Loaded<S>,
    decided: readonly EventOf<D>[],
    metadata: Metadata,
  ): {
    state: Loaded<S>
    events: SerializedEvent[]
    snapshot: NewSnapshot | null
    projections: ProjectorGroup[]
  } {
    const entityId = loaded.entityId
    const {events, state} = serializeEvents(
      aggregate,
      entityId,
      loaded.sequence,
      loaded.state,
      decided,
      metadata,
    )
    const interval = this.snapshotInterval(aggregate)
    const snapshot = commitSnapshot(aggregate, entityId, interval, loaded, events.length, state)

    const seenCommandIds = new Set(loaded.seenCommandIds)
    if (metadata.commandId !== null && metadata.commandId !== '') {
      seenCommandIds.add(metadata.commandId)
    }

    return {
      state: {
        entityId,
        state,
        sequence: loaded.sequence + BigInt(events.length),
        snapshotSequence: snapshot ? snapshot.sequence : loaded.snapshotSequence,
        seenCommandIds,
      },
      events,
      snapshot,
      projections: projectCommit(aggregate, entityId, state, events, snapshot),
    }
  }

  // The command's metadata with the store's region and the clock's timestamp where it has none,
  // and the commit ID the engine stamps: the command ID, else the caller's, else a generated one.
  // Field by field, so a field an untyped caller set to undefined is empty too, and every key is
  // written.
  #metadata(partial: Partial<Metadata> = {}): Metadata {
    const empty = emptyMetadata()
    const metadata: Metadata = {
      commandId: partial.commandId ?? empty.commandId,
      trace: partial.trace ? new Map(partial.trace) : empty.trace,
      origin: partial.origin ? {...partial.origin} : empty.origin,
      address: partial.address ?? empty.address,
      region: partial.region || this.#region,
      timestamp: partial.timestamp || rfc3339Seconds(this.#clock()),
      principalId: partial.principalId ?? empty.principalId,
      causationId: partial.causationId ?? empty.causationId,
      extensions: partial.extensions ? new Map(partial.extensions) : empty.extensions,
      commitId: partial.commitId ?? empty.commitId,
    }

    if (metadata.commandId !== null && metadata.commandId !== '') {
      metadata.commitId = metadata.commandId
    } else if (metadata.commitId === '') {
      metadata.commitId = this.#commitId()
    }

    return metadata
  }
}

// RFC 3339 in UTC, to the second, as Go's time.RFC3339 formats a UTC time.
function rfc3339Seconds(ms: number): string {
  return new Date(ms).toISOString().replace(/\.\d{3}Z$/, 'Z')
}

function isTransactional(repository: Repository): repository is TransactionalRepository {
  return typeof (repository as Partial<TransactionalRepository>).commitTransaction === 'function'
}
