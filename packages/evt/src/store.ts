// The Store: loads aggregates, runs commands, and commits their events with the snapshots the
// snapshot policy calls for.

import {
  type Aggregate,
  type EventDefinitions,
  type EventOf,
  type Rejection,
  replayEvent,
} from './aggregate.js'
import {
  Budget,
  OutcomeConfig,
  type OutcomeOptions,
  type PendingCommand,
  type Phase,
  acceptedOutcome,
  freezeCommand,
  persistRejection,
  preCheck,
  recordCommit,
  rejectedOutcome,
  replayed,
  requireCommandId,
  retryKind,
} from './command.js'
import {commitIds} from './commit-id.js'
import {
  DuplicateCommandError,
  EvtError,
  OutcomePrincipalMismatchError,
  SequenceGapError,
  SnapshotError,
  isConflict,
  isRetryable,
} from './errors.js'
import type {SerializedEvent} from './event.js'
import {type Metadata, emptyMetadata} from './metadata.js'
import {
  type CommandIdentity,
  type CommandOutcome,
  type Outcome,
  checkCommandId,
  checkMetadata,
  checkText,
  retriesExhaustedRejection,
} from './outcome.js'
import {
  type CallOptions,
  type Commit,
  type NewSnapshot,
  type OutcomeRepository,
  type ProjectorGroup,
  type Repository,
  type TransactionalRepository,
  checkNotReserved,
} from './repository.js'
import {
  DEFAULT_RETRY_POLICY,
  type RetryPolicy,
  checkRetryPolicy,
  retryDelay,
  sleep,
} from './retry.js'
import {commitSnapshot, projectCommit, serializeEvents} from './staging.js'
import {
  type ReadFailures,
  Transaction,
  type TransactionContext,
  type TransactionDecision,
} from './transaction.js'

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
  /**
   * Records a durable outcome for every command (ADR 0004): run commands with
   * {@link Store.executeWithOutcome}, and transactions with {@link Transaction.beginCommand}.
   * `true` takes every default. The repository must be an {@link OutcomeRepository}, as every one
   * this package ships is.
   */
  outcomes?: OutcomeOptions | true | undefined
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
  readonly #outcomes: OutcomeConfig | null

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

    const outcomes = options.outcomes
    this.#outcomes =
      outcomes === undefined ? null : new OutcomeConfig(outcomes === true ? {} : outcomes)
    if (this.#outcomes !== null && !isOutcomeRepository(repository)) {
      throw new EvtError(
        'invalidArgument',
        'command outcomes need an OutcomeRepository, which records them in the commit',
      )
    }
  }

  /** Whether this store records command outcomes. */
  get recordsOutcomes(): boolean {
    return this.#outcomes !== null
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
    if (this.#outcomes !== null) {
      throw new EvtError(
        'invalidArgument',
        'this store records command outcomes: execute would skip the outcome row; ' +
          'call executeWithOutcome',
      )
    }

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

    return this.#transaction(repository, {})
  }

  /**
   * Runs a multi-stream command on a store with outcomes, under fence-aware retry. Each attempt
   * begins a transaction, begins the command in it ({@link Transaction.beginCommand}), runs
   * `prepare` against it, and commits it as `prepare` decided: `accept` with
   * {@link Transaction.commitWithOutcome}, `reject` with {@link Transaction.reject}, or with
   * {@link Transaction.rejectWithEvents} when it keeps its events. A stored outcome with the same
   * identity digest is returned without running `prepare`, and one with another digest is an
   * `IdempotencyKeyReuseError`.
   *
   * `prepare` is the command's decision step: it reads what it needs, decides, appends to any of
   * the streams it read, adds the fences it stamps itself, and returns its decision. When the
   * commit conflicts in a way the engine can decide again (a stream it read or wrote moved, a
   * fence the engine filled), `prepare` runs again from scratch against fresh reads, after a
   * backoff, up to the store's retry policy; when the last attempt conflicts too, the store
   * records the `stale-fence` rejection, so the command ID keeps that answer. A fence the caller
   * stamped that went stale records the `stale-fence` rejection at once. A throttled backend is
   * retried the same way and thrown when the attempts run out, and stream head drift is thrown at
   * once. A read `prepare` passes on unchanged is retried, but never recorded against the command;
   * any other error it throws is its own failure, thrown at once with nothing recorded.
   *
   * `prepare` must be safe to repeat: it decides from the transaction's reads, not from state an
   * earlier run captured, and only the attempt that commits writes anything. The identity is the
   * caller's intent, fixed before anything is read: its targets are the streams the request names,
   * not the ones `prepare` ends up writing, which may change between attempts.
   */
  async executeTransaction(
    identity: CommandIdentity,
    prepare: (transaction: Transaction) => TransactionDecision | Promise<TransactionDecision>,
    options: ExecuteOptions = {},
  ): Promise<CommandOutcome> {
    const config = this.#outcomeConfig()
    const repository = this.repository as OutcomeRepository
    const pending = await freezeCommand(config, identity, this.#metadata(options.metadata))

    const budget = new Budget(config.retry)
    // Each attempt waits for the one before it.
    /* oxlint-disable no-await-in-loop */
    for (;;) {
      const attempt = await this.#attemptTransaction(
        repository,
        identity,
        pending,
        prepare,
        options,
      )
      if (attempt.kind === 'done') {
        return attempt.outcome
      }

      const kind = retryKind(attempt.error, attempt.phase)
      if (kind === 'never') {
        throw attempt.error
      }

      const delay = budget.next()
      if (delay === null) {
        if (kind !== 'conflict') {
          throw attempt.error
        }

        const rejection = config.staleFence(retriesExhaustedRejection(budget.attempts))
        return persistRejection(repository, pending, config, rejection, options)
      }

      await config.sleep(delay, options.signal)
    }
    /* oxlint-enable no-await-in-loop */
  }

  // One attempt of a multi-stream command: a new transaction, the outcome check, the decision
  // step and the commit.
  async #attemptTransaction(
    repository: OutcomeRepository,
    identity: CommandIdentity,
    pending: PendingCommand,
    prepare: (transaction: Transaction) => TransactionDecision | Promise<TransactionDecision>,
    options: CallOptions,
  ): Promise<Attempt> {
    const readFailures: ReadFailures = {byStream: new Map()}
    const transaction = this.#transaction(repository, {frozen: pending, readFailures})

    let decision: TransactionDecision
    try {
      const begun = await transaction.beginCommand(identity, options)
      if (begun.kind === 'replayed') {
        return {kind: 'done', outcome: begun.outcome}
      }
    } catch (error) {
      return failed(error, 'deciding')
    }

    try {
      decision = await prepare(transaction)
    } catch (error) {
      // An error the step passed on from a failed read is a load failure, retried but never
      // recorded. Any other is the step's own, even a conflict: it's thrown at once.
      const passedOn = [...readFailures.byStream.values()].includes(error)
      return failed(error, passedOn ? 'deciding' : 'internal')
    }

    try {
      const outcome =
        decision.kind === 'accept'
          ? await transaction.commitWithOutcome({
              response: decision.response,
              signal: options.signal,
            })
          : decision.withEvents
            ? await transaction.rejectWithEvents(decision.rejection, options)
            : await transaction.reject(decision.rejection, options)
      return {kind: 'done', outcome}
    } catch (error) {
      return failed(error, 'committing')
    }
  }

  #transaction(
    repository: TransactionalRepository,
    extra: Pick<TransactionContext, 'frozen' | 'readFailures'>,
  ): Transaction {
    return new Transaction({
      repository,
      load: (aggregate, entityId, options) => this.load(aggregate, entityId, options),
      snapshotInterval: (aggregate) => this.snapshotInterval(aggregate),
      metadata: (partial) => this.#metadata(partial),
      outcomes: this.#outcomes,
      ...extra,
    })
  }

  /**
   * Runs a command on a store with outcomes, recording its outcome in the same commit as its
   * events, so a retried command returns the recorded answer instead of running again. It needs a
   * command ID (at most 512 UTF-8 bytes) and the aggregate's {@link AggregateDefinition.identify},
   * both checked before anything is read.
   *
   * Every attempt reads the command's outcome row first. A stored outcome with the same identity
   * digest is returned (`replayed`) without running the handler, and one with another digest is an
   * `IdempotencyKeyReuseError`. Otherwise the handler runs:
   *
   * - accepted with events: they commit with an accepted outcome carrying the decision's
   *   `response`;
   * - accepted with no events: an accepted no-op outcome, with its response;
   * - rejected: a rejected outcome, returned, not thrown. It writes no events, unless the
   *   rejection carries some: those commit with the rejected outcome, which lists them;
   * - a thrown error: an internal failure, thrown with no outcome written.
   *
   * When the commit conflicts with another writer, the command runs again from a fresh load, after
   * a backoff, up to the store's retry policy. When the last attempt conflicts too, the store
   * records a stale-fence rejection as the outcome and returns it, so the command ID keeps that
   * answer; the remedy is a new command ID. A throttled backend is retried the same way and thrown
   * when the attempts run out. Stream head drift is thrown at once, and records nothing.
   *
   * A lost race for the outcome row resolves through the winner's row, and a commit whose result
   * is unknown is recovered by polling the row, never by running the handler again: a row that
   * doesn't appear within the recovery window is a retryable `AmbiguousWriteError`. An event
   * written before outcomes existed that already carries the command ID is a
   * `DuplicateCommandError`.
   *
   * The identity digest covers the command's type and payload, the entity ID and the metadata's
   * principal.
   */
  async executeWithOutcome<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    entityId: string,
    command: C,
    options: ExecuteOptions = {},
  ): Promise<CommandOutcome> {
    const config = this.#outcomeConfig()
    const repository = this.repository as OutcomeRepository
    const metadata = this.#metadata(options.metadata)
    const pending = await freezeCommand(
      config,
      singleIdentity(aggregate, entityId, command, metadata),
      metadata,
    )

    const budget = new Budget(config.retry)
    // Each attempt waits for the one before it.
    /* oxlint-disable no-await-in-loop */
    for (;;) {
      const attempt = await this.#attemptWithOutcome(aggregate, entityId, command, pending, options)
      if (attempt.kind === 'done') {
        return attempt.outcome
      }

      const kind = retryKind(attempt.error, attempt.phase)
      if (kind === 'never') {
        throw attempt.error
      }

      const delay = budget.next()
      if (delay === null) {
        if (kind === 'transient') {
          throw attempt.error
        }

        const rejection = config.staleFence(retriesExhaustedRejection(budget.attempts))
        return persistRejection(repository, pending, config, rejection, options)
      }

      await config.sleep(delay, options.signal)
    }
    /* oxlint-enable no-await-in-loop */
  }

  /**
   * Reads a command's outcome without running or writing anything, as ambiguous-write recovery
   * does: `command` and `entityId` give its identity exactly as {@link executeWithOutcome} digests
   * them, and the metadata its command ID and principal. Null when no row exists, which is never
   * proof the command didn't apply; the stored outcome (`replayed`) for the same digest; and an
   * `IdempotencyKeyReuseError` for another.
   */
  async resolveOutcome<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    entityId: string,
    command: C,
    options: ExecuteOptions = {},
  ): Promise<CommandOutcome | null> {
    this.#outcomeConfig()
    const metadata = this.#metadata(options.metadata)

    return this.resolveCommand(singleIdentity(aggregate, entityId, command, metadata), options)
  }

  /**
   * Looks a command's outcome up by its identity, without running or writing anything: the outcome
   * check every attempt starts with, for a single-aggregate command or a transaction's. Null when
   * no row exists, which is never proof the command didn't apply; the stored outcome (`replayed`)
   * for the same digest; and an `IdempotencyKeyReuseError` for another. The command is checked as
   * a command is before it runs: a missing or overlong command ID, or a reserved target, throws.
   */
  async resolveCommand(
    identity: CommandIdentity,
    options: ExecuteOptions = {},
  ): Promise<CommandOutcome | null> {
    const config = this.#outcomeConfig()
    const pending = await freezeCommand(config, identity, this.#metadata(options.metadata))
    const stored = await preCheck(this.repository as OutcomeRepository, pending, options)

    return stored === null ? null : replayed(stored)
  }

  /**
   * Looks a command's outcome up by its command ID alone, for a caller that can't rebuild the
   * command's identity, and answers it only for the principal that recorded it (null or empty for
   * none). Nothing runs, and the digest isn't compared. Null when no row exists; the stored
   * outcome (`replayed`) when the row records the same principal; an
   * `OutcomePrincipalMismatchError` when it records another (`recorded`), or no identity at all
   * (not `recorded`), so the lookup fails closed.
   */
  async resolveCommandForPrincipal(
    commandId: string,
    principal: string | null,
    options: CallOptions = {},
  ): Promise<CommandOutcome | null> {
    this.#outcomeConfig()
    checkCommandId(commandId)

    // An empty principal is no principal, as an outcome row records it.
    const asking = principal === '' ? null : principal
    if (asking !== null) {
      checkMetadata({...emptyMetadata(), principalId: asking})
    }

    const stored = await (this.repository as OutcomeRepository).outcome(commandId, options)
    if (stored === null) {
      return null
    }
    if (stored.identity === null) {
      throw new OutcomePrincipalMismatchError(commandId, false)
    }
    if (stored.identity.principal !== asking) {
      throw new OutcomePrincipalMismatchError(commandId, true)
    }

    return replayed(stored)
  }

  // One attempt of a command with outcomes: the outcome check, the load, the handler and the
  // commit, all from scratch.
  async #attemptWithOutcome<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    entityId: string,
    command: C,
    pending: PendingCommand,
    options: CallOptions,
  ): Promise<Attempt> {
    const repository = this.repository as OutcomeRepository
    const config = this.#outcomeConfig()

    let loaded: Loaded<S>
    try {
      const stored = await preCheck(repository, pending, options)
      if (stored !== null) {
        return {kind: 'done', outcome: replayed(stored)}
      }
      loaded = await this.load(aggregate, entityId, options)
    } catch (error) {
      return failed(error, 'deciding')
    }

    // An event the load folded in already carries the command ID. Another attempt of the command
    // may have committed since the row was read, so it's read again, and answers the command when
    // it's there. Without one, it's an event written before outcomes existed: the command ran, so
    // no outcome is recorded for it.
    if (loaded.seenCommandIds.has(pending.commandId)) {
      let stored: Outcome | null
      try {
        stored = await preCheck(repository, pending, options)
      } catch (error) {
        return failed(error, 'deciding')
      }
      if (stored !== null) {
        return {kind: 'done', outcome: replayed(stored)}
      }
      return failed(new DuplicateCommandError(pending.commandId), 'internal')
    }

    let commit: Commit
    try {
      commit = await this.#decide(aggregate, loaded, command, pending)
    } catch (error) {
      return failed(error, 'internal')
    }

    try {
      const outcome = await recordCommit(
        repository,
        pending,
        config,
        commit.outcome!,
        commit.events,
        () => repository.commit(commit, options),
      )
      return {kind: 'done', outcome}
    } catch (error) {
      return failed(error, 'committing')
    }
  }

  // Runs the handler and builds the commit that records its outcome: its events, with the
  // snapshot and projector operations they call for, or none.
  async #decide<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    loaded: Loaded<S>,
    command: C,
    pending: PendingCommand,
  ): Promise<Commit> {
    const entityId = loaded.entityId
    const decision = await aggregate.handle(loaded.state, command)

    // A rejection may carry events: they commit with the rejected outcome like an accepted
    // command's, decided on the state this attempt loaded.
    const outcome =
      decision.kind === 'reject'
        ? rejectedOutcome(pending, decision.rejection)
        : acceptedOutcome(pending, decision.response)
    const decided = decision.events ?? []
    if (decided.length === 0) {
      return {entityId, events: [], outcome}
    }

    const {events, snapshot, projections} = this.#stage(
      aggregate,
      loaded,
      decided,
      pending.metadata,
    )

    return {entityId, events, snapshot, projections, outcome}
  }

  #outcomeConfig(): OutcomeConfig {
    if (this.#outcomes === null) {
      throw new EvtError(
        'invalidArgument',
        'command outcomes are not enabled; construct the store with outcomes',
      )
    }

    return this.#outcomes
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

// An attempt of a command with outcomes: its outcome, or the error that ended it and the phase it
// arose in.
type Attempt =
  {kind: 'done'; outcome: CommandOutcome} | {kind: 'failed'; error: unknown; phase: Phase}

function failed(error: unknown, phase: Phase): Attempt {
  return {kind: 'failed', error, phase}
}

function isOutcomeRepository(repository: Repository): repository is OutcomeRepository {
  return (
    isTransactional(repository) &&
    typeof (repository as Partial<OutcomeRepository>).outcome === 'function'
  )
}

// A single-aggregate command's identity: the aggregate's description of it, with the entity as
// its one target. The target and the command ID are checked first, before the payload is encoded,
// in the order Go and Rust check them.
function singleIdentity<S, C, D extends EventDefinitions>(
  aggregate: Aggregate<S, C, D>,
  entityId: string,
  command: C,
  metadata: Metadata,
): CommandIdentity {
  checkText('a target stream ID', entityId)
  checkNotReserved(entityId)
  requireCommandId(metadata)
  if (aggregate.identify === undefined) {
    throw new EvtError(
      'invalidArgument',
      `aggregate ${aggregate.type} has no identify, which a store with outcomes needs`,
    )
  }

  const {commandType, payload} = aggregate.identify(command)

  return {commandType, targets: [entityId], payload}
}
