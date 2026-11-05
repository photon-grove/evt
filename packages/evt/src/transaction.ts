// The Store's multi-stream transaction (ADR 0004 C1, C2): a read set of any aggregate types, the
// events decided for some of them, and one atomic commit that fences every stream it read but
// didn't write.

import type {Aggregate, EventDefinitions, EventOf, Rejection} from './aggregate.js'
import {
  type OutcomeConfig,
  type PendingCommand,
  acceptedOutcome,
  freezeCommand,
  persistRejection,
  preCheck,
  recordCommit,
  rejectedOutcome,
  replayed,
} from './command.js'
import {DuplicateCommandError, EvtError, FenceConflictError} from './errors.js'
import type {SerializedEvent} from './event.js'
import {type Metadata, emptyMetadata} from './metadata.js'
import {
  type CommandIdentity,
  type CommandOutcome,
  type Outcome,
  staleFenceRejection,
} from './outcome.js'
import {
  type CallOptions,
  type Commit,
  type OutcomeRepository,
  type ReadFence,
  type TransactionalRepository,
  checkNotReserved,
} from './repository.js'
import {commitSnapshot, copyMetadata, projectCommit, serializeEvents} from './staging.js'
import type {ExecuteOptions, Loaded} from './store.js'
import {compareUtf8} from './utf8.js'

const MAX_INT64 = 2n ** 63n - 1n

// The key of the property that types a handle by its aggregate's events. It exists only in types,
// and a symbol keeps it out of the way while the published declarations still check it.
declare const eventTypes: unique symbol

/** What a transaction needs from its store. */
export interface TransactionContext {
  readonly repository: TransactionalRepository
  load<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    entityId: string,
    options?: CallOptions,
  ): Promise<Loaded<S>>
  snapshotInterval(aggregate: {readonly snapshotInterval?: number | undefined}): number
  /** The command's metadata with what the store fills in, and the commit ID stamped. */
  metadata(partial?: Partial<Metadata>): Metadata
  /** How the store records command outcomes, or null when it records none. */
  readonly outcomes?: OutcomeConfig | null | undefined
}

/**
 * What {@link Transaction.beginCommand} found:
 *
 * - `proceed`: no outcome is recorded for the command ID. Read, append and fence, then commit with
 *   {@link Transaction.commitWithOutcome}, or refuse with {@link Transaction.reject} or
 *   {@link Transaction.rejectWithEvents}.
 * - `replayed`: the command already has an outcome with the same identity digest. Nothing ran and
 *   nothing is written: return it.
 */
export type Begin = {kind: 'proceed'} | {kind: 'replayed'; outcome: CommandOutcome}

/** Options for {@link Transaction.beginCommand}. */
export interface BeginOptions extends CallOptions {
  /** The command's metadata: its command ID, its principal, and what the commit stamps. */
  metadata?: Partial<Metadata> | undefined
}

/** Options for {@link Transaction.commitWithOutcome}. */
export interface OutcomeCommitOptions extends CallOptions {
  /** Stored with the accepted outcome, for a retried command to return. Empty stores none. */
  response?: Uint8Array | null | undefined
}

// The commit a transaction builds: the written streams in read order, the fences, and the events
// in the order the commit writes them.
interface Prepared {
  readonly streams: Commit[]
  readonly fences: ReadFence[]
  readonly events: SerializedEvent[]
}

/** A fence the caller stamps on a stream the transaction doesn't read. */
export interface CallerFence {
  readonly entityId: string
  /** The stream's entity type, which a fence on a retained type is refused for. */
  readonly entityType: string
  /** The head the caller observed: 0 for a stream with no events. */
  readonly head: bigint
}

/**
 * What a transaction's commit did:
 *
 * - `committed`: every stream's events (possibly none) are stored, and every fence held. `events`
 *   are the stored events in the order the commit wrote them: by entity ID bytes, then sequence.
 * - `duplicate`: an event a read stream folded in since its latest snapshot already carries the
 *   command ID, so nothing was written.
 */
export type TransactionResult =
  {kind: 'committed'; events: SerializedEvent[]} | {kind: 'duplicate'; commandId: string}

// One stream of the read set: what the read loaded, and what the transaction appended since.
interface Stream {
  // oxlint-disable-next-line typescript/no-explicit-any -- each stream has its own aggregate type.
  readonly aggregate: Aggregate<any, any, any>
  // The position of its read call, which orders the commit's projector operations.
  readonly order: number
  readonly loaded: Loaded<unknown>
  state: unknown
  events: SerializedEvent[]
}

/**
 * A stream read into a {@link Transaction}, typed by its aggregate. Pass it to
 * {@link Transaction.append} to stage events for it; a read never appended to is fenced at the
 * head it was read at.
 */
export class StreamRead<S, D extends EventDefinitions> {
  readonly #stream: Stream
  // Ties the handle to its aggregate's event types, which only append reads.
  declare readonly [eventTypes]?: D

  /** @internal Built by {@link Transaction.read}. */
  constructor(stream: Stream) {
    this.#stream = stream
  }

  get entityId(): string {
    return this.#stream.loaded.entityId
  }

  /** The state, with the events appended through this handle. */
  get state(): S {
    return this.#stream.state as S
  }

  /** The sequence of the last event, with the events appended through this handle. */
  get sequence(): bigint {
    return this.#stream.loaded.sequence + BigInt(this.#stream.events.length)
  }

  /** The stream as it was read. */
  get loaded(): Loaded<S> {
    return this.#stream.loaded as Loaded<S>
  }
}

/**
 * One atomic commit across several streams. {@link read} loads streams of any aggregate types,
 * {@link append} stages the events decided for them, {@link fence} adds a fence the caller stamped
 * on a stream it didn't read, and {@link commit} writes everything in one repository transaction,
 * or nothing. A stream read but never appended to adds a fence the engine filled at the head it
 * was read at, so the commit fails with a retryable `FenceConflictError` once another writer
 * moves it. Begin one with {@link Store.transaction}.
 *
 * Every staging call checks its input before it changes anything, so one that throws leaves the
 * transaction as it was. A transaction commits once: after {@link commit}, whatever it returned
 * or threw, every call throws `invalidCommit`. Begin a new transaction to try again.
 */
export class Transaction {
  readonly #context: TransactionContext
  readonly #streams = new Map<StreamRead<unknown, EventDefinitions>, Stream>()
  // Every stream read or being read, by ID, with the position of its read call.
  readonly #ids = new Map<string, number>()
  readonly #fences = new Map<string, CallerFence>()
  #reads = 0
  #pending = 0
  #done = false
  // The command beginCommand froze, until an outcome method takes it.
  #command: PendingCommand | null = null
  #beginning = false

  /** @internal Begun by {@link Store.transaction}. */
  constructor(context: TransactionContext) {
    this.#context = context
  }

  /**
   * Loads a stream into the read set, as {@link Store.load} does, and returns its typed handle.
   * Reading a stream the transaction already read, or is reading, throws `invalidCommit`.
   */
  async read<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    entityId: string,
    options: CallOptions = {},
  ): Promise<StreamRead<S, D>> {
    this.#checkOpen()
    if (this.#ids.has(entityId)) {
      throw invalidCommit(`duplicate read of ${entityId}`)
    }

    // The ID is held while the load runs, so a concurrent read of the same stream is refused too,
    // and the read keeps the place it was called in, whenever its load finishes.
    const order = this.#reads++
    this.#ids.set(entityId, order)
    this.#pending++
    let loaded: Loaded<S>
    try {
      loaded = await this.#context.load(aggregate, entityId, options)
    } catch (error) {
      this.#ids.delete(entityId)
      throw error
    } finally {
      this.#pending--
    }

    const stream: Stream = {
      aggregate,
      order,
      loaded,
      state: loaded.state,
      events: [],
    }
    const read = new StreamRead<S, D>(stream)
    this.#streams.set(read as StreamRead<unknown, EventDefinitions>, stream)

    return read
  }

  /**
   * Stages events for a stream this transaction read. They're applied to the handle's state, and
   * appending again continues the sequence. The commit stamps their metadata, and runs the
   * aggregate's projectors over every event appended to the stream, with its final state. A
   * handle from another transaction throws `invalidCommit`.
   */
  append<S, D extends EventDefinitions>(
    read: StreamRead<S, D>,
    events: readonly EventOf<D>[],
  ): this {
    this.#checkOpen()
    const stream = this.#streams.get(read as StreamRead<unknown, EventDefinitions>)
    if (stream === undefined) {
      throw invalidCommit(`stream ${read.entityId} was not read by this transaction`)
    }

    // Serialized now, so an event the aggregate can't encode fails here, with nothing staged.
    const after = stream.loaded.sequence + BigInt(stream.events.length)
    const staged = serializeEvents(
      stream.aggregate,
      stream.loaded.entityId,
      after,
      stream.state,
      events,
      emptyMetadata(),
    )
    stream.events.push(...staged.events)
    stream.state = staged.state

    return this
  }

  /**
   * Adds a fence the caller stamped, for a stream the transaction doesn't read: the commit fails
   * with a `FenceConflictError` once another writer moves the stream past `head`, which isn't
   * retryable, since only the caller knows what it read. A fence on a stream the transaction reads,
   * before or after the read, is dropped at commit: the read's own fence, or its write, holds the
   * stream instead. A fence in a reserved namespace, at a head outside [0, 2^63 - 1), or on a
   * stream already fenced throws, with nothing added.
   */
  fence(fence: CallerFence): this {
    this.#checkOpen()
    checkNotReserved(fence.entityId)
    if (fence.head < 0n || fence.head >= MAX_INT64 || this.#fences.has(fence.entityId)) {
      throw invalidCommit(`invalid or duplicate fence ${fence.entityId}`)
    }

    this.#fences.set(fence.entityId, {...fence})

    return this
  }

  /**
   * Starts a command on a store with outcomes: validates the command ID (present, at most 512
   * UTF-8 bytes), computes the identity digest, and reads the command's outcome row. Call it
   * first, before any read, append or fence, so a repeated command does no work. A stored outcome
   * with the same digest is `replayed`, and the transaction is done; one with another digest is an
   * `IdempotencyKeyReuseError`.
   *
   * The identity is the caller's intent, fixed before any state is read: the streams the request
   * names, not the ones the transaction goes on to read and write.
   */
  async beginCommand(identity: CommandIdentity, options: BeginOptions = {}): Promise<Begin> {
    const config = this.#outcomeConfig()
    this.#checkOpen()
    if (this.#command !== null || this.#beginning) {
      throw invalidCommit('beginCommand was already called on this transaction')
    }
    // A read that failed left nothing behind, so it doesn't count.
    if (this.#ids.size > 0 || this.#fences.size > 0) {
      throw invalidCommit(
        'beginCommand must be the first operation on a transaction, before any read, append or fence',
      )
    }

    // Held while the command is checked and its row read, so a failed begin can be called again.
    this.#beginning = true
    try {
      const metadata = this.#context.metadata(options.metadata)
      const pending = await freezeCommand(config, identity, metadata)
      const repository = this.#context.repository as OutcomeRepository
      const stored = await preCheck(repository, pending, options)
      if (stored !== null) {
        this.#done = true
        return {kind: 'replayed', outcome: replayed(stored)}
      }

      this.#command = pending
    } finally {
      this.#beginning = false
    }

    return {kind: 'proceed'}
  }

  /**
   * Stamps one commit ID and the command's metadata on every appended event, and commits every
   * stream with events, a fence on every stream read without any, and the caller's fences, in one
   * atomic repository transaction. On failure nothing is written. A command ID that an event a
   * read stream folded in since its latest snapshot already carries is a `duplicate`, and nothing
   * is written. Projector operations follow the streams in the order they were read. A commit
   * while a read is still loading throws `invalidCommit`, and leaves the transaction open.
   *
   * A transaction of a store with outcomes refuses it: begin the command with
   * {@link beginCommand}, then commit with {@link commitWithOutcome}.
   */
  async commit(options: ExecuteOptions = {}): Promise<TransactionResult> {
    if (this.#context.outcomes) {
      throw new EvtError(
        'invalidArgument',
        'this transaction records command outcomes: commit would skip the outcome row; ' +
          'call beginCommand, then commitWithOutcome or reject',
      )
    }
    this.#checkCommittable()
    this.#done = true

    const metadata = this.#context.metadata(options.metadata)
    const commandId = metadata.commandId
    if (commandId !== null && commandId !== '' && this.#seen(commandId)) {
      return {kind: 'duplicate', commandId}
    }

    const {streams, fences, events} = this.#prepare(metadata)
    await this.#context.repository.commitTransaction({streams, fences}, options)

    return {kind: 'committed', events}
  }

  /**
   * Commits the transaction and records the command as accepted, in one atomic repository
   * commit: the appended events (none is an accepted no-op), the read set's fences, and then the
   * outcome row, which stores `response` for a retried command to return. The transaction must
   * have begun with {@link beginCommand}.
   *
   * A lost race for the outcome row resolves through the winner's row, and a commit whose result
   * is unknown through polling the row (an `AmbiguousWriteError` when it doesn't appear). A failed
   * fence the caller stamped records a stale-fence rejection instead, and returns it: the command
   * ID stays terminal, so the caller decides again under a new one. Any other conflict throws:
   * begin a new transaction and the command again.
   */
  async commitWithOutcome(options: OutcomeCommitOptions = {}): Promise<CommandOutcome> {
    const pending = this.#takeCommand()
    const prepared = this.#prepare(pending.metadata)

    return this.#record(pending, acceptedOutcome(pending, options.response), prepared, options)
  }

  /**
   * Records the command as rejected without writing events: the domain refused it, and the
   * staged appends are discarded ({@link rejectWithEvents} commits them). The commit still checks
   * a fence on every stream the transaction read, at the head it read, and every fence the caller
   * stamped, so a rejection decided on stale state isn't recorded: an engine fence that failed
   * throws a retryable `FenceConflictError`, and a caller's records a stale-fence rejection
   * instead. A retried command replays the stored rejection.
   */
  async reject(rejection: Rejection, options: CallOptions = {}): Promise<CommandOutcome> {
    const pending = this.#takeCommand()
    const outcome = rejectedOutcome(pending, rejection)
    const streams = [...this.#streams.values()]
    streams.sort((a, b) => a.order - b.order)
    const fences: ReadFence[] = streams.map((stream) => ({
      entityId: stream.loaded.entityId,
      entityType: stream.aggregate.type,
      head: stream.loaded.sequence,
      provenance: 'engine',
    }))
    fences.push(...this.#callerFences())

    return this.#record(pending, outcome, {streams: [], fences, events: []}, options)
  }

  /**
   * Records the command as rejected and commits the staged events in the same atomic commit, so
   * the history shows why the command was refused, or nothing is written. The commit is checked
   * as {@link commitWithOutcome}'s is, and the rejected outcome lists its events. With nothing
   * appended it's {@link reject}. A retried command replays the rejection and writes nothing.
   */
  async rejectWithEvents(rejection: Rejection, options: CallOptions = {}): Promise<CommandOutcome> {
    const pending = this.#takeCommand()
    const prepared = this.#prepare(pending.metadata)

    return this.#record(pending, rejectedOutcome(pending, rejection), prepared, options)
  }

  // Commits a prepared transaction with the command's outcome, and settles the result.
  async #record(
    pending: PendingCommand,
    outcome: Outcome,
    prepared: Prepared,
    options: CallOptions,
  ): Promise<CommandOutcome> {
    const config = this.#outcomeConfig()
    const repository = this.#context.repository as OutcomeRepository
    const {streams, fences, events} = prepared

    try {
      return await recordCommit(repository, pending, config, outcome, events, () =>
        repository.commitTransaction({streams, fences, outcome}, options),
      )
    } catch (error) {
      // Only the caller knows what it read, so a stale fence of its own is the command's answer.
      if (error instanceof FenceConflictError && error.provenance === 'caller') {
        const rejection = config.staleFence(staleFenceRejection())
        return persistRejection(repository, pending, config, rejection, options)
      }
      throw error
    }
  }

  // The command an outcome method records, which ends the transaction: it must have begun, and no
  // read may still be loading. An event written before outcomes existed in a stream the command
  // read already carries its ID without an outcome row: the command ran, so no outcome is recorded
  // for it.
  #takeCommand(): PendingCommand {
    this.#outcomeConfig()
    this.#checkCommittable()
    const pending = this.#command
    if (pending === null) {
      throw new EvtError(
        'invalidArgument',
        'beginCommand must be called before the transaction records an outcome',
      )
    }
    this.#done = true

    if (this.#seen(pending.commandId)) {
      throw new DuplicateCommandError(pending.commandId)
    }

    return pending
  }

  // Builds the commit: every stream with events, its metadata stamped and the snapshot and
  // projector operations it calls for; a fence on every stream read without any; and the caller's
  // fences on streams the transaction didn't read.
  #prepare(metadata: Metadata): Prepared {
    const streams = [...this.#streams.values()]
    streams.sort((a, b) => a.order - b.order)

    const commits: Commit[] = []
    const fences: ReadFence[] = []
    for (const stream of streams) {
      const {aggregate, loaded} = stream
      if (stream.events.length === 0) {
        fences.push({
          entityId: loaded.entityId,
          entityType: aggregate.type,
          head: loaded.sequence,
          provenance: 'engine',
        })
        continue
      }

      for (const event of stream.events) {
        event.metadata = copyMetadata(metadata)
      }
      const interval = this.#context.snapshotInterval(aggregate)
      const appended = stream.events.length
      const snapshot = commitSnapshot(
        aggregate,
        loaded.entityId,
        interval,
        loaded,
        appended,
        stream.state,
      )
      commits.push({
        entityId: loaded.entityId,
        events: stream.events,
        snapshot,
        projections: projectCommit(
          aggregate,
          loaded.entityId,
          stream.state,
          stream.events,
          snapshot,
        ),
      })
    }
    fences.push(...this.#callerFences())

    // Sorted apart from what the repository is given, which stays in read order.
    const written = [...commits]
    written.sort((a, b) => compareUtf8(a.entityId, b.entityId))

    return {streams: commits, fences, events: written.flatMap((commit) => commit.events)}
  }

  // A caller's fence on a stream the transaction read is superseded by the read's own.
  #callerFences(): ReadFence[] {
    return [...this.#fences.values()]
      .filter((fence) => !this.#ids.has(fence.entityId))
      .map((fence): ReadFence => Object.assign({}, fence, {provenance: 'caller' as const}))
  }

  // Whether an event a read stream folded in since its latest snapshot carries the command ID.
  #seen(commandId: string): boolean {
    return [...this.#streams.values()].some((stream) => stream.loaded.seenCommandIds.has(commandId))
  }

  #checkCommittable(): void {
    this.#checkOpen()
    if (this.#pending > 0) {
      throw invalidCommit('a read is still loading; await it before the commit')
    }
  }

  #outcomeConfig(): OutcomeConfig {
    const config = this.#context.outcomes
    if (!config) {
      throw new EvtError(
        'invalidArgument',
        'command outcomes are not enabled; construct the store with outcomes',
      )
    }

    return config
  }

  #checkOpen(): void {
    if (this.#done) {
      throw invalidCommit('the transaction has committed; begin a new one')
    }
  }
}

function invalidCommit(message: string): EvtError {
  return new EvtError('invalidCommit', `invalid commit: ${message}`)
}
