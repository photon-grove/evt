// The Store's multi-stream transaction (ADR 0004 C1, C2): a read set of any aggregate types, the
// events decided for some of them, and one atomic commit that fences every stream it read but
// didn't write.

import type {Aggregate, EventDefinitions, EventOf} from './aggregate.js'
import {EvtError} from './errors.js'
import type {SerializedEvent} from './event.js'
import {type Metadata, emptyMetadata} from './metadata.js'
import {
  type CallOptions,
  type Commit,
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
   * Stamps one commit ID and the command's metadata on every appended event, and commits every
   * stream with events, a fence on every stream read without any, and the caller's fences, in one
   * atomic repository transaction. On failure nothing is written. A command ID that an event a
   * read stream folded in since its latest snapshot already carries is a `duplicate`, and nothing
   * is written. Projector operations follow the streams in the order they were read. A commit
   * while a read is still loading throws `invalidCommit`, and leaves the transaction open.
   */
  async commit(options: ExecuteOptions = {}): Promise<TransactionResult> {
    this.#checkOpen()
    if (this.#pending > 0) {
      throw invalidCommit('a read is still loading; await it before the commit')
    }
    this.#done = true

    const metadata = this.#context.metadata(options.metadata)
    const commandId = metadata.commandId
    const streams = [...this.#streams.values()]
    streams.sort((a, b) => a.order - b.order)
    if (
      commandId !== null &&
      commandId !== '' &&
      streams.some((stream) => stream.loaded.seenCommandIds.has(commandId))
    ) {
      return {kind: 'duplicate', commandId}
    }

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

    // A caller's fence on a stream the transaction read is superseded by the read's own.
    for (const fence of this.#fences.values()) {
      if (!this.#ids.has(fence.entityId)) {
        fences.push({...fence, provenance: 'caller'})
      }
    }

    await this.#context.repository.commitTransaction({streams: commits, fences}, options)

    // Sorted apart from what the repository was given, which stays in read order.
    const written = [...commits]
    written.sort((a, b) => compareUtf8(a.entityId, b.entityId))

    return {
      kind: 'committed',
      events: written.flatMap((commit) => commit.events),
    }
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
