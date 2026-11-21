// JournalRepository: a repository that journals the events it commits (ADR 0005; storage format,
// "The commit" and "Feature marker and history").

import {
  ConflictError,
  EvtError,
  ProjectorOperationError,
  StoreNotInitializedError,
  StreamHeadsNotMigratedError,
  TransientError,
} from '../errors.js'
import type {SerializedEvent, SerializedSnapshot} from '../event.js'
import {
  JOURNAL_MARKER,
  type JournalDeclared,
  type JournalMarker,
  acceptJournal,
  declaredAnything,
} from '../features.js'
import type {Outcome} from '../outcome.js'
import {
  type CommitProjector,
  type ProjectedView,
  commitStream,
  expectPayload,
  projectViews,
  putView,
} from '../projection.js'
import {
  type CallOptions,
  type Commit,
  type CommitReceipt,
  type OutcomeRepository,
  type TransactionCommit,
  commitAsTransaction,
  commitTransactionWithReceipt,
  commitWithReceipt,
  normalizeTransaction,
} from '../repository.js'
import {retryDelay, sleep} from '../retry.js'
import {compareUtf8} from '../utf8.js'
import {type Journal, allocate} from './config.js'
import {PROJECTOR_NAME, PositionTaken, journalProjector} from './projector.js'
import {type JournalPage, type JournalStore, highWatermark, page} from './read.js'
import {type JournalRow, counterPayload} from './row.js'
import {
  COUNTER_ENTITY_TYPE,
  COUNTER_OWNER,
  COUNTER_SORT_KEY,
  counterPartitionKey,
  setAllocations,
} from './scope.js'

/**
 * What a journal repository wraps: a repository that records outcomes, reads its journal, and lets
 * the journal declare itself on the store's feature manifest, as `MemRepository` and
 * `DynamoRepository` (with a views table) do.
 */
export type JournalBackend = OutcomeRepository &
  JournalStore & {readonly [JOURNAL_MARKER]: JournalMarker}

/** Options for a {@link JournalRepository}. */
export interface JournalRepositoryOptions {
  /** Waits between contention attempts; a test can count or skip the waits. A timer by default. */
  readonly sleep?: ((ms: number, signal?: AbortSignal) => Promise<void>) | undefined
}

/**
 * A repository that journals the events it commits. For every commit that writes an event of a
 * journaled entity type, it:
 *
 * 1. reads the counter of every scope the commit's events map to, and gives each event the next
 *    position in each of its scopes, streams by entity ID bytes, events by sequence;
 * 2. writes the allocations into the events' metadata (`evt.journal`), so the rows and counters
 *    can always be derived again from the stored events;
 * 3. guards every row's size before anything is written ({@link JournalRowTooLargeError});
 * 4. adds the journal rows (from the journal's projector, owned by each event's stream) and the
 *    scope counters (compare-and-set on the counter it read, owned by the engine) to the commit's
 *    view operations, so they commit atomically with the events;
 * 5. declares the journal feature and the journaled entity types history-dependent before the
 *    first such commit, so a writer that can't keep the journal refuses the store and compaction
 *    refuses the events the journal is rebuilt from.
 *
 * When another writer takes a position first, it reads the counters again and allocates again,
 * with backoff, up to the journal's retry policy; when the attempts run out it throws a
 * {@link ConflictError} for the caller's own retry. A transient failure is retried the same way.
 * Everything else (reads, snapshots, compaction, outcomes) is the wrapped repository's.
 *
 * The allocations are only known once the commit lands, so its receipt returns the events as
 * stored, and the store hands those back. A wrapper above the journal must forward the receipt
 * methods. Every writer of a journal table must commit through a journal repository with an
 * equivalent configuration, so the repository it wraps becomes a journal writer, and every other
 * handle refuses a store that declares the journal. A commit through the wrapped repository itself
 * is accepted but not journaled.
 */
export class JournalRepository<R extends JournalBackend = JournalBackend>
  implements OutcomeRepository, JournalStore
{
  /** The wrapped repository, a journal writer. Commit through the journal, not through it. */
  readonly inner: R
  readonly journal: Journal
  /** The wrapped repository's history declaration, present only when it declares. */
  declare readonly declareHistoryDependent?: (
    types: readonly string[],
    options?: CallOptions,
  ) => Promise<void>
  readonly #sleep: (ms: number, signal?: AbortSignal) => Promise<void>
  #declared = false

  constructor(inner: R, journal: Journal, options: JournalRepositoryOptions = {}) {
    if (typeof inner[JOURNAL_MARKER]?.declareJournal !== 'function') {
      throw new EvtError(
        'invalidArgument',
        'a journal repository wraps a repository that can declare the journal',
      )
    }

    this.inner = inner
    this.journal = journal
    this.#sleep = options.sleep ?? sleep
    acceptJournal(inner)
    const declare = inner.declareHistoryDependent
    if (declare !== undefined) {
      this.declareHistoryDependent = (types, call) => declare.call(inner, types, call)
    }
  }

  /** The journal's commit-wide projector, for replays: regroup stored events and run it. */
  projector(): CommitProjector {
    return journalProjector(this.journal)
  }

  /** The scope's high watermark. */
  highWatermark(scope: string, options?: CallOptions): Promise<bigint> {
    return highWatermark(this.inner, scope, options)
  }

  /** A page of the scope's rows after `after`. */
  page(scope: string, after: bigint, limit: number, options?: CallOptions): Promise<JournalPage> {
    return page(this.inner, scope, after, limit, options)
  }

  readCounter(scope: string, options?: CallOptions): Promise<bigint> {
    return this.inner.readCounter(scope, options)
  }

  readCounters(scopes: readonly string[], options?: CallOptions): Promise<bigint[]> {
    return this.inner.readCounters(scopes, options)
  }

  readLastPosition(scope: string, options?: CallOptions): Promise<bigint> {
    return this.inner.readLastPosition(scope, options)
  }

  readRows(
    scope: string,
    after: bigint,
    limit: number,
    options?: CallOptions,
  ): Promise<JournalRow[]> {
    return this.inner.readRows(scope, after, limit, options)
  }

  async commit(commit: Commit, options?: CallOptions): Promise<void> {
    await this.commitWithReceipt(commit, options)
  }

  async commitTransaction(transaction: TransactionCommit, options?: CallOptions): Promise<void> {
    await this.commitTransactionWithReceipt(transaction, options)
  }

  commitWithReceipt(commit: Commit, options?: CallOptions): Promise<CommitReceipt> {
    return this.#drive(
      new StagedCommit(commit),
      (staged) => commitWithReceipt(this.inner, staged, options),
      options,
    )
  }

  commitTransactionWithReceipt(
    transaction: TransactionCommit,
    options?: CallOptions,
  ): Promise<CommitReceipt> {
    return this.#drive(
      new StagedTransaction(transaction),
      (staged) => commitTransactionWithReceipt(this.inner, staged, options),
      options,
    )
  }

  writesViews(): boolean {
    return this.inner.writesViews?.() === true
  }

  appliesExpectedPayload(): boolean {
    return this.inner.appliesExpectedPayload?.() === true
  }

  outcome(commandId: string, options?: CallOptions): Promise<Outcome | null> {
    return this.inner.outcome(commandId, options)
  }

  events(entityId: string, options?: CallOptions): Promise<SerializedEvent[]> {
    return this.inner.events(entityId, options)
  }

  eventsAfter(entityId: string, after: bigint, options?: CallOptions): Promise<SerializedEvent[]> {
    return this.inner.eventsAfter(entityId, after, options)
  }

  latest(entityId: string, n: number, options?: CallOptions): Promise<SerializedEvent[]> {
    return this.inner.latest(entityId, n, options)
  }

  snapshot(entityId: string, options?: CallOptions): Promise<SerializedSnapshot | null> {
    return this.inner.snapshot(entityId, options)
  }

  putSnapshot(snapshot: SerializedSnapshot, options?: CallOptions): Promise<boolean> {
    return this.inner.putSnapshot(snapshot, options)
  }

  compactBelow(entityId: string, through: bigint, options?: CallOptions): Promise<number> {
    return this.inner.compactBelow(entityId, through, options)
  }

  // Commits a staged commit with its journal rows, retrying while other writers take the positions
  // it allocated, and on a transient failure. The counters are read and the commit staged
  // (allocation, size guard, rows) before the manifest is touched, so a commit that can't be built
  // declares nothing. Returns the receipt of the attempt that committed.
  async #drive<C>(
    base: Staged<C>,
    send: (commit: C) => Promise<CommitReceipt>,
    options: CallOptions | undefined,
  ): Promise<CommitReceipt> {
    const covered = base.events().some((event) => this.journal.covers(event.entityType))
    // The rows and counters are view operations: a repository that drops them would commit the
    // events and their allocations without the rows.
    if (covered && !this.writesViews()) {
      throw new EvtError(
        'viewsUnsupported',
        'the repository does not write view operations; refusing to journal',
      )
    }
    // The counter put is a compare-and-set, never written unconditionally.
    if (covered && !this.appliesExpectedPayload()) {
      throw new EvtError(
        'expectedPayloadUnsupported',
        'the repository does not apply the expected-payload view condition; refusing to journal',
      )
    }

    const plan = base.events().map((event) => this.journal.scopesOf(event))
    if (!covered) {
      // Nothing is journaled, but the key is the engine's: an allocation forged on an uncovered
      // event must not reach the log, where a rebuild would read it.
      const attempt = base.duplicate()
      for (const event of attempt.events()) {
        setAllocations(event, [])
      }

      return send(attempt.value)
    }

    const scopes = [...new Set(plan.flat())]
    scopes.sort(compareUtf8)
    const policy = this.journal.retry
    let repair = false
    // Each attempt waits for the one before it.
    /* oxlint-disable no-await-in-loop */
    for (let retry = 0; ; retry++) {
      const counters = await this.#readCounters(scopes, repair, options)
      const attempt = base.duplicate()
      this.#stage(attempt, plan, counters)
      // What the repository will refuse is refused here, before the manifest is written.
      attempt.validate()
      const declared = await this.#ensureDeclared(options)

      let error: unknown
      try {
        return await send(attempt.value)
      } catch (failure) {
        error = failure
      }

      const uninitialized =
        error instanceof StreamHeadsNotMigratedError || error instanceof StoreNotInitializedError
      if (declaredAnything(declared) && uninitialized) {
        await this.#retract(declared, options)
        throw error
      }

      let lost: boolean
      if (taken(error)) {
        // Another writer holds a position this attempt allocated. If no counter moved since they
        // were read, a counter is behind its rows: the next attempt allocates from the highest row.
        const again = await this.#readCounters(scopes, false, options)
        repair ||= sameCounters(again.stored, counters.stored)
        lost = true
      } else if (error instanceof TransientError) {
        lost = false
      } else {
        // Everything else is the caller's: a conflict on a stream (which outranks the journal's
        // own failures), a failed outcome, an unknown result.
        throw error
      }

      if (retry + 1 >= policy.maxAttempts) {
        if (!lost) {
          throw error
        }
        throw new ConflictError(
          base.label(),
          `journal positions kept being taken by other writers; ${policy.maxAttempts} attempts ` +
            'exhausted',
        )
      }
      await this.#sleep(retryDelay(policy, retry), options?.signal)
    }
    /* oxlint-enable no-await-in-loop */
  }

  // The stored counter of every scope (0 for none), and the position each allocates from. With
  // `repair`, a counter behind its scope's highest row allocates from that row instead, and the
  // commit writes the counter back.
  async #readCounters(
    scopes: readonly string[],
    repair: boolean,
    options: CallOptions | undefined,
  ): Promise<Counters> {
    const values = await this.inner.readCounters(scopes, options)
    const lasts = repair
      ? await Promise.all(scopes.map((scope) => this.inner.readLastPosition(scope, options)))
      : []
    const stored = new Map<string, bigint>()
    const base = new Map<string, bigint>()
    for (const [index, scope] of scopes.entries()) {
      const value = values[index] ?? 0n
      const last = lasts[index] ?? 0n
      stored.set(scope, value)
      base.set(scope, last > value ? last : value)
    }

    return {stored, base}
  }

  // Stages one attempt: allocates from the counters, writes the allocations into the events'
  // metadata, guards every row, and appends the rows and counters to the commit's views.
  #stage<C>(commit: Staged<C>, plan: readonly (readonly string[])[], counters: Counters): void {
    const allocated = allocate(counters.base, plan)
    const projectorIndex = Math.max(-1, ...commit.views().map((view) => view.projectorIndex)) + 1

    let at = 0
    let commitId = ''
    let widest: ReturnType<Journal['widestRow']> = null
    const streams = commit.streams().map((events) => {
      for (const event of events) {
        // The key is the engine's: whatever a caller put there is replaced.
        const list = allocated.allocations[at++]!
        setAllocations(event, list)
        const refused = this.journal.widestRow(event, list)
        if (refused !== null && (widest === null || refused.size > widest.size)) {
          widest = refused
        }
      }
      const first = events[0]!
      if (commitId === '') {
        commitId = first.metadata.commitId
      }

      return commitStream(first.entityId, first.entityType, events, new Uint8Array())
    })
    // Every row is measured before any is refused, so the error names the widest one.
    if (widest !== null) {
      throw widest
    }

    const views = projectViews([journalProjector(this.journal)], {commitId, streams})
    for (const view of views) {
      Object.assign(view, {projectorIndex})
    }
    const counted = [...allocated.counters]
    counted.sort(([a], [b]) => compareUtf8(a, b))
    for (const [scope, position] of counted) {
      // A compare-and-set on the counter this attempt read: absent, or exactly its stored payload.
      // A writer that lost the race fails here even when the rows it allocated are gone.
      const stored = counters.stored.get(scope) ?? 0n
      const put = putView(
        counterPartitionKey(scope),
        COUNTER_SORT_KEY,
        COUNTER_OWNER,
        COUNTER_ENTITY_TYPE,
        counterPayload(scope, position),
        {
          condition: stored === 0n ? 'absent' : 'none',
          conflict: new PositionTaken(scope, position),
        },
      )
      views.push({
        projector: PROJECTOR_NAME,
        projectorIndex,
        operation: views.length,
        op: stored === 0n ? put : expectPayload(put, counterPayload(scope, stored)),
      })
    }
    // The journal's operations come after the commit's own projectors'.
    commit.addViews(views)
  }

  // Declares the journal feature and the journaled entity types history-dependent, once per
  // repository. Resolves to what this call wrote, which a commit refused as uninitialized takes
  // back.
  async #ensureDeclared(options: CallOptions | undefined): Promise<JournalDeclared> {
    if (this.#declared) {
      return {journal: false, history: []}
    }

    const declared = await this.inner[JOURNAL_MARKER].declareJournal(
      this.journal.entityTypes,
      options,
    )
    this.#declared = true

    return declared
  }

  // Takes back what a declaration wrote, for a commit refused as uninitialized or unmigrated: the
  // manifest must not declare the journal, or forbid compaction, on a store nothing was journaled
  // into. Best effort.
  async #retract(declared: JournalDeclared, options: CallOptions | undefined): Promise<void> {
    this.#declared = false
    try {
      await this.inner[JOURNAL_MARKER].retractJournal(declared, options)
    } catch {
      // The commit's own refusal is what the caller needs.
    }
  }
}

// What one attempt read: each scope's stored counter, which the counter put is conditioned on,
// and the position each scope allocates from.
interface Counters {
  readonly stored: ReadonlyMap<string, bigint>
  readonly base: ReadonlyMap<string, bigint>
}

function sameCounters(a: ReadonlyMap<string, bigint>, b: ReadonlyMap<string, bigint>): boolean {
  return a.size === b.size && [...a].every(([scope, value]) => b.get(scope) === value)
}

// Whether the error is a journal row whose position was taken, or a counter that moved since the
// attempt read it.
function taken(error: unknown): boolean {
  return (
    error instanceof ProjectorOperationError &&
    error.projector === PROJECTOR_NAME &&
    error.mapped instanceof PositionTaken
  )
}

// A commit the journal stages: its events' streams in canonical order, and its view operations.
abstract class Staged<C> {
  abstract readonly value: C
  /** A copy to stage one attempt on, its events copied so allocating changes nothing given. */
  abstract duplicate(): Staged<C>
  /** The entity ID an error names. */
  abstract label(): string
  /** The event lists of the streams that write events, by entity ID bytes. */
  abstract streams(): SerializedEvent[][]
  /** The commit-level view operations. */
  abstract views(): readonly ProjectedView[]
  abstract addViews(views: readonly ProjectedView[]): void
  /** The repository's checks before it writes, run on the staged attempt. */
  abstract validate(): void

  events(): SerializedEvent[] {
    return this.streams().flat()
  }
}

class StagedCommit extends Staged<Commit> {
  readonly value: Commit

  constructor(commit: Commit) {
    super()
    this.value = commit
  }

  duplicate(): StagedCommit {
    return new StagedCommit(copyCommit(this.value))
  }

  label(): string {
    return this.value.entityId
  }

  streams(): SerializedEvent[][] {
    return this.value.events.length === 0 ? [] : [this.value.events]
  }

  views(): readonly ProjectedView[] {
    return this.value.views ?? []
  }

  addViews(views: readonly ProjectedView[]): void {
    this.value.views = [...this.views(), ...views]
  }

  validate(): void {
    commitAsTransaction(this.value)
  }
}

class StagedTransaction extends Staged<TransactionCommit> {
  readonly value: TransactionCommit

  constructor(transaction: TransactionCommit) {
    super()
    this.value = transaction
  }

  duplicate(): StagedTransaction {
    return new StagedTransaction({...this.value, streams: this.value.streams.map(copyCommit)})
  }

  label(): string {
    return this.value.streams[0]?.entityId ?? ''
  }

  streams(): SerializedEvent[][] {
    const written = this.value.streams.filter((stream) => stream.events.length > 0)
    written.sort((a, b) => compareUtf8(a.entityId, b.entityId))

    return written.map((stream) => stream.events)
  }

  views(): readonly ProjectedView[] {
    return this.value.views ?? []
  }

  addViews(views: readonly ProjectedView[]): void {
    this.value.views = [...this.views(), ...views]
  }

  validate(): void {
    normalizeTransaction(this.value)
  }
}

function copyCommit(commit: Commit): Commit {
  return {...commit, events: commit.events.map((event) => ({...event}))}
}
