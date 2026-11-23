// The full projection rebuild (ADR 0004; storage format, "Rebuild fence"): commit-wide projectors
// replayed over the event log, their rows written in guarded batches under the rebuild fence, and
// compacted streams seeded from their snapshots, and the journal when it's given. It follows Rust's
// `ProjectionRebuild`, without its diff mode.

import {type Aggregate, replayEvent} from './aggregate.js'
import {type EventLog, isEventLog} from './enumerate.js'
import {
  CommitTooLargeError,
  CompactionUncoveredError,
  DuplicateViewKeyError,
  EvtError,
  RebuildBeginIndeterminateError,
  RebuildConflictError,
  RebuildFailure,
  SequenceGapError,
  SharedViewKeyError,
  StreamHeadDriftError,
} from './errors.js'
import type {SerializedEvent} from './event.js'
import {
  type CommitProjector,
  type CommitView,
  type ProjectedView,
  ViewReplay,
  commitStream,
  deleteView,
  groupCommits,
  isEngineProjector,
  projectWith,
  putView,
  viewOperationSize,
} from './projection.js'
import {
  MAX_REBUILD_BEGIN_ATTEMPTS,
  MAX_REBUILD_VIEW_BATCH,
  REBUILD_GUARD_SIZE,
  type RebuildFence,
  type RebuildLease,
  acquiredFromSettled,
  holdsRebuildBegun,
  isRebuildFence,
} from './rebuild-fence.js'
import {type CallOptions, MAX_COMMIT_BYTES, type Repository} from './repository.js'
import type {Journal} from './journal/config.js'
import {journalOperations, replay as replayJournal} from './journal/replay.js'
import {MAX_COMMIT_ITEM_BYTES} from './snapshots.js'
import {Store} from './store.js'
import {compareUtf8} from './utf8.js'

/**
 * An aggregate type a rebuild reads: its streams are those stored under its own type, or under
 * `entityType` when given, for stored events of another type that fold into it (after a rename,
 * say).
 */
export type RebuildAggregate =
  | Aggregate<any, any, any>
  | {readonly aggregate: Aggregate<any, any, any>; readonly entityType: string}

/** What a {@link ProjectionRebuild} rebuilds, and who watches it. */
export interface ProjectionRebuildOptions {
  /** The commit-wide projectors, at least one, in the order a store runs them. */
  readonly projectors: readonly CommitProjector[]
  /**
   * The aggregate types in scope, at least one: every type whose streams own the projectors' rows,
   * and every type a history-dependent projector names. A type registered twice is refused.
   */
  readonly aggregates: readonly RebuildAggregate[]
  /**
   * Called as a run goes: once every row is computed, before the first write, and after each
   * guarded batch. A dry run reports nothing.
   */
  readonly onProgress?: ((progress: RebuildProgress) => void) | undefined
  /**
   * The journal the table commits with, rebuilt in the same run under the same lease: its rows
   * and counters derived from every event of its entity types, read under the lock, and written in
   * the same guarded batches after the projectors' rows, so one generation covers both. With a
   * journal there may be no projectors and no aggregate types: the run rebuilds the journal alone.
   * Like the journal's own rebuild, it restores rows a compaction deleted.
   */
  readonly journal?: Journal | undefined
}

/** What a {@link ProjectionRebuild} did. */
export interface RebuildReport {
  /** Entities whose final state the current-state projectors ran over. */
  readonly entities: number
  /** Commits the history-dependent projectors replayed. */
  readonly commits: number
  /** Rows written. */
  readonly puts: number
  /** Rows deleted. */
  readonly deletes: number
  /** Guarded batches written (`writeRebuildViews` calls). */
  readonly batches: number
  /**
   * Events read from the log: every event of the types the history-dependent projectors name and
   * the journal reads, each counted once.
   */
  readonly events: number
  /** Journal rows the events imply, with a journal. */
  readonly journalRows: number
  /** Journal counters the events imply, with a journal. */
  readonly journalCounters: number
}

/** What a dry run ({@link ProjectionRebuild.plan}) found. */
export interface RebuildPlan {
  /** The counts a run would report; `batches` is 0, since nothing was written. */
  readonly report: RebuildReport
  /** The operations, in the order a run writes them. */
  readonly operations: readonly ProjectedView[]
  /** The guarded batches the operations split into. */
  readonly batches: number
}

/** A completed {@link ProjectionRebuild.runWithLease}: its report, and the lease as last renewed. */
export interface RebuildRun {
  readonly report: RebuildReport
  readonly lease: RebuildLease
}

/** Where a run is, for an operator watching it. */
export type RebuildProgress =
  | {
      /** Every row is computed; nothing is written yet. */
      readonly kind: 'planned'
      /** The counts so far; `batches` is 0. */
      readonly report: RebuildReport
      /** The guarded batches the run will write. */
      readonly batches: number
    }
  | {
      /** A guarded batch was written. */
      readonly kind: 'wrote'
      /** The batches written so far. */
      readonly batch: number
      /** The batches to write. */
      readonly of: number
    }

type Counts = {-readonly [K in keyof RebuildReport]: RebuildReport[K]}

interface Scope {
  readonly aggregate: Aggregate<any, any, any>
  readonly entityType: string
}

interface Indexed {
  readonly index: number
  readonly projector: CommitProjector
}

/**
 * A full rebuild of commit-wide projectors from the event log, under the rebuild fence, as Rust's
 * `ProjectionRebuild` runs it. {@link run} takes the rebuild lock, so every commit fails with a
 * `RebuildInProgressError` and readers that pin the epoch are fenced while it reads the log and
 * writes the rows. It computes every row first, checking them across the whole run, then writes
 * them with `writeRebuildViews` in batches of at most 99 operations that fit the commit budget,
 * each guarded by the lease, and releases the lock, which settles the generation.
 *
 * - A **current-state** projector (no `history`) is rebuilt from each stream's final state: for
 *   every entity of every registered type, it runs once over a view of that one stream with its
 *   state, no events and an empty commit ID. States are loaded from the latest snapshot and the
 *   events after it, so a compacted stream rebuilds correctly. Only rebuild a projector whose rows
 *   depend on their owner's state alone: a row owned by another stream fails the run before
 *   anything is written.
 * - A **history-dependent** projector is rebuilt by replaying every commit of the types it names
 *   (`groupCommits`), folding each stream's state after each commit. The rows the operations
 *   leave, puts and deletes, are written, refusing a row two owners wrote
 *   ({@link SharedViewKeyError}). The replay needs each stream's whole history, from sequence 1 to
 *   its head: a stream whose early events were compacted, or whose tail expired, fails the run
 *   with a `CompactionUncoveredError` before anything is written.
 *
 * Conditions aren't evaluated: a put is written over the owner's row and a delete removes it, so a
 * rebuild runs over existing rows and is idempotent. Rows no projector produces any more are left
 * alone. The run holds every row it will write in memory, and the replay every event of its types.
 * Read the log strongly consistently (DynamoDB's default), or a commit that landed just before the
 * lock can be missed and its row overwritten by an older one.
 *
 * A failure while reading or projecting leaves the rows as they were, and {@link run} releases the
 * lock when it acquired it from a settled generation. Once a write was attempted, or when the run
 * took over an expired lease or acquired the lock after a force-release, a failure keeps the lock:
 * the rows may be half-written, so commits stay refused until a rebuild completes.
 */
export class ProjectionRebuild {
  readonly #repository: Repository & EventLog & RebuildFence
  readonly #projectors: readonly CommitProjector[]
  readonly #scopes: readonly Scope[]
  readonly #onProgress: ((progress: RebuildProgress) => void) | undefined
  readonly #journal: Journal | null

  constructor(repository: Repository & EventLog & RebuildFence, options: ProjectionRebuildOptions) {
    this.#repository = repository
    this.#projectors = [...options.projectors]
    this.#scopes = options.aggregates.map((entry) =>
      'aggregate' in entry
        ? {aggregate: entry.aggregate, entityType: entry.entityType}
        : {aggregate: entry, entityType: entry.type},
    )
    this.#onProgress = options.onProgress
    this.#journal = options.journal ?? null
  }

  /**
   * Takes the rebuild lock as `owner` for a lease of `leaseMs`, rebuilds, and releases the lock,
   * which settles the generation. The lease is renewed for another `leaseMs` whenever half of it,
   * less a second, has passed: checked after each type's events are read, every 1024 replayed
   * commits, after each entity, and before each batch.
   *
   * A configuration error is an `invalidArgument` error, before the lock is taken. A lock another
   * rebuild holds is a `RebuildLockHeldError`. A begin whose result is unknown is begun again once
   * it can no longer apply, and is otherwise a `RebuildBeginIndeterminateError`, which a caller
   * whose owner token is its own settles with `holdsRebuildBegun` and ends. Any other failure is
   * the rebuild's own error, after the release rule in the class description.
   */
  async run(owner: string, leaseMs: number, options: CallOptions = {}): Promise<RebuildReport> {
    this.#check()

    const held = await takeRebuildLock(this.#repository, owner, leaseMs, options)
    let result: RebuildRun
    try {
      result = await this.#fenced(held.lease, leaseMs, held.taken, options)
    } catch (error) {
      return failRebuildLock(this.#repository, held, error as RebuildFailure)
    }

    // Every row is written: settling the generation runs even after an abort, so a finished
    // rebuild never leaves the store locked.
    await this.#repository.endRebuild(result.lease)

    return result.report
  }

  /**
   * Rebuilds under a lease the caller took with `beginRebuild` and will end, so one lease can
   * cover this rebuild and other work. The lease is renewed for `renewMs` (at least a second) when
   * the run starts, and again whenever half of it, less a second, has passed. It returns the
   * report and the renewed lease, which the caller ends.
   *
   * A failure is a {@link RebuildFailure} with the error, the guarded batches attempted and the
   * last lease. Release the lock only when no write was attempted under the lease (this run's or
   * your own) and your begin acquired it from a settled generation (`acquiredFromSettled`);
   * otherwise keep it until a rebuild completes.
   */
  async runWithLease(
    lease: RebuildLease,
    renewMs: number,
    options: CallOptions = {},
  ): Promise<RebuildRun> {
    try {
      this.#check()
      checkRenew(renewMs)
    } catch (error) {
      throw new RebuildFailure(error, 0, 0, lease)
    }

    return this.#fenced(lease, renewMs, null, options)
  }

  /**
   * A dry run: the operations a {@link run} would write, in order, its report and its batches,
   * with the same checks, without taking the lock or writing anything. Live commits may land
   * while it reads.
   */
  async plan(options: CallOptions = {}): Promise<RebuildPlan> {
    this.#check()

    const output = new Output(null, undefined)
    await this.#rebuild(output, options)

    return {
      report: {...output.report},
      operations: output.planned,
      batches: rebuildBatches(output.planned).length,
    }
  }

  // Refuses a configuration the rebuild can't run, before it reads or locks anything.
  #check(): void {
    const repository: object = this.#repository
    if (!isEventLog(repository as Repository) || !isRebuildFence(repository)) {
      throw invalid('a projection rebuild needs a repository that enumerates its log and fences')
    }
    const journal = this.#journal !== null
    if (this.#projectors.length === 0 && !journal) {
      throw invalid('a projection rebuild needs at least one commit-wide projector or a journal')
    }
    const engine = this.#projectors.find(isEngineProjector)
    if (engine !== undefined) {
      throw invalid(
        `projector ${JSON.stringify(engine.name)} reads engine metadata (the journal's); ` +
          'rebuild the journal with the journal option',
      )
    }
    if (this.#scopes.length === 0 && this.#projectors.length > 0) {
      throw invalid('a projection rebuild needs at least one aggregate type')
    }
    const seen = new Set<string>()
    for (const {entityType} of this.#scopes) {
      if (seen.has(entityType)) {
        throw invalid(`entity type ${JSON.stringify(entityType)} is registered twice`)
      }
      seen.add(entityType)
    }
    for (const projector of this.#projectors) {
      for (const entityType of projector.history ?? []) {
        if (!seen.has(entityType)) {
          throw invalid(
            `history-dependent projector ${JSON.stringify(projector.name)} reads entity type ` +
              `${JSON.stringify(entityType)}, which is not registered: register its aggregate ` +
              'so the replay can fold its state',
          )
        }
      }
    }
  }

  #scope(entityType: string): Scope {
    return this.#scopes.find((scope) => scope.entityType === entityType)!
  }

  // Rebuilds under `lease`, last taken or renewed at `renewed` (null renews it first). Any failure
  // is a RebuildFailure saying how far the run got.
  async #fenced(
    lease: RebuildLease,
    renewMs: number,
    renewed: number | null,
    options: CallOptions,
  ): Promise<RebuildRun> {
    const fence = new Renewing(this.#repository, lease, renewMs, renewed, options)
    const output = new Output(fence, this.#onProgress)
    try {
      await output.keepAlive()
      await this.#rebuild(output, options)
      await output.write()
    } catch (error) {
      throw new RebuildFailure(error, fence.attempted, output.batches, fence.lease)
    }

    return {report: {...output.report}, lease: fence.lease}
  }

  // Plans every operation: the current-state rows by aggregate registration order, entity ID
  // bytes, projector and emission order, then the history rows by (pk, sk) bytes.
  async #rebuild(output: Output, options: CallOptions): Promise<void> {
    const current: Indexed[] = []
    const history: Indexed[] = []
    this.#projectors.forEach((projector, index) => {
      const entry = {index, projector}
      if ((projector.history ?? []).length === 0) {
        current.push(entry)
      } else {
        history.push(entry)
      }
    })
    const historyTypes = [...new Set(history.flatMap((entry) => entry.projector.history ?? []))]
    historyTypes.sort(compareUtf8)

    // Each step needs the one before it, and renews the lease between them.
    /* oxlint-disable no-await-in-loop */
    // Every event the history replay and the journal read, each type read once. The history
    // replay's types must run from sequence 1 to their heads; the journal's rows are a function of
    // the stored allocations, whatever was compacted.
    const events = new Map<string, SerializedEvent[]>()
    for (const entityType of historyTypes) {
      events.set(entityType, await this.#wholeHistory(entityType, options))
      await output.keepAlive()
    }
    for (const entityType of this.#journal?.entityTypes ?? []) {
      if (!events.has(entityType)) {
        events.set(entityType, await this.#allEvents(entityType, options))
        await output.keepAlive()
      }
    }
    output.report.events = [...events.values()].reduce((sum, each) => sum + each.length, 0)

    const journal =
      this.#journal === null
        ? null
        : replayJournal(this.#journal.entityTypes.flatMap((type) => events.get(type) ?? []))
    const replayed =
      history.length === 0
        ? null
        : await this.#replayHistory(
            history,
            historyTypes.flatMap((type) => events.get(type) ?? []),
            output,
          )
    events.clear()

    if (current.length > 0) {
      const store = new Store(this.#repository)
      for (const {aggregate, entityType} of this.#scopes) {
        const entities = store.streamEntitiesFromSnapshots(aggregate, {entityType}, options)
        for await (const entity of entities) {
          if (!entity.ok) {
            throw entity.error
          }
          const state = aggregate.state.encode(entity.loaded.state)
          const view: CommitView = {
            commitId: '',
            streams: [commitStream(entity.entityId, entityType, [], state)],
          }
          for (const item of projectWith(current, view)) {
            output.plan(unconditioned(item))
          }
          output.report.entities++
          await output.keepAlive()
        }
      }
    }
    /* oxlint-enable no-await-in-loop */

    if (replayed !== null) {
      output.report.commits += replayed.count
      for (const item of replayed.replay.intoLast()) {
        output.plan(unconditioned(item))
      }
    }

    if (journal !== null) {
      output.report.journalRows = journal.rows.length
      output.report.journalCounters = journal.counters.length
      for (const item of journalOperations(journal)) {
        output.plan(item)
      }
    }
  }

  // Every stored event of one type, in no particular order, for the journal.
  async #allEvents(entityType: string, options: CallOptions): Promise<SerializedEvent[]> {
    const events: SerializedEvent[] = []
    for await (const batch of this.#repository.streamAllEvents({entityType}, options)) {
      events.push(...batch)
    }

    return events
  }

  // Replays every commit of `events`, the history-dependent projectors' types, folding each
  // stream's state after each commit, and returns the rows the replay leaves and the commits.
  async #replayHistory(
    projectors: readonly Indexed[],
    events: readonly SerializedEvent[],
    output: Output,
  ): Promise<{replay: ViewReplay; count: number}> {
    const commits = groupCommits(events)
    const folds = new Map<string, {scope: Scope; state: unknown; sequence: bigint}>()
    const replay = new ViewReplay()

    // The lease is renewed as the replay goes, every 1024 commits.
    /* oxlint-disable no-await-in-loop */
    for (const [index, commit] of commits.entries()) {
      if (index % 1024 === 1023) {
        await output.keepAlive()
      }

      const streams = commit.streams.map((stream) => {
        let fold = folds.get(stream.entityId)
        if (fold === undefined) {
          const scope = this.#scope(stream.entityType)
          fold = {scope, state: scope.aggregate.initial(), sequence: 0n}
          folds.set(stream.entityId, fold)
        }
        const {aggregate} = fold.scope
        for (const event of stream.events) {
          if (event.sequence !== fold.sequence + 1n) {
            throw new SequenceGapError(stream.entityId, fold.sequence, event.sequence)
          }
          fold.state = aggregate.apply(fold.state, replayEvent(aggregate.events, event))
          fold.sequence = event.sequence
        }

        return {...stream, state: aggregate.state.encode(fold.state)}
      })

      for (const item of projectWith(projectors, {commitId: commit.commitId, streams})) {
        replay.apply(item)
      }
    }
    /* oxlint-enable no-await-in-loop */

    return {replay, count: commits.length}
  }

  // Every event of every stream of one type, each stream from sequence 1 to its enumerated head,
  // in entity ID byte order. A stream whose early events were compacted (or expired), or whose
  // tail expired while an earlier event and a snapshot remain, is a CompactionUncoveredError:
  // `through` is the last missing sequence before the first event, or the head for a missing
  // tail. An event above the head is drift; a gap in between fails the fold.
  async #wholeHistory(entityType: string, options: CallOptions): Promise<SerializedEvent[]> {
    const repository = this.#repository
    const heads = await repository.entityHeads(entityType, options)

    const streams = new Map<string, SerializedEvent[]>()
    for await (const batch of repository.streamAllEvents({entityType}, options)) {
      for (const event of batch) {
        const stream = streams.get(event.entityId)
        if (stream === undefined) {
          streams.set(event.entityId, [event])
        } else {
          stream.push(event)
        }
      }
    }
    for (const stream of streams.values()) {
      stream.sort(bySequence)
    }

    const ids = [...heads.keys()]
    ids.sort(compareUtf8)
    let uncovered: {entityId: string; through: bigint} | null = null
    for (const entityId of ids) {
      const head = heads.get(entityId)!
      const stream = streams.get(entityId) ?? []
      const first = stream[0]?.sequence
      const last = stream.at(-1)?.sequence
      if (first !== undefined && first !== 1n) {
        uncovered = {entityId, through: first - 1n}
      } else if (last !== undefined && last > head) {
        throw new StreamHeadDriftError(entityId, head > 0n ? head : null, last)
      } else if (last === head) {
        continue
      } else {
        uncovered = {entityId, through: head}
      }
      break
    }
    if (uncovered !== null) {
      const snapshot = await repository.snapshot(uncovered.entityId, options)
      throw new CompactionUncoveredError(
        uncovered.entityId,
        uncovered.through,
        snapshot?.eventSequence ?? null,
      )
    }

    const all = [...streams.keys()]
    all.sort(compareUtf8)

    return all.flatMap((entityId) => streams.get(entityId)!)
  }
}

/**
 * Takes the rebuild lock as `owner`, reading the generation just before the begin to tell an
 * acquire from a settled generation from a takeover or an acquire after a force-release. A begin
 * whose result is unknown is begun again only once a fresh read shows it can no longer apply; every
 * other outcome, a failed read and a begin the read shows applied included, is thrown as the
 * indeterminate error, since the owner token may be shared and only the caller knows. Internal: a
 * projection rebuild and the journal's rebuild share it.
 */
export async function takeRebuildLock(
  repository: RebuildFence,
  owner: string,
  leaseMs: number,
  options: CallOptions,
): Promise<{lease: RebuildLease; fromSettled: boolean; taken: number}> {
  // Each round reads, then begins, after the one before it lost.
  /* oxlint-disable no-await-in-loop */
  for (let round = 0; round < MAX_REBUILD_BEGIN_ATTEMPTS; round++) {
    const before = await repository.rebuildGeneration(options)
    let lease: RebuildLease
    try {
      lease = await repository.beginRebuild(owner, leaseMs, options)
    } catch (error) {
      if (error instanceof RebuildBeginIndeterminateError) {
        const held = await repository.rebuildState(options).then(
          (state) => holdsRebuildBegun(error.lease, error.observed, state),
          () => null,
        )
        if (held === false) {
          continue
        }
      }
      throw error
    }

    return {lease, fromSettled: acquiredFromSettled(lease, before), taken: Date.now()}
  }
  /* oxlint-enable no-await-in-loop */

  throw new RebuildConflictError(MAX_REBUILD_BEGIN_ATTEMPTS)
}

/**
 * Ends a run that failed under a lock {@link takeRebuildLock} took, and throws the run's error. The
 * lock is released only when it was acquired from a settled generation and no write was attempted:
 * the rows were complete when the run began, so settling the generation is safe. Otherwise it is
 * kept, since the rows may be half-written. The release runs even after an abort, as cleanup, and a
 * failed release (a lost lease, say) leaves the lock to lapse: the run's own error is the one to
 * report. Internal: a projection rebuild and the journal's rebuild share it.
 */
export async function failRebuildLock(
  repository: RebuildFence,
  held: {readonly fromSettled: boolean},
  failure: RebuildFailure,
): Promise<never> {
  if (held.fromSettled && failure.attempted === 0) {
    await repository.endRebuild(failure.lease).catch(() => {})
  }

  throw failure.error
}

// A rebuild's planned operations, checked across the whole run, then written in guarded batches
// under the lease, or only collected for a dry run.
class Output {
  readonly planned: ProjectedView[] = []
  readonly report: Counts = {
    entities: 0,
    commits: 0,
    puts: 0,
    deletes: 0,
    batches: 0,
    events: 0,
    journalRows: 0,
    journalCounters: 0,
  }
  // The batches the plan splits into, once it's complete.
  batches = 0
  readonly #fence: Renewing | null
  readonly #onProgress: ((progress: RebuildProgress) => void) | undefined
  // Each planned key's operation.
  readonly #keys = new Map<string, ProjectedView>()

  constructor(
    fence: Renewing | null,
    onProgress: ((progress: RebuildProgress) => void) | undefined,
  ) {
    this.#fence = fence
    this.#onProgress = onProgress
  }

  // Plans one operation, refusing what no batch could write: an operation over the per-item byte
  // limit, or a key the run already planned (another owner's, or the same owner's again).
  plan(item: ProjectedView): void {
    const {pk, sk, owner} = item.op
    const size = viewOperationSize(item.op)
    if (size > MAX_COMMIT_ITEM_BYTES || size + REBUILD_GUARD_SIZE > MAX_COMMIT_BYTES) {
      throw new CommitTooLargeError(2, size + REBUILD_GUARD_SIZE, size)
    }

    // A key no projector can make, since checkViews refuses the separator in both parts.
    const key = `${pk}\0${sk}`
    const first = this.#keys.get(key)
    if (first !== undefined) {
      throw first.op.owner === owner
        ? new DuplicateViewKeyError(pk, sk, first, item)
        : new SharedViewKeyError(pk, sk, first.op.owner, owner)
    }
    this.#keys.set(key, item)

    if (item.op.kind === 'put') {
      this.report.puts++
    } else {
      this.report.deletes++
    }
    this.planned.push(item)
  }

  // Renews the lease when it's due; a dry run has none.
  async keepAlive(): Promise<void> {
    await this.#fence?.keepAlive()
  }

  // Writes the planned operations in guarded batches.
  async write(): Promise<void> {
    const batches = rebuildBatches(this.planned)
    this.batches = batches.length
    const fence = this.#fence
    if (fence === null) {
      return
    }

    this.#onProgress?.({kind: 'planned', report: {...this.report}, batches: batches.length})
    // Each batch is written after the one before it, under the lease it renewed.
    /* oxlint-disable no-await-in-loop */
    for (const batch of batches) {
      await fence.write(batch)
      this.report.batches++
      this.#onProgress?.({kind: 'wrote', batch: this.report.batches, of: batches.length})
    }
    /* oxlint-enable no-await-in-loop */
  }
}

/** A lease a rebuild renews as it goes, and the guarded batches it attempted under it. Internal. */
export class Renewing {
  lease: RebuildLease
  // The batches attempted, a failed one included: it may have applied.
  attempted = 0
  readonly #fence: RebuildFence
  readonly #renewMs: number
  readonly #options: CallOptions
  // When the lease was taken or last renewed (Date.now), or null to renew at the next check.
  #renewed: number | null

  constructor(
    fence: RebuildFence,
    lease: RebuildLease,
    renewMs: number,
    renewed: number | null,
    options: CallOptions,
  ) {
    this.#fence = fence
    this.lease = lease
    this.#renewMs = renewMs
    this.#renewed = renewed
    this.#options = options
  }

  // Renews the lease for another `renewMs` once half of it, less a second, has passed since it
  // was taken or last renewed, at once when that's unknown (or the clock went back). The second
  // covers the whole-second expiry: a lease taken late in a second lapses up to a second early.
  async keepAlive(): Promise<void> {
    // Planning reads and folds without I/O between some checkpoints, so an abort is seen here too.
    this.#options?.signal?.throwIfAborted()
    const due = Math.max(this.#renewMs / 2 - 1000, 0)
    const now = Date.now()
    const renewed = this.#renewed
    if (renewed === null || now < renewed || now - renewed >= due) {
      this.lease = await this.#fence.renewRebuild(this.lease, this.#renewMs, this.#options)
      this.#renewed = Date.now()
    }
  }

  // Writes one guarded batch, renewing the lease first if it's due.
  async write(batch: readonly ProjectedView[]): Promise<void> {
    await this.keepAlive()

    this.attempted++
    await this.#fence.writeRebuildViews(this.lease, batch, this.#options)
  }
}

// The operation a rebuild writes: the same row without its condition, mapped error or expected
// payload, since a replay applies what the live commits wrote rather than evaluating them again.
// The owner condition still applies when it's written.
function unconditioned(item: ProjectedView): ProjectedView {
  const {kind, pk, sk, owner, entityType, payload} = item.op
  const op =
    kind === 'put' ? putView(pk, sk, owner, entityType, payload) : deleteView(pk, sk, owner)

  return {...item, op}
}

// Splits rebuild operations into guarded batches in order: each at most 99 operations whose sizes,
// with the lease guard, fit the commit byte budget. An operation too large for any batch gets one
// of its own, which the write refuses (the plan already refused it).
export function rebuildBatches(views: readonly ProjectedView[]): ProjectedView[][] {
  const batches: ProjectedView[][] = []
  let start = 0
  while (start < views.length) {
    let end = start
    let bytes = REBUILD_GUARD_SIZE
    while (end < views.length && end - start < MAX_REBUILD_VIEW_BATCH) {
      const size = viewOperationSize(views[end]!.op)
      if (end > start && bytes + size > MAX_COMMIT_BYTES) {
        break
      }
      bytes += size
      end++
    }
    batches.push(views.slice(start, end))
    start = end
  }

  return batches
}

// Refuses a renewal under one second, before anything is read.
export function checkRenew(renewMs: number): void {
  if (!Number.isSafeInteger(renewMs) || renewMs < 1000) {
    throw invalid('a rebuild lease is renewed for at least one second, in whole milliseconds')
  }
}

function bySequence(a: SerializedEvent, b: SerializedEvent): number {
  return a.sequence < b.sequence ? -1 : a.sequence > b.sequence ? 1 : 0
}

function invalid(message: string): EvtError {
  return new EvtError('invalidArgument', message)
}
