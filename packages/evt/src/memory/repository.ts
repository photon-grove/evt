// The in-memory repository. It enforces the storage contract the way the live backends do: a head
// per stream that every commit raises, optimistic concurrency against it, the feature marker and
// the rebuild-lock check, and the same failure classification and transaction budget. It's for
// tests and local development.

import {
  ConflictError,
  CompactionUncoveredError,
  EvtError,
  FenceConflictError,
  HistoryDependentProjectionError,
  RebuildInProgressError,
  SequenceGapError,
  StreamHeadDriftError,
  StreamHeadsNotMigratedError,
} from '../errors.js'
import type {SerializedEvent, SerializedSnapshot} from '../event.js'
import {
  type FeatureManifest,
  checkSupported,
  checkWritable,
  declaresStreamHeads,
  entityTypeSet,
  withStreamHeads,
} from '../features.js'
import {int64Argument} from '../fields.js'
import type {Metadata} from '../metadata.js'
import {
  type CallOptions,
  type Commit,
  type NormalizedTransaction,
  type ReadFence,
  type StoreInitializer,
  type TransactionCommit,
  type TransactionalRepository,
  checkBudget,
  checkFenceRetention,
  checkNotReserved,
  normalizeTransaction,
  rankFailures,
  transactionSizes,
  validateCommit,
} from '../repository.js'

interface Stream {
  /** The stream's events by sequence. Compaction deletes from it. */
  events: Map<bigint, SerializedEvent>
  snapshot: SerializedSnapshot | null
  /** The highest event sequence ever committed to the stream, 0 while it has none. */
  head: bigint
}

/** The state of the rebuild fence's two rows that a commit's lock check reads. */
export interface RebuildState {
  /** The generation counter: odd while a rebuild holds the store or after a force-release. */
  generation: bigint
  /**
   * The lock row: a rebuild's lease, the item a force-release leaves in its place, or none. A
   * commit is refused while either exists, and while the generation is odd.
   */
  lock: 'held' | 'forceReleased' | null
}

// The store-wide control rows: the feature marker and the rebuild fence.
interface Controls {
  manifest: FeatureManifest | null
  rebuild: RebuildState
}

// Read a repository's private state, for the fixtures. The class's static block sets them.
let streamsOf: (repo: MemRepository) => Map<string, Stream>
let controlsOf: (repo: MemRepository) => Controls

/**
 * An in-memory {@link Repository}. Every call copies what it stores and what it returns, so a
 * caller that changes an event after committing it, or one it read, changes nothing stored.
 *
 * Each call runs to completion before another starts, so concurrent commits to one stream behave
 * like racing writers on a live backend: one wins and the rest fail with a conflict.
 *
 * A fresh store is initialized by its first commit or snapshot write, which declares stream-heads
 * on its feature manifest, so {@link initialize} is needed only where code treats every backend
 * alike.
 */
export class MemRepository implements TransactionalRepository, StoreInitializer {
  readonly #streams = new Map<string, Stream>()
  readonly #controls: Controls = {manifest: null, rebuild: {generation: 0n, lock: null}}

  async initialize(options?: CallOptions): Promise<boolean> {
    options?.signal?.throwIfAborted()
    checkSupported(this.#controls.manifest)
    if (declaresStreamHeads(this.#controls.manifest)) {
      return false
    }

    this.#declareStreamHeads()

    return true
  }

  async commit(commit: Commit, options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    validateCommit(commit)
    checkNotReserved(commit.entityId)
    if (commit.events.length > 0) {
      this.#write(normalizeTransaction({streams: [commit]}))
    }
  }

  async commitTransaction(transaction: TransactionCommit, options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    const normalized = normalizeTransaction(transaction)
    if (normalized.streams.length > 0 || normalized.fences.length > 0) {
      this.#write(normalized)
    }
  }

  // Writes a transaction, or refuses it with nothing written, in the order the live backends
  // check: the budget, the marker and the fences' retention, the rebuild lock, then every stream
  // and fence, ranked.
  #write(transaction: NormalizedTransaction): void {
    // Every backend budgets a commit the same way, so an oversized one fails before any write.
    checkBudget(transactionSizes(transaction, () => undefined))

    // The control rows come first, as the two checks that open every live commit: the marker,
    // then the rebuild lock, which an unsettled generation fails, a force-released one included.
    this.#checkManifest()
    const retention = this.#controls.manifest?.retention ?? []
    checkFenceRetention(transaction, (entityType) => retention.includes(entityType))
    const {rebuild} = this.#controls
    if (rebuild.lock !== null || rebuild.generation % 2n !== 0n) {
      throw new RebuildInProgressError()
    }

    const failures: EvtError[] = []
    for (const stream of transaction.streams) {
      const failure = checkWrite(stream, this.#streams.get(stream.entityId) ?? emptyStream())
      if (failure !== null) {
        failures.push(failure)
      }
    }
    for (const fence of transaction.fences) {
      const failure = checkFence(fence, this.#streams.get(fence.entityId) ?? emptyStream())
      if (failure !== null) {
        failures.push(failure)
      }
    }
    const failure = rankFailures(failures)
    if (failure !== undefined) {
      throw failure
    }

    // Copy everything before the first write, so a copy that throws leaves every stream as it was.
    const writes = transaction.streams.map((commit) => {
      const events = commit.events.map(cloneEvent)
      const last = events.at(-1)!.sequence
      const snapshot = commit.snapshot && {
        entityType: commit.snapshot.entityType,
        entityId: commit.entityId,
        sequence: commit.snapshot.sequence,
        eventSequence: last,
        payload: copy(commit.snapshot.payload),
      }

      return {entityId: commit.entityId, events, last, snapshot}
    })

    for (const {entityId, events, last, snapshot} of writes) {
      const held = this.#streams.get(entityId) ?? emptyStream()
      for (const event of events) {
        held.events.set(event.sequence, event)
      }
      held.head = held.head > last ? held.head : last
      if (snapshot) {
        held.snapshot = snapshot
      }
      this.#streams.set(entityId, held)
    }
  }

  async events(entityId: string, options?: CallOptions): Promise<SerializedEvent[]> {
    return this.eventsAfter(entityId, 0n, options)
  }

  async eventsAfter(
    entityId: string,
    after: bigint,
    options?: CallOptions,
  ): Promise<SerializedEvent[]> {
    options?.signal?.throwIfAborted()
    int64Argument(after, 'after')

    return this.#sorted(entityId)
      .filter((event) => event.sequence > after)
      .map(cloneEvent)
  }

  async latest(entityId: string, n: number, options?: CallOptions): Promise<SerializedEvent[]> {
    options?.signal?.throwIfAborted()
    if (!Number.isSafeInteger(n) || n < 1) {
      throw new EvtError(
        'invalidArgument',
        `latest(n) requires a whole number of at least one event: got n = ${n}`,
      )
    }

    return this.#sorted(entityId, true).slice(0, n).map(cloneEvent)
  }

  async snapshot(entityId: string, options?: CallOptions): Promise<SerializedSnapshot | null> {
    options?.signal?.throwIfAborted()
    const snapshot = this.#streams.get(entityId)?.snapshot

    return snapshot ? cloneSnapshot(snapshot) : null
  }

  async putSnapshot(snapshot: SerializedSnapshot, options?: CallOptions): Promise<boolean> {
    options?.signal?.throwIfAborted()
    checkNotReserved(snapshot.entityId)
    int64Argument(snapshot.sequence, 'snapshot.sequence')
    int64Argument(snapshot.eventSequence, 'snapshot.eventSequence')
    this.#checkManifest()

    // The stream's head must cover the snapshot. One of an empty stream (event sequence 0) may
    // have no head at all.
    const held = this.#streams.get(snapshot.entityId)
    const head = held?.head ?? 0n
    if (snapshot.eventSequence > 0n && head < snapshot.eventSequence) {
      const derived = held ? derivedHead(held) : 0n
      throw drift(
        snapshot.entityId,
        head,
        derived > snapshot.eventSequence ? derived : snapshot.eventSequence,
      )
    }

    const stored = held?.snapshot
    if (stored && stored.eventSequence > snapshot.eventSequence) {
      return false
    }

    const stream = held ?? emptyStream()
    stream.snapshot = cloneSnapshot(snapshot)
    this.#streams.set(snapshot.entityId, stream)

    return true
  }

  async compactBelow(entityId: string, through: bigint, options?: CallOptions): Promise<number> {
    options?.signal?.throwIfAborted()
    checkNotReserved(entityId)
    if (int64Argument(through, 'through') < 1n) {
      return 0
    }

    // Compaction validates the manifest, but never declares stream-heads itself.
    const {manifest} = this.#controls
    checkWritable(manifest)

    const stream = this.#streams.get(entityId)
    const snapshot = stream?.snapshot
    const covered = snapshot?.eventSequence ?? null
    if (stream === undefined || !snapshot || covered === null || covered < through) {
      throw new CompactionUncoveredError(entityId, through, covered)
    }

    // A history-dependent type's rebuild replays the events compaction would delete.
    if (manifest?.historyDependent.includes(snapshot.entityType)) {
      throw new HistoryDependentProjectionError(snapshot.entityType, entityId)
    }

    let deleted = 0
    for (const sequence of stream.events.keys()) {
      if (sequence <= through) {
        stream.events.delete(sequence)
        deleted++
      }
    }

    return deleted
  }

  static {
    streamsOf = (repo) => repo.#streams
    controlsOf = (repo) => repo.#controls
  }

  // Refuses a write to a store whose manifest declares an unsupported pair, and declares
  // stream-heads on a store without it, as its first writer, when the store holds no data.
  #checkManifest(): void {
    checkSupported(this.#controls.manifest)
    if (!declaresStreamHeads(this.#controls.manifest)) {
      this.#declareStreamHeads()
    }
  }

  #declareStreamHeads(): void {
    const holdsData = Array.from(this.#streams.values()).some(
      (stream) => stream.events.size > 0 || stream.snapshot !== null,
    )
    if (holdsData) {
      throw new StreamHeadsNotMigratedError()
    }

    this.#controls.manifest = withStreamHeads(this.#controls.manifest)
  }

  // The stream's events by sequence, ascending or descending. Sequences are unique.
  #sorted(entityId: string, descending = false): SerializedEvent[] {
    const events = Array.from(this.#streams.get(entityId)?.events.values() ?? [])
    const order = descending ? -1 : 1
    events.sort((a, b) => (a.sequence < b.sequence ? -order : order))

    return events
  }
}

// The failure of a commit the stored stream doesn't accept, as the live backends' conditions find
// it, or null. The head update needs no head for a create and the head to be the sequence before
// the first event for an append. Then no event may exist already, and a snapshot must follow the
// stored one.
function checkWrite(commit: Commit, held: Stream): EvtError | null {
  const first = commit.events[0]!.sequence
  const headPasses = first <= 1n ? held.head === 0n : held.head === first - 1n
  if (!headPasses) {
    return probe(commit.entityId, held, first)
  }

  // The head update passed and an event put would fail: an event sits above the head.
  if (commit.events.some((event) => held.events.has(event.sequence))) {
    return drift(commit.entityId, held.head, derivedHead(held))
  }

  const snapshot = commit.snapshot
  const stored = held.snapshot?.sequence ?? 0n
  if (snapshot && snapshot.sequence !== stored + 1n) {
    return new ConflictError(
      commit.entityId,
      `snapshot sequence moved: stored ${stored}, commit writes ${snapshot.sequence}`,
    )
  }

  return null
}

// The failure of a fence whose stream's head moved, or null: drift when the head disagrees with
// the log, and otherwise a fence conflict.
function checkFence(fence: ReadFence, held: Stream): EvtError | null {
  if (held.head === fence.head) {
    return null
  }

  const derived = derivedHead(held)
  if (held.head !== derived) {
    return drift(fence.entityId, held.head, derived)
  }

  return new FenceConflictError(fence.entityId, fence.head, fence.provenance)
}

// Classifies a failed head update the way the live backends' failure-path probe does: a stored
// head that disagrees with the head derived from the log is drift. Memory has no concurrent
// writes, so a second derivation would find the same head. A healthy stream fails with a gap when
// the commit skips past its head, and with a conflict otherwise.
function probe(entityId: string, held: Stream, first: bigint): EvtError {
  const derived = derivedHead(held)
  if (held.head !== derived) {
    return drift(entityId, held.head, derived)
  }
  if (held.head < first - 1n) {
    return new SequenceGapError(entityId, held.head, first)
  }

  return new ConflictError(
    entityId,
    'conditional check failed: the sequence or snapshot changed since it was read',
  )
}

// The head a stream's log implies: the higher of its last event and its snapshot's event sequence.
function derivedHead(stream: Stream): bigint {
  let head = stream.snapshot?.eventSequence ?? 0n
  for (const sequence of stream.events.keys()) {
    if (sequence > head) {
      head = sequence
    }
  }

  return head
}

// A stored head of 0 is an absent head.
function drift(entityId: string, stored: bigint, derived: bigint): StreamHeadDriftError {
  return new StreamHeadDriftError(entityId, stored > 0n ? stored : null, derived)
}

/** What a conformance suite needs to make a stream drift, which no repository call can do. */
export interface HeadFixtures {
  /** Appends an event without raising its stream's head, as a pre-heads writer would. */
  appendBypassingHead(event: SerializedEvent): void
  /** Deletes a stream's head, leaving its events and snapshot. */
  deleteHead(entityId: string): void
  /** A stream's stored head, or null when it has none. */
  storedHead(entityId: string): bigint | null
}

/**
 * Hooks that corrupt a repository's heads, for conformance suites only: a stream they touch
 * drifts, and every later write to it fails.
 */
export function headFixtures(repo: MemRepository): HeadFixtures {
  const streams = streamsOf(repo)

  return {
    appendBypassingHead(event) {
      const stream = streams.get(event.entityId) ?? emptyStream()
      stream.events.set(event.sequence, cloneEvent(event))
      streams.set(event.entityId, stream)
    },
    deleteHead(entityId) {
      const stream = streams.get(entityId)
      if (stream) {
        stream.head = 0n
      }
    },
    storedHead(entityId) {
      const head = streams.get(entityId)?.head ?? 0n

      return head > 0n ? head : null
    },
  }
}

/** A feature manifest's declarable parts, which a declaration changes in place. */
export interface ManifestDraft {
  features: Map<string, number>
  historyDependent: string[]
  retention: string[]
}

/** What a conformance suite needs to declare features and hold the rebuild lock. */
export interface ControlFixtures {
  /** The stored manifest, or null when the store has no marker row. */
  manifest(): FeatureManifest | null
  /** Changes the manifest in one declaration, bumping its version by one, and returns it. */
  declare(change: (draft: ManifestDraft) => void): FeatureManifest
  /** Sets the rebuild fence's rows, as a rebuild's lifecycle would. */
  setRebuild(state: RebuildState): void
}

/**
 * Hooks that change a repository's control rows, for conformance suites only. A declaration may
 * declare features this package doesn't model, which then refuse every write.
 */
export function controlFixtures(repo: MemRepository): ControlFixtures {
  const controls = controlsOf(repo)

  return {
    manifest() {
      return controls.manifest && cloneManifest(controls.manifest)
    },
    declare(change) {
      const draft: ManifestDraft = {
        features: new Map(controls.manifest?.features),
        historyDependent: [...(controls.manifest?.historyDependent ?? [])],
        retention: [...(controls.manifest?.retention ?? [])],
      }
      change(draft)

      // Stored as the marker row stores them: each set sorted and deduplicated.
      controls.manifest = {
        version: (controls.manifest?.version ?? 0n) + 1n,
        features: new Map(draft.features),
        historyDependent: entityTypeSet(draft.historyDependent),
        retention: entityTypeSet(draft.retention),
      }

      return cloneManifest(controls.manifest)
    },
    setRebuild(state) {
      int64Argument(state.generation, 'generation')
      controls.rebuild = {...state}
    },
  }
}

function cloneManifest(manifest: FeatureManifest): FeatureManifest {
  return {
    version: manifest.version,
    features: new Map(manifest.features),
    historyDependent: [...manifest.historyDependent],
    retention: [...manifest.retention],
  }
}

function emptyStream(): Stream {
  return {events: new Map(), snapshot: null, head: 0n}
}

function cloneEvent(event: SerializedEvent): SerializedEvent {
  return {
    ...event,
    payload: event.payload && copy(event.payload),
    metadata: cloneMetadata(event.metadata),
  }
}

function cloneMetadata(metadata: Metadata): Metadata {
  return {
    ...metadata,
    trace: metadata.trace && new Map(metadata.trace),
    origin: metadata.origin && {...metadata.origin},
    extensions: new Map(metadata.extensions),
  }
}

function cloneSnapshot(snapshot: SerializedSnapshot): SerializedSnapshot {
  return {...snapshot, payload: snapshot.payload && copy(snapshot.payload)}
}

// A copy of the bytes. Not slice(): a Node Buffer's slice is a view of the same memory.
function copy(bytes: Uint8Array): Uint8Array {
  return new Uint8Array(bytes)
}
