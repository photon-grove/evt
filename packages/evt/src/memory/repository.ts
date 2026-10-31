// The in-memory repository. It enforces the storage contract the way the live backends do: a head
// per stream that every commit raises, optimistic concurrency against it, and the same failure
// classification and transaction budget. It's for tests and local development.

import {
  ConflictError,
  CompactionUncoveredError,
  EvtError,
  SequenceGapError,
  StreamHeadDriftError,
} from '../errors.js'
import type {SerializedEvent, SerializedSnapshot} from '../event.js'
import {int64Argument} from '../fields.js'
import type {Metadata} from '../metadata.js'
import {
  type CallOptions,
  type Commit,
  type Repository,
  checkBudget,
  checkNotReserved,
  commitSizes,
  validateCommit,
} from '../repository.js'

interface Stream {
  /** The stream's events by sequence. Compaction deletes from it. */
  events: Map<bigint, SerializedEvent>
  snapshot: SerializedSnapshot | null
  /** The highest event sequence ever committed to the stream, 0 while it has none. */
  head: bigint
}

// Reads a repository's streams, for headFixtures. The class's static block sets it.
let streamsOf: (repo: MemRepository) => Map<string, Stream>

/**
 * An in-memory {@link Repository}. Every call copies what it stores and what it returns, so a
 * caller that changes an event after committing it, or one it read, changes nothing stored.
 *
 * Each call runs to completion before another starts, so concurrent commits to one stream behave
 * like racing writers on a live backend: one wins and the rest fail with a conflict.
 */
export class MemRepository implements Repository {
  readonly #streams = new Map<string, Stream>()

  async commit(commit: Commit, options?: CallOptions): Promise<void> {
    options?.signal?.throwIfAborted()
    validateCommit(commit)
    checkNotReserved(commit.entityId)
    if (commit.events.length === 0) {
      return
    }

    // Every backend budgets a commit the same way, so an oversized one fails before any write.
    checkBudget(commitSizes(commit, () => undefined))

    const held = this.#streams.get(commit.entityId) ?? emptyStream()
    checkWrite(commit, held)

    // Copy everything before the first write, so a copy that throws leaves the stream as it was.
    const events = commit.events.map(cloneEvent)
    const last = events.at(-1)!.sequence
    const snapshot = commit.snapshot && {
      entityType: commit.snapshot.entityType,
      entityId: commit.entityId,
      sequence: commit.snapshot.sequence,
      eventSequence: last,
      payload: copy(commit.snapshot.payload),
    }

    for (const event of events) {
      held.events.set(event.sequence, event)
    }
    held.head = held.head > last ? held.head : last
    if (snapshot) {
      held.snapshot = snapshot
    }

    this.#streams.set(commit.entityId, held)
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

    const stream = this.#streams.get(entityId)
    const covered = stream?.snapshot?.eventSequence ?? null
    if (stream === undefined || covered === null || covered < through) {
      throw new CompactionUncoveredError(entityId, through, covered)
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
  }

  // The stream's events by sequence, ascending or descending. Sequences are unique.
  #sorted(entityId: string, descending = false): SerializedEvent[] {
    const events = Array.from(this.#streams.get(entityId)?.events.values() ?? [])
    const order = descending ? -1 : 1
    events.sort((a, b) => (a.sequence < b.sequence ? -order : order))

    return events
  }
}

// Refuses a commit the stored stream doesn't accept, as the live backends' conditions do. The
// head update needs no head for a create and the head to be the sequence before the first event
// for an append. Then no event may exist already, and a snapshot must follow the stored one.
function checkWrite(commit: Commit, held: Stream): void {
  const first = commit.events[0]!.sequence
  const headPasses = first <= 1n ? held.head === 0n : held.head === first - 1n
  if (!headPasses) {
    throw probe(commit.entityId, held, first)
  }

  // The head update passed and an event put would fail: an event sits above the head.
  if (commit.events.some((event) => held.events.has(event.sequence))) {
    throw drift(commit.entityId, held.head, derivedHead(held))
  }

  const snapshot = commit.snapshot
  const stored = held.snapshot?.sequence ?? 0n
  if (snapshot && snapshot.sequence !== stored + 1n) {
    throw new ConflictError(
      commit.entityId,
      `snapshot sequence moved: stored ${stored}, commit writes ${snapshot.sequence}`,
    )
  }
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
