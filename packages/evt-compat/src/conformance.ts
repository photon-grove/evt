// A backend-neutral conformance suite for Repository implementations, the TypeScript counterpart
// of Rust's photon_evt::conformance and Go's conformance package. It checks the contract the
// Repository interface documents: ordering, isolation, optimistic concurrency, stream heads,
// snapshots, compaction, the budget and the reserved namespaces. Every case namespaces its entity
// IDs and types, so a durable backend can share one table across runs.
//
// The cases follow the Rust suite's, and keep its names, for what TypeScript implements so far.
// Every step depends on the ones before it, so the cases await in loops on purpose.
/* oxlint-disable no-await-in-loop */

import {
  CommitTooLargeError,
  CompactionUncoveredError,
  ConflictError,
  EvtError,
  type NewSnapshot,
  type Repository,
  ReservedEntityIdError,
  SequenceGapError,
  type SerializedEvent,
  type SerializedSnapshot,
  StreamHeadDriftError,
  emptyMetadata,
  eventId,
  isConflict,
  isRetryable,
} from '@photon-grove/evt'

import {type Checks, canonical} from './harness.js'

/** What a case needs to make a stream drift, which no repository call can do. */
export interface HeadFixtures {
  appendBypassingHead(event: SerializedEvent): void | Promise<void>
  deleteHead(entityId: string): void | Promise<void>
  storedHead(entityId: string): bigint | null | Promise<bigint | null>
}

/** Runs every case against `repo`, and the drift cases too when `fixtures` is given. */
export async function conformance(
  checks: Checks,
  repo: Repository,
  fixtures?: HeadFixtures,
): Promise<void> {
  const cases: [string, () => Promise<void>][] = [
    ['isolate entities', () => isolateEntities(repo)],
    ['commit and read', () => commitAndRead(repo)],
    ['metadata survives commit and read', () => metadataRoundTrip(repo)],
    ['reject events from another entity or of another type', () => foreignEvents(repo)],
    ['reject events out of order', () => outOfOrderEvents(repo)],
    ['an empty commit is a no-op', () => emptyCommit(repo, fixtures)],
    ['reject a duplicate sequence', () => duplicateSequence(repo)],
    ['snapshots round-trip', () => snapshotRoundTrip(repo)],
    ['reject a snapshot sequence conflict', () => snapshotSequenceConflict(repo)],
    ['put a snapshot outside a commit', () => putSnapshot(repo)],
    ['a snapshot write must be covered by the head', () => snapshotGuard(repo)],
    ['compaction never moves the head', () => headSurvivesCompaction(repo)],
    ['compact below a snapshot', () => compactBelow(repo)],
    ['reject a commit below the compacted floor', () => compactedFloor(repo)],
    ['a commit must continue from the stored head without a gap', () => sequenceGaps(repo)],
    ['an oversized commit is refused before any write', () => commitTooLarge(repo)],
    ['racing writers lose with conflicts, never with drift', () => racingWriters(repo)],
    ['latest(n) reads the newest events first', () => latestN(repo)],
    [
      'refuse reserved namespaces on every write path',
      () => reservedNamespaces(repo, fixtures),
    ],
    ['an aborted call rejects with its reason and writes nothing', () => aborted(repo)],
    ['stored events are copies', () => copies(repo)],
  ]
  if (fixtures) {
    cases.push([
      'a stream whose head disagrees with its log is drift',
      () => streamHeadDrift(repo, fixtures),
    ])
  }

  for (const [name, run] of cases) {
    await checks.case(`conformance: ${name}`, run)
  }
}

class Failure extends Error {}

function ensure(condition: boolean, what: string): void {
  if (!condition) {
    throw new Failure(what)
  }
}

function ensureEqual(got: unknown, want: unknown, what: string): void {
  const [gotText, wantText] = [canonical(got), canonical(want)]
  if (gotText !== wantText) {
    throw new Failure(`${what}\n      got  ${gotText}\n      want ${wantText}`)
  }
}

// Fails unless `call` rejects with an error `matches` accepts, and returns that error.
async function expectError(
  call: Promise<unknown>,
  what: string,
  matches: (error: unknown) => boolean,
): Promise<unknown> {
  let result: unknown
  try {
    result = await call
  } catch (error) {
    if (matches(error)) {
      return error
    }

    throw new Failure(`${what}: got ${describe(error)}`)
  }

  const got = result === undefined ? 'undefined' : canonical(result)
  throw new Failure(`${what}: got ${got}, want an error`)
}

// A stale writer: its first event doesn't follow the head. A conflict, and not a gap.
function stale(error: unknown): boolean {
  return error instanceof ConflictError && isConflict(error) && isRetryable(error)
}

function coded(code: string): (error: unknown) => boolean {
  return (error) => error instanceof EvtError && error.code === code
}

function describe(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error)
}

let counter = 0

// Random per process, so two processes sharing a durable table never make the same namespace.
const run = Array.from(crypto.getRandomValues(new Uint8Array(4)), (byte) =>
  byte.toString(16).padStart(2, '0'),
).join('')

// A token unique to this process and call. Zero-padded, so namespaces made in one process sort
// in the order they were made.
function namespace(): string {
  counter++

  return `conf-${run}-${String(counter).padStart(6, '0')}`
}

const encoder = new TextEncoder()

class Stream {
  readonly id: string
  readonly entityType: string

  constructor(ns: string, name: string, entityType = 'ledger') {
    this.id = `${ns}-${name}`
    this.entityType = `${ns}-${entityType}`
  }

  // Stored events whose payload records their sequence: {"amount":<sequence>}.
  events(from: number, through: number): SerializedEvent[] {
    const events = []
    for (let sequence = from; sequence <= through; sequence++) {
      events.push(this.event(sequence, sequence))
    }

    return events
  }

  event(sequence: number | bigint, amount: number): SerializedEvent {
    const at = BigInt(sequence)

    return {
      id: eventId(this.id, at),
      sequence: at,
      type: 'ledger:added',
      version: 1,
      entityId: this.id,
      entityType: this.entityType,
      payload: encoder.encode(`{"amount":${amount}}`),
      metadata: {
        ...emptyMetadata(),
        origin: {source: 'conformance', endpoint: 'suite'},
        trace: new Map([['traceparent', '00-trace-span-01']]),
        timestamp: '2026-01-02T03:04:05Z',
      },
    }
  }

  newSnapshot(sequence: number | bigint, payload: string): NewSnapshot {
    return {
      entityType: this.entityType,
      sequence: BigInt(sequence),
      payload: encoder.encode(payload),
    }
  }

  snapshot(sequence: number, eventSequence: number, payload: string): SerializedSnapshot {
    return {
      entityType: this.entityType,
      entityId: this.id,
      sequence: BigInt(sequence),
      eventSequence: BigInt(eventSequence),
      payload: encoder.encode(payload),
    }
  }

  commit(repo: Repository, events: SerializedEvent[], snapshot?: NewSnapshot): Promise<void> {
    return repo.commit({entityId: this.id, events, snapshot})
  }
}

async function isolateEntities(repo: Repository): Promise<void> {
  const ns = namespace()
  const a = new Stream(ns, 'a')
  const b = new Stream(ns, 'b')
  const missing = new Stream(ns, 'missing')

  await a.commit(repo, a.events(1, 2))
  await b.commit(repo, b.events(1, 1))

  ensureEqual(await repo.events(a.id), a.events(1, 2), 'events(a)')
  ensureEqual(await repo.events(b.id), b.events(1, 1), 'events(b)')
  ensure((await repo.events(missing.id)).length === 0, 'a missing entity has no events')
  ensure((await repo.snapshot(missing.id)) === null, 'a missing entity has no snapshot')
}

async function commitAndRead(repo: Repository): Promise<void> {
  const stream = new Stream(namespace(), '1')
  await stream.commit(repo, stream.events(1, 3))
  await stream.commit(repo, stream.events(4, 5))

  ensureEqual(
    await repo.events(stream.id),
    stream.events(1, 5),
    'events() returns every event as written, ascending',
  )

  const reads: [bigint, SerializedEvent[]][] = [
    [2n, stream.events(3, 5)],
    [5n, []],
    [9n, []],
    [2n ** 63n - 1n, []],
  ]
  for (const [after, want] of reads) {
    ensureEqual(await repo.eventsAfter(stream.id, after), want, `eventsAfter(${after})`)
  }
}

async function metadataRoundTrip(repo: Repository): Promise<void> {
  const stream = new Stream(namespace(), '1')
  const events = stream.events(1, 2)
  events.forEach((event, i) => {
    event.metadata.commandId = `command-${i + 1}`
    event.metadata.principalId = `principal-${i + 1}`
    event.metadata.causationId = `cause-${i + 1}`
    event.metadata.extensions = new Map([
      ['source', `source-${i + 1}`],
      ['empty', ''],
    ])
  })

  await stream.commit(repo, events)

  const stored = await repo.events(stream.id)
  ensure(stored.length === 2, `metadata event count: got ${stored.length}`)
  stored.forEach((got, i) => {
    const want = events[i]!.metadata
    ensureEqual(got.metadata.commandId, want.commandId, 'the command ID survives')
    ensureEqual(got.metadata.principalId, want.principalId, 'the principal ID survives')
    ensureEqual(got.metadata.causationId, want.causationId, 'an explicit causation ID survives')
    ensureEqual(
      got.metadata.extensions,
      want.extensions,
      'extensions, including an empty value, survive',
    )
  })
}

async function foreignEvents(repo: Repository): Promise<void> {
  const ns = namespace()
  const a = new Stream(ns, 'a')
  const b = new Stream(ns, 'b')
  const invalid = coded('invalidCommit')

  await expectError(a.commit(repo, b.events(1, 1)), "entity b's event under entity a", invalid)

  // Typed scans read each row's entity type, so one commit must not mix them.
  const mixed = [a.event(1, 1), {...a.event(2, 2), entityType: `${a.entityType}-other`}]
  await expectError(a.commit(repo, mixed), 'events of two entity types', invalid)

  const otherType = {...a.newSnapshot(1, '{}'), entityType: `${a.entityType}-other`}
  await expectError(
    a.commit(repo, a.events(1, 1), otherType),
    "a snapshot whose entity type differs from the events'",
    invalid,
  )

  const empty = (await repo.events(a.id)).length + (await repo.events(b.id)).length === 0
  ensure(empty, 'the rejected commits write nothing')
}

async function outOfOrderEvents(repo: Repository): Promise<void> {
  const ns = namespace()
  const invalid = coded('invalidCommit')

  // Loads refuse a stream with a gap, so a commit must not create one.
  const gapped = new Stream(ns, 'gap')
  await expectError(
    gapped.commit(repo, [gapped.event(1, 1), gapped.event(3, 3)]),
    'a commit that skips a sequence',
    invalid,
  )
  ensure((await repo.events(gapped.id)).length === 0, 'the rejected gap writes nothing')

  // DynamoDB keeps the snapshot at sk = 0 and reads after it, so sequences start at 1.
  const low = new Stream(ns, 'low')
  for (const sequence of [0, -1]) {
    await expectError(
      low.commit(repo, [low.event(sequence, 1)]),
      `a commit at sequence ${sequence}`,
      invalid,
    )
  }

  // DynamoDB rebuilds the ID from the key, so a stored ID must already be canonical.
  const renamed = {...low.event(1, 1), id: 'custom-id'}
  await expectError(low.commit(repo, [renamed]), 'an event whose ID is not canonical', invalid)

  // Snapshot sequences start at 1, and the DynamoDB condition subtracts one from them.
  for (const sequence of [0n, -(2n ** 63n)]) {
    await expectError(
      low.commit(repo, low.events(1, 1), low.newSnapshot(sequence, '{}')),
      `a snapshot at sequence ${sequence}`,
      invalid,
    )
  }

  const lowEmpty =
    (await repo.events(low.id)).length === 0 && (await repo.snapshot(low.id)) === null
  ensure(lowEmpty, 'the rejected commits write nothing')

  const stream = new Stream(ns, '1')
  await stream.commit(repo, stream.events(1, 5), stream.newSnapshot(1, '{}'))
  await repo.compactBelow(stream.id, 5n)

  // Only the first event is checked against the floor, so [6, 5] would recreate compacted 5.
  await expectError(
    stream.commit(repo, [stream.event(6, 6), stream.event(5, 5)]),
    'a commit whose sequences descend',
    invalid,
  )
  ensure((await repo.events(stream.id)).length === 0, 'the rejected commit writes nothing')
}

async function emptyCommit(repo: Repository, fixtures?: HeadFixtures): Promise<void> {
  const stream = new Stream(namespace(), '1')

  // The snapshot sequences are wrong for a new stream, and 0 is invalid, but an empty commit
  // ignores its snapshot rather than validating it.
  for (const sequence of [2, 0]) {
    await stream.commit(repo, [], stream.newSnapshot(sequence, '{}'))
  }
  ensure((await repo.snapshot(stream.id)) === null, 'an empty commit ignores its snapshot')
  if (fixtures) {
    ensureEqual(await fixtures.storedHead(stream.id), null, 'an empty commit writes no head')
  }

  // An empty commit creates no stream: the first commit still creates it.
  await stream.commit(repo, stream.events(1, 1))
  ensureEqual(await repo.events(stream.id), stream.events(1, 1), 'the stream starts at 1')
}

async function duplicateSequence(repo: Repository): Promise<void> {
  const stream = new Stream(namespace(), '1')
  await stream.commit(repo, stream.events(1, 2))

  await expectError(
    stream.commit(repo, [stream.event(1, 100)]),
    're-committing sequence 1',
    stale,
  )
  await expectError(
    stream.commit(repo, stream.events(2, 3)),
    'a commit overlapping the stored head',
    stale,
  )
  ensureEqual(await repo.events(stream.id), stream.events(1, 2), 'a rejected commit writes nothing')

  await stream.commit(repo, stream.events(3, 3))
  ensureEqual(await repo.events(stream.id), stream.events(1, 3), 'the next sequence commits')
}

async function snapshotRoundTrip(repo: Repository): Promise<void> {
  const stream = new Stream(namespace(), '1')

  await stream.commit(repo, stream.events(1, 3), stream.newSnapshot(1, '{"total":6}'))
  ensureEqual(
    await repo.snapshot(stream.id),
    stream.snapshot(1, 3, '{"total":6}'),
    "the snapshot covers the commit's last event",
  )

  await stream.commit(repo, stream.events(4, 5), stream.newSnapshot(2, '{"total":15}'))
  await stream.commit(repo, stream.events(6, 6))
  ensureEqual(
    await repo.snapshot(stream.id),
    stream.snapshot(2, 5, '{"total":15}'),
    'a later snapshot replaces it, and a commit without one leaves it',
  )
}

async function snapshotSequenceConflict(repo: Repository): Promise<void> {
  const ns = namespace()
  const first = new Stream(ns, 'first')
  await expectError(
    first.commit(repo, first.events(1, 2), first.newSnapshot(2, '{}')),
    'a first snapshot whose sequence is not 1',
    stale,
  )
  const firstEmpty =
    (await repo.snapshot(first.id)) === null && (await repo.events(first.id)).length === 0
  ensure(firstEmpty, 'the rejected first snapshot writes nothing')

  const stream = new Stream(ns, '1')
  await stream.commit(repo, stream.events(1, 2), stream.newSnapshot(1, '{}'))

  await expectError(
    stream.commit(repo, stream.events(3, 4), stream.newSnapshot(1, '{}')),
    'a snapshot that does not follow the stored one',
    stale,
  )
  ensureEqual(
    await repo.events(stream.id),
    stream.events(1, 2),
    "the rejected commit's events are not written",
  )
  ensureEqual(
    await repo.snapshot(stream.id),
    stream.snapshot(1, 2, '{}'),
    'the stored snapshot is unchanged',
  )

  await stream.commit(repo, stream.events(3, 4), stream.newSnapshot(2, '{}'))
}

async function putSnapshot(repo: Repository): Promise<void> {
  const stream = new Stream(namespace(), '1')

  // An out-of-band snapshot must be covered by the stream's head, so the stream has events up to
  // the highest snapshot below.
  await stream.commit(repo, stream.events(1, 7))

  ensure(await repo.putSnapshot(stream.snapshot(1, 5, '{"v":1}')), 'the first snapshot is written')
  ensure(
    !(await repo.putSnapshot(stream.snapshot(2, 3, '{"v":2}'))),
    'an older snapshot is not written',
  )
  ensureEqual(
    await repo.snapshot(stream.id),
    stream.snapshot(1, 5, '{"v":1}'),
    'the newer snapshot stays',
  )

  ensure(
    await repo.putSnapshot(stream.snapshot(3, 5, '{"v":3}')),
    'a snapshot at the same event sequence replaces it',
  )
  ensure(await repo.putSnapshot(stream.snapshot(4, 7, '{"v":4}')), 'a newer snapshot replaces it')
  ensureEqual(
    await repo.snapshot(stream.id),
    stream.snapshot(4, 7, '{"v":4}'),
    'the latest snapshot is stored',
  )
}

function isDrift(entityId: string, stored: bigint | null, derived: bigint) {
  return (error: unknown): boolean =>
    error instanceof StreamHeadDriftError &&
    error.entityId === entityId &&
    error.stored === stored &&
    error.derived === derived &&
    !isRetryable(error) &&
    !isConflict(error)
}

async function snapshotGuard(repo: Repository): Promise<void> {
  const ns = namespace()
  const a = new Stream(ns, 'a')
  await a.commit(repo, a.events(1, 3))

  // A snapshot ahead of the stream would let a writer that loaded earlier pass `head = 3` and
  // append events 4 and 5 beneath a snapshot that already covers them.
  await expectError(
    repo.putSnapshot(a.snapshot(1, 5, '{}')),
    'a snapshot ahead of the head',
    isDrift(a.id, 3n, 5n),
  )
  ensure((await repo.snapshot(a.id)) === null, 'the refused snapshot is not written')

  ensure(await repo.putSnapshot(a.snapshot(1, 3, '{}')), 'a snapshot at the head is written')

  // A stream that was never written has no head: a snapshot above 0 is drift with an absent
  // stored head, but a snapshot of the empty stream (event sequence 0) is allowed.
  const empty = new Stream(ns, 'empty')
  await expectError(
    repo.putSnapshot(empty.snapshot(1, 2, '{}')),
    'a snapshot of a stream with no head',
    isDrift(empty.id, null, 2n),
  )
  ensure(
    await repo.putSnapshot(empty.snapshot(1, 0, '{}')),
    'a snapshot at event sequence 0 needs no head',
  )
  ensureEqual(
    await repo.snapshot(empty.id),
    empty.snapshot(1, 0, '{}'),
    'the empty-stream snapshot is stored',
  )

  await empty.commit(repo, empty.events(1, 2))
  ensureEqual(
    await repo.events(empty.id),
    empty.events(1, 2),
    'the stream with an empty-stream snapshot accepts its first events',
  )
}

// Compaction deletes events but never touches the head: a writer whose load is current still
// appends, and a negative `after` reads the events only.
async function headSurvivesCompaction(repo: Repository): Promise<void> {
  const a = new Stream(namespace(), 'a')

  await a.commit(repo, a.events(1, 4), a.newSnapshot(1, '{}'))
  ensureEqual(
    await repo.compactBelow(a.id, 4n),
    4,
    'the head event is compacted away with the rest',
  )

  // The writer that loaded at head 4 before the compaction is still current.
  await a.commit(repo, a.events(5, 5))
  ensureEqual(await repo.events(a.id), a.events(5, 5), 'the append after compaction')

  ensureEqual(
    await repo.eventsAfter(a.id, -5n),
    a.events(5, 5),
    'a negative `after` returns the events only',
  )
}

async function compactBelow(repo: Repository): Promise<void> {
  const ns = namespace()
  const stream = new Stream(ns, '1')
  await stream.commit(repo, stream.events(1, 4), stream.newSnapshot(1, '{}'))
  await stream.commit(repo, stream.events(5, 6))

  for (const through of [0n, -1n]) {
    ensureEqual(await repo.compactBelow(stream.id, through), 0, `compactBelow(${through})`)
  }

  await expectError(
    repo.compactBelow(stream.id, 5n),
    'compacting past the snapshot',
    (error) => error instanceof CompactionUncoveredError && error.covered === 4n,
  )
  ensureEqual(
    await repo.events(stream.id),
    stream.events(1, 6),
    'a refused compaction deletes nothing',
  )

  ensureEqual(await repo.compactBelow(stream.id, 3n), 3, 'compactBelow(3) deletes events 1-3')
  ensureEqual(await repo.compactBelow(stream.id, 4n), 1, 'compactBelow(4) deletes event 4')
  ensureEqual(await repo.compactBelow(stream.id, 4n), 0, 'compaction is idempotent')
  ensureEqual(await repo.events(stream.id), stream.events(5, 6), 'uncovered events remain')
  ensureEqual(
    await repo.snapshot(stream.id),
    stream.snapshot(1, 4, '{}'),
    'compaction never deletes the snapshot',
  )

  const unsnapshotted = new Stream(ns, '2')
  await unsnapshotted.commit(repo, unsnapshotted.events(1, 2))
  const missing = new Stream(ns, 'missing')
  const uncovered: [Stream, string][] = [
    [unsnapshotted, 'no snapshot'],
    [missing, 'no stream'],
  ]
  for (const [target, what] of uncovered) {
    await expectError(
      repo.compactBelow(target.id, 1n),
      `compacting a stream with ${what}`,
      (error) => error instanceof CompactionUncoveredError && error.covered === null,
    )
  }
}

async function compactedFloor(repo: Repository): Promise<void> {
  const stream = new Stream(namespace(), '1')
  await stream.commit(repo, stream.events(1, 4), stream.newSnapshot(1, '{}'))
  await repo.compactBelow(stream.id, 4n)

  // A writer that loaded before the snapshot commits sequence 3 again. Its key was compacted
  // away, so only the head refuses it.
  await expectError(
    stream.commit(repo, stream.events(3, 3)),
    'a commit below the floor',
    stale,
  )
  await expectError(
    stream.commit(repo, stream.events(4, 5), stream.newSnapshot(2, '{}')),
    'a snapshot commit that starts at the floor',
    stale,
  )
  ensure((await repo.events(stream.id)).length === 0, 'rejected commits write nothing')

  await stream.commit(repo, stream.events(5, 5))
  ensureEqual(
    await repo.events(stream.id),
    stream.events(5, 5),
    'a commit above the floor succeeds',
  )
}

function gap(id: string, after: bigint, found: bigint) {
  return (error: unknown): boolean =>
    error instanceof SequenceGapError &&
    error.entityId === id &&
    error.after === after &&
    error.found === found &&
    isConflict(error) &&
    isRetryable(error)
}

async function sequenceGaps(repo: Repository): Promise<void> {
  const ns = namespace()

  // A first event above sequence 1 on a stream with no events is a gap.
  const fresh = new Stream(ns, 'new')
  await expectError(fresh.commit(repo, fresh.events(3, 3)), 'a fresh gap', gap(fresh.id, 0n, 3n))
  ensure((await repo.events(fresh.id)).length === 0, 'a refused gap writes nothing')

  // Skipping a sequence on an existing stream is a gap; continuing from the head is not.
  const a = new Stream(ns, 'a')
  await a.commit(repo, a.events(1, 1))
  await expectError(a.commit(repo, a.events(3, 3)), 'a skipped sequence', gap(a.id, 1n, 3n))
  await a.commit(repo, a.events(2, 2))

  // Compaction can delete every event a snapshot covers, the head's included: the snapshot's
  // event sequence is then the head, so the stream continues from it and still refuses a gap.
  const c = new Stream(ns, 'c')
  await c.commit(repo, c.events(1, 3), c.newSnapshot(1, '{}'))
  ensureEqual(await repo.compactBelow(c.id, 3n), 3, 'the head event is compacted away')
  await expectError(c.commit(repo, c.events(5, 5)), 'a gap after compaction', gap(c.id, 3n, 5n))
  await c.commit(repo, c.events(4, 4))
}

function actions(count: number) {
  return (error: unknown): boolean =>
    error instanceof CommitTooLargeError && error.actions === count
}

async function commitTooLarge(repo: Repository): Promise<void> {
  const ns = namespace()

  // An append costs the same as a create: the feature-marker check, the rebuild-lock check, the
  // head update, and 97 event puts are exactly 100 actions.
  const grows = new Stream(ns, 'grows')
  await grows.commit(repo, grows.events(1, 1))
  await expectError(
    grows.commit(repo, grows.events(2, 99)),
    '98 appended events are one action too many',
    actions(101),
  )
  await grows.commit(repo, grows.events(2, 98))
  ensureEqual((await repo.events(grows.id)).length, 98, '97 appended events fit')

  const fits = new Stream(ns, 'fits')
  await fits.commit(repo, fits.events(1, 97))
  ensureEqual((await repo.events(fits.id)).length, 97, 'a commit that fills the budget fits')

  // A snapshot put is one more action, so a commit with one holds 96 events, not 97.
  const snapshotted = new Stream(ns, 'snapshotted')
  await expectError(
    snapshotted.commit(repo, snapshotted.events(1, 97), snapshotted.newSnapshot(1, '{}')),
    '97 events and a snapshot are one action too many',
    actions(101),
  )
  await snapshotted.commit(repo, snapshotted.events(1, 96), snapshotted.newSnapshot(1, '{}'))

  // One event over the per-item limit is refused however few actions the commit has.
  const large = new Stream(ns, 'large')
  const big = {...large.event(1, 1), payload: encoder.encode(`"${'x'.repeat(380_000)}"`)}
  await expectError(
    large.commit(repo, [big]),
    'an event over the per-item limit',
    (error) => error instanceof CommitTooLargeError && error.largestItem > 380_000,
  )
  ensure((await repo.events(large.id)).length === 0, 'an oversized commit writes nothing')
}

async function racingWriters(repo: Repository): Promise<void> {
  const a = new Stream(namespace(), 'race')

  for (let round = 0; round < 4; round++) {
    const first = round * 6 + 1
    // Six writers compete for the same sequence of the same stream, all at once.
    const results = await Promise.allSettled(
      Array.from({length: 6}, (_, writer) => a.commit(repo, [a.event(first, 100 + writer)])),
    )

    const winners = results.filter((result) => result.status === 'fulfilled').length
    ensureEqual(winners, 1, 'exactly one racing writer commits')
    for (const result of results) {
      if (result.status === 'rejected') {
        ensure(
          stale(result.reason),
          `a losing writer sees a conflict, not drift: ${describe(result.reason)}`,
        )
      }
    }

    await a.commit(repo, a.events(first + 1, first + 5))
  }

  ensureEqual((await repo.events(a.id)).length, 24, 'every round committed six events')
}

async function latestN(repo: Repository): Promise<void> {
  const stream = new Stream(namespace(), 'latest')

  // A snapshot and compaction in the middle of the stream: `latest` reads the stored events only,
  // never the snapshot, whether or not earlier events were compacted away.
  await stream.commit(repo, stream.events(1, 3), stream.newSnapshot(1, '{}'))
  await stream.commit(repo, stream.events(4, 6))
  await repo.compactBelow(stream.id, 3n)

  const descending = (from: number, through: number) => {
    const events = []
    for (let sequence = through; sequence >= from; sequence--) {
      events.push(stream.event(sequence, sequence))
    }

    return events
  }

  ensureEqual(await repo.latest(stream.id, 3), descending(4, 6), 'latest(3) after compaction')
  ensureEqual(await repo.latest(stream.id, 2), descending(5, 6), 'latest(2)')
  ensureEqual(await repo.latest(stream.id, 1), descending(6, 6), 'latest(1)')
  ensureEqual(await repo.latest(stream.id, 10), descending(4, 6), 'latest(10)')
  ensureEqual(
    await repo.latest(stream.id, Number.MAX_SAFE_INTEGER),
    descending(4, 6),
    'latest with the largest safe count',
  )

  const missing = new Stream(namespace(), 'latest-missing')
  ensure((await repo.latest(missing.id, 3)).length === 0, 'a missing entity has no latest events')

  for (const n of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    await expectError(repo.latest(stream.id, n), `latest(${n})`, coded('invalidArgument'))
  }
}

function reserved(error: unknown): boolean {
  return error instanceof ReservedEntityIdError
}

async function reservedNamespaces(repo: Repository, fixtures?: HeadFixtures): Promise<void> {
  const ns = namespace()
  const template = new Stream(ns, 'template')

  // The reserved prefix starts the entity ID itself, so these aren't namespaced like the other
  // cases' IDs. They're refused before any write, so nothing collides across runs.
  for (const prefix of ['SYSTEM#', 'command-outcome#']) {
    const id = `${prefix}${ns}-acct`
    const events = [{...template.event(1, 1), entityId: id, id: eventId(id, 1n)}]
    await expectError(repo.commit({entityId: id, events}), `committing to ${id}`, reserved)
    const empty = (await repo.events(id)).length === 0 && (await repo.snapshot(id)) === null
    ensure(empty, `the refused commit wrote nothing to ${id}`)
    if (fixtures) {
      ensureEqual(await fixtures.storedHead(id), null, `the refused commit left no head for ${id}`)
    }

    await expectError(
      repo.putSnapshot({...template.snapshot(1, 1, '{}'), entityId: id}),
      `putSnapshot at ${id}`,
      reserved,
    )

    for (const through of [-1n, 0n, 1n]) {
      await expectError(
        repo.compactBelow(id, through),
        `compactBelow(${through}) at ${id}`,
        reserved,
      )
    }
  }
}

async function aborted(repo: Repository): Promise<void> {
  const stream = new Stream(namespace(), 'aborted')
  const reason = new Error('the caller gave up')
  const controller = new AbortController()
  controller.abort(reason)
  const signal = controller.signal
  const isReason = (error: unknown) => error === reason

  await expectError(
    repo.commit({entityId: stream.id, events: stream.events(1, 1)}, {signal}),
    'an aborted commit',
    isReason,
  )
  ensure((await repo.events(stream.id)).length === 0, 'the aborted commit wrote nothing')

  await stream.commit(repo, stream.events(1, 2))
  // Each call starts only when it's checked, so no rejection goes unhandled.
  const calls: [string, () => Promise<unknown>][] = [
    ['events', () => repo.events(stream.id, {signal})],
    ['eventsAfter', () => repo.eventsAfter(stream.id, 0n, {signal})],
    ['latest', () => repo.latest(stream.id, 1, {signal})],
    ['snapshot', () => repo.snapshot(stream.id, {signal})],
    ['putSnapshot', () => repo.putSnapshot(stream.snapshot(1, 2, '{}'), {signal})],
    ['compactBelow', () => repo.compactBelow(stream.id, 1n, {signal})],
  ]
  for (const [name, call] of calls) {
    await expectError(call(), `an aborted ${name}`, isReason)
  }
  ensure((await repo.snapshot(stream.id)) === null, 'the aborted snapshot write wrote nothing')
}

// Bytes whose slice() is a view of the same memory, as a Node Buffer's is.
class SharedSlices extends Uint8Array {
  override slice(start?: number, end?: number): Uint8Array<ArrayBuffer> {
    return this.subarray(start, end) as Uint8Array<ArrayBuffer>
  }
}

// A caller that changes an event after committing it, or one it read, changes nothing stored.
async function copies(repo: Repository): Promise<void> {
  const stream = new Stream(namespace(), 'copies')
  const events = stream.events(1, 1)
  events[0]!.payload = SharedSlices.from(events[0]!.payload!)
  const snapshot = stream.newSnapshot(1, '{}')
  snapshot.payload = SharedSlices.from(snapshot.payload)
  await stream.commit(repo, events, snapshot)
  snapshot.payload.fill(0x20)

  const [written] = events
  written!.payload!.fill(0x20)
  written!.metadata.extensions.set('changed', 'after commit')
  written!.metadata.trace!.set('traceparent', 'changed')

  const [read] = await repo.events(stream.id)
  read!.payload!.fill(0x20)
  read!.metadata.origin!.source = 'changed'
  const stored = await repo.snapshot(stream.id)
  stored!.payload!.fill(0x20)

  ensureEqual(await repo.events(stream.id), stream.events(1, 1), 'the stored events are the same')
  ensureEqual(
    await repo.snapshot(stream.id),
    stream.snapshot(1, 1, '{}'),
    'the stored snapshot is unchanged',
  )
}

// A stream whose head disagrees with its log is drift (ADR 0007), ranked above a gap and an
// ordinary conflict.
async function streamHeadDrift(repo: Repository, fixtures: HeadFixtures): Promise<void> {
  const ns = namespace()
  const a = new Stream(ns, 'a')

  // Head 2, then an event 3 stored without its head update.
  await a.commit(repo, a.events(1, 2))
  await fixtures.appendBypassingHead(a.event(3, 3))
  ensureEqual(await fixtures.storedHead(a.id), 2n, 'the bypassed event left the head behind')

  // The head update passes (`head = 2`) and the event put fails: certain drift.
  await expectError(
    a.commit(repo, [a.event(3, 9)]),
    'an append whose head update passes over an event above the head',
    isDrift(a.id, 2n, 3n),
  )

  // The head update fails (`head = 3` against 2) and the probe finds the log ahead: drift, not a
  // gap and not a conflict.
  await expectError(
    a.commit(repo, [a.event(4, 9)]),
    'an append past a drifted head',
    isDrift(a.id, 2n, 3n),
  )
  ensureEqual((await repo.events(a.id)).length, 3, 'the refused appends wrote nothing')

  // An out-of-band snapshot at the derived head is ahead of the stored head.
  await expectError(
    repo.putSnapshot(a.snapshot(1, 3, '{}')),
    'a snapshot over a drifted stream',
    isDrift(a.id, 2n, 3n),
  )

  // A head that is gone while the log remains is drift too, with an absent stored head.
  const b = new Stream(ns, 'b')
  await b.commit(repo, b.events(1, 2))
  await fixtures.deleteHead(b.id)
  ensureEqual(await fixtures.storedHead(b.id), null, 'the head is gone')
  await expectError(
    b.commit(repo, [b.event(3, 3)]),
    'an append to a stream with no head',
    isDrift(b.id, null, 2n),
  )
  await expectError(
    b.commit(repo, b.events(1, 1)),
    'a create over a stream with events and no head',
    isDrift(b.id, null, 2n),
  )
}
