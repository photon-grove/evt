// The journal's conformance cases, the TypeScript counterpart of Rust's
// photon_evt::conformance::journal_cases, keeping their names. Each case namespaces its entity IDs,
// types and scopes, so a durable backend can share one store across runs. The rebuild cases wait
// for the rebuild lifecycle.
// Every step depends on the ones before it, so the cases await in loops on purpose.
/* oxlint-disable no-await-in-loop */

import {
  type CallOptions,
  type Commit,
  type CommitReceipt,
  EvtError,
  type FeatureManifest,
  type Outcome,
  type SerializedEvent,
  type SerializedSnapshot,
  StoreNotInitializedError,
  StreamHeadsNotMigratedError,
  type TransactionCommit,
  TransientError,
  type ViewRow,
  commitTransactionWithReceipt,
  commitWithReceipt,
  emptyMetadata,
  eventId,
  groupCommits,
  isConflict,
  projectViews,
} from '@photon-grove/evt'
import {
  ALLOCATION_KEY,
  JOURNAL_FEATURE,
  JOURNAL_VERSION,
  type JournalBackend,
  type JournalPage,
  JournalRepository,
  type JournalRow,
  JournalRowTooLargeError,
  Journal,
  MAX_PAGE_ROWS,
  MAX_POSITION,
  allocations,
  counterPartitionKey,
  counterView,
  encodeAllocations,
  replay,
  rowPartitionKey,
  rowSize,
  rowSortKey,
} from '@photon-grove/evt/journal'

import {
  JOURNAL_MARKER,
  JOURNAL_WRITER_TARGET,
  type JournalDeclared,
  type JournalMarker,
} from '@photon-grove/evt/testing'

import {type Checks, canonical} from './harness.js'

/** The rows a case reads and changes behind the journal's back, as another writer would. */
export interface JournalViews {
  getViewAt(pk: string, sk: string): Promise<ViewRow | null>
  putView(row: ViewRow): Promise<void>
  deleteView(pk: string, sk: string): Promise<void>
  /** Every row of a partition, ascending by sort key. */
  listViewsByPk(pk: string): Promise<ViewRow[]>
}

/** What the journal cases run against. */
export interface JournalTarget {
  readonly repo: JournalBackend
  readonly views: JournalViews
  /** The store's feature manifest as stored. */
  manifest(): Promise<FeatureManifest | null>
  /** Another handle on the same store, which no journal wraps, when the backend can make one. */
  handle?: (() => JournalBackend) | undefined
}

/** Runs every journal case against `target`. */
export async function journalConformance(checks: Checks, target: JournalTarget): Promise<void> {
  const {repo, views} = target
  const cases: [string, () => Promise<void>][] = [
    [
      'positions are allocated per scope across streams, with rows, counters, and metadata',
      () => allocatesAndWrites(repo, views),
    ],
    [
      'an allocation a caller forged in metadata is replaced, and uncovered events are untouched',
      () => forgedAllocations(repo),
    ],
    [
      'a multi-stream commit allocates in stream-ID then sequence order',
      () => multiStreamCommit(repo),
    ],
    [
      "a commit's receipt returns the events as stored, with the allocations a page read returns",
      () => receiptsCarryAllocations(repo),
    ],
    [
      'a row over the cap is refused before any write, at the cap it passes, and a pre-check agrees',
      () => sizeGuard(repo, views),
    ],
    [
      'a writer that lost a position re-reads the counters and commits at the next one',
      () => contentionIsRetried(repo),
    ],
    [
      'contention that outlasts the policy is a conflict that wrote nothing',
      () => contentionIsBounded(repo),
    ],
    [
      'a conflict on the stream itself is not retried by the journal, even when a counter moved',
      () => streamConflictsAreNotRetried(repo),
    ],
    [
      'a transient failure is retried within the policy and returned when it runs out',
      () => transientFailuresAreRetried(repo),
    ],
    [
      'a counter behind its rows is repaired from the highest row, not wedged',
      () => counterBehindRowsIsRepaired(repo, views),
    ],
    [
      'a writer whose counter read went stale fails on the counter even where its rows are gone',
      () => staleCounterReadsFailOnTheCounter(repo, views),
    ],
    [
      "a repository that does not write views, or ignores the counter's condition, is refused",
      () => viewsUnsupportedIsRefused(target),
    ],
    ['a commit the repository would refuse declares nothing', () => invalidCommits(target)],
    [
      'a full page shows that rows follow it when the counter is missing or behind',
      () => fullPagesShowMoreRows(repo, views),
    ],
    ['a commit that cannot be built declares nothing', () => refusedCommits(target)],
    [
      'pages are contiguous, report the first gap, and never pass it',
      () => pagesAndGaps(repo, views),
    ],
    [
      'the first journaled commit declares the feature and the history, once',
      () => declaresFeatureAndHistory(target),
    ],
    [
      'replaying stored events in any order reproduces the live rows and counters',
      () => replayReproducesTheJournal(repo, views),
    ],
  ]
  if (target.handle !== undefined) {
    const handle = target.handle
    cases.push([
      'only a journal writer writes to a journal table; any other handle is refused',
      () => onlyJournalWritersWrite(target, handle),
    ])
  }

  for (const [name, run] of cases) {
    await checks.case(`journal: ${name}`, run)
  }
}

/**
 * The cases that need a fresh store: a strict store that refuses an uninitialized commit, and one
 * that holds unmigrated data. Each gets its own repository and controls.
 */
export async function journalStoreCases(
  checks: Checks,
  fresh: () =>
    | {repo: JournalBackend; manifest(): Promise<FeatureManifest | null>}
    | Promise<{repo: JournalBackend; manifest(): Promise<FeatureManifest | null>}>,
): Promise<void> {
  await checks.case(
    'journal: a commit to an uninitialized strict store declares nothing',
    async () => {
      const {repo, manifest} = await fresh()
      const ns = namespace()
      const stream = new Stream(ns, 'a', 'ledger')
      const lagging = new Lagging(repo, 0)
      lagging.uninitialized = true
      const before = canonical(await manifest())
      await expectError(
        stream.commit(lagging.journaled(journal(ns)), stream.events(1, 1)),
        'an uninitialized store',
        (error) => error instanceof StoreNotInitializedError,
      )
      ensureEqual(canonical(await manifest()), before, 'the refusal wrote nothing')
      ensureEqual((await repo.events(stream.id)).length, 0, 'no event')
    },
  )
  await checks.case(
    'journal: a commit to an unmigrated store retracts every declaration',
    async () => {
      const {repo, manifest} = await fresh()
      const ns = namespace()
      const stream = new Stream(ns, 'a', 'ledger')
      const lagging = new Lagging(repo, 0)
      lagging.unmigrated = true
      await expectError(
        stream.commit(lagging.journaled(journal(ns)), stream.events(1, 1)),
        'an unmigrated store',
        (error) => error instanceof StreamHeadsNotMigratedError,
      )
      const after = await manifest()
      ensure(after?.features.get(JOURNAL_FEATURE) === undefined, 'the journal was retracted')
      ensure(!(after?.historyDependent ?? []).includes(`${ns}-ledger`), 'the history was retracted')
      ensureEqual((await repo.events(stream.id)).length, 0, 'no event')
    },
  )
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

async function expectError(
  call: Promise<unknown>,
  what: string,
  matches: (error: unknown) => boolean,
): Promise<unknown> {
  let result: unknown
  try {
    result = await call
  } catch (error) {
    if (!matches(error)) {
      throw new Failure(`${what}: unexpected error ${describe(error)}`)
    }

    return error
  }

  throw new Failure(`${what}: expected an error, got ${canonical(result ?? null)}`)
}

function describe(error: unknown): string {
  return error instanceof EvtError ? `${error.code}: ${error.message}` : String(error)
}

function coded(code: string): (error: unknown) => boolean {
  return (error) => error instanceof EvtError && error.code === code
}

let counter = 0

// A token unique to this run and call, for entity IDs, types and scopes.
function namespace(): string {
  counter++

  return `jrn-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}-${counter}`
}

function scopeName(ns: string, name: string): string {
  return `${ns}-${name}`
}

const encoder = new TextEncoder()

// A journal over the namespace's `ledger` type: every event goes to the namespace's `world` scope
// and to a `place-<stream suffix>` scope of its own stream. Retries are immediate.
function journal(ns: string, maxRowBytes?: number): Journal {
  return new Journal(
    [`${ns}-ledger`],
    (event) => [scopeName(ns, 'world'), scopeName(ns, `place-${event.entityId.split('-').at(-1)}`)],
    {maxRowBytes, retry: {maxAttempts: 4, baseDelayMs: 0, maxDelayMs: 0}},
  )
}

// A journal repository that never waits between attempts.
function journaled(repo: JournalBackend, configuration: Journal): JournalRepository {
  return new JournalRepository(repo, configuration, {sleep: async () => {}})
}

function positions(page: JournalPage): number[] {
  return page.rows.map((row) => Number(row.position))
}

class Stream {
  readonly id: string
  readonly entityType: string

  constructor(ns: string, name: string, type: string) {
    this.id = `${ns}-${name}`
    this.entityType = `${ns}-${type}`
  }

  // Stored events whose payload records their sequence: {"amount":<sequence>}.
  events(from: number, through: number): SerializedEvent[] {
    const events = []
    for (let sequence = from; sequence <= through; sequence++) {
      const at = BigInt(sequence)
      events.push({
        id: eventId(this.id, at),
        sequence: at,
        type: 'ledger:added',
        version: 1,
        entityId: this.id,
        entityType: this.entityType,
        payload: encoder.encode(`{"amount":${sequence}}`),
        metadata: {
          ...emptyMetadata(),
          origin: {source: 'conformance', endpoint: 'suite'},
          trace: new Map([['traceparent', '00-trace-span-01']]),
          timestamp: '2026-01-02T03:04:05Z',
        },
      })
    }

    return events
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

  commit(repo: {commit(commit: Commit): Promise<void>}, events: SerializedEvent[]): Promise<void> {
    return repo.commit({entityId: this.id, events})
  }
}

// Takes one from a count that counts down to zero: true while it had one to give.
function take(count: {left: number}): boolean {
  if (count.left <= 0) {
    return false
  }
  count.left--

  return true
}

/**
 * A repository whose journal reads are stale for a number of counter reads (the position scan
 * that backs repair is stale as long), standing in for a writer that read the counters just
 * before another writer committed, and whose next commits can fail transiently. It counts the
 * journal's backoffs.
 */
class Lagging implements JournalBackend {
  readonly inner: JournalBackend
  readonly #stale: {left: number}
  readonly #transient = {left: 0}
  staleCounter = 0n
  backoffs = 0
  writes = true
  applies = true
  uninitialized = false
  unmigrated = false

  constructor(inner: JournalBackend, staleReads: number) {
    this.inner = inner
    this.#stale = {left: staleReads}
  }

  failing(commits: number): this {
    this.#transient.left = commits

    return this
  }

  // A journal repository over this one that counts its waits here.
  journaled(configuration: Journal): JournalRepository {
    return new JournalRepository(this, configuration, {
      sleep: async () => {
        this.backoffs++
      },
    })
  }

  [JOURNAL_WRITER_TARGET](): object {
    return this.inner
  }

  readonly [JOURNAL_MARKER]: JournalMarker = {
    declareJournal: (types: readonly string[], options?: CallOptions) => {
      if (this.uninitialized) {
        return Promise.reject(new StoreNotInitializedError())
      }

      return this.inner[JOURNAL_MARKER].declareJournal(types, options)
    },
    retractJournal: (declared: JournalDeclared, options?: CallOptions) =>
      this.inner[JOURNAL_MARKER].retractJournal(declared, options),
  }

  async readCounter(scope: string, options?: CallOptions): Promise<bigint> {
    if (take(this.#stale)) {
      return this.staleCounter
    }

    return this.inner.readCounter(scope, options)
  }

  async readCounters(scopes: readonly string[], options?: CallOptions): Promise<bigint[]> {
    const counters = []
    for (const scope of scopes) {
      counters.push(await this.readCounter(scope, options))
    }

    return counters
  }

  async readLastPosition(scope: string, options?: CallOptions): Promise<bigint> {
    return this.#stale.left > 0 ? 0n : this.inner.readLastPosition(scope, options)
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
    this.#refuse()
    if (take(this.#transient)) {
      throw new TransientError('an injected transient failure')
    }

    return this.inner.commit(commit, options)
  }

  async commitTransaction(transaction: TransactionCommit, options?: CallOptions): Promise<void> {
    this.#refuse()

    return this.inner.commitTransaction(transaction, options)
  }

  writesViews(): boolean {
    return this.writes && this.inner.writesViews?.() === true
  }

  appliesExpectedPayload(): boolean {
    return this.applies && this.inner.appliesExpectedPayload?.() === true
  }

  declareHistoryDependent(types: readonly string[], options?: CallOptions): Promise<void> {
    return this.inner.declareHistoryDependent!(types, options)
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

  #refuse(): void {
    if (this.uninitialized) {
      throw new StoreNotInitializedError()
    }
    if (this.unmigrated) {
      throw new StreamHeadsNotMigratedError()
    }
  }
}

async function allocatesAndWrites(repo: JournalBackend, views: JournalViews): Promise<void> {
  const ns = namespace()
  const writer = journaled(repo, journal(ns))
  const [a, b] = [new Stream(ns, 'a', 'ledger'), new Stream(ns, 'b', 'ledger')]
  const [world, placeA, placeB] = [
    scopeName(ns, 'world'),
    scopeName(ns, 'place-a'),
    scopeName(ns, 'place-b'),
  ]

  ensureEqual(await writer.highWatermark(world), 0n, 'an untouched scope has watermark 0')
  const empty = await writer.page(world, 0n, 10)
  ensure(
    empty.rows.length === 0 && empty.gap === null && empty.caughtUp,
    'an untouched scope pages empty with no gap',
  )

  await a.commit(writer, a.events(1, 3))
  await b.commit(writer, b.events(1, 2))
  await a.commit(writer, a.events(4, 4))
  ensureEqual(await writer.highWatermark(world), 6n, 'world counter')
  ensureEqual(await writer.highWatermark(placeA), 4n, 'place a counter')
  ensureEqual(await writer.highWatermark(placeB), 2n, 'place b counter')

  const page = await writer.page(world, 0n, 100)
  ensureEqual(positions(page), [1, 2, 3, 4, 5, 6], 'world positions')
  ensure(page.caughtUp, 'world page is caught up')
  ensureEqual(
    page.rows.map((row) => row.event.id),
    [`${a.id}:1`, `${a.id}:2`, `${a.id}:3`, `${b.id}:1`, `${b.id}:2`, `${a.id}:4`],
    'world rows are in commit order',
  )
  ensure(
    page.rows.every((row) => row.owner === row.event.entityId),
    "every row is owned by its event's stream",
  )
  ensureEqual(positions(await writer.page(placeA, 0n, 100)), [1, 2, 3, 4], 'place a positions')

  // The allocations are in the stored events' metadata, and agree with the rows.
  const stored = await repo.events(a.id)
  ensureEqual(stored.length, 4, "a's events")
  for (const [index, event] of stored.entries()) {
    const held = allocations(event.metadata)
    ensureEqual(held.length, 2, 'two scopes per event')
    ensureEqual(
      held.find((allocation) => allocation.scope === placeA)?.position,
      BigInt(index + 1),
      'the place position',
    )
    ensureEqual(
      event.metadata.extensions.get(ALLOCATION_KEY),
      encodeAllocations(held),
      'the stored value is the canonical encoding',
    )
  }

  // The rows are plain view rows in the views table: partition, 20-digit sort key, owner.
  const row = await views.getViewAt(rowPartitionKey(world), rowSortKey(4n))
  ensureEqual(row?.entityId, b.id, "the row's owner column")
  const stored_counter = await views.getViewAt(counterPartitionKey(world), 'COUNTER')
  ensureEqual(
    stored_counter && rowFields(stored_counter),
    rowFields(counterView(world, 6n)),
    'the stored counter row',
  )

  // A paged read walks the scope.
  let after = 0n
  const seen: number[] = []
  for (;;) {
    const next = await writer.page(world, after, 4)
    seen.push(...positions(next))
    after = next.nextAfter
    if (!next.hasMore) {
      break
    }
  }
  ensureEqual(seen, [1, 2, 3, 4, 5, 6], 'paging by 4 reads every row once')

  // Events of an entity type the journal doesn't cover add nothing.
  const other = new Stream(ns, 'x', 'other')
  await other.commit(writer, other.events(1, 2))
  ensureEqual(await writer.highWatermark(world), 6n, 'world is unmoved')
  ensure(
    (await repo.events(other.id)).every((event) => !event.metadata.extensions.has(ALLOCATION_KEY)),
    'uncovered events carry no allocation',
  )
}

// A view row's stored fields, to compare rows whatever their `ttl`.
function rowFields(row: ViewRow): unknown {
  return {
    pk: row.pk,
    sk: row.sk,
    entityId: row.entityId,
    entityType: row.entityType,
    payload: new TextDecoder().decode(row.payload),
  }
}

async function forgedAllocations(repo: JournalBackend): Promise<void> {
  const ns = namespace()
  const writer = journaled(repo, journal(ns))
  const [covered, uncovered] = [new Stream(ns, 'a', 'ledger'), new Stream(ns, 'u', 'other')]
  const forged = encodeAllocations([{scope: scopeName(ns, 'forged'), position: 99n}])
  for (const stream of [covered, uncovered]) {
    const events = stream.events(1, 1)
    events[0]!.metadata.extensions = new Map([[ALLOCATION_KEY, forged]])
    await stream.commit(writer, events)
  }

  const stored = await repo.events(covered.id)
  ensure(
    !stored[0]!.metadata.extensions.get(ALLOCATION_KEY)!.includes('forged'),
    "the engine's allocation replaced the forged one",
  )
  ensureEqual(
    await writer.highWatermark(scopeName(ns, 'forged')),
    0n,
    'no counter for a forged scope',
  )
  ensure(
    !(await repo.events(uncovered.id))[0]!.metadata.extensions.has(ALLOCATION_KEY),
    'a forged key on an uncovered event is removed',
  )
}

async function multiStreamCommit(repo: JournalBackend): Promise<void> {
  const ns = namespace()
  const writer = journaled(repo, journal(ns))
  const [a, c] = [new Stream(ns, 'a', 'ledger'), new Stream(ns, 'c', 'ledger')]
  const world = scopeName(ns, 'world')

  // Given in the opposite order: allocation follows the canonical order, not the given one.
  await writer.commitTransaction({
    streams: [
      {entityId: c.id, events: c.events(1, 2)},
      {entityId: a.id, events: a.events(1, 1)},
    ],
  })
  const page = await writer.page(world, 0n, 10)
  ensureEqual(
    page.rows.map((row) => row.event.id),
    [`${a.id}:1`, `${c.id}:1`, `${c.id}:2`],
    'stream a before stream c, each in sequence order',
  )
  ensureEqual(positions(page), [1, 2, 3], 'contiguous positions')

  // An event-less transaction commits without touching the journal.
  await writer.commitTransaction({streams: []})
  ensureEqual(await writer.highWatermark(world), 3n, 'an empty transaction moves nothing')
}

// The allocations live only in the stored events, so a commit's receipt is how the writer learns
// them: they must equal what the stored events and a page read say, for a single-stream commit, a
// multi-stream one given out of order, and a retried one that lost its first positions.
async function receiptsCarryAllocations(repo: JournalBackend): Promise<void> {
  const ns = namespace()
  const writer = journaled(repo, journal(ns))
  const [a, b, c] = ['a', 'b', 'c'].map((name) => new Stream(ns, name, 'ledger')) as [
    Stream,
    Stream,
    Stream,
  ]
  const world = scopeName(ns, 'world')

  const single = await commitWithReceipt(writer, {entityId: a.id, events: a.events(1, 2)})
  ensureEqual(single.events, await repo.events(a.id), 'the single-stream receipt is stored')

  const multi = await commitTransactionWithReceipt(writer, {
    streams: [
      {entityId: c.id, events: c.events(1, 1)},
      {entityId: b.id, events: b.events(1, 2)},
    ],
  })
  ensureEqual(
    multi.events,
    [...(await repo.events(b.id)), ...(await repo.events(c.id))],
    'the multi-stream receipt is the stored events, in persisted order',
  )

  // Another writer takes the next world position between this writer's counter read and its
  // commit; the retried commit's receipt carries the positions it finally holds.
  const stale = new Lagging(repo, 2)
  const lagging = stale.journaled(journal(ns))
  await a.commit(writer, a.events(3, 3))
  const retried: CommitReceipt = await lagging.commitWithReceipt({
    entityId: c.id,
    events: c.events(2, 2),
  })
  ensureEqual(stale.backoffs, 1, 'one retry after one lost attempt')
  ensureEqual(retried.events, await repo.eventsAfter(c.id, 1n), 'the retried receipt is stored')

  const page = await writer.page(world, 0n, 100)
  const returned = [...single.events, ...multi.events, ...retried.events].map((event) => {
    const position = allocations(event.metadata).find((held) => held.scope === world)?.position
    if (position === undefined) {
      throw new Failure(`${event.id} holds no world position`)
    }
    return {position, event}
  })
  returned.sort((x, y) => (x.position < y.position ? -1 : 1))
  const rows = page.rows
    .filter((row) => row.event.id !== `${a.id}:3`)
    .map((row) => ({position: row.position, event: row.event}))
  ensureEqual(returned, rows, 'every returned allocation is the world row at that position')
  ensureEqual(positions(page), [1, 2, 3, 4, 5, 6, 7], 'world positions are contiguous')
}

async function sizeGuard(repo: JournalBackend, views: JournalViews): Promise<void> {
  const ns = namespace()
  const s1 = new Stream(ns, 's1', 'ledger')
  // The size of the widest row of an event this shape of stream writes.
  await s1.commit(journaled(repo, journal(ns)), s1.events(1, 1))
  const world = scopeName(ns, 'world')
  const stored = await views.getViewAt(rowPartitionKey(world), rowSortKey(1n))
  const placeRow = await views.getViewAt(rowPartitionKey(scopeName(ns, 'place-s1')), rowSortKey(1n))
  if (stored === null || placeRow === null) {
    throw new Failure('the first rows are missing')
  }
  const widest = Math.max(stored.payload.length, placeRow.payload.length)
  ensureEqual(
    rowSize(world, 1n, (await repo.events(s1.id))[0]!),
    stored.payload.length,
    "the guard's measure is the stored payload length",
  )

  // The same shape of event on another stream with a name as long: world is at 2 now, as wide.
  const s2 = new Stream(ns, 's2', 'ledger')
  const exactly = journaled(repo, journal(ns, widest))
  await s2.commit(exactly, s2.events(1, 1))
  ensureEqual(await exactly.highWatermark(world), 2n, 'a row exactly at the cap is written')

  const s3 = new Stream(ns, 's3', 'ledger')
  const under = journaled(repo, journal(ns, widest - 1))
  const refused = (await expectError(
    s3.commit(under, s3.events(1, 1)),
    'one byte over the cap',
    (error) => error instanceof JournalRowTooLargeError,
  )) as JournalRowTooLargeError
  ensureEqual(
    [refused.size, refused.limit, refused.eventId],
    [widest, widest - 1, `${s3.id}:1`],
    'the refusal names the widest row',
  )
  ensure(
    refused.scope === world || refused.scope.includes('place-s3'),
    `the refusal names a scope of the event: ${refused.scope}`,
  )
  ensureEqual((await repo.events(s3.id)).length, 0, 'a refused commit wrote no event')
  ensureEqual(await under.highWatermark(world), 2n, 'a refused commit moved no counter')
  ensureEqual(
    await under.highWatermark(scopeName(ns, 'place-s3')),
    0n,
    'a refused commit moved no counter of its own scope',
  )

  // The pre-check measures every row at the widest position, so it's never more lenient than the
  // guard: a cap it passes is a cap the commit passes.
  const events = new Stream(ns, 's5', 'ledger').events(1, 1)
  const measured = (await expectError(
    Promise.resolve().then(() => journal(ns, 0).preCheck(events)),
    'a zero cap refuses every row',
    (error) => error instanceof JournalRowTooLargeError,
  )) as JournalRowTooLargeError
  ensureEqual(measured.position, MAX_POSITION, "the pre-check's position")
  ensure(measured.size > widest, 'the widest position makes the widest row')
  journal(ns, measured.size).preCheck(events)
  const row = (await expectError(
    Promise.resolve().then(() => journal(ns, measured.size - 1).preCheck(events)),
    'a cap one under the measured size',
    (error) => error instanceof JournalRowTooLargeError,
  )) as JournalRowTooLargeError
  ensureEqual(row.size, measured.size, 'the refused row is the widest')
  ensureEqual(row.toRejection().code, 'journal-row-too-large', "the application's rejection")
  const s5 = new Stream(ns, 's5', 'ledger')
  const passing = journaled(repo, journal(ns, measured.size))
  await s5.commit(passing, s5.events(1, 1))
  ensureEqual(await passing.highWatermark(world), 3n, 'a commit its pre-check passed is written')

  // An event wider than any cap is refused whole, even where another scope would have fit.
  const big = new Stream(ns, 's4', 'ledger')
  const wide = big.events(1, 1)
  wide[0]!.payload = encoder.encode(`{"amount":1,"note":"${'x'.repeat(40_000)}"}`)
  await expectError(
    big.commit(journaled(repo, journal(ns)), wide),
    'an event over the default cap',
    (error) => error instanceof JournalRowTooLargeError,
  )
  ensureEqual((await repo.events(big.id)).length, 0, 'the oversized event was not written')
}

async function contentionIsRetried(repo: JournalBackend): Promise<void> {
  const ns = namespace()
  const [rival, loser] = [new Stream(ns, 'a', 'ledger'), new Stream(ns, 'b', 'ledger')]
  const world = scopeName(ns, 'world')

  // The rival commits world positions 1 and 2; the loser's first counter read is stale.
  const winner = journaled(repo, journal(ns))
  await rival.commit(winner, rival.events(1, 2))
  const lagging = new Lagging(repo, 2)
  await loser.commit(lagging.journaled(journal(ns)), loser.events(1, 1))

  ensureEqual(lagging.backoffs, 1, 'one retry after one lost attempt')
  const page = await winner.page(world, 0n, 10)
  ensureEqual(positions(page), [1, 2, 3], 'the loser took the next position')
  ensureEqual(page.rows[2]?.event.id, `${loser.id}:1`, "the third row is the loser's event")
  ensureEqual(await winner.highWatermark(world), 3n, 'the counter follows the retried commit')
  const stored = await repo.events(loser.id)
  ensureEqual(stored.length, 1, "the loser's event was written once")
  ensure(
    allocations(stored[0]!.metadata).some((held) => held.scope === world && held.position === 3n),
    'the stored allocation is the one that committed',
  )
}

async function contentionIsBounded(repo: JournalBackend): Promise<void> {
  const ns = namespace()
  const [rival, loser] = [new Stream(ns, 'a', 'ledger'), new Stream(ns, 'b', 'ledger')]
  const world = scopeName(ns, 'world')

  const winner = journaled(repo, journal(ns))
  await rival.commit(winner, rival.events(1, 1))
  // Every counter read is stale, so every attempt lands on the taken position.
  const lagging = new Lagging(repo, Number.MAX_SAFE_INTEGER)
  await expectError(
    loser.commit(lagging.journaled(journal(ns)), loser.events(1, 1)),
    'contention past the retry policy',
    isConflict,
  )
  ensureEqual(lagging.backoffs, 3, "four attempts wait three times (the policy's attempts)")
  ensureEqual((await repo.events(loser.id)).length, 0, 'the exhausted commit wrote nothing')
  ensureEqual(await winner.highWatermark(world), 1n, 'the exhausted commit moved no counter')
}

async function streamConflictsAreNotRetried(repo: JournalBackend): Promise<void> {
  const ns = namespace()
  const stream = new Stream(ns, 'a', 'ledger')
  const world = scopeName(ns, 'world')
  const lagging = new Lagging(repo, 0)
  const writer = lagging.journaled(journal(ns))
  await stream.commit(writer, stream.events(1, 2))

  // The same sequences again: the stream's own head conflicts, which no journal re-read fixes.
  await expectError(
    stream.commit(writer, stream.events(2, 3)),
    'a stale commit of the stream',
    isConflict,
  )
  ensureEqual(lagging.backoffs, 0, 'the journal did not retry a stream conflict')
  ensureEqual(await writer.highWatermark(world), 2n, 'the refused commit moved no counter')

  // The same race with the counter moved under the loser: its first counter read is stale, and its
  // stream is behind too. The stream's conflict outranks the journal's, so it's the caller's.
  const other = new Stream(ns, 'b', 'ledger')
  const winner = journaled(repo, journal(ns))
  await other.commit(winner, other.events(1, 1))
  await stream.commit(winner, stream.events(3, 3))
  const stale = new Lagging(repo, 2)
  await expectError(
    stream.commit(stale.journaled(journal(ns)), stream.events(3, 3)),
    'a stream conflict under a moved counter',
    isConflict,
  )
  ensureEqual(stale.backoffs, 0, 'no resend for a stream conflict')
  ensureEqual(await winner.highWatermark(world), 4n, 'the losing commit moved no counter')
}

async function transientFailuresAreRetried(repo: JournalBackend): Promise<void> {
  const ns = namespace()
  const stream = new Stream(ns, 'a', 'ledger')
  const world = scopeName(ns, 'world')

  // Two transient failures, then success: allocated afresh each time, written once.
  const flaky = new Lagging(repo, 0).failing(2)
  const writer = flaky.journaled(journal(ns))
  await stream.commit(writer, stream.events(1, 1))
  ensureEqual(flaky.backoffs, 2, 'one wait per transient failure')
  ensureEqual(await writer.highWatermark(world), 1n, 'written once')
  ensureEqual((await repo.events(stream.id)).length, 1, 'one event')

  // More failures than attempts: the transient error itself is thrown, and nothing is written.
  const other = new Stream(ns, 'b', 'ledger')
  const doomed = new Lagging(repo, 0).failing(Number.MAX_SAFE_INTEGER)
  const writer2 = doomed.journaled(journal(ns))
  await expectError(
    other.commit(writer2, other.events(1, 1)),
    'transient failures past the policy',
    (error) => error instanceof TransientError,
  )
  ensureEqual(doomed.backoffs, 3, 'four attempts wait three times')
  ensureEqual((await repo.events(other.id)).length, 0, 'nothing written')
  ensureEqual(await writer2.highWatermark(world), 1n, 'no counter moved')
}

async function counterBehindRowsIsRepaired(
  repo: JournalBackend,
  views: JournalViews,
): Promise<void> {
  const ns = namespace()
  const writer = journaled(repo, journal(ns))
  const [a, b] = [new Stream(ns, 'a', 'ledger'), new Stream(ns, 'b', 'ledger')]
  const world = scopeName(ns, 'world')
  await a.commit(writer, a.events(1, 3))

  // A counter lowered under live rows: every writer would collide on the same position forever.
  await views.putView(counterView(world, 1n))
  await b.commit(writer, b.events(1, 1))
  ensureEqual(
    positions(await writer.page(world, 0n, 10)),
    [1, 2, 3, 4],
    'the next position follows the highest row',
  )
  ensureEqual(await writer.highWatermark(world), 4n, 'the repaired counter is written back')

  // A counter that is gone.
  await views.deleteView(counterPartitionKey(world), 'COUNTER')
  await b.commit(writer, b.events(2, 2))
  ensureEqual(
    positions(await writer.page(world, 0n, 10)),
    [1, 2, 3, 4, 5],
    'a lost counter is repaired the same way',
  )
  ensureEqual(await writer.highWatermark(world), 5n, 'counter')
}

async function staleCounterReadsFailOnTheCounter(
  repo: JournalBackend,
  views: JournalViews,
): Promise<void> {
  const ns = namespace()
  const a = new Stream(ns, 'a', 'ledger')
  const [world, place] = [scopeName(ns, 'world'), scopeName(ns, 'place-a')]
  await a.commit(journaled(repo, journal(ns)), a.events(1, 4))

  // The rows at 3 and 4 are gone, and a writer read both counters at 2 before they were written.
  // Its rows at 3 would be absent: only the counter's compare-and-set refuses the stale attempt.
  for (const at of [world, place]) {
    for (const position of [3n, 4n]) {
      await views.deleteView(rowPartitionKey(at), rowSortKey(position))
    }
  }
  const lagging = new Lagging(repo, 2)
  lagging.staleCounter = 2n
  const writer = lagging.journaled(journal(ns))
  await a.commit(writer, a.events(5, 5))
  const stored = await repo.events(a.id)
  ensureEqual(
    allocations(stored[4]!.metadata).map((held) => held.position),
    [5n, 5n],
    'the positions after the live counters',
  )
  ensure(lagging.backoffs >= 1, 'the stale attempt was retried')
  ensureEqual(await writer.highWatermark(world), 5n, 'the counter')
}

async function refusedCommits(target: JournalTarget): Promise<void> {
  const {repo} = target
  const ns = namespace()
  const stream = new Stream(ns, 'a', 'ledger')
  // The guard refuses it, so the store must not end up declaring the type's history.
  await expectError(
    stream.commit(journaled(repo, journal(ns, 10)), stream.events(1, 1)),
    'a row over the cap',
    (error) => error instanceof JournalRowTooLargeError,
  )
  ensure(!(await historyDeclared(target, stream.entityType)), 'a refused commit declared')

  // A scope resolver that fails, the same.
  const other = new Stream(ns, 'b', 'ledger')
  const failing = journaled(
    repo,
    new Journal([`${ns}-ledger`], () => {
      throw new Error('no scope for this event')
    }),
  )
  await expectError(
    other.commit(failing, other.events(1, 1)),
    'a failing scope resolver',
    coded('invalidJournal'),
  )
  ensure(!(await historyDeclared(target, other.entityType)), 'a failed resolver declared')
}

async function historyDeclared(target: JournalTarget, entityType: string): Promise<boolean> {
  return ((await target.manifest())?.historyDependent ?? []).includes(entityType)
}

async function pagesAndGaps(repo: JournalBackend, views: JournalViews): Promise<void> {
  const ns = namespace()
  const writer = journaled(repo, journal(ns))
  const a = new Stream(ns, 'a', 'ledger')
  const world = scopeName(ns, 'world')
  await a.commit(writer, a.events(1, 5))

  for (const bad of [0, MAX_PAGE_ROWS + 1]) {
    await expectError(
      writer.page(world, 0n, bad),
      'a page limit out of range',
      coded('invalidArgument'),
    )
  }

  // A row missing in the middle: the page stops before it and names it.
  await views.deleteView(rowPartitionKey(world), rowSortKey(3n))
  const page = await writer.page(world, 0n, 10)
  ensureEqual(positions(page), [1, 2], 'rows before the gap')
  ensureEqual(page.gap, {from: 3n, to: 3n}, 'the gap')
  ensureEqual(page.nextAfter, 2n, 'the reader may not pass the gap')
  ensure(!page.hasMore && !page.caughtUp, 'a gapped page is neither done nor continuable')
  ensureEqual(page.highWatermark, 5n, "the watermark is the counter's")

  // Starting after the gap, the rows are contiguous again.
  const beyond = await writer.page(world, 3n, 10)
  ensureEqual(positions(beyond), [4, 5], 'rows after the gap')
  ensure(beyond.gap === null, 'no gap after it')
  // Starting inside the gap: it is the page's first thing.
  const inside = await writer.page(world, 2n, 10)
  ensure(inside.rows.length === 0, 'a page that starts at a gap holds no rows')
  ensureEqual(inside.gap, {from: 3n, to: 3n}, 'and names it')

  // The last rows missing: the counter is ahead of the rows, a gap at the tail.
  await views.deleteView(rowPartitionKey(world), rowSortKey(5n))
  const tail = await writer.page(world, 3n, 10)
  ensureEqual(positions(tail), [4], 'rows before the tail gap')
  ensureEqual(tail.gap, {from: 5n, to: 5n}, 'the tail gap')

  // A lost counter doesn't hide rows: the watermark follows the rows it finds.
  await views.deleteView(counterPartitionKey(world), 'COUNTER')
  ensureEqual(await writer.highWatermark(world), 0n, 'no counter row')
  const uncounted = await writer.page(world, 3n, 10)
  ensureEqual(positions(uncounted), [4], 'the rows are still read')
  ensureEqual(uncounted.highWatermark, 4n, 'the watermark follows the rows')
  ensure(uncounted.gap === null, 'no gap')
}

async function declaresFeatureAndHistory(target: JournalTarget): Promise<void> {
  const {repo} = target
  const ns = namespace()
  const writer = journaled(repo, journal(ns))
  const [a, plain] = [new Stream(ns, 'a', 'ledger'), new Stream(ns, 'p', 'other')]

  // Uncovered events declare nothing.
  await plain.commit(writer, plain.events(1, 1))
  ensure(
    !(await historyDeclared(target, a.entityType)),
    'nothing is declared before a covered event',
  )

  await a.commit(writer, a.events(1, 2))
  const manifest = await target.manifest()
  ensureEqual(
    manifest?.features.get(JOURNAL_FEATURE),
    JOURNAL_VERSION,
    'the journal feature at its format version',
  )
  ensure(await historyDeclared(target, a.entityType), 'the journaled type is history-dependent')
  ensure(!(await historyDeclared(target, plain.entityType)), 'an uncovered type is not')

  // Another journal repository finds both declared and writes nothing to the manifest.
  await a.commit(journaled(repo, journal(ns)), a.events(3, 3))
  ensureEqual(
    (await target.manifest())?.version,
    manifest?.version,
    'a repository that finds the declaration does not bump the manifest',
  )

  // Compaction of the journaled type is refused, even by a journal writer.
  await writer.putSnapshot(a.snapshot(1, 3, '{}'))
  await expectError(
    writer.compactBelow(a.id, 3n),
    'compacting a journaled stream',
    coded('historyDependentProjection'),
  )
  ensureEqual((await repo.events(a.id)).length, 3, 'nothing was deleted')
}

// A handle that isn't a journal writer refuses a journal table on every write path, whatever the
// entity type, so no event can be committed around the journal. The handle the journal wraps
// writes as usual.
async function onlyJournalWritersWrite(
  target: JournalTarget,
  handle: () => JournalBackend,
): Promise<void> {
  const {repo} = target
  const ns = namespace()
  const bypass = handle()
  const writer = journaled(repo, journal(ns))
  const [a, other] = [new Stream(ns, 'a', 'ledger'), new Stream(ns, 'o', 'other')]
  await a.commit(writer, a.events(1, 1))

  const refused = refusedJournal
  await expectError(
    a.commit(bypass, a.events(2, 2)),
    'a journaled type committed around the journal',
    refused,
  )
  await expectError(
    other.commit(bypass, other.events(1, 1)),
    'an uncovered type committed around the journal',
    refused,
  )
  await expectError(
    bypass.putSnapshot(a.snapshot(1, 1, '{}')),
    'a snapshot written around the journal',
    refused,
  )
  await expectError(bypass.compactBelow(other.id, 1n), 'compaction around the journal', refused)
  await expectError(
    bypass.commitTransaction({streams: [{entityId: other.id, events: other.events(1, 1)}]}),
    'a transaction committed around the journal',
    refused,
  )
  ensureEqual((await repo.events(a.id)).length, 1, 'the journaled stream')
  ensureEqual((await repo.events(other.id)).length, 0, 'nothing was committed around it')
  ensure((await repo.snapshot(a.id)) === null, 'no snapshot was written around the journal')

  // The journal's own handle is a journal writer.
  await other.commit(writer.inner, other.events(1, 1))
  await writer.inner.putSnapshot(a.snapshot(1, 1, '{}'))
  ensureEqual((await repo.events(other.id)).length, 1, 'the uncovered stream')
}

// The refusal of a journal table by a handle that isn't a journal writer.
function refusedJournal(error: unknown): boolean {
  return (
    error instanceof EvtError &&
    error.code === 'unsupportedFeature' &&
    error.message.includes(JOURNAL_FEATURE)
  )
}

async function replayReproducesTheJournal(
  repo: JournalBackend,
  views: JournalViews,
): Promise<void> {
  const ns = namespace()
  const writer = journaled(repo, journal(ns))
  const [a, b, c] = ['a', 'b', 'c'].map((name) => new Stream(ns, name, 'ledger')) as [
    Stream,
    Stream,
    Stream,
  ]
  await a.commit(writer, a.events(1, 2))
  await b.commit(writer, b.events(1, 3))
  await writer.commitTransaction({
    streams: [
      {entityId: c.id, events: c.events(1, 2)},
      {entityId: a.id, events: a.events(3, 3)},
    ],
  })
  await b.commit(writer, b.events(4, 4))

  const events: SerializedEvent[] = []
  for (const stream of [a, b, c]) {
    events.push(...(await repo.events(stream.id)))
  }
  ensureEqual(events.length, 9, 'nine stored events')

  // Whatever the order the events come in, the state is the same.
  const forward = replay(events)
  const reversed = events.map((_, index) => events[events.length - 1 - index]!)
  const interleaved = [...events]
  interleaved.sort((x, y) =>
    x.sequence === y.sequence
      ? x.entityId < y.entityId
        ? 1
        : -1
      : x.sequence < y.sequence
        ? -1
        : 1,
  )
  ensureEqual(replay(reversed), forward, 'reverse order')
  ensureEqual(replay(interleaved), forward, 'interleaved order')

  // It's exactly what the commits wrote.
  const world = scopeName(ns, 'world')
  const live = (await views.listViewsByPk(rowPartitionKey(world))).map(rowFields)
  const replayed = forward.rows.filter((row) => row.scope === world).map(({row}) => rowFields(row))
  ensureEqual(live.length, 9, 'nine live world rows')
  ensureEqual(replayed, live, 'the replayed world rows are the live rows')
  for (const {scope: at, position} of forward.counters) {
    ensureEqual(await writer.highWatermark(at), position, 'the replayed counter is live')
  }

  // The journal's projector alone reproduces the rows too (counters aside), through the engine's
  // regroup-and-replay.
  const rows = groupCommits(events)
    .flatMap((view) => projectViews([writer.projector()], view))
    .filter(({op}) => op.pk === rowPartitionKey(world))
    .map(({op}) =>
      rowFields({
        pk: op.pk,
        sk: op.sk,
        entityId: op.owner,
        entityType: op.entityType,
        payload: op.payload,
      }),
    )
  rows.sort((x, y) => ((x as {sk: string}).sk < (y as {sk: string}).sk ? -1 : 1))
  ensureEqual(rows, live, 'the projector reproduces the live world rows')
}

async function viewsUnsupportedIsRefused(target: JournalTarget): Promise<void> {
  const {repo} = target
  const ns = namespace()
  const stream = new Stream(ns, 'a', 'ledger')
  const dropping = new Lagging(repo, 0)
  dropping.writes = false
  const writer = dropping.journaled(journal(ns))
  await expectError(
    stream.commit(writer, stream.events(1, 1)),
    'a repository that does not write views',
    coded('viewsUnsupported'),
  )
  ensureEqual((await repo.events(stream.id)).length, 0, 'no event was written')
  ensure(!(await historyDeclared(target, stream.entityType)), 'nothing was declared')

  // Events of a type the journal doesn't cover aren't its concern.
  const other = new Stream(ns, 'x', 'other')
  await other.commit(writer, other.events(1, 1))

  // A repository that ignores the counter's compare-and-set is refused the same way.
  const unconditional = new Lagging(repo, 0)
  unconditional.applies = false
  const next = new Stream(ns, 'b', 'ledger')
  await expectError(
    next.commit(unconditional.journaled(journal(ns)), next.events(1, 1)),
    'a repository that ignores the expected-payload condition',
    coded('expectedPayloadUnsupported'),
  )
  ensureEqual((await repo.events(next.id)).length, 0, 'the refused commit wrote nothing')
}

async function invalidCommits(target: JournalTarget): Promise<void> {
  const {repo} = target
  const ns = namespace()
  const writer = journaled(repo, journal(ns))
  const covered = `${ns}-ledger`

  // An event whose ID isn't `{entity}:{sequence}`, which the repository refuses.
  const stream = new Stream(ns, 'a', 'ledger')
  const events = stream.events(1, 1)
  events[0]!.id = 'not-the-id'
  await expectError(
    stream.commit(writer, events),
    'an event with the wrong ID',
    coded('invalidCommit'),
  )
  ensure(!(await historyDeclared(target, covered)), 'a malformed commit declared')

  // A reserved entity ID, alone and in a transaction.
  const reserved = Object.assign(new Stream(ns, 'r', 'ledger'), {id: `SYSTEM#${ns}`})
  await expectError(
    reserved.commit(writer, reserved.events(1, 1)),
    'a reserved entity ID',
    coded('reservedEntityId'),
  )
  await expectError(
    writer.commitTransaction({streams: [{entityId: reserved.id, events: reserved.events(1, 1)}]}),
    'a reserved entity ID in a transaction',
    coded('reservedEntityId'),
  )
  ensure(!(await historyDeclared(target, covered)), 'a refused commit declared')
}

async function fullPagesShowMoreRows(repo: JournalBackend, views: JournalViews): Promise<void> {
  const ns = namespace()
  const writer = journaled(repo, journal(ns))
  const a = new Stream(ns, 'a', 'ledger')
  const world = scopeName(ns, 'world')
  await a.commit(writer, a.events(1, 5))

  for (const [name, lower] of [
    ['a counter behind its rows', true],
    ['a missing counter', false],
  ] as const) {
    if (lower) {
      await views.putView(counterView(world, 1n))
    } else {
      await views.deleteView(counterPartitionKey(world), 'COUNTER')
    }
    const first = await writer.page(world, 0n, 2)
    ensureEqual(positions(first), [1, 2], `${name}: the first page`)
    ensure(
      first.gap === null && first.hasMore && !first.caughtUp,
      `${name}: a full page with rows after it is not caught up`,
    )
    ensureEqual(first.highWatermark, 3n, `${name}: the watermark follows the next row`)

    let after = 0n
    const seen: number[] = []
    for (;;) {
      const page = await writer.page(world, after, 2)
      seen.push(...positions(page))
      after = page.nextAfter
      if (!page.hasMore) {
        ensure(page.caughtUp, `${name}: the last page is caught up`)
        break
      }
    }
    ensureEqual(seen, [1, 2, 3, 4, 5], `${name}: paging by 2 reads every row`)
  }
}
