// The journal compaction cases, the TypeScript counterpart of Rust's
// photon_evt::conformance::compaction_cases, keeping their names: the snapshot and the `410 Gone`
// handoff, canonical chunks, the lease with takeover and force release, epoch fencing, the
// allocation floor, and the refusals. Every case namespaces its scopes and streams, so a durable
// table can be shared across runs. The cases declare `journal` and `journal-compaction` and take
// the rebuild lock, so run them on a store of their own that nothing else is rebuilding.
// Every step depends on the ones before it, so the cases await in loops on purpose.
/* oxlint-disable no-await-in-loop */

import {
  type CallOptions,
  type Commit,
  EvtError,
  type FeatureManifest,
  type Outcome,
  type RebuildFence,
  type RebuildLease,
  RebuildUnavailableError,
  type SerializedEvent,
  type SerializedSnapshot,
  type TransactionCommit,
  UnsupportedFeatureError,
  type ViewRow,
  emptyMetadata,
  eventId,
  isRetryable,
} from '@photon-grove/evt'
import {
  COMPACTION_ENTITY_TYPE,
  COUNTER_SORT_KEY,
  type CompactionBackend,
  CompactionLeaseHeldError,
  CompactionLeaseLostError,
  ForeignJournalRowError,
  JOURNAL_COMPACTION_FEATURE,
  JOURNAL_COMPACTION_VERSION,
  Journal,
  type JournalBackend,
  JournalGoneError,
  JournalRepository,
  type JournalRow,
  JournalSnapshotMovedError,
  LEASE_SORT_KEY,
  MANIFEST_SORT_KEY,
  MAX_CHUNK_BYTES,
  MAX_CHUNK_ROWS,
  allocations,
  beginCompaction,
  chunkSortKey,
  compact,
  compactWithLease,
  compactionPartitionKey,
  compactionView,
  counterPartitionKey,
  encodeRow,
  endCompaction,
  forceReleaseCompaction,
  manifestDigest,
  manifestPayload,
  page,
  pageAtEpoch,
  readAudit,
  readChunk,
  readFloor,
  readLease,
  readManifest,
  releasedPayload,
  renewCompaction,
  rebuildJournalWithLock,
  rowPartitionKey,
  rowSortKey,
  rowView,
} from '@photon-grove/evt/journal'
import {
  JOURNAL_MARKER,
  JOURNAL_WRITER_TARGET,
  type JournalDeclared,
  type JournalMarker,
} from '@photon-grove/evt/testing'

import type {ManifestDraft} from './conformance.js'
import {type Checks, canonical} from './harness.js'

/** The view rows a case reads and changes behind the engine's back, as another writer would. */
export interface CompactionViews {
  getViewAt(pk: string, sk: string): Promise<ViewRow | null>
  putView(row: ViewRow): Promise<void>
  deleteView(pk: string, sk: string): Promise<void>
}

/** A clock the lease cases move forward, which the repository reads as its compaction clock. */
export class TestClock {
  #ms: number

  constructor(ms: number) {
    this.#ms = ms
  }

  /** The clock, in Unix milliseconds, to give the repository. */
  readonly now = (): number => this.#ms

  advance(seconds: number): void {
    this.#ms += seconds * 1000
  }

  /** The time in whole Unix seconds, as compaction reads it. */
  seconds(): number {
    return Math.floor(this.#ms / 1000)
  }
}

/** What the compaction cases run against. */
export interface CompactionTarget {
  /** A repository that writes views, reading `clock` as its compaction clock. */
  readonly repo: JournalBackend & CompactionBackend & RebuildFence
  readonly views: CompactionViews
  readonly clock: TestClock
  /** The store's feature manifest as stored. */
  manifest(): Promise<FeatureManifest | null>
  /** Changes the manifest as another writer would, and has the repository read it again. */
  declare(change: (draft: ManifestDraft) => void): Promise<void>
}

/** The lease the cases compact under: a minute. */
const LEASE = 60_000

/** Runs every journal compaction case against `target`. */
export async function compactionConformance(
  checks: Checks,
  target: CompactionTarget,
): Promise<void> {
  const cases: [string, () => Promise<void>][] = [
    // First, so that on a fresh store nothing has declared compaction yet.
    [
      'a compaction refused before its lease takes none, and no refused run declares anything',
      () => refusedRunsChangeNothing(target),
    ],
    [
      'a compaction snapshots the prefix, deletes its rows, and hands readers below it a 410',
      () => snapshotAndGone(target),
    ],
    [
      'chunks are canonical: compacting in steps stores what one compaction stores',
      () => chunksAreCanonical(target),
    ],
    [
      'a rebuild restores compacted rows, readers are served them, and compacting again prunes them',
      () => rebuildRestoresRows(target),
    ],
    [
      'a lease excludes others until it lapses or is force-released, and a lost lease writes nothing',
      () => leaseLifecycle(target),
    ],
    ['compaction is fenced on the rebuild epoch', () => epochFencing(target)],
    [
      'a lost counter allocates above the floor, never at a compacted position',
      () => allocationRespectsTheFloor(target),
    ],
    [
      'a writer whose counter read went stale cannot reallocate a compacted position',
      () => staleWritersFailOnTheCounter(target),
    ],
    [
      'a compaction past the watermark, below the floor, or over a gap is refused before any write',
      () => refusals(target),
    ],
    [
      'a gap after chunks that would close is refused before any chunk is written',
      () => gappedRangeWritesNothing(target),
    ],
    [
      'a compaction row the engine did not write is refused, never trusted',
      () => foreignRowsAreRefused(target),
    ],
    ['a chunk read for a superseded manifest is reported moved', () => movedSnapshot(target)],
  ]

  for (const [name, run] of cases) {
    await checks.case(`journal compaction: ${name}`, run)
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

function leaseLost(error: unknown): boolean {
  return error instanceof CompactionLeaseLostError
}

function rebuildUnavailable(error: unknown): boolean {
  return error instanceof RebuildUnavailableError
}

function foreign(error: unknown): boolean {
  return error instanceof ForeignJournalRowError
}

// A call's outcome, held so it can be checked after the store is put back.
type Settled =
  {readonly ok: true; readonly value: unknown} | {readonly ok: false; readonly error: unknown}

async function settle(call: Promise<unknown>): Promise<Settled> {
  try {
    return {ok: true, value: await call}
  } catch (error) {
    return {ok: false, error}
  }
}

function replayed(settled: Settled): Promise<unknown> {
  return settled.ok ? Promise.resolve(settled.value) : Promise.reject(settled.error)
}

let counter = 0

// A token unique to this run and call, for entity IDs, types and scopes.
function namespace(): string {
  counter++

  return `cmp-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}-${counter}`
}

function world(ns: string): string {
  return `${ns}-world`
}

function owner(ns: string): string {
  return `${ns}-compactor`
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()

// A journal over the namespace's `ledger` type, every event in the namespace's world scope, with
// immediate retries.
function journal(ns: string, maxRowBytes?: number): Journal {
  const scope = world(ns)

  return new Journal([`${ns}-ledger`], () => [scope], {
    maxRowBytes,
    retry: {maxAttempts: 4, baseDelayMs: 0, maxDelayMs: 0},
  })
}

// A journal repository that never waits between attempts.
function journaled(repo: JournalBackend, configuration: Journal): JournalRepository {
  return new JournalRepository(repo, configuration, {sleep: async () => {}})
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

  // An event whose payload is about 100,000 bytes, so its row is about 133,000 and a chunk holds
  // two.
  wide(sequence: number): SerializedEvent {
    const [event] = this.events(sequence, sequence)
    event!.payload = encoder.encode(`{"amount":${sequence},"note":"${'x'.repeat(100_000)}"}`)

    return event!
  }

  commit(repo: {commit(commit: Commit): Promise<void>}, events: SerializedEvent[]): Promise<void> {
    return repo.commit({entityId: this.id, events})
  }
}

// Commits `count` events, one per commit, into the world scope, and returns the live rows.
async function fill(repo: JournalBackend, ns: string, count: number): Promise<JournalRow[]> {
  const writer = journaled(repo, journal(ns))
  const stream = new Stream(ns, 'a', 'ledger')
  for (let sequence = 1; sequence <= count; sequence++) {
    await stream.commit(writer, stream.events(sequence, sequence))
  }

  return repo.readRows(world(ns), 0n, 1000)
}

// Commits `count` wide events, one per commit, into the world scope.
async function fillWide(repo: JournalBackend, ns: string, count: number): Promise<void> {
  const writer = journaled(repo, journal(ns, 200_000))
  const stream = new Stream(ns, 'a', 'ledger')
  for (let sequence = 1; sequence <= count; sequence++) {
    await stream.commit(writer, [stream.wide(sequence)])
  }
}

function positions(rows: readonly JournalRow[]): number[] {
  return rows.map((row) => Number(row.position))
}

// The table's compaction declaration, if any.
async function compactionDeclared(target: CompactionTarget): Promise<number | null> {
  return (await target.manifest())?.features.get(JOURNAL_COMPACTION_FEATURE) ?? null
}

// Begins a rebuild: the generation moves to odd under a held lock.
function beginRebuild(target: CompactionTarget): Promise<RebuildLease> {
  return target.repo.beginRebuild(`compaction-conformance-${namespace()}`, LEASE)
}

// Ends a rebuild: the generation moves to the next even one, unlocked.
async function endRebuild(target: CompactionTarget, lease: RebuildLease): Promise<void> {
  await target.repo.endRebuild(lease)
}

async function snapshotAndGone(target: CompactionTarget): Promise<void> {
  const {repo, views} = target
  const ns = namespace()
  const scope = world(ns)
  const live = await fill(repo, ns, 6)

  ensureEqual(await readFloor(repo, scope), 0n, 'an uncompacted floor')

  const report = await compactWithLease(repo, scope, 4n, owner(ns), LEASE)
  ensureEqual(
    [report.checkpoint, report.chunkCount, report.chunksWritten, report.rowsDeleted],
    [4n, 1, 1, 4n],
    'the report',
  )

  // The snapshot: one chunk holding the live rows 1..=4, byte for byte.
  const manifest = await readManifest(repo, scope)
  if (manifest === null) {
    throw new Failure('no manifest')
  }
  const digest = await manifestDigest(manifest)
  ensureEqual(manifest.checkpoint, 4n, 'the floor')
  ensureEqual(digest, report.digest, 'the reported digest')
  const chunk = await readChunk(repo, manifest, 0)
  ensureEqual([chunk.from, chunk.through], [1n, 4n], "the chunk's range")
  ensureEqual(chunk.rows, live.slice(0, 4), 'the chunk holds the live rows')

  // The rows at or below the floor are gone; the rows above it are untouched.
  ensureEqual(positions(await repo.readRows(scope, 0n, 100)), [5, 6], 'the remaining rows')
  const stored = await views.getViewAt(compactionPartitionKey(scope), MANIFEST_SORT_KEY)
  if (stored === null) {
    throw new Failure('the manifest row')
  }
  ensureEqual(stored.entityId, 'SYSTEM#JOURNAL', "the manifest's owner")
  ensureEqual(stored.entityType, COMPACTION_ENTITY_TYPE, "the manifest's type")

  // A reader below the floor is handed the snapshot, not a gap.
  for (const after of [0n, 2n, 3n]) {
    const gone = (await expectError(
      page(repo, scope, after, 10),
      `a page after ${after}`,
      (error) => error instanceof JournalGoneError,
    )) as JournalGoneError
    ensureEqual(
      [gone.after, gone.checkpoint, gone.chunkCount, gone.digest],
      [after, 4n, 1, digest],
      'the handoff',
    )
    ensureEqual(gone.resumeAfter, 4n, 'the resume cursor')
  }
  await expectError(
    pageAtEpoch(repo, scope, 2n, 10),
    'an epoch-checked page below the floor',
    (error) => error instanceof JournalGoneError,
  )

  // Resuming from the checkpoint reads the live tail with no gap.
  const resumed = await page(repo, scope, 4n, 10)
  ensureEqual(positions(resumed.rows), [5, 6], 'the resumed page')
  ensure(resumed.caughtUp, `the resumed page is caught up: ${canonical(resumed)}`)
  ensureEqual(await repo.readCounter(scope), 6n, 'the watermark is untouched')

  // The audit row, the released lease, and the feature marker.
  const audit = await readAudit(repo, scope, 4n)
  if (audit === null) {
    throw new Failure('no audit row')
  }
  ensureEqual(
    [audit.checkpoint, audit.digest, audit.chunks, audit.deleted, audit.owner],
    [4n, digest, 1, 4n, owner(ns)],
    'the audit row',
  )
  const lease = await readLease(repo, scope)
  ensure(lease?.kind === 'released' && lease.token === 1n, 'the lease is released')
  ensureEqual(await compactionDeclared(target), JOURNAL_COMPACTION_VERSION, 'the declaration')

  // Compacting the same checkpoint again is a no-op that rewrites the audit row.
  const again = await compactWithLease(repo, scope, 4n, owner(ns), LEASE)
  ensureEqual(
    [again.chunksWritten, again.rowsDeleted, again.digest],
    [0, 0n, digest],
    'a repeated run',
  )

  // And a later checkpoint extends the snapshot.
  const further = await compactWithLease(repo, scope, 6n, owner(ns), LEASE)
  ensureEqual([further.chunkCount, further.rowsDeleted], [1, 2n], 'the extension')
  const extended = await readManifest(repo, scope)
  if (extended === null) {
    throw new Failure('no manifest')
  }
  ensureEqual((await readChunk(repo, extended, 0)).rows, live, 'every row is in the snapshot')
  const caughtUp = await page(repo, scope, 6n, 10)
  ensure(
    caughtUp.rows.length === 0 && caughtUp.caughtUp,
    `a reader at the checkpoint is caught up: ${canonical(caughtUp)}`,
  )
}

// A canonical chunk, built from the storage format's rules rather than the engine's builder.
interface ExpectedChunk {
  readonly index: number
  readonly from: bigint
  readonly through: bigint
  readonly payload: Uint8Array
  readonly digest: string
}

// A chunk's payload: its header, then each row's stored payload, joined by `,`.
function chunkPayload(scope: string, index: number, rows: readonly JournalRow[]): Uint8Array {
  const from = rows[0]!.position
  const through = rows.at(-1)!.position
  const embedded = rows.map((row) => decoder.decode(encodeRow(scope, row.position, row.event)))

  return encoder.encode(
    `{"scope":${JSON.stringify(scope)},"index":${index},"from":${from},"through":${through},` +
      `"rows":[${embedded.join(',')}]}`,
  )
}

async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const digest = new Uint8Array(
    await globalThis.crypto.subtle.digest('SHA-256', new Uint8Array(bytes)),
  )

  return Array.from(digest, (byte) => byte.toString(16).padStart(2, '0')).join('')
}

async function expectedChunk(
  scope: string,
  index: number,
  rows: readonly JournalRow[],
): Promise<ExpectedChunk> {
  const payload = chunkPayload(scope, index, rows)

  return {
    index,
    from: rows[0]!.position,
    through: rows.at(-1)!.position,
    payload,
    digest: await sha256Hex(payload),
  }
}

// The canonical chunks of rows from position 1: a chunk closes before the row that would make it
// hold more than MAX_CHUNK_ROWS rows or a payload over MAX_CHUNK_BYTES.
async function canonicalChunks(
  scope: string,
  rows: readonly JournalRow[],
): Promise<ExpectedChunk[]> {
  const chunks: ExpectedChunk[] = []
  let open: JournalRow[] = []
  for (const row of rows) {
    const grown = [...open, row]
    const full =
      grown.length > MAX_CHUNK_ROWS ||
      chunkPayload(scope, chunks.length, grown).length > MAX_CHUNK_BYTES
    if (open.length > 0 && full) {
      chunks.push(await expectedChunk(scope, chunks.length, open))
      open = [row]
    } else {
      open = grown
    }
  }
  if (open.length > 0) {
    chunks.push(await expectedChunk(scope, chunks.length, open))
  }

  return chunks
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && a.every((byte, at) => byte === b[at])
}

async function chunksAreCanonical(target: CompactionTarget): Promise<void> {
  const {repo} = target
  const ns = namespace()
  const scope = world(ns)
  // Rows of about 133,000 bytes, so a chunk holds two.
  await fillWide(repo, ns, 7)
  const live = await repo.readRows(scope, 0n, 100)

  // In steps: through 3 is chunks [1,2] [3]; through 7 grows the tail to [3,4] and adds [5,6] [7].
  const first = await compactWithLease(repo, scope, 3n, owner(ns), LEASE)
  ensureEqual([first.chunkCount, first.chunksWritten], [2, 2], 'through 3')
  const second = await compactWithLease(repo, scope, 7n, owner(ns), LEASE)
  ensureEqual([second.chunkCount, second.chunksWritten], [4, 3], 'through 7')

  // One compaction through 7, computed from the live rows, is the same snapshot. The engine's
  // chunk builder isn't exported, so the expected chunks follow the storage format's rules.
  const expected = await canonicalChunks(scope, live)
  const manifest = await readManifest(repo, scope)
  if (manifest === null) {
    throw new Failure('no manifest')
  }
  ensureEqual(
    manifest.chunks,
    expected.map((chunk) => chunk.digest),
    'the chunk digests',
  )
  for (const chunk of expected) {
    const stored = await repo.readCompactionRow(scope, chunkSortKey(chunk.index))
    if (stored === null) {
      throw new Failure(`chunk ${chunk.index}`)
    }
    ensure(sameBytes(stored.payload, chunk.payload), `chunk ${chunk.index} differs`)
    const read = await readChunk(repo, manifest, chunk.index)
    ensureEqual([read.from, read.through], [chunk.from, chunk.through], "a chunk's range")
  }
  ensureEqual(
    expected.map((chunk) => [chunk.from, chunk.through]),
    [
      [1n, 2n],
      [3n, 4n],
      [5n, 6n],
      [7n, 7n],
    ],
    'the canonical boundaries',
  )
}

async function rebuildRestoresRows(target: CompactionTarget): Promise<void> {
  const {repo, views} = target
  const ns = namespace()
  const scope = world(ns)
  const live = await fill(repo, ns, 5)
  await compactWithLease(repo, scope, 3n, owner(ns), LEASE)
  const manifest = await readManifest(repo, scope)

  // The rebuild derives every row from the stored events, the compacted ones included, and leaves
  // the compaction partition alone.
  const events = await repo.events(new Stream(ns, 'a', 'ledger').id)
  await rebuildJournalWithLock(repo, events, LEASE)
  ensureEqual(await repo.readRows(scope, 0n, 100), live, 'the restored rows')
  ensureEqual(await readManifest(repo, scope), manifest, 'the manifest stays')

  // A reader below the floor is served the restored rows, which are the snapshot's rows.
  const served = await page(repo, scope, 1n, 10)
  ensureEqual(positions(served.rows), [2, 3, 4, 5], 'a page over restored rows')

  // A row missing below the floor is a gap the floor explains: gone.
  await views.deleteView(rowPartitionKey(scope), rowSortKey(2n))
  await expectError(
    page(repo, scope, 1n, 10),
    'a page whose gap is below the floor',
    (error) => error instanceof JournalGoneError && error.checkpoint === 3n,
  )

  // Compacting at the floor prunes the restored rows without building anything.
  const pruned = await compactWithLease(repo, scope, 3n, owner(ns), LEASE)
  ensureEqual([pruned.chunksWritten, pruned.rowsDeleted], [0, 2n], 'the prune')
  ensureEqual(positions(await repo.readRows(scope, 0n, 100)), [4, 5], 'the rows above the floor')
}

async function leaseLifecycle(target: CompactionTarget): Promise<void> {
  const {repo, clock} = target
  const ns = namespace()
  const scope = world(ns)
  await fill(repo, ns, 4)
  const [a, b, c] = [`${ns}-a`, `${ns}-b`, `${ns}-c`]

  const held = await beginCompaction(repo, scope, a, 10_000)
  ensureEqual(held.token, 1n, 'the first token')
  await expectError(
    beginCompaction(repo, scope, b, LEASE),
    'a second compaction of the scope',
    (error) =>
      error instanceof CompactionLeaseHeldError &&
      error.owner === a &&
      error.leaseExpiresAt === held.leaseExpiresAt,
  )

  // Renewing extends it; the old lease value no longer names the hold.
  clock.advance(1)
  const renewed = await renewCompaction(repo, held, 10_000)
  await expectError(
    compact(repo, held, 2n),
    'a compaction under the lease as it was before a renew',
    leaseLost,
  )

  // Lapsed, it is taken over with a new token, and the old holder writes nothing.
  clock.advance(11)
  const taken = await beginCompaction(repo, scope, b, LEASE)
  ensureEqual(taken.token, 2n, "the takeover's token")
  const superseded: [string, () => Promise<unknown>][] = [
    ['compact', () => compact(repo, renewed, 2n)],
    ['end', () => endCompaction(repo, renewed)],
    ['renew', () => renewCompaction(repo, renewed, LEASE)],
  ]
  for (const [what, call] of superseded) {
    await expectError(call(), `a superseded lease's ${what}`, leaseLost)
  }
  ensureEqual(await readManifest(repo, scope), null, 'the lost lease wrote nothing')
  ensureEqual((await repo.readRows(scope, 0n, 10)).length, 4, 'no row was deleted')

  // The operator force-releases the holder by name; the holder's next write fails, and another
  // compaction can begin at once.
  await expectError(
    forceReleaseCompaction(repo, scope, a),
    'a force-release naming an owner that does not hold the lease',
    leaseLost,
  )
  await forceReleaseCompaction(repo, scope, b)
  await expectError(compact(repo, taken, 2n), "a force-released lease's compaction", leaseLost)
  const third = await beginCompaction(repo, scope, c, LEASE)
  ensureEqual(third.token, 3n, 'the next token')

  // A lease that lapsed by the repository's clock stops its own run before writing.
  clock.advance(61)
  await expectError(compact(repo, third, 2n), "a lapsed lease's compaction", leaseLost)
  const {report} = await compact(repo, await renewCompaction(repo, third, LEASE), 2n)
  ensureEqual(report.checkpoint, 2n, 'a renewed lapsed lease compacts')

  // A released lease cannot be released again.
  const stored = await readLease(repo, scope)
  if (stored?.kind !== 'held') {
    throw new Failure('the lease should be held')
  }
  await endCompaction(repo, stored.lease)
  await expectError(endCompaction(repo, stored.lease), 'a second release', leaseLost)

  // A run renews a lease with less than half of its length left, in place, so the caller
  // releases the lease the run ended with.
  const short = await beginCompaction(repo, scope, c, 10_000)
  clock.advance(6)
  const {lease} = await compact(repo, short, 3n)
  ensureEqual(lease.leaseExpiresAt, clock.seconds() + 10, 'the run renewed the lease')
  await endCompaction(repo, lease)
}

async function epochFencing(target: CompactionTarget): Promise<void> {
  const {repo} = target
  const ns = namespace()
  const scope = world(ns)
  await fill(repo, ns, 4)
  const held = await beginCompaction(repo, scope, owner(ns), LEASE)

  // While a rebuild holds the fence, compaction does not start, and neither does a lease.
  const rebuild = await beginRebuild(target)
  const during = await settle(compact(repo, held, 2n))
  const leased = await settle(beginCompaction(repo, scope, `${ns}-other`, LEASE))
  await endRebuild(target, rebuild)
  await expectError(replayed(during), 'a compaction during a rebuild', rebuildUnavailable)
  await expectError(replayed(leased), 'a lease during a rebuild', rebuildUnavailable)

  // A write made under an epoch a rebuild has since moved writes nothing.
  const epoch = await repo.observeRebuildEpoch()
  await endRebuild(target, await beginRebuild(target))
  const probe = compactionView(scope, 'probe', encoder.encode('{}'))
  ensureEqual(
    await repo.writeCompaction(epoch, [{kind: 'put', row: probe, expect: {kind: 'any'}}]),
    {kind: 'epochChanged'},
    'a write under a stale epoch',
  )
  ensureEqual(await repo.readCompactionRow(scope, 'probe'), null, 'the fenced write wrote nothing')

  // A failed condition names its write and writes nothing either.
  const current = await repo.observeRebuildEpoch()
  ensureEqual(
    await repo.writeCompaction(current, [
      {kind: 'put', row: probe, expect: {kind: 'any'}},
      {
        kind: 'check',
        pk: compactionPartitionKey(scope),
        sk: LEASE_SORT_KEY,
        expect: {kind: 'absent'},
      },
    ]),
    {kind: 'refused', at: 1},
    'a write whose check fails',
  )
  ensureEqual(await repo.readCompactionRow(scope, 'probe'), null, 'the refused write wrote nothing')

  // Under the current epoch the lease's run completes.
  const {report, lease} = await compact(repo, held, 2n)
  ensureEqual(report.rowsDeleted, 2n, 'the run')
  await endCompaction(repo, lease)
}

async function allocationRespectsTheFloor(target: CompactionTarget): Promise<void> {
  const {repo, views} = target
  const ns = namespace()
  const scope = world(ns)
  await fill(repo, ns, 6)
  await compactWithLease(repo, scope, 6n, owner(ns), LEASE)

  // Every row is compacted and the counter is lost: without the floor a commit would allocate
  // position 1 again, which no row guards any more.
  await views.deleteView(counterPartitionKey(scope), COUNTER_SORT_KEY)

  const stream = new Stream(ns, 'a', 'ledger')
  await stream.commit(journaled(repo, journal(ns)), stream.events(7, 7))

  const stored = await repo.events(stream.id)
  const held = allocations(stored[6]!.metadata)
  ensureEqual(held.length, 1, 'one allocation')
  ensureEqual(held[0]!.position, 7n, 'the position after the floor')
  ensureEqual(await repo.readCounter(scope), 7n, 'the counter is written back')
  const resumed = await page(repo, scope, 6n, 10)
  ensureEqual(positions(resumed.rows), [7], 'the new row')
}

async function staleWritersFailOnTheCounter(target: CompactionTarget): Promise<void> {
  const {repo} = target
  const ns = namespace()
  const scope = world(ns)
  await fill(repo, ns, 4)
  await compactWithLease(repo, scope, 4n, owner(ns), LEASE)

  // A writer that read the counter at 2, before the commits of 3 and 4 and the compaction that
  // deleted their rows. Its rows at 3 would be absent now: only the counter's condition (the
  // stored counter is 4, not the 2 it read) refuses the stale attempt, which then reads again.
  const lagging = new Lagging(repo, 1)
  lagging.staleCounter = 2n
  const stream = new Stream(ns, 'a', 'ledger')
  await stream.commit(lagging.journaled(journal(ns)), stream.events(5, 5))

  const stored = await repo.events(stream.id)
  ensureEqual(
    allocations(stored[4]!.metadata)[0]?.position,
    5n,
    'the position after the live counter',
  )
  ensure(lagging.backoffs >= 1, 'the stale attempt was retried')
  ensureEqual(await repo.readCounter(scope), 5n, 'the counter')
  ensureEqual(
    stored.map((event) => allocations(event.metadata)[0]?.position),
    [1n, 2n, 3n, 4n, 5n],
    'no position is held twice',
  )
}

async function refusals(target: CompactionTarget): Promise<void> {
  const {repo, views} = target
  const ns = namespace()
  const scope = world(ns)
  await fill(repo, ns, 5)
  const held = await beginCompaction(repo, scope, owner(ns), LEASE)

  for (const through of [0n, 6n]) {
    await expectError(
      compact(repo, held, through),
      `a compaction through ${through}`,
      coded('invalidArgument'),
    )
  }

  // A missing row is a gap compaction cannot snapshot.
  await views.deleteView(rowPartitionKey(scope), rowSortKey(2n))
  await expectError(compact(repo, held, 4n), 'a compaction over a gap', coded('invalidJournal'))
  ensureEqual(await readManifest(repo, scope), null, 'nothing was written')
  ensureEqual(positions(await repo.readRows(scope, 0n, 10)), [1, 3, 4, 5], 'nothing was deleted')

  // Above the gap it compacts once the row is back; below the floor it is refused.
  const events = await repo.events(new Stream(ns, 'a', 'ledger').id)
  await views.putView(rowView(scope, 2n, events[1]!))
  const {lease} = await compact(repo, held, 4n)
  await expectError(
    compact(repo, lease, 3n),
    'a compaction below the floor',
    coded('invalidArgument'),
  )
  await endCompaction(repo, lease)
}

async function refusedRunsChangeNothing(target: CompactionTarget): Promise<void> {
  const {repo, views} = target
  const ns = namespace()
  const scope = world(ns)
  await fill(repo, ns, 3)
  const declared = await compactionDeclared(target)

  // A checkpoint of 0, one above the watermark, and a mistyped scope (no journal at all) are
  // refused before a lease is taken.
  const mistyped = `${ns}-wrold`
  const runs: [string, bigint][] = [
    [scope, 0n],
    [scope, 4n],
    [mistyped, 1n],
  ]
  for (const [at, through] of runs) {
    await expectError(
      compactWithLease(repo, at, through, owner(ns), LEASE),
      `a compaction of ${at} through ${through}`,
      coded('invalidArgument'),
    )
    ensure(
      (await readLease(repo, at)) === null,
      `a refused compaction of ${at} through ${through} took no lease`,
    )
  }

  // A table declaring a feature this build does not model is refused before the checkpoint is
  // looked at, as during a rebuild.
  const unmodeledFeature = `${ns}-unmodeled`
  await target.declare((draft) => {
    draft.features.set(unmodeledFeature, 1)
  })
  const unmodeled = await settle(compactWithLease(repo, scope, 0n, owner(ns), LEASE))
  await target.declare((draft) => {
    draft.features.delete(unmodeledFeature)
  })
  await expectError(
    replayed(unmodeled),
    'a compaction of a table declaring an unmodeled feature',
    (error) =>
      error instanceof UnsupportedFeatureError &&
      error.unsupported.some((pair) => pair.name === unmodeledFeature),
  )

  const rebuild = await beginRebuild(target)
  const during = await settle(compactWithLease(repo, scope, 0n, owner(ns), LEASE))
  await endRebuild(target, rebuild)
  await expectError(replayed(during), 'a compaction during a rebuild', rebuildUnavailable)
  ensure((await readLease(repo, scope)) === null, 'neither took a lease')

  // A lease alone declares nothing, and neither does a run its preflight refuses.
  await views.deleteView(rowPartitionKey(scope), rowSortKey(2n))
  const held = await beginCompaction(repo, scope, owner(ns), LEASE)
  await expectError(compact(repo, held, 3n), 'a compaction over a gap', coded('invalidJournal'))
  await endCompaction(repo, held)

  ensureEqual(await compactionDeclared(target), declared, 'the compaction declaration')
  ensureEqual(await readManifest(repo, scope), null, 'no manifest')
}

async function gappedRangeWritesNothing(target: CompactionTarget): Promise<void> {
  const {repo, views} = target
  const ns = namespace()
  const scope = world(ns)
  // Rows of about 133,000 bytes, so a chunk holds two: [1,2] and [3,4] close before the gap.
  await fillWide(repo, ns, 6)
  await views.deleteView(rowPartitionKey(scope), rowSortKey(5n))

  await expectError(
    compactWithLease(repo, scope, 6n, owner(ns), LEASE),
    'a compaction over a gap after closed chunks',
    coded('invalidJournal'),
  )
  for (const sortKey of [chunkSortKey(0), chunkSortKey(1), MANIFEST_SORT_KEY]) {
    ensureEqual(await repo.readCompactionRow(scope, sortKey), null, `${sortKey} was not written`)
  }
  ensureEqual(positions(await repo.readRows(scope, 0n, 10)), [1, 2, 3, 4, 6], 'no row was deleted')
}

async function foreignRowsAreRefused(target: CompactionTarget): Promise<void> {
  const {repo, views} = target
  const ns = namespace()
  const scope = world(ns)
  const stream = new Stream(ns, 'a', 'ledger')
  await fill(repo, ns, 4)

  // A row at the manifest's key, written by a stream (as an application projector or a direct
  // view write would): it claims the scope is compacted through 4 with a well-formed payload.
  const forged = manifestPayload({scope, checkpoint: 4n, chunks: ['0'.repeat(64)]})
  await views.putView({
    pk: compactionPartitionKey(scope),
    sk: MANIFEST_SORT_KEY,
    entityId: stream.id,
    entityType: COMPACTION_ENTITY_TYPE,
    payload: forged,
  })

  await expectError(
    compactWithLease(repo, scope, 4n, owner(ns), LEASE),
    'a compaction over a foreign manifest',
    foreign,
  )
  await expectError(readManifest(repo, scope), 'reading a foreign manifest', foreign)
  ensureEqual(positions(await repo.readRows(scope, 0n, 10)), [1, 2, 3, 4], 'no row was deleted')

  // A reader that meets a gap does not take a foreign floor for a 410.
  await views.deleteView(rowPartitionKey(scope), rowSortKey(1n))
  await expectError(page(repo, scope, 0n, 10), 'a page under a foreign manifest', foreign)

  // A foreign lease row is refused the same way.
  await views.deleteView(compactionPartitionKey(scope), MANIFEST_SORT_KEY)
  await views.putView({
    pk: compactionPartitionKey(scope),
    sk: LEASE_SORT_KEY,
    entityId: stream.id,
    entityType: COMPACTION_ENTITY_TYPE,
    payload: releasedPayload(scope, 7n),
  })
  await expectError(
    beginCompaction(repo, scope, owner(ns), LEASE),
    'a lease over a foreign lease row',
    foreign,
  )
}

async function movedSnapshot(target: CompactionTarget): Promise<void> {
  const {repo} = target
  const ns = namespace()
  const scope = world(ns)
  await fill(repo, ns, 6)

  await compactWithLease(repo, scope, 3n, owner(ns), LEASE)
  const old = await readManifest(repo, scope)
  if (old === null) {
    throw new Failure('no manifest')
  }
  await compactWithLease(repo, scope, 6n, owner(ns), LEASE)

  await expectError(
    readChunk(repo, old, 0),
    'a chunk of a superseded manifest',
    (error) =>
      error instanceof JournalSnapshotMovedError && error.checkpoint === 3n && isRetryable(error),
  )
  await expectError(
    readChunk(repo, old, 1),
    'a chunk the manifest does not have',
    coded('invalidArgument'),
  )
}

/**
 * A repository whose counter reads are stale for a number of reads (the position scan that backs
 * repair is stale as long), standing in for a writer that read the counters just before other
 * writers committed. It counts the journal's backoffs.
 */
class Lagging implements JournalBackend {
  readonly inner: JournalBackend
  readonly #stale: {left: number}
  staleCounter = 0n
  backoffs = 0

  constructor(inner: JournalBackend, staleReads: number) {
    this.inner = inner
    this.#stale = {left: staleReads}
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
    declareJournal: (types: readonly string[], options?: CallOptions) =>
      this.inner[JOURNAL_MARKER].declareJournal(types, options),
    retractJournal: (declared: JournalDeclared, options?: CallOptions) =>
      this.inner[JOURNAL_MARKER].retractJournal(declared, options),
    checkCompactionMarker: (options?: CallOptions) =>
      this.inner[JOURNAL_MARKER].checkCompactionMarker(options),
    declareCompaction: (options?: CallOptions) =>
      this.inner[JOURNAL_MARKER].declareCompaction(options),
  }

  readCompactionRow(
    scope: string,
    sortKey: string,
    options?: CallOptions,
  ): Promise<ViewRow | null> {
    return this.inner.readCompactionRow(scope, sortKey, options)
  }

  async readCounter(scope: string, options?: CallOptions): Promise<bigint> {
    if (this.#stale.left > 0) {
      this.#stale.left--

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

  commit(commit: Commit, options?: CallOptions): Promise<void> {
    return this.inner.commit(commit, options)
  }

  commitTransaction(transaction: TransactionCommit, options?: CallOptions): Promise<void> {
    return this.inner.commitTransaction(transaction, options)
  }

  writesViews(): boolean {
    return this.inner.writesViews?.() === true
  }

  appliesExpectedPayload(): boolean {
    return this.inner.appliesExpectedPayload?.() === true
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
}
