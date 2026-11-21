// Journal compaction (ADR 0005; storage format, "Journal compaction"): a scope's oldest rows are
// stored as a snapshot (a manifest and its chunks) and deleted, under a lease, fenced on the
// rebuild epoch. The checkpoint becomes the scope's floor, and a reader below it is handed the
// snapshot.

import {EvtError, RebuildEpochChangedError, TransientError} from '../errors.js'
import {JOURNAL_MARKER, type JournalMarker} from '../features.js'
import type {CallOptions} from '../repository.js'
import {utf8Length} from '../utf8.js'
import type {ViewRow} from '../views.js'
import {
  readEngineRow,
  type BuiltChunk,
  ChunkBuilder,
  type CompactionAudit,
  type CompactionChunk,
  type CompactionLease,
  type CompactionManifest,
  type CompactionStore,
  type CompactionWrite,
  type Expect,
  JournalSnapshotMovedError,
  LEASE_SORT_KEY,
  MANIFEST_SORT_KEY,
  MAX_CHUNK_BYTES,
  MAX_COMPACTION_WRITES,
  auditPayload,
  auditSortKey,
  chunkSortKey,
  compactionPartitionKey,
  compactionView,
  decodeAudit,
  decodeChunk,
  decodeLease,
  decodeManifest,
  leasePayload,
  manifestDigest,
  manifestPayload,
  releasedPayload,
  sha256Hex,
  withDigest,
  writeKey,
  type LeaseRow,
  type RawBuilt,
} from './compaction.js'
import {MAX_PAGE_ROWS} from './read.js'
import type {JournalRow} from './row.js'
import {checkScope, rowPartitionKey, rowSortKey} from './scope.js'

/** A compaction store that can also check and declare the journal's features on its manifest. */
export type CompactionBackend = CompactionStore & {readonly [JOURNAL_MARKER]: JournalMarker}

/** The owner {@link compactWithLease} is usually given, for a scheduled compaction. */
export const COMPACTION_OWNER = 'journal-compaction'

/** The longest lease owner, in UTF-8 bytes. */
export const MAX_COMPACTION_OWNER_BYTES = 512

// How many times an acquire reads and puts the lease against racing acquirers.
const MAX_LEASE_ATTEMPTS = 8
// Row deletes per transaction: the lease check takes the last action.
const DELETE_BATCH = MAX_COMPACTION_WRITES - 1

/** What a compaction run did. */
export interface CompactionReport {
  readonly scope: string
  /** The scope's floor now. */
  readonly checkpoint: bigint
  /** The manifest's digest. */
  readonly digest: string
  readonly chunkCount: number
  readonly chunksWritten: number
  readonly rowsDeleted: bigint
}

/**
 * Another compaction of the scope holds an unexpired lease. Nothing was written.
 */
export class CompactionLeaseHeldError extends EvtError {
  override readonly name = 'CompactionLeaseHeldError'
  override readonly code = 'compactionLeaseHeld'
  readonly scope: string
  readonly owner: string
  readonly leaseExpiresAt: number

  constructor(scope: string, owner: string, leaseExpiresAt: number) {
    super(
      'compactionLeaseHeld',
      `the compaction lease of journal scope ${JSON.stringify(scope)} is held by ` +
        `${JSON.stringify(owner)} until ${leaseExpiresAt} (Unix seconds)`,
    )
    this.scope = scope
    this.owner = owner
    this.leaseExpiresAt = leaseExpiresAt
  }
}

/** A compaction's lease no longer matches: it lapsed, or another owner took it over. */
export class CompactionLeaseLostError extends EvtError {
  override readonly name = 'CompactionLeaseLostError'
  override readonly code = 'compactionLeaseLost'
  readonly scope: string
  readonly owner: string

  constructor(scope: string, owner: string) {
    super(
      'compactionLeaseLost',
      `compaction owner ${JSON.stringify(owner)} does not hold the lease of journal scope ` +
        JSON.stringify(scope),
    )
    this.scope = scope
    this.owner = owner
  }
}

/**
 * Chunk `index` of a manifest's snapshot, checked against the manifest's digest. A chunk missing
 * or not matching is a `JournalSnapshotMovedError`: read the manifest again.
 */
export async function readChunk(
  store: CompactionStore,
  manifest: CompactionManifest,
  index: number,
  options?: CallOptions,
): Promise<CompactionChunk> {
  const {scope} = manifest
  const digest = manifest.chunks[index]
  if (digest === undefined || !Number.isSafeInteger(index)) {
    throw new EvtError(
      'invalidArgument',
      `the snapshot of ${scope} has ${manifest.chunks.length} chunks; there is no chunk ${index}`,
    )
  }

  const row = await readEngineRow(store, scope, chunkSortKey(index), options)
  if (row === null || (await sha256Hex(row.payload)) !== digest) {
    throw new JournalSnapshotMovedError(scope, manifest.checkpoint)
  }
  const chunk = decodeChunk(scope, row.payload)
  if (chunk.index !== index) {
    throw new EvtError(
      'invalidRecord',
      `chunk ${index} of ${scope} says it is chunk ${chunk.index}`,
    )
  }

  return chunk
}

/** The audit row of a compaction through `checkpoint`, or null. */
export async function readAudit(
  store: CompactionStore,
  scope: string,
  checkpoint: bigint,
  options?: CallOptions,
): Promise<CompactionAudit | null> {
  const row = await readEngineRow(store, scope, auditSortKey(checkpoint), options)

  return row === null ? null : decodeAudit(scope, row.payload)
}

/** The scope's lease row, held or released, or null when it never had one. */
export async function readLease(
  store: CompactionStore,
  scope: string,
  options?: CallOptions,
): Promise<LeaseRow | null> {
  const row = await readEngineRow(store, scope, LEASE_SORT_KEY, options)

  return row === null ? null : decodeLease(scope, row.payload)
}

// A lease's length in whole seconds, rounded up: at least one second, with a valid owner.
function leaseSeconds(owner: string, leaseMs: number): number {
  if (typeof owner !== 'string' || owner === '') {
    throw new EvtError('invalidArgument', 'a compaction owner must not be empty')
  }
  if (utf8Length(owner) > MAX_COMPACTION_OWNER_BYTES) {
    throw new EvtError(
      'invalidArgument',
      `a compaction owner is at most ${MAX_COMPACTION_OWNER_BYTES} bytes`,
    )
  }
  if (owner.includes('\0')) {
    throw new EvtError('invalidArgument', 'a compaction owner must not contain U+0000')
  }
  if (!Number.isFinite(leaseMs) || leaseMs < 1000) {
    throw new EvtError('invalidArgument', `a lease is at least one second; got ${leaseMs} ms`)
  }

  return Math.ceil(leaseMs / 1000)
}

// The error a write refused by its epoch check reports: the generation it finds now.
async function epochError(
  store: CompactionStore,
  epoch: bigint,
  options?: CallOptions,
): Promise<Error> {
  try {
    const current = await store.observeRebuildEpoch(options)
    if (current !== epoch) {
      return new RebuildEpochChangedError(epoch, current)
    }

    return new TransientError('the rebuild generation moved during a compaction write')
  } catch (error) {
    return error as Error
  }
}

/**
 * Takes the scope's compaction lease for `owner`, for `leaseMs` (rounded up to whole seconds):
 * the next token, conditioned on the lease row it read. An unexpired lease is refused with a
 * {@link CompactionLeaseHeldError}; a lapsed one is taken over. The rebuild epoch must be settled
 * and the feature marker one this package models. It declares nothing.
 */
export async function beginCompaction(
  store: CompactionBackend,
  scope: string,
  owner: string,
  leaseMs: number,
  options?: CallOptions,
): Promise<CompactionLease> {
  const seconds = leaseSeconds(owner, leaseMs)
  checkScope(scope)
  const epoch = await store.observeRebuildEpoch(options)
  await store[JOURNAL_MARKER].checkCompactionMarker(options)

  // Each attempt reads the lease again after a racing acquirer's put.
  /* oxlint-disable no-await-in-loop */
  for (let attempt = 0; attempt < MAX_LEASE_ATTEMPTS; attempt++) {
    const stored = await readEngineRow(store, scope, LEASE_SORT_KEY, options)
    const now = store.compactionNow()
    let token = 1n
    let expect: Expect = {kind: 'absent'}
    if (stored !== null) {
      const lease = decodeLease(scope, stored.payload)
      if (lease.kind === 'held' && lease.lease.leaseExpiresAt >= now) {
        throw new CompactionLeaseHeldError(scope, lease.lease.owner, lease.lease.leaseExpiresAt)
      }
      token = (lease.kind === 'held' ? lease.lease.token : lease.token) + 1n
      expect = {kind: 'payload', payload: stored.payload}
    }

    const acquired: CompactionLease = {
      scope,
      owner,
      token,
      leaseExpiresAt: now + seconds,
      leaseSeconds: seconds,
    }
    const written = await store.writeCompaction(
      epoch,
      [{kind: 'put', row: compactionView(scope, LEASE_SORT_KEY, leasePayload(acquired)), expect}],
      options,
    )
    if (written.kind === 'written') {
      return acquired
    }
    if (written.kind === 'epochChanged') {
      throw await epochError(store, epoch, options)
    }
  }
  /* oxlint-enable no-await-in-loop */

  throw new TransientError(`the compaction lease of ${scope} kept changing`)
}

// Replaces a held lease's row, conditioned on its exact payload.
async function replaceLease(
  store: CompactionStore,
  lease: CompactionLease,
  payload: Uint8Array,
  options?: CallOptions,
): Promise<void> {
  const written = await store.writeCompaction(
    null,
    [
      {
        kind: 'put',
        row: compactionView(lease.scope, LEASE_SORT_KEY, payload),
        expect: {kind: 'payload', payload: leasePayload(lease)},
      },
    ],
    options,
  )
  if (written.kind !== 'written') {
    throw new CompactionLeaseLostError(lease.scope, lease.owner)
  }
}

/** Extends a held lease by `extendMs` from now, or throws a {@link CompactionLeaseLostError}. */
export async function renewCompaction(
  store: CompactionStore,
  lease: CompactionLease,
  extendMs: number,
  options?: CallOptions,
): Promise<CompactionLease> {
  const seconds = leaseSeconds(lease.owner, extendMs)
  const renewed = {...lease, leaseExpiresAt: store.compactionNow() + seconds, leaseSeconds: seconds}
  await replaceLease(store, lease, leasePayload(renewed), options)

  return renewed
}

/** Releases a held lease, keeping its token, or throws a {@link CompactionLeaseLostError}. */
export function endCompaction(
  store: CompactionStore,
  lease: CompactionLease,
  options?: CallOptions,
): Promise<void> {
  return replaceLease(store, lease, releasedPayload(lease.scope, lease.token), options)
}

/**
 * The operator's release: releases `owner`'s lease of the scope whatever its token or expiry, or
 * throws a {@link CompactionLeaseLostError} when `owner` holds none.
 */
export async function forceReleaseCompaction(
  store: CompactionStore,
  scope: string,
  owner: string,
  options?: CallOptions,
): Promise<void> {
  leaseSeconds(owner, 1000)
  const row = await readEngineRow(store, scope, LEASE_SORT_KEY, options)
  const lease = row === null ? null : decodeLease(scope, row.payload)
  if (lease === null || lease.kind !== 'held' || lease.lease.owner !== owner) {
    throw new CompactionLeaseLostError(scope, owner)
  }

  await replaceLease(store, lease.lease, releasedPayload(scope, lease.lease.token), options)
}

// A run: its lease, which it renews in place, and the epoch every write is fenced on.
class Run {
  readonly store: CompactionStore
  lease: CompactionLease
  readonly epoch: bigint
  readonly options: CallOptions | undefined

  constructor(
    store: CompactionStore,
    lease: CompactionLease,
    epoch: bigint,
    options: CallOptions | undefined,
  ) {
    this.store = store
    this.lease = lease
    this.epoch = epoch
    this.options = options
  }

  // Sends writes with a check of the lease's exact payload, fenced on the epoch.
  async write(writes: CompactionWrite[]): Promise<void> {
    await this.keep()
    const {scope} = this.lease
    const check = writes.length
    const all: CompactionWrite[] = [
      ...writes,
      {
        kind: 'check',
        pk: compactionPartitionKey(scope),
        sk: LEASE_SORT_KEY,
        expect: {kind: 'payload', payload: leasePayload(this.lease)},
      },
    ]
    const written = await this.store.writeCompaction(this.epoch, all, this.options)
    if (written.kind === 'written') {
      return
    }
    if (written.kind === 'epochChanged') {
      throw await epochError(this.store, this.epoch, this.options)
    }
    // Only a lease holder writes the manifest, so a manifest that changed under the run means it
    // lost the lease as surely as a failed lease check.
    if (written.at === check || writeKey(all[written.at]!).sk === MANIFEST_SORT_KEY) {
      throw new CompactionLeaseLostError(scope, this.lease.owner)
    }
    const {pk, sk} = writeKey(all[written.at]!)
    throw new EvtError(
      'invalidRecord',
      `a compaction write to ${pk}/${sk} of ${scope} found a row it does not own`,
    )
  }

  // Stops a run whose lease lapsed, and renews one with less than half of it left.
  async keep(): Promise<void> {
    const now = this.store.compactionNow()
    if (this.lease.leaseExpiresAt < now) {
      throw new CompactionLeaseLostError(this.lease.scope, this.lease.owner)
    }
    const seconds = this.lease.leaseSeconds
    if (seconds > 0 && this.lease.leaseExpiresAt - now < Math.floor((seconds + 1) / 2)) {
      this.lease = await renewCompaction(this.store, this.lease, seconds * 1000, this.options)
    }
  }

  chunkWrite(chunk: BuiltChunk): CompactionWrite {
    return {
      kind: 'put',
      row: compactionView(this.lease.scope, chunkSortKey(chunk.index), chunk.payload),
      expect: {kind: 'any'},
    }
  }
}

/**
 * Compacts the scope through `through` under a held lease: with `floor <= through <= high
 * watermark` and `through >= 1`, under a settled rebuild epoch. Before its first write, once the
 * checkpoint and the preflight pass, it declares `journal-compaction` on the feature marker. It
 * builds the chunks above the floor (a preflight first, which writes nothing and refuses a missing
 * row or an oversized manifest), swaps the manifest, deletes the rows at or below `through`, and
 * writes the audit row. Every step is idempotent: a run that stops is finished by compacting
 * again. The lease is renewed in place as the run goes, so `lease` ends as the one it holds.
 */
export async function compact(
  store: CompactionBackend,
  lease: CompactionLease,
  through: bigint,
  options?: CallOptions,
): Promise<{report: CompactionReport; lease: CompactionLease}> {
  const {scope, owner, token} = lease
  const epoch = await store.observeRebuildEpoch(options)
  const run = new Run(store, lease, epoch, options)
  const {stored, old} = await checkCheckpoint(store, scope, through, options)
  const floor = old?.checkpoint ?? 0n

  let manifest: CompactionManifest
  let chunksWritten = 0
  if (through > floor) {
    ;({manifest, written: chunksWritten} = await build(run, store, old, stored, through))
  } else {
    await store[JOURNAL_MARKER].declareCompaction(options)
    manifest = old!
  }

  const rowsDeleted = await deleteRows(run, through)
  const digest = await manifestDigest(manifest)
  const audit: CompactionAudit = {
    scope,
    checkpoint: through,
    digest,
    chunks: manifest.chunks.length,
    deleted: rowsDeleted,
    owner,
    token,
    completedAt: store.compactionNow(),
  }
  await run.write([
    {
      kind: 'put',
      row: compactionView(scope, auditSortKey(through), auditPayload(audit)),
      expect: {kind: 'any'},
    },
  ])

  return {
    report: {
      scope,
      checkpoint: through,
      digest,
      chunkCount: manifest.chunks.length,
      chunksWritten,
      rowsDeleted,
    },
    lease: run.lease,
  }
}

// Reads the manifest and checks `through` against the floor and the high watermark.
async function checkCheckpoint(
  store: CompactionStore,
  scope: string,
  through: bigint,
  options?: CallOptions,
): Promise<{stored: ViewRow | null; old: CompactionManifest | null}> {
  const stored = await readEngineRow(store, scope, MANIFEST_SORT_KEY, options)
  const old = stored === null ? null : decodeManifest(scope, stored.payload)
  const floor = old?.checkpoint ?? 0n
  if (through === 0n || through < floor) {
    throw new EvtError(
      'invalidArgument',
      `${scope} is compacted through ${floor}; compact through ${floor} or more, not ${through}`,
    )
  }
  const watermark = await store.readCounter(scope, options)
  if (through > watermark) {
    throw new EvtError(
      'invalidArgument',
      `${scope} has allocated through ${watermark}; it cannot be compacted through ${through}`,
    )
  }

  return {stored, old}
}

/**
 * Checks the rebuild fence, the marker and the checkpoint first, so a run they refuse takes no
 * lease; then takes a lease for `owner`, compacts through `through`, and releases it, after a
 * failure too.
 */
export async function compactWithLease(
  store: CompactionBackend,
  scope: string,
  through: bigint,
  owner: string,
  leaseMs: number,
  options?: CallOptions,
): Promise<CompactionReport> {
  checkScope(scope)
  await store.observeRebuildEpoch(options)
  await store[JOURNAL_MARKER].checkCompactionMarker(options)
  await checkCheckpoint(store, scope, through, options)

  let held = await beginCompaction(store, scope, owner, leaseMs, options)
  let report: CompactionReport
  try {
    const result = await compact(store, held, through, options)
    report = result.report
    held = result.lease
  } catch (error) {
    try {
      await endCompaction(store, held, options)
    } catch {
      // The run's own failure is what the caller needs.
    }
    throw error
  }
  await endCompaction(store, held, options)

  return report
}

// Streams the chunks a run builds: the manifest's last chunk resumed (checked against its digest),
// then the rows above the floor through `through`, which must all be there.
class Chunks {
  readonly #store: CompactionStore
  readonly #scope: string
  readonly #through: bigint
  readonly #options: CallOptions | undefined
  #builder: ChunkBuilder | null
  #after: bigint
  #rows: JournalRow[] = []

  private constructor(
    store: CompactionStore,
    scope: string,
    builder: ChunkBuilder,
    after: bigint,
    through: bigint,
    options: CallOptions | undefined,
  ) {
    this.#store = store
    this.#scope = scope
    this.#builder = builder
    this.#after = after
    this.#through = through
    this.#options = options
  }

  static async start(
    store: CompactionStore,
    scope: string,
    old: CompactionManifest | null,
    through: bigint,
    options: CallOptions | undefined,
  ): Promise<Chunks> {
    if (old === null) {
      return new Chunks(store, scope, new ChunkBuilder(scope, 0, 1n), 0n, through, options)
    }

    const tail = old.chunks.length - 1
    const row = await readEngineRow(store, scope, chunkSortKey(tail), options)
    if (row === null || (await sha256Hex(row.payload)) !== old.chunks[tail]) {
      throw new EvtError(
        'invalidJournal',
        `invalid journal: chunk ${tail} of ${scope} does not match its manifest`,
      )
    }

    return new Chunks(
      store,
      scope,
      ChunkBuilder.resume(scope, row.payload),
      old.checkpoint,
      through,
      options,
    )
  }

  // The next closed chunk, then the last one, then null.
  async next(): Promise<BuiltChunk | null> {
    // Each page of rows waits for the one before it.
    /* oxlint-disable no-await-in-loop */
    for (;;) {
      if (this.#builder === null) {
        return null
      }
      if (this.#after >= this.#through) {
        const last = this.#builder.build()
        this.#builder = null

        return withDigest(last)
      }
      if (this.#rows.length === 0) {
        const page = await this.#store.readRows(
          this.#scope,
          this.#after,
          MAX_PAGE_ROWS,
          this.#options,
        )
        this.#rows = page.filter((row) => row.position <= this.#through)
      }
      const row = this.#rows.shift()
      if (row === undefined || row.position !== this.#after + 1n) {
        throw new EvtError(
          'invalidJournal',
          `invalid journal: ${this.#scope} has no row at position ${this.#after + 1n} to compact`,
        )
      }
      this.#after = row.position
      const closed: RawBuilt | null = this.#builder.push(row)
      if (closed !== null) {
        return withDigest(closed)
      }
    }
    /* oxlint-enable no-await-in-loop */
  }
}

// Streams the whole range, writing nothing, so a missing row or an oversized manifest is refused
// before any write. The run keeps its lease meanwhile.
async function preflight(
  run: Run,
  store: CompactionStore,
  old: CompactionManifest | null,
  through: bigint,
): Promise<CompactionManifest> {
  const {scope} = run.lease
  const digests = old === null ? [] : old.chunks.slice(0, -1)
  const chunks = await Chunks.start(store, scope, old, through, run.options)
  /* oxlint-disable no-await-in-loop */
  for (let chunk = await chunks.next(); chunk !== null; chunk = await chunks.next()) {
    digests.push(chunk.digest)
    await run.keep()
  }
  /* oxlint-enable no-await-in-loop */

  const manifest = {scope, checkpoint: through, chunks: digests}
  const size = manifestPayload(manifest).length
  if (size > MAX_CHUNK_BYTES) {
    throw new EvtError(
      'invalidJournal',
      `invalid journal: the manifest of ${scope} through ${through} would list ` +
        `${digests.length} chunks in ${size} bytes, over ${MAX_CHUNK_BYTES}; compact in ` +
        'smaller steps',
    )
  }

  return manifest
}

// Builds and writes the chunks, then swaps the manifest, with the rewritten last chunk of the old
// manifest when it changed.
async function build(
  run: Run,
  store: CompactionBackend,
  old: CompactionManifest | null,
  stored: ViewRow | null,
  through: bigint,
): Promise<{manifest: CompactionManifest; written: number}> {
  const {scope} = run.lease
  const manifest = await preflight(run, store, old, through)
  await store[JOURNAL_MARKER].declareCompaction(run.options)

  const tail = old === null ? null : old.chunks.length - 1
  let written = 0
  let heldBack: BuiltChunk | null = null
  const chunks = await Chunks.start(store, scope, old, through, run.options)
  /* oxlint-disable no-await-in-loop */
  for (let chunk = await chunks.next(); chunk !== null; chunk = await chunks.next()) {
    if (manifest.chunks[chunk.index] !== chunk.digest) {
      throw new EvtError(
        'invalidJournal',
        `invalid journal: chunk ${chunk.index} of ${scope} changed between the preflight and ` +
          'the write',
      )
    }
    if (chunk.index === tail) {
      heldBack = chunk
    } else {
      await run.write([run.chunkWrite(chunk)])
      written++
    }
  }
  /* oxlint-enable no-await-in-loop */

  const swap: CompactionWrite[] = [
    {
      kind: 'put',
      row: compactionView(scope, MANIFEST_SORT_KEY, manifestPayload(manifest)),
      expect: stored === null ? {kind: 'absent'} : {kind: 'payload', payload: stored.payload},
    },
  ]
  if (heldBack !== null && (old === null || old.chunks[heldBack.index] !== heldBack.digest)) {
    swap.push(run.chunkWrite(heldBack))
    written++
  }
  await run.write(swap)

  return {manifest, written}
}

// Deletes every journal row at or below `through` that is there, a batch per transaction.
async function deleteRows(run: Run, through: bigint): Promise<bigint> {
  const {scope} = run.lease
  let deleted = 0n
  let after = 0n
  /* oxlint-disable no-await-in-loop */
  for (;;) {
    const rows = (await run.store.readRows(scope, after, DELETE_BATCH, run.options)).filter(
      (row) => row.position <= through,
    )
    const last = rows.at(-1)
    if (last === undefined) {
      return deleted
    }
    after = last.position
    await run.write(
      rows.map((row) => ({
        kind: 'delete',
        pk: rowPartitionKey(scope),
        sk: rowSortKey(row.position),
        owner: row.owner,
      })),
    )
    deleted += BigInt(rows.length)
    if (after >= through) {
      return deleted
    }
  }
  /* oxlint-enable no-await-in-loop */
}
