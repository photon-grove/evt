// Journal compaction's stored shapes (ADR 0005; storage format, "Journal compaction"): the rows of
// a scope's compaction partition (lease, manifest, chunks, audit), canonical chunking, the `410
// Gone` handoff, and the writes a compaction run sends.

import {EvtError} from '../errors.js'
import {JsonNumber, type JsonValue, parseJson, quote} from '../json.js'
import type {CallOptions} from '../repository.js'
import {utf8Decode, utf8Encode} from '../utf8.js'
import type {ViewRow} from '../views.js'
import type {JournalStore} from './read.js'
import {type JournalRow, decodeRowPayload, encodeRowText} from './row.js'
import {COUNTER_OWNER, POSITION_WIDTH, checkScope, u64} from './scope.js'

/** The partition-key prefix of a scope's compaction rows (`journal-compaction#<scope>`). */
export const COMPACTION_PARTITION_PREFIX = 'journal-compaction#'

/** The entity type of every compaction row. */
export const COMPACTION_ENTITY_TYPE = 'journal-compaction'

/** The sort key of a scope's compaction lease. */
export const LEASE_SORT_KEY = 'LEASE'

/** The sort key of a scope's snapshot manifest. */
export const MANIFEST_SORT_KEY = 'MANIFEST'

/** The sort-key prefix of a snapshot chunk (`chunk#<index, 20 digits>`). */
export const CHUNK_SORT_KEY_PREFIX = 'chunk#'

/** The sort-key prefix of a compaction's audit row (`audit#<checkpoint, 20 digits>`). */
export const AUDIT_SORT_KEY_PREFIX = 'audit#'

/** The most rows a chunk holds. Part of the format. */
export const MAX_CHUNK_ROWS = 500

/** The largest chunk payload, and the largest manifest, in bytes. Part of the format. */
export const MAX_CHUNK_BYTES = 375_000

/** The most writes one compaction transaction carries, besides its epoch check. */
export const MAX_COMPACTION_WRITES = 99

/** The partition key of a scope's compaction rows. */
export function compactionPartitionKey(scope: string): string {
  return COMPACTION_PARTITION_PREFIX + scope
}

/** The sort key of chunk `index`. */
export function chunkSortKey(index: number): string {
  return CHUNK_SORT_KEY_PREFIX + String(index).padStart(POSITION_WIDTH, '0')
}

/** The sort key of the audit row of a compaction through `checkpoint`. */
export function auditSortKey(checkpoint: bigint): string {
  return AUDIT_SORT_KEY_PREFIX + checkpoint.toString().padStart(POSITION_WIDTH, '0')
}

/** A compaction row checked to be the engine's, or null. */
export async function readEngineRow(
  store: JournalCompactionStore,
  scope: string,
  sortKey: string,
  options?: CallOptions,
): Promise<ViewRow | null> {
  const row = await store.readCompactionRow(checkScope(scope), sortKey, options)

  return row === null ? null : checkCompactionRow(scope, sortKey, row)
}

/** The scope's snapshot manifest, or null for a scope never compacted. */
export async function readManifest(
  store: JournalCompactionStore,
  scope: string,
  options?: CallOptions,
): Promise<CompactionManifest | null> {
  const row = await readEngineRow(store, scope, MANIFEST_SORT_KEY, options)

  return row === null ? null : decodeManifest(scope, row.payload)
}

/** The scope's compaction floor: its manifest's checkpoint, 0 for a scope never compacted. */
export async function readFloor(
  store: JournalCompactionStore,
  scope: string,
  options?: CallOptions,
): Promise<bigint> {
  return (await readManifest(store, scope, options))?.checkpoint ?? 0n
}

/** A compaction row: the engine's, in the scope's compaction partition. */
export function compactionView(scope: string, sk: string, payload: Uint8Array): ViewRow {
  return {
    pk: compactionPartitionKey(scope),
    sk,
    entityId: COUNTER_OWNER,
    entityType: COMPACTION_ENTITY_TYPE,
    payload,
  }
}

/** A row in a journal compaction partition that isn't the engine's. Nothing was written. */
export class ForeignJournalRowError extends EvtError {
  override readonly name = 'ForeignJournalRowError'
  override readonly code = 'foreignJournalRow'
  readonly pk: string
  readonly sk: string
  readonly owner: string
  readonly entityType: string

  constructor(row: ViewRow) {
    super(
      'foreignJournalRow',
      `the journal row ${quote(row.pk)}/${quote(row.sk)} is not the engine's (owner ` +
        `${quote(row.entityId)}, type ${quote(row.entityType)})`,
    )
    this.pk = row.pk
    this.sk = row.sk
    this.owner = row.entityId
    this.entityType = row.entityType
  }
}

/** Checks that a compaction row is the engine's, at its key, or throws a foreign-row error. */
export function checkCompactionRow(scope: string, sortKey: string, row: ViewRow): ViewRow {
  if (
    row.pk !== compactionPartitionKey(scope) ||
    row.sk !== sortKey ||
    row.entityId !== COUNTER_OWNER ||
    row.entityType !== COMPACTION_ENTITY_TYPE
  ) {
    throw new ForeignJournalRowError(row)
  }

  return row
}

function corrupt(scope: string, what: string, why: string): EvtError {
  return new EvtError('invalidRecord', `journal compaction ${what} of ${scope}: ${why}`)
}

// A stored payload's fields, read as Rust's serde reads a struct: by name, other keys ignored, a
// repeated key refused. A missing field is undefined.
function fields(
  scope: string,
  what: string,
  payload: Uint8Array,
  names: readonly string[],
): (JsonValue | undefined)[] {
  let value: JsonValue
  try {
    value = parseJson(utf8Decode(payload, what), {duplicateKeys: 'refuse'})
  } catch (error) {
    throw corrupt(scope, what, error instanceof Error ? error.message : String(error))
  }
  if (!(value instanceof Map)) {
    throw corrupt(scope, what, 'it is not an object')
  }

  return names.map((name) => value.get(name))
}

function scopeField(scope: string, what: string, value: JsonValue | undefined): void {
  if (typeof value !== 'string') {
    throw corrupt(scope, what, 'it has no scope')
  }
  if (value !== scope) {
    throw corrupt(scope, what, `it names ${quote(value)}`)
  }
}

function u64Field(scope: string, what: string, value: JsonValue | undefined): bigint {
  const n = value === undefined ? null : u64(value)
  if (n === null) {
    throw corrupt(scope, what, 'a number field is not a u64')
  }

  return n
}

/** A compaction lease a run holds. */
export interface CompactionLease {
  readonly scope: string
  /** Who holds it. */
  readonly owner: string
  /** The fencing token: one more than the last lease of the scope. */
  readonly token: bigint
  /** When it lapses, in whole Unix seconds; held up to and including that second. */
  readonly leaseExpiresAt: number
  /** Its length in whole seconds, which a run renews it by: 0 for a lease read back. */
  readonly leaseSeconds: number
}

/** A stored lease row: held, or released (keeping its token). */
export type LeaseRow =
  | {readonly kind: 'held'; readonly lease: CompactionLease}
  | {readonly kind: 'released'; readonly scope: string; readonly token: bigint}

/** A held lease's payload. */
export function leasePayload(lease: CompactionLease): Uint8Array {
  return utf8Encode(
    `{"scope":${quote(lease.scope)},"token":${lease.token},"owner":${quote(lease.owner)},` +
      `"leaseExpiresAt":${lease.leaseExpiresAt}}`,
  )
}

/** A released lease's payload. */
export function releasedPayload(scope: string, token: bigint): Uint8Array {
  return utf8Encode(`{"scope":${quote(scope)},"token":${token}}`)
}

/** Decodes a lease row's payload. */
export function decodeLease(scope: string, payload: Uint8Array): LeaseRow {
  const [named, token, owner, expires] = fields(scope, 'lease', payload, [
    'scope',
    'token',
    'owner',
    'leaseExpiresAt',
  ])
  scopeField(scope, 'lease', named)
  const value = u64Field(scope, 'lease', token)
  const held = owner !== undefined && owner !== null
  const expiring = expires !== undefined && expires !== null
  if (!held && !expiring) {
    return {kind: 'released', scope, token: value}
  }
  if (!held || !expiring || typeof owner !== 'string') {
    throw corrupt(scope, 'lease', 'an owner and an expiry come together')
  }
  const at = i64(expires)
  if (at === null) {
    throw corrupt(scope, 'lease', 'its expiry is not a whole number of seconds')
  }

  return {
    kind: 'held',
    lease: {scope, owner, token: value, leaseExpiresAt: at, leaseSeconds: 0},
  }
}

function i64(value: JsonValue): number | null {
  if (!(value instanceof JsonNumber)) {
    return null
  }
  const text = value.text
  if (!/^-?\d+$/.test(text)) {
    return null
  }
  const n = Number(text)

  return Number.isSafeInteger(n) ? n : null
}

/** A scope's snapshot manifest: its checkpoint (the floor), and each chunk's digest by index. */
export interface CompactionManifest {
  readonly scope: string
  readonly checkpoint: bigint
  readonly chunks: readonly string[]
}

/** A manifest's payload. */
export function manifestPayload(manifest: CompactionManifest): Uint8Array {
  const chunks = manifest.chunks.map((digest) => quote(digest)).join(',')

  return utf8Encode(
    `{"scope":${quote(manifest.scope)},"checkpoint":${manifest.checkpoint},"chunks":[${chunks}]}`,
  )
}

/** A manifest's digest, which identifies the snapshot: the SHA-256 hex of its payload. */
export function manifestDigest(manifest: CompactionManifest): Promise<string> {
  return sha256Hex(manifestPayload(manifest))
}

/** Decodes a manifest row's payload. */
export function decodeManifest(scope: string, payload: Uint8Array): CompactionManifest {
  const [named, checkpoint, chunks] = fields(scope, 'manifest', payload, [
    'scope',
    'checkpoint',
    'chunks',
  ])
  scopeField(scope, 'manifest', named)
  const at = u64Field(scope, 'manifest', checkpoint)
  if (!Array.isArray(chunks)) {
    throw corrupt(scope, 'manifest', 'it lists no chunks')
  }
  if (at === 0n || chunks.length === 0) {
    throw corrupt(scope, 'manifest', 'it covers nothing')
  }
  if (!chunks.every((digest) => typeof digest === 'string' && /^[0-9a-f]{64}$/.test(digest))) {
    throw corrupt(scope, 'manifest', 'a chunk digest is not SHA-256 hex')
  }

  return {scope, checkpoint: at, chunks: chunks as string[]}
}

/**
 * A journal page asked for rows below the scope's compaction floor that are gone: the `410 Gone`
 * handoff. It names the snapshot that replaces them: install it, and page on after its
 * checkpoint. The same read is gone again, so it isn't retryable as it is.
 */
export class JournalGoneError extends EvtError {
  /** The HTTP status an HTTP layer maps it to. */
  static readonly HTTP_STATUS = 410
  override readonly name = 'JournalGoneError'
  override readonly code = 'journalGone'
  readonly scope: string
  /** The cursor the page was read after. */
  readonly after: bigint
  /** The snapshot's checkpoint, the scope's floor. */
  readonly checkpoint: bigint
  readonly chunkCount: number
  /** The manifest's digest. */
  readonly digest: string

  constructor(
    scope: string,
    after: bigint,
    checkpoint: bigint,
    chunkCount: number,
    digest: string,
  ) {
    super(
      'journalGone',
      `journal scope ${scope} is compacted through ${checkpoint}: rows after ${after} are gone; ` +
        `install snapshot ${digest}`,
    )
    this.scope = scope
    this.after = after
    this.checkpoint = checkpoint
    this.chunkCount = chunkCount
    this.digest = digest
  }

  /** The cursor to page on after, once the snapshot is installed: its checkpoint. */
  get resumeAfter(): bigint {
    return this.checkpoint
  }
}

/** The handoff a manifest gives a reader whose cursor is `after`. */
export async function manifestGone(
  manifest: CompactionManifest,
  after: bigint,
): Promise<JournalGoneError> {
  return new JournalGoneError(
    manifest.scope,
    after,
    manifest.checkpoint,
    manifest.chunks.length,
    await manifestDigest(manifest),
  )
}

/**
 * A snapshot's chunk is missing or doesn't match the manifest it was read for: a compaction
 * swapped the manifest meanwhile. Retryable: read the manifest again and start over.
 */
export class JournalSnapshotMovedError extends EvtError {
  override readonly name = 'JournalSnapshotMovedError'
  override readonly code = 'journalSnapshotMoved'
  readonly scope: string
  readonly checkpoint: bigint

  constructor(scope: string, checkpoint: bigint) {
    super(
      'journalSnapshotMoved',
      `the snapshot of journal scope ${quote(scope)} through ${checkpoint} has moved`,
    )
    this.scope = scope
    this.checkpoint = checkpoint
  }
}

/** One chunk of a snapshot: contiguous rows, its first position through its last. */
export interface CompactionChunk {
  readonly scope: string
  readonly index: number
  readonly from: bigint
  readonly through: bigint
  readonly rows: readonly JournalRow[]
}

// A chunk's header fields and its rows' payloads exactly as stored.
interface RawChunk {
  readonly index: number
  readonly from: bigint
  readonly through: bigint
  readonly rows: readonly string[]
}

function decodeRawChunk(scope: string, payload: Uint8Array): RawChunk {
  const [named, index, from, through, rows] = fields(scope, 'chunk', payload, [
    'scope',
    'index',
    'from',
    'through',
    'rows',
  ])
  const at = index === undefined ? null : u64(index)
  if (
    named !== scope ||
    at === null ||
    at > BigInt(Number.MAX_SAFE_INTEGER) ||
    !Array.isArray(rows) ||
    rows.length === 0
  ) {
    throw corrupt(scope, 'chunk', 'it is not a chunk of this scope')
  }
  const first = u64Field(scope, 'chunk', from)
  const last = u64Field(scope, 'chunk', through)
  if (first === 0n) {
    throw corrupt(scope, 'chunk', 'it is not a chunk of this scope')
  }
  const raw = rawArrayElements(utf8Decode(payload, 'chunk'), 'rows')
  if (raw === null || raw.length !== rows.length) {
    throw corrupt(scope, 'chunk', 'its rows are not an array of rows')
  }

  return {index: Number(at), from: first, through: last, rows: raw}
}

/** Decodes a chunk row's payload, checking its rows run contiguously through its range. */
export function decodeChunk(scope: string, payload: Uint8Array): CompactionChunk {
  const stored = decodeRawChunk(scope, payload)
  const rows = stored.rows.map((raw) => decodeRowPayload(scope, utf8Encode(raw)))
  const contiguous = rows.every((row, at) => row.position === stored.from + BigInt(at))
  if (!contiguous || stored.through + 1n !== stored.from + BigInt(rows.length)) {
    throw corrupt(scope, 'chunk', `its rows are not ${stored.from}..=${stored.through}`)
  }

  return {scope, index: stored.index, from: stored.from, through: stored.through, rows}
}

// The raw text of each element of the top-level array under `key`, as written: a scan that tracks
// strings and nesting. Null when the document holds no such array.
function rawArrayElements(text: string, key: string): string[] | null {
  const start = topLevelKey(text, key)
  if (start === -1) {
    return null
  }

  const elements: string[] = []
  let depth = 0
  let from = start + 1
  let inString = false
  for (let i = start + 1; i < text.length; i++) {
    const c = text[i]
    if (inString) {
      if (c === '\\') {
        i++
      } else if (c === '"') {
        inString = false
      }
      continue
    }
    if (c === '"') {
      inString = true
    } else if (c === '[' || c === '{') {
      depth++
    } else if (c === ']' || c === '}') {
      if (depth === 0) {
        const last = text.slice(from, i).trim()
        if (last !== '') {
          elements.push(last)
        }

        return elements
      }
      depth--
    } else if (c === ',' && depth === 0) {
      elements.push(text.slice(from, i).trim())
      from = i + 1
    }
  }

  return null
}

// The index of the `[` that opens the value of `key` in the top-level object, or -1.
function topLevelKey(text: string, key: string): number {
  let depth = 0
  let inString = false
  let stringStart = -1
  for (let i = 0; i < text.length; i++) {
    const c = text[i]
    if (inString) {
      if (c === '\\') {
        i++
      } else if (c === '"') {
        inString = false
        if (depth === 1 && text.slice(stringStart, i + 1) === quote(key)) {
          let j = i + 1
          while (/\s/.test(text[j] ?? '')) {
            j++
          }
          if (text[j] === ':') {
            j++
            while (/\s/.test(text[j] ?? '')) {
              j++
            }
            if (text[j] === '[') {
              return j
            }
          }
        }
      }
      continue
    }
    if (c === '"') {
      inString = true
      stringStart = i
    } else if (c === '{' || c === '[') {
      depth++
    } else if (c === '}' || c === ']') {
      depth--
    }
  }

  return -1
}

/** A closed chunk: its payload and its digest. */
export interface BuiltChunk {
  readonly index: number
  readonly from: bigint
  readonly through: bigint
  readonly payload: Uint8Array
  readonly digest: string
}

/**
 * Builds canonical chunks: rows from position `from` in order, closed before the row that would
 * make a chunk hold more than {@link MAX_CHUNK_ROWS} rows or a payload over
 * {@link MAX_CHUNK_BYTES}. A closed chunk depends only on the rows up to the one that closed it.
 */
export class ChunkBuilder {
  readonly #scope: string
  #index: number
  #from: bigint
  #rows: string[] = []
  #rowBytes = 0

  constructor(scope: string, index: number, from: bigint) {
    this.#scope = scope
    this.#index = index
    this.#from = from
  }

  /** A builder that continues a stored chunk, its rows exactly as stored. */
  static resume(scope: string, payload: Uint8Array): ChunkBuilder {
    const stored = decodeRawChunk(scope, payload)
    const builder = new ChunkBuilder(scope, stored.index, stored.from)
    for (const raw of stored.rows) {
      builder.#rows.push(raw)
      builder.#rowBytes += utf8Encode(raw).length
    }

    return builder
  }

  /**
   * Adds the next row, and returns the chunk it closed, if it closed one. A row that isn't the
   * next position, or one too large for any chunk, is `invalidJournal`.
   */
  push(row: JournalRow): RawBuilt | null {
    const text = encodeRowText(row.scope, row.position, row.event)
    const bytes = utf8Encode(text).length
    const expected = this.#from + BigInt(this.#rows.length)
    if (row.position !== expected) {
      throw new EvtError(
        'invalidJournal',
        `invalid journal: compaction of ${this.#scope} expected position ${expected}, found ` +
          `${row.position}`,
      )
    }

    let closed: RawBuilt | null = null
    const full = this.#rows.length === MAX_CHUNK_ROWS || this.#sizeWith(bytes, 1) > MAX_CHUNK_BYTES
    if (this.#rows.length > 0 && full) {
      closed = this.build()
      this.#index++
      this.#from = row.position
      this.#rows = []
      this.#rowBytes = 0
    }
    this.#rowBytes += bytes
    this.#rows.push(text)
    if (this.#sizeWith(0, 0) > MAX_CHUNK_BYTES) {
      throw new EvtError(
        'invalidJournal',
        `invalid journal: the row at position ${row.position} of ${this.#scope} does not fit a ` +
          'chunk',
      )
    }

    return closed
  }

  /** The chunk as it stands: its payload, before its digest. */
  build(): RawBuilt {
    const through = this.#from + BigInt(this.#rows.length) - 1n
    const payload = utf8Encode(`${this.#header(through)}${this.#rows.join(',')}]}`)

    return {index: this.#index, from: this.#from, through, payload}
  }

  #header(through: bigint): string {
    return (
      `{"scope":${quote(this.#scope)},"index":${this.#index},"from":${this.#from},` +
      `"through":${through},"rows":[`
    )
  }

  #sizeWith(extra: number, count: number): number {
    const rows = this.#rows.length + count
    const through = this.#from + BigInt(rows) - 1n

    return (
      utf8Encode(this.#header(through)).length + this.#rowBytes + extra + Math.max(rows - 1, 0) + 2
    )
  }
}

/** A closed chunk before its digest is computed. */
export interface RawBuilt {
  readonly index: number
  readonly from: bigint
  readonly through: bigint
  readonly payload: Uint8Array
}

/** A closed chunk with its digest. */
export async function withDigest(chunk: RawBuilt): Promise<BuiltChunk> {
  return {...chunk, digest: await sha256Hex(chunk.payload)}
}

/** The audit row a finished compaction writes. */
export interface CompactionAudit {
  readonly scope: string
  readonly checkpoint: bigint
  readonly digest: string
  readonly chunks: number
  readonly deleted: bigint
  readonly owner: string
  readonly token: bigint
  /** When it finished, in Unix seconds. */
  readonly completedAt: number
}

/** An audit row's payload. */
export function auditPayload(audit: CompactionAudit): Uint8Array {
  return utf8Encode(
    `{"scope":${quote(audit.scope)},"checkpoint":${audit.checkpoint},"digest":` +
      `${quote(audit.digest)},"chunks":${audit.chunks},"deleted":${audit.deleted},"owner":` +
      `${quote(audit.owner)},"token":${audit.token},"completedAt":${audit.completedAt}}`,
  )
}

/** Decodes an audit row's payload. */
export function decodeAudit(scope: string, payload: Uint8Array): CompactionAudit {
  const [named, checkpoint, digest, chunks, deleted, owner, token, completedAt] = fields(
    scope,
    'audit',
    payload,
    ['scope', 'checkpoint', 'digest', 'chunks', 'deleted', 'owner', 'token', 'completedAt'],
  )
  scopeField(scope, 'audit', named)
  const count = chunks === undefined ? null : u64(chunks)
  const at = completedAt === undefined ? null : i64(completedAt)
  if (
    typeof digest !== 'string' ||
    typeof owner !== 'string' ||
    count === null ||
    count > BigInt(Number.MAX_SAFE_INTEGER) ||
    at === null
  ) {
    throw corrupt(scope, 'audit', 'a field is malformed')
  }

  return {
    scope,
    checkpoint: u64Field(scope, 'audit', checkpoint),
    digest,
    chunks: Number(count),
    deleted: u64Field(scope, 'audit', deleted),
    owner,
    token: u64Field(scope, 'audit', token),
    completedAt: at,
  }
}

/** What a compaction write expects at its key: anything, no row, or the engine's exact payload. */
export type Expect =
  | {readonly kind: 'any'}
  | {readonly kind: 'absent'}
  | {readonly kind: 'payload'; readonly payload: Uint8Array}

/** One write of a compaction transaction. */
export type CompactionWrite =
  | {readonly kind: 'put'; readonly row: ViewRow; readonly expect: Expect}
  | {readonly kind: 'delete'; readonly pk: string; readonly sk: string; readonly owner: string}
  | {readonly kind: 'check'; readonly pk: string; readonly sk: string; readonly expect: Expect}

/** A write's key. */
export function writeKey(write: CompactionWrite): {pk: string; sk: string} {
  return write.kind === 'put' ? {pk: write.row.pk, sk: write.row.sk} : {pk: write.pk, sk: write.sk}
}

/**
 * What a compaction transaction did: written; refused by the epoch check; or refused by the write
 * at an index, whose expectation (or a delete's owner) failed.
 */
export type CompactionWritten =
  | {readonly kind: 'written'}
  | {readonly kind: 'epochChanged'}
  | {readonly kind: 'refused'; readonly at: number}

/**
 * A journal store that also reads a scope's compaction rows, so it can tell a compacted range
 * from a gap. `MemRepository` and `DynamoRepository` (with a views table) implement it.
 */
export interface JournalCompactionStore extends JournalStore {
  /** The row at `sortKey` in the scope's compaction partition, strongly consistent, or null. */
  readCompactionRow(scope: string, sortKey: string, options?: CallOptions): Promise<ViewRow | null>
}

/**
 * A journal store that also writes a scope's compaction rows, under the rebuild epoch.
 * `MemRepository` and `DynamoRepository` (with a views table) implement it.
 */
export interface CompactionStore extends JournalCompactionStore {
  /**
   * Sends one compaction transaction: the writes in order, then, with an epoch, a check that the
   * rebuild generation still holds it. A failed condition is reported, never thrown; throttling and
   * transaction conflicts are transient.
   */
  writeCompaction(
    epoch: bigint | null,
    writes: readonly CompactionWrite[],
    options?: CallOptions,
  ): Promise<CompactionWritten>
  /** The repository's clock, in whole Unix seconds. */
  compactionNow(): number
  /**
   * The settled rebuild generation a run works under: even, or 0 with no generation row. An odd
   * one throws a `RebuildUnavailableError`.
   */
  observeRebuildEpoch(options?: CallOptions): Promise<bigint>
}

/** Refuses a compaction transaction with no writes, too many, or two at one key. */
export function checkCompactionWrites(writes: readonly CompactionWrite[]): void {
  if (writes.length === 0 || writes.length > MAX_COMPACTION_WRITES) {
    throw new EvtError(
      'invalidArgument',
      `a compaction write holds 1 to ${MAX_COMPACTION_WRITES} writes; got ${writes.length}`,
    )
  }
  const keys = new Set<string>()
  for (const write of writes) {
    const {pk, sk} = writeKey(write)
    const key = `${pk}\0${sk}`
    if (keys.has(key)) {
      throw new EvtError('invalidArgument', `a compaction write names ${pk}/${sk} twice`)
    }
    keys.add(key)
  }
}

/** Whether a stored row meets an expectation. */
export function meets(expect: Expect, row: ViewRow | null): boolean {
  switch (expect.kind) {
    case 'any':
      return true
    case 'absent':
      return row === null
    case 'payload':
      return (
        row !== null && row.entityId === COUNTER_OWNER && sameBytes(row.payload, expect.payload)
      )
  }
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && a.every((byte, at) => byte === b[at])
}

/** The lowercase hex SHA-256 of some bytes. */
export async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', new Uint8Array(bytes)))
  let hex = ''
  for (const byte of digest) {
    hex += byte.toString(16).padStart(2, '0')
  }

  return hex
}
