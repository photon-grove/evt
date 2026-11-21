// Reading the journal: highWatermark and page, with gap detection (storage format, "Reads").

import {EvtError, RebuildEpochChangedError} from '../errors.js'
import type {CallOptions} from '../repository.js'
import type {JournalRow} from './row.js'
import {type JournalCompactionStore, manifestGone, readManifest} from './compaction.js'
import {MAX_POSITION, checkScope} from './scope.js'

/** The most rows {@link page} returns at once. */
export const MAX_PAGE_ROWS = 1000

/**
 * A backend that holds a journal: it reads a scope's counter and rows, strongly consistently, from
 * the views table the commit path writes. `MemRepository` and `DynamoRepository` (with a views
 * table) implement it. Application code calls {@link highWatermark} and {@link page} instead.
 */
export interface JournalStore {
  /** The scope's counter, the highest position allocated: 0 when it has none. */
  readCounter(scope: string, options?: CallOptions): Promise<bigint>
  /** The counters of several scopes, in the order given. */
  readCounters(scopes: readonly string[], options?: CallOptions): Promise<bigint[]>
  /**
   * The highest position that has a row in the scope, 0 when it has none: the truth the counter
   * summarizes, which a writer whose counter is behind allocates from.
   */
  readLastPosition(scope: string, options?: CallOptions): Promise<bigint>
  /**
   * Up to `limit` rows of the scope above `after`, ascending, not stopped by a missing position:
   * a gap shows as a jump in the rows.
   */
  readRows(
    scope: string,
    after: bigint,
    limit: number,
    options?: CallOptions,
  ): Promise<JournalRow[]>
}

/** Positions the journal should hold but a read didn't find, a range whose bounds are both included. */
export interface Gap {
  readonly from: bigint
  readonly to: bigint
}

/**
 * One page of a scope's journal. A gap isn't an error the reader can fix: the missing rows may be
 * in flight or lost, so a consumer never moves past one. It reads the same page again, and
 * escalates when the gap persists.
 */
export interface JournalPage {
  readonly scope: string
  /** The position the page was read after. */
  readonly after: bigint
  /** The rows after `after`, starting at the next position, ascending and contiguous: the page stops before a gap. */
  readonly rows: readonly JournalRow[]
  /** The scope's high watermark when the page was read, at least the last row's position. */
  readonly highWatermark: bigint
  /** The first gap met, if any: the rows stop just before it. */
  readonly gap: Gap | null
  /**
   * `after` is above the high watermark: a journal restored or rebuilt shorter than the reader's
   * cursor. The page is empty, and the reader isn't caught up: it must resync, not wait.
   */
  readonly cursorAhead: boolean
  /** The position to read on after: the last row's, or `after` for an empty page. */
  readonly nextAfter: bigint
  /** More rows can be read now: the page is contiguous, and the watermark is ahead of it. */
  readonly hasMore: boolean
  /** Everything allocated when the page was read has been read, with no gap. */
  readonly caughtUp: boolean
}

/** The scope's high watermark: its counter, read strongly consistently, 0 for a new scope. */
export function highWatermark(
  store: JournalStore,
  scope: string,
  options?: CallOptions,
): Promise<bigint> {
  return store.readCounter(checkScope(scope), options)
}

/**
 * Reads up to `limit` rows (1 to {@link MAX_PAGE_ROWS}) of the scope after `after`. The counter is
 * read first, then the rows. The page keeps the contiguous prefix after `after` and records the
 * first gap: a jump in the rows, or, when they end short of the counter and the page isn't full,
 * the missing tail. A row that landed between the two reads raises the watermark to itself.
 *
 * A gap below the scope's compaction floor is rows a compaction deleted: the read throws a
 * `JournalGoneError` naming the snapshot to install instead (the `410 Gone` handoff). Rows below
 * the floor that are still there are served as usual.
 *
 * Start at `after = 0`, and page on after `nextAfter` until `caughtUp`. Poll at the same `after`
 * to wait for new rows.
 */
export async function page(
  store: JournalCompactionStore,
  scope: string,
  after: bigint,
  limit: number,
  options?: CallOptions,
): Promise<JournalPage> {
  checkScope(scope)
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_PAGE_ROWS) {
    throw new EvtError(
      'invalidArgument',
      `a journal page holds 1 to ${MAX_PAGE_ROWS} rows; got ${limit}`,
    )
  }
  if (after < 0n || after > MAX_POSITION) {
    throw new EvtError('invalidArgument', `a journal page reads after a position 0 to 2^64 - 1`)
  }

  const counter = await store.readCounter(scope, options)
  // One row more than the page holds: a full page can then tell that rows follow it, even when
  // the counter is missing or behind.
  const found = await store.readRows(scope, after, limit + 1, options)

  const read = assemble(scope, after, limit, counter, found)
  // A gap may be rows a compaction deleted. The manifest is read after the rows: a compaction
  // swaps it before it deletes anything, so a deletion the rows show is always explained by the
  // floor read here. A page that reaches the floor is served as it is.
  if (read.gap !== null) {
    const manifest = await readManifest(store, scope, options)
    if (manifest !== null && read.nextAfter < manifest.checkpoint) {
      throw await manifestGone(manifest, after)
    }
  }

  return read
}

/** Builds a page from the rows a read returned: the contiguous prefix, the first gap, the mark. */
export function assemble(
  scope: string,
  after: bigint,
  limit: number,
  counter: bigint,
  found: readonly JournalRow[],
): JournalPage {
  let watermark = counter
  for (const row of found) {
    watermark = row.position > watermark ? row.position : watermark
  }

  const rows: JournalRow[] = []
  let gap: Gap | null = null
  let expected = after + 1n
  for (const row of found.slice(0, limit)) {
    if (row.position !== expected) {
      gap = {from: expected, to: row.position - 1n}
      break
    }
    expected++
    rows.push(row)
  }
  // The rows ended before the counter says they should have: the missing tail is a gap, unless
  // the page is simply full.
  if (gap === null && rows.length < limit && expected <= watermark) {
    gap = {from: expected, to: watermark}
  }

  const cursorAhead = after > watermark
  const nextAfter = rows.at(-1)?.position ?? after

  return {
    scope,
    after,
    rows,
    highWatermark: watermark,
    gap,
    cursorAhead,
    nextAfter,
    hasMore: gap === null && nextAfter < watermark,
    caughtUp: gap === null && !cursorAhead && nextAfter >= watermark,
  }
}

/** A journal store that reads the rebuild generation, for the epoch-checked reads. */
export interface EpochStore extends JournalCompactionStore {
  /** The settled rebuild generation; an odd one throws a `RebuildUnavailableError`. */
  observeRebuildEpoch(options?: CallOptions): Promise<bigint>
}

/**
 * {@link highWatermark} under the rebuild epoch check: it fails with a `RebuildUnavailableError`
 * while a rebuild holds the store (or after a force-release), and with a
 * `RebuildEpochChangedError` when one started, finished or was taken over during the read. Both
 * are retryable: read again once the rebuild is over.
 */
export function highWatermarkAtEpoch(
  store: EpochStore,
  scope: string,
  options?: CallOptions,
): Promise<bigint> {
  return atEpoch(store, () => highWatermark(store, scope, options), options)
}

/**
 * {@link page} under the rebuild epoch check, as {@link highWatermarkAtEpoch}: the read a consumer
 * of a journal that can be rebuilt makes, so a page read while a rebuild rewrites rows and
 * counters is a retryable error rather than a gap to interpret. On a failure, keep the cursor
 * and read the same page again after a backoff.
 */
export function pageAtEpoch(
  store: EpochStore,
  scope: string,
  after: bigint,
  limit: number,
  options?: CallOptions,
): Promise<JournalPage> {
  return atEpoch(store, () => page(store, scope, after, limit, options), options)
}

// Observes the generation, reads, and verifies the generation again: the read counts only then.
async function atEpoch<T>(
  store: EpochStore,
  read: () => Promise<T>,
  options: CallOptions | undefined,
): Promise<T> {
  const epoch = await store.observeRebuildEpoch(options)
  const value = await read()
  const current = await store.observeRebuildEpoch(options)
  if (current !== epoch) {
    throw new RebuildEpochChangedError(epoch, current)
  }

  return value
}
