// Rebuilding the journal under the rebuild lock (storage format, "Rebuild"): its rows and counters
// derived from stored events, written as unconditioned view puts through the rebuild fence's
// guarded writer, in batches that each assert the lease.

import {RebuildFailure} from '../errors.js'
import type {SerializedEvent} from '../event.js'
import type {ProjectedView} from '../projection.js'
import type {RebuildFence, RebuildLease} from '../rebuild-fence.js'
import {Renewing, checkRenew, failRebuildLock, rebuildBatches, takeRebuildLock} from '../rebuild.js'
import type {CallOptions} from '../repository.js'
import {type JournalState, journalOperations, replay} from './replay.js'

/** The owner {@link rebuildJournalWithLock} takes the rebuild lock as. */
export const JOURNAL_REBUILD_OWNER = 'journal-rebuild'

/**
 * Rebuilds the journal under a rebuild lease: derives its rows and counters from the given `events`, in any
 * order, and writes them through `writeRebuildViews` in guarded batches. While the lease is held
 * every live commit fails with a `RebuildInProgressError`, so the unconditional rewrite can't race
 * a writer, and a batch under a lease that lost the lock writes nothing (`RebuildLeaseLostError`).
 * Read `events` after the lease was taken, or a commit that landed in between is missing.
 *
 * Rows are rewritten as they were: owned by the event's stream, counters by the engine, with no
 * `absent` condition. It's idempotent, and it doesn't delete. It restores rows a compaction
 * deleted, and leaves the compaction partition alone: the floor and its snapshot stay valid, and
 * compacting again at the manifest's checkpoint prunes them. It doesn't renew the lease;
 * {@link rebuildJournalWithLease} does. Resolves to the state it wrote.
 */
export async function rebuildJournal(
  fence: RebuildFence,
  events: Iterable<SerializedEvent>,
  lease: RebuildLease,
  options?: CallOptions,
): Promise<JournalState> {
  const state = replay(events)
  // Each batch is written after the one before it.
  /* oxlint-disable no-await-in-loop */
  for (const batch of rebuildBatches(journalOperations(state))) {
    await fence.writeRebuildViews(lease, batch, options)
  }
  /* oxlint-enable no-await-in-loop */

  return state
}

/** A completed {@link rebuildJournalWithLease}: what it wrote, and the lease as last renewed. */
export interface JournalRebuild {
  readonly state: JournalState
  readonly lease: RebuildLease
}

/**
 * {@link rebuildJournal} under a lease the caller took and will end, renewed as it goes, so one
 * lease can cover the journal and other derived state and a long rebuild doesn't lapse. The lease
 * is renewed for `renewMs` (at least a second) before the first batch, and again before a later
 * batch once half of it, less a second, has passed. A renewal that finds the lock gone stops the
 * run with a `RebuildLeaseLostError` before another write.
 *
 * A failure is a `RebuildFailure` with the error, the guarded batches attempted and the last lease.
 * Keep the lock unless no write was attempted under the lease (this run's or your own) and your
 * begin acquired it from a settled generation (`acquiredFromSettled`): otherwise the rows and
 * counters may be half-written, and only a rebuild that runs to its end may settle the generation.
 */
export async function rebuildJournalWithLease(
  fence: RebuildFence,
  events: Iterable<SerializedEvent>,
  lease: RebuildLease,
  renewMs: number,
  options: CallOptions = {},
): Promise<JournalRebuild> {
  let state: JournalState
  try {
    checkRenew(renewMs)
    state = replay(events)
  } catch (error) {
    throw new RebuildFailure(error, 0, 0, lease)
  }

  // Renewed (and so checked) before the first batch, even with nothing to write.
  const renewing = new Renewing(fence, lease, renewMs, null, options)
  try {
    await renewing.keepAlive()
  } catch (error) {
    throw new RebuildFailure(error, 0, 0, renewing.lease)
  }
  await write(renewing, journalOperations(state))

  return {state, lease: renewing.lease}
}

/**
 * {@link rebuildJournal} holding the lock for its whole run: derives the state of the given `events`, takes
 * the lock as {@link JOURNAL_REBUILD_OWNER} for `leaseMs`, writes it, renewing the lease before a
 * batch once half of it has passed, and releases the lock, which settles the generation. A lock
 * another rebuild holds is a `RebuildLockHeldError`, and a begin whose result is unknown that may
 * still hold the lock a `RebuildBeginIndeterminateError`. Events that can't be replayed
 * (`invalidJournal`) fail before the lock is taken. `events` are read before the lock, so rebuild
 * a table nothing writes to, or read the events under a lease and use
 * {@link rebuildJournalWithLease}.
 *
 * When a write was attempted and the rebuild fails, the lock is kept: the rows may be half-written,
 * so commits stay refused until a retry takes the lapsed lease over and runs to its end. So does a
 * run that took over a lapsed lease or acquired after a force-release, whatever failed. Only a run
 * that acquired from a settled generation and failed before its first write releases the lock.
 */
export async function rebuildJournalWithLock(
  fence: RebuildFence,
  events: Iterable<SerializedEvent>,
  leaseMs: number,
  options: CallOptions = {},
): Promise<JournalState> {
  const state = replay(events)
  const operations = journalOperations(state)
  const held = await takeRebuildLock(fence, JOURNAL_REBUILD_OWNER, leaseMs, options)
  const renewing = new Renewing(fence, held.lease, leaseMs, held.taken, options)

  try {
    await write(renewing, operations)
  } catch (error) {
    return failRebuildLock(fence, held, error as RebuildFailure)
  }
  // Every row is written: settling the generation runs even after an abort, so a finished rebuild
  // never leaves the store locked.
  await fence.endRebuild(renewing.lease)

  return state
}

// Writes the operations in guarded batches under a renewing lease. A failure is a RebuildFailure.
async function write(renewing: Renewing, operations: readonly ProjectedView[]): Promise<void> {
  const batches = rebuildBatches(operations)
  try {
    // Each batch is written after the one before it, under the lease it renewed.
    /* oxlint-disable no-await-in-loop */
    for (const batch of batches) {
      await renewing.write(batch)
    }
    /* oxlint-enable no-await-in-loop */
  } catch (error) {
    throw new RebuildFailure(error, renewing.attempted, batches.length, renewing.lease)
  }
}
