// The journal's commit-wide projector: journal rows derived from the allocations in the events'
// metadata (storage format, "Rows and counters", Ownership).

import {
  type CommitProjector,
  type CommitView,
  type ViewOperation,
  engineProjector,
  putView,
} from '../projection.js'
import type {Journal} from './config.js'
import {encodeRow} from './row.js'
import {ROW_ENTITY_TYPE, allocations, rowPartitionKey, rowSortKey} from './scope.js'

/** The name the journal's projector reports in errors. */
export const PROJECTOR_NAME = 'journal'

/**
 * The error a journal row's or counter's operation maps to when its position is taken: another
 * commit allocated it first. The journal's retry matches it, reads the counters again, and
 * allocates again.
 */
export class PositionTaken extends Error {
  override readonly name = 'PositionTaken'
  readonly scope: string
  readonly position: bigint

  constructor(scope: string, position: bigint) {
    super(`position ${position} of journal scope ${JSON.stringify(scope)} is already taken`)
    this.scope = scope
    this.position = position
  }
}

/**
 * The journal's rows of a commit: for each event, one row per allocation in its metadata, owned by
 * the event's stream and written only if absent, so a position is claimed exactly once. It's a
 * history-dependent projector over the journal's entity types, and a pure function of its commit
 * view, so regrouping stored events by commit and running it reproduces the live rows. It's one of
 * the engine's own projectors: it sees the allocations, which every other projector is shown
 * without, and a store refuses it as an application projector. The counters aren't its output: no
 * stream can own one, so the journal's repository writes them.
 */
export function journalProjector(journal: Journal): CommitProjector {
  // Frozen, so nothing can swap its project function and keep the engine's standing.
  return engineProjector(
    Object.freeze({
      name: PROJECTOR_NAME,
      history: journal.entityTypes,
      project(commit: CommitView): ViewOperation[] {
        const operations: ViewOperation[] = []
        for (const stream of commit.streams) {
          for (const event of stream.events) {
            for (const {scope, position} of allocations(event.metadata)) {
              operations.push(
                putView(
                  rowPartitionKey(scope),
                  rowSortKey(position),
                  event.entityId,
                  ROW_ENTITY_TYPE,
                  encodeRow(scope, position, event),
                  {condition: 'absent', conflict: new PositionTaken(scope, position)},
                ),
              )
            }
          }
        }

        return operations
      },
    }),
  )
}
