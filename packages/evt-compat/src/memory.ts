// Runs the conformance suite against the in-memory repository, on every runtime the fixture
// runner covers (ADR 0009, section 3).
import {
  MemRepository,
  MemViewRepository,
  controlFixtures,
  headFixtures,
  viewFixtures,
} from '@photon-grove/evt/memory'

import {TestClock, compactionConformance} from './compaction-conformance.js'
import {conformance, markerConformance} from './conformance.js'
import {Checks} from './harness.js'
import {
  type JournalRebuildTarget,
  journalConformance,
  journalRebuildConformance,
  journalStoreCases,
} from './journal-conformance.js'
import {committedViewConformance, viewRepositoryConformance} from './view-conformance.js'

const checks = new Checks()
const repo = new MemRepository()

await conformance(checks, repo, headFixtures(repo))

// The marker cases declare store-wide, so they get a store of their own.
const marked = new MemRepository()
await markerConformance(checks, marked, controlFixtures(marked))

await viewRepositoryConformance(checks, 'memory', new MemViewRepository())

// The repository is the view repository over the rows its commits write.
const viewing = new MemRepository()
await viewRepositoryConformance(checks, 'memory repository', viewing)
await committedViewConformance(checks, 'memory repository', viewing, viewing)

// The journal accepts the store it wraps, so it gets one of its own.
const journaled = new MemRepository()
const rows = viewFixtures(journaled)
const journalTarget: JournalRebuildTarget = {
  repo: journaled,
  manifest: async () => controlFixtures(journaled).manifest(),
  views: {
    getViewAt: async (pk, sk) => journaled.view(pk, sk),
    putView: async (row) => rows.putView(row),
    deleteView: async (pk, sk) => rows.deleteView(pk, sk),
    listViewsByPk: async (pk) => journaled.views().filter((row) => row.pk === pk),
  },
}
await journalConformance(checks, journalTarget)
// The rebuild cases take the store-wide rebuild lock: after the others, on the same store.
await journalRebuildConformance(checks, journalTarget)
await journalStoreCases(checks, () => {
  const fresh = new MemRepository()

  return {repo: fresh, manifest: async () => controlFixtures(fresh).manifest()}
})

// Compaction declares store-wide, sets the rebuild fence, and moves its clock: a store of its own.
const clock = new TestClock(1_800_000_000_000)
const compacted = new MemRepository({clock: clock.now})
const compactedRows = viewFixtures(compacted)
const compactedControls = controlFixtures(compacted)
await compactionConformance(checks, {
  repo: compacted,
  clock,
  views: {
    getViewAt: async (pk, sk) => compacted.view(pk, sk),
    putView: async (row) => compactedRows.putView(row),
    deleteView: async (pk, sk) => compactedRows.deleteView(pk, sk),
  },
  manifest: async () => compactedControls.manifest(),
  declare: async (change) => {
    compactedControls.declare(change)
  },
})

checks.report()
