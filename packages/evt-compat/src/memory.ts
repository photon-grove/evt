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
import {journalConformance, journalStoreCases} from './journal-conformance.js'
import {viewRepositoryConformance} from './view-conformance.js'

const checks = new Checks()
const repo = new MemRepository()

await conformance(checks, repo, headFixtures(repo))

// The marker cases declare store-wide, so they get a store of their own.
const marked = new MemRepository()
await markerConformance(checks, marked, controlFixtures(marked))

await viewRepositoryConformance(checks, 'memory', new MemViewRepository())

// The journal accepts the store it wraps, so it gets one of its own.
const journaled = new MemRepository()
const rows = viewFixtures(journaled)
await journalConformance(checks, {
  repo: journaled,
  manifest: async () => controlFixtures(journaled).manifest(),
  views: {
    getViewAt: async (pk, sk) => journaled.view(pk, sk),
    putView: async (row) => rows.putView(row),
    deleteView: async (pk, sk) => rows.deleteView(pk, sk),
    listViewsByPk: async (pk) => journaled.views().filter((row) => row.pk === pk),
  },
})
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
  setRebuild: async (state) => compactedControls.setRebuild(state),
})

checks.report()
