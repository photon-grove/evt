// Runs the conformance suite against the in-memory repository, on every runtime the fixture
// runner covers (ADR 0009, section 3).
import {
  MemRepository,
  MemViewRepository,
  controlFixtures,
  headFixtures,
} from '@photon-grove/evt/memory'

import {conformance, markerConformance} from './conformance.js'
import {Checks} from './harness.js'
import {viewRepositoryConformance} from './view-conformance.js'

const checks = new Checks()
const repo = new MemRepository()

await conformance(checks, repo, headFixtures(repo))

// The marker cases declare store-wide, so they get a store of their own.
const marked = new MemRepository()
await markerConformance(checks, marked, controlFixtures(marked))

await viewRepositoryConformance(checks, 'memory', new MemViewRepository())

checks.report()
