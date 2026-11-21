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

checks.report()
