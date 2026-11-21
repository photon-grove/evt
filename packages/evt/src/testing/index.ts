// @photon-grove/evt/testing: given/when/then helpers over MemRepository, which throw an
// AssertionError on failure so they work in any test runner, and a repository that injects commit
// faults.

export {AssertionError, deepEqual} from './equal.js'
export {FaultRepository} from './fault.js'
// The journal's internal hooks, for a test's repository wrapper that sits under a journal: it
// forwards a journal writer's acceptance and the journal's own declarations to what it wraps.
export {
  JOURNAL_MARKER,
  JOURNAL_WRITER_TARGET,
  type JournalDeclared,
  type JournalMarker,
} from '../features.js'
export {
  type Committed,
  DEFAULT_ENTITY_ID,
  Fixture,
  Outcome,
  Scenario,
  type ScenarioSetup,
  When,
  scenario,
} from './scenario.js'
