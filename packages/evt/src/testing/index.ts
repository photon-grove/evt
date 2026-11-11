// @photon-grove/evt/testing: given/when/then helpers over MemRepository, which throw an
// AssertionError on failure so they work in any test runner, and a repository that injects commit
// faults.

export {AssertionError, deepEqual} from './equal.js'
export {FaultRepository} from './fault.js'
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
