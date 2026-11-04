// Every step depends on the ones before it, so the runner awaits in loops on purpose.
/* oxlint-disable no-await-in-loop */

// The scenarios of compat/scenarios.json TypeScript supports, run against MemRepository and
// compared byte for byte with the Go-generated compat/fixtures/scenarios.json: every step's
// outcome, then each stream's events, snapshot, head, loaded state, DynamoDB items and SNS
// messages. TypeScript runs every step itself, whichever writer the step names, as the Rust
// fixture test does.

import {type JsonValue, encodeEnvelope, encodeEvent, encodeSnapshot} from '@photon-grove/evt'
import {headItem, eventItem, snapshotItem} from '@photon-grove/evt/dynamodb'
import {MemRepository, headFixtures} from '@photon-grove/evt/memory'

import {type Checks, canonical, field, list, loadFixture, str} from './harness.js'
import {
  ORIGIN_SOURCE,
  type Observed,
  Runner,
  SUPPORTED_SCENARIOS,
  checkScenario,
  checkStep,
  compareObserved,
  compareStream,
  expectedOutcome,
  optionalList,
  optionalString,
  streamId,
} from './runner.js'

const FIXTURE_RUN = 'fixture'

// The expectations only the scenarios TypeScript doesn't support yet produce.
const LATER_EXPECTATIONS = ['views', 'outcomes', 'transactions']

export async function scenarioChecks(checks: Checks): Promise<void> {
  const definitions = list(field(await loadFixture('../scenarios.json'), 'scenarios'))
  const fixtures = await loadFixture('scenarios.json')
  checks.equal('scenarios fixture run', str(field(fixtures, 'run')), FIXTURE_RUN)
  const expectations = new Map(
    list(field(fixtures, 'scenarios')).map((expected) => [str(field(expected, 'name')), expected]),
  )

  for (const name of SUPPORTED_SCENARIOS) {
    const scenario = definitions.find((definition) => str(field(definition, 'name')) === name)
    const expected = expectations.get(name)
    if (scenario === undefined || expected === undefined) {
      checks.equal(`scenario ${name} is defined and has a fixture`, false, true)
      continue
    }

    await checks.case(`scenario ${name}`, () => runScenario(checks, name, scenario, expected))
  }
}

async function runScenario(
  checks: Checks,
  name: string,
  scenario: JsonValue,
  expected: JsonValue,
): Promise<void> {
  checkScenario(scenario)
  for (const key of LATER_EXPECTATIONS) {
    checks.equal(`${name}: no ${key} expected`, optionalList(expected, key).length, 0)
  }

  const repository = new MemRepository()
  const runner = new Runner(repository, scenario, `compat-${FIXTURE_RUN}-${name}`)

  const steps = list(field(scenario, 'steps'))
  for (const [index, step] of steps.entries()) {
    checkStep(step, index)

    const want = expectedOutcome(field(step, 'expect'))
    const got = await runner.step(step)
    const label = `${name} step ${index} (${str(field(step, 'op'))})`
    checks.equal(label, canonical(got), canonical(want))
  }

  checks.equal(`${name}: entity ID`, runner.entityId, str(field(expected, 'entityId')))
  compareObserved(checks, name, await observe(repository, runner, runner.entityId), expected)

  // Sibling streams in ascending order, as the fixture lists them.
  const suffixes = [...new Set(steps.flatMap((step) => optionalString(step, 'suffix') ?? []))]
    .filter((suffix) => suffix !== '')
  suffixes.sort()
  const siblings = optionalList(expected, 'siblings')
  checks.equal(`${name}: sibling count`, siblings.length, suffixes.length)
  for (const [index, suffix] of suffixes.entries()) {
    const id = streamId(runner.entityId, suffix)
    const sibling = siblings[index]
    checks.equal(`${name}: sibling ${suffix} ID`, id, sibling && str(field(sibling, 'entityId')))
    if (sibling !== undefined) {
      const observed = await observe(repository, runner, id)
      compareStream(checks, `${name} sibling ${suffix}`, observed, sibling)
    }
  }
}

// The memory repository holds no items, so the stream's items are built from what it stores.
async function observe(repository: MemRepository, runner: Runner, id: string): Promise<Observed> {
  const events = await repository.events(id)
  const snapshot = await repository.snapshot(id)
  const head = headFixtures(repository).storedHead(id)

  const dynamoItems: unknown[] = []
  if (head !== null) {
    dynamoItems.push(headItem(id, head))
  }
  if (snapshot) {
    dynamoItems.push(snapshotItem(snapshot))
  }
  dynamoItems.push(...events.map((event) => eventItem(event)))

  return {
    ...(await runner.load(id)),
    events: events.map(encodeEvent),
    snapshot: snapshot && encodeSnapshot(snapshot),
    head,
    dynamoItems,
    snsMessages: events.map((event) => encodeEnvelope(event, ORIGIN_SOURCE)),
  }
}
