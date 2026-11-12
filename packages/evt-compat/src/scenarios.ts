// Every step depends on the ones before it, so the runner awaits in loops on purpose.
/* oxlint-disable no-await-in-loop */

// The scenarios of compat/scenarios.json TypeScript supports, run against MemRepository and
// compared byte for byte with the Go-generated compat/fixtures/scenarios.json: every step's
// outcome, then each stream's events, snapshot, head, loaded state, DynamoDB items and SNS
// messages. TypeScript runs every step itself, whichever writer the step names, as the Rust
// fixture test does.

import {
  type CallOptions,
  type Commit,
  type JsonValue,
  type NormalizedTransaction,
  type TransactionCommit,
  commitAsTransaction,
  compareUtf8,
  encodeEnvelope,
  encodeEvent,
  encodeSnapshot,
  normalizeTransaction,
} from '@photon-grove/evt'
import {
  type AttributeValue,
  type TransactItem,
  eventItem,
  headItem,
  outcomeItem,
  snapshotItem,
  transactionItems,
} from '@photon-grove/evt/dynamodb'
import {MemRepository, headFixtures} from '@photon-grove/evt/memory'
import {FaultRepository} from '@photon-grove/evt/testing'

import {type Checks, canonical, field, list, loadFixture, plain, str} from './harness.js'
import {
  FIXTURE_RUN,
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
  transactionCommit,
} from './runner.js'

// The expectations only the scenarios TypeScript doesn't support yet produce.
const LATER_EXPECTATIONS = ['views']

// The marker version a fresh store's transactions check, and the table the fixture's actions
// leave out.
const MARKER_VERSION = 1n
const TABLE = 'events'

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
  const recorder = new Recorder(repository)
  const runner = new Runner(recorder, scenario, FIXTURE_RUN, {
    bypassHead: async (event) => headFixtures(repository).appendBypassingHead(event),
  })
  const outcomes = scenario instanceof Map && scenario.get('outcomes') === true

  const steps = list(field(scenario, 'steps'))
  const transactions: unknown[] = []
  for (const [index, step] of steps.entries()) {
    checkStep(step, index)

    const want = expectedOutcome(field(step, 'expect'))
    const sent = recorder.calls.length
    const got = await runner.step(step)
    const label = `${name} step ${index} (${str(field(step, 'op'))})`
    checks.equal(label, canonical(got), canonical(want))

    // An outcomes scenario pins every request each step sends, as Go's generator records them.
    if (outcomes) {
      const calls = recorder.calls.slice(sent)
      if (calls.length > 0) {
        transactions.push({step: index, calls: calls.map((items) => items.map(action))})
      }
      continue
    }

    // The request a DynamoDB writer sends for the step, whether it commits or conflicts.
    if (str(field(step, 'op')) === 'transaction') {
      const transaction = normalizeTransaction(transactionCommit(runner.entityId, step))
      const items = transactionItems(TABLE, transaction, MARKER_VERSION, () => undefined)
      transactions.push({step: index, actions: items.map(action)})
    }
  }
  checks.equal(
    `${name}: transactions`,
    canonical(transactions),
    canonical(optionalList(expected, 'transactions').map(plain)),
  )

  checks.equal(`${name}: entity ID`, runner.entityId, str(field(expected, 'entityId')))
  compareObserved(checks, name, await observe(repository, runner, runner.entityId), expected)
  if (outcomes) {
    checks.equal(
      `${name}: outcomes`,
      canonical(await outcomeRows(repository, runner.entityId, steps)),
      canonical(optionalList(expected, 'outcomes').map(plain)),
    )
  }

  // Sibling streams in ascending order, as the fixture lists them.
  const suffixes = [...new Set(steps.flatMap(stepSuffixes))].filter((suffix) => suffix !== '')
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

// The outcome rows a scenario's commands left, as DynamoDB items, by command ID bytes.
async function outcomeRows(
  repository: MemRepository,
  entityId: string,
  steps: readonly JsonValue[],
): Promise<unknown[]> {
  const ids = [...new Set(steps.flatMap((step) => optionalString(step, 'commandId') ?? []))]
    .map((id) => `${entityId}-${id}`)
    .filter((id) => new TextEncoder().encode(id).length <= 512)
  ids.sort(compareUtf8)

  const rows: unknown[] = []
  for (const commandId of ids) {
    const outcome = await repository.outcome(commandId)
    if (outcome !== null) {
      rows.push({commandId, dynamoItem: outcomeItem(outcome)})
    }
  }

  return rows
}

/**
 * Records the DynamoDB request each commit sends, built from what the repository is given, and
 * passes it on. A commit the DynamoDB repository would refuse before any request records nothing.
 */
class Recorder extends FaultRepository {
  readonly calls: TransactItem[][] = []

  override commit(commit: Commit, options?: CallOptions): Promise<void> {
    this.#record(() => commitAsTransaction(commit))

    return super.commit(commit, options)
  }

  override commitTransaction(transaction: TransactionCommit, options?: CallOptions): Promise<void> {
    this.#record(() => normalizeTransaction(transaction))

    return super.commitTransaction(transaction, options)
  }

  #record(normalize: () => NormalizedTransaction): void {
    let normalized: NormalizedTransaction
    try {
      normalized = normalize()
    } catch {
      return
    }
    // The repository sends nothing for a commit that writes nothing at all.
    const {streams, fences, outcome} = normalized
    if (streams.length > 0 || fences.length > 0 || outcome !== null) {
      this.calls.push(transactionItems(TABLE, normalized, MARKER_VERSION, () => undefined))
    }
  }
}

// The streams a step names: its own, and a transaction's written and fenced ones.
function stepSuffixes(step: JsonValue): string[] {
  const interfere = step instanceof Map ? step.get('interfere') : undefined
  const nested = [
    ...optionalList(step, 'streams'),
    ...optionalList(step, 'fences'),
    ...optionalList(step, 'writes'),
    ...optionalList(step, 'retryWrites'),
    ...(interfere instanceof Map ? [interfere] : []),
  ]
  const reads = optionalList(step, 'reads').map(str)

  return [...reads, ...[step, ...nested].flatMap((value) => optionalString(value, 'suffix') ?? [])]
}

// An action as the Go fixture generator records it: the operation, the key, the expressions and
// their values. A Put's attribute names are left out, as Go leaves them.
function action(item: TransactItem): unknown {
  const [operation, body] = Object.entries(item)[0] as [
    string,
    {
      Key?: Record<string, AttributeValue>
      Item?: Record<string, AttributeValue>
      UpdateExpression?: string
      ConditionExpression?: string
      ExpressionAttributeNames?: Record<string, string>
      ExpressionAttributeValues?: Record<string, AttributeValue>
    },
  ]
  const key = body.Key ?? body.Item ?? {}
  const recorded: Record<string, unknown> = {
    operation,
    pk: attribute(key, 'pk', 'S'),
    sk: Number(attribute(key, 'sk', 'N')),
  }
  if (body.UpdateExpression !== undefined) {
    recorded.update = body.UpdateExpression
  }
  if (body.ConditionExpression !== undefined) {
    recorded.condition = body.ConditionExpression
  }
  const names = body.ExpressionAttributeNames ?? {}
  if (operation !== 'Put' && Object.keys(names).length > 0) {
    recorded.names = names
  }
  const values = body.ExpressionAttributeValues ?? {}
  if (Object.keys(values).length > 0) {
    recorded.values = values
  }

  return recorded
}

function attribute(item: Record<string, AttributeValue>, name: string, type: 'S' | 'N'): string {
  const value = item[name] as Record<string, unknown> | undefined
  const text = value?.[type]
  if (typeof text !== 'string') {
    throw new Error(`the action's ${name} is not ${type}`)
  }

  return text
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
