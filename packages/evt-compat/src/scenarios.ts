// Every step depends on the ones before it, so the runner awaits in loops on purpose.
/* oxlint-disable no-await-in-loop */

// The scenarios of compat/scenarios.json that need nothing beyond phase 1 of ADR 0009, run against
// MemRepository and compared byte for byte with the Go-generated compat/fixtures/scenarios.json:
// every step's outcome, then each stream's events, snapshot, head, loaded state, DynamoDB items
// and SNS messages. TypeScript runs every step itself, whichever writer the step names, as the
// Rust fixture test does.

import {
  type JsonValue,
  type Metadata,
  type SerializedEvent,
  Store,
  StreamHeadDriftError,
  emptyMetadata,
  encodeEnvelope,
  encodeEvent,
  encodeSnapshot,
  eventId,
  isConflict,
  stringifyJson,
} from '@photon-grove/evt'
import {headItem, eventItem, snapshotItem} from '@photon-grove/evt/dynamodb'
import {MemRepository, headFixtures} from '@photon-grove/evt/memory'

import {account, accountCommand} from './account.js'
import {type Checks, canonical, field, int, list, loadFixture, num, plain, str} from './harness.js'

/** The scenarios TypeScript supports so far. */
export const PHASE_1_SCENARIOS = [
  'metadata-extensions',
  'go-writes-rust-reads',
  'rust-writes-go-reads',
  'interleaved',
  'compaction-handoff',
  'upcast-legacy',
  'conflict-probe',
  'dedupe-window',
  'batch-crosses-snapshot',
  'snapshots-disabled',
  'stream-heads-snapshot-guard',
  'latest-n',
]

const ORIGIN_SOURCE = 'evt-compat'
const REGION = 'us-west-2'
const DEFAULT_TIMESTAMP = '2026-10-12T00:00:00Z'
const FIXTURE_RUN = 'fixture'

// The outcome keys a phase-1 step's expectation may hold.
const OUTCOME_KEYS = new Set(['result', 'events', 'deleted', 'sequences'])

// The step keys this runner reads. A phase-1 scenario that gains another fails loudly rather
// than running without it.
const STEP_KEYS = new Set([
  'by',
  'op',
  'expect',
  'commandId',
  'principalId',
  'causationId',
  'extensions',
  'trace',
  'address',
  'timestamp',
  'commitId',
  'command',
  'event',
  'sequence',
  'through',
  'snapshot',
  'eventSequence',
  'suffix',
  'n',
])

// The scenario flags that need what phase 1 lacks, and the expectations only they produce.
const LATER_FLAGS = ['outcomes', 'projector', 'exclusive']
const LATER_EXPECTATIONS = ['views', 'outcomes', 'transactions']

const encoder = new TextEncoder()
const decoder = new TextDecoder()

export async function scenarioChecks(checks: Checks): Promise<void> {
  const definitions = list(field(await loadFixture('../scenarios.json'), 'scenarios'))
  const fixtures = await loadFixture('scenarios.json')
  checks.equal('scenarios fixture run', str(field(fixtures, 'run')), FIXTURE_RUN)
  const expectations = new Map(
    list(field(fixtures, 'scenarios')).map((expected) => [str(field(expected, 'name')), expected]),
  )

  for (const name of PHASE_1_SCENARIOS) {
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
  for (const flag of LATER_FLAGS) {
    if (optionalBoolean(scenario, flag)) {
      throw new Error(`the scenario sets ${flag}, which phase 1 doesn't support`)
    }
  }
  for (const key of LATER_EXPECTATIONS) {
    checks.equal(`${name}: no ${key} expected`, optionalList(expected, key).length, 0)
  }

  const repository = new MemRepository()
  const runner = new Runner(
    repository,
    new Store(repository, {snapshotInterval: num(field(scenario, 'snapshotInterval'))}),
    `compat-${FIXTURE_RUN}-${name}`,
  )

  const steps = list(field(scenario, 'steps'))
  for (const [index, step] of steps.entries()) {
    for (const key of step instanceof Map ? step.keys() : []) {
      if (!STEP_KEYS.has(key)) {
        throw new Error(`step ${index} sets ${key}, which this runner doesn't read`)
      }
    }
    const raw = step instanceof Map ? step.get('event') : undefined
    if (raw instanceof Map && raw.has('pad')) {
      throw new Error(`step ${index} pads its event, which this runner doesn't do`)
    }

    const want = expectedOutcome(field(step, 'expect'))
    const got = await runner.step(step)
    const label = `${name} step ${index} (${str(field(step, 'op'))})`
    checks.equal(label, canonical(got), canonical(want))
  }

  const observed = await runner.observe(runner.entityId)
  checks.equal(`${name}: entity ID`, runner.entityId, str(field(expected, 'entityId')))
  checks.equal(`${name}: state`, observed.state, str(field(expected, 'state')))
  checks.equal(`${name}: sequence`, observed.sequence, int(field(expected, 'sequence')))
  checks.equal(
    `${name}: snapshot sequence`,
    observed.snapshotSequence,
    int(field(expected, 'snapshotSequence')),
  )
  compareStream(checks, name, observed, expected)
  checks.equal(
    `${name}: sns messages`,
    canonical(observed.snsMessages),
    canonical(optionalList(expected, 'snsMessages').map(str)),
  )

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
      compareStream(checks, `${name} sibling ${suffix}`, await runner.observe(id), sibling)
    }
  }
}

function compareStream(checks: Checks, name: string, observed: Observed, expected: JsonValue) {
  checks.equal(
    `${name}: events`,
    canonical(observed.events),
    canonical(list(field(expected, 'events')).map(str)),
  )
  const snapshot = field(expected, 'snapshot')
  checks.equal(`${name}: snapshot`, observed.snapshot, snapshot === null ? null : str(snapshot))
  const head = field(expected, 'head')
  checks.equal(`${name}: head`, observed.head, head === null ? null : int(head))
  checks.equal(
    `${name}: dynamo items`,
    canonical(observed.dynamoItems),
    canonical(optionalList(expected, 'dynamoItems').map(plain)),
  )
}

interface Outcome {
  result: string
  events: number
  deleted: number
  sequences?: number[]
}

function expectedOutcome(expect: JsonValue): Outcome {
  if (!(expect instanceof Map)) {
    throw new Error('a step has no expectation')
  }
  for (const key of expect.keys()) {
    if (!OUTCOME_KEYS.has(key)) {
      throw new Error(`a phase-1 step expects ${key}, which this runner doesn't observe`)
    }
  }

  return outcome(
    str(field(expect, 'result')),
    expect.has('events') ? num(field(expect, 'events')) : 0,
    expect.has('deleted') ? num(field(expect, 'deleted')) : 0,
    expect.has('sequences') ? list(field(expect, 'sequences')).map(num) : [],
  )
}

function outcome(result: string, events = 0, deleted = 0, sequences: number[] = []): Outcome {
  return sequences.length > 0 ? {result, events, deleted, sequences} : {result, events, deleted}
}

interface Observed {
  state: string
  sequence: bigint
  snapshotSequence: bigint
  events: string[]
  snapshot: string | null
  head: bigint | null
  dynamoItems: unknown[]
  snsMessages: string[]
}

class Runner {
  readonly #repository: MemRepository
  readonly #store: Store
  readonly entityId: string

  constructor(repository: MemRepository, store: Store, entityId: string) {
    this.#repository = repository
    this.#store = store
    this.entityId = entityId
  }

  async step(step: JsonValue): Promise<Outcome> {
    const op = str(field(step, 'op'))
    const id = streamId(this.entityId, optionalString(step, 'suffix') ?? '')

    switch (op) {
      case 'execute':
        return this.#execute(id, step)
      case 'appendRaw': {
        const raw = field(step, 'event')
        const loaded = await this.#store.load(account, id)
        const event = rawEvent(id, loaded.sequence + 1n, step, {
          type: str(field(raw, 'type')),
          version: num(field(raw, 'version')),
          payload: str(field(raw, 'payload')),
        })
        return this.#commitRaw(event)
      }
      case 'conflictProbe': {
        const sequence = int(field(step, 'sequence'))
        const payload = '{"amountCents":1,"memo":null}'
        const probe = {type: 'account:deposited', version: 1, payload}
        return this.#commitRaw(rawEvent(this.entityId, sequence, step, probe))
      }
      case 'putSnapshot': {
        const snapshot = field(step, 'snapshot')
        try {
          await this.#repository.putSnapshot({
            entityType: 'account',
            entityId: id,
            sequence: int(field(snapshot, 'sequence')),
            eventSequence: int(field(step, 'eventSequence')),
            payload: encoder.encode(str(field(snapshot, 'payload'))),
          })
          return outcome('ok')
        } catch (error) {
          if (error instanceof StreamHeadDriftError) {
            return outcome('drift')
          }
          throw error
        }
      }
      case 'readLatest': {
        const events = await this.#repository.latest(id, num(field(step, 'n')))
        return outcome('ok', events.length, 0, events.map((event) => Number(event.sequence)))
      }
      case 'compact':
        try {
          const deleted = await this.#repository.compactBelow(id, int(field(step, 'through')))
          return outcome('compacted', 0, deleted)
        } catch (error) {
          if (error instanceof Error && 'code' in error && error.code === 'compactionUncovered') {
            return outcome('uncovered')
          }
          throw error
        }
      default:
        throw new Error(`a phase-1 scenario has no ${op} step`)
    }
  }

  async #execute(id: string, step: JsonValue): Promise<Outcome> {
    const command = accountCommand(field(step, 'command'))
    const before = (await this.#store.load(account, id)).sequence

    try {
      const result = await this.#store.execute(account, id, command, {metadata: metadata(step)})
      switch (result.kind) {
        case 'committed':
          return outcome('ok', Number(result.state.sequence - before))
        case 'rejected':
          return outcome('rejected')
        case 'duplicate':
          return outcome('duplicate')
      }
    } catch (error) {
      return classify(error)
    }
  }

  async #commitRaw(event: SerializedEvent): Promise<Outcome> {
    try {
      await this.#repository.commit({entityId: event.entityId, events: [event]})
      return outcome('ok', 1)
    } catch (error) {
      return classify(error)
    }
  }

  async observe(id: string): Promise<Observed> {
    const events = await this.#repository.events(id)
    const snapshot = await this.#repository.snapshot(id)
    const head = headFixtures(this.#repository).storedHead(id)

    // A stream with a sequence gap has no coherent state, so it is observed as absent.
    let state = ''
    let sequence = 0n
    let snapshotSequence = 0n
    try {
      const loaded = await this.#store.load(account, id)
      state = decoder.decode(account.state.encode(loaded.state))
      sequence = loaded.sequence
      snapshotSequence = loaded.snapshotSequence
    } catch (error) {
      if (!(error instanceof Error && 'code' in error && error.code === 'sequenceGap')) {
        throw error
      }
    }

    const dynamoItems: unknown[] = []
    if (head !== null) {
      dynamoItems.push(headItem(id, head))
    }
    if (snapshot) {
      dynamoItems.push(snapshotItem(snapshot))
    }
    dynamoItems.push(...events.map((event) => eventItem(event)))

    return {
      state,
      sequence,
      snapshotSequence,
      events: events.map(encodeEvent),
      snapshot: snapshot && encodeSnapshot(snapshot),
      head,
      dynamoItems,
      snsMessages: events.map((event) => encodeEnvelope(event, ORIGIN_SOURCE)),
    }
  }
}

function classify(error: unknown): Outcome {
  if (isConflict(error)) {
    return outcome('conflict')
  }
  if (error instanceof StreamHeadDriftError) {
    return outcome('drift')
  }

  throw error
}

// A step's metadata. Every field is determined by the step, so every language writes the same
// bytes for it. The commit ID follows the command ID, or an explicit one pinned for a step without
// a command: a raw commit writes it as is, and the store derives it from the command ID.
function metadata(step: JsonValue): Partial<Metadata> {
  const by = str(field(step, 'by'))
  const commandId = nonEmpty(optionalString(step, 'commandId'))

  return {
    commandId,
    trace: optionalStrings(step, 'trace'),
    origin: {source: ORIGIN_SOURCE, endpoint: by},
    address: nonEmpty(optionalString(step, 'address')),
    region: REGION,
    timestamp: nonEmpty(optionalString(step, 'timestamp')) ?? DEFAULT_TIMESTAMP,
    principalId: nonEmpty(optionalString(step, 'principalId')),
    causationId: nonEmpty(optionalString(step, 'causationId')),
    extensions: optionalStrings(step, 'extensions') ?? new Map(),
    commitId:
      commandId === null ? (optionalString(step, 'commitId') ?? '') : `compat-${by}-${commandId}`,
  }
}

function rawEvent(
  entityId: string,
  sequence: bigint,
  step: JsonValue,
  raw: {type: string; version: number; payload: string},
): SerializedEvent {
  return {
    id: eventId(entityId, sequence),
    sequence,
    type: raw.type,
    version: raw.version,
    entityId,
    entityType: 'account',
    payload: encoder.encode(raw.payload),
    metadata: {...emptyMetadata(), ...metadata(step)},
  }
}

function streamId(base: string, suffix: string): string {
  return suffix === '' ? base : `${base}-${suffix}`
}

function nonEmpty(value: string | null): string | null {
  return value === null || value === '' ? null : value
}

function optionalString(value: JsonValue, name: string): string | null {
  const found = value instanceof Map ? value.get(name) : undefined

  return found === undefined || found === null ? null : str(found)
}

function optionalStrings(value: JsonValue, name: string): Map<string, string> | null {
  const found = value instanceof Map ? value.get(name) : undefined
  if (found === undefined || found === null) {
    return null
  }
  if (!(found instanceof Map)) {
    throw new Error(`${name} is not an object: ${stringifyJson(found)}`)
  }

  return new Map(Array.from(found, ([key, entry]) => [key, str(entry)]))
}

function optionalBoolean(value: JsonValue, name: string): boolean {
  const found = value instanceof Map ? value.get(name) : undefined

  return found === true
}

function optionalList(value: JsonValue, name: string): JsonValue[] {
  const found = value instanceof Map ? value.get(name) : undefined

  return found === undefined || found === null ? [] : list(found)
}
