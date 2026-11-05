// The compat/scenarios.json runner both TypeScript drivers share: the fixture run against
// MemRepository (scenarios.ts) and the live driver of the cross-language run (live.ts). It runs one
// step of a scenario against any repository and observes a stream the way the Go-generated
// compat/fixtures/scenarios.json records it.

import {
  type Commit,
  type JsonValue,
  type Metadata,
  type ReadFence,
  type Repository,
  type SerializedEvent,
  Store,
  StreamHeadDriftError,
  type TransactionCommit,
  type TransactionalRepository,
  emptyMetadata,
  encodeEvent,
  encodeSnapshot,
  eventId,
  isConflict,
  stringifyJson,
} from '@photon-grove/evt'

import {account, accountCommand} from './account.js'
import {type Checks, canonical, field, int, list, num, plain, str} from './harness.js'

/** The scenarios TypeScript supports so far, in memory and in the live run. */
export const SUPPORTED_SCENARIOS: readonly string[] = [
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
  'multi-stream',
  'read-fences',
]

export const ORIGIN_SOURCE = 'evt-compat'
const REGION = 'us-west-2'
const DEFAULT_TIMESTAMP = '2026-10-12T00:00:00Z'

// The outcome keys a supported step's expectation may hold.
const OUTCOME_KEYS = new Set(['result', 'events', 'deleted', 'sequences'])

// The step keys this runner reads. A supported scenario that gains another fails loudly rather
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
  'streams',
  'fences',
])

// The keys this runner reads of a transaction step's streams, their events and its fences.
const STREAM_KEYS = new Set(['suffix', 'sequence', 'events', 'snapshot'])
const EVENT_KEYS = new Set(['type', 'version', 'payload'])
const FENCE_KEYS = new Set(['suffix', 'head', 'callerStamped'])

// The scenario flags that need what TypeScript lacks so far.
const LATER_FLAGS = ['outcomes', 'projector', 'exclusive']

const encoder = new TextEncoder()
const decoder = new TextDecoder()

/** Throws when a scenario sets a flag this runner doesn't support. */
export function checkScenario(scenario: JsonValue): void {
  for (const flag of LATER_FLAGS) {
    if (optionalBoolean(scenario, flag)) {
      throw new Error(`the scenario sets ${flag}, which TypeScript doesn't support yet`)
    }
  }
}

/** Throws when a step sets a key this runner doesn't read. */
export function checkStep(step: JsonValue, index: number): void {
  for (const key of step instanceof Map ? step.keys() : []) {
    if (!STEP_KEYS.has(key)) {
      throw new Error(`step ${index} sets ${key}, which this runner doesn't read`)
    }
  }

  const raw = step instanceof Map ? step.get('event') : undefined
  if (raw instanceof Map) {
    checkKeys(raw, EVENT_KEYS, `step ${index} event`)
  }
  for (const stream of optionalList(step, 'streams')) {
    checkKeys(stream, STREAM_KEYS, `step ${index} stream`)
    for (const event of list(field(stream, 'events'))) {
      checkKeys(event, EVENT_KEYS, `step ${index} stream event`)
    }
  }
  for (const fence of optionalList(step, 'fences')) {
    checkKeys(fence, FENCE_KEYS, `step ${index} fence`)
  }
}

function checkKeys(value: JsonValue, known: ReadonlySet<string>, what: string): void {
  for (const key of value instanceof Map ? value.keys() : []) {
    if (!known.has(key)) {
      throw new Error(`${what} sets ${key}, which this runner doesn't read`)
    }
  }
}

export interface Outcome {
  result: string
  events: number
  deleted: number
  sequences?: number[]
}

export function expectedOutcome(expect: JsonValue): Outcome {
  if (!(expect instanceof Map)) {
    throw new Error('a step has no expectation')
  }
  for (const key of expect.keys()) {
    if (!OUTCOME_KEYS.has(key)) {
      throw new Error(`a supported step expects ${key}, which this runner doesn't observe`)
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

/**
 * A stream as compat/fixtures/scenarios.json records it. The DynamoDB items and SNS messages are
 * observed only where the stream is in DynamoDB, or in memory, which builds them.
 */
export interface Observed {
  state: string
  sequence: bigint
  snapshotSequence: bigint
  events: string[]
  snapshot: string | null
  head: bigint | null
  dynamoItems?: unknown[]
  snsMessages?: string[]
}

/** Compares a scenario's main stream with its fixture. */
export function compareObserved(
  checks: Checks,
  name: string,
  observed: Observed,
  expected: JsonValue,
): void {
  checks.equal(`${name}: state`, observed.state, str(field(expected, 'state')))
  checks.equal(`${name}: sequence`, observed.sequence, int(field(expected, 'sequence')))
  checks.equal(
    `${name}: snapshot sequence`,
    observed.snapshotSequence,
    int(field(expected, 'snapshotSequence')),
  )
  compareStream(checks, name, observed, expected)
  if (observed.snsMessages !== undefined) {
    checks.equal(
      `${name}: sns messages`,
      canonical(observed.snsMessages),
      canonical(optionalList(expected, 'snsMessages').map(str)),
    )
  }
}

/** Compares a stream's events, snapshot, head and any DynamoDB items with its fixture. */
export function compareStream(
  checks: Checks,
  name: string,
  observed: Observed,
  expected: JsonValue,
): void {
  checks.equal(
    `${name}: events`,
    canonical(observed.events),
    canonical(list(field(expected, 'events')).map(str)),
  )
  const snapshot = field(expected, 'snapshot')
  checks.equal(`${name}: snapshot`, observed.snapshot, snapshot === null ? null : str(snapshot))
  const head = field(expected, 'head')
  checks.equal(`${name}: head`, observed.head, head === null ? null : int(head))
  if (observed.dynamoItems !== undefined) {
    checks.equal(
      `${name}: dynamo items`,
      canonical(observed.dynamoItems),
      canonical(optionalList(expected, 'dynamoItems').map(plain)),
    )
  }
}

/** A stream's loaded state, or an empty state when a sequence gap leaves it no coherent one. */
export interface Loaded {
  state: string
  sequence: bigint
  snapshotSequence: bigint
}

export class Runner {
  readonly #repository: Repository
  readonly #store: Store
  readonly entityId: string

  constructor(repository: Repository, scenario: JsonValue, entityId: string) {
    this.#repository = repository
    const snapshotInterval = num(field(scenario, 'snapshotInterval'))
    this.#store = new Store(repository, {snapshotInterval})
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
      case 'transaction':
        return this.#transaction(step)
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
        throw new Error(`a supported scenario has no ${op} step`)
    }
  }

  async load(id: string): Promise<Loaded> {
    try {
      const loaded = await this.#store.load(account, id)
      return {
        state: decoder.decode(account.state.encode(loaded.state)),
        sequence: loaded.sequence,
        snapshotSequence: loaded.snapshotSequence,
      }
    } catch (error) {
      if (!(error instanceof Error && 'code' in error && error.code === 'sequenceGap')) {
        throw error
      }

      return {state: '', sequence: 0n, snapshotSequence: 0n}
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

  // Commits the step's streams and fences through the repository, as the Go and Rust drivers do,
  // and checks that a failed transaction left every stream it touches unchanged.
  async #transaction(step: JsonValue): Promise<Outcome> {
    const repository = this.#repository
    if (!isTransactional(repository)) {
      throw new Error('the repository has no commitTransaction')
    }

    const transaction = transactionCommit(this.entityId, step)
    const touched = [
      ...transaction.streams.map((stream) => stream.entityId),
      ...(transaction.fences ?? []).map((fence) => fence.entityId),
    ]
    const before = await this.#stored(touched)
    try {
      await repository.commitTransaction(transaction)
      const events = transaction.streams.reduce((sum, stream) => sum + stream.events.length, 0)
      return outcome('ok', events)
    } catch (error) {
      if ((await this.#stored(touched)) !== before) {
        throw new Error(`a failed transaction changed a stream it touches: ${String(error)}`, {
          cause: error,
        })
      }
      return classify(error)
    }
  }

  // The stored events and snapshot of each stream, to compare before and after a transaction.
  async #stored(entityIds: readonly string[]): Promise<string> {
    const stored = await Promise.all(
      entityIds.map(async (id) => {
        const events = await this.#repository.events(id)
        const snapshot = await this.#repository.snapshot(id)

        return [events.map(encodeEvent), snapshot && encodeSnapshot(snapshot)]
      }),
    )

    return stringifyJson(stored)
  }

  async #commitRaw(event: SerializedEvent): Promise<Outcome> {
    try {
      await this.#repository.commit({entityId: event.entityId, events: [event]})
      return outcome('ok', 1)
    } catch (error) {
      return classify(error)
    }
  }
}

function isTransactional(repository: Repository): repository is TransactionalRepository {
  return 'commitTransaction' in repository
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

/**
 * A transaction step's commit: each stream's raw events from its sequence, with the step's
 * metadata and its inline snapshot, and each fence, the engine's unless the caller stamped it.
 */
export function transactionCommit(base: string, step: JsonValue): TransactionCommit {
  const streams = optionalList(step, 'streams').map((stream): Commit => {
    const id = streamId(base, optionalString(stream, 'suffix') ?? '')
    const first = int(field(stream, 'sequence'))
    const events = list(field(stream, 'events')).map((raw, index) =>
      rawEvent(id, first + BigInt(index), step, {
        type: str(field(raw, 'type')),
        version: num(field(raw, 'version')),
        payload: str(field(raw, 'payload')),
      }),
    )
    const snapshot = stream instanceof Map ? stream.get('snapshot') : undefined
    if (snapshot === undefined || snapshot === null) {
      return {entityId: id, events}
    }

    return {
      entityId: id,
      events,
      snapshot: {
        entityType: 'account',
        sequence: int(field(snapshot, 'sequence')),
        payload: encoder.encode(str(field(snapshot, 'payload'))),
      },
    }
  })
  const fences = optionalList(step, 'fences').map((fence): ReadFence => ({
    entityId: streamId(base, optionalString(fence, 'suffix') ?? ''),
    entityType: 'account',
    head: int(field(fence, 'head')),
    provenance: optionalBoolean(fence, 'callerStamped') ? 'caller' : 'engine',
  }))

  return {streams, fences}
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

export function streamId(base: string, suffix: string): string {
  return suffix === '' ? base : `${base}-${suffix}`
}

function nonEmpty(value: string | null): string | null {
  return value === null || value === '' ? null : value
}

export function optionalString(value: JsonValue, name: string): string | null {
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

export function optionalList(value: JsonValue, name: string): JsonValue[] {
  const found = value instanceof Map ? value.get(name) : undefined

  return found === undefined || found === null ? [] : list(found)
}
