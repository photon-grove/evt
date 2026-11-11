// The compat/scenarios.json runner both TypeScript drivers share: the fixture run against
// MemRepository (scenarios.ts) and the live driver of the cross-language run (live.ts). It runs one
// step of a scenario against any repository and observes a stream the way the Go-generated
// compat/fixtures/scenarios.json records it.

import {
  AmbiguousWriteError,
  CommandIdTooLongError,
  type CommandOutcome,
  type Commit,
  DEFAULT_RETRY_POLICY,
  type DigestInput,
  type EventOf,
  IdempotencyKeyReuseError,
  type JsonValue,
  type Metadata,
  type OutcomeOptions,
  type OutcomeRepository,
  type ReadFence,
  type Repository,
  type SerializedEvent,
  Store,
  type StoreOptions,
  StreamHeadDriftError,
  type StreamRead,
  type Transaction,
  type TransactionCommit,
  type TransactionalRepository,
  defaultDigest,
  emptyMetadata,
  encodeEvent,
  encodeSnapshot,
  eventId,
  isConflict,
  stringifyJson,
} from '@photon-grove/evt'
import {FaultRepository} from '@photon-grove/evt/testing'

import {type Account, account, accountCommand, sortedKeys} from './account.js'
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
  'outcomes-idempotency',
  'outcomes-noop',
  'outcomes-rejection',
  'outcomes-rejection-events',
  'outcomes-ambiguous',
  'stream-heads-drift',
  'fence-aware-retry',
]

/** The fixture run's name, which every run's identity digests use. */
export const FIXTURE_RUN = 'fixture'

export const ORIGIN_SOURCE = 'evt-compat'
const REGION = 'us-west-2'
const DEFAULT_TIMESTAMP = '2026-10-12T00:00:00Z'

// The outcome keys a supported step's expectation may hold.
const OUTCOME_KEYS = new Set([
  'result',
  'events',
  'deleted',
  'sequences',
  'code',
  'replayed',
  'prepares',
])

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
  'injectCommitFault',
  'identity',
  'response',
  'attempts',
  'reads',
  'writes',
  'retryWrites',
  'interfere',
])

// The keys this runner reads of an executeTransaction step's writes and its interference.
const WRITE_KEYS = new Set(['suffix', 'event'])
const INTERFERE_KEYS = new Set(['suffix', 'event', 'times'])

// The keys this runner reads of a transaction step's streams, their events and its fences.
const STREAM_KEYS = new Set(['suffix', 'sequence', 'events', 'snapshot'])
const EVENT_KEYS = new Set(['type', 'version', 'payload'])
const FENCE_KEYS = new Set(['suffix', 'head', 'callerStamped'])

// The scenario flags that need what TypeScript lacks so far.
const LATER_FLAGS = ['projector', 'exclusive']

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
  for (const write of [...optionalList(step, 'writes'), ...optionalList(step, 'retryWrites')]) {
    checkKeys(write, WRITE_KEYS, `step ${index} write`)
    checkKeys(field(write, 'event'), EVENT_KEYS, `step ${index} write event`)
  }
  const interfere = step instanceof Map ? step.get('interfere') : undefined
  if (interfere instanceof Map) {
    checkKeys(interfere, INTERFERE_KEYS, `step ${index} interference`)
    checkKeys(field(interfere, 'event'), EVENT_KEYS, `step ${index} interference event`)
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
  /** A rejected command's code. */
  code?: string
  /** The command's outcome was read from its stored row. */
  replayed?: true
  /** How many times a multi-stream command's decision step ran. */
  prepares?: number
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

  return {
    ...outcome(
      str(field(expect, 'result')),
      expect.has('events') ? num(field(expect, 'events')) : 0,
      expect.has('deleted') ? num(field(expect, 'deleted')) : 0,
      expect.has('sequences') ? list(field(expect, 'sequences')).map(num) : [],
    ),
    ...commandFields(
      optionalString(expect, 'code') ?? '',
      expect.has('replayed') && field(expect, 'replayed') === true,
    ),
    ...(expect.has('prepares') && num(field(expect, 'prepares')) !== 0
      ? {prepares: num(field(expect, 'prepares'))}
      : {}),
  }
}

function outcome(result: string, events = 0, deleted = 0, sequences: number[] = []): Outcome {
  return sequences.length > 0 ? {result, events, deleted, sequences} : {result, events, deleted}
}

// A command outcome's code and replay flag, present only when set, as Go's Outcome omits them.
function commandFields(code: string, replayed: boolean): Partial<Outcome> {
  return {...(code !== '' && {code}), ...(replayed && {replayed: true as const})}
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

/** What a runner needs of its backend beyond the repository. */
export interface RunnerOptions {
  /**
   * Appends an event to the backend's log without maintaining the stream's head item, as a writer
   * that predates heads would: the writer a head-aware build must detect. Backs the
   * appendRawBypassingHead step.
   */
  bypassHead?: ((event: SerializedEvent) => Promise<void>) | undefined
}

export class Runner {
  readonly #repository: Repository
  readonly #store: Store
  readonly #outcomes: boolean
  readonly #options: StoreOptions
  readonly #bypassHead: RunnerOptions['bypassHead']
  readonly #run: string
  readonly entityId: string

  /** A runner of `scenario` in the run named `run`, whose stream is `compat-<run>-<scenario>`. */
  constructor(
    repository: Repository,
    scenario: JsonValue,
    run: string,
    options: RunnerOptions = {},
  ) {
    this.#repository = repository
    this.#run = run
    this.entityId = `compat-${run}-${str(field(scenario, 'name'))}`
    this.#outcomes = optionalBoolean(scenario, 'outcomes')
    this.#options = {
      snapshotInterval: num(field(scenario, 'snapshotInterval')),
      ...(this.#outcomes && {outcomes: outcomeOptions(run)}),
    }
    this.#store = new Store(repository, this.#options)
    this.#bypassHead = options.bypassHead
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
      case 'appendRawBypassingHead': {
        if (this.#bypassHead === undefined) {
          throw new Error('the backend has no way to bypass the head item')
        }
        const raw = field(step, 'event')
        const loaded = await this.#store.load(account, id)
        await this.#bypassHead(
          rawEvent(id, loaded.sequence + 1n, step, {
            type: str(field(raw, 'type')),
            version: num(field(raw, 'version')),
            payload: str(field(raw, 'payload')),
          }),
        )
        return outcome('ok', 1)
      }
      case 'outcomeOnly':
        return this.#outcomeOnly(step)
      case 'executeTransaction':
        return this.#executeTransaction(step)
      case 'resolveOutcome':
        return this.#resolveOutcome(id, step)
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

  // A command on an outcomes runner records its outcome. A step that injects a commit fault runs
  // on a store over a repository that fails its commit.
  async #executeWithOutcome(id: string, step: JsonValue): Promise<Outcome> {
    const command = accountCommand(field(step, 'command'))
    let store = this.#store
    const fault = optionalString(step, 'injectCommitFault')
    if (fault !== null) {
      const repository = new FaultRepository(this.#repository as OutcomeRepository)
      switch (fault) {
        case 'applyThenFail':
          repository.applyThenFail()
          break
        case 'failWithoutApplying':
          repository.failWithoutApplying()
          break
        default:
          throw new Error(`unknown injectCommitFault ${fault}`)
      }
      store = new Store(repository, this.#options)
    }

    try {
      const result = await store.executeWithOutcome(account, id, command, {
        metadata: this.#metadata(step),
      })
      return commandResult(result)
    } catch (error) {
      return classifyCommand(error)
    }
  }

  // A multi-stream command under fence-aware retry, with the step's number of immediate attempts.
  // The decision step reads every stream in `reads` and every one it writes, stamps the step's
  // fences, and appends `writes` on the first attempt and `retryWrites` (when set) on the ones
  // after, so its write set can change while its identity, the command's type and payload
  // targeting the primary stream, stays the same. `interfere` moves a stream before each of the
  // first commits, as another writer would. The outcome counts the decision step's runs.
  async #executeTransaction(step: JsonValue): Promise<Outcome> {
    const base = this.entityId
    const identity = field(step, 'identity')
    const payload = identity instanceof Map ? identity.get('payload') : undefined
    const interference = step instanceof Map ? step.get('interfere') : undefined

    // Before each of the first `times` commits that carry the outcome, the interference's event
    // lands on its stream as another writer's, under a commit ID of its own.
    const repository = new FaultRepository(this.#repository as OutcomeRepository)
    if (interference instanceof Map) {
      const times = num(field(interference, 'times'))
      const commandId = optionalString(step, 'commandId') ?? ''
      const by = str(field(step, 'by'))
      let moved = 0
      const interfere = async (recorded: unknown): Promise<void> => {
        if (recorded === null || recorded === undefined || moved >= times) {
          return
        }
        moved++
        const id = streamId(base, optionalString(interference, 'suffix') ?? '')
        const loaded = await this.#store.load(account, id)
        const writer = new Map<string, JsonValue>([
          ['by', by],
          ['commitId', `compat-${by}-${commandId}-interfere-${moved}`],
        ])
        await this.#repository.commit({
          entityId: id,
          events: [
            rawEvent(id, loaded.sequence + 1n, writer, rawFields(field(interference, 'event'))),
          ],
        })
      }
      const inner = this.#repository as OutcomeRepository
      repository.commit = async (commit, options) => {
        await interfere(commit.outcome)
        return inner.commit(commit, options)
      }
      repository.commitTransaction = async (transaction, options) => {
        await interfere(transaction.outcome)
        return inner.commitTransaction(transaction, options)
      }
    }

    const attempts = Math.max(optionalNumber(step, 'attempts') ?? 1, 1)
    const store = new Store(repository, {
      ...this.#options,
      outcomes: {
        ...outcomeOptions(this.#run),
        retry: {maxAttempts: attempts, baseDelayMs: 0, maxDelayMs: 0},
      },
    })

    let prepares = 0
    let result: Outcome
    try {
      const recorded = await store.executeTransaction(
        {
          commandType: str(field(identity, 'type')),
          targets: [base],
          payload:
            payload === undefined || payload === null
              ? null
              : encoder.encode(stringifyJson(sortedKeys(payload, true))),
        },
        async (transaction) => {
          prepares++
          const retryWrites = optionalList(step, 'retryWrites')
          const writes =
            prepares > 1 && retryWrites.length > 0 ? retryWrites : optionalList(step, 'writes')
          await prepareStep(transaction, base, step, writes)

          return {kind: 'accept'}
        },
        {metadata: this.#metadata(step)},
      )
      result = commandResult(recorded)
    } catch (error) {
      result = classifyCommand(error)
    }

    // Go omits a count of zero, as it omits every empty field.
    return prepares === 0 ? result : {...result, prepares}
  }

  // A command that writes no events, recorded as an accepted outcome carrying the step's response.
  async #outcomeOnly(step: JsonValue): Promise<Outcome> {
    const identity = field(step, 'identity')
    const payload = identity instanceof Map ? identity.get('payload') : undefined
    const response = optionalString(step, 'response') ?? ''

    try {
      const transaction = this.#store.transaction()
      const begun = await transaction.beginCommand(
        {
          commandType: str(field(identity, 'type')),
          payload:
            payload === undefined || payload === null
              ? null
              : encoder.encode(stringifyJson(sortedKeys(payload, true))),
        },
        {metadata: this.#metadata(step)},
      )
      if (begun.kind === 'replayed') {
        return commandResult(begun.outcome)
      }

      return commandResult(
        await transaction.commitWithOutcome({response: encoder.encode(response)}),
      )
    } catch (error) {
      return classifyCommand(error)
    }
  }

  // Reads the step's command's outcome row, as ambiguous-write recovery does, running nothing.
  async #resolveOutcome(id: string, step: JsonValue): Promise<Outcome> {
    const command = accountCommand(field(step, 'command'))
    try {
      const result = await this.#store.resolveOutcome(account, id, command, {
        metadata: this.#metadata(step),
      })
      return result === null ? outcome('absent') : commandResult(result)
    } catch (error) {
      return classifyCommand(error)
    }
  }

  // A step's metadata. On an outcomes runner the command ID is scoped to the run and the
  // scenario, since outcome rows are global to the table.
  #metadata(step: JsonValue): Partial<Metadata> {
    const stamped = metadata(step)
    if (!this.#outcomes || stamped.commandId === null || stamped.commandId === undefined) {
      return stamped
    }

    const commandId = `${this.entityId}-${stamped.commandId}`

    return {...stamped, commandId, commitId: `compat-${str(field(step, 'by'))}-${commandId}`}
  }

  async #execute(id: string, step: JsonValue): Promise<Outcome> {
    if (this.#outcomes) {
      return this.#executeWithOutcome(id, step)
    }

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

// A command outcome as a step's result: the events its row lists, its code when rejected, and
// whether it was replayed.
function commandResult(result: CommandOutcome): Outcome {
  const {outcome: recorded, replayed} = result
  const rejected = recorded.rejection !== null

  return {
    ...outcome(rejected ? 'rejected' : 'ok', recorded.eventIds.length),
    ...commandFields(recorded.rejection?.code ?? '', replayed),
  }
}

// The errors of a command on an outcomes runner, then the ones every step classifies.
function classifyCommand(error: unknown): Outcome {
  if (error instanceof IdempotencyKeyReuseError) {
    return outcome('reuse')
  }
  if (error instanceof CommandIdTooLongError) {
    return outcome('idTooLong')
  }
  if (error instanceof AmbiguousWriteError) {
    return outcome('ambiguous')
  }

  return classify(error)
}

// The outcome options of a runner: the default digest with the run's name in each target
// replaced by the fixture run's, so a command digests alike in every run and the fixtures'
// digests hold against live backends; a short recovery window, which keeps the dropped-commit
// step quick; and one attempt per step, since a step pins what one attempt returns.
function outcomeOptions(run: string): OutcomeOptions {
  const from = `compat-${run}-`
  const to = `compat-${FIXTURE_RUN}-`

  return {
    digest: (input: DigestInput) =>
      defaultDigest({
        ...input,
        // The first occurrence, as Go's strings.Replace(stream, from, to, 1) does.
        targets: input.targets.map((target) => target.replace(from, to)),
      }),
    recoveryWaitMs: 100,
    retry: {...DEFAULT_RETRY_POLICY, maxAttempts: 1},
  }
}

// An executeTransaction step's decision: read, fence, append. Each read is in the read set's order,
// so it awaits in loops on purpose.
/* oxlint-disable no-await-in-loop */
async function prepareStep(
  transaction: Transaction,
  base: string,
  step: JsonValue,
  writes: readonly JsonValue[],
): Promise<void> {
  const reads = new Map<string, StreamRead<Account, typeof account.events>>()
  const load = async (suffix: string) => {
    const id = streamId(base, suffix)
    let read = reads.get(id)
    if (read === undefined) {
      read = await transaction.read(account, id)
      reads.set(id, read)
    }

    return read
  }

  for (const suffix of optionalList(step, 'reads')) {
    await load(str(suffix))
  }
  for (const fence of optionalList(step, 'fences')) {
    transaction.fence({
      entityId: streamId(base, optionalString(fence, 'suffix') ?? ''),
      entityType: 'account',
      head: int(field(fence, 'head')),
    })
  }
  for (const write of writes) {
    const read = await load(optionalString(write, 'suffix') ?? '')
    const {type, payload} = rawFields(field(write, 'event'))
    const definition = account.events[type as keyof typeof account.events]
    if (definition === undefined) {
      throw new Error(`unknown account event ${type}`)
    }
    const data = definition.codec.decode(encoder.encode(payload))
    transaction.append(read, [{type, data} as EventOf<typeof account.events>])
  }
}
/* oxlint-enable no-await-in-loop */

// A raw event of a step: its type, version and payload text.
function rawFields(event: JsonValue): {type: string; version: number; payload: string} {
  return {
    type: str(field(event, 'type')),
    version: num(field(event, 'version')),
    payload: str(field(event, 'payload')),
  }
}

function optionalNumber(value: JsonValue, name: string): number | null {
  const found = value instanceof Map ? value.get(name) : undefined

  return found === undefined || found === null ? null : num(found)
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
