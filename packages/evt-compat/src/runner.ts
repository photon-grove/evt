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
  CommitTooLargeError,
  type DigestInput,
  DuplicateViewKeyError,
  type EventOf,
  type FeatureManifest,
  HistoryDependentProjectionError,
  IdempotencyKeyReuseError,
  type JsonValue,
  type Metadata,
  type OutcomeOptions,
  type OutcomeRepository,
  type ReadFence,
  ProjectorOnlyCommitError,
  ProjectorOperationError,
  type ProjectedView,
  type RebuildFence,
  type RebuildLease,
  RebuildInProgressError,
  RebuildLeaseLostError,
  RebuildLockHeldError,
  RebuildUnavailableError,
  type Repository,
  ReservedEntityIdError,
  RetentionConflictError,
  type SerializedEvent,
  Store,
  type StoreOptions,
  StreamHeadDriftError,
  type StreamRead,
  type Transaction,
  type TransactionCommit,
  type TransactionalRepository,
  UnsupportedFeatureError,
  commitTransactionWithReceipt,
  commitWithReceipt,
  defaultDigest,
  deleteView,
  emptyMetadata,
  encodeEvent,
  isEventLog,
  isReserved,
  observeRebuildEpoch,
  encodeSnapshot,
  eventId,
  groupCommits,
  isConflict,
  putView,
  stringifyJson,
  utf8Length,
  ViewKeyTooLongError,
} from '@photon-grove/evt'
import {FaultRepository} from '@photon-grove/evt/testing'

import {type Account, account, accountCommand, ledgerProjector, sortedKeys} from './account.js'
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
  'commit-wide-views',
  'commit-wide-projector',
  'commit-too-large',
  'commit-grouping',
  'feature-marker',
  'reserved-namespaces',
  'rebuild-fence',
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
  'projector',
  'operation',
  'mapped',
  'groups',
  'delta',
  'forceReleased',
  'rows',
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
  'projectors',
  'features',
  'retention',
  'historyDependent',
  'retract',
  'entityPrefix',
  'action',
  'owner',
  'leaseSeconds',
  'seconds',
])

// The keys this runner reads of an executeTransaction step's writes and its interference.
const WRITE_KEYS = new Set(['suffix', 'event'])
const INTERFERE_KEYS = new Set(['suffix', 'event', 'times'])

// The keys this runner reads of a transaction step's streams, their events and its fences.
const STREAM_KEYS = new Set(['suffix', 'sequence', 'events', 'snapshot', 'entityPrefix'])
const EVENT_KEYS = new Set(['type', 'version', 'payload', 'pad'])
const FENCE_KEYS = new Set(['suffix', 'head', 'callerStamped', 'entityType'])

// The scenario keys this runner reads. An exclusive scenario holds store-wide state across its
// steps, so a driver runs it alone, which the fixture run does with a store of its own.
const SCENARIO_KEYS = new Set([
  'name',
  'description',
  'snapshotInterval',
  'steps',
  'outcomes',
  'projector',
  'exclusive',
])

const encoder = new TextEncoder()
const decoder = new TextDecoder()

/** Throws when a scenario sets a key this runner doesn't read. */
export function checkScenario(scenario: JsonValue): void {
  if (!(scenario instanceof Map)) {
    throw new Error('a scenario is not an object')
  }
  checkKeys(scenario, SCENARIO_KEYS, `scenario ${str(field(scenario, 'name'))}`)
}

/** Whether the scenario holds store-wide state, so a driver runs it alone. */
export function isExclusive(scenario: JsonValue): boolean {
  return optionalBoolean(scenario, 'exclusive')
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
  /** The projector of a failed view operation, its operation, and its mapped error's message. */
  projector?: string
  operation?: number
  mapped?: string
  /** A groupCommits step's commits, each its ID and its events. */
  groups?: string[]
  /** How far a declare or rebuild step moved the feature manifest's version or the generation. */
  delta?: number
  /** A rebuild step left the generation flagged force-released. */
  forceReleased?: true
  /** The rows a rebuild's writeViews step left in the scenario's rebuild partition. */
  rows?: number
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
    ...viewFields(
      optionalString(expect, 'projector') ?? '',
      expect.has('operation') ? num(field(expect, 'operation')) : 0,
      optionalString(expect, 'mapped') ?? '',
    ),
    ...(optionalList(expect, 'groups').length > 0 && {
      groups: optionalList(expect, 'groups').map(str),
    }),
    ...(expect.has('delta') && num(field(expect, 'delta')) !== 0
      ? {delta: num(field(expect, 'delta'))}
      : {}),
    ...(expect.has('forceReleased') &&
      field(expect, 'forceReleased') === true && {forceReleased: true as const}),
    ...(expect.has('rows') && num(field(expect, 'rows')) !== 0
      ? {rows: num(field(expect, 'rows'))}
      : {}),
  }
}

// A failed view operation's fields, each only when set, as Go's Outcome omits empty ones.
function viewFields(projector: string, operation: number, mapped: string): Partial<Outcome> {
  return {
    ...(projector !== '' && {projector}),
    ...(operation !== 0 && {operation}),
    ...(mapped !== '' && {mapped}),
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
  /** Reads and declares the store's feature manifest, as another writer would. Backs declare. */
  marker?: MarkerDriver | undefined
  /** The store's rebuild fence, and how time passes and rows are counted. Backs rebuild. */
  rebuild?: RebuildDriver | undefined
}

/** What a rebuild step needs: the fence, a clock to wait on, and the view rows of a partition. */
export interface RebuildDriver {
  readonly fence: RebuildFence
  /** Lets `seconds` pass: an injected clock moves, a live run sleeps. */
  wait(seconds: number): Promise<void>
  /** How many view rows the partition `pk` holds. */
  rows(pk: string): Promise<number>
}

/** A store's feature manifest as a declaring writer sees it. */
export interface MarkerDriver {
  manifest(): FeatureManifest | null | Promise<FeatureManifest | null>
  /** Changes the manifest's declarable parts in one declaration that bumps its version by one. */
  declare(change: (draft: ManifestDraft) => void): FeatureManifest | Promise<FeatureManifest>
}

/** A manifest's declarable parts, which a declaration changes in place. */
export interface ManifestDraft {
  features: Map<string, number>
  historyDependent: string[]
  retention: string[]
}

export class Runner {
  readonly #repository: Repository
  readonly #store: Store
  readonly #outcomes: boolean
  readonly #options: StoreOptions
  readonly #bypassHead: RunnerOptions['bypassHead']
  readonly #marker: RunnerOptions['marker']
  readonly #rebuild: RunnerOptions['rebuild']
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
      ...(optionalBoolean(scenario, 'projector') && {commitProjectors: [ledgerProjector]}),
    }
    this.#store = new Store(repository, this.#options)
    this.#bypassHead = options.bypassHead
    this.#marker = options.marker
    this.#rebuild = options.rebuild
  }

  async step(step: JsonValue): Promise<Outcome> {
    const op = str(field(step, 'op'))
    // A prefix puts the stream of an appendRaw or putSnapshot step in a reserved namespace, which
    // every write path refuses. A transaction's streams carry their own.
    const prefix = optionalString(step, 'entityPrefix') ?? ''
    if (prefix !== '' && op !== 'appendRaw' && op !== 'putSnapshot') {
      throw new Error(`a ${op} step sets entityPrefix, which only appendRaw and putSnapshot read`)
    }
    const id = prefix + streamId(this.entityId, optionalString(step, 'suffix') ?? '')

    switch (op) {
      case 'execute':
        return this.#execute(id, step)
      case 'appendRaw': {
        const raw = field(step, 'event')
        // A stream in a reserved namespace is never loaded: the write is refused before anything
        // is read, so it's tried at sequence 1.
        const sequence = prefix === '' ? (await this.#store.load(account, id)).sequence : 0n
        const event = rawEvent(id, sequence + 1n, step, rawFields(raw))
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
          return classify(error)
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
      case 'groupCommits':
        return this.#groupCommits(step)
      case 'compact':
        try {
          const deleted = await this.#repository.compactBelow(id, int(field(step, 'through')))
          return outcome('compacted', 0, deleted)
        } catch (error) {
          if (error instanceof Error && 'code' in error && error.code === 'compactionUncovered') {
            return outcome('uncovered')
          }
          return classify(error)
        }
      case 'declare':
        return this.#declare(step)
      case 'rebuild':
        return this.#rebuildStep(step)
      case 'enumerate':
        return this.#enumerate()
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
      // The store commits through the receipt methods, which must interfere as well.
      repository.commitWithReceipt = async (commit, options) => {
        await interfere(commit.outcome)
        return commitWithReceipt(inner, commit, options)
      }
      repository.commitTransactionWithReceipt = async (transaction, options) => {
        await interfere(transaction.outcome)
        return commitTransactionWithReceipt(inner, transaction, options)
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

  // Reads the primary stream and each of `reads`, regroups their events into the commits that
  // wrote them, and lists each commit: its ID (`-` for none), then its events as `suffix:sequence`.
  async #groupCommits(step: JsonValue): Promise<Outcome> {
    const base = this.entityId
    const suffixes = ['', ...optionalList(step, 'reads').map(str)]
    const events = (
      await Promise.all(suffixes.map((suffix) => this.#repository.events(streamId(base, suffix))))
    ).flat()
    const groups = groupCommits(events).map((commit) => {
      const members = commit.streams.flatMap((stream) => {
        const suffix = stream.entityId.startsWith(base)
          ? stream.entityId.slice(base.length).replace(/^-/, '')
          : stream.entityId.replace(/^-/, '')

        return stream.events.map((event) => `${suffix}:${event.sequence}`)
      })

      return `${commit.commitId === '' ? '-' : commit.commitId} ${members.join(' ')}`
    })

    return {...outcome('ok', events.length), ...(groups.length > 0 && {groups})}
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

  // One step of the rebuild fence's lifecycle. The owner is scoped to the scenario's stream, and a
  // step that presents a lease rebuilds it from the store, as a separate process would: the lock's
  // fencing generation when it names the owner, else 0. The outcome reports how far the generation
  // moved, whether it's force-released, and a write's rows.
  async #rebuildStep(step: JsonValue): Promise<Outcome> {
    const driver = this.#rebuild
    if (driver === undefined) {
      throw new Error('the backend has no rebuild fence driver')
    }

    const {fence} = driver
    const owner = `${this.entityId}-${optionalString(step, 'owner') ?? ''}`
    const action = str(field(step, 'action'))
    const leaseMs = () => num(field(step, 'leaseSeconds')) * 1000
    const held = async (): Promise<RebuildLease> => {
      const {lock} = await fence.rebuildState()
      return {owner, generation: lock?.owner === owner ? lock.generation : 0n, leaseExpiresAt: 0}
    }
    const partition = `rebuild-${this.entityId}`

    const before = await fence.rebuildGeneration()
    let result = 'ok'
    try {
      switch (action) {
        case 'begin':
          await fence.beginRebuild(owner, leaseMs())
          break
        case 'renew':
          await fence.renewRebuild(await held(), leaseMs())
          break
        case 'end':
          await fence.endRebuild(await held())
          break
        case 'forceRelease':
          await fence.forceReleaseRebuild(owner)
          break
        case 'generation':
          await observeRebuildEpoch(fence)
          result = 'settled'
          break
        case 'wait':
          await driver.wait(num(field(step, 'seconds')))
          break
        case 'writeViews':
          await fence.writeRebuildViews(await held(), [
            {
              projector: 'rebuild',
              projectorIndex: 0,
              operation: 0,
              op: putView(partition, owner, owner, 'rebuild', `{"owner":${JSON.stringify(owner)}}`),
            },
          ])
          break
        default:
          throw new Error(`unknown rebuild action ${action}`)
      }
    } catch (error) {
      if (error instanceof RebuildLockHeldError) {
        result = 'held'
      } else if (error instanceof RebuildLeaseLostError) {
        result = 'leaseLost'
      } else if (error instanceof RebuildUnavailableError) {
        result = 'unavailable'
      } else {
        throw error
      }
    }
    const after = await fence.rebuildGeneration()
    const rows = action === 'writeViews' ? await driver.rows(partition) : 0

    return {
      ...outcome(result),
      ...(after.value !== before.value && {delta: Number(after.value - before.value)}),
      ...(after.forceReleased && {forceReleased: true as const}),
      ...(rows !== 0 && {rows}),
    }
  }

  // Walks the event log and the entity heads, as a rebuild does, and counts the events of the
  // scenario's streams. An engine row of the scenario's (an outcome row, say) in either walk fails
  // the step.
  async #enumerate(): Promise<Outcome> {
    const repository = this.#repository
    if (!isEventLog(repository)) {
      throw new Error('the repository does not enumerate its log')
    }

    let count = 0
    for await (const batch of repository.streamAllEvents()) {
      for (const event of batch) {
        if (!event.entityId.includes(this.entityId)) {
          continue
        }
        if (isReserved(event.entityId)) {
          throw new Error(`enumeration yielded the engine row ${event.entityId}`)
        }
        count++
      }
    }
    for (const entityId of (await repository.entityHeads()).keys()) {
      if (entityId.includes(this.entityId) && isReserved(entityId)) {
        throw new Error(`entity heads yielded the engine row ${entityId}`)
      }
    }

    return outcome('ok', count)
  }

  // Applies one declaration to the store's feature manifest, as another writer would, and reports
  // how far it moved the version. The entity types name the scenario's stream with {id}.
  async #declare(step: JsonValue): Promise<Outcome> {
    const marker = this.#marker
    if (marker === undefined) {
      throw new Error('the backend has no feature marker driver')
    }

    const scoped = (name: string) =>
      optionalList(step, name).map((entityType) =>
        str(entityType).replaceAll('{id}', this.entityId),
      )
    const retention = scoped('retention')
    const history = scoped('historyDependent')
    const raw = step instanceof Map ? step.get('features') : undefined
    const features =
      raw instanceof Map ? Array.from(raw, ([name, format]) => [name, num(format)] as const) : []
    const retract = optionalBoolean(step, 'retract')

    const before = await marker.manifest()
    const declared = await marker.declare((draft) => {
      if (retract) {
        for (const [name] of features) {
          draft.features.delete(name)
        }
        draft.retention = draft.retention.filter((entityType) => !retention.includes(entityType))
        draft.historyDependent = draft.historyDependent.filter(
          (entityType) => !history.includes(entityType),
        )

        return
      }

      for (const [name, format] of features) {
        draft.features.set(name, format)
      }
      draft.retention.push(
        ...retention.filter((entityType) => !draft.retention.includes(entityType)),
      )
      draft.historyDependent.push(
        ...history.filter((entityType) => !draft.historyDependent.includes(entityType)),
      )
    })

    return {...outcome('ok'), delta: Number(declared.version - (before?.version ?? 0n))}
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

// A key padded with `x` to `size` UTF-8 bytes, so a step can make an overlong one.
function padKey(text: string, size: number): string {
  const length = utf8Length(text)

  return length < size ? text + 'x'.repeat(size - length) : text
}

// A raw event of a step: its type, version and payload text.
function rawFields(event: JsonValue): {type: string; version: number; payload: string} {
  // A payload's first `{pad}` stands for `pad` bytes of `x`, so a step can make a large event.
  const pad = optionalNumber(event, 'pad') ?? 0

  return {
    type: str(field(event, 'type')),
    version: num(field(event, 'version')),
    payload: str(field(event, 'payload')).replace('{pad}', () => 'x'.repeat(pad)),
  }
}

function optionalNumber(value: JsonValue, name: string): number | null {
  const found = value instanceof Map ? value.get(name) : undefined

  return found === undefined || found === null ? null : num(found)
}

// In the order Go classifies them.
function classify(error: unknown): Outcome {
  if (error instanceof CommitTooLargeError) {
    return outcome('commitTooLarge')
  }
  if (error instanceof ReservedEntityIdError) {
    return outcome('reserved')
  }
  if (error instanceof UnsupportedFeatureError) {
    return outcome('unsupported')
  }
  if (error instanceof RetentionConflictError) {
    return outcome('retentionConflict')
  }
  if (error instanceof HistoryDependentProjectionError) {
    return outcome('historyDependent')
  }
  if (error instanceof ProjectorOperationError) {
    return {
      ...outcome('projector'),
      ...viewFields(error.projector, error.operation, error.mapped ? messageOf(error.mapped) : ''),
    }
  }
  if (error instanceof ProjectorOnlyCommitError) {
    return outcome('projectorOnly')
  }
  if (error instanceof ViewKeyTooLongError) {
    return outcome('viewKeyTooLong')
  }
  if (error instanceof DuplicateViewKeyError) {
    return outcome('duplicateViewKey')
  }
  if (error instanceof StreamHeadDriftError) {
    return outcome('drift')
  }
  if (error instanceof RebuildInProgressError) {
    return outcome('rebuildInProgress')
  }
  if (isConflict(error)) {
    return outcome('conflict')
  }

  throw error
}

// A mapped error's message, as Go's Err.Error() gives it.
function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
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
    const prefix = optionalString(stream, 'entityPrefix') ?? ''
    const id = prefix + streamId(base, optionalString(stream, 'suffix') ?? '')
    const first = int(field(stream, 'sequence'))
    const events = list(field(stream, 'events')).map((raw, index) =>
      rawEvent(id, first + BigInt(index), step, rawFields(raw)),
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
    // A fence names its stream's type when it isn't the account's, with {id} for the scenario's.
    entityType: (optionalString(fence, 'entityType') ?? 'account').replaceAll('{id}', base),
    head: int(field(fence, 'head')),
    provenance: optionalBoolean(fence, 'callerStamped') ? 'caller' : 'engine',
  }))
  const views = stepViews(base, step)

  return views.length > 0 ? {streams, fences, views} : {streams, fences}
}

/**
 * A transaction step's view operations, in its projectors' order, each operation once or
 * `repeat` times. `{id}` is the scenario's entity ID, `{n}` the copy's index as three digits, and
 * `pkPad` and `skPad` pad a key with `x` to that many bytes.
 */
export function stepViews(base: string, step: JsonValue): ProjectedView[] {
  const views: ProjectedView[] = []
  const scope = (text: string) => text.replaceAll('{id}', base)

  optionalList(step, 'projectors').forEach((projector, projectorIndex) => {
    const name = str(field(projector, 'name'))
    let operation = 0
    for (const item of list(field(projector, 'operations'))) {
      const repeat = Math.max(optionalNumber(item, 'repeat') ?? 0, 1)
      for (let copy = 0; copy < repeat; copy++) {
        const number = (text: string) => text.replaceAll('{n}', String(copy).padStart(3, '0'))
        const pk = padKey(number(scope(str(field(item, 'pk')))), optionalNumber(item, 'pkPad') ?? 0)
        const sk = padKey(number(scope(str(field(item, 'sk')))), optionalNumber(item, 'skPad') ?? 0)
        const owner = scope(str(field(item, 'owner')))
        const condition = (optionalString(item, 'condition') ?? '') || 'none'
        const conflict = optionalString(item, 'conflict') ?? ''
        const mapped = conflict === '' ? {} : {conflict: new Error(conflict)}
        const op =
          str(field(item, 'op')) === 'delete'
            ? deleteView(pk, sk, owner, {condition: condition as 'none' | 'present', ...mapped})
            : putView(
                pk,
                sk,
                owner,
                optionalString(item, 'entityType') ?? '',
                number(scope(optionalString(item, 'payload') ?? '')),
                {condition: condition as 'none' | 'absent', ...mapped},
              )
        views.push({projector: name, projectorIndex, operation, op})
        operation++
      }
    }
  })

  return views
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
