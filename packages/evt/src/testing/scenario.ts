// Given/when/then helpers over MemRepository, the counterpart of Go's `test` package and Rust's
// `photon_evt::testing`.

import {
  type Aggregate,
  type EventDefinitions,
  type EventOf,
  type Rejection,
  encodeEventData,
  replayEvent,
} from '../aggregate.js'
import {type SerializedEvent, eventId} from '../event.js'
import {MemRepository} from '../memory/repository.js'
import {type Metadata, emptyMetadata} from '../metadata.js'
import {type ExecuteResult, type Loaded, Store, type StoreOptions} from '../store.js'
import {AssertionError, deepEqual, show} from './equal.js'

/** The entity ID a {@link Scenario} runs against unless {@link Scenario.entity} sets one. */
export const DEFAULT_ENTITY_ID = 'entity-1'

/**
 * A {@link MemRepository} with helpers to seed and read streams of any aggregate. Hand its
 * `repository` to the code under test, or run several {@link Scenario}s on it with
 * {@link Scenario.on} so they build on each other.
 */
export class Fixture {
  readonly repository: MemRepository

  constructor(repository: MemRepository = new MemRepository()) {
    this.repository = repository
  }

  /** A store over the fixture's repository. */
  store(options: StoreOptions = {}): Store {
    return new Store(this.repository, options)
  }

  /**
   * Appends events to a stream as if earlier commands had committed them. On a stream that
   * already has events, it continues the sequence from the loaded aggregate, which is where the
   * snapshot leaves off after a compaction. It writes no snapshot, and its metadata is empty but
   * for a commit ID.
   */
  async given<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    entityId: string,
    events: readonly EventOf<D>[],
  ): Promise<void> {
    if (events.length === 0) {
      return
    }

    let sequence = (await this.load(aggregate, entityId)).sequence
    const commitId = `given-${entityId}-${sequence + 1n}`
    const stored: SerializedEvent[] = events.map((event) => {
      sequence++
      const {version, payload} = encodeEventData(aggregate.events, event)
      const metadata: Metadata = {...emptyMetadata(), commitId}

      return {
        id: eventId(entityId, sequence),
        sequence,
        type: event.type,
        version,
        entityId,
        entityType: aggregate.type,
        payload,
        metadata,
      }
    })

    await this.repository.commit({entityId, events: stored})
  }

  /** The stream's stored events, upcast and decoded, oldest first. */
  async events<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    entityId: string,
  ): Promise<EventOf<D>[]> {
    const stored = await this.repository.events(entityId)

    return stored.map((event) => replayEvent(aggregate.events, event))
  }

  /** The aggregate loaded from its stream, from the snapshot like {@link Store.load}. */
  load<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    entityId: string,
  ): Promise<Loaded<S>> {
    return this.store().load(aggregate, entityId)
  }

  /** Asserts that the stream holds exactly these events, decoded. */
  async assertEvents<S, C, D extends EventDefinitions>(
    aggregate: Aggregate<S, C, D>,
    entityId: string,
    expected: readonly EventOf<D>[],
  ): Promise<void> {
    const actual = await this.events(aggregate, entityId)

    assertEqual(actual, expected, `events of stream ${entityId}`)
  }
}

/**
 * Starts a given/when/then test of one command against one stream:
 *
 * ```ts
 * await scenario(account)
 *   .given({type: 'account:opened', data: {owner: 'ada'}})
 *   .when({type: 'deposit', amountCents: 5})
 *   .thenEvents({type: 'account:deposited', data: {amountCents: 5}})
 * ```
 *
 * Each `then` method runs the command through a real {@link Store} over a {@link MemRepository}
 * and throws an {@link AssertionError} when the outcome differs, which fails the surrounding test
 * in any runner. {@link When.run} gives the {@link Outcome} to inspect further.
 */
export function scenario<S, C, D extends EventDefinitions>(
  aggregate: Aggregate<S, C, D>,
): Scenario<S, C, D> {
  return new Scenario(aggregate, {
    fixture: null,
    entityId: DEFAULT_ENTITY_ID,
    given: [],
    metadata: {},
    options: {},
  })
}

/** What a {@link Scenario} runs with. Build one with {@link scenario}. */
export interface ScenarioSetup<D extends EventDefinitions> {
  readonly fixture: Fixture | null
  readonly entityId: string
  readonly given: readonly EventOf<D>[]
  readonly metadata: Partial<Metadata>
  readonly options: StoreOptions
}

/**
 * A command's setting: the aggregate, the stream and its prior events, and how the store runs.
 * Each method returns a new scenario, so one can be the base of several tests. Without
 * {@link Scenario.on}, each run gets a fixture of its own.
 */
export class Scenario<S, C, D extends EventDefinitions> {
  readonly aggregate: Aggregate<S, C, D>
  readonly #setup: ScenarioSetup<D>

  constructor(aggregate: Aggregate<S, C, D>, setup: ScenarioSetup<D>) {
    this.aggregate = aggregate
    this.#setup = setup
  }

  /** Adds events the stream holds before the command. */
  given(...events: EventOf<D>[]): Scenario<S, C, D> {
    return this.#with({given: [...this.#setup.given, ...events]})
  }

  /** Sets the stream's entity ID ({@link DEFAULT_ENTITY_ID} by default). */
  entity(entityId: string): Scenario<S, C, D> {
    return this.#with({entityId})
  }

  /** Sets the metadata the command runs with, such as a command ID to test duplicates. */
  metadata(metadata: Partial<Metadata>): Scenario<S, C, D> {
    return this.#with({metadata})
  }

  /** Sets the store's options: its clock, region, snapshot interval or commit IDs. */
  options(options: StoreOptions): Scenario<S, C, D> {
    return this.#with({options})
  }

  /**
   * Runs on an existing fixture, so the streams it holds are visible and several scenarios build
   * on each other. Each run seeds the given events and runs the command again, so on a shared
   * fixture, running a scenario twice writes it twice. Each run gets a new fixture otherwise.
   */
  on(fixture: Fixture): Scenario<S, C, D> {
    return this.#with({fixture})
  }

  /** Names the command to run. */
  when(command: C): When<S, C, D> {
    return new When(this.aggregate, this.#setup, command)
  }

  #with(change: Partial<ScenarioSetup<D>>): Scenario<S, C, D> {
    return new Scenario(this.aggregate, {...this.#setup, ...change})
  }
}

/** A {@link Scenario} with its command, ready to run. */
export class When<S, C, D extends EventDefinitions> {
  readonly #aggregate: Aggregate<S, C, D>
  readonly #setup: ScenarioSetup<D>
  readonly #command: C

  constructor(aggregate: Aggregate<S, C, D>, setup: ScenarioSetup<D>, command: C) {
    this.#aggregate = aggregate
    this.#setup = setup
    this.#command = command
  }

  /**
   * Seeds the given events, runs the command, and resolves to its {@link Outcome}. Each call does
   * both again. Invalid store options or a failure to seed throw; a failure of the command is the
   * outcome's.
   */
  async run(): Promise<Outcome<S, C, D>> {
    const {entityId, given, metadata, options} = this.#setup
    const fixture = this.#setup.fixture ?? new Fixture()
    const store = fixture.store(options)
    await fixture.given(this.#aggregate, entityId, given)

    let settled: PromiseSettledResult<ExecuteResult<S>>
    try {
      const value = await store.execute(this.#aggregate, entityId, this.#command, {metadata})
      settled = {status: 'fulfilled', value}
    } catch (error) {
      settled = {status: 'rejected', reason: error}
    }

    return new Outcome(this.#aggregate, fixture, entityId, settled)
  }

  /** Asserts that the command commits exactly these events. */
  async thenEvents(...expected: EventOf<D>[]): Promise<void> {
    const outcome = await this.run()
    outcome.assertEvents(...expected)
  }

  /** Asserts that the command commits and emits nothing. */
  async thenNoEvents(): Promise<void> {
    const outcome = await this.run()
    outcome.assertEvents()
  }

  /**
   * Asserts that the handler rejects the command, with this code or a rejection `matches`
   * accepts.
   */
  async thenRejected(matches?: string | ((rejection: Rejection) => boolean)): Promise<void> {
    const outcome = await this.run()
    outcome.assertRejected(matches)
  }

  /** Asserts that the command is a duplicate of one already in the dedupe window. */
  async thenDuplicate(): Promise<void> {
    const outcome = await this.run()
    outcome.assertDuplicate()
  }

  /** Asserts that the store throws, with an error `matches` accepts. */
  async thenError(matches?: (error: unknown) => boolean): Promise<void> {
    const outcome = await this.run()
    outcome.assertError(matches)
  }

  /** Asserts that the command commits, then hands `check` the state after it. */
  async thenState(check: (state: S) => void | Promise<void>): Promise<void> {
    const outcome = await this.run()
    await check(outcome.state.state)
  }
}

/** The result of a command that committed. */
export type Committed<S> = Extract<ExecuteResult<S>, {kind: 'committed'}>

/** What running a {@link Scenario} did. */
export class Outcome<S, C, D extends EventDefinitions> {
  readonly aggregate: Aggregate<S, C, D>
  /** The fixture the scenario ran on, to inspect other streams. */
  readonly fixture: Fixture
  readonly entityId: string
  /** What {@link Store.execute} resolved to, or the error it threw. */
  readonly settled: PromiseSettledResult<ExecuteResult<S>>

  constructor(
    aggregate: Aggregate<S, C, D>,
    fixture: Fixture,
    entityId: string,
    settled: PromiseSettledResult<ExecuteResult<S>>,
  ) {
    this.aggregate = aggregate
    this.fixture = fixture
    this.entityId = entityId
    this.settled = settled
  }

  /** The events as stored, if the command committed. */
  get storedEvents(): SerializedEvent[] | null {
    const settled = this.settled

    return settled.status === 'fulfilled' && settled.value.kind === 'committed'
      ? settled.value.events
      : null
  }

  /** The aggregate after the command. Throws unless it committed. */
  get state(): Loaded<S> {
    return this.assertCommitted().state
  }

  /** Asserts that the command committed, and returns what it did. */
  assertCommitted(): Committed<S> {
    const settled = this.settled
    if (settled.status === 'fulfilled' && settled.value.kind === 'committed') {
      return settled.value
    }

    throw new AssertionError(
      `expected the ${this.aggregate.type} command to commit, got ${this.#describe()}`,
    )
  }

  /** Asserts that the command committed exactly these events, decoded. */
  assertEvents(...expected: EventOf<D>[]): void {
    const committed = this.assertCommitted()
    const actual = committed.events.map((event) => replayEvent(this.aggregate.events, event))

    assertEqual(actual, expected, `events emitted for ${this.aggregate.type} ${this.entityId}`)
  }

  /** Asserts that the handler rejected the command, with this code or as `matches` accepts. */
  assertRejected(matches?: string | ((rejection: Rejection) => boolean)): Rejection {
    const settled = this.settled
    if (settled.status !== 'fulfilled' || settled.value.kind !== 'rejected') {
      throw new AssertionError(`expected a rejection, got ${this.#describe()}`)
    }

    const rejection = settled.value.rejection
    if (typeof matches === 'string') {
      if (rejection.code !== matches) {
        throw new AssertionError(
          `expected the rejection ${show(matches)}, got ${show(rejection.code)}`,
          rejection.code,
          matches,
        )
      }
    } else if (matches && !matches(rejection)) {
      throw new AssertionError(`unexpected rejection: ${show(rejection)}`, rejection)
    }

    return rejection
  }

  /** Asserts that the command was a duplicate, and returns its command ID. */
  assertDuplicate(): string {
    const settled = this.settled
    if (settled.status !== 'fulfilled' || settled.value.kind !== 'duplicate') {
      throw new AssertionError(`expected a duplicate command, got ${this.#describe()}`)
    }

    return settled.value.commandId
  }

  /** Asserts that the store threw, with an error `matches` accepts, and returns the error. */
  assertError(matches?: (error: unknown) => boolean): unknown {
    const settled = this.settled
    if (settled.status !== 'rejected') {
      throw new AssertionError(`expected a store error, got ${this.#describe()}`)
    }
    if (matches && !matches(settled.reason)) {
      throw new AssertionError(`unexpected store error: ${show(settled.reason)}`, settled.reason)
    }

    return settled.reason
  }

  #describe(): string {
    const settled = this.settled
    if (settled.status === 'rejected') {
      return `a store error: ${show(settled.reason)}`
    }

    const result = settled.value
    switch (result.kind) {
      case 'committed':
        return `committed events: ${showEvents(this.aggregate.events, result.events)}`
      case 'duplicate':
        return `a duplicate of command ${show(result.commandId)}`
      case 'rejected':
        return `a rejection: ${show(result.rejection)}`
    }
  }
}

// The events decoded, or as stored when they don't decode, so a failure message never throws.
function showEvents<D extends EventDefinitions>(events: D, stored: SerializedEvent[]): string {
  try {
    return show(stored.map((event) => replayEvent(events, event)))
  } catch {
    return `${show(stored.map((event) => `${event.type} v${event.version}`))} (undecodable)`
  }
}

function assertEqual(actual: unknown, expected: unknown, what: string): void {
  if (!deepEqual(actual, expected)) {
    throw new AssertionError(
      `${what}: expected ${show(expected)}, got ${show(actual)}`,
      actual,
      expected,
    )
  }
}
