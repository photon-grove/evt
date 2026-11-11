// Aggregates: an entity's state folded from its events, the commands it decides on, and the typed
// events it records, each with its stored version, payload codec and upcasters.

import type {Codec} from './codec.js'
import {EvtError, ReplayError, messageOf} from './errors.js'
import type {SerializedEvent} from './event.js'
import {type JsonValue, parseJson, stringifyJson} from './json.js'
import type {ProjectorGroup} from './repository.js'
import {utf8Decode, utf8Encode} from './utf8.js'

/**
 * Migrates a stored payload of its event type before it is decoded, taking an event at the
 * upcaster's from version to its to version. An event type's upcasters run in declaration order,
 * each once, and each sees the output of the ones before it, so a 1 → 2 upcaster followed by a
 * 2 → 3 one takes a version 1 event to version 3. Stored events are never rewritten.
 */
export interface Upcaster {
  readonly from: number
  readonly to: number
  upcast(payload: Uint8Array): Uint8Array
}

/**
 * An upcaster that rewrites a JSON payload. `migrate` gets it losslessly, as {@link parseJson}
 * reads it (objects as `Map`s in their stored order, numbers as {@link JsonNumber}s), and returns
 * the new payload, which is written with {@link stringifyJson}. What the migration doesn't touch
 * is written back exactly, whatever its integers or keys.
 */
export function jsonUpcaster(
  from: number,
  to: number,
  migrate: (payload: JsonValue) => unknown,
): Upcaster {
  return {
    from,
    to,
    upcast(payload) {
      const migrated = migrate(parseJson(utf8Decode(payload, 'payload')))

      return utf8Encode(stringifyJson(migrated))
    },
  }
}

/** How one event type is stored: its current version, its payload codec, and its upcasters. */
export interface EventDefinition<T> {
  /** The version new events are written with, and the one stored events are upcast to. */
  readonly version: number
  readonly codec: Codec<T>
  readonly upcasters?: readonly Upcaster[] | undefined
}

// oxlint-disable-next-line typescript/no-explicit-any -- each entry has its own data type.
export type EventDefinitions = Readonly<Record<string, EventDefinition<any>>>

/** The events a set of definitions describes: `{type, data}` for each event type. */
export type EventOf<D extends EventDefinitions> = {
  [K in keyof D & string]: {
    type: K
    data: D[K] extends EventDefinition<infer T> ? T : never
  }
}[keyof D & string]

/** Declares an aggregate's event types, checking each version and upcaster. */
export function defineEvents<const D extends EventDefinitions>(definitions: D): D {
  for (const [type, definition] of Object.entries(definitions)) {
    positive(definition.version, `${type} version`)
    for (const upcaster of definition.upcasters ?? []) {
      positive(upcaster.from, `${type} upcaster from`)
      positive(upcaster.to, `${type} upcaster to`)
    }
  }

  return definitions
}

/** A domain refusal of a command: a code to branch on, a message, and optional JSON details. */
export interface Rejection {
  code: string
  message: string
  details?: unknown
}

/**
 * What a command handler decides: accept with events (possibly none), or reject.
 *
 * On a store with command outcomes, an accepted command's `response` is stored with its outcome,
 * so a retried command returns the original answer (at most 65536 bytes; empty stores none). A
 * rejection's `events` commit with the rejected outcome in the same transaction, so the history
 * shows why the command was refused. A store without outcomes stores neither: it writes nothing
 * for a rejection.
 */
export type Decision<E> =
  | {kind: 'accept'; events: readonly E[]; response?: Uint8Array | null | undefined}
  | {kind: 'reject'; rejection: Rejection; events?: readonly E[] | undefined}

/**
 * What a command is to its outcome's identity digest: its type string and its payload's canonical
 * JSON bytes, empty or absent when it has none. Two commands with the same command ID, type,
 * target, payload and principal are the same command, so everything that distinguishes a request
 * must be in the payload.
 */
export interface CommandDescription {
  readonly commandType: string
  readonly payload?: Uint8Array | null | undefined
}

/** An aggregate type, as {@link defineAggregate} takes it. */
export interface AggregateDefinition<S, C, D extends EventDefinitions> {
  /** The stored entity type, such as `account`. */
  readonly type: string
  readonly events: D
  /** The snapshot codec: how the state is stored. */
  readonly state: Codec<S>
  /** The state of an entity with no events. */
  initial(): S
  /** Decides what a command does. It must not change `state`. */
  handle(state: S, command: C): Decision<EventOf<D>> | Promise<Decision<EventOf<D>>>
  /** Folds one event into the state, returning the next state. It must not fail. */
  apply(state: S, event: EventOf<D>): S
  /**
   * This type's own number of events between snapshots, overriding the store's. 0 disables its
   * snapshots.
   */
  readonly snapshotInterval?: number | undefined
  /**
   * The projectors whose operations every commit of this type writes in the same transaction as
   * its events, in this order, such as the DynamoDB backend's `viewProjector`.
   */
  readonly projectors?: readonly Projector<S>[] | undefined
  /**
   * Describes a command to the identity digest of a store with command outcomes. A store with
   * outcomes needs it to run this type's commands; the entity ID is the command's one target.
   */
  identify?(command: C): CommandDescription
}

/** What a projector sees of a commit: its entity, the state after its events, and the events. */
export interface Projection<S> {
  readonly entityType: string
  readonly entityId: string
  /** The state after the commit's events. */
  readonly state: S
  /** The state as the aggregate's state codec encodes it, as a snapshot stores it. */
  readonly encodedState: Uint8Array
  /** Copies of the commit's events, as they are stored. */
  readonly events: readonly Readonly<SerializedEvent>[]
}

/**
 * Builds the operations a commit writes beside its events, in the same transaction, such as a
 * view of the entity's state. Its group names the backend that writes it. `project` returns null
 * when the commit needs none. It runs only for a commit with events, again on every retry, and it
 * must not fail or change the state.
 */
export interface Projector<S> {
  project(projection: Projection<S>): ProjectorGroup | null
}

/** A checked aggregate type. */
export type Aggregate<S, C, D extends EventDefinitions> = Readonly<AggregateDefinition<S, C, D>>

/** Declares an aggregate type. */
export function defineAggregate<S, C, D extends EventDefinitions>(
  definition: AggregateDefinition<S, C, D>,
): Aggregate<S, C, D> {
  if (definition.type === '') {
    throw new EvtError('invalidArgument', 'an aggregate type must have a name')
  }
  if (definition.snapshotInterval !== undefined) {
    const interval = definition.snapshotInterval
    if (!Number.isSafeInteger(interval) || interval < 0) {
      throw new EvtError(
        'invalidArgument',
        `${definition.type} snapshot interval must be a whole number of events: ${interval}`,
      )
    }
  }

  return definition
}

/** Encodes one event's data with its type's codec, as the payload and version to store. */
export function encodeEventData<D extends EventDefinitions>(
  events: D,
  event: EventOf<D>,
): {version: number; payload: Uint8Array} {
  const definition = definitionOf(events, event.type)
  if (definition === undefined) {
    throw new EvtError('invalidArgument', `${event.type} is not one of the aggregate's event types`)
  }

  return {version: definition.version, payload: definition.codec.encode(event.data)}
}

/** Upcasts and decodes a stored event, failing closed with a {@link ReplayError}. */
export function replayEvent<D extends EventDefinitions>(
  events: D,
  stored: SerializedEvent,
): EventOf<D> {
  const definition = definitionOf(events, stored.type)
  if (definition === undefined) {
    throw new ReplayError(stored.type, stored.version, 'decode', 'unknown event type')
  }

  let payload = stored.payload ?? new Uint8Array()
  let version = stored.version
  for (const upcaster of definition.upcasters ?? []) {
    if (upcaster.from !== version) {
      continue
    }

    try {
      payload = upcaster.upcast(payload)
    } catch (error) {
      throw new ReplayError(stored.type, version, 'upcast', messageOf(error), error)
    }
    version = upcaster.to
  }

  if (version !== definition.version) {
    throw new ReplayError(
      stored.type,
      version,
      'decode',
      `no upcaster takes it to version ${definition.version}`,
    )
  }

  try {
    return {type: stored.type, data: definition.codec.decode(payload)} as EventOf<D>
  } catch (error) {
    throw new ReplayError(stored.type, version, 'decode', messageOf(error), error)
  }
}

function definitionOf<D extends EventDefinitions>(
  events: D,
  type: string,
): EventDefinition<unknown> | undefined {
  return Object.hasOwn(events, type) ? events[type] : undefined
}

function positive(value: number, what: string): void {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new EvtError('invalidArgument', `${what} must be a whole number of at least 1: ${value}`)
  }
}
