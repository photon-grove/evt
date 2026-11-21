// Event-log enumeration (ADR 0004, C8): walking the whole log in batches, each entity's head, and
// folding the log into aggregates, for rebuilds and diagnostics. Engine rows in the reserved
// namespaces (`SYSTEM#`, `command-outcome#`) are never yielded, and snapshots and head items are
// not events.

import {type Aggregate, type EventDefinitions, replayEvent} from './aggregate.js'
import {SequenceGapError} from './errors.js'
import type {SerializedEvent} from './event.js'
import type {CallOptions, Repository} from './repository.js'
import type {Loaded} from './store.js'

/**
 * Narrows an enumeration to one entity type. Without `entityType` it matches every entity; an
 * empty `entityType` matches only entities stored with an empty type, as in Rust.
 */
export interface StreamFilter {
  readonly entityType?: string | undefined
}

/** Whether an entity of `entityType` passes the filter. */
export function matchesFilter(filter: StreamFilter | undefined, entityType: string): boolean {
  return filter?.entityType === undefined || filter.entityType === entityType
}

/**
 * A repository whose whole log can be enumerated, as Go's `StreamAllEvents` and
 * `EntityHeadStreamer` and Rust's `EventLog` and `Repository::entity_heads` do. Every repository
 * this package ships is one. Both calls read the whole log on some backends (a DynamoDB Scan), so
 * use them for rebuilds and diagnostics, not request paths.
 */
export interface EventLog {
  /**
   * Whether {@link streamAllEvents} yields each entity's events whole, in one batch and in
   * ascending sequence order (memory, PostgreSQL). Otherwise (DynamoDB, a batch per Scan page)
   * an entity's events can be in any batch and any order.
   */
  readonly batchesByEntity: boolean

  /**
   * The event log in batches, never empty, skipping the reserved namespaces, snapshots and head
   * items. A backend failure rejects the iteration.
   */
  streamAllEvents(filter?: StreamFilter, options?: CallOptions): AsyncIterable<SerializedEvent[]>

  /**
   * Each entity's head: the larger of its highest event sequence and its snapshot's event
   * sequence, so a compacted stream still reports what its snapshot covers. Entities with no
   * event and no snapshot are left out, and so are the reserved namespaces. `entityType`
   * narrows it to one type.
   */
  entityHeads(entityType?: string, options?: CallOptions): Promise<Map<string, bigint>>
}

/** Whether a repository enumerates its log. */
export function isEventLog(repository: Repository): repository is Repository & EventLog {
  const log = repository as Partial<EventLog>

  return typeof log.streamAllEvents === 'function' && typeof log.entityHeads === 'function'
}

/**
 * One entity of an enumeration: loaded, or the error that failed it (an unknown event type, a
 * failed upcast, a sequence gap), after which the enumeration goes on with the next entity.
 */
export type EnumeratedEntity<S> =
  | {readonly ok: true; readonly entityId: string; readonly loaded: Loaded<S>}
  | {readonly ok: false; readonly entityId: string; readonly error: unknown}

/**
 * Folds the log's batches into entities, from the events alone as Rust's `stream_entities` does:
 * an entity whose early events were compacted fails with a {@link SequenceGapError}, where Go's
 * `StreamEntities` folds the events that are left. With batches by
 * entity it folds a batch at a time; otherwise it groups every event first, and yields entities in
 * the order the first of their events was seen.
 */
export async function* foldEntities<S, C, D extends EventDefinitions>(
  aggregate: Aggregate<S, C, D>,
  log: EventLog,
  filter: StreamFilter,
  options?: CallOptions,
): AsyncGenerator<EnumeratedEntity<S>> {
  const grouped = new Map<string, SerializedEvent[]>()
  for await (const batch of log.streamAllEvents(filter, options)) {
    for (const event of batch) {
      const events = grouped.get(event.entityId)
      if (events === undefined) {
        grouped.set(event.entityId, [event])
      } else {
        events.push(event)
      }
    }
    if (log.batchesByEntity) {
      yield* drain(aggregate, grouped)
    }
  }
  yield* drain(aggregate, grouped)
}

// Folds and yields every grouped entity, in first-seen order, and empties the groups.
function* drain<S, C, D extends EventDefinitions>(
  aggregate: Aggregate<S, C, D>,
  grouped: Map<string, SerializedEvent[]>,
): Generator<EnumeratedEntity<S>> {
  for (const [entityId, events] of grouped) {
    grouped.delete(entityId)
    let entity: EnumeratedEntity<S>
    try {
      entity = {ok: true, entityId, loaded: fold(aggregate, entityId, events)}
    } catch (error) {
      entity = {ok: false, entityId, error}
    }
    yield entity
  }
}

// An entity's state from its events, ascending by sequence, which must run from 1 without a gap.
function fold<S, C, D extends EventDefinitions>(
  aggregate: Aggregate<S, C, D>,
  entityId: string,
  events: SerializedEvent[],
): Loaded<S> {
  events.sort((a, b) => (a.sequence < b.sequence ? -1 : a.sequence > b.sequence ? 1 : 0))

  let state = aggregate.initial()
  let sequence = 0n
  const seenCommandIds = new Set<string>()
  for (const stored of events) {
    if (stored.sequence !== sequence + 1n) {
      throw new SequenceGapError(entityId, sequence, stored.sequence)
    }

    state = aggregate.apply(state, replayEvent(aggregate.events, stored))
    sequence = stored.sequence
    const commandId = stored.metadata.commandId
    if (commandId !== null && commandId !== '') {
      seenCommandIds.add(commandId)
    }
  }

  return {entityId, state, sequence, snapshotSequence: 0n, seenCommandIds}
}
