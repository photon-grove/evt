// What a commit of one stream carries beyond its decided events, the same for a single command
// and for a transaction: the stored events, the snapshot the policy calls for, and the projectors'
// operations.

import {
  type Aggregate,
  type EventDefinitions,
  type EventOf,
  type Projection,
  encodeEventData,
} from './aggregate.js'
import {EvtError} from './errors.js'
import {type SerializedEvent, eventId} from './event.js'
import type {Metadata} from './metadata.js'
import type {NewSnapshot, ProjectorGroup} from './repository.js'
import {MAX_COMMIT_ITEM_BYTES, snapshotCommitSize, snapshotEventsDue} from './snapshots.js'

const INT64_MAX = 2n ** 63n - 1n

/**
 * Serializes decided events continuing the stream after `after`, each with its own copy of
 * `metadata`, and then folds them into `state`. Every event is encoded before any is applied, so
 * when one fails to encode, `state` is untouched, even for an aggregate whose `apply` changes the
 * state it's given.
 */
export function serializeEvents<S, C, D extends EventDefinitions>(
  aggregate: Aggregate<S, C, D>,
  entityId: string,
  after: bigint,
  state: S,
  decided: readonly EventOf<D>[],
  metadata: Metadata,
): {events: SerializedEvent[]; state: S} {
  let sequence = after
  const events: SerializedEvent[] = []

  for (const event of decided) {
    if (sequence >= INT64_MAX) {
      throw new EvtError(
        'invalidCommit',
        `invalid commit: entity ${entityId} is at the maximum event sequence`,
      )
    }
    sequence++

    const {version, payload} = encodeEventData(aggregate.events, event)
    events.push({
      id: eventId(entityId, sequence),
      sequence,
      type: event.type,
      version,
      entityId,
      entityType: aggregate.type,
      payload,
      metadata: copyMetadata(metadata),
    })
  }

  for (const event of decided) {
    state = aggregate.apply(state, event)
  }

  return {events, state}
}

/**
 * The snapshot a commit of `appended` events after `loadedSequence` carries, or null when the
 * interval calls for none. A snapshot whose item is over the per-item commit budget is left out
 * too, since it would fail every commit that carries it.
 */
export function commitSnapshot<S, C, D extends EventDefinitions>(
  aggregate: Aggregate<S, C, D>,
  entityId: string,
  interval: number,
  loaded: {readonly sequence: bigint; readonly snapshotSequence: bigint},
  appended: number,
  state: S,
): NewSnapshot | null {
  if (snapshotEventsDue(loaded.sequence, appended, interval) === 0) {
    return null
  }

  const snapshot: NewSnapshot = {
    entityType: aggregate.type,
    sequence: loaded.snapshotSequence + 1n,
    payload: aggregate.state.encode(state),
  }
  const eventSequence = loaded.sequence + BigInt(appended)
  const size = snapshotCommitSize({...snapshot, entityId, eventSequence})

  return size > MAX_COMMIT_ITEM_BYTES ? null : snapshot
}

/** The operations the aggregate's projectors add to a commit, in declaration order. */
export function projectCommit<S, C, D extends EventDefinitions>(
  aggregate: Aggregate<S, C, D>,
  entityId: string,
  state: S,
  events: readonly SerializedEvent[],
  snapshot: NewSnapshot | null,
): ProjectorGroup[] {
  const projectors = aggregate.projectors ?? []
  if (projectors.length === 0) {
    return []
  }

  // Each projector gets its own frozen copy of the events, their metadata and every byte array,
  // so none can change what another one sees, or what the commit writes.
  const encodedState = snapshot?.payload ?? aggregate.state.encode(state)
  const detached = (): Projection<S> =>
    Object.freeze({
      entityType: aggregate.type,
      entityId,
      state,
      encodedState: encodedState.slice(),
      events: Object.freeze(
        events.map((event) =>
          Object.freeze({
            ...event,
            payload: event.payload && event.payload.slice(),
            metadata: Object.freeze(copyMetadata(event.metadata)),
          }),
        ),
      ),
    })

  return projectors.flatMap((projector) => projector.project(detached()) ?? [])
}

/** A copy of the metadata, so changing one event's changes no other. */
export function copyMetadata(metadata: Metadata): Metadata {
  return {
    ...metadata,
    trace: metadata.trace && new Map(metadata.trace),
    origin: metadata.origin && {...metadata.origin},
    extensions: new Map(metadata.extensions),
  }
}
