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
import {type CommitProjector, type ProjectedView, commitStream, projectViews} from './projection.js'
import type {CallOptions, NewSnapshot, ProjectorGroup, Repository} from './repository.js'
import {compareUtf8} from './utf8.js'
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
      encodedState: new Uint8Array(encodedState),
      events: Object.freeze(
        events.map((event) =>
          Object.freeze({
            ...event,
            payload: event.payload && new Uint8Array(event.payload),
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

/** A stream a commit writes, as the commit-wide projectors read it. */
export interface WrittenStream {
  readonly entityId: string
  readonly entityType: string
  readonly events: readonly SerializedEvent[]
  /**
   * The state after the commit, as its snapshot would carry it. Encoded only when a projector
   * runs, so a store without projectors never encodes a state it doesn't snapshot.
   */
  state(): Uint8Array
}

/**
 * The commit-wide projectors' view operations for a commit that writes `streams`: none when there
 * are no projectors or no events. The repository must write views, and declare a history-dependent
 * projector's entity types, which {@link declareHistory} does before the commit. The projectors
 * read copies, sorted by entity ID bytes.
 */
export function projectStreams(
  projectors: readonly CommitProjector[],
  repository: Repository,
  streams: readonly WrittenStream[],
): ProjectedView[] {
  const written = streams.filter((stream) => stream.events.length > 0)
  if (projectors.length === 0 || written.length === 0) {
    return []
  }
  readyProjectors(projectors, repository)

  const sorted = [...written]
  sorted.sort((a, b) => compareUtf8(a.entityId, b.entityId))

  return projectViews(projectors, {
    commitId: sorted[0]!.events[0]!.metadata.commitId,
    streams: sorted.map((stream) =>
      commitStream(
        stream.entityId,
        stream.entityType,
        stream.events.map((event) =>
          Object.assign({}, event, {
            payload: event.payload && new Uint8Array(event.payload),
            metadata: copyMetadata(event.metadata),
          }),
        ),
        new Uint8Array(stream.state()),
      ),
    ),
  })
}

/**
 * Refuses commit-wide projectors the repository can't serve: it must write view operations, or
 * their rows would be dropped, and a history-dependent projector needs a repository that declares
 * its entity types in the store's feature manifest.
 */
export function readyProjectors(
  projectors: readonly CommitProjector[],
  repository: Repository,
): void {
  if (projectors.length === 0) {
    return
  }

  const name = repository.constructor.name || 'repository'
  if (repository.writesViews?.() !== true) {
    throw new EvtError(
      'viewsUnsupported',
      `repository ${name} does not write view operations; refusing a commit-wide projector`,
    )
  }
  const historyDependent = projectors.find((projector) => (projector.history ?? []).length > 0)
  if (historyDependent !== undefined && repository.declareHistoryDependent === undefined) {
    throw new EvtError(
      'historyDeclarationUnsupported',
      `repository ${name} cannot declare history-dependent entity types; refusing a ` +
        `history-dependent projector (${JSON.stringify(historyDependent.name)})`,
    )
  }
}

/**
 * What a store has declared of its projectors' history: the entity types declared, and the
 * declaration in flight, which concurrent commits wait for instead of sending their own.
 */
export interface HistoryLedger {
  readonly declared: Set<string>
  inflight: Promise<void> | null
}

/**
 * Durably declares the entity types the projectors read events of history-dependent, before a
 * commit that runs them: each type once per store, which the ledger remembers, so compaction
 * refuses them in every process from then on. A failed declaration marks nothing, so the next
 * commit declares again.
 */
export async function declareHistory(
  projectors: readonly CommitProjector[],
  repository: Repository,
  ledger: HistoryLedger,
  options?: CallOptions,
): Promise<void> {
  const pending: string[] = []
  for (const projector of projectors) {
    for (const entityType of projector.history ?? []) {
      if (!ledger.declared.has(entityType) && !pending.includes(entityType)) {
        pending.push(entityType)
      }
    }
  }
  if (pending.length === 0) {
    return
  }

  // Another commit's declaration: wait for it, then see what's left, whatever its result.
  const inflight = ledger.inflight
  if (inflight !== null) {
    await inflight.catch(() => undefined)

    return declareHistory(projectors, repository, ledger, options)
  }

  readyProjectors(projectors, repository)
  const run = repository.declareHistoryDependent!(pending, options).then(() => {
    for (const entityType of pending) {
      ledger.declared.add(entityType)
    }
  })
  ledger.inflight = run
  try {
    await run
  } finally {
    ledger.inflight = null
  }
}
