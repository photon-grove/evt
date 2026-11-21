// Commit-wide projectors (ADR 0004, C7): pure functions over everything one commit wrote, whose
// view operations commit atomically with it. This module holds the backend-neutral operations, the
// commit view a projector reads, running the projectors, the checks every repository applies to
// their operations, and regrouping stored events into the commits that wrote them. The backends
// write the operations: DynamoDB to its views table, PostgreSQL through a view writer.

import {
  CommitOrderError,
  DuplicateViewKeyError,
  EvtError,
  ProjectorError,
  ReservedViewKeyError,
  ViewKeyTooLongError,
} from './errors.js'
import type {SerializedEvent} from './event.js'
import {compareUtf8, utf8Length} from './utf8.js'

/**
 * The view partition-key prefixes the engine keeps for its own rows (the journal's). An
 * application projector's operation whose `pk` starts with one is refused.
 */
export const RESERVED_VIEW_PREFIXES: readonly string[] = Object.freeze([
  'journal#',
  'journal-counter#',
  'journal-compaction#',
])

/** The longest view partition key, in UTF-8 bytes (DynamoDB's own limit). */
export const MAX_VIEW_PK_BYTES = 2048

/** The longest view sort key, in UTF-8 bytes (DynamoDB's own limit). */
export const MAX_VIEW_SK_BYTES = 1024

/**
 * The reason of a view operation whose condition didn't hold, on every backend: DynamoDB's
 * cancellation reason code, and the value PostgreSQL and the in-memory repository report.
 */
export const REASON_CONDITION_FAILED = 'ConditionalCheckFailed'

/**
 * A condition on a view operation. `absent` (no row may exist at the key) is valid only on a put,
 * and `present` (a row must exist) only on a delete. A failed condition fails the commit with a
 * `ProjectorOperationError`.
 */
export type ViewCondition = 'none' | 'absent' | 'present'

/**
 * One backend-neutral view write, evaluated and written by the repository in the commit's
 * transaction. Build one with {@link putView} or {@link deleteView}.
 */
export interface ViewOperation {
  readonly kind: 'put' | 'delete'
  /** The row's partition key, chosen by the projector. */
  readonly pk: string
  /** The row's sort key, chosen by the projector. */
  readonly sk: string
  /**
   * The stream the row belongs to, a stream the commit wrote, stored as the row's `entityID`.
   * Every write to a row is conditioned on its owner, so a stream never overwrites or deletes
   * another stream's row. A row's owner must never change: to hand a key to another stream, keep
   * the row and rewrite it, as its owner, with a tombstone payload.
   */
  readonly owner: string
  /** Informational, and always written by a put. Empty for a delete. */
  readonly entityType: string
  /** The view state JSON of a put, valid UTF-8. Empty for a delete. */
  readonly payload: Uint8Array
  readonly condition: ViewCondition
  /**
   * The projector's own error for a failed condition, which the commit's
   * `ProjectorOperationError` reports as its `mapped` error. Never stored.
   */
  readonly conflict?: unknown
  /**
   * An engine-only condition on a put (the journal's counter, ADR 0005): the row must exist, be
   * the owner's, and hold exactly these payload bytes, valid UTF-8. Only a put with no other
   * condition may carry it, and a commit-wide projector's operation that sets it is refused, so
   * only the engine writes it. A repository that writes the journal applies it in the commit
   * (DynamoDB: `entityID = :owner AND payload = :expected`) and says so with
   * `Repository.appliesExpectedPayload`. Like every put's condition, it isn't budgeted.
   */
  readonly expectedPayload?: Uint8Array | null | undefined
}

const encoder = new TextEncoder()
const decoder = new TextDecoder('utf-8', {fatal: true, ignoreBOM: true})

/** An operation that writes a row at `pk` and `sk`, owned by `owner`. */
export function putView(
  pk: string,
  sk: string,
  owner: string,
  entityType: string,
  payload: Uint8Array | string,
  options: {condition?: 'none' | 'absent'; conflict?: unknown} = {},
): ViewOperation {
  return {
    kind: 'put',
    pk,
    sk,
    owner,
    entityType,
    payload: typeof payload === 'string' ? encoder.encode(payload) : payload,
    condition: options.condition ?? 'none',
    ...('conflict' in options && {conflict: options.conflict}),
  }
}

/**
 * The engine's compare-and-set put: `op` with the condition that the stored row is the owner's
 * and holds exactly `payload`. Internal: a projector cannot set it.
 */
export function expectPayload(op: ViewOperation, payload: Uint8Array): ViewOperation {
  return {...op, expectedPayload: payload}
}

/** An operation that deletes the row at `pk` and `sk`, which only its `owner` may delete. */
export function deleteView(
  pk: string,
  sk: string,
  owner: string,
  options: {condition?: 'none' | 'present'; conflict?: unknown} = {},
): ViewOperation {
  return {
    kind: 'delete',
    pk,
    sk,
    owner,
    entityType: '',
    payload: new Uint8Array(),
    condition: options.condition ?? 'none',
    ...('conflict' in options && {conflict: options.conflict}),
  }
}

/** One stream a commit wrote, as a commit-wide projector sees it. */
export interface CommitStream {
  readonly entityId: string
  /** The stream's aggregate type. */
  readonly entityType: string
  /** This commit's events for the stream, ascending by sequence, as stored. */
  readonly events: readonly SerializedEvent[]
  /**
   * The stream's state after the commit: the JSON its snapshot would carry. Empty when there is
   * none to give, as {@link groupCommits} leaves it.
   */
  readonly state: Uint8Array
}

/**
 * Everything one commit wrote, a projector's input: the commit ID every event carries (empty for
 * an event written before commit IDs), and the written streams, ascending by entity ID bytes.
 */
export interface CommitView {
  readonly commitId: string
  readonly streams: readonly CommitStream[]
}

/**
 * A pure function over every event of a commit, whose view operations commit atomically with it.
 * It runs once per commit that writes at least one event, and again on every retry, so it must be
 * deterministic: its operations may depend only on the commit view, never on a clock, a random
 * source or remembered state. Its operations are written in the projectors' declaration order,
 * each projector's in the order it returns them.
 *
 * A current-state projector (no `history`) computes its rows from each stream's state after the
 * commit: it's given an empty commit ID and no events. A history-dependent projector also reads
 * the commit ID and the events of the entity types it names, and nothing of any other stream.
 * Every operation must be owned by a stream the commit wrote, and a history-dependent projector's
 * by a stream of a type it names.
 */
export interface CommitProjector {
  /** Identifies the projector in errors. */
  readonly name: string
  /** The entity types whose events the projector reads; none for a current-state projector. */
  readonly history?: readonly string[] | undefined
  /** The commit's view operations. A thrown error fails the commit before any write. */
  project(commit: CommitView): readonly ViewOperation[]
}

/** A view operation with which projector emitted it, at which position, as which operation. */
export interface ProjectedView {
  readonly projector: string
  /** The projector's position in declaration order. */
  readonly projectorIndex: number
  /** The operation's index among those the projector emitted. */
  readonly operation: number
  readonly op: ViewOperation
}

/** The number of events across a commit view's streams. */
export function eventCount(view: CommitView): number {
  return view.streams.reduce((sum, stream) => sum + stream.events.length, 0)
}

/**
 * A commit view's stream. Each event's metadata gets its effective causation ID (the command ID
 * when none was set), the value a stored event carries, so a projector reads the same metadata
 * live and on replay.
 */
export function commitStream(
  entityId: string,
  entityType: string,
  events: readonly SerializedEvent[],
  state: Uint8Array,
): CommitStream {
  return {
    entityId,
    entityType,
    events: events.map((event) =>
      event.metadata.causationId === null
        ? {...event, metadata: {...event.metadata, causationId: event.metadata.commandId}}
        : event,
    ),
    state,
  }
}

/**
 * Runs the projectors over a commit view, in declaration order, and returns their operations with
 * provenance in the order the commit writes them. A view with no events runs no projector. A
 * projector's thrown error is a {@link ProjectorError}; each operation must then be owned by a
 * written stream, outside the engine's reserved prefixes, and pass {@link checkViews}.
 */
export function projectViews(
  projectors: readonly CommitProjector[],
  view: CommitView,
): ProjectedView[] {
  if (projectors.length === 0 || eventCount(view) === 0) {
    return []
  }

  const projected: ProjectedView[] = []
  projectors.forEach((projector, projectorIndex) => {
    const history = projector.history ?? []
    let operations: readonly ViewOperation[]
    try {
      operations = projector.project(visibleTo(view, history))
    } catch (error) {
      throw new ProjectorError(projector.name, error)
    }

    operations.forEach((op, operation) => {
      const item: ProjectedView = {projector: projector.name, projectorIndex, operation, op}
      if (op.expectedPayload !== null && op.expectedPayload !== undefined) {
        throw invalidView(item, "an expected payload is the engine's alone")
      }
      checkOwner(item, view, history)
      checkUnreserved(item)
      projected.push(item)
    })
  })

  checkViews(projected)

  return projected
}

/**
 * Validates a commit's view operations before any write: each, in order, must be valid (non-empty
 * keys and owner, text without U+0000, a UTF-8 payload, a condition its kind allows), then within
 * the key limits, then not repeat an earlier operation's key.
 */
export function checkViews(views: readonly ProjectedView[]): void {
  const seen = new Map<string, ProjectedView>()

  for (const view of views) {
    const {op} = view
    const reason = invalidReason(op)
    if (reason !== null) {
      throw invalidView(view, reason)
    }

    for (const [field, value, max] of [
      ['pk', op.pk, MAX_VIEW_PK_BYTES],
      ['sk', op.sk, MAX_VIEW_SK_BYTES],
    ] as const) {
      const length = utf8Length(value)
      if (length > max) {
        throw new ViewKeyTooLongError(field, length, max, view.projector, view.operation)
      }
    }

    // A key no projector can make, since the separator is refused in both parts.
    const key = `${op.pk}\0${op.sk}`
    const first = seen.get(key)
    if (first !== undefined) {
      throw new DuplicateViewKeyError(op.pk, op.sk, first, view)
    }
    seen.set(key, view)
  }
}

function invalidReason(op: ViewOperation): string | null {
  if (op.kind === 'put' && op.condition === 'present') {
    return 'a put cannot carry the "present" condition'
  }
  if (op.kind === 'delete' && op.condition === 'absent') {
    return 'a delete cannot carry the "absent" condition'
  }
  const expected = op.expectedPayload
  if (expected !== null && expected !== undefined) {
    if (op.kind !== 'put' || op.condition !== 'none') {
      return 'an expected payload is valid only on a put with no other condition'
    }
    if (!isUtf8(expected)) {
      return 'an expected view payload must be UTF-8'
    }
  }
  if (op.pk === '') {
    return 'pk is empty'
  }
  if (op.sk === '') {
    return 'sk is empty'
  }
  if (op.owner === '') {
    return 'owner is empty'
  }

  for (const [name, value] of [
    ['pk', op.pk],
    ['sk', op.sk],
    ['owner', op.owner],
  ] as const) {
    if (value.includes('\0')) {
      return `${name} contains U+0000`
    }
  }

  if (op.kind === 'put') {
    if (op.entityType.includes('\0')) {
      return 'entityType contains U+0000'
    }
    let payload: string
    try {
      payload = decoder.decode(op.payload)
    } catch {
      return 'payload is not valid UTF-8'
    }
    if (payload.includes('\0')) {
      return 'payload contains U+0000'
    }
  }

  return null
}

function isUtf8(bytes: Uint8Array): boolean {
  try {
    decoder.decode(bytes)
    return true
  } catch {
    return false
  }
}

function invalidView(view: ProjectedView, reason: string): EvtError {
  return new EvtError(
    'invalidViewOperation',
    `invalid view operation: projector ${JSON.stringify(view.projector)} ` +
      `(#${view.projectorIndex}) operation ${view.operation}: ${reason}`,
  )
}

function checkOwner(view: ProjectedView, commit: CommitView, history: readonly string[]): void {
  const owner = view.op.owner
  const stream = commit.streams.find((each) => each.entityId === owner)
  if (stream === undefined) {
    throw invalidView(
      view,
      `the owner stream ${JSON.stringify(owner)} was not written by this commit`,
    )
  }
  if (history.length > 0 && !history.includes(stream.entityType)) {
    throw invalidView(
      view,
      `the owner stream ${JSON.stringify(owner)} is of type ${JSON.stringify(stream.entityType)}, ` +
        'which the projector does not declare in its history',
    )
  }
}

function checkUnreserved(view: ProjectedView): void {
  if (RESERVED_VIEW_PREFIXES.some((prefix) => view.op.pk.startsWith(prefix))) {
    throw new ReservedViewKeyError(view.op.pk, view.projector, view.operation)
  }
}

// The view a projector is given: a current-state projector gets an empty commit ID and no events,
// and the state of every stream; a history-dependent one gets the commit ID, and the events and
// state of the streams of the types it names only.
// Each projector gets its own copies, so none can change what another one sees. A copy is a new
// array, never a slice, which a Node Buffer shares with its source.
function visibleTo(view: CommitView, history: readonly string[]): CommitView {
  if (history.length === 0) {
    return {
      commitId: '',
      streams: view.streams.map((stream) => ({
        ...stream,
        events: [],
        state: new Uint8Array(stream.state),
      })),
    }
  }

  return {
    commitId: view.commitId,
    streams: view.streams.map((stream) =>
      history.includes(stream.entityType)
        ? {...stream, events: stream.events.map(copyEvent), state: new Uint8Array(stream.state)}
        : {...stream, events: [], state: new Uint8Array()},
    ),
  }
}

function copyEvent(event: SerializedEvent): SerializedEvent {
  return {
    ...event,
    payload: event.payload && new Uint8Array(event.payload),
    metadata: {
      ...event.metadata,
      trace: event.metadata.trace && new Map(event.metadata.trace),
      origin: event.metadata.origin && {...event.metadata.origin},
      extensions: new Map(event.metadata.extensions),
    },
  }
}

/**
 * Regroups stored events into the commits that wrote them, as a rebuild replays a
 * history-dependent projector: events that share a commit ID form one view, its streams ascending
 * by entity ID bytes and each stream's events by sequence, and an event with no commit ID is a
 * commit of its own. Each stream's `state` is left empty.
 *
 * The views come in an order every stream agrees with: a commit that wrote a stream's sequence 3
 * precedes the one that wrote its sequence 4. Commits no stream orders relative to each other come
 * by their first event's entity ID bytes, then sequence. Throws a {@link CommitOrderError} when no
 * such order exists.
 */
export function groupCommits(events: Iterable<SerializedEvent>): CommitView[] {
  const sorted = [...events]
  sorted.sort(compareEvents)

  const groups: SerializedEvent[][] = []
  const byCommit = new Map<string, number>()
  const members: number[] = []
  for (const event of sorted) {
    const commitId = event.metadata.commitId
    if (commitId === '') {
      members.push(groups.length)
      groups.push([event])
      continue
    }

    let index = byCommit.get(commitId)
    if (index === undefined) {
      index = groups.length
      byCommit.set(commitId, index)
      groups.push([])
    }
    members.push(index)
    groups[index]!.push(event)
  }

  // Each stream orders the commits that wrote it: an edge from one commit to the next.
  const successors: number[][] = groups.map(() => [])
  const waiting: number[] = groups.map(() => 0)
  for (let i = 1; i < sorted.length; i++) {
    const [previous, current] = [members[i - 1]!, members[i]!]
    if (sorted[i - 1]!.entityId !== sorted[i]!.entityId || previous === current) {
      continue
    }
    successors[previous]!.push(current)
    waiting[current]!++
  }

  // A commit's place among the ready ones: its first event's (entity ID bytes, sequence).
  const before = (a: number, b: number): number => compareEvents(groups[a]![0]!, groups[b]![0]!)
  const ready = groups.flatMap((_, group) => (waiting[group] === 0 ? [group] : []))
  const order: number[] = []
  while (ready.length > 0) {
    ready.sort(before)
    const group = ready.shift()!
    order.push(group)
    for (const successor of successors[group]!) {
      waiting[successor]!--
      if (waiting[successor] === 0) {
        ready.push(successor)
      }
    }
  }

  if (order.length < groups.length) {
    const commits = groups.flatMap((group, index) =>
      waiting[index]! > 0 ? [group[0]!.metadata.commitId] : [],
    )
    commits.sort(compareUtf8)
    throw new CommitOrderError(commits)
  }

  return order.map((group) => commitViewOf(groups[group]!))
}

// The view of one commit's events, already in (entity ID bytes, sequence) order.
function commitViewOf(events: readonly SerializedEvent[]): CommitView {
  const streams: {entityId: string; entityType: string; events: SerializedEvent[]}[] = []
  for (const event of events) {
    const last = streams.at(-1)
    if (last !== undefined && last.entityId === event.entityId) {
      last.events.push(event)
    } else {
      streams.push({entityId: event.entityId, entityType: event.entityType, events: [event]})
    }
  }

  return {
    commitId: events[0]?.metadata.commitId ?? '',
    streams: streams.map((stream) =>
      commitStream(stream.entityId, stream.entityType, stream.events, new Uint8Array()),
    ),
  }
}

function compareEvents(a: SerializedEvent, b: SerializedEvent): number {
  const byId = compareUtf8(a.entityId, b.entityId)
  if (byId !== 0) {
    return byId
  }

  return a.sequence < b.sequence ? -1 : a.sequence > b.sequence ? 1 : 0
}

// The condition expressions of a DynamoDB view delete, whose lengths its budgeted size counts.
const DELETE_PRESENT = 'attribute_exists(pk) AND entityID = :owner'
const DELETE_OWNED = 'attribute_not_exists(pk) OR entityID = :owner'

/**
 * A view operation's size in the commit budget, as DynamoDB counts its action: a put counts its
 * five attributes' names and values, whatever its condition, and a delete its two key attributes,
 * its condition expression and the owner value it binds. Every backend budgets views by it.
 */
export function viewOperationSize(op: ViewOperation): number {
  const keys = 2 + utf8Length(op.pk) + 2 + utf8Length(op.sk)
  if (op.kind === 'delete') {
    const condition = op.condition === 'present' ? DELETE_PRESENT : DELETE_OWNED

    return keys + condition.length + ':owner'.length + utf8Length(op.owner)
  }

  return (
    keys +
    'entityID'.length +
    utf8Length(op.owner) +
    'entityType'.length +
    utf8Length(op.entityType) +
    'payload'.length +
    op.payload.length
  )
}
