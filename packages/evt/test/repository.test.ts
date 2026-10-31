import {describe, expect, test} from 'vitest'

import {headActionSize, headUpdate} from '../src/dynamodb/head.ts'
import {
  CommitTooLargeError,
  ConflictError,
  EvtError,
  SequenceGapError,
  StreamHeadDriftError,
  isConflict,
  isRetryable,
} from '../src/errors.ts'
import {type SerializedEvent, type SerializedSnapshot, eventId} from '../src/event.ts'
import {emptyMetadata} from '../src/metadata.ts'
import {
  type Commit,
  type NewSnapshot,
  checkBudget,
  checkNotReserved,
  commitSizes,
  validateCommit,
} from '../src/repository.ts'
import {MAX_COMMIT_ITEM_BYTES, snapshotCommitSize} from '../src/snapshots.ts'

const encoder = new TextEncoder()

function event(entityId: string, sequence: number): SerializedEvent {
  return {
    id: eventId(entityId, BigInt(sequence)),
    sequence: BigInt(sequence),
    type: 'ledger:added',
    version: 1,
    entityId,
    entityType: 't',
    payload: encoder.encode('{}'),
    metadata: emptyMetadata(),
  }
}

function events(entityId: string, from: number, through: number): SerializedEvent[] {
  return Array.from({length: through - from + 1}, (_, i) => event(entityId, from + i))
}

function failure(run: () => unknown): unknown {
  try {
    run()
  } catch (error) {
    return error
  }

  return undefined
}

const none = () => undefined

function newSnapshot(entityType: string, sequence: bigint): NewSnapshot {
  return {entityType, sequence, payload: new Uint8Array()}
}

describe('the commit budget', () => {
  test('counts the control checks, the head update and the event puts', () => {
    // The marker check, the rebuild-lock check, the head update and 97 events fill the 100
    // actions, whether the commit creates the stream or appends to it.
    for (const from of [1, 2]) {
      const fits: Commit = {entityId: 'a', events: events('a', from, from + 96)}
      expect(commitSizes(fits, none)).toHaveLength(100)
      checkBudget(commitSizes(fits, none))

      const over: Commit = {entityId: 'a', events: events('a', from, from + 97)}
      const error = failure(() => checkBudget(commitSizes(over, none)))
      expect(error).toBeInstanceOf(CommitTooLargeError)
      expect((error as CommitTooLargeError).actions).toBe(101)
    }

    // A snapshot put is one more action, so 96 events fill the budget with one.
    const snapshot = {entityType: 't', sequence: 1n, payload: encoder.encode('{}')}
    const snapshotted: Commit = {entityId: 'a', events: events('a', 1, 96), snapshot}
    expect(commitSizes(snapshotted, none)).toHaveLength(100)
  })

  test('sizes a snapshot as the preflight does, short of the widest ttl', () => {
    const snapshot = {entityType: 't', sequence: 1n, payload: encoder.encode('{}')}
    const sizes = commitSizes({entityId: 'a', events: [event('a', 1)], snapshot}, none)
    const stored: SerializedSnapshot = {
      entityType: 't',
      entityId: 'a',
      sequence: 1n,
      eventSequence: 1n,
      payload: encoder.encode('{}'),
    }

    const widestTtl = 'ttl'.length + '9223372036854775807'.length
    expect(snapshotCommitSize(stored)).toBe(sizes[4]! + widestTtl)
  })

  test('sizes the control checks by their keys', () => {
    const [marker, lock] = commitSizes({entityId: 'a', events: []}, none)

    expect(marker).toBe('pk'.length + 'SYSTEM#FEATURES'.length + 'sk'.length + 1)
    expect(lock).toBe('pk'.length + 'SYSTEM#REBUILD'.length + 'sk'.length + 1)
  })

  test('sizes the head update by its expressions', () => {
    // pk + "acct" + sk + "-1"
    const key = 2 + 4 + 2 + 2

    // A create: "SET head = :last" + "attribute_not_exists(head)" + ":last" + "2"
    expect(headActionSize(headUpdate('acct', 1n, 2n))).toBe(
      key + 'SET head = :last'.length + 'attribute_not_exists(head)'.length + ':last'.length + 1,
    )

    // An append adds `:prev` and its condition.
    const append = headActionSize(headUpdate('acct', 8n, 9n))
    expect(append).toBe(
      key + 'SET head = :last'.length + 'head = :prev'.length + ':last'.length + 1 + 6,
    )

    // A retained append adds the `ttl` name placeholder and `:t`.
    const retained = headActionSize(headUpdate('acct', 8n, 9n, 1_760_086_400))
    expect(retained).toBe(
      key +
        'SET head = :last, #ttl = :t'.length +
        'head = :prev AND (attribute_not_exists(#ttl) OR #ttl <= :t)'.length +
        '#ttl'.length +
        'ttl'.length +
        ':last'.length +
        1 +
        ':prev'.length +
        1 +
        ':t'.length +
        10,
    )

    // The estimator uses the commit's own ttl for `:t`.
    const commit: Commit = {entityId: 'acct', events: events('acct', 8, 9)}
    const sizes = commitSizes(commit, () => 1_760_086_400)
    expect(sizes[2]).toBe(retained)
  })

  test('refuses one item over the per-item limit, and bytes over the commit limit', () => {
    const item = failure(() => checkBudget([10, MAX_COMMIT_ITEM_BYTES + 1]))
    expect(item).toBeInstanceOf(CommitTooLargeError)
    expect((item as CommitTooLargeError).largestItem).toBe(MAX_COMMIT_ITEM_BYTES + 1)

    const full = Array.from({length: 10}, () => MAX_COMMIT_ITEM_BYTES)
    checkBudget(full)
    const bytes = failure(() => checkBudget([...full, 1]))
    expect((bytes as CommitTooLargeError).bytes).toBe(3_800_001)
  })
})

describe('commit validation', () => {
  test('accepts a contiguous commit and ignores the snapshot of an empty one', () => {
    const empty: Commit = {entityId: 'a', events: [], snapshot: newSnapshot('other', 0n)}

    expect(failure(() => validateCommit({entityId: 'a', events: events('a', 3, 5)}))).toBe(
      undefined,
    )
    expect(failure(() => validateCommit(empty))).toBe(undefined)
  })

  test('refuses a commit built wrong', () => {
    const commits: Commit[] = [
      {entityId: 'a', events: [event('b', 1)]},
      {entityId: 'a', events: [event('a', 1), {...event('a', 2), entityType: 'u'}]},
      {entityId: 'a', events: [event('a', 1)], snapshot: newSnapshot('u', 1n)},
      {entityId: 'a', events: [event('a', 1)], snapshot: newSnapshot('t', 0n)},
      {entityId: 'a', events: [{...event('a', 1), id: 'custom'}]},
      {entityId: 'a', events: [event('a', 0)]},
      {entityId: 'a', events: [event('a', 1), event('a', 3)]},
      {entityId: 'a', events: [event('a', 2), event('a', 1)]},
    ]

    for (const commit of commits) {
      const error = failure(() => validateCommit(commit))
      expect(error).toBeInstanceOf(EvtError)
      expect((error as EvtError).code).toBe('invalidCommit')
    }
  })

  test('refuses the reserved namespaces', () => {
    for (const id of ['SYSTEM#FEATURES', 'command-outcome#x']) {
      expect((failure(() => checkNotReserved(id)) as EvtError).code).toBe('reservedEntityId')
    }
    checkNotReserved('system#lower')
  })
})

describe('error classes', () => {
  test('a conflict and a gap are retryable conflicts, and drift is neither', () => {
    const conflict = new ConflictError('a', 'moved')
    const gap = new SequenceGapError('a', 1n, 3n)
    const drift = new StreamHeadDriftError('a', null, 3n)

    expect([isConflict(conflict), isRetryable(conflict)]).toEqual([true, true])
    expect([isConflict(gap), isRetryable(gap)]).toEqual([true, true])
    expect([isConflict(drift), isRetryable(drift)]).toEqual([false, false])
    expect(isConflict(new Error('not evt'))).toBe(false)
  })

  test('name the error and carry its code', () => {
    const drift = new StreamHeadDriftError('a', null, 3n)

    expect(drift).toBeInstanceOf(EvtError)
    expect(drift.name).toBe('StreamHeadDriftError')
    expect(drift.code).toBe('streamHeadDrift')
    expect(drift.message).toBe('stream head drift on entity a: stored head absent, derived head 3')
    expect(new SequenceGapError('a', 1n, 3n).message).toContain('skips from sequence 1 to 3')
  })
})
