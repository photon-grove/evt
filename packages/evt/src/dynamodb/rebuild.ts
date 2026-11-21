// The rebuild fence's items and requests on DynamoDB (storage format, "Rebuild fence"), byte for
// byte as Go's and Rust's repositories send them, which compat/fixtures/feature-marker.json pins.
// Both rows live in the events table under the reserved SYSTEM# namespace, at `sk = 0`:
//
// | Item       | Key                               | Attributes                                    |
// | ---------- | --------------------------------- | --------------------------------------------- |
// | lock       | `pk = SYSTEM#REBUILD`             | `owner`, `generation`, `leaseExpiresAt`        |
// | released   | `pk = SYSTEM#REBUILD`             | `forceReleased = true`, nothing else           |
// | generation | `pk = SYSTEM#REBUILD#GENERATION`  | `value`, and `forceReleased = true` when set   |

import {EvtError} from '../errors.js'
import {
  REBUILD_GENERATION_ID,
  REBUILD_LOCK_ID,
  type RebuildBeginPlan,
  type RebuildGeneration,
  type RebuildLease,
  type RebuildLock,
  type RebuildState,
} from '../rebuild-fence.js'
import type {TransactItem, UpdateItemInput} from './client.js'
import type {Item} from './item.js'
import {controlKey} from './requests.js'

/** The lock item's key. */
export function rebuildLockKey(): Item {
  return controlKey(REBUILD_LOCK_ID)
}

/** The generation item's key. */
export function rebuildGenerationKey(): Item {
  return controlKey(REBUILD_GENERATION_ID)
}

/** The generation an item stores; no item is generation 0, never force-released. */
export function generationFromItem(item: Item | null | undefined): RebuildGeneration {
  if (!item || Object.keys(item).length === 0) {
    return {value: 0n, forceReleased: false}
  }
  const value = item['value']
  if (value === undefined || !('N' in value)) {
    throw invalid('the generation item has no numeric value')
  }
  const flag = item['forceReleased']
  if (flag !== undefined && !('BOOL' in flag)) {
    throw invalid('the generation item has a forceReleased that is not a boolean')
  }

  return {value: BigInt(value.N), forceReleased: flag?.BOOL === true}
}

/** The lock an item stores, or null for no item and for the item a force-release leaves. */
export function lockFromItem(item: Item | null | undefined): RebuildLock | null {
  if (!item || Object.keys(item).length === 0) {
    return null
  }
  const flag = item['forceReleased']
  if (flag !== undefined) {
    if ('BOOL' in flag && flag.BOOL === true) {
      return null
    }
    throw invalid('the rebuild lock item has a forceReleased that is not true')
  }
  const owner = item['owner']
  const expiry = item['leaseExpiresAt']
  const generation = item['generation']
  if (owner === undefined || !('S' in owner)) {
    throw invalid('the rebuild lock item has no owner')
  }
  if (expiry === undefined || !('N' in expiry)) {
    throw invalid('the rebuild lock item has no numeric leaseExpiresAt')
  }
  if (generation === undefined || !('N' in generation)) {
    throw invalid('the rebuild lock item has no numeric generation')
  }

  return {owner: owner.S, generation: BigInt(generation.N), leaseExpiresAt: Number(expiry.N)}
}

/** The generation item, or null for generation 0 that was never force-released. */
export function generationItem(generation: RebuildGeneration): Item | null {
  if (generation.value === 0n && !generation.forceReleased) {
    return null
  }

  return {
    ...rebuildGenerationKey(),
    value: {N: generation.value.toString()},
    ...(generation.forceReleased && {forceReleased: {BOOL: true}}),
  }
}

/** The lock item, the item a force-release leaves, or null, for a state. */
export function lockItem(state: RebuildState): Item | null {
  const {lock, generation} = state
  if (lock !== null) {
    return {
      ...rebuildLockKey(),
      owner: {S: lock.owner},
      generation: {N: lock.generation.toString()},
      leaseExpiresAt: {N: lock.leaseExpiresAt.toString()},
    }
  }
  if (generation.forceReleased && generation.value % 2n !== 0n) {
    return {...rebuildLockKey(), forceReleased: {BOOL: true}}
  }

  return null
}

/**
 * The transaction that takes the lock as `plan` decided at `now`: the lock item's put, conditioned
 * on no lock or a force-released one (or, for a takeover, the expired lease of the owner it read),
 * and the generation's update, conditioned on the generation it read.
 */
export function beginRebuildItems(
  table: string,
  plan: RebuildBeginPlan,
  lease: RebuildLease,
  now: number,
): TransactItem[] {
  const lock = {
    ...rebuildLockKey(),
    owner: {S: lease.owner},
    generation: {N: lease.generation.toString()},
    leaseExpiresAt: {N: lease.leaseExpiresAt.toString()},
  }
  if (plan.kind === 'takeover') {
    return [
      {
        Put: {
          TableName: table,
          Item: lock,
          ConditionExpression: 'leaseExpiresAt < :now AND #owner = :observedOwner',
          ExpressionAttributeNames: {'#owner': 'owner'},
          ExpressionAttributeValues: {
            ':now': {N: now.toString()},
            ':observedOwner': {S: plan.observedOwner},
          },
        },
      },
      {
        Update: {
          TableName: table,
          Key: rebuildGenerationKey(),
          UpdateExpression: 'SET #value = #value + :two',
          ConditionExpression: '#value = :seenOdd',
          ExpressionAttributeNames: {'#value': 'value'},
          ExpressionAttributeValues: {':two': {N: '2'}, ':seenOdd': {N: plan.seenOdd.toString()}},
        },
      },
    ]
  }

  return [
    {
      Put: {
        TableName: table,
        Item: lock,
        ConditionExpression: 'attribute_not_exists(pk) OR forceReleased = :true',
        ExpressionAttributeValues: {':true': {BOOL: true}},
      },
    },
    {
      Update: {
        TableName: table,
        Key: rebuildGenerationKey(),
        UpdateExpression: 'SET #value = if_not_exists(#value, :zero) + :increment',
        ConditionExpression:
          'attribute_not_exists(#value) OR #value = :seenEven OR ' +
          '(#value = :seenOdd AND forceReleased = :true)',
        ExpressionAttributeNames: {'#value': 'value'},
        ExpressionAttributeValues: {
          ':zero': {N: '0'},
          ':increment': {N: plan.increment.toString()},
          ':seenEven': {N: plan.seenEven.toString()},
          ':seenOdd': {N: plan.seenOdd.toString()},
          ':true': {BOOL: true},
        },
      },
    },
  ]
}

/** The update that extends a lease to `expiry`, conditioned on the lease's owner and generation. */
export function renewRebuildUpdate(
  table: string,
  lease: RebuildLease,
  expiry: number,
): UpdateItemInput {
  return {
    TableName: table,
    Key: rebuildLockKey(),
    UpdateExpression: 'SET leaseExpiresAt = :expiry',
    ConditionExpression: '#owner = :owner AND #generation = :generation',
    ExpressionAttributeNames: {'#owner': 'owner', '#generation': 'generation'},
    ExpressionAttributeValues: {
      ':expiry': {N: expiry.toString()},
      ':owner': {S: lease.owner},
      ':generation': {N: lease.generation.toString()},
    },
  }
}

/**
 * The transaction that ends a lease: the lock's delete, conditioned on the lease, and the
 * generation's step to even, which clears the force-release flag.
 */
export function endRebuildItems(table: string, lease: RebuildLease): TransactItem[] {
  return [
    {
      Delete: {
        TableName: table,
        Key: rebuildLockKey(),
        ConditionExpression: '#owner = :me AND #generation = :generation',
        ExpressionAttributeNames: {'#owner': 'owner', '#generation': 'generation'},
        ExpressionAttributeValues: {
          ':me': {S: lease.owner},
          ':generation': {N: lease.generation.toString()},
        },
      },
    },
    {
      Update: {
        TableName: table,
        Key: rebuildGenerationKey(),
        UpdateExpression: 'SET #value = #value + :one REMOVE forceReleased',
        ConditionExpression: '#value = :seenOdd',
        ExpressionAttributeNames: {'#value': 'value'},
        ExpressionAttributeValues: {
          ':one': {N: '1'},
          ':seenOdd': {N: lease.generation.toString()},
        },
      },
    },
  ]
}

/**
 * The transaction that force-releases `owner`'s lease from the odd generation `seen`: the lock
 * becomes the force-released item, and the generation steps two and keeps odd, flagged.
 */
export function forceReleaseRebuildItems(
  table: string,
  owner: string,
  seen: bigint,
): TransactItem[] {
  return [
    {
      Put: {
        TableName: table,
        Item: {...rebuildLockKey(), forceReleased: {BOOL: true}},
        ConditionExpression: '#owner = :requestedOwner',
        ExpressionAttributeNames: {'#owner': 'owner'},
        ExpressionAttributeValues: {':requestedOwner': {S: owner}},
      },
    },
    {
      Update: {
        TableName: table,
        Key: rebuildGenerationKey(),
        UpdateExpression: 'SET #value = #value + :two, forceReleased = :true',
        ConditionExpression: '#value = :seenOdd',
        ExpressionAttributeNames: {'#value': 'value'},
        ExpressionAttributeValues: {
          ':two': {N: '2'},
          ':seenOdd': {N: seen.toString()},
          ':true': {BOOL: true},
        },
      },
    },
  ]
}

/** The lease guard a guarded write ends with: the lease holds the lock, unexpired at `now`. */
export function rebuildGuardCheck(table: string, lease: RebuildLease, now: number): TransactItem {
  return {
    ConditionCheck: {
      TableName: table,
      Key: rebuildLockKey(),
      ConditionExpression: '#owner = :me AND #generation = :generation AND leaseExpiresAt > :now',
      ExpressionAttributeNames: {'#owner': 'owner', '#generation': 'generation'},
      ExpressionAttributeValues: {
        ':me': {S: lease.owner},
        ':generation': {N: lease.generation.toString()},
        ':now': {N: now.toString()},
      },
    },
  }
}

function invalid(message: string): EvtError {
  return new EvtError('invalidRecord', message)
}
