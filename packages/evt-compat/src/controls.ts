// The feature marker and rebuild-fence rows of a live store, written straight to the table, as
// another writer would: the conformance suites declare and lock with them, and the live driver's
// declare step declares with them.
import type {DynamoDB} from '@aws-sdk/client-dynamodb'
import type {Pool} from 'pg'

import {type FeatureManifest, decodeManifest, encodeManifest} from '@photon-grove/evt'
import {type Item, manifestFromItem, markerItem} from '@photon-grove/evt/dynamodb'

import type {ControlFixtures, ManifestDraft, RebuildState} from './conformance.js'

/** The control rows of the DynamoDB event log `table`, written straight to the table. */
export function dynamoControls(client: DynamoDB, table: string): ControlFixtures {
  const manifest = async (): Promise<FeatureManifest | null> => {
    const {Item: item} = await client.getItem({
      TableName: table,
      Key: key('SYSTEM#FEATURES', 0n),
      ConsistentRead: true,
    })

    return item ? manifestFromItem(item as Item) : null
  }

  return {
    manifest,
    async declare(change: (draft: ManifestDraft) => void) {
      const stored = await manifest()
      const draft: ManifestDraft = {
        features: new Map(stored?.features),
        historyDependent: [...(stored?.historyDependent ?? [])],
        retention: [...(stored?.retention ?? [])],
      }
      change(draft)

      const next = {...draft, version: (stored?.version ?? 0n) + 1n}
      await client.putItem({
        TableName: table,
        Item: markerItem(next),
        ...(stored
          ? {
              ConditionExpression: 'version = :observed',
              ExpressionAttributeValues: {':observed': {N: stored.version.toString()}},
            }
          : {ConditionExpression: 'attribute_not_exists(pk)'}),
      })

      // Read back as stored: each set sorted and deduplicated.
      return (await manifest())!
    },
    async setRebuild(state: RebuildState) {
      await client.putItem({
        TableName: table,
        Item: {
          ...key('SYSTEM#REBUILD#GENERATION', 0n),
          value: {N: state.generation.toString()},
          // A force-release flags the generation, as the lifecycle's own force-release does.
          ...(state.lock === 'forceReleased' && {forceReleased: {BOOL: true}}),
        },
      })

      const lock = key('SYSTEM#REBUILD', 0n)
      if (state.lock === null) {
        await client.deleteItem({TableName: table, Key: lock})
      } else if (state.lock === 'held') {
        await client.putItem({
          TableName: table,
          Item: {
            ...lock,
            owner: {S: 'conformance'},
            generation: {N: state.generation.toString()},
            leaseExpiresAt: {N: String(Math.floor(Date.now() / 1000) + 300)},
          },
        })
      } else {
        await client.putItem({TableName: table, Item: {...lock, forceReleased: {BOOL: true}}})
      }
    },
  }
}

/**
 * The control rows of the PostgreSQL event log whose events table is `events`, written straight to
 * the tables.
 */
export function postgresControls(pool: Pool, events: string): ControlFixtures {
  const features = `${events}__evt_features`

  const manifest = async (): Promise<FeatureManifest | null> => {
    const {rows} = await pool.query<Record<string, string>>(
      'SELECT version::text AS version, features, history_dependent, retention ' +
        `FROM ${features} WHERE id = 0`,
    )
    const row = rows[0]

    return row === undefined
      ? null
      : decodeManifest(BigInt(row['version']!), {
          features: row['features']!,
          historyDependent: row['history_dependent']!,
          retention: row['retention']!,
        })
  }

  return {
    manifest,
    async declare(change: (draft: ManifestDraft) => void) {
      const stored = await manifest()
      const draft: ManifestDraft = {
        features: new Map(stored?.features),
        historyDependent: [...(stored?.historyDependent ?? [])],
        retention: [...(stored?.retention ?? [])],
      }
      change(draft)

      const texts = encodeManifest({...draft, version: 0n})
      const version = ((stored?.version ?? 0n) + 1n).toString()
      await pool.query(
        `INSERT INTO ${features} (id, version, features, history_dependent, retention) ` +
          'VALUES (0, $1::bigint, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET ' +
          'version = EXCLUDED.version, features = EXCLUDED.features, ' +
          'history_dependent = EXCLUDED.history_dependent, retention = EXCLUDED.retention',
        [version, texts.features, texts.historyDependent, texts.retention],
      )

      // Read back as stored: each set sorted and deduplicated.
      return (await manifest())!
    },
    async setRebuild(state: RebuildState) {
      // PostgreSQL keeps no lock row for a force-release: the generation row's flag records it.
      const generation = state.generation.toString()
      await pool.query(
        `INSERT INTO ${events}__evt_rebuild_generation (id, value, force_released) ` +
          'VALUES (0, $1::bigint, $2) ON CONFLICT (id) DO UPDATE SET ' +
          'value = EXCLUDED.value, force_released = EXCLUDED.force_released',
        [generation, state.lock === 'forceReleased'],
      )
      await pool.query(`DELETE FROM ${events}__evt_rebuild_lock WHERE id = 0`)
      if (state.lock === 'held') {
        await pool.query(
          `INSERT INTO ${events}__evt_rebuild_lock (id, owner, generation, lease_expires_at) ` +
            "VALUES (0, 'conformance', $1::bigint, now() + interval '5 minutes')",
          [generation],
        )
      }
    },
  }
}

function key(pk: string, sk: bigint): Item {
  return {pk: {S: pk}, sk: {N: sk.toString()}}
}
