// The feature marker's row on PostgreSQL (storage format, "Feature marker"): one row, `id = 0`, in
// the events table's `__evt_features` relation, with the manifest's stored JSON texts. A
// declaration writes it under the manifest's exclusive lock: the first inserts it at version 1,
// and every later one updates it from the version it read.

import {type FeatureManifest, encodeManifest} from '../features.js'
import {type PgTables, featuresTable} from './schema.js'

/** The marker row as PostgreSQL stores it, the version as text. */
export interface ManifestRow {
  readonly id: 0
  readonly version: string
  readonly features: string
  readonly history_dependent: string
  readonly retention: string
}

/** The row a declaration of `manifest` stores. */
export function manifestRow(manifest: FeatureManifest): ManifestRow {
  const texts = encodeManifest(manifest)

  return {
    id: 0,
    version: manifest.version.toString(),
    features: texts.features,
    history_dependent: texts.historyDependent,
    retention: texts.retention,
  }
}

/**
 * The statement that declares `next` over the `stored` manifest, as Go's repository writes it:
 * an insert of the first row, which is always version 1, or an update conditioned on the version
 * it read. Either returns one row when it wrote.
 */
export function manifestWrite(
  tables: PgTables,
  stored: FeatureManifest | null,
  next: FeatureManifest,
): {text: string; values: string[]} {
  const table = featuresTable(tables)
  const row = manifestRow(next)
  if (stored === null) {
    if (next.version !== 1n) {
      throw new RangeError(`the first declaration is version 1, not ${next.version}`)
    }

    return {
      text:
        `INSERT INTO ${table} (id, version, features, history_dependent, retention) ` +
        'VALUES (0, 1, $1, $2, $3) RETURNING 1',
      values: [row.features, row.history_dependent, row.retention],
    }
  }

  return {
    text:
      `UPDATE ${table} SET version = $1::bigint, features = $2, history_dependent = $3, ` +
      'retention = $4 WHERE id = 0 AND version = $5::bigint RETURNING 1',
    values: [
      row.version,
      row.features,
      row.history_dependent,
      row.retention,
      stored.version.toString(),
    ],
  }
}
