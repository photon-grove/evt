// The DynamoDB item of the feature-marker row. The control rows live in the reserved SYSTEM#
// namespace at sort key 0.
//
// | Attribute          | Type | Feature-marker item (`pk = SYSTEM#FEATURES`, `sk = 0`)       |
// | ------------------ | ---- | ------------------------------------------------------------ |
// | `version`          | N    | bumped on every change to the manifest                       |
// | `features`         | S    | JSON object, feature name → stored-format version, sorted    |
// | `historyDependent` | S    | JSON array of entity types, sorted. Absent when empty        |
// | `retention`        | S    | JSON array of entity types, sorted. Absent when empty        |

import {checkInt64, invalid} from '../fields.js'
import {type FeatureManifest, decodeManifest, encodeManifest} from '../features.js'
import {type Item, SNAPSHOT_SORT_KEY} from './item.js'

/** The partition key of the feature-marker row. */
export const FEATURES_PK = 'SYSTEM#FEATURES'

/** The partition key of the rebuild-lock row, which every commit checks is absent. */
export const REBUILD_LOCK_PK = 'SYSTEM#REBUILD'

/** Encodes a feature manifest as its marker item. Empty entity-type sets are left out. */
export function markerItem(manifest: FeatureManifest): Item {
  const texts = encodeManifest(manifest)
  const item: Item = {
    pk: {S: FEATURES_PK},
    sk: {N: SNAPSHOT_SORT_KEY.toString()},
    version: {N: manifest.version.toString()},
    features: {S: texts.features},
  }
  if (manifest.historyDependent.length > 0) {
    item['historyDependent'] = {S: texts.historyDependent}
  }
  if (manifest.retention.length > 0) {
    item['retention'] = {S: texts.retention}
  }

  return item
}

/** Decodes a marker item. An absent text attribute is an empty map or set. */
export function manifestFromItem(item: Item): FeatureManifest {
  const version = item['version']
  if (version === undefined || !('N' in version) || !/^\d+$/.test(version.N)) {
    throw invalid('the feature marker has no version')
  }

  return decodeManifest(checkInt64(BigInt(version.N), 'the feature marker version'), {
    features: text(item, 'features'),
    historyDependent: text(item, 'historyDependent'),
    retention: text(item, 'retention'),
  })
}

function text(item: Item, name: string): string {
  const value = item[name]
  if (value === undefined) {
    return ''
  }
  if (!('S' in value)) {
    throw invalid(`the feature marker's ${name} attribute is not a string`)
  }

  return value.S
}
