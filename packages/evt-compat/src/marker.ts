// feature-marker.json declarations: the manifest's stored JSON texts and its DynamoDB item, for
// each declaration of a chain. The commit, head and PostgreSQL vectors wait for the backends that
// write them.
import {type FeatureManifest, encodeManifest} from '@photon-grove/evt'
import {type Item, manifestFromItem, markerItem} from '@photon-grove/evt/dynamodb'

import {type Checks, canonical, field, int, list, loadFixture, num, plain, str} from './harness.js'

export async function markerVectors(checks: Checks): Promise<void> {
  const declarations = list(field(await loadFixture('feature-marker.json'), 'declarations'))
  checks.equal('feature-marker declarations present', declarations.length > 0, true)

  for (const vector of declarations) {
    const name = `feature-marker declaration ${str(field(vector, 'name'))}`
    checks.run(name, () => {
      const input = field(vector, 'input')
      const output = field(vector, 'output')
      const features = field(input, 'features')
      if (!(features instanceof Map)) {
        throw new Error('the declaration has no features object')
      }

      // The PostgreSQL row records the version the chain reached, apart from the item.
      const manifest: FeatureManifest = {
        version: int(field(field(output, 'postgresRow'), 'version')),
        features: new Map(Array.from(features, ([key, value]) => [key, num(value)])),
        historyDependent: list(field(input, 'historyDependent')).map(str),
        retention: list(field(input, 'retention')).map(str),
      }

      const texts = encodeManifest(manifest)
      for (const [part, json] of Object.entries(texts)) {
        checks.equal(`${name} ${part}`, json, str(field(output, `${part}Json`)))
      }

      const item = plain(field(output, 'dynamoItem')) as Item
      const want = canonical(item)
      checks.equal(`${name} dynamoItem`, canonical(markerItem(manifest)), want)

      // Decoding the item gives back the manifest as stored: each set sorted and deduplicated.
      checks.equal(`${name} decoded`, canonical(markerItem(manifestFromItem(item))), want)
    })
  }
}
