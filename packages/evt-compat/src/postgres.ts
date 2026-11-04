// feature-marker.json postgresSchemas: the DDL Go generates for each pair of table names, which
// the TypeScript schema must match byte for byte so every language creates the same schema.
import {pgTables, schemaDdl} from '@photon-grove/evt/postgres'

import {type Checks, field, list, loadFixture, str} from './harness.js'

export async function postgresSchemaVectors(checks: Checks): Promise<void> {
  const schemas = list(field(await loadFixture('feature-marker.json'), 'postgresSchemas'))
  checks.equal('postgres schemas present', schemas.length > 0, true)

  for (const vector of schemas) {
    const events = str(field(vector, 'eventsTable'))
    const snapshots = str(field(vector, 'snapshotsTable'))
    const name = `postgres schema ${events}/${snapshots}`
    checks.run(name, () => {
      checks.equal(name, schemaDdl(pgTables(events, snapshots)), str(field(vector, 'ddl')))
    })
  }
}
