// Runs every fixture vector the TypeScript implementation covers so far. Each phase of ADR 0009
// adds the vectors for what it builds.
import {codecVectors} from './codec.js'
import {Checks} from './harness.js'
import {headVectors, markerVectors, postgresSchemaVectors} from './marker.js'
import {outcomeVectors} from './outcomes.js'
import {scenarioChecks} from './scenarios.js'

const checks = new Checks()

// The published entry points resolve through the exports map on this runtime.
const evt: object = await import('@photon-grove/evt')
checks.equal('@photon-grove/evt loads', typeof evt, 'object')
const dynamodb: object = await import('@photon-grove/evt/dynamodb')
checks.equal('@photon-grove/evt/dynamodb loads', typeof dynamodb, 'object')
const postgres: object = await import('@photon-grove/evt/postgres')
checks.equal('@photon-grove/evt/postgres loads', typeof postgres, 'object')

await codecVectors(checks)
await markerVectors(checks)
await headVectors(checks)
await postgresSchemaVectors(checks)
await outcomeVectors(checks)
await scenarioChecks(checks)

checks.report()
