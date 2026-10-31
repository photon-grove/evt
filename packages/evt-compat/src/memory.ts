// Runs the conformance suite against the in-memory repository, on every runtime the fixture
// runner covers (ADR 0009, section 3).
import {MemRepository, headFixtures} from '@photon-grove/evt/memory'

import {conformance} from './conformance.js'
import {Checks} from './harness.js'

const checks = new Checks()
const repo = new MemRepository()

await conformance(checks, repo, headFixtures(repo))

checks.report()
