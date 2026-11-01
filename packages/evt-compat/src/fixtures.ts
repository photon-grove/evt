// Runs every fixture vector the TypeScript implementation covers so far. Each phase of ADR 0009
// adds the vectors for what it builds.
import {DEFAULT_RETRY_POLICY} from '@photon-grove/evt'

import {codecVectors} from './codec.js'
import {Checks, field, list, loadFixture, num, str} from './harness.js'
import {scenarioChecks} from './scenarios.js'

const checks = new Checks()

// The published entry points resolve through the exports map on this runtime.
const evt: object = await import('@photon-grove/evt')
checks.equal('@photon-grove/evt loads', typeof evt, 'object')
const dynamodb: object = await import('@photon-grove/evt/dynamodb')
checks.equal('@photon-grove/evt/dynamodb loads', typeof dynamodb, 'object')

await codecVectors(checks)
await outcomeDigests()
await retryDefaults()
await scenarioChecks(checks)

checks.report()

// outcomes.json digests: Web Crypto's SHA-256 of the canonical identity bytes, which the outcome
// identity digest will use.
async function outcomeDigests(): Promise<void> {
  const digests = list(field(await loadFixture('outcomes.json'), 'digests'))
  checks.equal('outcomes digests present', digests.length > 0, true)
  const got = await Promise.all(
    digests.map(async (vector) => {
      const canonical = hexBytes(str(field(vector, 'canonicalHex')))
      return bytesHex(new Uint8Array(await crypto.subtle.digest('SHA-256', canonical)))
    }),
  )

  digests.forEach((vector, i) => {
    const name = `outcomes digest ${str(field(vector, 'name'))}`
    checks.equal(name, got[i], str(field(vector, 'digest')))
  })
}

// outcomes.json retry: the default policy Go and Rust retry with.
async function retryDefaults(): Promise<void> {
  const retry = field(await loadFixture('outcomes.json'), 'retry')
  for (const key of ['maxAttempts', 'baseDelayMs', 'maxDelayMs'] as const) {
    checks.equal(`retry ${key}`, DEFAULT_RETRY_POLICY[key], num(field(retry, key)))
  }
}

function hexBytes(hex: string): Uint8Array<ArrayBuffer> {
  const bytes = new Uint8Array(hex.length / 2)
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = Number.parseInt(hex.slice(2 * i, 2 * i + 2), 16)
  }

  return bytes
}

function bytesHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')
}
