// Runs every fixture vector the TypeScript implementation covers so far. Each phase of ADR 0009
// adds the vectors for what it builds.
import {Checks, loadFixture} from './harness.js'

const checks = new Checks()

// The published entry point resolves through the exports map on this runtime.
const evt: object = await import('@photon-grove/evt')
checks.equal('@photon-grove/evt loads', typeof evt, 'object')

await outcomeDigests()

checks.report()

interface DigestVector {
  name: string
  canonicalHex: string
  digest: string
}

// outcomes.json digests: Web Crypto's SHA-256 of the canonical identity bytes, which the outcome
// identity digest will use.
async function outcomeDigests(): Promise<void> {
  const {digests} = (await loadFixture('outcomes.json')) as {digests: DigestVector[]}
  checks.equal('outcomes digests present', digests.length > 0, true)
  const got = await Promise.all(
    digests.map(async (vector) => {
      const digest = await crypto.subtle.digest('SHA-256', hexBytes(vector.canonicalHex))
      return bytesHex(new Uint8Array(digest))
    }),
  )

  digests.forEach((vector, i) => {
    checks.equal(`outcomes digest ${vector.name}`, got[i], vector.digest)
  })
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
