// Commit IDs for commits whose command carries none: ULID-format identifiers, 26 Crockford base32
// characters for a 48-bit millisecond timestamp and 80 random bits. IDs one generator makes in one
// millisecond increase, so they sort by creation time.

const ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'
const ENTROPY_MAX = (1n << 80n) - 1n
const TIMESTAMP_MASK = (1n << 48n) - 1n

/** Makes a ULID-format commit ID generator that reads `clock` (Unix milliseconds). */
export function commitIds(clock: () => number): () => string {
  let lastMs = -1n
  let entropy = 0n

  return () => {
    // A clock that steps backwards keeps the last timestamp, so IDs never decrease.
    let ms = BigInt(Math.max(0, Math.floor(clock()))) & TIMESTAMP_MASK
    if (ms < lastMs) {
      ms = lastMs
    }

    if (ms === lastMs && entropy < ENTROPY_MAX) {
      entropy++
    } else {
      // A new millisecond, or 2^80 IDs in this one.
      if (ms === lastMs) {
        ms = (ms + 1n) & TIMESTAMP_MASK
      }
      lastMs = ms
      entropy = randomEntropy()
    }

    return encode((ms << 80n) | entropy)
  }
}

function randomEntropy(): bigint {
  const bytes = crypto.getRandomValues(new Uint8Array(10))

  return bytes.reduce((value, byte) => (value << 8n) | BigInt(byte), 0n)
}

function encode(value: bigint): string {
  let text = ''
  for (let i = 0; i < 26; i++) {
    text = ALPHABET[Number(value & 31n)]! + text
    value >>= 5n
  }

  return text
}
