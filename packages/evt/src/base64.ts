// Standard, padded base64 (Go's base64.StdEncoding), which is how a Go []byte appears in JSON.
// It's our own code because Buffer is Node-only and Uint8Array.fromBase64 isn't on every runtime.

import {EvtError} from './errors.js'

const ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'

const SYMBOLS = new Map(Array.from(ALPHABET, (symbol, value) => [symbol.charCodeAt(0), value]))

const PAD = 0x3d // '='

/** Encodes bytes as standard, padded base64. */
export function base64Encode(bytes: Uint8Array): string {
  let out = ''
  for (let i = 0; i < bytes.length; i += 3) {
    const b0 = bytes[i] ?? 0
    const b1 = bytes[i + 1] ?? 0
    const b2 = bytes[i + 2] ?? 0
    const triple = (b0 << 16) | (b1 << 8) | b2

    out += ALPHABET.charAt((triple >> 18) & 63)
    out += ALPHABET.charAt((triple >> 12) & 63)
    out += i + 1 < bytes.length ? ALPHABET.charAt((triple >> 6) & 63) : '='
    out += i + 2 < bytes.length ? ALPHABET.charAt(triple & 63) : '='
  }

  return out
}

/**
 * Decodes standard, padded base64 the way Go does: `\r` and `\n` are skipped, and anything else
 * outside the alphabet, missing padding, or padding before the end is refused.
 */
export function base64Decode(text: string): Uint8Array<ArrayBuffer> {
  const symbols: number[] = []
  for (let i = 0; i < text.length; i++) {
    const unit = text.charCodeAt(i)
    if (unit !== 0x0d && unit !== 0x0a) {
      symbols.push(unit)
    }
  }
  if (symbols.length % 4 !== 0) {
    throw invalid(`length ${symbols.length}`)
  }

  const quads = symbols.length / 4
  const last = symbols.slice(-2)
  const padding = last[1] === PAD ? (last[0] === PAD ? 2 : 1) : 0
  const out = new Uint8Array(quads * 3 - padding)

  for (let quad = 0; quad < quads; quad++) {
    let triple = 0
    for (let offset = 0; offset < 4; offset++) {
      const index = quad * 4 + offset
      const symbol = symbols[index] ?? PAD
      const isPadding = quad === quads - 1 && offset >= 4 - padding
      const value = isPadding ? 0 : SYMBOLS.get(symbol)
      if (value === undefined) {
        throw invalid(`at input byte ${index}`)
      }

      triple = (triple << 6) | value
    }

    const at = quad * 3
    out[at] = triple >> 16
    if (at + 1 < out.length) {
      out[at + 1] = (triple >> 8) & 0xff
    }
    if (at + 2 < out.length) {
      out[at + 2] = triple & 0xff
    }
  }

  return out
}

function invalid(where: string): EvtError {
  return new EvtError('invalidRecord', `illegal base64 data ${where}`)
}
