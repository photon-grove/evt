// UTF-8 sizes and conversions. The storage format counts and compares UTF-8 bytes, and a JS string
// is UTF-16, so nothing here uses `.length` or `<` on strings.

import {EvtError} from './errors.js'

const encoder = new TextEncoder()

// fatal refuses bytes that aren't UTF-8, and ignoreBOM keeps a leading U+FEFF, which Go keeps.
const decoder = new TextDecoder('utf-8', {fatal: true, ignoreBOM: true})

/**
 * The UTF-8 length of a string in bytes, counted without allocating. A lone surrogate counts as
 * 3, the U+FFFD that `TextEncoder` writes for it.
 */
export function utf8Length(text: string): number {
  let bytes = 0
  for (let i = 0; i < text.length; i++) {
    const unit = text.charCodeAt(i)
    if (unit < 0x80) {
      bytes += 1
    } else if (unit < 0x800) {
      bytes += 2
    } else if (isHighSurrogate(unit) && isLowSurrogate(text.charCodeAt(i + 1))) {
      bytes += 4
      i++
    } else {
      bytes += 3
    }
  }

  return bytes
}

/** Encodes a string as UTF-8. A lone surrogate becomes U+FFFD. */
export function utf8Encode(text: string): Uint8Array<ArrayBuffer> {
  return encoder.encode(text)
}

/** Decodes UTF-8 bytes, refusing bytes that aren't UTF-8. */
export function utf8Decode(bytes: Uint8Array, what: string): string {
  try {
    return decoder.decode(bytes)
  } catch (error) {
    throw new EvtError('invalidRecord', `${what} is not valid UTF-8`, {cause: error})
  }
}

/**
 * Compares two strings by their UTF-8 bytes, which is how Go orders map keys. Code point order is
 * UTF-8 byte order, so this compares code points, where `<` would compare UTF-16 code units.
 */
export function compareUtf8(a: string, b: string): number {
  let i = 0
  let j = 0
  while (i < a.length && j < b.length) {
    const x = codePointAt(a, i)
    const y = codePointAt(b, j)
    if (x !== y) {
      return x < y ? -1 : 1
    }

    i += x > 0xffff ? 2 : 1
    j += y > 0xffff ? 2 : 1
  }

  return a.length - i - (b.length - j)
}

/** The code point at `i`, with a lone surrogate read as U+FFFD (what Go writes for it). */
function codePointAt(text: string, i: number): number {
  const unit = text.charCodeAt(i)
  if (isHighSurrogate(unit)) {
    const next = text.charCodeAt(i + 1)
    if (isLowSurrogate(next)) {
      return (unit - 0xd800) * 0x400 + (next - 0xdc00) + 0x10000
    }
  }

  return isSurrogate(unit) ? 0xfffd : unit
}

export function isHighSurrogate(unit: number): boolean {
  return unit >= 0xd800 && unit <= 0xdbff
}

export function isLowSurrogate(unit: number): boolean {
  return unit >= 0xdc00 && unit <= 0xdfff
}

function isSurrogate(unit: number): boolean {
  return unit >= 0xd800 && unit <= 0xdfff
}
