// The canonical text of the numbers in a stored rejection (storage format, "Rejections").
// PostgreSQL's jsonb prints a number in plain decimal digits, whatever literal it was given, so
// every encoder writes that text itself: then every backend stores, and reads back, the same
// bytes, and a first execution equals a replay. A port of Go's canonicalNumbers and Rust's
// canonical_numbers.

import {EvtError, OutcomeTooLargeError} from './errors.js'
import {utf8Length} from './utf8.js'

/** The most bytes a stored response, rejection or recorded identity may take. */
export const MAX_OUTCOME_BYTES = 65536

// The most digits after the decimal point in the canonical text.
const MAX_SCALE = 16383

// The most significant digits before the decimal point.
const MAX_INTEGER_DIGITS = 131_072

// Saturates a written exponent so the digit arithmetic stays exact. It is far past both limits: a
// non-zero number with a larger exponent is refused by one of them, and a zero is 0 whatever its
// exponent.
const EXPONENT_CAP = 2 ** 30

/**
 * Rewrites every number in an encoded JSON document into its canonical text, the one PostgreSQL's
 * jsonb prints: plain decimal digits with no exponent, the fraction digits the literal implies
 * (its own, minus the exponent, never fewer than zero), and no sign on zero. So `1e+21` becomes
 * `1000000000000000000000`, `1.50e1` becomes `15.0`, and `-0` becomes `0`. Everything outside the
 * numbers is kept as it is.
 *
 * The input must be valid JSON, as an encoder writes it. A number whose canonical text PostgreSQL's
 * numeric type can't hold (more than 16383 digits after the decimal point, or more than 131072
 * before it) is refused with `invalidArgument`, and a document whose canonical form is over
 * {@link MAX_OUTCOME_BYTES} with an {@link OutcomeTooLargeError}, before it's built.
 */
export function canonicalNumbers(json: string): string {
  const parts = tokens(json)
  let size = 0
  let changed = false

  for (const part of parts) {
    if (typeof part === 'string') {
      size += utf8Length(part)
      continue
    }

    part.check()
    size += part.canonicalLength()
    // A literal without an exponent is already canonical, except for a negative zero: a JSON
    // encoder writes no leading zeros and no other sign.
    changed ||= /[eE]/.test(part.text) || (part.negative && part.isZero())
  }

  if (size > MAX_OUTCOME_BYTES) {
    throw new OutcomeTooLargeError('rejection', size)
  }
  if (!changed) {
    return json
  }

  return parts.map((part) => (typeof part === 'string' ? part : part.canonical())).join('')
}

// Splits a valid, compact JSON document into number literals and the runs of text between them.
function tokens(json: string): (string | NumberLiteral)[] {
  const parts: (string | NumberLiteral)[] = []
  let start = 0
  let index = 0
  let inString = false

  while (index < json.length) {
    const c = json[index]!
    if (inString && c === '\\') {
      index += 2
      continue
    }

    if (c === '"') {
      inString = !inString
    } else if (!inString && (c === '-' || isDigit(c))) {
      let end = index + 1
      while (end < json.length && isNumberChar(json[end]!)) {
        end++
      }

      if (index > start) {
        parts.push(json.slice(start, index))
      }
      parts.push(new NumberLiteral(json.slice(index, end)))
      start = end
      index = end
      continue
    }

    index++
  }

  if (start < json.length) {
    parts.push(json.slice(start))
  }

  return parts
}

function isDigit(c: string): boolean {
  return c >= '0' && c <= '9'
}

function isNumberChar(c: string): boolean {
  return isDigit(c) || c === '-' || c === '+' || c === '.' || c === 'e' || c === 'E'
}

// One JSON number literal, split into the parts its canonical text is built from.
class NumberLiteral {
  readonly text: string
  readonly negative: boolean
  // The integer digits and the fraction digits, without the decimal point.
  readonly #digits: string
  // The decimal point's position in the digits once the exponent is applied. It may lie before
  // the first digit (negative) or past the last.
  readonly #point: number
  // The number of digits after the decimal point in the canonical text.
  readonly #scale: number

  constructor(text: string) {
    this.text = text
    this.negative = text.startsWith('-')
    const unsigned = this.negative ? text.slice(1) : text

    const at = unsigned.search(/[eE]/)
    const mantissa = at === -1 ? unsigned : unsigned.slice(0, at)
    const exponent = at === -1 ? 0 : parseExponent(unsigned.slice(at + 1))

    const point = mantissa.indexOf('.')
    const integer = point === -1 ? mantissa : mantissa.slice(0, point)
    const fraction = point === -1 ? '' : mantissa.slice(point + 1)

    this.#digits = integer + fraction
    this.#point = integer.length + exponent
    this.#scale = Math.max(fraction.length - exponent, 0)
  }

  isZero(): boolean {
    return this.#firstSignificant() === this.#digits.length
  }

  // Refuses a number past PostgreSQL's numeric limits.
  check(): void {
    if (this.#scale > MAX_SCALE || this.#integerDigits() > MAX_INTEGER_DIGITS) {
      const shown =
        this.text.length <= 64
          ? this.text
          : `${this.text.slice(0, 64)}... (${this.text.length} bytes)`
      throw new EvtError(
        'invalidArgument',
        `rejection details hold a number out of the storable range: ${shown}`,
      )
    }
  }

  canonicalLength(): number {
    let length = Math.max(this.#integerDigits(), 1)
    if (this.negative && !this.isZero()) {
      length += 1
    }
    if (this.#scale > 0) {
      length += 1 + this.#scale
    }

    return length
  }

  // The sign unless zero, the integer digits without leading zeros (at least one), then the
  // decimal point and exactly `scale` fraction digits.
  canonical(): string {
    let out = this.negative && !this.isZero() ? '-' : ''
    out += this.#integerDigits() === 0 ? '0' : this.#range(this.#firstSignificant(), this.#point)
    if (this.#scale > 0) {
      out += `.${this.#range(this.#point, this.#point + this.#scale)}`
    }

    return out
  }

  // The digits at positions start to end, with zeros past either end of the written ones.
  #range(start: number, end: number): string {
    const length = this.#digits.length
    const before = Math.max(0, Math.min(end, 0) - start)
    const after = Math.max(0, end - Math.max(start, length))
    const written = this.#digits.slice(Math.max(start, 0), Math.max(Math.min(end, length), 0))

    return '0'.repeat(before) + written + '0'.repeat(after)
  }

  // The index of the first non-zero digit, or the digit count for zero.
  #firstSignificant(): number {
    const at = this.#digits.search(/[1-9]/)

    return at === -1 ? this.#digits.length : at
  }

  // The number of significant digits before the decimal point.
  #integerDigits(): number {
    if (this.isZero()) {
      return 0
    }

    return Math.max(this.#point - this.#firstSignificant(), 0)
  }
}

// Reads an optionally signed exponent, saturating at EXPONENT_CAP. A character that isn't a digit
// counts as zero, so malformed input can't throw.
function parseExponent(text: string): number {
  const sign = text[0]
  const negative = sign === '-'
  const digits = sign === '-' || sign === '+' ? text.slice(1) : text

  let magnitude = 0
  for (const c of digits) {
    const digit = isDigit(c) ? c.charCodeAt(0) - 48 : 0
    magnitude = Math.min(magnitude * 10 + digit, EXPONENT_CAP)
  }

  return negative ? -magnitude : magnitude
}
