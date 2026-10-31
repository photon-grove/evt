// evt's JSON reader and writer. The writer produces Go's encoding/json bytes, which every stored
// document must match, and the reader keeps what JSON.parse loses: each number's text, and object
// keys in the order they were written.

import {base64Encode} from './base64.js'
import {EvtError} from './errors.js'
import {compareUtf8, isHighSurrogate, isLowSurrogate} from './utf8.js'

/** A value read by {@link parseJson}. Objects are Maps, so integer-like keys keep their order. */
export type JsonValue = null | boolean | string | JsonNumber | JsonValue[] | Map<string, JsonValue>

const NUMBER = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/

const INTEGER = /^-?(?:0|[1-9]\d*)$/

/** A JSON number kept as its literal text, so 2^53 + 1 or a 40-digit decimal stays exact. */
export class JsonNumber {
  readonly text: string

  constructor(text: string) {
    if (!NUMBER.test(text)) {
      throw new EvtError('invalidArgument', `not a JSON number: ${text}`)
    }

    this.text = text
  }

  /** Whether the literal is an integer: no fraction and no exponent, as Go's int64 requires. */
  get isInteger(): boolean {
    return INTEGER.test(this.text)
  }
}

/** Already-encoded JSON the writer copies as it is. The caller vouches for its bytes. */
export class RawJson {
  constructor(readonly text: string) {}
}

/**
 * Encodes a value the way Go's `json.Marshal` encodes the same shape:
 *
 * - A plain object is a struct: its own properties in order, skipping `undefined` ones. An
 *   integer-like key (`"10"`) is refused, because the engine has already moved it to the front.
 * - A `Map` is a Go map, written with its keys sorted by their UTF-8 bytes. Keys are strings, or
 *   integers (`number` or `bigint`), which are written as text and sorted as text, like Go's.
 * - A `bigint` is an integer, a `Uint8Array` is a `[]byte` (padded base64), and a
 *   {@link JsonNumber} is written as its text.
 * - Strings escape `<`, `>`, `&`, U+2028 and U+2029, and a lone surrogate becomes `\ufffd`, as Go
 *   writes invalid UTF-8.
 *
 * Anything else, such as a `Date`, a class instance or a number that isn't finite, is refused.
 */
export function stringifyJson(value: unknown): string {
  const out: string[] = []
  // The arrays and objects being written, innermost last. The writer keeps its own stack, as the
  // reader does, so a deep value is written without overflowing the call stack.
  const open: Writing[] = []
  const writing = new Set<object>()
  let next = value
  for (;;) {
    const opened = writeValue(next, out)
    if (opened !== undefined) {
      if (writing.has(opened.value)) {
        throw new EvtError('invalidArgument', 'cannot encode a cyclic value as JSON')
      }

      writing.add(opened.value)
      open.push(opened)
    }

    // Move to the next entry of the innermost open container, closing each one that is done.
    for (;;) {
      const top = open.at(-1)
      if (top === undefined) {
        return out.join('')
      }

      const entry = top.entries[top.next]
      if (entry === undefined) {
        out.push(top.close)
        writing.delete(top.value)
        open.pop()
        continue
      }

      if (top.next > 0) {
        out.push(',')
      }
      top.next++

      const [key, entryValue] = entry
      if (key !== undefined) {
        out.push(quote(key), ':')
      }
      next = entryValue
      break
    }
  }
}

// An array or object being written: its entries (keyed for an object), the next one to write,
// and the character that closes it.
interface Writing {
  value: object
  entries: [string | undefined, unknown][]
  next: number
  close: string
}

// Writes a scalar, or the opening of an array or object, which it returns for the caller to fill.
function writeValue(value: unknown, out: string[]): Writing | undefined {
  switch (typeof value) {
    case 'string':
      out.push(quote(value))
      return undefined
    case 'boolean':
      out.push(value ? 'true' : 'false')
      return undefined
    case 'bigint':
      out.push(value.toString())
      return undefined
    case 'number':
      out.push(formatNumber(value))
      return undefined
    case 'object':
      if (value === null) {
        out.push('null')
        return undefined
      }

      return writeObject(value, out)
    default:
      throw new EvtError('invalidArgument', `cannot encode a ${typeof value} as JSON`)
  }
}

function writeObject(value: object, out: string[]): Writing | undefined {
  if (value instanceof JsonNumber || value instanceof RawJson) {
    out.push(value.text)
    return undefined
  }
  if (value instanceof Uint8Array) {
    out.push('"', base64Encode(value), '"')
    return undefined
  }
  if (Array.isArray(value)) {
    out.push('[')
    return {value, entries: arrayEntries(value), next: 0, close: ']'}
  }

  const entries = value instanceof Map ? mapEntries(value) : structEntries(value)
  out.push('{')

  return {value, entries, next: 0, close: '}'}
}

function arrayEntries(value: unknown[]): [undefined, unknown][] {
  return Array.from(value, (entry) => {
    if (entry === undefined) {
      throw new EvtError('invalidArgument', 'cannot encode undefined in an array as JSON')
    }

    return [undefined, entry]
  })
}

function mapEntries(value: Map<unknown, unknown>): [string, unknown][] {
  const entries: [string, unknown][] = []
  const seen = new Set<string>()
  for (const [key, entry] of value) {
    const text = mapKey(key)
    if (seen.has(text)) {
      throw new EvtError('invalidArgument', `map key ${text} appears twice once written as text`)
    }

    seen.add(text)
    entries.push([text, entry])
  }

  entries.sort(([a], [b]) => compareUtf8(a, b))

  return entries
}

function mapKey(key: unknown): string {
  if (typeof key === 'string') {
    return key
  }
  if (typeof key === 'bigint' || (typeof key === 'number' && Number.isSafeInteger(key))) {
    return key.toString()
  }

  throw new EvtError('invalidArgument', 'map keys must be strings or integers')
}

function structEntries(value: object): [string, unknown][] {
  const prototype: unknown = Object.getPrototypeOf(value)
  if (prototype !== Object.prototype && prototype !== null) {
    const name = value.constructor.name || 'object'
    throw new EvtError('invalidArgument', `cannot encode a ${name} as JSON`)
  }

  const entries: [string, unknown][] = []
  for (const [key, entry] of Object.entries(value)) {
    if (isArrayIndex(key)) {
      throw new EvtError(
        'invalidArgument',
        `object key "${key}" is integer-like, so its order is lost: use a Map`,
      )
    }
    if (entry !== undefined) {
      entries.push([key, entry])
    }
  }

  return entries
}

// The keys an engine enumerates first, in numeric order: canonical integers below 2^32 - 1.
function isArrayIndex(key: string): boolean {
  return /^(?:0|[1-9]\d{0,9})$/.test(key) && Number(key) < 4294967295
}

function formatNumber(value: number): string {
  if (!Number.isFinite(value)) {
    throw new EvtError('invalidArgument', `cannot encode ${value} as JSON`)
  }

  // Both write the shortest text that reads back, with an exponent below 1e-6 and from 1e21. A
  // number is usually an integer in a payload, so negative zero (from 0 * -1, say) is written as
  // 0, as a Go int is, not as the -0 of a Go float64.
  return value.toString()
}

const SHORT_ESCAPES: Record<number, string> = {
  0x08: '\\b',
  0x09: '\\t',
  0x0a: '\\n',
  0x0c: '\\f',
  0x0d: '\\r',
  0x22: '\\"',
  0x5c: '\\\\',
}

// <, > and &, which Go escapes so the output is safe in HTML, and the two line separators.
const HTML_ESCAPES = new Set([0x26, 0x3c, 0x3e, 0x2028, 0x2029])

/** Quotes a string with Go's escaping. */
export function quote(text: string): string {
  let out = '"'
  let start = 0
  for (let i = 0; i < text.length; i++) {
    const unit = text.charCodeAt(i)
    let escaped: string
    if (isHighSurrogate(unit) && isLowSurrogate(text.charCodeAt(i + 1))) {
      i++
      continue
    } else if (unit >= 0xd800 && unit <= 0xdfff) {
      escaped = '\\ufffd'
    } else if (unit < 0x20 || unit === 0x22 || unit === 0x5c || HTML_ESCAPES.has(unit)) {
      escaped = SHORT_ESCAPES[unit] ?? `\\u${unit.toString(16).padStart(4, '0')}`
    } else {
      continue
    }

    out += text.slice(start, i) + escaped
    start = i + 1
  }

  return out + text.slice(start) + '"'
}

// Go's decoder refuses documents nested deeper than this. The parser keeps its own stack, so a
// deep document is refused cleanly instead of overflowing the call stack.
const MAX_DEPTH = 10_000

/** How {@link parseJson} treats an object that repeats a key. */
export interface ParseOptions {
  /**
   * `last` (the default) keeps the key's first place and its last value. `refuse` refuses the
   * document, which is how evt reads its own documents: Go merges a repeated key into the earlier
   * value and Rust refuses a repeated field, so no reading of one matches both.
   */
  duplicateKeys?: 'last' | 'refuse'
}

/**
 * Parses JSON without losing anything `JSON.parse` loses. Objects come back as Maps in written
 * order, and numbers come back as {@link JsonNumber}. An escaped lone surrogate reads as U+FFFD,
 * as it does in Go.
 */
export function parseJson(text: string, options: ParseOptions = {}): JsonValue {
  const parser = new Parser(text, options.duplicateKeys === 'refuse')
  parser.skipWhitespace()
  const value = parser.value()
  parser.skipWhitespace()
  if (parser.pos < text.length) {
    throw parser.error('unexpected text after the value')
  }

  return value
}

// An array or object being read, and for an object the key its next value goes under.
type Open = {array: JsonValue[]} | {object: Map<string, JsonValue>; key: string}

class Parser {
  pos = 0

  constructor(
    private readonly text: string,
    private readonly refuseDuplicates: boolean,
  ) {}

  value(): JsonValue {
    const open: Open[] = []
    for (;;) {
      let value: JsonValue
      const unit = this.text.charCodeAt(this.pos)
      if (unit === 0x7b || unit === 0x5b) {
        // { or [
        if (open.length >= MAX_DEPTH) {
          throw this.error('exceeded max depth')
        }

        this.pos++
        this.skipWhitespace()
        const empty = this.text.charCodeAt(this.pos) === unit + 2
        if (!empty) {
          open.push(unit === 0x5b ? {array: []} : {object: new Map(), key: this.key()})
          continue
        }

        this.pos++
        value = unit === 0x5b ? [] : new Map()
      } else {
        value = this.scalar(unit)
      }

      // Add the value to the innermost open container, and close each container that ends here.
      for (;;) {
        const top = open.at(-1)
        if (top === undefined) {
          return value
        }

        if ('array' in top) {
          top.array.push(value)
        } else if (this.refuseDuplicates && top.object.has(top.key)) {
          throw this.error(`the key "${top.key}" appears twice`)
        } else {
          top.object.set(top.key, value)
        }

        this.skipWhitespace()
        if (this.text.charCodeAt(this.pos) === 0x2c) {
          this.pos++
          this.skipWhitespace()
          if ('object' in top) {
            top.key = this.key()
          }
          break
        }

        if ('array' in top) {
          this.expect(0x5d, "',' or ']'")
          value = top.array
        } else {
          this.expect(0x7d, "',' or '}'")
          value = top.object
        }
        open.pop()
      }
    }
  }

  private scalar(unit: number): JsonValue {
    switch (unit) {
      case 0x22: // "
        return this.string()
      case 0x74: // t
        return this.literal('true', true)
      case 0x66: // f
        return this.literal('false', false)
      case 0x6e: // n
        return this.literal('null', null)
      default:
        return this.number()
    }
  }

  // An object key and its colon, leaving the position at the value.
  private key(): string {
    if (this.text.charCodeAt(this.pos) !== 0x22) {
      throw this.error('expected an object key')
    }

    const key = this.string()
    this.skipWhitespace()
    this.expect(0x3a, "':'")
    this.skipWhitespace()

    return key
  }

  private string(): string {
    let out = ''
    this.pos++
    let start = this.pos
    for (;;) {
      if (this.pos >= this.text.length) {
        throw this.error('unterminated string')
      }

      const unit = this.text.charCodeAt(this.pos)
      if (unit === 0x22) {
        out += this.text.slice(start, this.pos)
        this.pos++
        return out
      }
      if (unit < 0x20) {
        throw this.error('control character in string')
      }
      if (unit === 0x5c) {
        out += this.text.slice(start, this.pos) + this.escape()
        start = this.pos
        continue
      }
      if (unit >= 0xd800 && unit <= 0xdfff) {
        // A raw surrogate that isn't half of a pair is invalid UTF-8 to Go.
        if (isHighSurrogate(unit) && isLowSurrogate(this.text.charCodeAt(this.pos + 1))) {
          this.pos += 2
          continue
        }

        out += this.text.slice(start, this.pos) + '\ufffd'
        this.pos++
        start = this.pos
        continue
      }

      this.pos++
    }
  }

  private escape(): string {
    const unit = this.text.charCodeAt(this.pos + 1)
    this.pos += 2
    switch (unit) {
      case 0x22:
        return '"'
      case 0x5c:
        return '\\'
      case 0x2f:
        return '/'
      case 0x62:
        return '\b'
      case 0x66:
        return '\f'
      case 0x6e:
        return '\n'
      case 0x72:
        return '\r'
      case 0x74:
        return '\t'
      case 0x75: {
        const high = this.hex()
        if (!isHighSurrogate(high)) {
          return isLowSurrogate(high) ? '\ufffd' : String.fromCharCode(high)
        }

        // A high surrogate pairs only with an escaped low surrogate right after it.
        if (this.text.startsWith('\\u', this.pos)) {
          const save = this.pos
          this.pos += 2
          const low = this.hex()
          if (isLowSurrogate(low)) {
            return String.fromCharCode(high, low)
          }

          this.pos = save
        }

        return '\ufffd'
      }
      default:
        this.pos -= 2
        throw this.error('invalid escape')
    }
  }

  private hex(): number {
    const digits = this.text.slice(this.pos, this.pos + 4)
    if (!/^[0-9a-fA-F]{4}$/.test(digits)) {
      throw this.error('invalid \\u escape')
    }

    this.pos += 4
    return Number.parseInt(digits, 16)
  }

  private number(): JsonNumber {
    const match = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y
    match.lastIndex = this.pos
    const found = match.exec(this.text)
    if (found === null) {
      throw this.error('unexpected character')
    }

    this.pos += found[0].length
    return new JsonNumber(found[0])
  }

  private literal<T>(word: string, value: T): T {
    if (!this.text.startsWith(word, this.pos)) {
      throw this.error('unexpected character')
    }

    this.pos += word.length
    return value
  }

  private expect(unit: number, what: string): void {
    if (this.text.charCodeAt(this.pos) !== unit) {
      throw this.error(`expected ${what}`)
    }

    this.pos++
  }

  skipWhitespace(): void {
    for (;;) {
      const unit = this.text.charCodeAt(this.pos)
      if (unit !== 0x20 && unit !== 0x09 && unit !== 0x0a && unit !== 0x0d) {
        return
      }

      this.pos++
    }
  }

  error(message: string): EvtError {
    const where = this.pos >= this.text.length ? 'at the end' : `at offset ${this.pos}`
    return new EvtError('invalidJson', `invalid JSON ${where}: ${message}`)
  }
}
