// Structural equality and printing for test assertions, so /testing depends on no test framework.

/**
 * A failed assertion. It carries `actual` and `expected` the way Node's `assert` does, so runners
 * such as Vitest and Jest show a diff, and `node:test` prints both.
 */
export class AssertionError extends Error {
  override readonly name = 'AssertionError'
  readonly actual: unknown
  readonly expected: unknown

  constructor(message: string, actual?: unknown, expected?: unknown) {
    super(message)
    this.actual = actual
    this.expected = expected
  }
}

/**
 * Whether two values have the same structure: objects of the same prototype by their own
 * enumerable string and symbol keys (a key whose value is `undefined` counts as absent), arrays by
 * their elements, holes included, `Map`s by their entries and `Set`s by their members, buffers and
 * typed arrays by their bytes, dates by their time, regular expressions by their source and flags,
 * and everything else with `Object.is`. `Map` keys and `Set` members match by identity, and
 * `#private` fields aren't seen.
 */
export function deepEqual(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) {
    return true
  }
  if (typeof a !== 'object' || typeof b !== 'object' || a === null || b === null) {
    return false
  }
  if (Object.getPrototypeOf(a) !== Object.getPrototypeOf(b)) {
    return false
  }

  if (Array.isArray(a)) {
    const other = b as unknown[]
    if (a.length !== other.length) {
      return false
    }

    // An index loop, since every() skips holes.
    for (let i = 0; i < a.length; i++) {
      if (i in a !== i in other || !deepEqual(a[i], other[i])) {
        return false
      }
    }

    return true
  }
  if (a instanceof ArrayBuffer || ArrayBuffer.isView(a)) {
    const left = bytes(a)
    const right = bytes(b as ArrayBuffer | ArrayBufferView)

    return left.length === right.length && left.every((byte, i) => byte === right[i])
  }
  if (a instanceof Map) {
    const other = b as Map<unknown, unknown>

    return (
      a.size === other.size &&
      [...a].every(([key, entry]) => other.has(key) && deepEqual(entry, other.get(key)))
    )
  }
  if (a instanceof Set) {
    const other = b as Set<unknown>

    return a.size === other.size && [...a].every((entry) => other.has(entry))
  }
  if (a instanceof Date) {
    return Object.is(a.getTime(), (b as Date).getTime())
  }
  if (a instanceof RegExp) {
    return a.source === (b as RegExp).source && a.flags === (b as RegExp).flags
  }

  const left = definedKeys(a)
  const right = definedKeys(b)

  return (
    left.length === right.length &&
    left.every((key) => Object.hasOwn(b, key) && deepEqual(field(a, key), field(b, key)))
  )
}

function bytes(value: ArrayBuffer | ArrayBufferView): Uint8Array {
  return value instanceof ArrayBuffer
    ? new Uint8Array(value)
    : new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
}

// The own enumerable keys, symbols included, whose values aren't undefined.
function definedKeys(value: object): (string | symbol)[] {
  const symbols = Object.getOwnPropertySymbols(value).filter((key) =>
    Object.prototype.propertyIsEnumerable.call(value, key),
  )

  return [...Object.keys(value), ...symbols].filter((key) => field(value, key) !== undefined)
}

function field(value: object, key: string | symbol): unknown {
  return (value as Record<string | symbol, unknown>)[key]
}

/** Prints a value for an assertion message, with `bigint`s, `Map`s and byte arrays legible. */
export function show(value: unknown): string {
  if (typeof value === 'string') {
    return JSON.stringify(value)
  }
  if (typeof value === 'bigint') {
    return `${value}n`
  }
  if (Object.is(value, -0)) {
    return '-0'
  }
  if (typeof value !== 'object' || value === null) {
    return String(value)
  }

  if (Array.isArray(value)) {
    return `[${value.map(show).join(', ')}]`
  }
  if (value instanceof Uint8Array) {
    return `Uint8Array [${value.join(', ')}]`
  }
  if (value instanceof Map) {
    const entries = [...value].map(([key, entry]) => `${show(key)} => ${show(entry)}`)

    return `Map {${entries.join(', ')}}`
  }
  if (value instanceof Set) {
    return `Set {${[...value].map(show).join(', ')}}`
  }
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? 'Invalid Date' : `Date ${value.toISOString()}`
  }
  if (value instanceof Error) {
    return `${value.name}: ${value.message}`
  }

  const entries = Object.entries(value).map(([key, entry]) => `${show(key)}: ${show(entry)}`)

  return `{${entries.join(', ')}}`
}
