// Lenient decoding of engine documents by Go's rules: a missing field or null is the zero value,
// unknown fields are ignored, and a value of the wrong JSON type is an error. Field names match
// exactly, as in Rust (Go also matches them case-insensitively).

import {base64Decode} from './base64.js'
import {EvtError} from './errors.js'
import {JsonNumber, type JsonValue, type ParseOptions} from './json.js'

export type JsonObject = Map<string, JsonValue>

const INT64_MIN = -(2n ** 63n)

const INT64_MAX = 2n ** 63n - 1n

/** How evt parses its own documents: a repeated key is refused. */
export const DOCUMENT: ParseOptions = {duplicateKeys: 'refuse'}

export function invalid(message: string): EvtError {
  return new EvtError('invalidRecord', message)
}

/** An object, or null for JSON null. */
export function objectOrNull(value: JsonValue | undefined, what: string): JsonObject | null {
  if (value === undefined || value === null) {
    return null
  }
  if (!(value instanceof Map)) {
    throw invalid(`${what} is not an object`)
  }

  return value
}

/** A string field, `""` when missing or null. */
export function stringField(object: JsonObject, name: string, what: string): string {
  return nullableString(object, name, what) ?? ''
}

/** A string field that may be null (a Go `*string`). */
export function nullableString(object: JsonObject, name: string, what: string): string | null {
  const value = object.get(name)
  if (value === undefined || value === null) {
    return null
  }
  if (typeof value !== 'string') {
    throw invalid(`${what}.${name} is not a string`)
  }

  return value
}

/** An int64 field, `0n` when missing or null. `1.0`, `1e3` and `"1"` are refused, as in Go. */
export function int64Field(object: JsonObject, name: string, what: string): bigint {
  const value = object.get(name)
  if (value === undefined || value === null) {
    return 0n
  }

  return int64(value, `${what}.${name}`)
}

/** A small integer field, `0` when missing or null, refused if it isn't a safe integer. */
export function safeIntegerField(object: JsonObject, name: string, what: string): number {
  const value = int64Field(object, name, what)
  if (value < BigInt(Number.MIN_SAFE_INTEGER) || value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw invalid(`${what}.${name} is ${value}, past the integers this field holds`)
  }

  return Number(value)
}

/** Reads a JSON number as an int64. */
export function int64(value: JsonValue, what: string): bigint {
  if (!(value instanceof JsonNumber) || !value.isInteger) {
    throw invalid(`${what} is not an integer`)
  }

  return checkInt64(BigInt(value.text), what)
}

/** Refuses an integer outside int64. */
export function checkInt64(value: bigint, what: string): bigint {
  if (value < INT64_MIN || value > INT64_MAX) {
    throw invalid(`${what} is ${value}, outside int64`)
  }

  return value
}

/** Refuses to write an integer that no reader would read back as an int64. */
export function int64Argument(value: bigint, what: string): bigint {
  if (value < INT64_MIN || value > INT64_MAX) {
    throw new EvtError('invalidArgument', `${what} is ${value}, outside int64`)
  }

  return value
}

/** Refuses to write a small integer that isn't a safe integer, such as 1.5 or NaN. */
export function safeIntegerArgument(value: number, what: string): number {
  if (!Number.isSafeInteger(value)) {
    throw new EvtError('invalidArgument', `${what} is ${value}, not a safe integer`)
  }

  return value
}

/** A base64 field (a Go `[]byte`), null when missing or null. */
export function bytesField(object: JsonObject, name: string, what: string): Uint8Array | null {
  const text = nullableString(object, name, what)

  return text === null ? null : base64Decode(text)
}

/**
 * A Go `map[string]string`: null for a missing or null map, and `""` for a null value.
 */
export function stringMap(value: JsonValue | undefined, what: string): Map<string, string> | null {
  const object = objectOrNull(value, what)
  if (object === null) {
    return null
  }

  const map = new Map<string, string>()
  for (const [key, entry] of object) {
    if (entry !== null && typeof entry !== 'string') {
      throw invalid(`${what}.${key} is not a string`)
    }

    map.set(key, entry ?? '')
  }

  return map
}
