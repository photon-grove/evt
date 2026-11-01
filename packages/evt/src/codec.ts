// Payload codecs: how an application turns event data and aggregate state into stored bytes and
// back. evt stores the bytes verbatim, so a codec owns their format.

import {EvtError} from './errors.js'
import {JsonNumber, type JsonValue, parseJson, stringifyJson} from './json.js'
import {utf8Decode, utf8Encode} from './utf8.js'

/** Encodes values of `T` to stored bytes and decodes them back. */
export interface Codec<T> {
  encode(value: T): Uint8Array
  decode(bytes: Uint8Array): T
}

/**
 * The part of a [Standard Schema](https://standardschema.dev) validator evt calls, so Zod,
 * Valibot, ArkType and the rest work without evt depending on any of them.
 */
export interface StandardSchemaV1<Input = unknown, Output = Input> {
  readonly '~standard': {
    readonly version: 1
    readonly vendor: string
    readonly validate: (value: unknown) => StandardResult<Output> | Promise<StandardResult<Output>>
    readonly types?: {readonly input: Input; readonly output: Output} | undefined
  }
}

/** A Standard Schema validation result: the value, or the issues that refused it. */
export type StandardResult<Output> =
  | {readonly value: Output; readonly issues?: undefined}
  | {readonly issues: readonly {readonly message: string}[]}

/** Options for {@link json}. */
export interface JsonOptions<T> {
  /** Validates each value before it's encoded and after it's decoded, synchronously. */
  schema?: StandardSchemaV1<unknown, T> | undefined
}

/**
 * A JSON codec written with evt's encoder (the one Go's `encoding/json` output is reproduced
 * with). Plain objects encode in property order.
 *
 * Decoding gives plain objects, arrays and numbers, so `T` must be plain JSON: a `Map`, a `bigint`
 * or a `Date` doesn't decode back to itself. A domain that must match a Go or Rust one byte for
 * byte, with a map whose keys are written sorted, needs a codec of its own that decodes the map as
 * a `Map`. An integer past `Number.MAX_SAFE_INTEGER` is refused rather than rounded.
 *
 * With a `schema`, each value is validated before it's encoded and after it's decoded, so an
 * event or a snapshot the schema refuses fails its commit instead of every later load. The schema
 * must not transform values: the value is encoded as given.
 */
export function json<T>(options: JsonOptions<T> = {}): Codec<T> {
  const schema = options.schema

  return {
    encode(value) {
      if (schema) {
        validate(schema, value, 'invalidArgument')
      }

      return utf8Encode(stringifyJson(value))
    },
    decode(bytes) {
      const value = plainJson(parseJson(utf8Decode(bytes, 'payload')))

      return schema ? validate(schema, value, 'invalidRecord') : (value as T)
    },
  }
}

/**
 * A JSON value as plain JavaScript: objects as plain objects, numbers as `number`. An integer past
 * the safe range is refused, because `number` would round it.
 */
export function plainJson(value: JsonValue): unknown {
  if (value instanceof JsonNumber) {
    const number = Number(value.text)
    if (value.isInteger && !Number.isSafeInteger(number)) {
      throw new EvtError(
        'invalidRecord',
        `the integer ${value.text} is past the integers a number holds; decode it with a codec ` +
          'of its own',
      )
    }

    return number
  }
  if (Array.isArray(value)) {
    return value.map(plainJson)
  }
  if (value instanceof Map) {
    const object: Record<string, unknown> = {}
    for (const [key, entry] of value) {
      // defineProperty, so a "__proto__" key is a plain property and never a prototype.
      Object.defineProperty(object, key, {
        value: plainJson(entry),
        enumerable: true,
        writable: true,
        configurable: true,
      })
    }

    return object
  }

  return value
}

function validate<T>(
  schema: StandardSchemaV1<unknown, T>,
  value: unknown,
  code: 'invalidArgument' | 'invalidRecord',
): T {
  const result = schema['~standard'].validate(value)
  if ('then' in result && typeof result.then === 'function') {
    throw new EvtError('invalidArgument', 'a codec schema must validate synchronously')
  }

  const settled = result as StandardResult<T>
  if (settled.issues) {
    const issues = settled.issues.map((issue) => issue.message).join('; ')
    throw new EvtError(code, `the payload does not match its schema: ${issues}`)
  }

  return settled.value
}
