// Scopes, the journal's key layout, and the allocations an event carries in its metadata (storage
// format, "Journal" and "Allocations in event metadata").

import {EvtError} from '../errors.js'
import type {SerializedEvent} from '../event.js'
import {JsonNumber, type JsonValue, parseJson, quote} from '../json.js'
import type {Metadata} from '../metadata.js'
import {ENGINE_ALLOCATION_KEY} from '../projection.js'
import {compareUtf8, isHighSurrogate, isLowSurrogate, utf8Length} from '../utf8.js'

/** The metadata extension key an event's allocations are stored under. */
export const ALLOCATION_KEY = ENGINE_ALLOCATION_KEY

/** The longest scope, in UTF-8 bytes. */
export const MAX_SCOPE_BYTES = 512

/** The partition-key prefix of a scope's journal rows (`journal#<scope>`). */
export const ROW_PARTITION_PREFIX = 'journal#'

/** The partition-key prefix of a scope's counter row (`journal-counter#<scope>`). */
export const COUNTER_PARTITION_PREFIX = 'journal-counter#'

/** The sort key of a scope's counter row. */
export const COUNTER_SORT_KEY = 'COUNTER'

/**
 * The owner of every counter row: a scope's counter is shared by every stream that writes it, so
 * the engine owns it, from the reserved `SYSTEM#` namespace no stream can use.
 */
export const COUNTER_OWNER = 'SYSTEM#JOURNAL'

/** The entity type of a journal row. */
export const ROW_ENTITY_TYPE = 'journal'

/** The entity type of a counter row. */
export const COUNTER_ENTITY_TYPE = 'journal-counter'

/** The width of a row's sort key: its position in zero-padded decimal. */
export const POSITION_WIDTH = 20

/** The largest position, a u64's. */
export const MAX_POSITION = (1n << 64n) - 1n

/**
 * Checks a scope: 1 to {@link MAX_SCOPE_BYTES} bytes of UTF-8 without U+0000 or a lone surrogate.
 * The engine treats it as opaque. Returns it, or throws `invalidJournal`.
 */
export function checkScope(scope: string): string {
  if (typeof scope !== 'string' || scope === '') {
    throw invalidJournal('a scope cannot be empty')
  }
  const bytes = utf8Length(scope)
  if (bytes > MAX_SCOPE_BYTES) {
    throw invalidJournal(
      `a scope is at most ${MAX_SCOPE_BYTES} bytes; ${quote([...scope].slice(0, 32).join(''))}... ` +
        `is ${bytes}`,
    )
  }
  if (scope.includes('\0')) {
    throw invalidJournal('a scope cannot contain U+0000')
  }
  if (hasLoneSurrogate(scope)) {
    throw invalidJournal('a scope cannot contain a lone surrogate')
  }

  return scope
}

/** The partition key of a scope's journal rows. */
export function rowPartitionKey(scope: string): string {
  return ROW_PARTITION_PREFIX + scope
}

/** The partition key of a scope's counter row. */
export function counterPartitionKey(scope: string): string {
  return COUNTER_PARTITION_PREFIX + scope
}

/** The sort key of the row at `position`: 20 digits, zero-padded, so bytes order as numbers. */
export function rowSortKey(position: bigint): string {
  return position.toString().padStart(POSITION_WIDTH, '0')
}

/** A row sort key's position: null unless it is exactly 20 digits of a u64. */
export function parseRowSortKey(sk: string): bigint | null {
  if (!/^\d{20}$/.test(sk)) {
    return null
  }
  const position = BigInt(sk)

  return position <= MAX_POSITION ? position : null
}

/** One position an event holds in one scope. */
export interface Allocation {
  readonly scope: string
  /** The event's position in the scope, from 1. */
  readonly position: bigint
}

/**
 * Encodes allocations as the metadata extension value: a JSON array of `{scope, position}`
 * objects, in the order given (an event's are ascending by scope bytes).
 */
export function encodeAllocations(list: readonly Allocation[]): string {
  const items = list.map(({scope, position}) => `{"scope":${quote(scope)},"position":${position}}`)

  return `[${items.join(',')}]`
}

/**
 * Stores an event's allocations in its metadata, replacing whatever the key held; an empty list
 * removes the key. The event's metadata is replaced, never mutated in place.
 */
export function setAllocations(event: SerializedEvent, list: readonly Allocation[]): void {
  const extensions = new Map(event.metadata.extensions)
  if (list.length === 0) {
    extensions.delete(ALLOCATION_KEY)
  } else {
    extensions.set(ALLOCATION_KEY, encodeAllocations(list))
  }
  event.metadata = {...event.metadata, extensions}
}

/**
 * The allocations stored in an event's metadata, or none for an event that wasn't journaled. The
 * list must be canonical (ascending by scope bytes, no scope twice, positions from 1), or this
 * throws `invalidJournal`: a reader never guesses.
 */
export function allocations(metadata: Metadata): Allocation[] {
  const text = metadata.extensions.get(ALLOCATION_KEY)
  if (text === undefined) {
    return []
  }

  // An escaped lone surrogate would parse as U+FFFD, which Rust refuses: refuse it too.
  if (LONE_SURROGATE_ESCAPE.test(text)) {
    throw invalidJournal(`the ${ALLOCATION_KEY} metadata escapes a lone surrogate`)
  }
  let parsed: JsonValue
  try {
    parsed = parseJson(text, {duplicateKeys: 'refuse'})
  } catch (error) {
    throw invalidJournal(`the ${ALLOCATION_KEY} metadata: ${messageOf(error)}`)
  }
  if (!Array.isArray(parsed)) {
    throw invalidJournal(`the ${ALLOCATION_KEY} metadata is not an array`)
  }

  const list = parsed.map((item) => {
    const fields = structFields(item, ['scope', 'position'])
    const [scope, position] = fields ?? []
    const value = position === undefined ? null : u64(position)
    if (typeof scope !== 'string' || value === null) {
      throw invalidJournal(`the ${ALLOCATION_KEY} metadata holds a malformed allocation`)
    }
    checkScope(scope)
    if (value === 0n) {
      throw invalidJournal(`scope ${quote(scope)} has position 0; positions start at 1`)
    }

    return {scope, position: value}
  })
  for (let index = 1; index < list.length; index++) {
    const [before, after] = [list[index - 1]!, list[index]!]
    if (compareUtf8(before.scope, after.scope) >= 0) {
      throw invalidJournal(
        `allocations must ascend by scope with no repeat; ${quote(before.scope)} is followed by ` +
          quote(after.scope),
      )
    }
  }

  return list
}

/**
 * A struct's fields, as Rust's serde reads one: from an object, by name in any order, ignoring
 * other keys (a missing one is undefined), or from an array of exactly the fields in order. Null
 * for any other value.
 */
export function structFields(value: JsonValue, names: readonly string[]): JsonValue[] | null {
  if (value instanceof Map) {
    if (names.some((name) => !value.has(name))) {
      return null
    }

    return names.map((name) => value.get(name)!)
  }
  if (Array.isArray(value) && value.length === names.length) {
    return value
  }

  return null
}

/** A JSON number as a u64, or null when it isn't one (negative, fractional, an exponent, too big). */
export function u64(value: JsonValue): bigint | null {
  if (!(value instanceof JsonNumber) || !/^\d+$/.test(value.text)) {
    return null
  }
  const n = BigInt(value.text)

  return n <= MAX_POSITION ? n : null
}

/** An `invalidJournal` error. */
export function invalidJournal(message: string): EvtError {
  return new EvtError('invalidJournal', `invalid journal: ${message}`)
}

// A \u escape (after an even run of backslashes, so not an escaped backslash) of a high surrogate
// not followed by one of a low surrogate, or of a low surrogate not preceded by one of a high one.
const LONE_SURROGATE_ESCAPE =
  /(?<!\\)(?:\\\\)*\\u[dD][89abAB][0-9a-fA-F]{2}(?!\\u[dD][c-fC-F][0-9a-fA-F]{2})|(?<!\\)(?:\\\\)*(?<!\\u[dD][89abAB][0-9a-fA-F]{2})\\u[dD][c-fC-F][0-9a-fA-F]{2}/

function hasLoneSurrogate(text: string): boolean {
  for (let i = 0; i < text.length; i++) {
    const unit = text.charCodeAt(i)
    if (isHighSurrogate(unit) && isLowSurrogate(text.charCodeAt(i + 1))) {
      i++
    } else if (isHighSurrogate(unit) || isLowSurrogate(unit)) {
      return true
    }
  }

  return false
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
