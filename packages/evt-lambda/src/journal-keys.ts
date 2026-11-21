// The journal's key layout and counter payload (storage format, "Journal"), as much of it as a
// wake-up reads. The TypeScript journal itself is not ported yet; when `@photon-grove/evt/journal`
// lands, these move there and this package imports them.

import {JsonNumber, parseJson, utf8Length} from '@photon-grove/evt'
import {type Item} from '@photon-grove/evt/dynamodb'

/** The partition-key prefix of a scope's journal rows (`journal#<scope>`). */
export const ROW_PARTITION_PREFIX = 'journal#'

/** The partition-key prefix of a scope's counter row (`journal-counter#<scope>`). */
export const COUNTER_PARTITION_PREFIX = 'journal-counter#'

/** The sort key of a scope's counter row. */
export const COUNTER_SORT_KEY = 'COUNTER'

/** The longest scope, in UTF-8 bytes. */
export const MAX_SCOPE_BYTES = 512

/** The width of a row's sort key: a position in zero-padded decimal. */
export const POSITION_WIDTH = 20

const U64_MAX = (1n << 64n) - 1n

/** Whether a scope is valid: 1 to {@link MAX_SCOPE_BYTES} bytes of UTF-8 without U+0000. */
export function isScope(scope: string): boolean {
  const bytes = utf8Length(scope)

  return bytes > 0 && bytes <= MAX_SCOPE_BYTES && !scope.includes('\0')
}

/** The sort key of the row at `position`: 20 digits, zero-padded. */
export function rowSortKey(position: bigint): string {
  return position.toString().padStart(POSITION_WIDTH, '0')
}

/** Parses a row sort key back to its position: undefined unless it is exactly 20 digits of a u64. */
export function parseRowSortKey(sk: string): bigint | undefined {
  if (!/^\d{20}$/.test(sk)) {
    return undefined
  }

  const position = BigInt(sk)

  return position <= U64_MAX ? position : undefined
}

/**
 * Reads the position a counter item holds, checking it is the counter row of `scope`: undefined
 * when the item is not a well-formed view row, is another row, or its payload is not
 * `{"scope":…,"position":…}` naming the scope.
 */
export function readCounter(scope: string, item: Item): bigint | undefined {
  const row = viewRow(item)
  if (
    row === undefined ||
    row.pk !== COUNTER_PARTITION_PREFIX + scope ||
    row.sk !== COUNTER_SORT_KEY
  ) {
    return undefined
  }

  let payload
  try {
    payload = parseJson(row.payload, {duplicateKeys: 'refuse'})
  } catch {
    return undefined
  }

  // A struct reads from an object, or from an array of its fields in order.
  let named: unknown
  let position: unknown
  if (payload instanceof Map) {
    named = payload.get('scope')
    position = payload.get('position')
  } else if (Array.isArray(payload) && payload.length === 2) {
    ;[named, position] = payload
  } else {
    return undefined
  }

  if (named !== scope || !(position instanceof JsonNumber) || !/^\d+$/.test(position.text)) {
    return undefined
  }

  const value = BigInt(position.text)

  return value <= U64_MAX ? value : undefined
}

// A views-table item's text fields, as the Rust crate's view row decoder reads them: a missing or
// NULL attribute is empty, a string or number is its text, anything else is malformed, and a `ttl`
// must be an int64.
function viewRow(item: Item): {pk: string; sk: string; payload: string} | undefined {
  const fields: Record<string, string> = {}
  for (const name of ['pk', 'sk', 'entityID', 'entityType', 'payload']) {
    const value = Object.hasOwn(item, name) ? item[name] : undefined
    if (value === undefined || 'NULL' in value) {
      fields[name] = ''
    } else if ('S' in value) {
      fields[name] = value.S
    } else if ('N' in value) {
      fields[name] = value.N
    } else {
      return undefined
    }
  }

  const ttl = Object.hasOwn(item, 'ttl') ? item['ttl'] : undefined
  if (ttl !== undefined && !('NULL' in ttl)) {
    if (!('N' in ttl) || !isInt64(ttl.N)) {
      return undefined
    }
  }

  return {pk: fields['pk']!, sk: fields['sk']!, payload: fields['payload']!}
}

// An i64 as Rust's `str::parse` reads it: an optional sign, then at least one digit.
function isInt64(text: string): boolean {
  if (!/^[+-]?\d+$/.test(text)) {
    return false
  }

  const value = BigInt(text)

  return value >= -(1n << 63n) && value < 1n << 63n
}
