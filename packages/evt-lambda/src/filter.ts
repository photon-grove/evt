// Event source mapping filter patterns, and the same test applied in the handler.
//
// A Lambda event source mapping can drop records before they reach the function: its filter
// criteria hold JSON patterns over each record, and only records that match a pattern invoke the
// function. EventFilter writes such a pattern for an event-log table's stream (`pattern()`), and
// `matches()` applies the same test to a record in the handler, so a function stays correct when
// its mapping has no filter, or an older one.
//
// Patterns use only the rules Lambda supports for DynamoDB records: string equality, `prefix`, and
// `exists`, over `eventName` and the `S` values of `Keys` and `NewImage`. Numbers in DynamoDB JSON
// are strings, so numeric rules never match them, and none are used.

import {type SerializedEvent, compareUtf8, utf8Length} from '@photon-grove/evt'

import {FilterError} from './errors.js'
import {type StreamItem, type StreamRecord, eventName, ownAttribute, stringOf} from './record.js'

/** The longest filter pattern Lambda accepts, in bytes. */
export const MAX_PATTERN_BYTES = 4096

type Matcher = string | {prefix: string} | {exists: true}

/** One string attribute's rule: any of some values or prefixes, or, with neither, present. */
export class StringRule {
  readonly values: ReadonlySet<string>
  readonly prefixes: ReadonlySet<string>

  constructor(values: Iterable<string> = [], prefixes: Iterable<string> = []) {
    this.values = new Set(values)
    this.prefixes = new Set(prefixes)
  }

  withValue(value: string): StringRule {
    return new StringRule([...this.values, value], this.prefixes)
  }

  withPrefix(prefix: string): StringRule {
    return new StringRule(this.values, [...this.prefixes, prefix])
  }

  isAny(): boolean {
    return this.values.size === 0 && this.prefixes.size === 0
  }

  matches(value: string): boolean {
    if (this.isAny() || this.values.has(value)) {
      return true
    }

    for (const prefix of this.prefixes) {
      if (value.startsWith(prefix)) {
        return true
      }
    }

    return false
  }

  matchesAttribute(item: StreamItem, name: string): boolean {
    const value = stringOf(ownAttribute(item, name))

    return value !== undefined && this.matches(value)
  }

  check(what: string): void {
    for (const value of [...this.values, ...this.prefixes]) {
      if (value === '') {
        throw FilterError.empty(what)
      }
    }
  }

  // Values, then prefixes, each in byte order, as the Rust crate's sorted sets write them.
  field(): {S: Matcher[]} {
    if (this.isAny()) {
      return {S: [{exists: true}]}
    }

    return {
      S: [...sorted(this.values), ...sorted(this.prefixes).map((prefix): Matcher => ({prefix}))],
    }
  }
}

function sorted(values: ReadonlySet<string>): string[] {
  const list = [...values]
  list.sort(compareUtf8)

  return list
}

/** A field of a pattern: an attribute name and its rule. */
export type PatternField = readonly [name: string, rule: StringRule]

/**
 * Encodes a pattern over the record's event name and the string attributes of its keys and new
 * image, each object's fields in byte order, refusing one longer than {@link MAX_PATTERN_BYTES}.
 */
export function encodePattern(
  eventNames: readonly string[],
  keys: readonly PatternField[],
  newImage: readonly PatternField[],
): string {
  const dynamodb: Record<string, Record<string, {S: Matcher[]}>> = {}
  if (keys.length > 0) {
    dynamodb['Keys'] = fieldMap(keys)
  }
  if (newImage.length > 0) {
    dynamodb['NewImage'] = fieldMap(newImage)
  }

  // JSON.stringify escapes exactly what serde_json does for well-formed strings, so the text is the
  // Rust crate's byte for byte.
  const encoded = JSON.stringify({eventName: eventNames, dynamodb})
  const length = utf8Length(encoded)
  if (length > MAX_PATTERN_BYTES) {
    throw FilterError.tooLong(length, MAX_PATTERN_BYTES)
  }

  return encoded
}

function fieldMap(fields: readonly PatternField[]): Record<string, {S: Matcher[]}> {
  const ordered = [...fields]
  ordered.sort(([a], [b]) => compareUtf8(a, b))

  const map: Record<string, {S: Matcher[]}> = {}
  for (const [name, rule] of ordered) {
    map[name] = rule.field()
  }

  return map
}

const ANY = new StringRule()

/**
 * Selects the committed events of an event-log table's stream, by event type and entity type.
 *
 * A record matches when it is an `INSERT` whose new image has a string `metadata` (only event items
 * have one: heads, snapshots, the feature marker, the rebuild fence, and outcomes do not), a `type`
 * that is one of the event types or starts with one of the prefixes, and an `entityType` that is
 * one of the entity types. An empty set of event types and prefixes, or of entity types, matches
 * any. The table's stream must write new images (`NEW_IMAGE` or `NEW_AND_OLD_IMAGES`): on a
 * `KEYS_ONLY` or `OLD_IMAGE` stream the pattern matches nothing, so the mapping drops every record
 * without an error. (`decodeEvent` without a mapping filter throws a `missingNewImage`
 * `DecodeError` instead.)
 *
 * A filter is immutable: each method returns a new one.
 *
 * ```ts
 * const filter = new EventFilter()
 *   .entityType('account')
 *   .eventType('account:opened')
 *   .eventTypePrefix('account:closed')
 *
 * filter.pattern()
 * // {"eventName":["INSERT"],"dynamodb":{"NewImage":{"entityType":{"S":["account"]},
 * //  "metadata":{"S":[{"exists":true}]},"type":{"S":["account:opened",{"prefix":"account:closed"}]}}}}
 * ```
 */
export class EventFilter {
  #eventType = ANY
  #entityType = ANY

  #with(eventType: StringRule, entityType: StringRule): EventFilter {
    const next = new EventFilter()
    next.#eventType = eventType
    next.#entityType = entityType

    return next
  }

  /** Also matches events of these types. */
  eventType(...eventTypes: string[]): EventFilter {
    return this.#with(
      eventTypes.reduce((rule, eventType) => rule.withValue(eventType), this.#eventType),
      this.#entityType,
    )
  }

  /** Also matches events whose type starts with `prefix`, such as `account:`. */
  eventTypePrefix(...prefixes: string[]): EventFilter {
    return this.#with(
      prefixes.reduce((rule, prefix) => rule.withPrefix(prefix), this.#eventType),
      this.#entityType,
    )
  }

  /** Also matches events of streams of these entity types. */
  entityType(...entityTypes: string[]): EventFilter {
    return this.#with(
      this.#eventType,
      entityTypes.reduce((rule, entityType) => rule.withValue(entityType), this.#entityType),
    )
  }

  /**
   * The event source mapping filter pattern: one entry of the mapping's `FilterCriteria`
   * (`Filters[].Pattern`). The text is deterministic, so it can be compared with the deployed one.
   * Throws a {@link FilterError} for an empty value or a pattern Lambda would refuse as too long.
   */
  pattern(): string {
    this.#eventType.check('event type')
    this.#entityType.check('entity type')

    const image: PatternField[] = [
      ['metadata', ANY],
      ['type', this.#eventType],
    ]
    if (!this.#entityType.isAny()) {
      image.push(['entityType', this.#entityType])
    }

    return encodePattern(['INSERT'], [], image)
  }

  /** Whether the record matches, by the rule the pattern states. */
  matches(record: StreamRecord): boolean {
    const image = record.dynamodb?.NewImage
    if (image == null) {
      return false
    }

    return (
      eventName(record) === 'INSERT' &&
      ANY.matchesAttribute(image, 'metadata') &&
      this.#eventType.matchesAttribute(image, 'type') &&
      (this.#entityType.isAny() || this.#entityType.matchesAttribute(image, 'entityType'))
    )
  }

  /** Whether a decoded event matches the filter's event and entity types. */
  matchesEvent(event: SerializedEvent): boolean {
    return this.#eventType.matches(event.type) && this.#entityType.matches(event.entityType)
  }
}
