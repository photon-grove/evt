// Journal wake-ups: a views-table stream record that says a journal scope has new rows.
//
// The journal's rows (`journal#<scope>` / 20-digit position) and counters
// (`journal-counter#<scope>` / `COUNTER`) are view rows in the views table, written in the commit
// (see the storage format spec, "Journal"). A consumer turns on the table's stream and filters it to
// those keys with JournalFilter; the records it then receives are wake-ups, not deliveries. Stream
// records are ordered per item, not per scope, so a consumer reads the scope from its own cursor:
// the page is ordered and reports gaps, and a redelivered or coalesced wake-up only causes a read
// that finds nothing new.
//
// Wake-ups decode from the record's keys alone, so they work with every stream view type,
// `KEYS_ONLY` included. They also name no table: the same helpers work unchanged on a stream of a
// dedicated journal table, should one hold the rows.

import {type PatternField, StringRule, encodePattern} from './filter.js'
import {
  COUNTER_PARTITION_PREFIX,
  COUNTER_SORT_KEY,
  ROW_PARTITION_PREFIX,
  isScope,
  parseRowSortKey,
  readCounter,
} from './journal-keys.js'
import {
  type StreamRecord,
  eventName,
  itemFromStream,
  recordKey,
  recordPk,
  sequenceNumber,
  stringOf,
} from './record.js'

/** A scope with new rows, from one stream record (or, coalesced, several). */
export interface JournalWakeup {
  /** The scope. */
  readonly scope: string
  /**
   * The highest position the record shows: the row's position, or the counter's value when the
   * record has a new image. Undefined for a counter record without one (`KEYS_ONLY`), or whose image
   * does not decode. The scope may hold more rows than this by the time the consumer reads it.
   */
  readonly position: bigint | undefined
  /** The sequence number of the (first) record that woke the scope. */
  readonly sequenceNumber: string
}

/**
 * Decodes the journal wake-up a record carries, or undefined if it carries none.
 *
 * A wake-up is an `INSERT` or a `MODIFY` of a journal row (`journal#<scope>` at a 20-digit
 * position) or a journal counter (`journal-counter#<scope>` at `COUNTER`). Everything else is
 * undefined: other view rows, removals (a compaction deletes rows; that is not news), and keys in
 * those partitions that the journal never writes (a scope that is not valid, any other sort key).
 * Decoding never throws, so no record of the views table can stop a shard: a corrupt journal is
 * reported where it is read, by the page the consumer reads (a gap, or an invalid row). A counter
 * whose image does not decode still wakes its scope, with no position.
 */
export function decodeWakeup(record: StreamRecord): JournalWakeup | undefined {
  const name = eventName(record)
  if (name !== 'INSERT' && name !== 'MODIFY') {
    return undefined
  }

  const pk = recordPk(record)
  const sk = stringOf(recordKey(record, 'sk'))
  if (pk === undefined || sk === undefined) {
    return undefined
  }

  let scope: string
  let position: bigint | undefined
  if (pk.startsWith(ROW_PARTITION_PREFIX)) {
    scope = pk.slice(ROW_PARTITION_PREFIX.length)
    position = parseRowSortKey(sk)
    if (!isScope(scope) || position === undefined) {
      return undefined
    }
  } else if (pk.startsWith(COUNTER_PARTITION_PREFIX) && sk === COUNTER_SORT_KEY) {
    scope = pk.slice(COUNTER_PARTITION_PREFIX.length)
    if (!isScope(scope)) {
      return undefined
    }

    position = counterPosition(scope, record)
  } else {
    return undefined
  }

  return {scope, position, sequenceNumber: sequenceNumber(record)}
}

function counterPosition(scope: string, record: StreamRecord): bigint | undefined {
  const image = record.dynamodb?.NewImage
  if (image == null) {
    return undefined
  }

  try {
    return readCounter(scope, itemFromStream(image))
  } catch {
    return undefined
  }
}

/**
 * Merges wake-ups by scope: one per scope, in the order the scopes first appear, with the highest
 * position seen and the first record's sequence number.
 */
export function coalesceWakeups(wakeups: Iterable<JournalWakeup>): JournalWakeup[] {
  return coalesceIndexed(Array.from(wakeups, (wakeup) => [0, wakeup] as const)).map(
    ([, wakeup]) => wakeup,
  )
}

/** {@link coalesceWakeups}, keeping the batch index of each scope's first record. */
export function coalesceIndexed(
  wakeups: Iterable<readonly [number, JournalWakeup]>,
): [number, JournalWakeup][] {
  const merged: [number, JournalWakeup][] = []
  const atScope = new Map<string, number>()

  for (const [index, wakeup] of wakeups) {
    const at = atScope.get(wakeup.scope)
    if (at === undefined) {
      atScope.set(wakeup.scope, merged.length)
      merged.push([index, wakeup])
      continue
    }

    const entry = merged[at]!
    entry[1] = {...entry[1], position: maxPosition(entry[1].position, wakeup.position)}
  }

  return merged
}

// The higher of two positions, where a missing one is lower than any.
function maxPosition(a: bigint | undefined, b: bigint | undefined): bigint | undefined {
  if (a === undefined) {
    return b
  }
  if (b === undefined) {
    return a
  }

  return a > b ? a : b
}

/**
 * Selects journal wake-ups in a views-table stream: the records of journal rows, or of journal
 * counters, optionally of some scopes only.
 *
 * A record matches when it is an `INSERT` or a `MODIFY` whose key `pk` is in the selected
 * partitions. The pattern reads only `Keys`, so a `KEYS_ONLY` stream is enough.
 *
 * - {@link JournalFilter.counters} wakes once per scope per commit (the commit puts each touched
 *   scope's counter once), which is the fewest invocations. Records carry the counter's value when
 *   the stream writes new images.
 * - {@link JournalFilter.rows} wakes once per row, and each record names the row's position.
 *
 * A filter is immutable: each method returns a new one.
 *
 * ```ts
 * JournalFilter.counters().pattern()
 * // {"eventName":["INSERT","MODIFY"],"dynamodb":{"Keys":{"pk":{"S":[{"prefix":"journal-counter#"}]}}}}
 *
 * JournalFilter.rows().scope('world:7').scopePrefix('world:7/').pattern()
 * // {"eventName":["INSERT","MODIFY"],"dynamodb":{"Keys":{"pk":{"S":["journal#world:7",{"prefix":"journal#world:7/"}]}}}}
 * ```
 */
export class JournalFilter {
  readonly #partitionPrefix: string
  readonly #pk: StringRule

  private constructor(partitionPrefix: string, pk: StringRule) {
    this.#partitionPrefix = partitionPrefix
    this.#pk = pk
  }

  /** Wake-ups from journal rows (`journal#<scope>`). */
  static rows(): JournalFilter {
    return new JournalFilter(ROW_PARTITION_PREFIX, new StringRule())
  }

  /** Wake-ups from journal counters (`journal-counter#<scope>`). */
  static counters(): JournalFilter {
    return new JournalFilter(COUNTER_PARTITION_PREFIX, new StringRule())
  }

  /**
   * Also matches these scopes. A scope is 1 to 512 bytes of UTF-8 without U+0000; another one
   * throws a `RangeError`.
   */
  scope(...scopes: string[]): JournalFilter {
    let pk = this.#pk
    for (const scope of scopes) {
      if (!isScope(scope)) {
        throw new RangeError(
          `a journal scope is 1 to 512 bytes of UTF-8 without U+0000: ${JSON.stringify(scope.slice(0, 32))}`,
        )
      }

      pk = pk.withValue(this.#partitionPrefix + scope)
    }

    return new JournalFilter(this.#partitionPrefix, pk)
  }

  /** Also matches every scope that starts with `prefix`, such as `world:7/`. */
  scopePrefix(...prefixes: string[]): JournalFilter {
    return new JournalFilter(
      this.#partitionPrefix,
      prefixes.reduce((pk, prefix) => pk.withPrefix(this.#partitionPrefix + prefix), this.#pk),
    )
  }

  #rule(): StringRule {
    return this.#pk.isAny() ? new StringRule([], [this.#partitionPrefix]) : this.#pk
  }

  /**
   * The event source mapping filter pattern (`Filters[].Pattern`). Deterministic text. Throws a
   * `FilterError` for a pattern Lambda would refuse as too long.
   */
  pattern(): string {
    const keys: PatternField[] = [['pk', this.#rule()]]

    return encodePattern(['INSERT', 'MODIFY'], keys, [])
  }

  /** Whether the record matches, by the rule the pattern states. */
  matches(record: StreamRecord): boolean {
    const name = eventName(record)
    const keys = record.dynamodb?.Keys

    return (
      (name === 'INSERT' || name === 'MODIFY') &&
      keys != null &&
      this.#rule().matchesAttribute(keys, 'pk')
    )
  }
}
