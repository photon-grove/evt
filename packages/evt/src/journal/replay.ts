// Rebuilding the journal from stored events. Every row and counter is a function of stored events
// alone, because an event's allocations are in its metadata, so the events may come in any order,
// from any number of streams, and the same rows and counters come out (storage format, "Rebuild").

import {type SerializedEvent, encodeEvent} from '../event.js'
import {compareUtf8} from '../utf8.js'
import type {ViewRow} from '../views.js'
import {counterView, rowView} from './row.js'
import {allocations, invalidJournal} from './scope.js'

/** The journal rows and counters a set of stored events implies. */
export interface JournalState {
  /** Every journal row, ascending by scope bytes, then position. */
  readonly rows: readonly {scope: string; position: bigint; row: ViewRow}[]
  /** Every counter, the highest position any event holds in its scope, ascending by scope bytes. */
  readonly counters: readonly {scope: string; position: bigint}[]
}

/**
 * Derives the journal's rows and counters from stored events, in any order. Each event gives one
 * row per allocation in its metadata, and raises its scope's counter to the position if higher.
 * Two different records holding one position of a scope never come from this journal, and fail
 * with `invalidJournal`, whatever their order; only an identical record given twice is one row.
 */
export function replay(events: Iterable<SerializedEvent>): JournalState {
  const claimed = new Map<string, {event: SerializedEvent; json: string}>()
  const rows: {scope: string; position: bigint; row: ViewRow}[] = []
  const counters = new Map<string, bigint>()

  for (const event of events) {
    for (const {scope, position} of allocations(event.metadata)) {
      // A key no scope can forge: a scope can't hold U+0000.
      const key = `${scope}\0${position}`
      const json = encodeEvent(event)
      const holder = claimed.get(key)
      if (holder !== undefined) {
        // The same stored event given twice is one row. Anything else at the position, even a
        // record with the same ID and allocation but another payload, is a different event.
        if (holder.json !== json) {
          throw invalidJournal(
            `position ${position} of scope ${scope} is held by two different records, ` +
              `${holder.event.id} and ${event.id}`,
          )
        }
        continue
      }

      claimed.set(key, {event, json})
      rows.push({scope, position, row: rowView(scope, position, event)})
      const counter = counters.get(scope) ?? 0n
      counters.set(scope, counter > position ? counter : position)
    }
  }

  rows.sort((a, b) => compareUtf8(a.scope, b.scope) || compare(a.position, b.position))
  const sorted = [...counters].map(([scope, position]) => ({scope, position}))
  sorted.sort((a, b) => compareUtf8(a.scope, b.scope))

  return {rows, counters: sorted}
}

/** The view rows a state writes: the journal rows in order, then the counters in order. */
export function stateViews(state: JournalState): ViewRow[] {
  return [
    ...state.rows.map(({row}) => row),
    ...state.counters.map(({scope, position}) => counterView(scope, position)),
  ]
}

function compare(a: bigint, b: bigint): number {
  return a < b ? -1 : a > b ? 1 : 0
}
