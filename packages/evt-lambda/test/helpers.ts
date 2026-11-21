// Shared test fixtures, and an evaluator of the Lambda filter patterns this package writes.
import {type SerializedEvent, emptyMetadata, stringifyJson} from '@photon-grove/evt'
import {eventItem} from '@photon-grove/evt/dynamodb'

import {
  type EventName,
  type StreamItem,
  type StreamRecord,
  streamItem,
  streamRecord,
} from '../src/record.ts'

export function event(entityType: string, eventType: string, sequence = 1n): SerializedEvent {
  return {
    id: `e-1:${sequence}`,
    sequence,
    type: eventType,
    version: 1,
    entityId: 'e-1',
    entityType,
    payload: new TextEncoder().encode('{}'),
    metadata: {...emptyMetadata(), commandId: 'cmd-1', causationId: 'cmd-1'},
  }
}

/** The keys of an item: its pk and sk. */
export function keysOf(item: StreamItem): StreamItem {
  return {pk: item['pk']!, sk: item['sk']!}
}

/** The insert of an event item, as a NEW_IMAGE stream writes it. */
export function eventRecord(serialized: SerializedEvent, sequence = '1'): StreamRecord {
  const item = streamItem(eventItem(serialized))

  return streamRecord('INSERT', keysOf(item), sequence, {newImage: item})
}

/** A record of a views-table item with string keys and no image, as a KEYS_ONLY stream writes it. */
export function keysOnly(name: EventName, pk: string, sk: string, sequence: string): StreamRecord {
  return streamRecord(name, {pk: {S: pk}, sk: {S: sk}}, sequence)
}

/** A journal counter's view row, as the Rust journal writes it. */
export function counterItem(scope: string, position: bigint): StreamItem {
  return {
    pk: {S: `journal-counter#${scope}`},
    sk: {S: 'COUNTER'},
    entityID: {S: 'SYSTEM#JOURNAL'},
    entityType: {S: 'journal-counter'},
    payload: {S: stringifyJson({scope, position})},
  }
}

/** The 20-digit sort key of a journal row. */
export function rowSk(position: number): string {
  return String(position).padStart(20, '0')
}

/**
 * Evaluates a Lambda filter pattern over a record's JSON, for the rules this package emits: a list
 * of values (equality, `prefix`, `exists`) at a leaf, nested objects otherwise.
 */
export function lambdaMatches(pattern: string, record: StreamRecord): boolean {
  return walk(JSON.parse(pattern), JSON.parse(JSON.stringify(record)))
}

function walk(pattern: unknown, value: unknown): boolean {
  if (Array.isArray(pattern)) {
    return pattern.some((rule: unknown) => {
      if (typeof rule === 'string') {
        return value === rule
      }

      const object = rule as {prefix?: string; exists?: boolean}
      if (object.prefix !== undefined) {
        return typeof value === 'string' && value.startsWith(object.prefix)
      }
      if (object.exists !== undefined) {
        return object.exists === (value !== undefined)
      }

      throw new Error(`unexpected rule ${JSON.stringify(rule)}`)
    })
  }
  if (typeof pattern === 'object' && pattern !== null) {
    return Object.entries(pattern).every(([name, rule]) =>
      walk(rule, (value as Record<string, unknown> | undefined)?.[name]),
    )
  }

  throw new Error(`unexpected pattern node ${JSON.stringify(pattern)}`)
}
