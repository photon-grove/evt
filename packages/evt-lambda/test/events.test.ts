// NEW_IMAGE decode of event items, and the engine items it skips.
import {type SerializedEvent, emptyMetadata} from '@photon-grove/evt'
import {eventItem} from '@photon-grove/evt/dynamodb'
import {describe, expect, test} from 'vitest'

import {DecodeError} from '../src/errors.ts'
import {decodeEvent, isEventKey} from '../src/events.ts'
import {
  type StreamAttributeValue,
  type StreamRecord,
  streamItem,
  streamRecord,
} from '../src/record.ts'

const deposited: SerializedEvent = {
  id: 'acct-1:3',
  sequence: 3n,
  type: 'account:deposited',
  version: 1,
  entityId: 'acct-1',
  entityType: 'account',
  payload: new TextEncoder().encode('{"amount":5}'),
  metadata: {...emptyMetadata(), commandId: 'cmd-1', causationId: 'cmd-1'},
}

function keys(pk: string, sk: StreamAttributeValue) {
  return {pk: {S: pk}, sk}
}

function n(value: number): StreamAttributeValue {
  return {N: String(value)}
}

function thrown(run: () => unknown): DecodeError {
  try {
    run()
  } catch (error) {
    if (error instanceof DecodeError) {
      return error
    }

    throw error
  }

  throw new Error('nothing was thrown')
}

describe('decodeEvent', () => {
  test('an inserted event decodes with its idempotency key', () => {
    const record = streamRecord('INSERT', keys('acct-1', n(3)), '7', {
      newImage: streamItem(eventItem(deposited)),
    })
    record.dynamodb!.ApproximateCreationDateTime = 1_760_918_400

    const decoded = decodeEvent(record)

    expect(decoded?.event).toEqual(deposited)
    expect(decoded?.sequenceNumber).toBe('7')
    expect(decoded?.idempotencyKey).toBe('acct-1:3')
    expect(decoded?.approximateCreationDateTime).toBe(1_760_918_400)
  })

  test('engine items and non-inserts are not events', () => {
    const image = streamItem(eventItem(deposited))
    const cases: [string, StreamRecord][] = [
      ['modify', streamRecord('MODIFY', keys('acct-1', n(3)), '1')],
      ['remove', streamRecord('REMOVE', keys('acct-1', n(3)), '1')],
      ['unknown', {...streamRecord('INSERT', keys('acct-1', n(3)), '1'), eventName: 'LATER'}],
      ['head', streamRecord('INSERT', keys('acct-1', n(-1)), '1')],
      ['snapshot', streamRecord('INSERT', keys('acct-1', n(0)), '1')],
      ['marker', streamRecord('INSERT', keys('SYSTEM#FEATURES', n(0)), '1')],
      ['fence', streamRecord('INSERT', keys('SYSTEM#REBUILD', n(1)), '1')],
      ['outcome', streamRecord('INSERT', keys('command-outcome#cmd-1', n(1)), '1')],
      ['view row', streamRecord('INSERT', keys('account#acct-1', {S: 'VIEW'}), '1')],
    ]

    const decoded = cases.map(([name, record]) => {
      record.dynamodb!.NewImage = image

      return [name, decodeEvent(record)]
    })

    expect(decoded).toEqual(cases.map(([name]) => [name, undefined]))
  })

  test('an event key is a string pk outside the reserved namespaces and an int64 sk of at least 1', () => {
    expect(isEventKey(keys('acct-1', n(1)))).toBe(true)
    expect(isEventKey(keys('acct-1', {N: '+7'}))).toBe(true)
    expect(isEventKey(keys('acct-1', {N: '9223372036854775807'}))).toBe(true)
    expect(isEventKey(keys('acct-1', {N: '9223372036854775808'}))).toBe(false)
    expect(isEventKey(keys('acct-1', {N: '1.0'}))).toBe(false)
    expect(isEventKey(keys('acct-1', {S: '1'}))).toBe(false)
    expect(isEventKey({pk: {N: '1'}, sk: n(1)})).toBe(false)
    expect(isEventKey({sk: n(1)})).toBe(false)
  })

  test('a null image or null keys read as missing, as the Rust crate reads them', () => {
    const nullImage = {eventName: 'INSERT', dynamodb: {Keys: keys('acct-1', n(3)), NewImage: null}}
    expect(thrown(() => decodeEvent(nullImage as never)).kind).toBe('missingNewImage')

    const nullKeys = {
      eventName: 'INSERT',
      dynamodb: {Keys: null, NewImage: streamItem(eventItem(deposited))},
    }
    expect(decodeEvent(nullKeys as never)?.event.id).toBe('acct-1:3')
  })

  test('keys missing from the record are read from the new image', () => {
    const image = streamItem(eventItem(deposited))
    const record = streamRecord('INSERT', {}, '8', {newImage: image})

    expect(decodeEvent(record)?.event.id).toBe('acct-1:3')
  })

  test('an event without a new image, or with a corrupt item, is an error', () => {
    const keysOnly = streamRecord('INSERT', keys('acct-1', n(3)), '9', {viewType: 'KEYS_ONLY'})
    const missing = thrown(() => decodeEvent(keysOnly))
    expect(missing.kind).toBe('missingNewImage')
    expect(missing.viewType).toBe('KEYS_ONLY')
    expect(missing.sequenceNumber).toBe('9')
    expect(missing.message).toBe(
      'stream record 9 inserts an event but has no NewImage; the stream view type must be ' +
        'NEW_IMAGE or NEW_AND_OLD_IMAGES (it is KEYS_ONLY)',
    )

    const unset = {eventName: 'INSERT', dynamodb: {Keys: keys('acct-1', n(3)), SequenceNumber: '9'}}
    expect(thrown(() => decodeEvent(unset)).viewType).toBe('unset')

    const item = streamItem(eventItem(deposited))
    delete item['metadata']
    const corrupt = streamRecord('INSERT', keys('acct-1', n(3)), '10', {newImage: item})
    const invalid = thrown(() => decodeEvent(corrupt))
    expect(invalid.kind).toBe('invalidRecord')
    expect(invalid.sequenceNumber).toBe('10')
    expect(invalid.message).toBe('stream record 10: event acct-1:3 has empty metadata')

    const malformed = streamRecord('INSERT', keys('acct-1', n(3)), '11', {
      newImage: {...streamItem(eventItem(deposited)), extra: {B: 'not base64!'}},
    })
    expect(thrown(() => decodeEvent(malformed)).kind).toBe('invalidRecord')
  })
})
