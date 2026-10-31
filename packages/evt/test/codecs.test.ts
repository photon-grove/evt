import {describe, expect, test} from 'vitest'

import {base64Decode, base64Encode} from '../src/base64.ts'
import {
  eventFromItem,
  eventItem,
  expiry,
  headFromItem,
  headItem,
  itemSize,
  snapshotItem,
  sortKey,
} from '../src/dynamodb/item.ts'
import {decodeEnvelope} from '../src/envelope.ts'
import {EvtError} from '../src/errors.ts'
import {type SerializedEvent, decodeEvent, encodeEvent, encodeSnapshot} from '../src/event.ts'
import {decodeMetadata, emptyMetadata, encodeMetadata} from '../src/metadata.ts'
import {snapshotEventsDue} from '../src/snapshots.ts'
import {compareUtf8, utf8Decode, utf8Length} from '../src/utf8.ts'

function code(run: () => unknown): string | undefined {
  try {
    run()
  } catch (error) {
    return error instanceof EvtError ? error.code : 'not an EvtError'
  }

  return undefined
}

const encoder = new TextEncoder()

function event(overrides: Partial<SerializedEvent> = {}): SerializedEvent {
  return {
    id: 'acct-1:1',
    sequence: 1n,
    type: 'account:opened',
    version: 1,
    entityId: 'acct-1',
    entityType: 'account',
    payload: encoder.encode('{}'),
    metadata: {...emptyMetadata(), region: 'us-west-2'},
    ...overrides,
  }
}

describe('base64', () => {
  test('matches RFC 4648', () => {
    const vectors = [['', ''], ['f', 'Zg=='], ['fo', 'Zm8='], ['foo', 'Zm9v'], ['foob', 'Zm9vYg=='],
      ['fooba', 'Zm9vYmE='], ['foobar', 'Zm9vYmFy']]
    for (const [plain = '', encoded = ''] of vectors) {
      expect(base64Encode(encoder.encode(plain))).toBe(encoded)
      expect(new TextDecoder().decode(base64Decode(encoded))).toBe(plain)
    }
  })

  test('round-trips every byte value', () => {
    for (let length = 0; length <= 258; length++) {
      const bytes = Uint8Array.from({length}, (_, i) => (i * 7 + length) & 0xff)
      expect(base64Decode(base64Encode(bytes))).toEqual(bytes)
    }
  })

  test('refuses what Go refuses, and skips CR and LF', () => {
    for (const text of ['Zm8', 'Zm=8', 'Zg==Zg==', 'Z===', 'Zm9v!A==', '====', 'Zm9v Zg==']) {
      expect({text, code: code(() => base64Decode(text))}).toEqual({text, code: 'invalidRecord'})
    }

    expect(new TextDecoder().decode(base64Decode('Zm9v\r\nYmFy'))).toBe('foobar')
  })
})

describe('utf8', () => {
  test('counts UTF-8 bytes, with a lone surrogate as 3', () => {
    for (const text of ['', 'a', 'é', '漢', '🚀', 'a é🚀漢']) {
      expect(utf8Length(text)).toBe(encoder.encode(text).length)
    }

    expect(utf8Length('\ud800')).toBe(3)
    expect(utf8Length('a\udc00\ud83d')).toBe(7)
  })

  test('compares strings by UTF-8 bytes', () => {
    expect(compareUtf8('～', '\u{1f600}')).toBeLessThan(0)
    expect(compareUtf8('a', 'ab')).toBeLessThan(0)
    expect(compareUtf8('ab', 'ab')).toBe(0)
    expect(compareUtf8('b', 'a')).toBeGreaterThan(0)
  })

  test('refuses bytes that aren’t UTF-8 and keeps a byte-order mark', () => {
    expect(code(() => utf8Decode(new Uint8Array([0xff]), 'payload'))).toBe('invalidRecord')
    expect(utf8Decode(new Uint8Array([0xef, 0xbb, 0xbf, 0x61]), 'payload')).toBe('﻿a')
  })
})

describe('decoding', () => {
  test('refuses an int64 that doesn’t fit, or that isn’t an integer literal', () => {
    const base = '{"id":"a:1","type":"t","version":1,"entityId":"a","entityType":"x","metadata":{}'
    expect(decodeEvent(`${base},"sequence":9223372036854775807}`).sequence).toBe(2n ** 63n - 1n)
    expect(decodeEvent(`${base},"sequence":-9223372036854775808}`).sequence).toBe(-(2n ** 63n))
    for (const sequence of ['9223372036854775808', '1.0', '1e3', '"1"', 'true']) {
      const decoded = code(() => decodeEvent(`${base},"sequence":${sequence}}`))
      expect({sequence, code: decoded}).toEqual({sequence, code: 'invalidRecord'})
    }
  })

  test('refuses an event version past the safe integers', () => {
    expect(code(() => decodeEvent('{"version":9007199254740992}'))).toBe('invalidRecord')
  })

  test('refuses a wrong JSON type', () => {
    expect(code(() => decodeMetadata('{"commandId":1}'))).toBe('invalidRecord')
    expect(code(() => decodeMetadata('{"trace":[]}'))).toBe('invalidRecord')
    expect(code(() => decodeMetadata('{"extensions":{"a":1}}'))).toBe('invalidRecord')
    expect(code(() => decodeMetadata('[]'))).toBe('invalidRecord')
    expect(code(() => decodeEvent('{"metadata":"x"}'))).toBe('invalidRecord')
    expect(code(() => decodeEvent('{"payload":"!"}'))).toBe('invalidRecord')
  })

  test('refuses a repeated key, which Go merges and Rust refuses', () => {
    expect(code(() => decodeMetadata('{"region":"a","region":null}'))).toBe('invalidJson')
    expect(code(() => decodeEvent('{"metadata":{},"metadata":{}}'))).toBe('invalidJson')
    expect(code(() => decodeEnvelope('{"detail":{"id":"a:1","id":"a:2"}}'))).toBe('invalidJson')
  })

  test('matches field names exactly', () => {
    expect(decodeMetadata('{"CommandId":"c-1","REGION":"r"}')).toEqual(emptyMetadata())
  })

  test('writes the effective causation ID and decodes a missing one as null', () => {
    const metadata = decodeMetadata('{"commandId":"c-1"}')
    expect(metadata.causationId).toBeNull()
    expect(encodeMetadata(metadata)).toContain('"causationId":"c-1"')
  })

  test('refuses an envelope without an entity or event ID', () => {
    expect(code(() => decodeEnvelope('{}'))).toBe('invalidRecord')
    expect(code(() => decodeEnvelope('{"detail":{"entityId":"a"}}'))).toBe('invalidRecord')
  })
})

describe('encoding', () => {
  test('refuses integers no reader would read back', () => {
    expect(code(() => encodeEvent(event({sequence: 2n ** 63n})))).toBe('invalidArgument')
    expect(code(() => encodeEvent(event({version: 1.5})))).toBe('invalidArgument')
    expect(code(() => encodeEvent(event({version: Number.NaN})))).toBe('invalidArgument')
    expect(code(() => eventItem(event({sequence: -(2n ** 63n) - 1n})))).toBe('invalidArgument')
    expect(code(() => eventItem(event(), 1.5))).toBe('invalidArgument')
    expect(code(() => headItem('a', 2n ** 63n))).toBe('invalidArgument')

    const snapshot = {entityType: 'x', entityId: 'a', sequence: 1n, eventSequence: 2n ** 64n}
    expect(code(() => encodeSnapshot({...snapshot, payload: null}))).toBe('invalidArgument')
    expect(code(() => snapshotItem({...snapshot, payload: null}))).toBe('invalidArgument')
  })
})

describe('DynamoDB items', () => {
  test('refuses a payload that isn’t UTF-8, since it is stored as a string', () => {
    expect(code(() => eventItem(event({payload: new Uint8Array([0xc3])})))).toBe('invalidArgument')
  })

  test('writes ttl only when positive', () => {
    expect(eventItem(event(), 0)['ttl']).toBeUndefined()
    expect(eventItem(event(), 42)['ttl']).toEqual({N: '42'})
  })

  test('decodes an item and refuses the wrong attribute types', () => {
    const item = eventItem(event())
    expect(encodeEvent(eventFromItem(item))).toBe(encodeEvent(event()))

    const binary = {B: new Uint8Array()}
    expect(code(() => eventFromItem({...item, payload: binary}))).toBe('invalidRecord')
    expect(code(() => eventFromItem({...item, sk: {S: '1'}}))).toBe('invalidRecord')
    expect(code(() => eventFromItem({...item, sk: {N: '1.5'}}))).toBe('invalidRecord')
    const pastInt64 = {N: '9223372036854775808'}
    expect(code(() => eventFromItem({...item, sk: pastInt64}))).toBe('invalidRecord')
    expect(code(() => eventFromItem({...item, metadata: {S: ' '}}))).toBe('invalidRecord')
    expect(code(() => eventFromItem({...item, metadata: {S: '{'}}))).toBe('invalidRecord')

    const {pk: _, ...withoutKey} = item
    expect(code(() => eventFromItem(withoutKey))).toBe('invalidRecord')
  })

  test('decodes null metadata as the zero value, as Go does', () => {
    const decoded = eventFromItem({...eventItem(event()), metadata: {S: 'null'}})
    expect(decoded.metadata).toEqual(emptyMetadata())
  })

  test('reads sort keys and heads', () => {
    expect(sortKey({sk: {N: '-1'}})).toBe(-1n)
    expect(sortKey({sk: {S: 'VIEW'}})).toBeUndefined()
    expect(headFromItem({head: {N: '7'}, ttl: {N: '9'}})).toEqual({head: 7n, ttl: 9})
    expect(headFromItem({head: {N: '7'}})).toEqual({head: 7n, ttl: undefined})
  })

  test('sizes every attribute type as the Rust crate does', () => {
    const scalars = {pk: {S: 'é'}, n: {N: '10'}, b: {B: new Uint8Array(4)}, t: {BOOL: true}}
    expect(itemSize(scalars)).toBe(2 + 2 + 1 + 2 + 1 + 4 + 1 + 1)
    expect(itemSize({m: {M: {a: {S: 'x'}}}, l: {L: [{S: 'ab'}, {NULL: true}]}})).toBe(
      1 + (3 + 2 + 1) + 1 + (3 + 3 + 2),
    )
  })

  test('computes the expiry in whole seconds, rounded down after adding', () => {
    expect(expiry(1_791_763_200_900, 86_400_500)).toBe(1_791_849_601)
    expect(expiry(1_000, 0)).toBeUndefined()
    expect(code(() => expiry(1_000, 0.5))).toBe('invalidArgument')
    expect(code(() => expiry(1_000, -1))).toBe('invalidArgument')
  })
})

describe('snapshotEventsDue', () => {
  test('snapshots when a commit reaches or crosses the next multiple', () => {
    expect(snapshotEventsDue(1n, 6, 3)).toBe(6)
    expect(snapshotEventsDue(7n, 1, 3)).toBe(0)
    expect(snapshotEventsDue(2n ** 62n, 5, 0)).toBe(0)
  })

  test('disables snapshots for a negative interval, as Go does, and refuses a fraction', () => {
    expect(snapshotEventsDue(0n, 1, -3)).toBe(0)
    expect(snapshotEventsDue(5n, 2, -3)).toBe(0)
    expect(code(() => snapshotEventsDue(0n, 1, 1.5))).toBe('invalidArgument')
    expect(code(() => snapshotEventsDue(0n, 0.5, 2))).toBe('invalidArgument')
  })
})
