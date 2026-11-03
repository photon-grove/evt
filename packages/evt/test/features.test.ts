import {describe, expect, test} from 'vitest'

import {manifestFromItem, markerItem} from '../src/dynamodb/marker.ts'
import {
  RebuildInProgressError,
  StreamHeadsNotMigratedError,
  UnsupportedFeatureError,
  isConflict,
  isRetryable,
} from '../src/errors.ts'
import {type SerializedEvent, eventId} from '../src/event.ts'
import {SUPPORTED_FEATURES, checkSupported, decodeManifest} from '../src/features.ts'
import {MemRepository, controlFixtures} from '../src/memory/repository.ts'
import {emptyMetadata} from '../src/metadata.ts'

function event(sequence: number): SerializedEvent {
  return {
    id: eventId('a', BigInt(sequence)),
    sequence: BigInt(sequence),
    type: 'ledger:added',
    version: 1,
    entityId: 'a',
    entityType: 't',
    payload: new TextEncoder().encode('{}'),
    metadata: emptyMetadata(),
  }
}

const streamHeads = [['stream-heads', 1]]

describe('MemRepository initialization', () => {
  test("a fresh store's first commit declares stream-heads", async () => {
    const repo = new MemRepository()
    const controls = controlFixtures(repo)
    expect(controls.manifest()).toBeNull()

    await repo.commit({entityId: 'a', events: [event(1)]})

    expect(controls.manifest()?.version).toBe(1n)
    expect(Array.from(controls.manifest()!.features)).toEqual(streamHeads)
    expect(await repo.initialize()).toBe(false)
  })

  test('so does its first snapshot write', async () => {
    const repo = new MemRepository()
    const snapshot = {
      entityType: 't',
      entityId: 'a',
      sequence: 1n,
      eventSequence: 0n,
      payload: new TextEncoder().encode('{}'),
    }

    expect(await repo.putSnapshot(snapshot)).toBe(true)
    expect(controlFixtures(repo).manifest()?.version).toBe(1n)
  })

  test('initialize declares stream-heads once', async () => {
    const repo = new MemRepository()

    expect(await repo.initialize()).toBe(true)
    expect(await repo.initialize()).toBe(false)
    expect(controlFixtures(repo).manifest()?.version).toBe(1n)
  })

  test('compaction never declares stream-heads', async () => {
    const repo = new MemRepository()

    await expect(repo.compactBelow('a', 1n)).rejects.toThrow(StreamHeadsNotMigratedError)
    expect(controlFixtures(repo).manifest()).toBeNull()
  })

  test('a declaration keeps the manifest it adds stream-heads to', async () => {
    const repo = new MemRepository()
    const controls = controlFixtures(repo)
    controls.declare((draft) => {
      draft.retention.push('session')
    })

    await repo.commit({entityId: 'a', events: [event(1)]})

    expect(controls.manifest()).toEqual({
      version: 2n,
      features: new Map(streamHeads as [string, number][]),
      historyDependent: [],
      retention: ['session'],
    })
  })

  test('an aborted initialization declares nothing', async () => {
    const repo = new MemRepository()
    const reason = new Error('stop')

    await expect(repo.initialize({signal: AbortSignal.abort(reason)})).rejects.toBe(reason)
    expect(controlFixtures(repo).manifest()).toBeNull()
  })
})

describe('the feature manifest', () => {
  test('an unsupported declaration lists every pair, sorted by name', () => {
    const manifest = {
      version: 1n,
      features: new Map([
        ['stream-heads', 1],
        ['journal-compaction', 1],
        ['journal', 2],
      ]),
      historyDependent: [],
      retention: [],
    }

    let error: unknown
    try {
      checkSupported(manifest)
    } catch (thrown) {
      error = thrown
    }

    expect(error).toBeInstanceOf(UnsupportedFeatureError)
    expect((error as UnsupportedFeatureError).unsupported).toEqual([
      {name: 'journal', version: 2},
      {name: 'journal-compaction', version: 1},
    ])
    expect((error as Error).message).toBe(
      'store declares unsupported features (journal@2, journal-compaction@1); refusing to write',
    )
  })

  test('a version past the safe integers is unsupported, as an int64 is in Go and Rust', () => {
    const manifest = decodeManifest(1n, {
      features: '{"journal":1152921504606846976}',
      historyDependent: ' ',
      retention: '',
    })

    expect(() => checkSupported(manifest)).toThrow(UnsupportedFeatureError)
    expect(manifest.historyDependent).toEqual([])
  })

  test('the supported pairs are frozen', () => {
    expect(Object.isFrozen(SUPPORTED_FEATURES)).toBe(true)
    expect(SUPPORTED_FEATURES.every((pair) => Object.isFrozen(pair))).toBe(true)
  })

  test('an absent marker supports every writer', () => {
    expect(() => checkSupported(null)).not.toThrow()
  })

  test('stored sets decode sorted by UTF-8 bytes and deduplicated', () => {
    // U+FF5E sorts before U+1F600 in UTF-8, after it in UTF-16.
    const manifest = decodeManifest(4n, {
      features: '{}',
      historyDependent: '["\u{1F600}","～","～"]',
      retention: '',
    })

    expect(manifest.historyDependent).toEqual(['～', '\u{1F600}'])
    expect(manifest.retention).toEqual([])
  })

  test.each([
    ['features that are not JSON', {features: '{', historyDependent: '', retention: ''}],
    ['features that are not an object', {features: '[]', historyDependent: '', retention: ''}],
    ['a fractional version', {features: '{"journal":1.5}', historyDependent: '', retention: ''}],
    ['an unsafe version', {features: '{"journal":1e300}', historyDependent: '', retention: ''}],
    ['a set of numbers', {features: '{}', historyDependent: '[1]', retention: ''}],
    [
      'a version past int64',
      {features: '{"a":9223372036854775808}', historyDependent: '', retention: ''},
    ],
    ['duplicate feature keys', {features: '{"a":1,"a":2}', historyDependent: '', retention: ''}],
  ])('refuses %s', (_, texts) => {
    expect(() => decodeManifest(1n, texts)).toThrow(
      expect.objectContaining({code: 'invalidRecord'}),
    )
  })

  test('a marker item needs a version', () => {
    const item = markerItem({
      version: 1n,
      features: new Map(),
      historyDependent: [],
      retention: [],
    })
    delete item['version']

    expect(() => manifestFromItem(item)).toThrow(expect.objectContaining({code: 'invalidRecord'}))
  })

  test('a rebuild in progress is retryable, and not a conflict', () => {
    const error = new RebuildInProgressError()

    expect(isRetryable(error)).toBe(true)
    expect(isConflict(error)).toBe(false)
  })
})
