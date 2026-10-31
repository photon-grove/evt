import {Buffer} from 'node:buffer'

import {describe, expect, test} from 'vitest'

import {type SerializedEvent, eventId} from '../src/event.ts'
import {MemRepository, headFixtures} from '../src/memory/repository.ts'
import {emptyMetadata} from '../src/metadata.ts'

function event(sequence: number): SerializedEvent {
  return {
    id: eventId('a', BigInt(sequence)),
    sequence: BigInt(sequence),
    type: 'ledger:added',
    version: 1,
    entityId: 'a',
    entityType: 't',
    payload: Buffer.from(`{"amount":${sequence}}`),
    metadata: emptyMetadata(),
  }
}

describe('MemRepository', () => {
  test('copies a Buffer payload, whose slice() would share its memory', async () => {
    const repo = new MemRepository()
    const written = event(1)
    await repo.commit({entityId: 'a', events: [written]})

    written.payload!.fill(0x20)
    const [read] = await repo.events('a')
    read!.payload!.fill(0x20)

    const [stored] = await repo.events('a')
    expect(new TextDecoder().decode(stored!.payload!)).toBe('{"amount":1}')
  })

  test('a commit that fails while copying writes nothing', async () => {
    const repo = new MemRepository()
    await repo.commit({entityId: 'a', events: [event(1)]})

    // Untyped callers can pass a plain object where the metadata takes a Map. The encoder accepts
    // it, so the commit passes its checks and fails only when the events are copied.
    const malformed = event(3)
    malformed.metadata.extensions = {key: 'value'} as unknown as Map<string, string>
    const commit = repo.commit({entityId: 'a', events: [event(2), malformed]})
    await expect(commit).rejects.toThrow(TypeError)

    expect((await repo.events('a')).map((stored) => stored.sequence)).toEqual([1n])
    expect(headFixtures(repo).storedHead('a')).toBe(1n)
    await repo.commit({entityId: 'a', events: [event(2)]})
  })
})
