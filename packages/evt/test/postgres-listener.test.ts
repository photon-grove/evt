import {describe, expect, test} from 'vitest'

import {EvtError, TransientError} from '../src/errors.ts'
import type {PgResult} from '../src/postgres/client.ts'
import {
  JournalListener,
  type PgListenConnection,
  type PgNotification,
  type Wakeup,
} from '../src/postgres/listener.ts'

// A dedicated connection whose statements resolve unless `answer` says otherwise, and whose events
// a test raises.
class FakeConnection implements PgListenConnection {
  readonly log: string[] = []
  ended = false
  answer: (text: string) => Promise<PgResult> = async () => ({rows: []})
  readonly #handlers = new Map<string, ((value: never) => void)[]>()

  async query(text: string): Promise<PgResult> {
    this.log.push(text)

    return this.answer(text)
  }

  on(event: string, listener: (value: never) => void): this {
    this.#handlers.set(event, [...(this.#handlers.get(event) ?? []), listener])

    return this
  }

  async end(): Promise<void> {
    this.ended = true
  }

  notify(channel: string, payload?: string): void {
    const message: PgNotification = {channel, payload}
    for (const handler of this.#handlers.get('notification') ?? []) {
      handler(message as never)
    }
  }

  emit(event: 'end' | 'error'): void {
    for (const handler of this.#handlers.get(event) ?? []) {
      handler((event === 'error' ? new Error('connection lost') : undefined) as never)
    }
  }
}

// A listener over fresh fake connections, one per connect, which the test reads from `opened`.
function listening(options: {pollIntervalMs?: number; connectTimeoutMs?: number} = {}): {
  listener: JournalListener
  opened: FakeConnection[]
} {
  const opened: FakeConnection[] = []
  const listener = new JournalListener({
    eventsTable: 'Evt_Events',
    pollIntervalMs: options.pollIntervalMs ?? 60_000,
    connectTimeoutMs: options.connectTimeoutMs,
    connect: async () => {
      const connection = new FakeConnection()
      opened.push(connection)

      return connection
    },
  })

  return {listener, opened}
}

const CHANNEL = 'evt_events__evt_journal'

async function fresh(): Promise<FakeConnection> {
  return new FakeConnection()
}
const scope = (name: string): Wakeup => ({kind: 'scope', scope: name})

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

describe('JournalListener', () => {
  test('listens on the channel the events table names', async () => {
    const {listener, opened} = listening()
    expect(listener.channel).toBe(CHANNEL)
    expect(await listener.next()).toEqual({kind: 'connected'})
    expect(opened[0]!.log).toEqual([`LISTEN ${CHANNEL}`])
  })

  test('refuses an invalid events table, connect or interval at construction', () => {
    const connect = fresh
    expect(() => new JournalListener({connect, eventsTable: 'bad name'})).toThrow(EvtError)
    expect(() => new JournalListener({connect, pollIntervalMs: 0})).toThrow(/poll interval/)
    expect(() => new JournalListener({connect, connectTimeoutMs: 1.5})).toThrow(/connect timeout/)
    expect(() => new JournalListener({connect, pollIntervalMs: 2 ** 31})).toThrow(/poll interval/)
    expect(() => new JournalListener({} as never)).toThrow(/connect function/)
  })

  test('wakes each notified scope once, in arrival order, and ignores the rest', async () => {
    const {listener, opened} = listening()
    await listener.next()
    const connection = opened[0]!
    connection.notify(CHANNEL, 'b')
    connection.notify(CHANNEL, 'a')
    connection.notify(CHANNEL, 'b')
    connection.notify('another_channel', 'c')
    connection.notify(CHANNEL, '')
    connection.notify(CHANNEL)
    expect(await listener.next()).toEqual(scope('b'))
    expect(await listener.next()).toEqual(scope('a'))

    // A notification while a call waits wakes it.
    const waiting = listener.next()
    connection.notify(CHANNEL, 'later')
    expect(await waiting).toEqual(scope('later'))
  })

  test('polls at its interval, ahead of pending scopes, and then checks the connection', async () => {
    const {listener, opened} = listening({pollIntervalMs: 30})
    await listener.next()
    expect(await listener.next()).toEqual({kind: 'poll'})

    const connection = opened[0]!
    await delay(40)
    connection.notify(CHANNEL, 'a')
    expect(await listener.next()).toEqual({kind: 'poll'})
    expect(await listener.next()).toEqual(scope('a'))
    // The overdue poll and the pending scope come before the check, which the next call runs.
    expect(connection.log).toEqual([`LISTEN ${CHANNEL}`])
    expect(await listener.next()).toEqual({kind: 'poll'})
    expect(connection.log).toEqual([`LISTEN ${CHANNEL}`, 'SELECT 1'])
  })

  test('replaces a connection that fails its check or does not answer it', async () => {
    const {listener, opened} = listening({pollIntervalMs: 20, connectTimeoutMs: 30})
    await listener.next()
    expect(await listener.next()).toEqual({kind: 'poll'})
    opened[0]!.answer = async () => {
      throw new Error('gone')
    }
    expect(await listener.next()).toEqual({kind: 'connected'})
    expect(opened[0]!.ended).toBe(true)

    expect(await listener.next()).toEqual({kind: 'poll'})
    opened[1]!.answer = () => new Promise(() => undefined)
    expect(await listener.next()).toEqual({kind: 'connected'})
    expect(opened[1]!.ended).toBe(true)
    expect(opened).toHaveLength(3)
  })

  test('reconnects after the connection ends or fails, and says so', async () => {
    const {listener, opened} = listening()
    await listener.next()
    const waiting = listener.next()
    opened[0]!.emit('end')
    expect(await waiting).toEqual({kind: 'connected'})
    opened[1]!.emit('error')
    expect(await listener.next()).toEqual({kind: 'connected'})
    expect(opened).toHaveLength(3)

    // A lost connection's late notifications are not the listener's.
    opened[0]!.notify(CHANNEL, 'stale')
    opened[2]!.notify(CHANNEL, 'fresh')
    expect(await listener.next()).toEqual(scope('fresh'))
  })

  test('rejects a connection it cannot open, and tries again on the next call', async () => {
    let attempts = 0
    const listener = new JournalListener({
      connect: async () => {
        attempts++
        if (attempts === 1) {
          throw new Error('refused')
        }
        return new FakeConnection()
      },
    })
    await expect(listener.next()).rejects.toThrow('refused')
    expect(await listener.next()).toEqual({kind: 'connected'})
  })

  test('bounds a connection attempt by the connect timeout, and ends one that opens late', async () => {
    const late = new FakeConnection()
    const pending: {open?: (connection: FakeConnection) => void} = {}
    const listener = new JournalListener({
      connectTimeoutMs: 20,
      connect: () => new Promise((resolve) => (pending.open = resolve)),
    })
    await expect(listener.next()).rejects.toBeInstanceOf(TransientError)
    pending.open?.(late)
    await delay(0)
    expect(late.ended).toBe(true)
  })

  test('an abort rejects the call and keeps pending scopes', async () => {
    const {listener, opened} = listening()
    await listener.next()
    const controller = new AbortController()
    const waiting = listener.next({signal: controller.signal})
    controller.abort(new Error('stop'))
    await expect(waiting).rejects.toThrow('stop')

    opened[0]!.notify(CHANNEL, 'a')
    await expect(listener.next({signal: AbortSignal.abort()})).rejects.toBeDefined()
    expect(await listener.next()).toEqual(scope('a'))
  })

  test('an abort during the health check runs it again on the next call', async () => {
    const {listener, opened} = listening({pollIntervalMs: 20})
    await listener.next()
    expect(await listener.next()).toEqual({kind: 'poll'})
    const connection = opened[0]!
    connection.answer = () => new Promise(() => undefined)
    await expect(listener.next({signal: AbortSignal.timeout(10)})).rejects.toBeDefined()
    connection.answer = async () => ({rows: []})
    expect(await listener.next()).toEqual({kind: 'poll'})
    expect(connection.log).toEqual([`LISTEN ${CHANNEL}`, 'SELECT 1', 'SELECT 1'])
    expect(opened).toHaveLength(1)
  })

  test('a close during a connection attempt rejects the call and ends the connection', async () => {
    const connection = new FakeConnection()
    const pending: {open?: (connection: FakeConnection) => void} = {}
    const listener = new JournalListener({
      connect: () => new Promise((resolve) => (pending.open = resolve)),
    })
    const waiting = listener.next()
    await delay(0)
    await listener.close()
    pending.open?.(connection)
    await expect(waiting).rejects.toThrow(/closed/)
    expect(connection.ended).toBe(true)
  })

  test('takes one call at a time', async () => {
    const {listener} = listening()
    await listener.next()
    const waiting = listener.next()
    await expect(listener.next()).rejects.toThrow(/one call/)
    await listener.close()
    await expect(waiting).rejects.toThrow(/closed/)
  })

  test('close ends the connection, and a later call rejects', async () => {
    const {listener, opened} = listening()
    await listener.next()
    await listener.close()
    expect(opened[0]!.ended).toBe(true)
    await expect(listener.next()).rejects.toThrow(/closed/)
  })
})
