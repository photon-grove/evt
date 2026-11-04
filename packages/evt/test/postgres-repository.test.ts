import {describe, expect, test} from 'vitest'

import {EvtError, TransientError} from '../src/errors.ts'
import {type SerializedEvent, eventId} from '../src/event.ts'
import {emptyMetadata} from '../src/metadata.ts'
import type {PgClient, PgConnection, PgResult} from '../src/postgres/client.ts'
import {PgRepository} from '../src/postgres/repository.ts'

function event(sequence: number): SerializedEvent {
  return {
    id: eventId('a', BigInt(sequence)),
    sequence: BigInt(sequence),
    type: 'ledger:added',
    version: 1,
    entityId: 'a',
    entityType: 'ledger',
    payload: new TextEncoder().encode('{}'),
    metadata: emptyMetadata(),
  }
}

// The rows a store that declares stream-heads, holds no rebuild lock and accepts every write
// returns.
function answer(text: string): PgResult {
  if (text.includes('to_regclass')) {
    return {rows: [{}]}
  }
  if (text.startsWith('SELECT version::text')) {
    const manifest = {features: '{"stream-heads":1}', history_dependent: '[]', retention: '[]'}
    return {rows: [{version: '1', ...manifest}]}
  }
  if (text.startsWith('INSERT') || text.startsWith('UPDATE')) {
    return {rows: [{}]}
  }

  return {rows: []}
}

interface Fake {
  client: PgClient
  // Every statement, the pool's and its connections', in the order they ran.
  log: string[]
  released: (Error | undefined)[]
}

// A client over that store. `fail` returns what a statement throws, `pooled` false leaves out
// connect(), and `delay` holds every statement for that many milliseconds.
function fake(
  options: {fail?: (text: string) => unknown; pooled?: boolean; delay?: number} = {},
): Fake {
  const log: string[] = []
  const released: (Error | undefined)[] = []
  const query = async (text: string): Promise<PgResult> => {
    log.push(text)
    if (options.delay !== undefined) {
      await new Promise((resolve) => setTimeout(resolve, options.delay))
    }
    const failure = options.fail?.(text)
    if (failure !== undefined) {
      throw failure
    }

    return answer(text)
  }

  const client: PgClient = {query}
  if (options.pooled ?? true) {
    client.connect = async (): Promise<PgConnection> => ({
      query,
      release: (error?: Error) => released.push(error),
    })
  }

  return {client, log, released}
}

// The first words of each statement a transaction ran, from its START TRANSACTION on.
function transaction(log: string[]): string[] {
  return log.slice(log.indexOf('START TRANSACTION ISOLATION LEVEL READ COMMITTED')).map(head)
}

function head(text: string): string {
  return text.split(/\s+/).slice(0, 4).join(' ')
}

describe('PgRepository', () => {
  test('commits in the order Go and Rust do, at READ COMMITTED', async () => {
    const {client, log, released} = fake()
    await new PgRepository(client).commit({entityId: 'a', events: [event(1)]})

    expect(transaction(log)).toEqual([
      'START TRANSACTION ISOLATION LEVEL',
      'SELECT pg_advisory_xact_lock(1702261760, hashtext($1))',
      'SELECT pg_advisory_xact_lock_shared(1702261761, hashtext($1))',
      'SELECT 1 WHERE to_regclass($1)',
      'SELECT version::text AS version,',
      'SELECT pg_advisory_xact_lock_shared(1702261761, hashtext($1))',
      'SELECT 1 WHERE to_regclass($1)',
      'SELECT 1 WHERE EXISTS',
      'INSERT INTO evt_events__evt_stream_heads (entity_id,',
      'INSERT INTO evt_events (entity_id,',
      'COMMIT',
    ])
    expect(released).toEqual([undefined])
  })

  test('rolls back an aborted commit and sends no COMMIT', async () => {
    const controller = new AbortController()
    const {client, log} = fake({
      fail: (text) => {
        if (text.startsWith('INSERT INTO evt_events__evt_stream_heads')) {
          controller.abort(new Error('stop'))
        }
      },
    })
    const commit = new PgRepository(client).commit(
      {entityId: 'a', events: [event(1)]},
      {signal: controller.signal},
    )

    await expect(commit).rejects.toThrow('stop')
    expect(log.at(-1)).toBe('ROLLBACK')
    expect(log).not.toContain('COMMIT')
  })

  test('rejects a commit aborted while its COMMIT is in flight, with no rollback', async () => {
    const controller = new AbortController()
    const {client, log} = fake({
      fail: (text) => {
        if (text === 'COMMIT') {
          controller.abort(new Error('stop'))
        }
      },
    })
    const commit = new PgRepository(client).commit(
      {entityId: 'a', events: [event(1)]},
      {signal: controller.signal},
    )

    await expect(commit).rejects.toThrow('stop')
    expect(log.at(-1)).toBe('COMMIT')
  })

  test.each(['40001', '40P01'])('reports SQLSTATE %s as transient', async (code) => {
    const {client, log} = fake({
      fail: (text) =>
        text.startsWith('INSERT INTO evt_events (')
          ? Object.assign(new Error(code), {code})
          : undefined,
    })
    const commit = new PgRepository(client).commit({entityId: 'a', events: [event(1)]})

    await expect(commit).rejects.toBeInstanceOf(TransientError)
    expect(log.at(-1)).toBe('ROLLBACK')
  })

  test('hands a connection that failed to roll back to the pool as broken', async () => {
    const lost = new Error('connection lost')
    const {client, released} = fake({
      fail: (text) =>
        text.startsWith('INSERT INTO evt_events (') || text === 'ROLLBACK' ? lost : undefined,
    })
    const commit = new PgRepository(client).commit({entityId: 'a', events: [event(1)]})

    await expect(commit).rejects.toBe(lost)
    expect(released).toEqual([lost])
  })

  test('hands a connection back when its transaction fails to start', async () => {
    const lost = new Error('connection lost')
    const {client, released} = fake({fail: (text) => (text.startsWith('START') ? lost : undefined)})
    const commit = new PgRepository(client).commit({entityId: 'a', events: [event(1)]})

    await expect(commit).rejects.toBe(lost)
    expect(released).toHaveLength(1)
  })

  test("refuses a client whose connect() lends no connection, such as pg's Client", async () => {
    const client: PgClient = {
      query: async (text) => answer(text),
      connect: async () => undefined as unknown as PgConnection,
    }
    const commit = new PgRepository(client).commit({entityId: 'a', events: [event(1)]})

    await expect(commit).rejects.toMatchObject({code: 'invalidArgument'})
  })

  test('runs one operation at a time on a connection, across repositories', async () => {
    const {client, log} = fake({pooled: false, delay: 1})
    const repo = new PgRepository(client)
    const other = new PgRepository(client, {eventsTable: 'other_events'})
    await Promise.all([
      repo.commit({entityId: 'a', events: [event(1)]}),
      repo.events('a'),
      other.commit({entityId: 'a', events: [event(1)]}),
    ])

    // Each transaction runs from its START to its COMMIT before anything else starts.
    const starts = log.flatMap((text, i) => (text.startsWith('START') ? [i] : []))
    const commits = log.flatMap((text, i) => (text === 'COMMIT' ? [i] : []))
    expect(starts).toHaveLength(2)
    expect(commits[0]).toBeLessThan(starts[1]!)
    const readsInside = log.filter(
      (text, i) =>
        text.startsWith('SELECT event_id') &&
        starts.some((start, n) => start < i && i < commits[n]!),
    )
    expect(readsInside).toEqual([])
  })

  test('creates the schema one statement at a time, under the schema lock', async () => {
    const {client, log} = fake()
    await new PgRepository(client, {eventsTable: 'orders'}).ensureSchema()

    const statements = transaction(log).slice(1, -1)
    expect(statements[0]).toBe('SELECT pg_advisory_xact_lock(1702261762, hashtext($1))')
    expect(statements).toHaveLength(11)
    const ddl = log.slice(log.indexOf('START TRANSACTION ISOLATION LEVEL READ COMMITTED') + 2, -1)
    expect(ddl.every((text) => /^(CREATE|DO|ALTER) /.test(text))).toBe(true)
    // The recorded-identity columns are added by a guarded DO block, so a schema that has them takes
    // no lock on the outcomes table.
    const upgrade = ddl.filter((text) => text.includes('ALTER TABLE orders__evt_command_outcomes'))
    expect(upgrade).toHaveLength(1)
    expect(upgrade[0]).toMatch(/^DO \$\$\nBEGIN\n    IF \(SELECT count\(\*\) FROM pg_attribute/)
    expect(ddl.find((text) => text.startsWith('DO '))).toMatch(/END IF;\nEND \$\$$/)
  })

  test('refuses invalid table names at construction', () => {
    const {client} = fake()
    expect(() => new PgRepository(client, {eventsTable: 'x; DROP TABLE y'})).toThrow(EvtError)
  })
})
