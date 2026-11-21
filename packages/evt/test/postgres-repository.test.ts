import {describe, expect, test} from 'vitest'

import {
  CommitTooLargeError,
  EvtError,
  ProjectorError,
  ProjectorOperationError,
  TransientError,
  ViewsNotConfiguredError,
} from '../src/errors.ts'
import {type SerializedEvent, eventId} from '../src/event.ts'
import {emptyMetadata} from '../src/metadata.ts'
import {type ProjectedView, type ViewOperation, deleteView, putView} from '../src/projection.ts'
import type {PgClient, PgConnection, PgResult} from '../src/postgres/client.ts'
import {PgRepository} from '../src/postgres/repository.ts'
import type {ViewStatement, ViewWriter} from '../src/postgres/views.ts'

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
  // Each statement's values, by the same index.
  values: (unknown[] | undefined)[]
  released: (Error | undefined)[]
}

// A client over that store. `fail` returns what a statement throws, `pooled` false leaves out
// connect(), `delay` holds every statement for that many milliseconds, and `respond` answers a
// statement in the store's place when it returns a result.
function fake(
  options: {
    fail?: (text: string) => unknown
    pooled?: boolean
    delay?: number
    respond?: (text: string) => PgResult | undefined
  } = {},
): Fake {
  const log: string[] = []
  const values: (unknown[] | undefined)[] = []
  const released: (Error | undefined)[] = []
  const query = async (text: string, params?: unknown[]): Promise<PgResult> => {
    log.push(text)
    values.push(params)
    if (options.delay !== undefined) {
      await new Promise((resolve) => setTimeout(resolve, options.delay))
    }
    const failure = options.fail?.(text)
    if (failure !== undefined) {
      throw failure
    }

    return options.respond?.(text) ?? answer(text)
  }

  const client: PgClient = {query}
  if (options.pooled ?? true) {
    client.connect = async (): Promise<PgConnection> => ({
      query,
      release: (error?: Error) => released.push(error),
    })
  }

  return {client, log, values, released}
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

  test('locks every stream of a transaction once, in byte order, before the marker', async () => {
    const {client, log, values} = fake()
    const b = {...event(1), id: eventId('b', 1n), entityId: 'b'}
    const a = {...event(1), id: eventId('a', 1n), entityId: 'a'}
    await new PgRepository(client).commitTransaction({
      streams: [
        {entityId: 'b', events: [b]},
        {entityId: 'a', events: [a]},
      ],
      fences: [
        {entityId: 'c', entityType: 'ledger', head: 0n, provenance: 'engine'},
        {entityId: 'a', entityType: 'ledger', head: 5n, provenance: 'engine'},
      ],
    })

    const start = log.indexOf('START TRANSACTION ISOLATION LEVEL READ COMMITTED')
    const locks = log.slice(start + 1, start + 4)
    expect(locks.every((text) => text.startsWith('SELECT pg_advisory_xact_lock(1702261760'))).toBe(
      true,
    )
    expect(values.slice(start + 1, start + 4)).toEqual([['a'], ['b'], ['c']])
    expect(log[start + 4]).toMatch(/^SELECT pg_advisory_xact_lock_shared/)
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

// A view operation of the `handles` projector.
function projected(op: ViewOperation, operation = 0): ProjectedView {
  return {projector: 'handles', projectorIndex: 0, operation, op}
}

// A PostgreSQL error as `pg` raises it.
function pgError(code: string, constraint?: string): Error {
  return Object.assign(new Error(`SQLSTATE ${code}`), {code, constraint})
}

// A writer that puts every operation into a `views` table, with the compat drivers' statements.
const writer: ViewWriter = (op) => {
  if (op.kind === 'delete') {
    return op.condition === 'present'
      ? {sql: 'DELETE FROM views WHERE pk = $1', args: [op.pk, op.sk, op.owner]}
      : {sql: 'WITH gone AS (DELETE FROM views) SELECT 1', args: [op.pk, op.sk, op.owner]}
  }
  const insert = 'INSERT INTO views (pk, sk, owner, payload) VALUES ($1, $2, $3, $4)'
  return op.condition === 'absent'
    ? {sql: insert, args: [op.pk, op.sk, op.owner, op.payload], keyConstraint: 'views_pkey'}
    : {sql: `${insert} ON CONFLICT (pk, sk) DO UPDATE`, args: [op.pk, op.sk, op.owner, op.payload]}
}

// A writer whose statements are `size` bytes long and over.
function sized(size: number): ViewWriter {
  return () => ({sql: `SELECT 1 ${'-'.repeat(size)}`})
}

// A writer of short statements that name their sort keys.
const short: ViewWriter = (view): ViewStatement => ({sql: `SELECT 1 /* ${view.sk} */`})

// `count` puts of distinct rows.
function puts(count: number): ViewOperation[] {
  return Array.from({length: count}, (_, n) =>
    putView('k', String(n).padStart(3, '0'), 'a', 'ledger', '{}'),
  )
}

// A COMMIT that fails with a unique violation.
function failAtCommit(text: string): unknown {
  return text === 'COMMIT' ? pgError('23505') : undefined
}

// Commits one event of stream `a` with a snapshot and the given view operations.
function commitViews(repo: PgRepository, ...ops: ViewOperation[]): Promise<void> {
  const payload = new TextEncoder().encode('{}')
  return repo.commit({
    entityId: 'a',
    events: [event(1)],
    snapshot: {entityType: 'ledger', sequence: 1n, payload},
    views: ops.map((op, index) => projected(op, index)),
  })
}

async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise
  } catch (error) {
    return error
  }
  throw new Error('expected a failure')
}

// A client whose view statements resolve `result`, and a repository with the writer on it.
function answering(result: PgResult): {repo: PgRepository; log: string[]} {
  const {client, log} = fake({respond: (text) => (/views/.test(text) ? result : undefined)})
  return {repo: new PgRepository(client, {viewWriter: writer}), log}
}

// What a commit whose view statement raises `error` rejects with.
function raising(error: unknown, op: ViewOperation): Promise<unknown> {
  const {client} = fake({fail: (text) => (/views/.test(text) ? error : undefined)})
  return rejection(commitViews(new PgRepository(client, {viewWriter: writer}), op))
}

describe('PgRepository view writer', () => {
  test('runs view statements after the events and before the snapshot, with their own args', async () => {
    const {client, log, values} = fake()
    const repo = new PgRepository(client, {viewWriter: writer})
    expect(repo.writesViews()).toBe(true)
    await commitViews(repo, putView('k', 'one', 'a', 'ledger', '{"n":1}'))

    expect(transaction(log).slice(-5)).toEqual([
      'INSERT INTO evt_events (entity_id,',
      'INSERT INTO views (pk,',
      'SELECT sequence::text AS sequence',
      'INSERT INTO evt_snapshots (entity_id,',
      'COMMIT',
    ])
    const args = values[log.findIndex((text) => text.startsWith('INSERT INTO views'))]
    expect(args?.slice(0, 3)).toEqual(['k', 'one', 'a'])
    expect(args?.[3]).toEqual(new TextEncoder().encode('{"n":1}'))
  })

  test.each<[string, ViewOperation, PgResult]>([
    [
      'a put that affects no row',
      putView('k', 'one', 'a', 'ledger', '{}'),
      {rows: [], rowCount: 0},
    ],
    [
      'a delete of a required row that affects none',
      deleteView('k', 'one', 'a', {condition: 'present'}),
      {rows: [], rowCount: 0},
    ],
    ['a delete that reports a foreign row', deleteView('k', 'one', 'a'), {rows: [{}]}],
    [
      'a delete that reports a foreign row the driver counts as none',
      deleteView('k', 'one', 'a'),
      {rows: [{}], affectedRows: 0},
    ],
  ])('fails the condition: %s', async (_name, op, result) => {
    const {repo, log} = answering(result)
    const error = await rejection(commitViews(repo, op))
    expect(error).toBeInstanceOf(ProjectorOperationError)
    expect(error).toMatchObject({reason: 'ConditionalCheckFailed', pk: 'k', sk: 'one'})
    expect(log.at(-1)).toBe('ROLLBACK')
  })

  test.each<[string, ViewOperation, PgResult]>([
    ['a put that affects a row', putView('k', 'one', 'a', 'ledger', '{}'), {rows: [], rowCount: 1}],
    [
      'a delete of a required row that PGlite counts',
      deleteView('k', 'one', 'a', {condition: 'present'}),
      {rows: [], affectedRows: 1},
    ],
    ['a delete that reports nothing', deleteView('k', 'one', 'a'), {rows: [], rowCount: 0}],
    [
      'an absent put, which needs no count',
      putView('k', 'one', 'a', 'ledger', '{}', {condition: 'absent'}),
      {rows: []},
    ],
  ])('holds the condition: %s', async (_name, op, result) => {
    const {repo, log} = answering(result)
    await commitViews(repo, op)
    expect(log.at(-1)).toBe('COMMIT')
  })

  test.each<[string, ViewOperation]>([
    ['a put', putView('k', 'one', 'a', 'ledger', '{}')],
    ['a delete of a required row', deleteView('k', 'one', 'a', {condition: 'present'})],
  ])('refuses %s whose count the driver does not report', async (_name, op) => {
    const {repo} = answering({rows: []})
    const error = await rejection(commitViews(repo, op))
    expect(error).toMatchObject({code: 'invalidViewOperation'})
  })

  test('counts the rows an unconditioned delete reports, with no driver count', async () => {
    const {repo, log} = answering({rows: []})
    await commitViews(repo, deleteView('k', 'one', 'a'))
    expect(log.at(-1)).toBe('COMMIT')
  })

  test("runs a transaction's views after every stream's events, before every snapshot", async () => {
    const {client, log} = fake()
    const payload = new TextEncoder().encode('{}')
    const b = {...event(1), id: eventId('b', 1n), entityId: 'b'}
    await new PgRepository(client, {viewWriter: writer}).commitTransaction({
      streams: [
        {
          entityId: 'a',
          events: [event(1)],
          snapshot: {entityType: 'ledger', sequence: 1n, payload},
        },
        {entityId: 'b', events: [b], snapshot: {entityType: 'ledger', sequence: 1n, payload}},
      ],
      fences: [],
      views: [projected(putView('k', 'one', 'a', 'ledger', '{}'))],
    })

    const writes = transaction(log)
      .filter((text) => /^INSERT INTO (evt_events|views|evt_snapshots) /.test(text))
      .map((text) => text.split(' ')[2])
    expect(writes).toEqual(['evt_events', 'evt_events', 'views', 'evt_snapshots', 'evt_snapshots'])
  })

  test("sends a copy of the writer's arguments", async () => {
    const args = ['k', 'one', 'a']
    const {client, values, log} = fake()
    const repo = new PgRepository(client, {viewWriter: () => ({sql: 'SELECT 1 FROM views', args})})
    await commitViews(repo, deleteView('k', 'one', 'a'))
    const sent = values[log.indexOf('SELECT 1 FROM views')]
    expect(sent).toEqual(args)
    expect(sent).not.toBe(args)
  })

  test('maps the errors a view statement raises', async () => {
    const taken = new Error('taken')
    const claim = putView('k', 'one', 'a', 'ledger', '{}', {condition: 'absent', conflict: taken})
    const plain = putView('k', 'one', 'a', 'ledger', '{}')

    // The key constraint, named in any case, is the failed condition.
    expect(await raising(pgError('23505', 'Views_PKey'), claim)).toMatchObject({
      reason: 'ConditionalCheckFailed',
      mapped: taken,
    })
    // Another constraint's violation, or one on a put without the condition, is the statement's.
    expect(await raising(pgError('23505', 'views_payload_key'), claim)).toMatchObject({
      reason: '23505',
    })
    expect(await raising(pgError('23505', 'views_pkey'), plain)).toMatchObject({
      reason: '23505',
      mapped: undefined,
    })
    expect(await raising(pgError('23514'), claim)).toMatchObject({
      code: 'projectorOperation',
      reason: '23514',
    })
    // A rollback is transient or passes through, and so does a lost connection.
    expect(await raising(pgError('40001'), claim)).toBeInstanceOf(TransientError)
    expect(await raising(pgError('40P01'), claim)).toBeInstanceOf(TransientError)
    expect(await raising(pgError('40003'), claim)).toMatchObject({code: '40003'})
    const lost = new Error('connection reset')
    expect(await raising(lost, claim)).toBe(lost)
  })

  test('an integrity violation at COMMIT of a commit with views is the projector failure', async () => {
    const {client} = fake({fail: failAtCommit})
    const repo = new PgRepository(client, {viewWriter: writer})
    const error = await rejection(commitViews(repo, putView('k', 'one', 'a', 'ledger', '{}')))
    expect(error).toBeInstanceOf(ProjectorError)
    expect(error).toMatchObject({projector: '(at commit)'})

    const bare = fake({fail: failAtCommit})
    const refused = await rejection(
      new PgRepository(bare.client).commit({entityId: 'a', events: [event(1)]}),
    )
    expect(refused).not.toBeInstanceOf(ProjectorError)
  })

  test.each<[string, string, ViewWriter | undefined, ViewOperation]>([
    ['no writer', 'viewsNotConfigured', undefined, putView('k', 'one', 'a', 'ledger', '{}')],
    [
      'an empty statement',
      'invalidViewOperation',
      () => ({sql: '  '}),
      putView('k', 'one', 'a', 'ledger', '{}'),
    ],
    [
      'an absent put without its key constraint',
      'invalidViewOperation',
      () => ({sql: 'INSERT INTO views VALUES ($1)', args: ['k']}),
      putView('k', 'one', 'a', 'ledger', '{}', {condition: 'absent'}),
    ],
  ])('refuses %s before any query', async (_name, code, viewWriter, op) => {
    const {client, log} = fake()
    const repo = new PgRepository(client, viewWriter && {viewWriter})
    const error = await rejection(commitViews(repo, op))
    expect(error).toMatchObject({code})
    expect(error instanceof ViewsNotConfiguredError).toBe(viewWriter === undefined)
    expect(log).toEqual([])
  })

  test("passes a writer's own error through, before any query", async () => {
    const refused = new Error('no table for this row')
    const {client, log} = fake()
    const repo = new PgRepository(client, {
      viewWriter: () => {
        throw refused
      },
    })
    expect(await rejection(commitViews(repo, putView('k', 'one', 'a', 'ledger', '{}')))).toBe(
      refused,
    )
    expect(log).toEqual([])
  })

  test('budgets each view as one action sized by its SQL text', async () => {
    const {client, log} = fake()
    const op = putView('k', 'one', 'a', 'ledger', '{}')
    const long = new PgRepository(client, {viewWriter: sized(400_000)})
    expect(await rejection(commitViews(long, op))).toBeInstanceOf(CommitTooLargeError)

    // Two control checks, the head, the event and the snapshot leave room for 95 views.
    const many = new PgRepository(client, {viewWriter: short})
    expect(await rejection(commitViews(many, ...puts(96)))).toBeInstanceOf(CommitTooLargeError)
    expect(log).toEqual([])

    const fits = fake({
      respond: (text) => (text.startsWith('SELECT 1 /*') ? {rows: [{}]} : undefined),
    })
    await commitViews(new PgRepository(fits.client, {viewWriter: short}), ...puts(95))
    expect(fits.log.at(-1)).toBe('COMMIT')
  })
})
