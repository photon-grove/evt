// Command outcomes at the repository level (ADR 0004, C3): the stored rejection and the bounds,
// then the outcome row each backend writes as a commit's last action and reads back. The rows'
// bytes are pinned by compat/fixtures/outcomes.json in packages/evt-compat.
import {describe, expect, test} from 'vitest'

import type {DynamoClient, Page, TransactItem} from '../src/dynamodb/client.ts'
import type {Item} from '../src/dynamodb/item.ts'
import {markerItem} from '../src/dynamodb/marker.ts'
import {outcomeItem, outcomeItemSize} from '../src/dynamodb/outcome.ts'
import {DynamoRepository} from '../src/dynamodb/repository.ts'
import {
  CommandIdTooLongError,
  ConflictError,
  EvtError,
  IndeterminateError,
  OutcomeConflictError,
  OutcomeTooLargeError,
  RebuildInProgressError,
} from '../src/errors.ts'
import {type SerializedEvent, eventId} from '../src/event.ts'
import {JsonNumber} from '../src/json.ts'
import {controlFixtures, MemRepository} from '../src/memory/repository.ts'
import {emptyMetadata} from '../src/metadata.ts'
import {
  type Outcome,
  canonicalRejection,
  checkCommandId,
  checkOutcome,
  defaultDigest,
  encodeRejection,
} from '../src/outcome.ts'
import type {PgClient, PgConnection, PgResult} from '../src/postgres/client.ts'
import {PgRepository} from '../src/postgres/repository.ts'
import {normalizeTransaction, transactionSizes} from '../src/repository.ts'

const encoder = new TextEncoder()

function event(entityId: string, sequence: number): SerializedEvent {
  return {
    id: eventId(entityId, BigInt(sequence)),
    sequence: BigInt(sequence),
    type: 'ledger:added',
    version: 1,
    entityId,
    entityType: 'ledger',
    payload: encoder.encode('{}'),
    metadata: emptyMetadata(),
  }
}

function accepted(commandId: string, overrides: Partial<Outcome> = {}): Outcome {
  return {
    commandId,
    status: 'accepted',
    digest: 'digest',
    response: null,
    eventIds: [],
    rejection: null,
    identity: {commandType: 'ledger:add', targets: ['a'], principal: null},
    ...overrides,
  }
}

function rejected(commandId: string): Outcome {
  return {
    ...accepted(commandId),
    status: 'rejected',
    rejection: {code: 'taken', message: 'the handle is taken', details: {handle: 'ada'}},
  }
}

async function caught(promise: Promise<unknown>): Promise<unknown> {
  return promise.then(
    () => undefined,
    (error: unknown) => error,
  )
}

describe('the stored rejection', () => {
  test("writes its details as Go maps: every object's keys sorted, numbers canonical", () => {
    const encoded = encodeRejection({
      code: 'c',
      message: 'm',
      details: {b: 1e21, a: {z: -0, y: [{d: 1, c: 2}]}},
    })

    expect(encoded).toBe(
      '{"code":"c","message":"m","details":{"a":{"y":[{"c":2,"d":1}],"z":0},"b":1000000000000000000000}}',
    )
  })

  test('reads back as Maps of exact numbers, so a first run equals a replay', () => {
    const rejection = canonicalRejection({
      code: 'c',
      message: 'm',
      details: {precise: new JsonNumber('9007199254740993'), scaled: new JsonNumber('1.50e1')},
    })
    const details = rejection.details as Map<string, JsonNumber>

    expect(details.get('precise')?.text).toBe('9007199254740993')
    expect(details.get('scaled')?.text).toBe('15.0')
    expect(encodeRejection(rejection)).toBe(
      encodeRejection({code: 'c', message: 'm', details: Object.fromEntries(details)}),
    )
  })

  test.each([
    ['details that are not an object', {code: 'c', message: 'm', details: [1]}],
    ['U+0000 in the message', {code: 'c', message: 'a\0b', details: null}],
    ['U+0000 in a nested key', {code: 'c', message: 'm', details: {a: {'b\0': 1}}}],
    ['a lone surrogate in the code', {code: 'c\ud800', message: 'm', details: null}],
    [
      'a number PostgreSQL cannot hold',
      {code: 'c', message: 'm', details: {n: new JsonNumber('1e-16384')}},
    ],
  ])('refuses %s as an invalid argument', (_, rejection) => {
    expect(() => encodeRejection(rejection)).toThrow(
      expect.objectContaining({code: 'invalidArgument'}),
    )
  })

  test('skips an undefined property, as the writer does on a plain object', () => {
    const encoded = encodeRejection({code: 'c', message: 'm', details: {b: undefined, a: 1}})

    expect(encoded).toBe('{"code":"c","message":"m","details":{"a":1}}')
  })

  test('refuses details nested past the call stack as an invalid argument', () => {
    let details: unknown = 1
    for (let i = 0; i < 100_000; i++) {
      details = {d: details}
    }

    expect(() => encodeRejection({code: 'c', message: 'm', details})).toThrow(
      expect.objectContaining({code: 'invalidArgument'}),
    )
  })

  test('refuses an encoding over 65536 bytes, counted on the canonical text', () => {
    const exponent = {code: 'c', message: 'm', details: {n: new JsonNumber('1e65536')}}
    const error = (() => {
      try {
        return encodeRejection(exponent)
      } catch (thrown) {
        return thrown
      }
    })()

    expect(error).toBeInstanceOf(OutcomeTooLargeError)
    expect(error).toMatchObject({what: 'rejection'})
  })
})

describe('the bounds', () => {
  test('a command ID is required, free of U+0000, and at most 512 bytes', () => {
    expect(() => checkCommandId('')).toThrow(expect.objectContaining({code: 'invalidArgument'}))
    expect(() => checkCommandId('a\0')).toThrow(expect.objectContaining({code: 'invalidArgument'}))
    expect(() => checkCommandId('é'.repeat(256))).not.toThrow()
    expect(() => checkCommandId(`${'é'.repeat(256)}a`)).toThrow(CommandIdTooLongError)
  })

  test('an outcome is checked in the pinned order before any write', () => {
    expect(() => checkOutcome(accepted('c', {digest: ''}))).toThrow(
      expect.objectContaining({code: 'invalidArgument'}),
    )
    expect(() => checkOutcome({...rejected('c'), status: 'accepted'})).toThrow(
      expect.objectContaining({code: 'invalidArgument'}),
    )
    expect(() => checkOutcome({...rejected('c'), rejection: {code: '', message: 'm'}})).toThrow(
      expect.objectContaining({code: 'invalidArgument'}),
    )
    expect(() => checkOutcome(accepted('c', {response: new Uint8Array(65537)}))).toThrow(
      expect.objectContaining({what: 'response', size: 65537}),
    )
    expect(() => checkOutcome(accepted('c', {response: new Uint8Array(65536)}))).not.toThrow()
  })

  test('the default digest is the worked example of the storage format', async () => {
    const digest = await defaultDigest({
      commandType: 'account:open',
      targets: ['acct-1', 'acct-1'],
      payload: encoder.encode('{"owner":"Ada"}'),
      principal: 'user:42',
    })

    expect(digest).toBe('08f63bf06ef7167f79dda7f5de9307e1ffb626de015ecbc1c1482d1e67e5abd6')
  })
})

describe('MemRepository outcomes', () => {
  test('an outcome-only commit records the row alone, and a read returns a copy', async () => {
    const repo = new MemRepository()
    const response = encoder.encode('{"ok":true}')
    await repo.commit({entityId: 'a', events: [], outcome: accepted('cmd-1', {response})})
    response[0] = 0

    const stored = await repo.outcome('cmd-1')
    expect(stored?.response).toEqual(encoder.encode('{"ok":true}'))
    expect(stored?.eventIds).toEqual([])
    expect(await repo.events('a')).toEqual([])
    expect(await repo.outcome('cmd-2')).toBeNull()
  })

  test('lists the events the commit wrote, by stream bytes, whatever the outcome said', async () => {
    const repo = new MemRepository()
    await repo.commitTransaction({
      streams: [
        {entityId: 'b', events: [event('b', 1), event('b', 2)]},
        {entityId: 'a', events: [event('a', 1)]},
      ],
      outcome: accepted('cmd-1', {eventIds: ['x:9']}),
    })

    expect((await repo.outcome('cmd-1'))?.eventIds).toEqual(['a:1', 'b:1', 'b:2'])
  })

  test('replays a rejection in its stored form', async () => {
    const repo = new MemRepository()
    await repo.commit({entityId: 'a', events: [], outcome: rejected('cmd-1')})

    const stored = await repo.outcome('cmd-1')
    expect(stored?.rejection?.code).toBe('taken')
    expect(stored?.rejection?.details).toEqual(new Map([['handle', 'ada']]))
  })

  test('a recorded outcome outranks a stream conflict, and writes nothing', async () => {
    const repo = new MemRepository()
    await repo.commit({entityId: 'a', events: [event('a', 1)], outcome: accepted('cmd-1')})

    const again = await caught(
      repo.commit({entityId: 'a', events: [event('a', 1)], outcome: accepted('cmd-1')}),
    )
    expect(again).toBeInstanceOf(OutcomeConflictError)
    expect(again).toMatchObject({commandId: 'cmd-1'})
    expect(await repo.events('a')).toHaveLength(1)
  })

  test('the rebuild lock outranks a recorded outcome', async () => {
    const repo = new MemRepository()
    await repo.commit({entityId: 'a', events: [], outcome: accepted('cmd-1')})
    controlFixtures(repo).setRebuild({generation: 1n, lock: 'held'})

    const commit = repo.commit({entityId: 'a', events: [], outcome: accepted('cmd-1')})
    await expect(commit).rejects.toBeInstanceOf(RebuildInProgressError)
  })

  test('a failed commit leaves no outcome row', async () => {
    const repo = new MemRepository()
    await repo.commit({entityId: 'a', events: [event('a', 1)]})

    const commit = repo.commit({entityId: 'a', events: [event('a', 1)], outcome: accepted('cmd-1')})
    await expect(commit).rejects.toBeInstanceOf(ConflictError)
    expect(await repo.outcome('cmd-1')).toBeNull()
  })

  test("refuses U+0000 in an event's metadata only when the commit records an outcome", async () => {
    const repo = new MemRepository()
    const bad = {...event('a', 1), metadata: {...emptyMetadata(), region: 'a\0'}}

    const recorded = repo.commit({entityId: 'a', events: [bad], outcome: accepted('cmd-1')})
    await expect(recorded).rejects.toMatchObject({code: 'invalidArgument'})
    await repo.commit({entityId: 'a', events: [bad]})
    expect(await repo.events('a')).toHaveLength(1)
  })

  test('an outcome-only commit checks the stream and fences it names', async () => {
    const repo = new MemRepository()
    const single = repo.commit({entityId: 'a\0b', events: [], outcome: accepted('cmd-1')})
    const listed = repo.commitTransaction({
      streams: [{entityId: 'b\ud800', events: []}],
      outcome: accepted('cmd-2'),
    })

    await expect(single).rejects.toMatchObject({code: 'invalidArgument'})
    await expect(listed).rejects.toMatchObject({code: 'invalidArgument'})
    expect(await repo.outcome('cmd-1')).toBeNull()
  })

  test('a read checks the command ID as a write does', async () => {
    const repo = new MemRepository()

    await expect(repo.outcome('')).rejects.toMatchObject({code: 'invalidArgument'})
    await expect(repo.outcome('x'.repeat(513))).rejects.toBeInstanceOf(CommandIdTooLongError)
  })

  test('the outcome put is the last action of the budget', () => {
    const outcome = rejected('cmd-1')
    const transaction = normalizeTransaction({
      streams: [{entityId: 'a', events: [event('a', 1)]}],
      outcome,
    })
    const sizes = transactionSizes(transaction, () => undefined)

    expect(sizes).toHaveLength(5)
    expect(sizes.at(-1)).toBe(outcomeItemSize({...outcome, eventIds: ['a:1']}))
  })
})

function markerRow(): Item {
  return markerItem({
    version: 1n,
    features: new Map([['stream-heads', 1]]),
    historyDependent: [],
    retention: [],
  })
}

function cancelled(...codes: string[]): Error {
  return Object.assign(new Error('Transaction cancelled'), {
    name: 'TransactionCanceledException',
    CancellationReasons: codes.map((Code) => ({Code})),
  })
}

// A client with a marker row, stored outcome items, and transactions that answer in turn.
class Scripted implements DynamoClient {
  readonly transactions: TransactItem[][] = []
  readonly reads: {Key: Item; ConsistentRead?: boolean}[] = []
  readonly outcomes = new Map<string, Item>()
  readonly #answers: (Error | null)[]

  constructor(...answers: (Error | null)[]) {
    this.#answers = answers
  }

  async getItem(input: {Key: Item; ConsistentRead?: boolean}): Promise<{Item?: Item}> {
    this.reads.push(input)
    const pk = input.Key['pk']
    if (pk && 'S' in pk && pk.S === 'SYSTEM#FEATURES') {
      return {Item: markerRow()}
    }
    const item = pk && 'S' in pk ? this.outcomes.get(pk.S) : undefined

    return item ? {Item: item} : {}
  }

  async putItem(): Promise<unknown> {
    throw new Error('unexpected putItem')
  }

  async query(): Promise<Page> {
    return {Items: []}
  }

  async scan(): Promise<Page> {
    return {Items: []}
  }

  async transactWriteItems(input: {TransactItems: TransactItem[]}): Promise<unknown> {
    this.transactions.push(input.TransactItems)
    const answer = this.#answers.shift()
    if (answer) {
      throw answer
    }

    return {}
  }
}

describe('DynamoRepository outcomes', () => {
  test('puts the outcome last, on the condition that none is recorded', async () => {
    const client = new Scripted()
    await new DynamoRepository(client, 'events').commitTransaction({
      streams: [{entityId: 'a', events: [event('a', 1)]}],
      fences: [{entityId: 'f', entityType: 'ledger', head: 2n, provenance: 'engine'}],
      outcome: accepted('cmd-1'),
    })

    // The control checks, the head update and event put, the fence check, then the outcome.
    const items = client.transactions[0]!
    expect(items).toHaveLength(6)
    expect(items.at(-1)).toEqual({
      Put: {
        TableName: 'events',
        Item: outcomeItem({...accepted('cmd-1'), eventIds: ['a:1']}),
        ConditionExpression: 'attribute_not_exists(pk)',
      },
    })
  })

  test('an outcome-only commit sends the control checks and the put', async () => {
    const client = new Scripted()
    await new DynamoRepository(client, 'events').commit({
      entityId: 'a',
      events: [],
      outcome: rejected('cmd-1'),
    })

    expect(client.transactions[0]!.map((item) => Object.keys(item)[0])).toEqual([
      'ConditionCheck',
      'ConditionCheck',
      'Put',
    ])
  })

  test('a lost outcome put outranks a failed stream, and is never a conflict', async () => {
    const failed = cancelled(
      'None',
      'None',
      'ConditionalCheckFailed',
      'None',
      'ConditionalCheckFailed',
    )
    const client = new Scripted(failed)
    const commit = new DynamoRepository(client, 'events').commit({
      entityId: 'a',
      events: [event('a', 1)],
      outcome: accepted('cmd-1'),
    })

    const error = await caught(commit)
    expect(error).toBeInstanceOf(OutcomeConflictError)
    expect(error).toMatchObject({commandId: 'cmd-1'})
  })

  test('the rebuild lock outranks a lost outcome put', async () => {
    const failed = cancelled('None', 'ConditionalCheckFailed', 'ConditionalCheckFailed')
    const commit = new DynamoRepository(new Scripted(failed), 'events').commit({
      entityId: 'a',
      events: [],
      outcome: accepted('cmd-1'),
    })

    await expect(commit).rejects.toBeInstanceOf(RebuildInProgressError)
  })

  test.each([
    ['a server fault', Object.assign(new Error('boom'), {$metadata: {httpStatusCode: 500}})],
    [
      'a 200 whose body could not be read',
      Object.assign(new Error('Unexpected end of JSON input'), {$metadata: {httpStatusCode: 200}}),
    ],
    ['a timeout with no response', Object.assign(new Error('timed out'), {name: 'TimeoutError'})],
    [
      'a transaction still in progress',
      Object.assign(new Error('busy'), {
        name: 'TransactionInProgressException',
        $metadata: {httpStatusCode: 400},
      }),
    ],
  ])('%s is indeterminate when the commit records an outcome', async (_, failure) => {
    const withOutcome = new DynamoRepository(new Scripted(failure), 'events').commit({
      entityId: 'a',
      events: [event('a', 1)],
      outcome: accepted('cmd-1'),
    })
    const without = new DynamoRepository(new Scripted(failure), 'events').commit({
      entityId: 'a',
      events: [event('a', 1)],
    })

    const error = await caught(withOutcome)
    expect(error).toBeInstanceOf(IndeterminateError)
    expect((error as EvtError).cause).toBe(failure)
    await expect(without).rejects.toBe(failure)
  })

  test('a definitive refusal stays as it is', async () => {
    const refused = Object.assign(new Error('bad'), {
      name: 'ValidationException',
      $metadata: {httpStatusCode: 400},
    })
    const commit = new DynamoRepository(new Scripted(refused), 'events').commit({
      entityId: 'a',
      events: [],
      outcome: accepted('cmd-1'),
    })

    await expect(commit).rejects.toBe(refused)
  })

  test('reads the row strongly consistent, even when reads are eventually consistent', async () => {
    const client = new Scripted()
    client.outcomes.set('command-outcome#cmd-1', outcomeItem(rejected('cmd-1')))
    const repo = new DynamoRepository(client, 'events', {consistentRead: false})

    const stored = await repo.outcome('cmd-1')
    expect(stored?.rejection?.code).toBe('taken')
    expect(client.reads.at(-1)).toMatchObject({
      Key: {pk: {S: 'command-outcome#cmd-1'}, sk: {N: '1'}},
      ConsistentRead: true,
    })
    expect(await repo.outcome('cmd-2')).toBeNull()
  })
})

// The rows of a store that declares stream-heads, holds no rebuild lock and accepts every write.
function pgAnswer(text: string, stored: Record<string, unknown> | undefined): PgResult {
  if (text.includes('to_regclass')) {
    return {rows: [{}]}
  }
  if (text.startsWith('SELECT version::text')) {
    const declared = {features: '{"stream-heads":1}', history_dependent: '[]', retention: '[]'}
    return {rows: [{version: '1', ...declared}]}
  }
  if (text.startsWith('SELECT 1 AS recorded') || text.startsWith('SELECT command_id')) {
    return {rows: stored ? [stored] : []}
  }
  if (text.startsWith('INSERT') || text.startsWith('UPDATE')) {
    return {rows: [{}]}
  }

  return {rows: []}
}

function pg(options: {fail?: (text: string) => unknown; stored?: Record<string, unknown>} = {}): {
  client: PgClient
  log: string[]
  values: (unknown[] | undefined)[]
} {
  const log: string[] = []
  const values: (unknown[] | undefined)[] = []
  const query = async (text: string, params?: unknown[]): Promise<PgResult> => {
    log.push(text)
    values.push(params)
    const failure = options.fail?.(text)
    if (failure !== undefined) {
      throw failure
    }

    return pgAnswer(text, options.stored)
  }
  const client: PgClient = {
    query,
    connect: async (): Promise<PgConnection> => ({query, release: () => undefined}),
  }

  return {client, log, values}
}

const REJECTED = '{"code": "c", "details": null, "message": "m"}'

function heads(log: string[]): string[] {
  const start = log.indexOf('START TRANSACTION ISOLATION LEVEL READ COMMITTED')

  return log.slice(start).map((text) => text.split(/\s+/).slice(0, 4).join(' '))
}

describe('PgRepository outcomes', () => {
  test('probes for a recorded outcome after the rebuild lock, and inserts it last', async () => {
    const {client, log, values} = pg()
    await new PgRepository(client).commit({
      entityId: 'a',
      events: [event('a', 1)],
      outcome: rejected('cmd-1'),
    })

    expect(heads(log)).toEqual([
      'START TRANSACTION ISOLATION LEVEL',
      'SELECT pg_advisory_xact_lock(1702261760, hashtext($1))',
      'SELECT pg_advisory_xact_lock_shared(1702261761, hashtext($1))',
      'SELECT 1 WHERE to_regclass($1)',
      'SELECT version::text AS version,',
      'SELECT pg_advisory_xact_lock_shared(1702261761, hashtext($1))',
      'SELECT 1 WHERE to_regclass($1)',
      'SELECT 1 WHERE EXISTS',
      'SELECT 1 AS recorded',
      'INSERT INTO evt_events__evt_stream_heads (entity_id,',
      'INSERT INTO evt_events (entity_id,',
      'INSERT INTO evt_events__evt_command_outcomes (command_id,',
      'COMMIT',
    ])
    expect(values[log.length - 2]).toEqual([
      'cmd-1',
      'rejected',
      'digest',
      null,
      '["a:1"]',
      '{"code":"taken","message":"the handle is taken","details":{"handle":"ada"}}',
      'taken',
      'ledger:add',
      '["a"]',
      null,
    ])
  })

  test('a recorded outcome is an outcome conflict before any stream statement', async () => {
    const {client, log} = pg({stored: {recorded: 1}})
    const commit = new PgRepository(client).commit({
      entityId: 'a',
      events: [event('a', 1)],
      outcome: accepted('cmd-1'),
    })

    await expect(commit).rejects.toBeInstanceOf(OutcomeConflictError)
    expect(log.some((text) => text.startsWith('INSERT INTO evt_events'))).toBe(false)
    expect(log.at(-1)).toBe('ROLLBACK')
  })

  test("the insert's unique violation is an outcome conflict", async () => {
    const {client} = pg({
      fail: (text) =>
        text.startsWith('INSERT INTO evt_events__evt_command_outcomes')
          ? Object.assign(new Error('duplicate key'), {code: '23505'})
          : undefined,
    })
    const commit = new PgRepository(client).commit({
      entityId: 'a',
      events: [],
      outcome: accepted('cmd-1'),
    })

    await expect(commit).rejects.toBeInstanceOf(OutcomeConflictError)
  })

  test('a COMMIT lost without a database error is indeterminate, with an outcome only', async () => {
    const lost = Object.assign(new Error('read ECONNRESET'), {code: 'ECONNRESET'})
    const fail = (text: string): unknown => (text === 'COMMIT' ? lost : undefined)

    const withOutcome = new PgRepository(pg({fail}).client).commit({
      entityId: 'a',
      events: [],
      outcome: accepted('cmd-1'),
    })
    const without = new PgRepository(pg({fail}).client).commit({
      entityId: 'a',
      events: [event('a', 1)],
    })

    await expect(withOutcome).rejects.toBeInstanceOf(IndeterminateError)
    await expect(without).rejects.toBe(lost)
  })

  test.each(['23503', '2F005', '0A000'])(
    'a database error %s at COMMIT stays as it is',
    async (code) => {
      const deferred = Object.assign(new Error(code), {code})
      const commit = new PgRepository(
        pg({fail: (text) => (text === 'COMMIT' ? deferred : undefined)}).client,
      ).commit({entityId: 'a', events: [], outcome: accepted('cmd-1')})

      await expect(commit).rejects.toBe(deferred)
    },
  )

  test('a lost connection before COMMIT stays as it is', async () => {
    const lost = Object.assign(new Error('read ECONNRESET'), {code: 'ECONNRESET'})
    const before = new PgRepository(
      pg({fail: (text) => (text.startsWith('INSERT INTO evt_events (') ? lost : undefined)}).client,
    ).commit({entityId: 'a', events: [event('a', 1)], outcome: accepted('cmd-1')})

    await expect(before).rejects.toBe(lost)
  })

  test('an abort while COMMIT is in flight is indeterminate with an outcome', async () => {
    const controller = new AbortController()
    const {client} = pg({
      fail: (text) => {
        if (text === 'COMMIT') {
          controller.abort(new Error('stop'))
        }
      },
    })
    const commit = new PgRepository(client).commit(
      {entityId: 'a', events: [], outcome: accepted('cmd-1')},
      {signal: controller.signal},
    )

    await expect(commit).rejects.toBeInstanceOf(IndeterminateError)
  })

  const row = {
    command_id: 'cmd-1',
    status: 'accepted',
    digest: 'd',
    response: null,
    events: '[]',
    rejection: null,
    code: null,
    command_type: null,
    targets: null,
    principal: null,
  }

  test.each([
    ['an unknown status', {status: 'pending'}],
    ['an empty digest', {digest: ''}],
    ['an accepted row with a code', {code: 'c'}],
    ['a rejected row whose code disagrees', {status: 'rejected', rejection: REJECTED, code: 'd'}],
    ['a rejected row without a rejection', {status: 'rejected', code: 'c'}],
    ['a malformed event ID', {events: '["a:01"]'}],
    ['targets without a command type', {targets: '[]'}],
    ['a command type without targets', {command_type: 't'}],
    ['targets that hold a null', {command_type: 't', targets: '["a",null]'}],
  ])('refuses a stored row with %s as corrupt', async (_, change) => {
    const {client} = pg({stored: {...row, ...change}})
    const read = new PgRepository(client).outcome('cmd-1')

    await expect(read).rejects.toMatchObject({code: 'invalidRecord'})
  })

  test('reads a row, whatever key order and spacing jsonb gave its JSON', async () => {
    const {client} = pg({
      stored: {
        command_id: 'cmd-1',
        status: 'rejected',
        digest: 'd',
        response: '7b7d',
        events: '["a:1"]',
        rejection: '{"code": "taken", "details": {"n": 1.50}, "message": "m"}',
        code: 'taken',
        command_type: 't',
        targets: '["a"]',
        principal: null,
      },
    })

    const stored = await new PgRepository(client).outcome('cmd-1')
    expect(stored).toMatchObject({
      commandId: 'cmd-1',
      status: 'rejected',
      response: encoder.encode('{}'),
      eventIds: ['a:1'],
      identity: {commandType: 't', targets: ['a'], principal: null},
    })
    expect(encodeRejection(stored!.rejection!)).toBe(
      '{"code":"taken","message":"m","details":{"n":1.50}}',
    )
  })
})
