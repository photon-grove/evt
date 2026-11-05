// The TypeScript driver of the cross-language run (scripts/compat-integration.sh, ADR 0009):
//
//   live.js scenarios
//   live.js step   --backend dynamo|postgres --run RUN --index N --as go|rust
//   live.js verify --backend dynamo|postgres --run RUN
//
// TypeScript joins the run as a stand-in. step executes step N of every scenario TypeScript
// supports whose writer is the language --as names, and stamps that language's name into the
// event's origin and commit ID, so the streams match the fixtures byte for byte. In such a pass
// the other language's driver writes its own steps of the same scenarios, limited to the list
// scenarios prints (-scenarios or --scenarios). verify checks the streams of the supported
// scenarios against the golden fixtures: the events and snapshot read through the repository, the
// loaded state and the head, and what the backend stores: the raw DynamoDB items and the SNS
// messages they publish, or the PostgreSQL event and snapshot rows.
//
// It runs against a local DynamoDB emulator, such as Moto, at AWS_ENDPOINT_URL, never real AWS,
// or a local PostgreSQL server at DATABASE_URL, in the default tables the Go and Rust drivers use.
// TypeScript has no event-log enumeration yet, so it doesn't join the enumeration check.

// Each step needs the one before it, so the driver awaits in loops on purpose.
/* oxlint-disable no-await-in-loop */
import {type AttributeValue, DynamoDB} from '@aws-sdk/client-dynamodb'
import process from 'node:process'
import {Pool} from 'pg'

import {
  type JsonValue,
  type Repository,
  type SerializedEvent,
  decodeEvent,
  decodeSnapshot,
  encodeEnvelope,
  encodeEvent,
  encodeMetadata,
  encodeSnapshot,
  parseJson,
} from '@photon-grove/evt'
import {
  DynamoRepository,
  type Item,
  eventFromItem,
  eventItem,
  headFromItem,
  outcomeFromItem,
  outcomeKey,
} from '@photon-grove/evt/dynamodb'
import {PgRepository, outcomeRow, outcomesTable, streamHeadsTable} from '@photon-grove/evt/postgres'

import {
  Checks,
  canonical,
  field,
  list,
  loadFixture,
  loadFixtureForRun,
  plain,
  str,
} from './harness.js'
import {
  ORIGIN_SOURCE,
  type Observed,
  Runner,
  SUPPORTED_SCENARIOS,
  checkScenario,
  checkStep,
  compareObserved,
  compareStream,
  expectedOutcome,
  optionalList,
} from './runner.js'

const WRITERS = ['go', 'rust']

interface Args {
  command: string
  backend: string
  run: string
  index: number
  as: string
}

/** A backend the run writes, and how to read a stream back as it stores it. */
interface Backend {
  readonly repository: Repository
  /** Appends an event to the log without maintaining the stream's head. */
  bypassHead(event: SerializedEvent): Promise<void>
  observe(runner: Runner, id: string): Promise<Observed>
  /** Compares what the backend stores for a stream beyond what `observe` reads. */
  checkRows(checks: Checks, name: string, id: string, expected: JsonValue): Promise<void>
  /** Compares a command's stored outcome row with its fixture's DynamoDB item. */
  checkOutcome(checks: Checks, name: string, commandId: string, expected: JsonValue): Promise<void>
  close(): Promise<void>
}

const cli = parseArgs(process.argv.slice(2))

if (cli.command === 'scenarios') {
  console.log(SUPPORTED_SCENARIOS.join(','))
} else {
  if (cli.run === '') {
    throw new Error('--run is required')
  }

  const backend = await openBackend(cli.backend)
  const checks = new Checks()
  let quiet = false
  try {
    switch (cli.command) {
      case 'step':
        if (!WRITERS.includes(cli.as)) {
          throw new Error(`--as must be go or rust, not ${cli.as || '-'}`)
        }
        // An index with no step of the stood-in language's checks nothing, so it reports nothing.
        quiet = (await runStep(checks, backend, cli)) === 0
        break
      case 'verify':
        await verify(checks, backend, cli.run)
        break
      default:
        throw new Error(`unknown command ${cli.command}`)
    }
  } finally {
    await backend.close()
  }

  if (!quiet) {
    checks.report()
  }
}

function parseArgs(argv: string[]): Args {
  const [command, ...flags] = argv
  if (command === undefined) {
    throw new Error('missing command: scenarios | step | verify')
  }

  const parsed: Args = {command, backend: '', run: '', index: 0, as: ''}
  for (let i = 0; i < flags.length; i += 2) {
    const flag = flags[i]
    const value = flags[i + 1]
    if (value === undefined) {
      throw new Error(`missing value for ${flag}`)
    }

    switch (flag) {
      case '--backend':
        parsed.backend = value
        break
      case '--run':
        parsed.run = value
        break
      case '--as':
        parsed.as = value
        break
      case '--index':
        if (!/^\d+$/.test(value)) {
          throw new Error(`invalid index ${value}`)
        }
        parsed.index = Number(value)
        break
      default:
        throw new Error(`unknown flag ${flag}`)
    }
  }

  return parsed
}

async function openBackend(name: string): Promise<Backend> {
  switch (name) {
    case 'dynamo':
      return dynamoBackend()
    case 'postgres':
      return postgresBackend()
    default:
      throw new Error(`--backend must be dynamo or postgres, not ${name || '-'}`)
  }
}

// Only a local server: a loopback address or a .localhost name.
function local(url: string, variable: string, what: string): string {
  const host = URL.canParse(url) ? new URL(url).hostname : ''
  if (!['localhost', '127.0.0.1', '[::1]'].includes(host) && !host.endsWith('.localhost')) {
    throw new Error(`${variable} must name a local ${what}: ${url}`)
  }

  return url
}

function dynamoBackend(): Backend {
  const endpoint = local(
    process.env['AWS_ENDPOINT_URL'] ?? 'http://localhost:4566',
    'AWS_ENDPOINT_URL',
    'DynamoDB emulator',
  )

  // One attempt, as the Go driver makes, so a refused request fails the step instead of retrying.
  const client = new DynamoDB({
    endpoint,
    region: process.env['AWS_REGION'] ?? 'us-west-2',
    maxAttempts: 1,
    credentials: {
      accessKeyId: process.env['AWS_ACCESS_KEY_ID'] ?? 'test',
      secretAccessKey: process.env['AWS_SECRET_ACCESS_KEY'] ?? 'test',
    },
  })
  const table = process.env['EVT_COMPAT_DYNAMO_TABLE'] ?? 'evt-local-event-log'
  // Lazily initializing, as the Go and Rust drivers are, so a fresh table takes the first write.
  const repository = new DynamoRepository(client, table, {lazyInitialization: true})

  return {
    repository,
    // The event item alone, with none of the head update a commit carries, as Go's driver puts it.
    async bypassHead(event) {
      await client.putItem({
        TableName: table,
        Item: eventItem(event),
        ConditionExpression: 'attribute_not_exists(sk)',
      })
    },
    // A stream as the table stores it and as the repository reads it back.
    async observe(runner, id) {
      const items = await partition(client, table, id)
      const head = items.find((item) => sortKey(item) === '-1')
      const eventItems = items.filter((item) => !['-1', '0'].includes(sortKey(item)))
      const snapshot = await repository.snapshot(id)

      return {
        ...(await runner.load(id)),
        events: (await repository.events(id)).map(encodeEvent),
        snapshot: snapshot && encodeSnapshot(snapshot),
        head: head === undefined ? null : headFromItem(head).head,
        dynamoItems: items,
        snsMessages: eventItems.map((item) => encodeEnvelope(eventFromItem(item), ORIGIN_SOURCE)),
      }
    },
    // The items are compared with the stream itself.
    async checkRows() {},
    async checkOutcome(checks, name, commandId, expected) {
      const items = await partition(client, table, outcomeKey(commandId))
      checks.equal(`${name}: outcome ${commandId} item`, canonical(items), canonical([expected]))
    },
    async close() {
      client.destroy()
    },
  }
}

async function postgresBackend(): Promise<Backend> {
  // The local-only placeholder credentials infra/local-postgres creates, as the Go driver uses.
  const url = local(
    process.env['DATABASE_URL'] || 'postgres://evt:evt@localhost:5432/evt_local',
    'DATABASE_URL',
    'PostgreSQL server',
  )
  const pool = new Pool({connectionString: url, max: 2})
  const repository = new PgRepository(pool)
  try {
    // As the Go and Rust drivers do: whichever language writes first creates the schema.
    await repository.ensureSchema()
  } catch (error) {
    await pool.end()
    throw error
  }

  return {
    repository,
    // The event row alone, leaving the stream's head row as it was, as Go's driver inserts it.
    async bypassHead(event) {
      await pool.query(
        `INSERT INTO ${repository.tables.events} (entity_id, sequence, event_id, event_type, ` +
          'version, entity_type, payload, metadata) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)',
        [
          event.entityId,
          event.sequence.toString(),
          event.id,
          event.type,
          event.version,
          event.entityType,
          event.payload ?? new Uint8Array(),
          encodeMetadata(event.metadata),
        ],
      )
    },
    async observe(runner, id) {
      const snapshot = await repository.snapshot(id)
      const {rows} = await pool.query<{head: string}>(
        `SELECT head::text AS head FROM ${streamHeadsTable(repository.tables)} ` +
          'WHERE entity_id = $1',
        [id],
      )

      return {
        ...(await runner.load(id)),
        events: (await repository.events(id)).map(encodeEvent),
        snapshot: snapshot && encodeSnapshot(snapshot),
        head: rows[0] === undefined ? null : BigInt(rows[0].head),
      }
    },
    // The event and snapshot rows as stored, as the Go driver compares them: each event's
    // metadata as JSON, since jsonb normalizes its text.
    async checkRows(checks, name, id, expected) {
      const {events, snapshots} = repository.tables
      const stored = await pool.query<Record<string, string>>(
        'SELECT entity_id, sequence::text AS sequence, event_id, event_type, ' +
          "version::text AS version, entity_type, encode(payload, 'hex') AS payload, " +
          `metadata::text AS metadata FROM ${events} WHERE entity_id = $1 ` +
          // The column, not the text alias, which would put 10 before 9.
          `ORDER BY ${events}.sequence`,
        [id],
      )
      const want = list(field(expected, 'events')).map((encoded) => {
        const event = decodeEvent(str(encoded))
        return {
          entity_id: event.entityId,
          sequence: event.sequence.toString(),
          event_id: event.id,
          event_type: event.type,
          version: event.version.toString(),
          entity_type: event.entityType,
          payload: hex(event.payload),
          metadata: parseJson(encodeMetadata(event.metadata)),
        }
      })
      const got = stored.rows.map((row) =>
        Object.assign({}, row, {metadata: parseJson(row['metadata']!)}),
      )
      checks.equal(`${name}: event rows`, canonical(got), canonical(want))

      const snapshot = await pool.query<Record<string, string>>(
        'SELECT entity_id, entity_type, sequence::text AS sequence, ' +
          "event_sequence::text AS event_sequence, encode(payload, 'hex') AS payload " +
          `FROM ${snapshots} WHERE entity_id = $1`,
        [id],
      )
      const encoded = field(expected, 'snapshot')
      const wantSnapshot = encoded === null ? null : decodeSnapshot(str(encoded))
      checks.equal(
        `${name}: snapshot row`,
        canonical(snapshot.rows[0] ?? null),
        canonical(
          wantSnapshot && {
            entity_id: wantSnapshot.entityId,
            entity_type: wantSnapshot.entityType,
            sequence: wantSnapshot.sequence.toString(),
            event_sequence: wantSnapshot.eventSequence.toString(),
            payload: hex(wantSnapshot.payload),
          },
        ),
      )
    },
    // The row as stored, its jsonb columns parsed, against the row the fixture's item encodes to.
    async checkOutcome(checks, name, commandId, expected) {
      const {rows} = await pool.query<Record<string, string | null>>(
        "SELECT command_id, status, digest, encode(response, 'hex') AS response, " +
          'events::text AS events, rejection::text AS rejection, code, command_type, ' +
          `targets::text AS targets, principal FROM ${outcomesTable(repository.tables)} ` +
          'WHERE command_id = $1',
        [commandId],
      )
      const got = rows.map((row) =>
        Object.assign({}, row, {
          events: jsonColumn(row['events']),
          rejection: jsonColumn(row['rejection']),
          targets: jsonColumn(row['targets']),
        }),
      )
      const row = outcomeRow(outcomeFromItem(plain(expected) as Item))
      const want = {
        ...row,
        response: row.response === null ? null : hex(row.response),
        events: jsonColumn(row.events),
        rejection: jsonColumn(row.rejection),
        targets: jsonColumn(row.targets),
      }
      checks.equal(`${name}: outcome ${commandId} row`, canonical(got), canonical([want]))
    },
    async close() {
      await pool.end()
    },
  }
}

// A jsonb column as its parsed value, since jsonb normalizes its text.
function jsonColumn(text: string | null | undefined): unknown {
  return text === null || text === undefined ? null : plain(parseJson(text))
}

async function supportedScenarios(): Promise<JsonValue[]> {
  const definitions = list(field(await loadFixture('../scenarios.json'), 'scenarios'))

  return SUPPORTED_SCENARIOS.map((name) => {
    const scenario = definitions.find((definition) => str(field(definition, 'name')) === name)
    if (scenario === undefined) {
      throw new Error(`scenario ${name} is not defined`)
    }
    checkScenario(scenario)

    return scenario
  })
}

async function runStep(checks: Checks, backend: Backend, args: Args): Promise<number> {
  let executed = 0
  for (const scenario of await supportedScenarios()) {
    const name = str(field(scenario, 'name'))
    const step = list(field(scenario, 'steps'))[args.index]
    if (step === undefined || str(field(step, 'by')) !== args.as) {
      continue
    }
    checkStep(step, args.index)

    const runner = new Runner(backend.repository, scenario, args.run, {
      bypassHead: (event) => backend.bypassHead(event),
    })
    const got = await runner.step(step)
    const want = expectedOutcome(field(step, 'expect'))
    checks.equal(`${name} step ${args.index}`, canonical(got), canonical(want))
    console.log(`ts as ${args.as} step ${args.index} ${name}: ${got.result}`)
    executed++
  }

  return executed
}

async function verify(checks: Checks, backend: Backend, run: string): Promise<void> {
  const fixtures = await loadFixtureForRun('scenarios.json', run)
  const expectations = new Map(
    list(field(fixtures, 'scenarios')).map((expected) => [str(field(expected, 'name')), expected]),
  )

  for (const scenario of await supportedScenarios()) {
    const name = str(field(scenario, 'name'))
    const expected = expectations.get(name)
    if (expected === undefined) {
      checks.equal(`scenario ${name} has a fixture`, false, true)
      continue
    }

    const runner = new Runner(backend.repository, scenario, run)
    checks.equal(`${name}: entity ID`, runner.entityId, str(field(expected, 'entityId')))
    compareObserved(checks, name, await backend.observe(runner, runner.entityId), expected)
    await backend.checkRows(checks, name, runner.entityId, expected)

    // The outcome rows the scenario's commands left.
    for (const row of optionalList(expected, 'outcomes')) {
      const commandId = str(field(row, 'commandId'))
      await backend.checkOutcome(checks, name, commandId, field(row, 'dynamoItem'))
    }

    // Every other stream the scenario touches.
    for (const sibling of optionalList(expected, 'siblings')) {
      const id = str(field(sibling, 'entityId'))
      const label = `${name} sibling ${id}`
      compareStream(checks, label, await backend.observe(runner, id), sibling)
      await backend.checkRows(checks, label, id, sibling)
    }
  }
}

// An event-log item's numeric sort key: -1 for the head, 0 for the snapshot, or the sequence.
function sortKey(item: Item): string {
  const sk = item['sk']

  return sk !== undefined && 'N' in sk ? sk.N : ''
}

// Every item of a stream's partition, in sort-key order: the head, the snapshot, then the events.
async function partition(client: DynamoDB, table: string, pk: string): Promise<Item[]> {
  const items: Item[] = []
  let start: Record<string, AttributeValue> | undefined
  do {
    const page = await client.query({
      TableName: table,
      ConsistentRead: true,
      KeyConditionExpression: 'pk = :pk',
      ExpressionAttributeValues: {':pk': {S: pk}},
      ...(start && {ExclusiveStartKey: start}),
    })
    items.push(...((page.Items ?? []) as Item[]))
    start = page.LastEvaluatedKey
  } while (start !== undefined)

  return items
}

// Bytes as lowercase hex, as encode(payload, 'hex') writes them. A null payload is stored empty.
function hex(bytes: Uint8Array | null): string {
  return Array.from(bytes ?? [], (byte) => byte.toString(16).padStart(2, '0')).join('')
}
