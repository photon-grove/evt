// The TypeScript driver of the cross-language run (scripts/compat-integration.sh, ADR 0009):
//
//   live.js scenarios --backend dynamo|postgres
//   live.js exclusive --backend dynamo|postgres
//   live.js step   --backend dynamo|postgres --run RUN --index N --as go|rust [--only NAME]
//   live.js verify --backend dynamo|postgres --run RUN
//   live.js enumerate --backend dynamo|postgres --run RUN
//
// TypeScript joins the run as a stand-in. step executes step N of every scenario TypeScript
// supports whose writer is the language --as names, and stamps that language's name into the
// event's origin and commit ID, so the streams match the fixtures byte for byte. An exclusive
// scenario holds store-wide state across its steps (a declared feature refuses every other
// scenario's commits), so step skips it unless --only names it, which runs that scenario alone;
// exclusive lists the exclusive scenarios TypeScript supports. In such a pass
// the other language's driver writes its own steps of the same scenarios, limited to the list
// scenarios prints (-scenarios or --scenarios). verify checks the streams of the supported
// scenarios against the golden fixtures: the events and snapshot read through the repository, the
// loaded state and the head, and what the backend stores: the raw DynamoDB items and the SNS
// messages they publish, or the PostgreSQL event and snapshot rows, and the view rows commit-wide
// projectors wrote.
//
// enumerate prints every event of the run the event log yields, unfiltered and narrowed to the
// account type, as the Go and Rust drivers print them, so the script diffs the three.
//
// It runs against a local DynamoDB emulator, such as Moto, at AWS_ENDPOINT_URL, never real AWS,
// or a local PostgreSQL server at DATABASE_URL, in the default tables the Go and Rust drivers use.

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
  compareUtf8,
  isEventLog,
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
  type MarkerDriver,
  checkScenario,
  checkStep,
  compareObserved,
  compareStream,
  expectedOutcome,
  isExclusive,
  optionalList,
} from './runner.js'
import {dynamoControls, postgresControls} from './controls.js'
import {POSTGRES_VIEW_TABLE, postgresViewDdl, postgresViewWriter} from './pgviews.js'
import {viewPartitions} from './scenarios.js'

const WRITERS = ['go', 'rust']

interface Args {
  command: string
  backend: string
  run: string
  index: number
  as: string
  only: string
}

/** A backend the run writes, and how to read a stream back as it stores it. */
interface Backend {
  readonly repository: Repository
  /** Appends an event to the log without maintaining the stream's head. */
  bypassHead(event: SerializedEvent): Promise<void>
  /** Declares on the store's feature manifest, as another writer would. */
  readonly marker: MarkerDriver
  observe(runner: Runner, id: string): Promise<Observed>
  /** Compares what the backend stores for a stream beyond what `observe` reads. */
  checkRows(checks: Checks, name: string, id: string, expected: JsonValue): Promise<void>
  /**
   * Compares the view rows of `partitions` whose key names `base` with the fixture's, which
   * records them as DynamoDB items.
   */
  checkViews(
    checks: Checks,
    name: string,
    partitions: readonly string[],
    base: string,
    expected: readonly JsonValue[],
  ): Promise<void>
  /** Compares a command's stored outcome row with its fixture's DynamoDB item. */
  checkOutcome(checks: Checks, name: string, commandId: string, expected: JsonValue): Promise<void>
  close(): Promise<void>
}

const cli = parseArgs(process.argv.slice(2))

if (cli.command === 'scenarios') {
  console.log(scenarioNames(cli.backend).join(','))
} else if (cli.command === 'exclusive') {
  for (const scenario of await supportedScenarios(cli.backend)) {
    if (isExclusive(scenario)) {
      console.log(str(field(scenario, 'name')))
    }
  }
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
        await verify(checks, backend, cli.backend, cli.run)
        break
      case 'enumerate':
        await enumerate(backend, cli.run)
        quiet = true
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
    throw new Error('missing command: scenarios | exclusive | step | verify | enumerate')
  }

  const parsed: Args = {command, backend: '', run: '', index: 0, as: '', only: ''}
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
      case '--only':
        parsed.only = value
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
  const viewsTable = process.env['EVT_COMPAT_DYNAMO_VIEWS_TABLE'] ?? 'evt-local-entity-views'
  // Lazily initializing, as the Go and Rust drivers are, so a fresh table takes the first write.
  const repository = new DynamoRepository(client, table, {lazyInitialization: true, viewsTable})

  return {
    repository,
    marker: dynamoControls(client, table),
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
    async checkViews(checks, name, partitions, base, expected) {
      const rows: unknown[] = []
      for (const pk of partitions) {
        const items = await partition(client, viewsTable, pk)
        const owned = items.filter((item) => ownsViewRow(base, pk, textOf(item['sk'])))
        owned.sort((a, b) => compareUtf8(textOf(a['sk']), textOf(b['sk'])))
        rows.push(...owned.map((item) => ({pk, sk: textOf(item['sk']), dynamoItem: item})))
      }
      checks.equal(`${name}: views`, canonical(rows), canonical(expected.map(plain)))
    },
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
  // The view writer the Go and Rust drivers use, on the view table they create.
  const repository = new PgRepository(pool, {viewWriter: postgresViewWriter()})
  try {
    // As the Go and Rust drivers do: whichever language writes first creates the schema.
    await repository.ensureSchema()
    await pool.query(postgresViewDdl())
  } catch (error) {
    await pool.end()
    throw error
  }

  return {
    repository,
    marker: postgresControls(pool, repository.tables.events),
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
    // The rows as the neutral five columns, against the fixture items', as Go's driver compares
    // them.
    async checkViews(checks, name, partitions, base, expected) {
      const {rows} = await pool.query<Record<string, string>>(
        'SELECT pk, sk, entity_id, entity_type, payload FROM ' +
          `${POSTGRES_VIEW_TABLE} WHERE pk = ANY($1)`,
        [partitions],
      )
      const got = rows
        .filter((row) => ownsViewRow(base, row['pk']!, row['sk']!))
        .map((row) => ({
          pk: row['pk']!,
          sk: row['sk']!,
          entityId: row['entity_id']!,
          entityType: row['entity_type']!,
          payload: row['payload']!,
        }))
      const want = expected.map((view) => {
        const item = field(view, 'dynamoItem')
        const attribute = (key: string) => str(field(field(item, key), 'S'))

        return {
          pk: attribute('pk'),
          sk: attribute('sk'),
          entityId: attribute('entityID'),
          entityType: attribute('entityType'),
          payload: attribute('payload'),
        }
      })
      checks.equal(`${name}: views`, canonical(byKey(got)), canonical(byKey(want)))
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

// The scenarios TypeScript supports, which it runs on every backend.
function scenarioNames(backend: string): string[] {
  if (!['dynamo', 'postgres'].includes(backend)) {
    throw new Error(`--backend must be dynamo or postgres, not ${backend || '-'}`)
  }

  return [...SUPPORTED_SCENARIOS]
}

async function supportedScenarios(backend: string): Promise<JsonValue[]> {
  const definitions = list(field(await loadFixture('../scenarios.json'), 'scenarios'))

  return scenarioNames(backend).map((name) => {
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
  for (const scenario of await supportedScenarios(args.backend)) {
    const name = str(field(scenario, 'name'))
    // The interleaved run skips exclusive scenarios; a run with --only executes that one alone.
    if (isExclusive(scenario) !== (args.only !== '') || (args.only !== '' && name !== args.only)) {
      continue
    }
    const step = list(field(scenario, 'steps'))[args.index]
    if (step === undefined || str(field(step, 'by')) !== args.as) {
      continue
    }
    checkStep(step, args.index)

    const runner = new Runner(backend.repository, scenario, args.run, {
      bypassHead: (event) => backend.bypassHead(event),
      marker: backend.marker,
    })
    const got = await runner.step(step)
    const want = expectedOutcome(field(step, 'expect'))
    checks.equal(`${name} step ${args.index}`, canonical(got), canonical(want))
    console.log(`ts as ${args.as} step ${args.index} ${name}: ${got.result}`)
    executed++
  }

  return executed
}

async function verify(
  checks: Checks,
  backend: Backend,
  backendName: string,
  run: string,
): Promise<void> {
  const fixtures = await loadFixtureForRun('scenarios.json', run)
  const expectations = new Map(
    list(field(fixtures, 'scenarios')).map((expected) => [str(field(expected, 'name')), expected]),
  )

  for (const scenario of await supportedScenarios(backendName)) {
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

    // The view rows the scenario's projectors left in its own partitions.
    const partitions = viewPartitions(scenario, runner.entityId)
    if (partitions.length > 0) {
      const views = optionalList(expected, 'views')
      await backend.checkViews(checks, name, partitions, runner.entityId, views)
    }

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

// Whether a row of a shared view partition belongs to the run's scenario: its partition or sort key
// names the scenario's entity ID, as Go's OwnsViewRow decides.
function ownsViewRow(base: string, pk: string, sk: string): boolean {
  return pk.includes(base) || sk.includes(base)
}

// Neutral view rows by partition, then sort key bytes.
function byKey<T extends {pk: string; sk: string}>(rows: T[]): T[] {
  rows.sort((a, b) => compareUtf8(a.pk, b.pk) || compareUtf8(a.sk, b.sk))

  return rows
}

// Every event of the run the log yields, once unfiltered and once narrowed to the account type, as
// sorted "section entity sequence type" lines, which is what the Go and Rust drivers print.
async function enumerate(backend: Backend, run: string): Promise<void> {
  const repository = backend.repository
  if (!isEventLog(repository)) {
    throw new Error('the repository does not enumerate its log')
  }

  const lines: string[] = []
  for (const [section, filter] of [
    ['all', {}],
    ['typed', {entityType: 'account'}],
  ] as const) {
    for await (const batch of repository.streamAllEvents(filter)) {
      for (const event of batch) {
        if (event.entityId.includes(run)) {
          lines.push(`${section} ${event.entityId} ${event.sequence} ${event.type}`)
        }
      }
    }
  }
  // Byte order, as Go's sort.Strings sorts.
  lines.sort(compareUtf8)
  for (const line of lines) {
    console.log(line)
  }
}

// A string attribute's text, or empty.
function textOf(value: AttributeValue | undefined): string {
  return value !== undefined && 'S' in value && typeof value.S === 'string' ? value.S : ''
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
