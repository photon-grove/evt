// The TypeScript driver of the cross-language run (scripts/compat-integration.sh, ADR 0009):
//
//   live.js scenarios
//   live.js step   --backend dynamo --run RUN --index N --as go|rust
//   live.js verify --backend dynamo --run RUN
//
// TypeScript joins the run as a stand-in. step executes step N of every scenario TypeScript
// supports whose writer is the language --as names, and stamps that language's name into the
// event's origin and commit ID, so the streams match the fixtures byte for byte. In such a pass
// the other language's driver writes its own steps of the same scenarios, limited to the list
// scenarios prints (-scenarios or --scenarios). verify checks the streams of the supported
// scenarios against the golden fixtures: the raw DynamoDB items, the SNS messages, the events and
// snapshot read through the repository, and the loaded state.
//
// It runs against a local DynamoDB emulator, such as Moto, at AWS_ENDPOINT_URL, never real AWS.
// TypeScript has no PostgreSQL repository and no event-log enumeration yet, so it joins neither
// the PostgreSQL run nor the enumeration check.

// Each step needs the one before it, so the driver awaits in loops on purpose.
/* oxlint-disable no-await-in-loop */
import {type AttributeValue, DynamoDB} from '@aws-sdk/client-dynamodb'
import process from 'node:process'

import {type JsonValue, encodeEnvelope, encodeEvent, encodeSnapshot} from '@photon-grove/evt'
import {
  DynamoRepository,
  type Item,
  eventFromItem,
  headFromItem,
} from '@photon-grove/evt/dynamodb'

import {Checks, canonical, field, list, loadFixture, loadFixtureForRun, str} from './harness.js'
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

const cli = parseArgs(process.argv.slice(2))

if (cli.command === 'scenarios') {
  console.log(SUPPORTED_SCENARIOS.join(','))
} else {
  if (cli.backend !== 'dynamo') {
    throw new Error(`the TypeScript driver runs only against dynamo, not ${cli.backend || '-'}`)
  }
  if (cli.run === '') {
    throw new Error('--run is required')
  }

  const table = process.env['EVT_COMPAT_DYNAMO_TABLE'] ?? 'evt-local-event-log'
  const client = dynamoClient()
  // Lazily initializing, as the Go and Rust drivers are, so a fresh table takes the first write.
  const repository = new DynamoRepository(client, table, {lazyInitialization: true})
  const checks = new Checks()
  let quiet = false

  switch (cli.command) {
    case 'step':
      if (!WRITERS.includes(cli.as)) {
        throw new Error(`--as must be go or rust, not ${cli.as || '-'}`)
      }
      // An index with no step of the stood-in language's checks nothing, so it reports nothing.
      quiet = (await runStep(checks, repository, cli)) === 0
      break
    case 'verify':
      await verify(checks, repository, client, table, cli.run)
      break
    default:
      throw new Error(`unknown command ${cli.command}`)
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

// Only a local emulator: a loopback address or a .localhost name.
function dynamoClient(): DynamoDB {
  const endpoint = process.env['AWS_ENDPOINT_URL'] ?? 'http://localhost:4566'
  const host = URL.canParse(endpoint) ? new URL(endpoint).hostname : ''
  const local = ['localhost', '127.0.0.1', '[::1]'].includes(host) || host.endsWith('.localhost')
  if (!local) {
    throw new Error(`AWS_ENDPOINT_URL must name a local DynamoDB emulator: ${endpoint}`)
  }

  // One attempt, as the Go driver makes, so a refused request fails the step instead of retrying.
  return new DynamoDB({
    endpoint,
    region: process.env['AWS_REGION'] ?? 'us-west-2',
    maxAttempts: 1,
    credentials: {
      accessKeyId: process.env['AWS_ACCESS_KEY_ID'] ?? 'test',
      secretAccessKey: process.env['AWS_SECRET_ACCESS_KEY'] ?? 'test',
    },
  })
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

async function runStep(
  checks: Checks,
  repository: DynamoRepository,
  args: Args,
): Promise<number> {
  let executed = 0
  for (const scenario of await supportedScenarios()) {
    const name = str(field(scenario, 'name'))
    const step = list(field(scenario, 'steps'))[args.index]
    if (step === undefined || str(field(step, 'by')) !== args.as) {
      continue
    }
    checkStep(step, args.index)

    const runner = new Runner(repository, scenario, `compat-${args.run}-${name}`)
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
  repository: DynamoRepository,
  client: DynamoDB,
  table: string,
  run: string,
): Promise<void> {
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

    const runner = new Runner(repository, scenario, `compat-${run}-${name}`)
    checks.equal(`${name}: entity ID`, runner.entityId, str(field(expected, 'entityId')))
    const observe = (id: string) => observeStream(repository, client, table, runner, id)
    compareObserved(checks, name, await observe(runner.entityId), expected)

    // Every other stream the scenario touches.
    for (const sibling of optionalList(expected, 'siblings')) {
      const id = str(field(sibling, 'entityId'))
      compareStream(checks, `${name} sibling ${id}`, await observe(id), sibling)
    }
  }
}

// A stream as the table stores it and as the repository reads it back.
async function observeStream(
  repository: DynamoRepository,
  client: DynamoDB,
  table: string,
  runner: Runner,
  id: string,
): Promise<Observed> {
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
