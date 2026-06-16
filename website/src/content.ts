const repo = 'https://github.com/photon-grove/evt'
const blob = `${repo}/blob/main`

export const repoUrl = repo

export const installCommand = 'go get github.com/photon-grove/evt'

// A single, honest "test today, ship tomorrow" snippet. The aggregate
// contracts never change between the two stores — only the wiring does.
export const quickStartCode = `// Start in memory — the same contracts you ship to production.
repo := mem.NewRepository()
store := mem.NewStoreFromRepo(repo)

entity := account.NewEntity("acct-1")
err := store.Execute(ctx, entity, "acct-1",
    &account.Open{InitialBalance: 100}, evt.Metadata{})

// Move writes to DynamoDB without touching your aggregates.
repo := dynamo.NewRepository(dynamoClient, "event-log")
store := snapshots.NewStore(repo, 25)`

export const capabilities = [
  {
    tag: 'evt',
    title: 'Command execution',
    body: 'Load an aggregate, run a command, serialize the produced events, and commit them under optimistic concurrency.',
  },
  {
    tag: 'evt/dynamo',
    title: 'DynamoDB event log',
    body: 'Append immutable rows under stable pk/sk keys, with inline snapshots at sk=0 and conditional writes for ordering.',
  },
  {
    tag: 'evt/projectors',
    title: 'Materialized views',
    body: 'Write deterministic read-model rows through projection transactions — then rebuild them by replaying events.',
  },
  {
    tag: 'evt/snapshots',
    title: 'Snapshots & upcasters',
    body: 'Keep replay fast while old event payloads evolve forward through explicit, versioned upcasters.',
  },
  {
    tag: 'evt/projectors',
    title: 'Stream projectors',
    body: 'Run DynamoDB Streams Lambda projectors with idempotency, retry classification, and partial-batch failures.',
  },
  {
    tag: 'evt/publishers',
    title: 'Event publishers',
    body: 'Fan event-log INSERT records out to SNS, with optional FIFO companion topics for ordered workflows.',
  },
]

export const gettingStarted = [
  'Model one aggregate with its command and event types.',
  'Start on mem.NewRepository for fast, deterministic tests.',
  'Move production writes to dynamo.NewRepository + snapshots.NewStore.',
  'Add view projectors only once the event contract is stable.',
  'Exercise real DynamoDB behavior against Moto with the integration task.',
]

export const packages = [
  {name: 'evt', body: 'Aggregate, command, event, metadata, transaction, repository, and rebuild contracts.'},
  {name: 'evt/mem', body: 'In-memory repository and store for fast unit tests.'},
  {name: 'evt/dynamo', body: 'DynamoDB event log, snapshots, views, stream helpers, and transaction groups.'},
  {name: 'evt/snapshots', body: 'Store with snapshot loading and configurable write thresholds.'},
  {name: 'evt/stream', body: 'Stream handler and publisher helpers for event fanout.'},
  {name: 'evt/projectors', body: 'Streams Lambda projector runtime with idempotency and retry classification.'},
  {name: 'evt/publishers', body: 'Streams publisher handler with ingress and retry budgets.'},
  {name: 'evt/policy', body: 'Backend-neutral retry helpers for commit paths (+ policy/dynamodb).'},
  {name: 'evt/viewstore', body: 'Typed JSON helper around evt.ViewRepository.'},
  {name: 'evt/test', body: 'Shared test aggregate, commands, events, and helpers for adopters.'},
]

export const cookbook = [
  {
    title: 'Single-table event log',
    body: 'Keep one DynamoDB event-log table for events and inline snapshots — pk as entity ID, sk as numeric sequence.',
  },
  {
    title: 'Projection rebuilds',
    body: 'Treat view tables as disposable. Wipe a bad read model, replay entities, and write deterministic rows again.',
  },
  {
    title: 'Command deduplication',
    body: 'Pass a stable command ID in metadata so duplicate retries fail safely instead of recording duplicate facts.',
  },
  {
    title: 'Schema evolution',
    body: 'Bump event versions when payload shape changes and register upcasters with fixtures for every historical shape.',
  },
  {
    title: 'Stream fanout',
    body: 'Publish only event-log INSERT records. Drop malformed rows deliberately and return partial-batch failures.',
  },
  {
    title: 'Local integration',
    body: 'Run Moto, apply infra/local Terraform, then run the integration task with AWS_ENDPOINT_URL on the emulator.',
  },
]

export const docLinks = [
  {
    title: 'Getting started',
    body: 'Define an aggregate, handle commands, and apply events end to end.',
    href: `${blob}/docs/getting-started.md`,
  },
  {
    title: 'Concepts',
    body: 'Entities, commands, events, metadata, and transactions explained.',
    href: `${blob}/docs/concepts.md`,
  },
  {
    title: 'DynamoDB integration',
    body: 'Event-log table shape, key patterns, inline snapshots, and views.',
    href: `${blob}/docs/dynamodb.md`,
  },
  {
    title: 'Projections & rebuilds',
    body: 'Deterministic read models and safe wipe-and-replay rebuilds.',
    href: `${blob}/docs/projections.md`,
  },
  {
    title: 'Streams & publishers',
    body: 'Lambda projectors, idempotency, retry policy, and SNS fanout.',
    href: `${blob}/docs/streams.md`,
  },
  {
    title: 'Behavioral invariants',
    body: 'The exact serialization and DynamoDB schema guarantees evt holds.',
    href: `${blob}/BEHAVIORAL_INVARIANTS.md`,
  },
]

export const examples = [
  {
    title: 'In-memory aggregate test',
    command: 'go test ./examples/banking',
    body: 'A compact aggregate that opens and deposits to an account using mem.NewRepository.',
  },
  {
    title: 'DynamoDB integration',
    command: 'moon run evt:integration',
    body: 'Runs the DynamoDB repository suite against local emulator tables managed by Terraform.',
  },
  {
    title: 'Docs build',
    command: 'moon run website:build',
    body: 'Builds this site and validates that the interactive architecture diagrams compile.',
  },
]
