export const repoUrl = 'https://github.com/photon-grove/evt'

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

// A linked content card. `doc`, when set, is the slug of the in-site guide it links to.
export interface ContentCard {
  title: string
  body: string
  doc?: string
}

// Capability-focused cards: what the framework does and where to learn it. The
// "Package reference" list below is the authoritative "what do I import" map, so
// these stay deliberately package-agnostic.
export const capabilities: ContentCard[] = [
  {
    title: 'Command execution',
    body: 'Load an aggregate, run a command, serialize produced events, and commit them with optimistic concurrency.',
    doc: 'getting-started',
  },
  {
    title: 'DynamoDB event log',
    body: 'Store immutable event rows under stable pk/sk keys, with inline snapshots at sk=0 and conditional writes for ordering.',
    doc: 'dynamodb',
  },
  {
    title: 'Materialized views',
    body: 'Write deterministic read-model rows through projection transactions and rebuild them by replaying events.',
    doc: 'projections',
  },
  {
    title: 'Incremental rebuilds',
    body: 'Track each entity head in a small table to rebuild only what changed, with constant-memory enumeration that does not grow with entity count.',
    doc: 'projections',
  },
  {
    title: 'Snapshots and upcasters',
    body: 'Keep replay fast while allowing old event payloads to evolve through explicit versioned upcasters.',
    doc: 'concepts',
  },
  {
    title: 'Stream projectors',
    body: 'Run DynamoDB Streams Lambda projectors with idempotency, retry classification, and partial-batch failure responses.',
    doc: 'streams',
  },
  {
    title: 'Event publishers',
    body: 'Fan out event-log INSERT records to SNS, including optional FIFO companion topics for ordered real-time workflows.',
    doc: 'streams',
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

export const cookbook: ContentCard[] = [
  {
    title: 'Single-table event log',
    body: 'Keep one DynamoDB event-log table for events and inline snapshots — pk as entity ID, sk as numeric sequence.',
    doc: 'dynamodb',
  },
  {
    title: 'Projection rebuilds',
    body: 'Treat view tables as disposable. Wipe a bad read model, replay entities, and write deterministic rows again.',
    doc: 'projections',
  },
  {
    title: 'Command deduplication',
    body: 'Pass a stable command ID in metadata so duplicate retries fail safely instead of recording duplicate facts.',
    doc: 'integration-cookbook',
  },
  {
    title: 'Schema evolution',
    body: 'Bump event versions when payload shape changes and register upcasters with fixtures for every historical shape.',
    doc: 'concepts',
  },
  {
    title: 'Stream fanout',
    body: 'Publish only event-log INSERT records. Drop malformed rows deliberately and return partial-batch failures.',
    doc: 'streams',
  },
  {
    title: 'Local integration',
    body: 'Run Moto, apply infra/local Terraform, then run the integration task with AWS_ENDPOINT_URL on the emulator.',
    doc: 'integration-cookbook',
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
