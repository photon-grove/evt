export const repoUrl = 'https://github.com/photon-grove/evt'

export const installCommand = 'go get github.com/photon-grove/evt/go'

// The aggregate contract is the same for the memory and DynamoDB stores. The names come from
// go/examples/banking.
export const quickStartCode = `// In tests, run commands against the in-memory store.
store := mem.NewStore()

account := NewAccount("acct-1")
err := store.Execute(ctx, account, "acct-1",
    &OpenAccount{AccountID: "acct-1", InitialBalance: 100}, evt.Metadata{})

// In production, the same aggregate runs on DynamoDB,
// with a snapshot every 25 events.
repo := dynamo.NewRepository(dynamoClient, "event-log")
store = snapshots.NewStore(repo, 25)`

// A linked content card. `doc`, when set, is the slug of the in-site guide it links to.
export interface ContentCard {
  title: string
  body: string
  doc?: string
}

// Capability-focused cards link to guides. The package list below identifies the
// import path for each component.
export const capabilities: ContentCard[] = [
  {
    title: 'Command execution',
    body: 'Load an aggregate, run a command against it, and commit the events it decides on. Optimistic concurrency means a writer that raced you gets a conflict instead of a silent overwrite.',
    doc: 'guide/getting-started',
  },
  {
    title: 'DynamoDB event log',
    body: 'Events live under stable pk/sk keys, with the snapshot inline at sk=0 and conditional writes keeping each stream in order.',
    doc: 'guide/dynamodb',
  },
  {
    title: 'Materialized views',
    body: 'Projectors write read-model rows in the same transaction as the events, so you can wipe a view and rebuild it by replaying the log.',
    doc: 'guide/projections',
  },
  {
    title: 'Snapshots and upcasters',
    body: 'Snapshots keep replay short. Versioned upcasters let old event payloads change shape without rewriting history.',
    doc: 'guide/concepts',
  },
  {
    title: 'Stream projectors',
    body: 'Projectors read the DynamoDB Stream directly or through SNS or SQS, with idempotency, retry classification and partial-batch failure responses.',
    doc: 'guide/streams',
  },
]

export const gettingStarted = [
  'Model one aggregate with its command and event types.',
  'Test it on mem.NewStore, which is fast and deterministic.',
  'Move production writes to dynamo.NewRepository and snapshots.NewStore.',
  'Add view projectors once the event contract has settled.',
  'Check DynamoDB behavior against the Moto emulator with the integration task.',
]

export const packages = [
  {name: 'evt/go', body: 'The core contracts: aggregates, commands, events, metadata, transactions, repositories and rebuilds.'},
  {name: 'evt/go/backends/mem', body: 'In-memory repository and store for unit tests.'},
  {name: 'evt/go/backends/dynamo', body: 'DynamoDB event log, snapshots, views, stream helpers and transaction groups.'},
  {name: 'evt/go/backends/postgres', body: 'PostgreSQL event log and snapshots behind the same evt.Repository contract.'},
  {name: 'evt/go/snapshots', body: 'The store that loads from snapshots and writes them on a threshold.'},
  {name: 'evt/go/stream', body: 'The stream envelope and the DynamoDB Streams handler.'},
  {name: 'evt/go/projectors', body: 'Projector runtime for the DynamoDB Stream, SNS or SQS, with idempotency and retry classification.'},
  {name: 'evt/go/policy', body: 'Backend-neutral retry classification and backoff helpers.'},
  {name: 'evt/go/viewcodec', body: 'A typed JSON helper around evt.ViewRepository.'},
  {name: 'evt/go/result', body: 'The result.Result value the repository stream channels carry.'},
  {name: 'evt/go/conformance', body: 'The backend-neutral suite a custom evt.Repository should pass.'},
  {name: 'evt/go/test', body: 'A shared test aggregate, commands, events and helpers.'},
  {name: 'evt/go/cmd/evt-doctor', body: 'A CLI that inspects an event log and runs the stream-heads migration.'},
]

export const cookbook: ContentCard[] = [
  {
    title: 'Single-table event log',
    body: 'One DynamoDB table holds the events and their inline snapshots, with pk as the entity ID and sk as the sequence number.',
    doc: 'guide/dynamodb',
  },
  {
    title: 'Projection rebuilds',
    body: 'View tables are disposable. When a read model goes bad, wipe it and replay the entities to write the rows again.',
    doc: 'guide/projections',
  },
  {
    title: 'Command deduplication',
    body: 'Put a stable command ID in the metadata, and a retried command is caught as a duplicate instead of recording the same fact twice.',
    doc: 'guide/integration',
  },
  {
    title: 'Schema evolution',
    body: 'Bump the event version when a payload changes shape, and register an upcaster with a fixture for every old shape.',
    doc: 'guide/concepts',
  },
  {
    title: 'Stream fan-out',
    body: 'Forward only the event-log INSERT records. Drop malformed rows on purpose and return partial-batch failures for the rest.',
    doc: 'guide/streams',
  },
  {
    title: 'Local integration',
    body: 'Run Moto, apply the infra/local Terraform, then run the integration task with AWS_ENDPOINT_URL pointed at the emulator.',
    doc: 'guide/integration',
  },
]

export const examples = [
  {
    title: 'In-memory aggregate test',
    command: 'go -C go test ./examples/banking',
    body: 'A small aggregate that opens an account and deposits to it on the in-memory store.',
  },
  {
    title: 'DynamoDB integration',
    command: 'moon run evt:integration',
    body: 'Runs the DynamoDB repository suite against local emulator tables that Terraform creates.',
  },
  {
    title: 'Docs build',
    command: 'moon run website:build',
    body: 'Builds this site, which also checks that the architecture diagrams compile.',
  },
]
