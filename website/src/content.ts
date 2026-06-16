// A linked content card. `doc`, when set, is the slug of the in-site guide it links to.
export interface ContentCard {
  title: string
  body: string
  doc?: string
}

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
  'Model one aggregate with command and event types.',
  'Start with mem.NewStore for fast tests.',
  'Move production writes to dynamo.NewRepository plus snapshots.NewStore.',
  'Add view projectors only after the event contract is stable.',
  'Exercise DynamoDB behavior against Moto with the integration task.',
]

export const cookbook: ContentCard[] = [
  {
    title: 'Single-table event log',
    body: 'Use one DynamoDB event-log table for events and inline snapshots. Keep pk as entity ID and sk as numeric sequence.',
    doc: 'dynamodb',
  },
  {
    title: 'Projection rebuilds',
    body: 'Treat view tables as disposable derived state. Delete a bad read model, replay entities, and write deterministic rows again.',
    doc: 'projections',
  },
  {
    title: 'Command deduplication',
    body: 'Pass a stable command ID in metadata so duplicate retries fail safely instead of creating duplicate event facts.',
    doc: 'integration-cookbook',
  },
  {
    title: 'Schema evolution',
    body: 'Increment event versions when payload shape changes and register upcasters with tests for every historical shape.',
    doc: 'concepts',
  },
  {
    title: 'Stream fanout',
    body: 'Publish only event-log INSERT records. Drop malformed rows deliberately and return partial batch failures for retryable records.',
    doc: 'streams',
  },
  {
    title: 'Local integration',
    body: 'Run Moto, apply infra/local Terraform, then run the integration task with AWS_ENDPOINT_URL pointed at the emulator.',
    doc: 'integration-cookbook',
  },
]

export const examples = [
  {
    title: 'In-memory aggregate test',
    command: 'go test ./examples/banking',
    body: 'A compact aggregate that opens and deposits to an account using mem.NewStore.',
  },
  {
    title: 'DynamoDB integration',
    command: 'moon run evt:integration',
    body: 'Runs the DynamoDB repository suite against local emulator tables managed by Terraform.',
  },
  {
    title: 'Docs build',
    command: 'moon run website:build',
    body: 'Builds this site and validates the interactive architecture diagrams compile.',
  },
]
