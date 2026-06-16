import type {DiagramSpec} from '@photon-grove/react-flow-diagrams'

export const diagrams: DiagramSpec[] = [
  {
    id: 'execution',
    title: 'Command execution',
    group: 'Core runtime',
    description:
      'A command is handled by a fresh aggregate, becomes immutable events, and commits under optimistic concurrency.',
    layout: {
      lanes: [
        {id: 'caller', label: 'Caller'},
        {id: 'runtime', label: 'evt runtime'},
        {id: 'domain', label: 'Domain'},
        {id: 'storage', label: 'Storage'},
      ],
    },
    nodes: [
      {id: 'caller', kind: 'client', label: 'Caller', sublabel: 'API · worker · CLI', domain: 'web', icon: 'browser', lane: 'caller'},
      {id: 'store', kind: 'service', label: 'evt.Store', sublabel: 'Load · handle · commit', domain: 'api', icon: 'lambda', lane: 'runtime'},
      {id: 'entity', kind: 'process', label: 'Aggregate', sublabel: 'Handle(command)', domain: 'domain', icon: 'entity', lane: 'domain'},
      {id: 'events', kind: 'topic', label: 'New events', sublabel: 'Immutable facts', domain: 'event', icon: 'event', lane: 'domain'},
      {id: 'repo', kind: 'service', label: 'Repository', sublabel: 'Commit serialized result', domain: 'api', icon: 'worker', lane: 'runtime'},
      {id: 'eventlog', kind: 'datastore', label: 'event-log', sublabel: 'DynamoDB pk/sk', domain: 'data', icon: 'datastore', lane: 'storage'},
    ],
    edges: [
      {id: 'caller-store', source: 'caller', target: 'store', label: 'Execute', variant: 'request'},
      {id: 'store-entity', source: 'store', target: 'entity', label: 'Load state', variant: 'data'},
      {id: 'entity-events', source: 'entity', target: 'events', label: 'CommandResult', variant: 'event'},
      {id: 'events-repo', source: 'events', target: 'repo', label: 'Serialize', variant: 'data'},
      {id: 'repo-log', source: 'repo', target: 'eventlog', label: 'Conditional write', variant: 'data'},
    ],
  },
  {
    id: 'storage',
    title: 'DynamoDB storage shape',
    group: 'Persistence',
    description:
      'The event log is append-only. Snapshots share the table at sk=0. Views live in a separate rebuildable table.',
    layout: {direction: 'DOWN'},
    nodes: [
      {id: 'aggregate', kind: 'process', label: 'Aggregate stream', sublabel: 'entityID', domain: 'domain', icon: 'entity'},
      {id: 'event1', kind: 'datastore', label: 'Event rows', sublabel: 'pk=entityID · sk=1..n', domain: 'event', icon: 'datastore'},
      {id: 'snapshot', kind: 'datastore', label: 'Snapshot row', sublabel: 'pk=entityID · sk=0', domain: 'data', icon: 'archive'},
      {id: 'projector', kind: 'service', label: 'Projector', sublabel: 'Deterministic write model', domain: 'api', icon: 'worker'},
      {id: 'views', kind: 'datastore', label: 'entity-views', sublabel: 'pk/sk + entityType-index', domain: 'data', icon: 'datastore'},
    ],
    edges: [
      {id: 'aggregate-events', source: 'aggregate', target: 'event1', label: 'append facts', variant: 'event'},
      {id: 'events-snapshot', source: 'event1', target: 'snapshot', label: 'checkpoint', variant: 'data'},
      {id: 'events-projector', source: 'event1', target: 'projector', label: 'replay or stream', variant: 'async'},
      {id: 'projector-views', source: 'projector', target: 'views', label: 'derived rows', variant: 'data'},
    ],
  },
  {
    id: 'rebuilds',
    title: 'Projection rebuild',
    group: 'Operations',
    description:
      'Rebuilds stream entities, run projectors against final state, and write replacement view rows without treating views as truth.',
    layout: {
      lanes: [
        {id: 'operator', label: 'Operator'},
        {id: 'rebuild', label: 'Rebuild'},
        {id: 'eventlog', label: 'Event log'},
        {id: 'projection', label: 'Projection'},
        {id: 'views', label: 'Views'},
      ],
    },
    nodes: [
      {id: 'operator', kind: 'client', label: 'Operator', sublabel: 'CLI or job', domain: 'web', icon: 'terminal', lane: 'operator'},
      {id: 'rebuild', kind: 'service', label: 'RebuildProjections', sublabel: 'stream · filter · project', domain: 'api', icon: 'worker', lane: 'rebuild'},
      {id: 'repo', kind: 'datastore', label: 'event-log', sublabel: 'source of truth', domain: 'event', icon: 'datastore', lane: 'eventlog'},
      {id: 'projectors', kind: 'process', label: 'Projectors', sublabel: 'full-state projection', domain: 'domain', icon: 'projector', lane: 'projection'},
      {id: 'views', kind: 'datastore', label: 'entity-views', sublabel: 'safe to wipe', domain: 'data', icon: 'datastore', lane: 'views'},
      {id: 'report', kind: 'process', label: 'Progress report', sublabel: 'processed · skipped · errors', domain: 'ops', icon: 'metrics', lane: 'rebuild'},
    ],
    edges: [
      {id: 'operator-rebuild', source: 'operator', target: 'rebuild', label: 'start run', variant: 'request'},
      {id: 'rebuild-repo', source: 'rebuild', target: 'repo', label: 'StreamEntities', variant: 'data'},
      {id: 'repo-projectors', source: 'repo', target: 'projectors', label: 'reconstituted entity', variant: 'event'},
      {id: 'projectors-views', source: 'projectors', target: 'views', label: 'TransactionGroup', variant: 'data'},
      {id: 'rebuild-report', source: 'rebuild', target: 'report', label: 'OnProgress', variant: 'async'},
    ],
  },
  {
    id: 'incremental-rebuild',
    title: 'Incremental rebuild',
    group: 'Operations',
    description:
      'A heads projector keeps one small row per entity (its highest sequence). The rebuild reads that table, not the log, and reprojects only entities whose head moved past their checkpoint — no secondary index, no global counter, no commit-path change.',
    layout: {direction: 'DOWN'},
    nodes: [
      {id: 'eventlog', kind: 'datastore', label: 'event-log stream', sublabel: 'DynamoDB NEW_IMAGE', domain: 'event', icon: 'datastore'},
      {id: 'headsproj', kind: 'service', label: 'Heads projector', sublabel: 'HeadStore · monotonic upsert', domain: 'api', icon: 'worker'},
      {id: 'headstable', kind: 'datastore', label: 'heads table', sublabel: 'pk=entityID · headSeq', domain: 'data', icon: 'datastore'},
      {id: 'rebuild', kind: 'service', label: 'Incremental rebuild', sublabel: 'detect · reproject', domain: 'api', icon: 'worker'},
      {id: 'checkpoint', kind: 'datastore', label: 'Projection checkpoint', sublabel: 'last-built sequence', domain: 'data', icon: 'archive'},
      {id: 'projectors', kind: 'process', label: 'Projectors', sublabel: 'changed entities only', domain: 'domain', icon: 'projector'},
      {id: 'views', kind: 'datastore', label: 'entity-views', sublabel: 'replacement rows', domain: 'data', icon: 'datastore'},
    ],
    edges: [
      {id: 'log-headsproj', source: 'eventlog', target: 'headsproj', label: 'INSERT batch', variant: 'event'},
      {id: 'headsproj-headstable', source: 'headsproj', target: 'headstable', label: 'max(seq)', variant: 'data'},
      {id: 'headstable-rebuild', source: 'headstable', target: 'rebuild', label: 'StreamEntityHeads', variant: 'data'},
      {id: 'checkpoint-rebuild', source: 'checkpoint', target: 'rebuild', label: 'last sequence', variant: 'dependency'},
      {id: 'rebuild-projectors', source: 'rebuild', target: 'projectors', label: 'changed only', variant: 'event'},
      {id: 'projectors-views', source: 'projectors', target: 'views', label: 'derived rows', variant: 'data'},
      {id: 'rebuild-checkpoint', source: 'rebuild', target: 'checkpoint', label: 'advance', variant: 'async'},
    ],
  },
  {
    id: 'streams',
    title: 'Streams, projectors, and publishers',
    group: 'Async flows',
    description:
      'One publisher consumes the DynamoDB stream and fans events out over SNS. Projectors, the heads table, and other consumers each subscribe to the topic and build read models independently.',
    layout: {direction: 'DOWN'},
    nodes: [
      {id: 'eventlog', kind: 'datastore', label: 'event-log stream', sublabel: 'DynamoDB NEW_IMAGE', domain: 'event', icon: 'datastore'},
      {id: 'publisher', kind: 'service', label: 'Publisher', sublabel: 'sole stream consumer', domain: 'api', icon: 'worker'},
      {id: 'sns', kind: 'topic', label: 'events topic', sublabel: 'SNS · CloudWatchEvent · opt. FIFO', domain: 'queue', icon: 'topic'},
      {id: 'projector', kind: 'service', label: 'Projector runtime', sublabel: 'SNS→SQS · idempotent', domain: 'api', icon: 'worker'},
      {id: 'readmodel', kind: 'datastore', label: 'read models', sublabel: 'views or search rows', domain: 'data', icon: 'datastore'},
      {id: 'heads', kind: 'service', label: 'Heads projector', sublabel: 'change detection', domain: 'api', icon: 'worker'},
      {id: 'consumers', kind: 'external', label: 'Other consumers', sublabel: 'feeds · webhooks · search', domain: 'external', icon: 'external'},
    ],
    edges: [
      {id: 'log-publisher', source: 'eventlog', target: 'publisher', label: 'INSERT batch', variant: 'event'},
      {id: 'publisher-sns', source: 'publisher', target: 'sns', label: 'CloudWatchEvent envelope', variant: 'async'},
      {id: 'sns-projector', source: 'sns', target: 'projector', label: 'SNS→SQS', variant: 'async'},
      {id: 'projector-readmodel', source: 'projector', target: 'readmodel', label: 'idempotent projection', variant: 'data'},
      {id: 'sns-heads', source: 'sns', target: 'heads', label: 'SNS→SQS', variant: 'async'},
      {id: 'sns-consumers', source: 'sns', target: 'consumers', label: 'fan out', variant: 'async'},
    ],
  },
  {
    id: 'evolution',
    title: 'Event evolution',
    group: 'Reliability',
    description:
      'Versioned event payloads evolve through explicit upcasters before they are applied to current aggregate code.',
    layout: {direction: 'DOWN'},
    nodes: [
      {id: 'old', kind: 'datastore', label: 'Stored event v1', sublabel: 'historical payload', domain: 'event', icon: 'archive'},
      {id: 'deserialize', kind: 'service', label: 'DeserializeEvent', sublabel: 'strict event type check', domain: 'api', icon: 'worker'},
      {id: 'upcaster', kind: 'process', label: 'Upcaster chain', sublabel: 'v1 -> v2 -> current', domain: 'domain', icon: 'projector'},
      {id: 'current', kind: 'topic', label: 'Current event', sublabel: 'apply-safe shape', domain: 'event', icon: 'event'},
      {id: 'entity', kind: 'process', label: 'Aggregate Apply', sublabel: 'state transition', domain: 'domain', icon: 'entity'},
    ],
    edges: [
      {id: 'old-deserialize', source: 'old', target: 'deserialize', label: 'load', variant: 'data'},
      {id: 'deserialize-upcaster', source: 'deserialize', target: 'upcaster', label: 'needs newer version', variant: 'dependency'},
      {id: 'upcaster-current', source: 'upcaster', target: 'current', label: 'upcasted payload', variant: 'event'},
      {id: 'current-entity', source: 'current', target: 'entity', label: 'Apply', variant: 'request'},
    ],
  },
]
