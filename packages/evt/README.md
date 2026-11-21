# @photon-grove/evt

Event sourcing for TypeScript, wire-compatible with evt's Go module and its Rust crate. Event
sourcing means the log of events is the record of truth, and an aggregate's state is whatever you
get by folding its events in order. This package isn't a binding to the Go or Rust code. It's a
third native implementation of the same
[storage format](https://github.com/photon-grove/evt/blob/main/docs/reference/storage-format.md),
held to the fixtures Go and Rust generate, byte for byte.

```sh
npm install @photon-grove/evt
```

So far it has the core, multi-stream transactions with read fences, command outcomes,
commit-wide projectors, the in-memory, DynamoDB and PostgreSQL backends, the view repository
with its typed view codec, the journal on the in-memory and DynamoDB backends, and the testing
helpers. The
[feature matrix](https://github.com/photon-grove/evt/blob/main/docs/reference/feature-matrix.md)
tracks each one. The
[getting started guide](https://github.com/photon-grove/evt/blob/main/docs/guide/getting-started.md#the-same-model-in-typescript)
builds a bank account aggregate with it, end to end.

## Entry points

| Import                       | Contents                                                                   |
| ---------------------------- | -------------------------------------------------------------------------- |
| `@photon-grove/evt`          | Aggregates, codecs, the `Store`, retry, `ViewCodec`, and the wire codecs   |
| `@photon-grove/evt/memory`   | `MemRepository` and `MemViewRepository`, for tests and local use           |
| `@photon-grove/evt/testing`  | `scenario` and `Fixture`, the given/when/then helpers over `MemRepository` |
| `@photon-grove/evt/dynamodb` | `DynamoRepository`, `viewProjector`, `DynamoViewRepository`, item codecs   |
| `@photon-grove/evt/postgres` | `PgRepository`, `PgViewRepository`, `ViewWriter`, the schema and its DDL   |
| `@photon-grove/evt/journal`  | `Journal`, `JournalRepository`, `page` and `replay`: the journal           |

## DynamoDB

`DynamoRepository` takes the AWS SDK's low-level client, `DynamoDB` from `@aws-sdk/client-dynamodb`
(an optional peer dependency), and the event-log table, laid out as the
[DynamoDB guide](https://github.com/photon-grove/evt/blob/main/docs/guide/dynamodb.md) describes:

```ts
import {DynamoDB} from '@aws-sdk/client-dynamodb'
import {Store} from '@photon-grove/evt'
import {DynamoRepository} from '@photon-grove/evt/dynamodb'

const repository = new DynamoRepository(new DynamoDB({}), 'event-log')
await repository.initialize() // once, at deploy time
const store = new Store(repository)
```

The package imports nothing from the SDK, so any client with the same methods works. A store must
be initialized before writers start: until then every write fails with a
`StoreNotInitializedError`, so writers never need `dynamodb:Scan`. `initialize()` scans the table
to make sure it's fresh, and `{lazyInitialization: true}` makes the first write do it instead, for
tests and local use. Reads are strongly consistent unless `{consistentRead: false}`. When another
writer declares a feature, the next write fails once with a retryable
`FeatureManifestChangedError`, which `executeWithRetry` runs again.

`{retention: {scratch_job: 86_400_000}}` stamps a DynamoDB `ttl` on every event, snapshot and head
item of the entity types it names, their write time plus the retention in milliseconds, as Go's
`WithRetention` does. A head's `ttl` is never lowered: a writer with a shorter retention that
hasn't loaded the stream gets a conflict, and its retry passes. TTL deletes items in no order, so
use retention only for short-lived streams that nothing replays, and declare the retained types in
the feature manifest from Go or Rust, so that their writers refuse read fences on them.

`viewProjector('views')`, listed in an aggregate's `projectors`, puts the entity's view row with
every commit, in the same transaction: `pk = {type}#{id}`, `sk = VIEW`, and the state as its
codec encodes it, as Go's `dynamo.NewViewProjector` does.

## PostgreSQL

`PgRepository` takes a pool shaped like `pg`'s `Pool`, and writes the tables the
[PostgreSQL guide](https://github.com/photon-grove/evt/blob/main/docs/guide/postgres.md) describes,
as Go's and Rust's repositories do:

```ts
import {Store} from '@photon-grove/evt'
import {PgRepository} from '@photon-grove/evt/postgres'
import {Pool} from 'pg'

const repository = new PgRepository(new Pool({connectionString: process.env.DATABASE_URL}))
await repository.ensureSchema() // idempotent, safe on every start
const store = new Store(repository)
```

The package imports no driver: Neon's serverless `Pool` works too, and so does a single connection
without `connect()`, such as PGlite, which the repository runs one operation at a time on. Every
value it sends is a string and every column it reads is text, so no driver's type parsing touches a
stored value. `{eventsTable, snapshotsTable}` let several event logs share a database. A writer
initializes a fresh store itself on its first write, as Go's and Rust's do, and `initialize()` is
there for a deploy step that treats every backend alike. Every transaction runs at `READ
COMMITTED`, whatever the server's default. An abort is checked between statements and rolls the
transaction back.

Commit-wide projectors write their views either to the evt-owned views table or to tables the
application owns. With `viewsTable: true`, the commit writes them to `{events}__evt_views`, which
`ensureSchema()` creates, as Rust's `with_views_table()` does: evt builds each statement with the
DynamoDB repository's owner rules, counts it in the commit budget as its DynamoDB item, and
refuses a key whose entity type, `pk` and `sk` are over 2600 bytes together (a
`ViewKeyTooLongError`), which the table's btree indexes can't hold. The database must be `UTF8`.
Before its first write the repository declares the `postgres-views` feature in the store's
manifest, and from then on every writer without the views table refuses the store with an
`UnsupportedFeatureError`, Go's included, so a store's views never split across two tables.
`repository.viewRepository()` reads the rows back (see [Views](#views)).

Otherwise the `viewWriter` option is a function from a view operation to the statement that
writes it to the application's table, `{sql, args, keyConstraint}`, as Go's `WithViewWriter` takes. The statements run in the
commit's transaction, after the events and before the snapshots, and the repository reads each
condition from the statement's outcome, so the statements must store the row's owner and enforce
it: a put with `condition: 'absent'` is a plain `INSERT` whose unique violation of the
`keyConstraint` it names fails the condition, any other put an `INSERT ... ON CONFLICT DO UPDATE ...
WHERE` the owner matches, which affects no row when it doesn't, a delete with `condition:
'present'` must delete the owner's row, and any other delete reports the rows of another owner it
found (`WITH gone AS (DELETE ...) SELECT 1 FROM ... WHERE owner <> $3`). The count is the rows a
statement returns, or the driver's `rowCount` (`affectedRows` on PGlite) when it returns none,
and only its rows when it's marked `returning: true`, for a driver that reports no count.
Any other error the database raises fails the commit with a `ProjectorOperationError` whose
`reason` is the SQLSTATE, and each view counts in the commit budget as one action the size of its
SQL text. A repository takes the views table or a writer, not both, and with neither, a commit
that carries view operations is refused with a `ViewsNotConfiguredError`.

## Views

A `ViewRepository` reads and writes view rows outside the commit path, as Go's
`dynamo.ViewRepository` and Rust's `ViewRepository` do: `getView`, `putView` (with an optional
`ttl` in Unix seconds), `putViews`, `deleteView`, `batchGetViews`, and listings by partition key and
by entity type, whole or a page at a time. `DynamoViewRepository` works on the views table the
[DynamoDB guide](https://github.com/photon-grove/evt/blob/main/docs/guide/dynamodb.md#entity-views-table)
describes, and `MemViewRepository` keeps rows in memory:

```ts
import {DynamoDB} from '@aws-sdk/client-dynamodb'
import {ViewCodec} from '@photon-grove/evt'
import {DynamoViewRepository} from '@photon-grove/evt/dynamodb'

interface Balance {
  accountId: string
  cents: number
}

const views = new DynamoViewRepository(new DynamoDB({}), 'views')
const balances = new ViewCodec<Balance>(views, 'balance').single(
  (accountId: string) => `balance#${accountId}`,
  (balance) => balance.accountId,
)
await balances.put('acct-1', {accountId: 'acct-1', cents: 500})
const balance = await balances.get('acct-1') // null when there is no row
```

Point reads and partition listings are strongly consistent on DynamoDB, so a handler reads its
own commit's rows; `{consistentRead: false}` halves the cost of a partition listing. Entity-type
listings read the `entityType-index` index, so they can lag behind a commit. A page's `nextCursor`
is Go's cursor byte for byte, so a Go, Rust or TypeScript process can continue another's listing.
A cursor from another listing is refused, as are U+0000 or a lone surrogate anywhere in a row or a
key, an empty `pk`, and a payload that isn't UTF-8.

`ViewCodec<T>` writes a value as a row's payload with a payload codec, `json()` by default, and
decodes it back; a row that doesn't decode throws a `ViewDecodeError` naming it, and the lenient
`listByPk` and `listAll` skip such rows and report them to `onDecodeError`. A view a Go or Rust
service reads must declare its properties in the Go struct's order, as for events. These writes are
unconditional and part of no commit, so keep them to rows no commit-wide projector owns, and to
rebuilds: a view is a projection, and every row must come back when the events are replayed.

On PostgreSQL, `PgViewRepository` works on the views table, as Rust's does, and
`repository.viewRepository()` returns the one a `PgRepository` writes to. Its keys order by bytes,
as on DynamoDB, its payloads come back byte for byte, and its cursors are the DynamoDB ones, so a
Rust process can continue a TypeScript listing. Every read sees every committed transaction,
entity-type listings included, and a page without a `limit` holds at most 1000 rows. `putViews`
writes its rows in one transaction, so unlike DynamoDB's batch it's atomic. Expired rows stay
readable until `purgeExpired(now)` deletes them, since PostgreSQL has no TTL sweep. Go has no
PostgreSQL view repository.

## Journal

The journal (`@photon-grove/evt/journal`, [ADR 0005](https://github.com/photon-grove/evt/blob/main/docs/adr/0005-rust-first-extensions-journal-and-lambda.md))
delivers a store's events in per-scope order, as rows written in the commit that wrote the events.
A `Journal` names the entity types it covers and maps each of their events to zero or more scopes,
with a pure function of the event. A `JournalRepository` wraps an in-memory repository, or a
DynamoDB one with a views table, and journals every commit through it:

```ts
import {Journal, JournalRepository} from '@photon-grove/evt/journal'

const journal = new Journal(['account'], (event) => [`account:${event.entityId}`])
const repository = new JournalRepository(new DynamoRepository(client, 'events', {viewsTable}), journal)
const store = new Store(repository)

const page = await repository.page('account:a-1', 0n, 100)
```

- **Allocations live on the event.** Each event takes the next position in each of its scopes,
  stored in its metadata extension `evt.journal`, so every row and counter can be derived again
  from the stored events (`replay`). The store hands back the events as stored, allocations
  included, through the commit's receipt. No other projector sees the extension.
- **Rows and counters commit with the events.** The journal's rows come from its own
  history-dependent projector, each owned by its event's stream and claimed only if absent, and
  each scope's counter is a compare-and-set on the counter the commit read. A writer that lost a
  position reads the counters again and allocates again, up to the journal's `retry` policy, then
  throws a `ConflictError`; a counter behind its rows is repaired from the highest row.
- **Reads.** `highWatermark(scope)` reads the counter, and `page(scope, after, limit)` returns the
  contiguous rows after `after`, stopping before the first gap, which it reports. A reader never
  moves past a gap: it reads the same page again. A consumer of a journal that can be rebuilt reads with
  `pageAtEpoch` and `highWatermarkAtEpoch`, which fail retryably while a rebuild rewrites it.
- **Row size.** A row over `maxRowBytes` (30,000 by default) fails the commit before any write with
  a `JournalRowTooLargeError`, and a store with outcomes records it as the command's
  `journal-row-too-large` rejection. `journal.preCheck(events)` measures the same rows in a
  handler, so it can reject first.
- **Declarations.** The first journaled commit declares the `journal` feature and the journaled
  types history-dependent, so compaction refuses them. Only the repository handle a journal wraps
  accepts a store that declares the journal: every writer must journal its commits.

- **Compaction.** `compactWithLease(store, scope, through, owner, leaseMs)` stores a scope's rows
  up to `through` as a snapshot (a manifest and canonical chunks) and deletes them, under a lease
  and the rebuild epoch; give it the repository the journal wraps. The checkpoint becomes the
  scope's floor: `page` hands a reader below it a `JournalGoneError` (HTTP 410) naming the snapshot,
  which it installs with `readManifest` and `readChunk` before paging on from the checkpoint, and
  allocation never goes below it. `beginCompaction`, `compact`, `renewCompaction`,
  `endCompaction` and `forceReleaseCompaction` run the steps by hand.

Rebuilds under the rebuild lock and the journal on PostgreSQL are still Rust only.

## Runtimes

The package is ESM only and has no runtime dependencies. CI runs it on Node 26, Node 22.12, Bun and
Deno. Its code uses only web-standard globals (`TextEncoder`, `TextDecoder`, Web Crypto,
`AbortSignal`, timers), never Node's built-ins, so it should run on edge runtimes and in browsers
too, though CI doesn't run them yet. It never uses top-level await, so CommonJS code on Node 22.12
and later can `require()` it. Its type declarations need TypeScript 5.7 or later and a declaration
of `AbortSignal`, which the DOM library, `@types/node`, and Bun's and Deno's types all provide.

## The model

- **Events.** `defineEvents` gives each event type a version, a codec and, optionally, upcasters.
  The type string, the version and the payload bytes are the durable contract. `EventOf<typeof
  events>` is the union of `{type, data}` objects the aggregate works with.
- **Codecs.** `json()` writes plain objects with evt's own JSON writer, in property order, and
  decodes them back. It refuses an integer past `Number.MAX_SAFE_INTEGER` rather than round it,
  when encoding as well as decoding. It takes an optional synchronous
  [Standard Schema](https://standardschema.dev) validator (Zod, Valibot, ArkType), checked on both
  sides. Any `{encode, decode}` pair of bytes works as a codec.
- **Aggregates.** `defineAggregate` names the type, the events, the state's codec and its initial
  value. `handle(state, command)` returns a decision, `{kind: 'accept', events}` or
  `{kind: 'reject', rejection: {code, message}}`, and may be async. On a store with command
  outcomes, an accepted decision can carry a `response` and a rejection its own `events`. `apply(state,
  event)` returns the next state and never validates.
- **The store.** `new Store(repository, options)` loads an aggregate from its latest snapshot and
  the events after it, runs a command, and commits the decided events with optimistic concurrency:
  the commit fails with a `ConflictError` if another writer moved the stream first.
  `executeWithRetry` loads and runs the command again after a conflict, with a jittered backoff
  (`DEFAULT_RETRY_POLICY`, the same as Go's and Rust's). Without command outcomes, a rejection
  commits nothing.
- **Snapshots.** The store writes a snapshot every five events by default (the `snapshotInterval`
  option, or the aggregate's own). A snapshot too large for one DynamoDB item is skipped rather
  than failing the commit, and later loads replay from the one before.
- **Duplicate commands.** A command whose metadata carries a command ID already seen since the
  latest snapshot doesn't run, and `execute` resolves to `{kind: 'duplicate'}`. The window ends at
  the snapshot, so once a snapshot covers the first run, a retry runs the command again.
- **Upcasting.** An event type's upcasters migrate a stored payload from older versions to the
  current one as it's read, in the order they're declared. `jsonUpcaster(from, to, migrate)` does
  it on the parsed JSON, losslessly. A stored version the upcasters don't bring to the declared one
  fails the load with a `ReplayError` rather than decoding into the wrong shape.
- **The feature marker.** A store keeps one manifest of the features it uses. Every write path
  refuses a store that declares a feature this package doesn't model with an
  `UnsupportedFeatureError`, and so does every handle but the one a `JournalRepository` wraps for
  a store that declares the journal or journal compaction. A `PgRepository` without `viewsTable: true` refuses one that
  declares the PostgreSQL views table (`postgres-views`), and compaction
  refuses an entity type the manifest declares history-dependent, which every repository's
  `declareHistoryDependent(types)` declares. `MemRepository` declares the required `stream-heads`
  feature on a fresh store's first write, and `initialize()` declares it up front.
- **Projectors.** An aggregate's `projectors` run on the state after each commit's events, and
  the operations they return are written in the commit's own transaction, by the backend they
  name. `MemRepository` budgets them and writes none, as Go's in-memory repository does, so code
  with projectors runs on it unchanged. A commit with operations but no events is refused with a
  `ProjectorOnlyCommitError`.
- **Commit-wide projectors.** A store's `commitProjectors` run over everything a commit writes,
  once per commit that writes events (`execute`, `executeWithOutcome`, transactions and a
  rejection's events, never a plain rejection, a no-op or a replay), and their view operations
  commit in the same transaction, after the aggregates' projector operations. A projector gets a
  `CommitView` (each written stream, sorted by entity ID bytes, with its state after the commit as
  its snapshot would carry it) and returns `putView(pk, sk, owner, entityType, payload)` and
  `deleteView(pk, sk, owner)` operations, at any key. Each row belongs to its owner, a stream the
  commit wrote, and no other stream may write or delete it. A put with `condition: 'absent'` claims
  a key, a delete with `condition: 'present'` releases one, and a failed condition fails the whole
  commit with a `ProjectorOperationError` naming the projector and the operation, with the
  operation's own `conflict` error as `mapped`. Operations are checked before any write (keys up to
  2048 and 1024 bytes, no key twice, none in the engine's journal partitions), and counted in the
  commit budget. `MemRepository` keeps the rows (`view`, `views`), `DynamoRepository` writes
  them to its `viewsTable`, and `PgRepository` to its views table or through its `viewWriter`. A
  projector that names a `history` of entity types is history-dependent: it also gets the commit
  ID and the events of those types, and the store declares them in its feature manifest's
  historyDependent set, once, before the first commit that runs it, so compaction refuses them in
  every process. A repository without `declareHistoryDependent` refuses it with
  `historyDeclarationUnsupported`. `groupCommits` regroups stored events into the commits that
  wrote them.
- **Enumeration.** Every repository is an `EventLog`: `streamAllEvents(filter)` walks the log in
  batches (one per entity in memory and on PostgreSQL, one per Scan page on DynamoDB), and
  `entityHeads(entityType)` reports each entity's head, the larger of its highest event sequence
  and what its snapshot covers. Neither yields a snapshot, a head item or an engine row in the
  `SYSTEM#` or `command-outcome#` namespaces, and both read the whole log on DynamoDB, so keep
  them to rebuilds and diagnostics. `store.streamEntities(aggregate)` folds each entity of the
  aggregate's type from its events, as Rust's `stream_entities` does, and
  `store.streamEntitiesFromSnapshots(aggregate)` loads each from its snapshot and the events
  after it, so a compacted stream loads too. Both yield `{ok, entityId, loaded}`, or the entity's
  `error` and go on with the next.
- **Rebuilds.** While a projection rebuild holds the store, every commit fails with a
  `RebuildInProgressError`. It's retryable, so `executeWithRetry` runs the command again after its
  backoff, though a rebuild usually outlasts the default policy's attempts.
- **Transactions.** `store.transaction()` commits several streams at once. `read(aggregate, id)`
  loads a stream of any aggregate type and returns a typed handle, `append(handle, events)` stages
  the events decided for it, and `commit({metadata})` stamps one commit ID on every event and
  writes them all atomically, with each stream's snapshot and projector operations, or writes
  nothing. A stream read but never appended to is fenced at the head it was read at, so the commit
  fails with a `FenceConflictError` once another writer moves it. `fence({entityId, entityType,
  head})` adds a fence of your own on a stream the transaction doesn't read. A fence the engine
  filled is retryable, and one the caller stamped isn't. A transaction commits once, and `execute`
  stays the shortcut for a single stream. Underneath, every repository is a
  `TransactionalRepository`, whose `commitTransaction({streams, fences})` takes the streams and
  fences directly. A fence on an entity type with retention is refused with a
  `RetentionConflictError`.
- **Command outcomes.** A store built with `outcomes` records a durable outcome for every command,
  in the same commit as its events, so a retried command returns the recorded answer instead of
  running again. Run commands with `executeWithOutcome(aggregate, id, command, {metadata})`, which
  needs a command ID and the aggregate's `identify(command)`: the command's type and the canonical
  JSON of its payload, which with the entity ID and the metadata's principal make up its identity
  digest. Every attempt reads the command's outcome row first. A stored outcome with the same
  digest is returned with `replayed: true` and the handler doesn't run, and one with another digest
  is an `IdempotencyKeyReuseError`. Otherwise the handler runs, and an accepted decision's
  `response` is stored with its outcome. A rejection is recorded and returned, not thrown, and the
  `events` it carries commit with the rejected row. A conflict runs the command again from a fresh
  load, and when the attempts run out the store records a `stale-fence` rejection, so the command
  ID keeps that answer. A commit whose result is unknown is settled by polling the outcome row,
  never by running the handler again: a row that doesn't appear within `recoveryWaitMs` is a
  retryable `AmbiguousWriteError`. `resolveOutcome` and `resolveCommand(identity)` read the row
  without running anything, and `resolveCommandForPrincipal(commandId, principal)` answers only the
  principal that recorded it (`OutcomePrincipalMismatchError` otherwise). In a transaction,
  `beginCommand(identity, {metadata})` comes first, and `commitWithOutcome({response})`, `reject`
  and `rejectWithEvents` commit it. A failed fence the caller stamped records the `stale-fence`
  rejection instead, and `expect(id, head)` records the `stale-version` rejection when a stream
  the client saw at `head` has moved. `executeTransaction(identity, prepare, {metadata})` runs a
  multi-stream command under the same retry: each attempt begins a transaction and the command,
  runs `prepare` against it, and commits what it decides (`{kind: 'accept', response}` or
  `{kind: 'reject', rejection, withEvents}`). An engine fence that moved runs `prepare` again from
  fresh reads, so it must be safe to repeat. On a store with outcomes, `execute` and a
  transaction's `commit` are refused, since they would skip the row. Underneath, every repository is an `OutcomeRepository`: it writes
  a commit's `outcome` as its last action, on the condition that no row exists for the command ID,
  and reads it back with `outcome()`, strongly consistent. `FaultRepository` in
  `@photon-grove/evt/testing` makes a commit apply and then fail, or fail without applying, to
  test recovery.
- **Commit receipts.** The events `execute`, `executeWithOutcome`, `executeTransaction` and a
  transaction's `commit` hand back are the events as the repository stored them, from its
  receipt, by entity ID bytes then sequence. A repository that rewrites events as it commits, as
  `JournalRepository` does, implements `commitWithReceipt` and `commitTransactionWithReceipt`, and
  a wrapper must forward them. The root helpers of the same names call them, or commit and return
  the commit's own events for a repository without them, as every backend here is.
- **Sequences** are `bigint`s, because Go and Rust store them as 64-bit integers.

Errors are `EvtError`s with a `code`. `isConflict` and `isRetryable` tell them apart.

## Testing

`scenario` runs one command through a real `Store` over `MemRepository` and checks what it did:

```ts
await scenario(account)
  .given({type: 'bank_account.opened', data: {accountId: 'acct-1', balance: 100}})
  .when({type: 'deposit', amount: 25})
  .thenEvents({type: 'bank_account.money_deposited', data: {accountId: 'acct-1', amount: 25}})
```

The other checks are `thenNoEvents`, `thenRejected` (with a code or a predicate), `thenDuplicate`,
`thenError` and `thenState`. `run()` resolves to the outcome itself instead. `.entity()`,
`.metadata()` and `.options()` change the stream, the command's metadata and the store's options.
Each returns a new scenario, so one can be the base of several tests. `.on(fixture)` runs on a
shared `Fixture`, where every run seeds its given events and runs its command again.

`Fixture` seeds streams of any aggregate (`given`), reads them back decoded (`events`,
`assertEvents`) and loads them (`load`). After a snapshot and a compaction, seeding continues from
the snapshot. A failed check throws an `AssertionError` with `actual` and `expected`, so it works in
any test runner, and Vitest and Jest show a diff.

## Sharing streams with Go and Rust

A TypeScript writer and a Go or Rust writer can share a stream once the TypeScript backends ship, as
long as they agree on these:

- The event type strings, versions and payload JSON, and the aggregate state's JSON, because each
  language reads the others' snapshots. `json()` writes an object's properties in order, so declare
  them in the order of the Go struct's fields.
- Maps whose keys Go writes sorted. A Go map is a `Map` here, and it needs a codec of its own that
  decodes it back as a `Map`, since `json()` decodes plain objects.
- Integral payload numbers (cents, for example), because the languages format some floats
  differently.
- Event versions. Like Rust, TypeScript refuses a stored version it can't upcast to the declared
  one, and Go decodes it anyway, so deploy readers before writers when you bump a version.

- On a store whose Go or Rust writers record command outcomes, build the TypeScript `Store` with
  `outcomes` too, and run commands with `executeWithOutcome`. A command digests alike in every
  language only when its type and payload bytes match: Go marshals a command's map with its keys
  sorted, for example.

A TypeScript writer of an entity type that has commit-wide projectors registered in a Go or Rust
service must register the same projectors, writing the same bytes, as every writer of the type
does, and on PostgreSQL a view writer that issues the same statements.

The [known differences](https://github.com/photon-grove/evt/blob/main/docs/reference/storage-format.md#known-differences)
between the languages are in the storage format, and
[ADR 0009](https://github.com/photon-grove/evt/blob/main/docs/adr/0009-typescript-implementation.md)
explains how this package is built and tested.

## License

Apache-2.0.
