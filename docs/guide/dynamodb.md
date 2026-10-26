# DynamoDB

The `dynamo` package is the DynamoDB storage backend. It implements the same
[`evt.Repository`](concepts.md#repository) contract as `mem`, so aggregates move between the two
unchanged. This page covers the table shapes, the wiring, what a commit actually sends, and the
snapshot, compaction and retention options for logs that keep growing.

The key patterns, attribute names and serialized formats below are a stability contract, so any
change to them is breaking and gets documented. The Rust `photon-evt` crate implements the same
[storage format](../reference/storage-format.md), which means Go and Rust services can share one
table. The
[behavioral invariants](https://github.com/photon-grove/evt/blob/main/docs/internal/behavioral-invariants.md)
have the full list of guarantees.

## Wiring

```go
repo := dynamo.NewRepository(client, "evt-local-event-log")
store := snapshots.NewStore(repo, 25)
```

`NewRepository(client, eventsTable)` takes an AWS SDK v2 DynamoDB client and the event-log table
name. The table must be initialized once before writers start, with `evt-doctor init` or
`repo.Initialize(ctx)` in a deploy step. Until then every write is refused with
`*evt.StoreNotInitializedError`, and [Permissions and billing](#permissions-and-billing) below says
why. You tune the repository with chainable options:

| Option | Effect |
| --- | --- |
| `WithScanSegments(n)` | Parallel-scan the table with `n` segments during rebuilds (faster, more capacity). |
| `WithConsistentRead(true)` | Strongly consistent reads on loads. |
| `WithRetention(retention)` | Stamp a DynamoDB `ttl` on policed entity types (see [Retention](#retention)). |
| `WithLazyInitialization(true)` | Initialize an uninitialized table on the first write instead of refusing it (tests and local use, and it needs `dynamodb:Scan`). |
| `WithViewsTable(name)` | Where commit-wide projectors write their rows (see [Entity-views table](#entity-views-table)). |
| `WithLogger(logger)` | Inject a `*slog.Logger` for structured logs. |

## Event-log table

One table holds the events and inline snapshots for every entity. The partition key `pk` (`S`) is
the entity ID and the sort key `sk` (`N`) is the event sequence, so an entity's events sit at
`sk = 1, 2, 3, …`. Two sort keys are reserved. `sk = 0` is the entity's inline snapshot row, and
`sk = -1` is the stream's head item, which holds `head` (and a `ttl` for retained types) and is the
one item a commit conditions on for concurrency
([Stream Heads](../reference/storage-format.md#stream-heads)). Event rows also carry `type`,
`version`, `entityType`, `payload` and `metadata`, plus an optional `ttl` (`N`) when a
[retention](#retention) policy applies. Turn on DynamoDB Streams with `NEW_IMAGE` if you use async
[projectors](streams.md).

Event rows are append-only and protected by conditional writes. A commit succeeds only if the next
sequence is unclaimed, and that's how concurrent writers on one entity stay ordered. Each written
stream starts its group with one conditional `Update` of its head item, `head = first − 1` (or
`attribute_not_exists(head)` for a stream that starts at 1). So no sequence is ever skipped, and a
commit that starts at or below a snapshot's `eventSequence` is rejected. That second rule stops a
writer that loaded before a compaction from recreating the deleted events
([Continuity](../reference/storage-format.md#continuity)). Compaction never moves the head, so a
writer whose load is current never conflicts with it.

Every commit also carries a feature-marker check and a rebuild-lock check. Of DynamoDB's 100
transaction items, a single-stream commit reserves three (the head update, the marker and the
rebuild lock). That leaves 97 events and projector operations whether the commit creates its
stream or appends to it, or 96 when it also writes a snapshot, since the snapshot put sits beside
the head update. These actions are billed as writes, and the commit budget counts only the actions
a commit actually emits. The full budget
([storage item budget](../reference/storage-format.md#writes)) is:

```text
items = Σ(events per written stream + 1 head update + 1 snapshot put if the commit writes one)
      + 1 × read-only fenced streams (one head check each)
      + outcome put (when the store records command outcomes)
      + feature-marker check
      + rebuild-lock check
      + projector operations
```

With [command outcomes](commands.md) enabled, the outcome row is the last action of every commit
and counts as one more item. A single-stream commit then leaves 96 events and projector
operations, creating or appending, or 95 with a snapshot. The outcome is a `Put` on
`(command-outcome#<id>, sk = 1)` conditioned on `attribute_not_exists(pk)`, and it's read back with
a strongly consistent `GetItem` whatever the repository's read-consistency setting. An
outcome-only commit, meaning an accepted no-op or a persisted rejection, is the two control checks
plus that put.

There are no reads before a commit. A failed commit is told apart by reading the stream on the
failure path only ([Classification](../reference/storage-format.md#failure-classification)). A head
item that disagrees with its log is `*evt.StreamHeadDriftError`. A store that holds data without
declaring the `stream-heads` feature is refused until it's migrated (initialization refuses it with
`*evt.StreamHeadsNotMigratedError`). An empty table is initialized once with `evt-doctor init`. A
table written by v0.8 is migrated with `evt-doctor migrate-heads`, which is stop-the-world and one-way
(see the [migration runbook](migrating-to-stream-heads.md)).

The `sk = 0` snapshot row records the entity's serialized state plus the `eventSequence` it
covers. Snapshot writes are monotonic. A write that would move the covered sequence backwards is a
no-op, so a slow background writer can never regress the floor.

### Permissions and billing

The head update is an `Update` action inside `TransactWriteItems`, so a writer's role needs
`dynamodb:UpdateItem` on the event-log table as well as `PutItem`, `ConditionCheckItem` and
`TransactWriteItems`. A writer never scans the event log, so its role needs no `dynamodb:Scan`. It
refuses a table that doesn't declare `stream-heads` with `*evt.StoreNotInitializedError` (Rust
`Error::StoreNotInitialized`) and writes nothing. You initialize the table once instead, at deploy
time, before any writer starts:

```sh
evt-doctor init -events-table event-log
```

You can also call `repo.Initialize(ctx)` (Rust `repository.initialize().await`) from a deploy step.
Initialization runs a strongly consistent `Scan` to prove the table holds no data, declares the
feature, then scans once more to prove no stream without a head item slipped in before the
declaration. So the initializing role needs `dynamodb:GetItem`, `dynamodb:Scan` and
`dynamodb:PutItem` on the event-log table. It's idempotent. On a table that already declares the
feature it reads the marker and stops, and two jobs initializing the same empty table at once let
exactly one of them write, so it's safe in every deploy, concurrent ones included. Tests, local
emulators and quickstarts can skip the step with
`dynamo.NewRepository(client, table).WithLazyInitialization(true)` (Rust
`.with_lazy_initialization(true)`). The first write then initializes the table itself, and that
writer needs `dynamodb:Scan`. Writers older than the head item never read the marker, so they have
to be stopped before the first head-aware writer runs against a table ([the
obligation](../reference/storage-format.md#required-feature)).

Every transactional action is billed as two writes of the checked or written item's size, rounded
up to 1 KB. The head item stays well under 1 KB, so a head update or a fence is the cheapest action
a commit can send, and it no longer scales with the size of a snapshot. Whether a `ConditionCheck`
bills by the size of the item it reads is documented only indirectly. It's still to be measured on
real DynamoDB with the `go/backends/dynamo/awsmeasure_test.go` script
([#159](https://github.com/photon-grove/evt/issues/159)).

### The rebuild lock

The rebuild-lock check is a `ConditionCheck` of `attribute_not_exists(pk)` on
`(SYSTEM#REBUILD, sk = 0)`, and it's the second action of every commit. It fails whenever a rebuild
holds the lock, an expired one included, and it's reported as `*evt.RebuildInProgressError` ahead
of every other failure except the marker check. Like the marker check, it's billed as one write
unit.

Rebuild workers manage the lock and its generation counter through `Repository.RebuildFence()`
(see [Fencing a rebuild](projections.md#fencing-a-rebuild)). Every lease-guarded write is
conditioned on the lock item's `owner` and `generation`, the fencing token `Begin` returned, so
`End` sends only its one transaction with no read first. The lifecycle's transactions write only
the two `SYSTEM#REBUILD*` items. A worker's role needs `dynamodb:PutItem`, `dynamodb:UpdateItem`,
`dynamodb:DeleteItem` and `dynamodb:ConditionCheckItem` on the table for them, and `GetItem` for the
consistent reads of the generation.

Because every commit checks the lock item, it's a hot item under heavy write traffic. A lifecycle
transaction that writes it can lose a race against the commits' checks (or another `Begin`), so
`Begin` retries from a fresh read up to eight times before returning the transient
`*evt.RebuildConflictError`. Retry it.

### Snapshots

`snapshots.NewStore(repo, snapshotSize)` wraps the repository and writes a snapshot roughly every
`snapshotSize` committed events. A load then reads the latest snapshot and only the events after
it, instead of replaying from sequence 1. The snapshot is a
[performance optimization, never a source of truth](concepts.md#snapshot), and replay can always
reproduce it.

### Compaction

Event rows are kept in full by default. A stream can opt into compaction:

```go
deleted, err := repo.CompactBelow(ctx, entityID, throughSequence)
```

`CompactBelow` deletes the events in `[1, throughSequence]`, but only after confirming the inline
`sk = 0` snapshot already covers them (`snapshot.EventSequence >= throughSequence`), and it never
deletes the snapshot row itself. An uncovered request returns `evt.ErrCompactionUncovered` and
deletes nothing. Once a stream is compacted, rebuild it through the snapshot-aware path
(`RebuildConfig.SeedAggregate` and `evt.SnapshotStreamer`) and never with a full replay from
sequence 1, because the early events are gone.

Feature-marker checks also refuse unsupported features and history-dependent entity types across
repository handles. Here's the tradeoff on DynamoDB. It fences each delete batch (at most 99 events
plus one marker check) rather than the whole compaction, because there's no whole-operation lease.
A declaration racing a multi-batch compaction can stop the later batches but can't roll back a
prefix that already committed, which leaves the stream partially compacted. A history-dependent
rebuild then reports `CompactionUncovered` instead of quietly replaying incomplete history. So
declare it early, as [keeping the log](concepts.md#keeping-the-log) says. PostgreSQL holds its shared
feature advisory lock through validation and all deletes in one transaction instead (see
[Feature Marker](../reference/storage-format.md#feature-marker)).

There's no raw delete helper. For fixture teardown or a reset, delete the items with the AWS SDK or
recreate the table. `CompactBelow` is for snapshot-verified truncation in any environment, not
fixture cleanup, since it only deletes a snapshot-covered prefix.
[ADR 0001](../adr/0001-event-compaction-and-snapshot-truncation.md) has the background.

### Retention

For terminal, short-lived, fully transient streams that no rebuild replays, `WithRetention` stamps
a DynamoDB `ttl`:

```go
repo := dynamo.NewRepository(client, eventsTable).
    WithRetention(dynamo.Retention{"scratch_job": 24 * time.Hour})
```

Only policed types get a `ttl`, and durable types never expire. A retained stream's head item
carries a `ttl` too, at least that of every other item in the stream and never lowered. Each commit
raises it to the larger of its own and the largest `ttl` this repository loaded for the stream. A
writer that hasn't loaded the stream conflicts on the `ttl` condition and passes on its retry. An
out-of-band `PutSnapshot` refreshes it in the same transaction.

TTL deletion is lazy and unordered, so a stream can be partly deleted for a while. A commit that
finds a head item absent while every remaining item has expired gets a conflict that heals once
they're deleted, not `StreamHeadDriftError`. And because TTL expires items individually, an older
prefix can disappear while newer events remain. So use retention only for streams that are never
replayed and never appended to after they become terminal. For streams that accumulate, keep a
snapshot and use [compaction](#compaction).

Read fences, meaning checks that a stream you read but didn't write is still at the head you saw,
refuse retained types. TTL deletes items lazily, so a retained stream can expire and be recreated
at the same head, and a fence can't tell the new stream from the old one. So a fence on a retained
entity type is refused with a retention conflict. The refusal combines two sources: this
repository's own `WithRetention` configuration, checked without any I/O, and the retention set in
the durable feature marker. Local configuration alone protects only this process. So also declare
each retained type in the marker (`FeatureMarker.Declare`, adding it to `Retention`) and every
other writer refuses fences on it too. The Rust crate's `with_retention` follows the same rule.

## Entity-views table

Read models live in their own table, separate from the log. Its `pk` (`S`) is the
projection-owned lookup key and its `sk` (`S`) is the view sort key, defaulting to `VIEW`
(`evt.DefaultViewSK`). An `entityType` (`S`) attribute backs the `entityType-index` GSI for
type-wide queries, and a `ttl` (`N`) is there for when expiring derived rows is useful.

Commit-wide projectors write to this table too. Configure the repository with
`dynamo.NewRepository(client, events).WithViewsTable("views")`, and every view operation in a commit
becomes one `Put` or `Delete` in the commit's transaction, after the feature-marker and
rebuild-lock checks and before the written streams. A put carries all five attributes (`pk`, `sk`,
`entityID`, `entityType`, `payload`) and never a `ttl`. An `IfAbsent` put is conditioned on
`attribute_not_exists(pk)` and an `IfPresent` delete on `attribute_exists(pk)`. A failed condition
is a `*evt.ProjectorOperationError`. The rules are in
[commit-wide projectors](projections.md#commit-wide-projectors).

Views are a rebuildable cache, not a source of truth. The `viewcodec` package offers a typed JSON
helper over `evt.ViewRepository` so projectors and readers share one codec. Use `viewcodec.New[T]`
for arbitrary pk/sk, or `viewcodec.NewSingle` for the common one-row-per-entity pattern. See
[Projections and rebuilds](projections.md).

`dynamo.ViewRepository` reads the base table strongly consistently. `GetView` and `BatchGetViews`
always do, and the partition listings (`ListViewsByPK`, `ListViewsByPKPaged`, `ListViewsByPKEach`)
do by default. So a handler that commits and then reads the rows its commit wrote, to return them
or to validate the next step, sees them. A strongly consistent read costs twice the read capacity
of an eventually consistent one (one read unit per 4 KB instead of per 8 KB). A listing that can
tolerate a stale page can opt out with `WithConsistentRead(false)` on the `*dynamo.ViewRepository`.
`NewViewRepository` returns the interface, so type-assert it first. Point reads stay strong either
way. The entity-type listings read the `entityType-index` global secondary index, which DynamoDB
can only read eventually consistently, so a row a commit just wrote can be missing from them for a
moment. Rust's `DynamoViewRepository` behaves the same way (`with_consistent_read`).

## Local tables

The [`infra/local`](https://github.com/photon-grove/evt/tree/main/infra/local) Terraform stack
creates emulator tables that match the integration suite, `evt-local-event-log` and
`evt-local-entity-views`. Apply it against a local DynamoDB-compatible emulator (Moto) before you
run the integration tests, and keep real account IDs, ARNs and hostnames out of this repo.
