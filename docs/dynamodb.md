# DynamoDB Integration

The `dynamo` package is the DynamoDB storage backend. It implements the same
[`evt.Repository`](concepts.md#repository) contract as `mem`, so aggregates move
between them unchanged. This page documents the required table shapes, wiring, and
the snapshot, compaction, and retention options for growing logs.

> The key patterns, attribute names, and serialized formats below are a stability
> contract. Treat changes to them as breaking and document them. See
> [`BEHAVIORAL_INVARIANTS.md`](https://github.com/photon-grove/evt/blob/main/BEHAVIORAL_INVARIANTS.md)
> and the [storage format](spec/storage-format.md), which the Rust `photon-evt`
> crate implements too: Go and Rust services can share one table.

## Wiring

```go
repo := dynamo.NewRepository(client, "evt-local-event-log")
store := snapshots.NewStore(repo, 25)
```

`NewRepository(client, eventsTable)` takes an AWS SDK v2 DynamoDB client and the
event-log table name. Tune it with chainable options:

| Option | Effect |
| --- | --- |
| `WithScanSegments(n)` | Parallel-scan the table with `n` segments during rebuilds (faster, more capacity). |
| `WithConsistentRead(true)` | Strongly consistent reads on loads. |
| `WithRetention(retention)` | Stamp a DynamoDB `ttl` on policed entity types (see [Retention](#retention)). |
| `WithLogger(logger)` | Inject a `*slog.Logger` for structured logs. |

## Event-log table

A single table holds events and inline snapshots for every entity.

- `pk` (`S`) — the entity ID
- `sk` (`N`) — the event sequence (`1, 2, 3, …`)
- `sk = 0` — the inline **snapshot** row for that entity
- plus `type`, `version`, `entityType`, `payload`, and `metadata` attributes
- `sk = -1` — the stream's **head item** (`head`, and `ttl` for retained types), the one item a
  commit conditions on for concurrency ([Stream Heads](spec/storage-format.md#stream-heads))
- optional `ttl` (`N`) when a [retention](#retention) policy applies
- **DynamoDB Streams** with `NEW_IMAGE` when using async [projectors](streams.md)

Event rows are **append-only and protected by conditional writes** — a commit only
succeeds if the next sequence is unclaimed, which is how concurrent writers on one
entity stay correctly ordered. Each written stream also starts its group with one conditional
`Update` of its head item: `head = first − 1` (or `attribute_not_exists(head)` for a stream that
starts at 1), so no sequence is ever skipped and a commit that starts at or below a snapshot's
`eventSequence` is rejected, which keeps a writer that loaded before a compaction from recreating the
deleted events ([Continuity](spec/storage-format.md#continuity)). Compaction never moves the head,
so a writer whose load is current never conflicts with it. Every v0.9 commit also carries a
feature-marker check. Of the 100 transaction items, a single-stream commit reserves two (head
update + marker), leaving **98 event/projector operations** whether it creates its stream or appends
to it, and **97** when it also writes a snapshot (the snapshot put sits beside the head update).
These actions are billed as writes. The commit budget counts only the actions a commit actually
emits.

**Permissions and billing.** The head update is an `Update` action inside `TransactWriteItems`, so a
writer's role needs `dynamodb:UpdateItem` on the event-log table as well as `PutItem`,
`ConditionCheckItem` and `TransactWriteItems`. The first write to a table that does not declare
`stream-heads` runs a strongly consistent `Scan` to prove the table holds no data, declares the
feature, and scans once more to prove no stream without a head item slipped in before the
declaration (`dynamodb:Scan`); a writer to a table that already declares the feature never scans.
Writers older than the head item never read the marker, so they must be stopped before the first
head-aware writer runs against a table ([the obligation](spec/storage-format.md#required-feature)). Every
transactional action is billed as two writes of the checked or written item's size, rounded up to
1 KB; the head item stays well under 1 KB, so a head update or a fence is the cheapest action a commit
can send, and it no longer scales with the size of a snapshot. (Whether a `ConditionCheck` bills by
the size of the item it reads is documented only indirectly, and is still to be measured on real
DynamoDB with the `dynamo/awsmeasure` script: [#159](https://github.com/photon-grove/evt/issues/159).)

There are no reads before a commit. A failed commit is told apart by reading the stream on the
failure path only ([Classification](spec/storage-format.md#failure-classification)). A head item
that disagrees with its log is `*evt.StreamHeadDriftError`, and a store that holds data without
declaring the `stream-heads` feature is refused with `*evt.StreamHeadsNotMigratedError` until it is
migrated; an empty table is declared by its first writer, so a fresh table needs no step. A table
written by v0.8 is migrated with `evt-doctor migrate-heads` (stop-the-world, one-way): see the
[migration runbook](migrating-to-stream-heads.md).

The full v0.9 target ([storage item budget](spec/storage-format.md#writes), C1/C2/C3/C10) is:

```text
items = Σ(events per written stream + 1 head update + 1 snapshot put if the commit writes one)
      + 1 × read-only fenced streams (one head check each)
      + outcome put (when the store records command outcomes)
      + feature-marker check
      + rebuild-lock check (once C10 emits it)
      + projector operations
```

With command outcomes enabled (C3), the outcome row is the last action of every commit and counts
as one more item, so a single-stream commit leaves **97** event/projector operations, creating or
appending (96 when it also writes a snapshot). The outcome is a `Put` on
`(command-outcome#<id>, sk = 1)` conditioned on `attribute_not_exists(pk)`; it is read back with a
strongly consistent `GetItem`, whatever the repository's read-consistency setting. An outcome-only
commit (an accepted no-op or a persisted rejection) is the feature-marker check plus that put.

The rebuild-lock check (C10) is **not implemented yet** and is not budgeted; when C10 starts
emitting it, the two capacities above drop by one, to **97** and **96**.

The `sk = 0` snapshot row records the entity's serialized state plus the
`eventSequence` it covers, and snapshot writes are monotonic: a write that would move
the covered sequence backwards is a no-op, so a slow background writer can never
regress the floor.

### Snapshots

`snapshots.NewStore(repo, snapshotSize)` wraps the repository and writes a snapshot
roughly every `snapshotSize` committed events. Loading then reads the latest
snapshot and only the events after it, instead of replaying from sequence 1. The
snapshot is a [performance optimization, never a source of truth](concepts.md#snapshot)
— it is always reproducible by replay.

### Compaction

Event rows are retained in full by default. A stream may opt into compaction with:

```go
deleted, err := repo.CompactBelow(ctx, entityID, throughSequence)
```

`CompactBelow` deletes events in `[1, throughSequence]` — but **only after
confirming the inline `sk = 0` snapshot already covers them**
(`snapshot.EventSequence >= throughSequence`), and it never deletes the snapshot
row itself. An uncovered request returns `evt.ErrCompactionUncovered` and deletes
nothing. After a stream is compacted you **must** rebuild it through the
snapshot-aware path (`RebuildConfig.SeedEntity` / `evt.SnapshotStreamer`), never a
full replay from sequence 1 — the early events are gone.

Feature-marker checks also refuse unsupported features and history-dependent entity types
across repository handles. DynamoDB fences **each delete batch** (at most 99 events plus
one marker check), not the entire compaction: there is no whole-operation lease. A declaration
racing an in-progress multi-batch compaction can stop later batches but cannot roll back an
already committed prefix, leaving the stream partially compacted. A history-dependent rebuild
then reports `CompactionUncovered`, rather than silently replaying incomplete history. Declare
history dependence **before writing data of that entity type**. PostgreSQL instead holds its
shared feature advisory lock through validation and all deletes in one transaction. See
[Feature Marker](spec/storage-format.md#feature-marker).

The raw `dynamo.Delete` helper is snapshot-unsafe, intended for local/staging
fixtures only, and excluded from production builds behind a `//go:build !prod`
tag. It is deprecated in v0.9.0
([ADR 0006](adr/0006-parity-policy-and-feature-cuts.md), K7) and will be removed in the next
minor release with no in-library replacement: for fixture teardown or reset, delete the items
with the AWS SDK or recreate the table. (`CompactBelow` is for snapshot-verified truncation, not
fixture cleanup — it only deletes a snapshot-covered prefix and never deletes the snapshot row.)
Prefer `CompactBelow` for principled truncation in any environment. Background and rationale
live in [ADR 0001](adr/0001-event-compaction-and-snapshot-truncation.md).

### Retention

For **terminal, short-lived, fully transient** streams that no rebuild replays,
`WithRetention` stamps a DynamoDB `ttl`:

```go
repo := dynamo.NewRepository(client, eventsTable).
    WithRetention(dynamo.Retention{"scratch_job": 24 * time.Hour})
```

Only policed types receive `ttl`; durable types never expire. A retained stream's head item
carries a `ttl` too, at least that of every other item of the stream and never lowered: each commit
raises it to the larger of its own and the largest `ttl` this repository loaded for the stream
(a writer that has not loaded the stream conflicts on the `ttl` condition and passes on its retry),
and an out-of-band `PutSnapshot` refreshes it in the same transaction. TTL deletion is lazy and
unordered, so a stream can be partly deleted for a while; a commit that finds a head item absent
while every remaining item has expired gets a conflict that heals once they are deleted, not
`StreamHeadDriftError`. TTL expires items
individually, so an older prefix can disappear while newer events remain. Use
retention only for streams never replayed and never appended after becoming
terminal. For accumulating streams, keep a snapshot and use [compaction](#compaction).

**Read fences refuse retained types.** TTL deletion is lazy, so a fence on a retained stream is not
incarnation-proof (the stream can expire and be created again at the same head), and a fence on a
retained entity type is refused with a retention conflict.
The refusal is the union of two sources: this repository's own `WithRetention` configuration
(checked without any I/O) and the retention set in the durable feature marker. Local
configuration alone protects only this process. Also declare each retained type in the marker
(`FeatureMarker.Declare`, adding it to `Retention`) so every other writer refuses fences on it too;
the Rust crate's `with_retention` follows the same rule.

## Entity-views table

Read models live in their own table, separate from the log:

- `pk` (`S`) — the projection-owned lookup key
- `sk` (`S`) — the view sort key, defaulting to `VIEW` (`evt.DefaultViewSK`)
- `entityType` (`S`) — backing the `entityType-index` GSI for type-wide queries
- `ttl` (`N`) — when expiring derived rows is useful

Views are a **rebuildable cache, not a source of truth**. The `viewstore` package
offers a typed JSON helper over `evt.ViewRepository` so projectors and readers
share one codec — `viewstore.New[T]` for arbitrary pk/sk, or `viewstore.NewSingle`
for the common "one row per entity" pattern. See
[Projections and rebuilds](projections.md).

## Entity-heads table

*Deprecated in v0.9.0 ([ADR 0006](adr/0006-parity-policy-and-feature-cuts.md), K4):* the heads
table and `dynamo.HeadStore` are cut — derive entity heads from the log with
`Repository.StreamEntityHeads` (a scan-and-fold over the event log) instead. The paragraph below
describes the pre-v0.9 table for readers still on it.

To support [incremental rebuilds](projections.md), the repository can track one
small row per entity (`pk` = entity ID) recording its current head sequence. This
lets a rebuild enumerate entity heads with a key-only scan and re-project only the
streams that changed, instead of re-reading the whole log.

## Local tables

The [`infra/local`](https://github.com/photon-grove/evt/tree/main/infra/local)
Terraform stack creates emulator tables that match the integration suite —
`evt-local-event-log`, `evt-local-entity-views`, and `evt-local-entity-heads`.
Apply it against a local DynamoDB-compatible emulator (Moto) before running
integration tests; keep real account IDs, ARNs, and hostnames out of this repo.
