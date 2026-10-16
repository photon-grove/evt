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
- optional `ttl` (`N`) when a [retention](#retention) policy applies
- **DynamoDB Streams** with `NEW_IMAGE` when using async [projectors](streams.md)

Event rows are **append-only and protected by conditional writes** — a commit only
succeeds if the next sequence is unclaimed, which is how concurrent writers on one
entity stay correctly ordered. Each commit also checks the `sk = 0` snapshot row: a
commit that starts at or below the snapshot's `eventSequence` is rejected, so a writer
that loaded before a compaction cannot recreate the deleted events. Every v0.9 commit also carries a
feature-marker check, and a commit that appends to an existing stream also pins the stream's head
with a continuation check, so no sequence is ever skipped ([Continuity](spec/storage-format.md#continuity)).
Of the 100 transaction items, a single-stream commit that creates its stream reserves two (floor +
marker), leaving **98 event/projector operations**, and one that appends to an existing stream
reserves three (continuation + floor + marker), leaving **97**. A commit with a snapshot leaves
the same numbers: the snapshot put replaces the floor check. These checks are billed as writes. The
commit budget counts only the actions a commit actually emits.

The full v0.9 target ([storage item budget](spec/storage-format.md#writes), C1/C2/C3/C10) is:

```text
items = Σ(events per written stream + 1 snapshot/floor + 1 continuation if first sequence > 1)
      + 2 × read-only fenced streams
      + outcome put (when the store records command outcomes)
      + feature-marker check
      + rebuild-lock check (once C10 emits it)
      + projector operations
```

With command outcomes enabled (C3), the outcome row is the last action of every commit and counts
as one more item, so a single-stream commit that creates its stream leaves **97** event/projector
operations (96 when it appends to an existing stream). The outcome is a `Put` on
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

Only policed types receive `ttl`; durable types never expire. TTL expires items
individually, so an older prefix can disappear while newer events remain. Use
retention only for streams never replayed and never appended after becoming
terminal. For accumulating streams, keep a snapshot and use [compaction](#compaction).

**Read fences refuse retained types.** TTL deletion reopens a fenced slot without leaving a
successor snapshot, so a fence on a retained entity type is refused with a retention conflict.
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
