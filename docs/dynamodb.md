# DynamoDB Integration

The `dynamo` package is the production storage backend. It implements the same
[`evt.Repository`](concepts.md#repository) contract as `mem`, so aggregates move
between them unchanged. This page documents the table shapes it expects, how to
wire it up, and the operational levers — snapshots, compaction, and retention —
that keep a growing log healthy.

> The key patterns, attribute names, and serialized formats below are a stability
> contract. Treat changes to them as breaking and document them. See
> [`BEHAVIORAL_INVARIANTS.md`](https://github.com/photon-grove/evt/blob/main/BEHAVIORAL_INVARIANTS.md).

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
- **DynamoDB Streams** with `NEW_IMAGE` when using async [projectors or
  publishers](streams.md)

Event rows are **append-only and protected by conditional writes** — a commit only
succeeds if the next sequence is unclaimed, which is how concurrent writers on one
entity stay correctly ordered. The `sk = 0` snapshot row records the entity's
serialized state plus the `eventSequence` it covers, and snapshot writes are
monotonic: a write that would move the covered sequence backwards is a no-op, so a
slow background writer can never regress the floor.

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

The raw `dynamo.Delete` helper is snapshot-unsafe, intended for local/staging
fixtures only, and excluded from production builds behind a `//go:build !prod`
tag. Prefer `CompactBelow` for any principled truncation. Background and rationale
live in [ADR 0001](adr/0001-event-compaction-and-snapshot-truncation.md).

### Retention

For **terminal, short-lived, fully transient** streams — scaffolding that drives a
one-time side effect and is never replayed by a rebuild — a per-entity-type
retention policy stamps a DynamoDB `ttl` so the table auto-expires those rows:

```go
repo := dynamo.NewRepository(client, eventsTable).
    WithRetention(dynamo.Retention{"scratch_job": 24 * time.Hour})
```

Only policed types ever carry a `ttl`; durable types are written without one and
are never expired. **This is dangerous for any stream a rebuild replays:** TTL
expires items individually, so an older prefix can vanish while newer events
survive, leaving a partial suffix on load. Use retention only when wipe-and-replay
does not depend on the events; for streams that accumulate over time, keep a
snapshot and use [compaction](#compaction) instead.

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
