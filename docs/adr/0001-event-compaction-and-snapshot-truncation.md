# ADR 0001: Event-log compaction via snapshot-and-truncate

## Status

Accepted (2026-06-04)

## Context

- `evt` is an append-only event store. Each entity (stream) keeps its events in DynamoDB keyed by
  `pk = entityID`, `sk = sequence` (N ≥ 1), with an inline snapshot at `sk = 0`. The `snapshots`
  package takes a snapshot every N events and, on load, replays only events after the snapshot's
  `eventSeq`. Snapshots were purely a **load-time optimization**: every event was retained forever,
  and the log only ever grew.
- Unbounded growth is a real operational cost. Long-lived, high-churn streams accumulate thousands
  of events that are never read again once a newer snapshot exists, inflating storage and the cost
  of any full scan (rebuilds, exports).
- The only existing deletion path was `dynamo.Repository.Delete`, a raw point-delete by `(pk, sk)`
  with no snapshot-safety logic and a comment warning "use only in local and staging". It was unsafe
  to use as a compaction primitive and carried no enforcement.
- Consuming apps rely on **wipe-and-replay** to rebuild projections. In `photon-grove/apps`,
  [ADR 0012 — _Views are projections of immutable events_](https://github.com/photon-grove/apps/blob/main/docs/adr/0012-views-are-projections-of-immutable-events.md)
  guarantees that any view table can be wiped and rebuilt by replaying the event log, and
  `cmd/rebuild-projections` does exactly that. Historically that replay started from event sequence
  1. Any compaction interacts directly with this guarantee.

## Decision

Introduce **principled, snapshot-verified compaction** and make the full-replay path
**snapshot-aware**, so that truncating events below a durable snapshot is safe.

### 1. `CompactBelow` (new repository capability)

```go
CompactBelow(ctx, entityID, throughSequence) (deleted int, err error)
```

- Deletes events for `entityID` whose sequence is in `[1, throughSequence]`, but **only after**
  reading the inline snapshot (`sk = 0`) and verifying `snapshot.EventSequence >= throughSequence`
  — i.e. every event being deleted is already captured by a durable snapshot.
- If no snapshot exists, or it does not cover `throughSequence`, it deletes nothing and returns
  `evt.ErrCompactionUncovered`. The `sk = 0` snapshot row is never deleted. `throughSequence < 1`
  is a no-op. The operation is idempotent (re-running over an already-empty range deletes nothing).
- Implemented in the DynamoDB repository (key-only range query + `BatchWriteItem` deletes with
  bounded `UnprocessedItems` retries) and in the in-memory repository.

### 2. Snapshot-aware rebuild

- New `SnapshotStreamer.StreamEntitiesFromSnapshots(ctx, expr, seedEntity, applyEvent)` seeds each
  entity from its inline snapshot before applying only the events recorded **after** the snapshot.
  Streams with no snapshot fall back to full replay from sequence 1 (unchanged behavior).
- `RebuildProjections` gains an opt-in `RebuildConfig.SeedEntity` callback. When set, the rebuild
  uses the snapshot-aware stream (required for correctness after compaction); when nil, it uses the
  legacy `StreamEntities` full-replay path. If `SeedEntity` is set but the repository does not
  implement `SnapshotStreamer`, `RebuildProjections` fails fast rather than silently producing
  partial state.

### 3. Capability interfaces, not core-interface expansion

`Compactor` and `SnapshotStreamer` are **optional capability interfaces** detected by type
assertion (the same pattern the `snapshots` package already uses for `PutSnapshot`). They are
deliberately **not** added to the core `evt.Repository` interface, so this change is purely
additive and does not break existing `evt.Repository` implementations (including test fakes in
consumer repos). Backends opt in by implementing the methods; `dynamo` and `mem` both do.

### 4. Fate of the raw `Delete`

- **Kept, but compiled out of production builds.** `dynamo.Repository.Delete` is now guarded by
  `//go:build !prod`. Released artifacts build with `-tags prod` (wired in `.goreleaser.yml`), so a
  production binary physically does not contain `Delete` and cannot call it. Consumers that build
  their own production binaries from source should add `-tags prod`.
- **Why keep it at all?** A survey of consumers found **no production callers** (in
  `photon-grove/apps` the only `Delete` references are unrelated connection-registry / view-store
  methods; within `evt` it is used only by tests). `Delete` still has a legitimate local/staging
  role — point-deleting fixtures to reset test data — that `CompactBelow` deliberately cannot serve,
  because `CompactBelow` refuses to remove anything not covered by a snapshot. Removing `Delete`
  entirely would break that fixture-reset workflow for no safety gain beyond the build tag.
- `CompactBelow` is the supported path for log truncation in any environment, including production.

## New invariants

- A stream's events **below its latest durable snapshot's `eventSeq` are not required for rebuild.**
  The authoritative starting point of a compacted stream is its snapshot, not event 1.
- Compaction never deletes an event unless a durable snapshot already captures it
  (`snapshot.EventSequence >= throughSequence`). The `sk = 0` snapshot row is never deleted.
- Correct rebuild of a compacted stream **requires** the snapshot-aware path
  (`SnapshotStreamer` / `RebuildConfig.SeedEntity`). Legacy full-replay-from-1 over a compacted
  stream would reconstruct incorrect state and must not be used once compaction has run.

## Consequences

- **Forfeited property:** "replay from the very first event" no longer holds for compacted streams.
  This is the deliberate trade for bounded log growth.
- **Coordination with ADR 0012:** wipe-and-replay of *projections* remains fully valid — but the
  replay must seed from snapshots (`RebuildConfig.SeedEntity`) rather than assume events 1..N
  exist. ADR 0012's guarantee is preserved in substance (views are still rebuildable from the event
  store); only the rebuild's starting point moves from "event 1" to "latest durable snapshot".
  `cmd/rebuild-projections` in `photon-grove/apps` should pass a `SeedEntity` callback
  (`NewEntityForType` + `json.Unmarshal` of the snapshot payload) when it adopts a compacting
  version of `evt`.
- **Concurrency:** `CompactBelow` only removes low, immutable, already-snapshotted events. Concurrent
  command handlers only append higher sequences and advance the `sk = 0` snapshot forward (coverage
  only grows), so they never touch the deleted range; the coverage check is safe without a
  transaction. Verified by an integration test that compacts while appending.

## Backward compatibility

- Purely additive at the package level: no existing `evt.Repository` method changed signature, and
  the new capabilities are optional interfaces. Existing callers and implementers compile unchanged.
- This is a v0.1.x minor, additive change. Consumers gain compaction by opting in (type-asserting
  `Compactor` / setting `SeedEntity`); doing nothing preserves today's behavior, including
  full-replay-from-1, because no events are deleted until `CompactBelow` is called.

## Migration / rollout

1. **Upgrade only** — bump `evt`. No data migration; nothing is deleted until `CompactBelow` runs.
2. **Make rebuild snapshot-aware first.** Before compacting any stream, update rebuild/backfill
   jobs to pass `RebuildConfig.SeedEntity`. This is safe on uncompacted data (it still seeds from
   snapshots when present and falls back to full replay otherwise), so it can ship ahead of any
   deletion.
3. **Then compact.** Run `CompactBelow(ctx, entityID, snapshot.EventSequence)` per stream (typically
   driven by a background job that reads the current snapshot). It is idempotent and refuses any
   uncovered range, so it is safe to re-run.
4. **Production builds** should add `-tags prod` to exclude the raw `Delete`; released `evt`
   binaries already do.
5. **Rollback:** because compaction is opt-in and additive, rolling back the library is safe for
   streams that were never compacted. For streams already compacted, retain a DynamoDB
   point-in-time-recovery / backup window if you need the ability to restore pre-compaction history.
