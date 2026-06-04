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
- Downstream consumers rely on **wipe-and-replay** to rebuild projections — a common event-sourcing
  guarantee that any view/read-model table is derived state, safe to wipe and rebuild by replaying
  the event log. A consumer's rebuild job historically replayed each stream from event sequence 1.
  Any compaction interacts directly with that guarantee.

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

- New `SnapshotStreamer.StreamEntitiesFromSnapshots(ctx, entityType, seedEntity, applyEvent)` seeds
  each entity from its inline snapshot before applying only the events recorded **after** the
  snapshot. Streams with no snapshot fall back to full replay from sequence 1 (unchanged behavior).
  The DynamoDB implementation uses the bounded-memory **enumerate-then-query** model: it enumerates
  distinct entity IDs and reconstructs each from its own partition (`GetSnapshot` + `GetLatestEvents`,
  sort-key ordered), so it does not depend on scan ordering.
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
- **Why keep it at all?** A survey of known consumers found **no production callers** (the only
  `Delete` references in consumer code are unrelated connection-registry / view-store methods of the
  same name; within `evt` it is used only by tests). `Delete` still has a legitimate local/staging
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
- **The `sk = 0` snapshot floor is monotonic in `eventSeq`.** `dynamo.PutSnapshot` writes
  conditionally and never lowers a stream's recorded `eventSeq`, so the floor cannot regress below
  already-compacted events. Without this, a stale background snapshot writer could overwrite `sk = 0`
  with an older `eventSeq`, and a later snapshot-aware load would query events in
  `(staleEventSeq, throughSequence]` that compaction has already deleted — rebuilding with missing
  history. The transactional commit path was already monotonic (its snapshot put is conditioned on
  the previous snapshot sequence); this extends the same guarantee to the standalone catch-up writer.

## Consequences

- **Forfeited property:** "replay from the very first event" no longer holds for compacted streams.
  This is the deliberate trade for bounded log growth.
- **Coordination with consumer wipe-and-replay:** wipe-and-replay of *projections* remains fully
  valid — but the replay must seed from snapshots (`RebuildConfig.SeedEntity`) rather than assume
  events 1..N exist. The "views are rebuildable from the event store" guarantee is preserved in
  substance; only the rebuild's starting point moves from "event 1" to "latest durable snapshot". A
  consumer's rebuild/backfill job should pass a `SeedEntity` callback (an entity factory keyed by
  type + `json.Unmarshal` of the snapshot payload) when it adopts a compacting version of `evt`.
- **Concurrency:** `CompactBelow` only removes low, immutable, already-snapshotted events. Concurrent
  command handlers only append higher sequences and advance the `sk = 0` snapshot forward (coverage
  only grows, and the floor is monotonic — see above), so they never touch the deleted range; the
  coverage check is safe without a transaction. Verified by integration tests that compact while
  appending and that reject a regressing snapshot write.
- **Known limitation — stale-context sequence reuse.** Deleting the covered event rows also removes
  the `attribute_not_exists(sk)` optimistic-lock evidence below the floor. A command handler that
  loaded a stale context *before* the covering snapshot existed (so its `CurrentSequence` is below
  the compacted floor) and then commits a plain, non-snapshot commit *after* compaction could
  re-create a sub-floor sequence number that DynamoDB would now accept. This does **not** corrupt
  reconstructed state: the post-compaction invariant requires the snapshot-aware path, which only
  applies events with `sk > snapshot.EventSequence` and therefore ignores any sub-floor rows — they
  are log-hygiene debris, not part of the rebuilt entity. In normal operation the window does not
  arise: a handler loads via `LoadEntity`, which seeds `CurrentSequence` from the snapshot, so its
  next sequence is always above the floor. Durable commit-side floor enforcement (e.g. a tombstone
  the commit path checks) is noted as future hardening for deployments that hold load contexts open
  across a snapshot and a compaction.

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
