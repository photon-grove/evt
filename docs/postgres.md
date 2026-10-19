# PostgreSQL Integration

The `evt/postgres` package implements the same backend-neutral `evt.Repository`
and `evt.Store` contracts as DynamoDB, on a relational store. The aggregate,
projector, and snapshot code you write does not change between backends — only
the wiring does. The backend passes `conformance.RunRepositorySuite` with
`SupportsSnapshots` and `EnforcesOptimisticConcurrency` enabled. The Rust
`photon-evt` crate reads and writes the same tables (see the
[storage format](spec/storage-format.md)).

## Wiring

```go
pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
if err != nil {
    return err
}

repo := postgres.NewRepository(pool)
if err := repo.EnsureSchema(ctx); err != nil { // idempotent CREATE TABLE IF NOT EXISTS
    return err
}

store := postgres.NewStoreFromRepo(repo) // or postgres.NewStore(pool)
```

`NewRepository` accepts any `*pgxpool.Pool` (or anything satisfying the small
`postgres.DB` interface) and `WithEventsTable` / `WithSnapshotsTable` options so
several logical event logs can share one database.

## Schema

`EnsureSchema` owns the table definitions so the relational schema stays in
lockstep with the Go types that read and write it:

- `evt_events` — the append-only log, keyed by a `(entity_id, sequence)` primary
  key. The unique key gives optimistic concurrency: a duplicate
  `(entity_id, sequence)` insert fails and surfaces as an `evt.ConflictError`,
  classified as transient, so fence-aware retry re-prepares the command.
  The commit conditions on the stream's row in the head table below, so a commit whose first
  sequence is at or below the entity's snapshot `event_sequence` also fails with a conflict even
  though compaction deleted the keys.
  An `entity_type` index backs `StreamFilter` narrowing.
- `evt_snapshots` — one durable snapshot per entity, keyed by `entity_id`.
- `evt_events__evt_stream_heads` — one row per written stream (ADR 0007), keyed by `entity_id`:
  `entity_type` and `head`, the highest event sequence ever committed to the stream. A commit
  inserts the row (a new stream) or advances it with `UPDATE … WHERE head = first − 1`, and a fence
  is one read of it, all under the per-entity advisory locks. A head row that disagrees with the log
  is `*evt.StreamHeadDriftError`. A store that holds data must declare the `stream-heads` feature
  before a writer writes (`*evt.StreamHeadsNotMigratedError`), and an empty one is declared by its
  first writer; entity heads read this table once it is declared, and the aggregate over events and
  snapshots until then. Run `EnsureSchema` to create it. A database written by v0.8 is migrated with
  `evt-doctor migrate-heads -backend postgres`, which backfills, verifies, and declares in one
  transaction: see the [migration runbook](migrating-to-stream-heads.md).
- `evt_events__evt_command_outcomes` — one row per command (v0.9, C3), keyed by `command_id`:
  `status`, the identity `digest`, the stored `response`, the `events` the commit wrote, and a
  rejection's `rejection` and `code`. The row is inserted last in the commit transaction; a unique
  violation means another writer recorded the command first and surfaces as
  `*evt.OutcomeConflictError`, which the store resolves by reading the row. Its name derives from the
  events table through the reserved `__evt_` infix. Use `Store.WithOutcomes` to record outcomes.

Every `SerializedResult` is committed inside a single SQL transaction, so events
and their snapshot are never observable apart.

### Compaction

A stream may opt into compaction with
`evt.Compactor.CompactBelow(ctx, entityID, throughSequence)`, which deletes event
rows in `[1, throughSequence]` — but only after confirming the durable snapshot
covers them (`snapshot.EventSequence >= throughSequence`). The snapshot lives in
its own table and is never touched, so it remains the authoritative floor.
Uncovered requests return `evt.ErrCompactionUncovered` and delete nothing.
Rebuild compacted streams via the snapshot-aware path
(`evt.SnapshotStreamer.StreamEntitiesFromSnapshots` / `RebuildConfig.SeedEntity`).
See [ADR 0001](adr/0001-event-compaction-and-snapshot-truncation.md).

### Incremental rebuilds

The repository implements `evt.EntityHeadStreamer` and `evt.EntityHeadVisitor`
over a streamed `SELECT entity_id, MAX(sequence) …` cursor, reporting each
entity's head with constant memory for change-detection rebuilds. The head folds
in the snapshot's recorded sequence, so it stays correct for streams whose early
events were compacted away.

## Local Development

The `infra/local-postgres` Terraform stack provisions the local database; the
Repository owns the tables inside it. Apply it before running integration tests:

```sh
docker run -d --name evt-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:17
terraform -chdir=infra/local-postgres init
terraform -chdir=infra/local-postgres apply -auto-approve
moon run evt:integration-postgres
```

Credentials are local-development placeholders only.
