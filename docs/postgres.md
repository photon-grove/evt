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
  classified as transient so `policy.ExecuteWithRetry` retries it.
  Because compaction deletes keys, a commit also fails with a conflict when its first
  sequence is at or below the entity's snapshot `event_sequence`.
  An `entity_type` index backs `StreamFilter` narrowing.
- `evt_snapshots` — one durable snapshot per entity, keyed by `entity_id`.
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
