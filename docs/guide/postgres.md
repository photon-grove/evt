# PostgreSQL

The `evt/go/backends/postgres` package implements the same backend-neutral `evt.Repository` and
`evt.Store` contracts as DynamoDB, on a relational store. The aggregate, projector and snapshot code
you write doesn't change between backends. Only the wiring does. The backend passes
`conformance.RunRepositorySuite` with `SupportsSnapshots` and `EnforcesOptimisticConcurrency`
enabled, and the Rust `photon-evt` crate reads and writes the same tables (see the
[storage format](../reference/storage-format.md)).

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

`NewRepository` accepts any `*pgxpool.Pool`, or anything that satisfies the small `postgres.DB`
interface. The `WithEventsTable` and `WithSnapshotsTable` options let several logical event logs
share one database.

## Schema

`EnsureSchema` owns the table definitions, so the relational schema stays in lockstep with the Go
types that read and write it. Every `SerializedResult` is committed inside a single SQL
transaction, so events and their snapshot are never observable apart. With the default names, the
tables are these.

`evt_events` is the append-only log, with a `(entity_id, sequence)` primary key. That unique key is
what gives optimistic concurrency: a duplicate `(entity_id, sequence)` insert fails and surfaces as
an `evt.ConflictError`, which is classified as transient, so fence-aware retry re-prepares the
command. The commit also conditions on the stream's row in the head table, so a commit whose first
sequence is at or below the entity's snapshot `event_sequence` fails with a conflict too, even
though compaction deleted those keys. An `entity_type` index backs `StreamFilter` narrowing.

`evt_snapshots` holds one durable snapshot per entity, keyed by `entity_id`. A commit's snapshot
must continue the stored one's sequence (1 for the first, then the stored sequence plus one), as on
DynamoDB: the commit reads the stored sequence under the entity's lock and fails with a transient
`evt.ConflictError` otherwise.

`evt_events__evt_stream_heads` holds one row per written stream
([ADR 0007](../adr/0007-per-stream-head-item.md)), keyed by `entity_id`, with `entity_type` and
`head`, the highest event sequence ever committed to the stream. A commit inserts the row for a new
stream or advances it with `UPDATE … WHERE head = first − 1`, and a fence is one read of it, all
under the per-entity advisory locks. A head row that disagrees with the log is
`*evt.StreamHeadDriftError`. A store that holds data must declare the `stream-heads` feature before
a writer writes (`*evt.StreamHeadsNotMigratedError` until then). An empty one is initialized by its
first writer, which costs two `EXISTS` probes under the feature lock, so writers keep doing it
unlike DynamoDB's. You can also initialize explicitly with `repo.Initialize(ctx)` or
`evt-doctor init -backend postgres`. Entity heads read this table once the feature is declared, and the aggregate over
events and snapshots until then. A database written by v0.8 is migrated with
`evt-doctor migrate-heads -backend postgres`, which backfills, verifies and declares in one
transaction (see the [migration runbook](migrating-to-stream-heads.md)).

`evt_events__evt_command_outcomes` holds one row per command when the store records
[command outcomes](commands.md) (`Store.WithOutcomes`). It's keyed by `command_id` and holds the
`status`, the identity `digest`, the stored `response`, the `events` the commit wrote, and a
rejection's `rejection` and `code`. The row is inserted last in the commit transaction. A unique
violation means another writer recorded the command first, and it surfaces as
`*evt.OutcomeConflictError`, which the store resolves by reading the row. The table's name derives
from the events table through the reserved `__evt_` infix.

`evt_events__evt_rebuild_lock` and `evt_events__evt_rebuild_generation` are the
[rebuild fence](projections.md#fencing-a-rebuild), with one row each at `id = 0`. The lock row names
the `owner` and the `lease_expires_at` of the rebuild that holds it. The generation row counts
`value`, odd while a rebuild is in progress, with a `force_released` flag. Every commit takes the
shared advisory lock `pg_advisory_xact_lock_shared(1702261761, hashtext('evt_events:rebuild'))` and
asserts that the lock row is absent (an expired row counts as present) and the generation is even.
A force-release leaves it odd until a rebuild ends. If either assertion fails, the commit fails
with `*evt.RebuildInProgressError`. The lifecycle (`Repository.RebuildFence()`: `Begin`, `Renew`,
`End`, `ForceRelease`, `WriteViews`) takes the same advisory lock exclusively for its short
transactions, so no acquire or release can slip between a commit's assertion and its `COMMIT`. A
guarded view write selects the lock row `FOR UPDATE` and asserts the owner and an unexpired lease
in the transaction that carries the view statements. So a takeover waits for it, and a worker that
lost the lock aborts without writing. A database created before the fence has neither table, and
commits pass until `EnsureSchema` creates them.

### Compaction

A stream can opt into compaction with `evt.Compactor.CompactBelow(ctx, entityID, throughSequence)`.
It deletes the event rows in `[1, throughSequence]`, but only after confirming the durable snapshot
covers them (`snapshot.EventSequence >= throughSequence`). The snapshot lives in its own table and
is never touched, so it stays the authoritative floor. An uncovered request returns
`evt.ErrCompactionUncovered` and deletes nothing. Rebuild compacted streams through the
snapshot-aware path (`evt.SnapshotStreamer.StreamEntitiesFromSnapshots` or
`RebuildConfig.SeedAggregate`). [ADR 0001](../adr/0001-event-compaction-and-snapshot-truncation.md)
has the reasoning.

### Incremental rebuilds

The repository implements `evt.EntityHeadStreamer` and `evt.EntityHeadVisitor` over a streamed
`SELECT entity_id, MAX(sequence) …` cursor, so it reports each entity's head in constant memory for
change-detection rebuilds. The head folds in the snapshot's recorded sequence, so it stays correct
for streams whose early events were compacted away.

### Views

evt defines no PostgreSQL view schema. Your application owns its view tables and their DDL. But
[commit-wide projectors](projections.md#commit-wide-projectors) still commit their rows atomically
with the events, through a view writer that turns each view operation into the SQL statement for
your table:

```go
repo := postgres.NewRepository(pool).WithViewWriter(func(op evt.ViewOperation) (postgres.ViewStatement, error) {
    // The table's entity_id column holds the row's owner, the stream it belongs to.
    const insert = `INSERT INTO views (pk, sk, entity_id, entity_type, payload) VALUES ($1, $2, $3, $4, $5)`

    switch {
    case op.Kind == evt.ViewDelete && op.Condition == evt.ViewPresent:
        // Deletes the owner's row; affecting no row (absent, or another owner's) fails the condition.
        return postgres.ViewStatement{
            SQL: `DELETE FROM views WHERE pk = $1 AND sk = $2 AND entity_id = $3`,
            Args: []any{op.PK, op.SK, string(op.Owner)},
        }, nil
    case op.Kind == evt.ViewDelete:
        // Deletes the owner's row if there is one, and reports another owner's: any row it
        // affects is a failed condition.
        return postgres.ViewStatement{
            SQL: `WITH gone AS (DELETE FROM views WHERE pk = $1 AND sk = $2 AND entity_id = $3)
                  SELECT 1 FROM views WHERE pk = $1 AND sk = $2 AND entity_id <> $3`,
            Args: []any{op.PK, op.SK, string(op.Owner)},
        }, nil
    case op.Condition == evt.ViewAbsent:
        // A plain INSERT: a violation of the (pk, sk) key constraint is the failed condition.
        return postgres.ViewStatement{SQL: insert, Args: viewArgs(op), KeyConstraint: "views_pkey"}, nil
    default:
        // Overwrites the owner's row only; a row of another owner leaves the upsert affecting none.
        return postgres.ViewStatement{SQL: insert + ` ON CONFLICT (pk, sk) DO UPDATE SET
            entity_type = EXCLUDED.entity_type, payload = EXCLUDED.payload
            WHERE views.entity_id = EXCLUDED.entity_id`, Args: viewArgs(op)}, nil
    }
})
```

The statements run in the commit's transaction after the events and before the snapshots. So a
writer that lost a race fails on its stream's head, as a conflict, before it reaches a view row.

Your writer is what enforces the owner. DynamoDB and the in-memory repository condition every write
on the owner themselves, but evt can't see your PostgreSQL table. It reads whether an operation's
condition held from the statement's outcome, and every statement has to keep a row's owner from
ever changing. That gives four cases, which the example above covers:

- An `IfAbsent` put is a plain `INSERT` that raises a unique violation of the `(pk, sk)` key
  constraint when the row exists. Name the constraint in `ViewStatement.KeyConstraint`. A unique
  violation of any other constraint is an ordinary failure.
- Any other put is an upsert guarded by the owner (`... DO UPDATE ... WHERE table.entity_id =
  EXCLUDED.entity_id`). Affecting no row means another stream owns it, which fails the condition.
- An `IfPresent` delete deletes the owner's row. Affecting no row fails the condition.
- Any other delete succeeds if the row is absent and fails if another stream owns it. Its statement
  reports the foreign rows it found, and affecting a row fails the condition.

A failed condition is a `*evt.ProjectorOperationError` with reason `ConditionalCheckFailed`. Any
other error the database raises for a view statement is one too, with its SQLSTATE as the reason.
An integrity violation raised at `COMMIT`, which only a deferred constraint on your table can
cause, is a `*evt.ProjectorError`. A commit that carries view operations but has no writer fails
with `*evt.ViewsNotConfiguredError` before any write. Each statement counts as one action in the
commit budget, sized by its SQL text.

## Local development

The `infra/local-postgres` Terraform stack provisions the local database, and the repository owns
the tables inside it. Apply it before you run the integration tests:

```sh
docker run -d --name evt-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:17
terraform -chdir=infra/local-postgres init
terraform -chdir=infra/local-postgres apply -auto-approve
moon run evt:integration-postgres
```

The credentials are local-development placeholders only.
