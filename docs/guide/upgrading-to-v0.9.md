# Upgrading to v0.9

v0.9 cuts a set of features, breaks a few signatures without a deprecation window, and adds
multi-stream commits on the existing `Repository.Commit`. This page is the checklist for moving an
application, or a custom backend, across. The reasoning behind the cuts is in
[ADR 0006](../adr/0006-parity-policy-and-feature-cuts.md), and the
[feature matrix](../reference/feature-matrix.md) records each one.

## Renamed

The Go module moved into the repo's `go/` directory, so its path is now
`github.com/photon-grove/evt/go` and its releases are tagged `go/vX.Y.Z`. The package is still
named `evt`. The backends moved under `backends/`, so `evt/dynamo` is now
`github.com/photon-grove/evt/go/backends/dynamo`, and the same goes for `postgres` and `mem`. A few
identifiers were renamed in the same release to match the Rust crate, so one find-and-replace pass
covers the whole upgrade:

| Old | New |
| --- | --- |
| `evt.Entity` | `evt.Aggregate` (`EntityID` and `EntityType` keep their names) |
| `evt.Context` | `evt.CommandContext`, which stops colliding with `context.Context` |
| package `viewstore` | package `viewcodec`, matching Rust's `ViewCodec` |
| `dynamo.Transaction` | `evt.Transaction`. The alias is gone |
| `dynamo.StorageType == "DynamoDB"` | `dynamo.StorageType == "dynamo"` |

None of these touch stored data. Item shapes, keys and marker names are unchanged, and the
`StorageType` string is only compared in memory.

## Removed

- `Repository.CommitStream` is gone. Commit batched commands through the normal commit path.
- The snapshot freshness observer, replay estimate and replay budget are gone. Wrap `Load` with your
  own metrics.
- `dynamo.StreamEntitiesByQuery`, its `HeadSource` option and `RebuildProjectionsByQuery` are gone.
  `StreamEntitiesFromSnapshots` stays. `StreamEntities` buffers every matched event, so for very
  large tables page your own DynamoDB Query or Scan with `LastEvaluatedKey`.
- `dynamo.HeadStore` and the entity-heads table are gone. `Repository.StreamEntityHeads` derives
  heads from the log and stays.
- The `evt/publishers` package and `stream.SNSPublisher` are gone. The stream envelope stays,
  because it's the cross-language wire format. Read the DynamoDB Stream with `stream.DynamoDBHandler`
  or `projectors.NewLambdaHandler`, and publish onward from your own handler.
- `policy.ExecuteWithRetry`, its hooks and the pluggable classifier, plus `evt/policy/dynamodb`, are
  gone. [Fence-aware retry](commands.md#fence-aware-retry) replaces blind whole-command retry.
- `evt.BaseEntity`, `evt.NewEntity`, `Entity.Base`, the `WithFactory` helpers, `evt.ByCommandID`,
  `CommandResult.Transaction`, the `do.go` providers, `dynamo.Repository.Delete`,
  `dynamo.ExpressionStreamer` and the public `...ByExpression` methods are gone, and `dynamo/mock`
  is no longer importable. Aggregates define their own `ID` field and `GetID()`. Use `StreamFilter`
  for entity-type filtering and `snapshots.Store.WithCommitProjectors` for commit-wide projectors.
  The `result` package stays, because the repository stream channels still carry `result.Result`.

## Source-breaking changes

Three changes break source without a deprecation window. The second one also touches
`evt.CommandResult`.

`evt.Metadata` can no longer be compared with `==`. The new `Extensions` map makes the struct
non-comparable. Use `evt.Metadata.Equal` instead, which treats nil and empty maps as equal.

`evt.SerializedResult` gained `Snapshots`, `Fences` and `Outcome`, growing from two exported fields
to five. So an unkeyed struct literal such as `evt.SerializedResult{events, transaction}` no longer
compiles. Use keyed fields. The zero value of the new ones keeps the single-stream shape.
`evt.CommandResult` also gained a `Response` field, so use keyed literals there too.

```go
result := evt.SerializedResult{
    Events:      events,
    Transaction: transaction,
}
```

`evt.Repository` now requires `GetRecentEvents(ctx context.Context, entityID evt.EntityID, limit
int) ([]evt.SerializedEvent, error)`. Add it to custom backends and mocks. For `limit < 1`, return a
typed `*evt.InvalidArgumentError` before any I/O (match it with `errors.Is(err,
evt.ErrInvalidArgument)`). Otherwise return at most `limit` events, newest first by sequence
regardless of insertion order, with snapshot rows excluded. Bound the storage query itself. Don't
call `GetEvents` and slice a full replay.

For example, a SQL backend with a separate events table can implement it like this. `queryEvents` is
the backend's existing query-and-decode helper. It runs the SQL it's given and decodes event rows,
so this isn't a full-stream replay:

```go
func (r *Repository) GetRecentEvents(
    ctx context.Context, entityID evt.EntityID, limit int,
) ([]evt.SerializedEvent, error) {
    if limit < 1 {
        return nil, &evt.InvalidArgumentError{Argument: "limit", Message: "must be at least 1"}
    }

    return r.queryEvents(ctx,
        `SELECT * FROM events WHERE entity_id = $1 AND sequence > 0
         ORDER BY sequence DESC LIMIT $2`, string(entityID), int64(limit))
}
```

Use your backend's event-column list and decoder in place of `SELECT *`. Snapshots have to stay out
of this result. Mocks implement the same signature and the same bounded-read contract.

## Multi-stream commits

Multi-stream transactions and read fences arrive without a new interface method. `Repository.Commit`
already accepts a `SerializedResult`, so one commit can append to several streams and snapshots at
once, and the engine pins their order bytewise by stream ID.

A `SerializedResult.Fences` entry pins a read-only stream to the head it was observed at. The commit
fails with `*evt.FenceConflictError` when another writer, or a compaction, moves it. Fences for
entity types with retention are refused. That set is the union of the repository's own
`WithRetention` configuration and the feature marker's durable retention set, so declare retained
types in the marker and every process refuses them.

Every commit, single-stream or multi-stream, is checked against the shared budget of 100 items,
3.8 MB and 380 KB, and fails with `*evt.CommitTooLargeError` before any write, on every backend. The
budget counts the actions a commit actually emits. Every written stream is protected by one
conditional update of its head item ([ADR 0007](../adr/0007-per-stream-head-item.md)). The
feature-marker check and the rebuild-lock check are in every commit. So a single-stream commit
leaves room for 97 events and projector operations, whether it creates its stream or appends to it,
and 96 when it also writes a snapshot. A fence is one check of the same head item.

A commit whose first sequence skips past the stream's stored head is refused with
`*evt.SequenceGapError`. A stream whose head item disagrees with its log fails with
`*evt.StreamHeadDriftError`, which is never retried. A store must declare the `stream-heads` feature
before anyone writes to it. An empty store is declared by its first writer. A store written before
head items existed has to be migrated first with `evt-doctor migrate-heads`, and until then writers
get `*evt.StreamHeadsNotMigratedError`. That migration is a stop-the-world, one-way procedure,
documented in the [migration runbook](migrating-to-stream-heads.md). The exact contract is in the
[storage format](../reference/storage-format.md#writes).

## Custom repositories

A custom `Repository` written before v0.9 compiles unchanged, but it now receives
`SerializedResult.Snapshots` and `.Fences`. An implementation that can't honor them atomically has to
return an error rather than ignore them. Ignoring a fence commits a decision made on a stale read
with no isolation guarantee, and ignoring a snapshot loses one the engine believes is durable. The
contract is documented on `evt.Repository.Commit`, and `evt.NormalizeCommit` and
`evt.PreflightCommit` give a backend the shared validation and budget checks.

Commit-wide view operations arrive as `SerializedResult.Views`. A repository that can't write them in
the commit's transaction has to return an error rather than ignore them.

Atomic multi-stream support is an explicit capability. A repository declares it by implementing
`evt.TransactionalRepository` (a marker method, `AtomicTransactions()`), and should only do so once
its `Commit` honors the full contract above. `snapshots.Store.Transaction` fails closed. `Read` and
`Commit` refuse a repository without the capability with `*evt.TransactionsUnsupportedError`
(`errors.Is(err, evt.ErrTransactionsUnsupported)`) before anything is loaded or written. The
in-memory, DynamoDB and PostgreSQL repositories declare it.

The Go PostgreSQL repository runs commit-wide view operations through a view writer
(`WithViewWriter`, see [PostgreSQL](postgres.md#views)). It refuses the operations of a transaction
group built for another storage type with `*evt.ProjectorOperationsUnsupportedError` before writing,
instead of dropping the view writes.

To use transactions on a custom backend, make `Commit` atomic over every stream, snapshot and fence,
then add the marker method. Wrapping a repository in a decorator type hides the capability unless the
wrapper declares it too.
