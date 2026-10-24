# Integration cookbook

These are the integration rules that come up most, with the constraint behind each one. Most of
them follow from [keeping the log](concepts.md#keeping-the-log) the only source of truth.

## Separate fact and view tables

Store event rows and view rows in different tables. The split keeps scans, rebuilds, TTLs and IAM
permissions easy to reason about. The event log is append-only and tightly guarded, while views are
disposable and a projector can get broader write access to them. See the
[table shapes](dynamodb.md).

## Rebuild before you patch

If a view is wrong, fix the projector and [rebuild](projections.md). Don't edit view rows by hand. A
later rebuild removes the edit, and the edit can hide a missing event or a projector bug in the
meantime.

## Command IDs as retry keys

Set `Metadata.CommandID` from an idempotency key, request ID, job ID or message ID, building it with
`evt.WithCommandID(...)`. A duplicate attempt then fails as an `evt.DuplicateCommandError` (test for
it with `evt.IsDuplicateCommandErr`) and nothing is appended. That covers duplicate
submissions and the duplicates at-least-once delivery produces.

Some retries need to return the original answer, and need to keep working after the first attempt's
events were folded into a snapshot. For those, enable [command outcomes](commands.md) with
`store.WithOutcomes()` and call `ExecuteWithOutcome`. A retry with the same ID and the same command
returns the stored outcome (`Replayed`). A different command under the same ID is
`evt.ErrIdempotencyKeyReuse`. And a command whose result was lost to a timeout is resolved by reading
the outcome row again, not by running the command again. Use one stable key per logical request,
never one per attempt.

A command that loses a race with another writer is re-prepared for you, meaning the engine reloads
the aggregate and runs the handler again, by
[fence-aware retry](commands.md#fence-aware-retry). The handler runs again against fresh state, up to
the `store.WithRetry(...)` attempts, and when those run out the command is recorded as a
`stale-fence` rejection that a replay of the same ID returns. So write handlers that are safe to run
more than once, and use a new key to try again after a stale-fence rejection.

Give a multi-stream command an identity that names only what the request names, in the
`evt.CommandIdentity` `Targets`. Everything that distinguishes the request has to be in `Targets` or
the payload. [Identity](commands.md#identity) explains why, with the transfer example that gets it
wrong.

## Failures beside rejections

Some refusals have to leave a trace in the domain. When applying a proposal finds its base revision
stale, the client should get a rejection, and the proposal should also record an `apply-failed`
event, so it's no longer "applicable" and the history shows the attempt. Writing the event in a
second step isn't atomic, and accepting the command gives the client the wrong answer.

So return the event with the rejection. In Go, mark the handler's error with `evt.RejectWithEvents`
(in a `Prepare` function, stage the event with `tx.Append` first). In Rust, return it from
`Aggregate::rejection_events` or finish with `Decision::reject_with_events`. The event and the
rejected [outcome](commands.md) commit together or not at all. A replay of the command ID returns the
rejection without writing the event again, and a command that loses a race is re-prepared and may
decide differently. Choose those events the way you would for an accepted command: record facts,
never view state.

## Upcasting old shapes

Never assume a stored payload has the latest struct shape. When an event's JSON changes, bump its
`Version()`, add an [`EventUpcaster`](concepts.md#upcaster), and write a fixture test for every
historical version. An append-only log has no migration window, so old rows have to keep loading.

## Decisions are events

Human decisions, external signals, publish flags and accept or reject verdicts are state. Write them
straight to a view table and a rebuild erases them. Record them as events first, then let a
projector derive the view. Here's the test: could you drop every view table and replay the log with
nothing lost?

## Snapshots and compaction

Wrap the repository in [`snapshots.NewStore(repo, size)`](dynamodb.md#snapshots) so hot entities
don't replay from sequence 1. Only after a durable snapshot covers a range should you call
[`CompactBelow`](dynamodb.md#compaction) to truncate it. Then rebuild that stream through
`RebuildConfig.SeedAggregate`, never with a full replay. For transient streams that are never
replayed, a [retention TTL](dynamodb.md#retention) is a better fit than compaction.

## Testing against Moto

Use `mem` for unit tests. It's fast, offline and implements the same contract. But run integration
tests against a DynamoDB-compatible emulator with the exact key and index shapes you use in
production, because conditional writes, the `entityType-index` GSI (global secondary index) and snapshot rows are where
backend-specific bugs live. The [`infra/local`](dynamodb.md#local-tables) Terraform stack provisions
matching tables.

## Stream handlers are batch processors

[Projectors](streams.md) receive batches. Process each record on its own, report partial-batch
failures precisely so retries cover only the records that failed, and keep every handler idempotent,
because delivery is at least once.
