# Integration Cookbook

Common integration rules and the constraints behind them. They all follow the
same invariant: **events are the source of truth, and derived state must survive a
wipe-and-replay.**

## Keep fact tables and projection tables separate

Store event rows and view rows in different tables. Separation makes table scans,
rebuilds, TTLs, and IAM permissions easy to reason about: the event log is
append-only and tightly guarded, while views are disposable and can be granted
broader write access to a projector. See the [table shapes](dynamodb.md).

## Rebuild before you patch

If a view is wrong, fix the projector and [rebuild](projections.md). Do not edit
view rows by hand. A later rebuild removes the edit and can hide a missing event or
projector defect.

## Treat command IDs as retry keys

Set `Metadata.CommandID` from an idempotency key, request ID, job ID, or message
ID, and build it with `evt.WithCommandID(...)`. A duplicate attempt then fails as
an `evt.DuplicateCommandError` (test with `evt.IsDuplicateCommandErr`) instead of
appending the same fact twice. This prevents duplicate submissions and
at-least-once retry duplicates.

For retries that must return the original answer, and that must keep working after the first
attempt's events were folded into a snapshot, enable
[command outcomes](../README.md#command-outcomes-and-rejections-v09-c3c4) with
`store.WithOutcomes()` and call `ExecuteWithOutcome`: a retry with the same ID and the same
command returns the stored outcome (`Replayed`), a different command under the same ID is
`evt.ErrIdempotencyKeyReuse`, and a command whose result was lost to a timeout is resolved by
re-reading the outcome row rather than re-running it. Use a stable idempotency key per logical
request, never one per attempt.

A command that loses a race with another writer is re-prepared for you
([fence-aware retry](../README.md#command-outcomes-and-rejections-v09-c3c4)): the handler runs again
against fresh state, up to `store.WithRetry(...)` attempts, and when they run out the command is
recorded as a `stale-fence` rejection that a replay of the same ID returns. Write handlers that are
safe to run more than once, and use a new key to try again after a stale-fence rejection. Give a
multi-stream command an identity that names only what the request names (`evt.CommandIdentity`
`Targets`): the streams it reads and writes may come from state and change between attempts, and
they are not part of the identity.

## Upcast historical shapes, always

Never assume a stored payload has the latest struct shape. When an event's JSON
changes, bump its `Version()`, add an [`EventUpcaster`](concepts.md#upcaster), and
write a fixture test for every historical version. Append-only logs have no
migration window, so old rows must keep loading.

## Don't persist decisions straight to a view

Human decisions, external signals, publish flags, and accept/reject verdicts are
state. If you write them directly to a view table, a rebuild erases them. Record
them as **events first**, then let a projector derive the view. The test: could
you drop every view table and replay the log with nothing lost? If not, something
that should be an event is hiding in a view.

## Snapshot long streams; compact only when covered

Wrap the repository in [`snapshots.NewStore(repo, size)`](dynamodb.md#snapshots)
so hot entities don't replay from sequence 1. Only after a durable snapshot covers
a range should you [`CompactBelow`](dynamodb.md#compaction) to truncate it — and
then rebuild that stream through `RebuildConfig.SeedEntity`, never a full replay.
For genuinely transient, never-replayed streams, prefer a
[retention TTL](dynamodb.md#retention) over compaction.

## Test against the real DynamoDB shape

Use `mem` for unit tests — fast, offline, and contract-identical. But run
integration tests against a DynamoDB-compatible emulator with the **exact key and
index shapes** you use in production, because conditional writes, the
`entityType-index` GSI, and snapshot rows are where backend-specific bugs live.
The [`infra/local`](dynamodb.md#local-tables) Terraform stack provisions matching
tables.

## Treat stream handlers as batch processors

[Projectors](streams.md) receive batches. Process each record
independently, report **partial-batch failures** precisely so retries are scoped
to the records that actually failed, and keep every handler idempotent — the same
event will eventually arrive twice.
