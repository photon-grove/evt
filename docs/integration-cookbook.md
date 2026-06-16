# Integration Cookbook

Patterns adopters reach for repeatedly, distilled. Each one is a rule plus the
reason it exists — copy the rule, keep the reason in mind. They all serve the same
invariant: **events are the source of truth; everything derived must survive a
wipe-and-replay.**

## Keep fact tables and projection tables separate

Store event rows and view rows in different tables. Separation makes table scans,
rebuilds, TTLs, and IAM permissions easy to reason about: the event log is
append-only and tightly guarded, while views are disposable and can be granted
broader write access to a projector. See the [table shapes](dynamodb.md).

## Rebuild before you patch

If a view is wrong, fix the projector and [rebuild](projections.md) — don't hand-
edit view rows. A manual edit disappears on the next rebuild and quietly hides the
fact that an event was missing or a projector was buggy. The rebuild path is the
real fix; the hand edit is a time bomb.

## Treat command IDs as retry keys

Set `Metadata.CommandID` from an idempotency key, request ID, job ID, or message
ID, and build it with `evt.WithCommandID(...)`. A duplicate attempt then fails as
an `evt.DuplicateCommandError` (test with `evt.IsDuplicateCommandErr`) instead of
appending the same fact twice. This is the cheapest insurance against double
submits and at-least-once retries.

## Upcast historical shapes, always

Never assume a stored payload has the latest struct shape. The moment an event's
JSON changes, bump its `Version()`, add an [`EventUpcaster`](concepts.md#upcaster),
and write a fixture test for every historical version. Old rows must keep loading
forever — there is no migration window for an append-only log.

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

[Projectors and publishers](streams.md) receive batches. Process each record
independently, report **partial-batch failures** precisely so retries are scoped
to the records that actually failed, and keep every handler idempotent — the same
event will eventually arrive twice.
