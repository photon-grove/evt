# evt for Go

This is the Go module for evt, an event-sourcing library where an append-only log of events is the
record of truth and every view is derived from it. The core package is a handful of interfaces
(`Aggregate`, `Command`, `Event`, `Repository`) plus the serialization and commit rules that sit
between them. Everything else is optional: pick a backend, add snapshots, add projectors when you
need read models.

```sh
go get github.com/photon-grove/evt/go
```

The import path ends in `go`, but the package is still named `evt`, so code reads `evt.Aggregate`
and `evt.Metadata`. Writing the import as `evt "github.com/photon-grove/evt/go"` makes that
explicit, and editors resolve it either way. The module needs Go 1.26.

## A quick start

An aggregate handles commands, emits events, and folds those events into its state. Here's the
shape of one, from [`examples/banking`](examples/banking), which is the full runnable version:

```go
func (a *Account) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
	switch cmd := command.(type) {
	case *OpenAccount:
		if a.Opened {
			return evt.CommandResult{}, evt.NewConflictError("account already opened")
		}
		return evt.CommandResult{Events: []evt.Event{AccountOpened{
			AccountID: cmd.AccountID,
			Balance:   cmd.InitialBalance,
			At:        time.Now().UTC(),
		}}}, nil
	// ...
	}
}

func (a *Account) Apply(event evt.Event) error {
	switch e := event.(type) {
	case AccountOpened:
		a.ID, a.Balance, a.Opened = e.AccountID, e.Balance, true
	// ...
	}
	return nil
}
```

In tests, drive it with the in-memory store. `Execute` replays the stream onto the instance you
pass and leaves it holding the post-command state, so hand it a fresh instance per command:

```go
store := mem.NewStore()

account := NewAccount("acct-1")
err := store.Execute(ctx, account, "acct-1", &OpenAccount{AccountID: "acct-1", InitialBalance: 100}, evt.Metadata{})
```

In production the same aggregate runs against DynamoDB or PostgreSQL behind a snapshotting store.
This one writes a snapshot every 25 events, so a load reads the latest snapshot and only the events
after it:

```go
repo := dynamo.NewRepository(dynamoClient, "event-log")
store := snapshots.NewStore(repo, 25)
```

A snapshot is a single item, so keep an aggregate's state bounded and its unbounded history in
events. The store skips (and logs) a snapshot over the commit's per-item budget,
`evt.MaxCommitItemBytes`, instead of failing the commit, and loads then replay from the last
snapshot that fit ([Oversized snapshots](../docs/reference/storage-format.md#oversized-snapshots)).

[Getting started](../docs/guide/getting-started.md#the-same-model-in-go) walks through all of
this end to end, and [Concepts](../docs/guide/concepts.md) explains aggregates, the
`CommandContext` a load returns, metadata, snapshots and projectors.

Two more runnable examples sit beside banking. [`examples/todo`](examples/todo) writes a view row
in the same commit as the events with a commit-wide projector, then rebuilds the rows from the log
alone. [`examples/transfer`](examples/transfer) moves money between two accounts in one commit
across both streams, with durable command outcomes, so a retried command ID replays its stored
answer. Run any of them with `go run ./examples/<name>` from this directory, and see
[Examples](../docs/guide/examples.md) for the full list in both languages.

## Packages

The core is the module root, `github.com/photon-grove/evt/go`. The three backends sit under
`backends/` because they all implement the same `evt.Repository` contract, and the conformance
suite checks each of them against it.

| Package | What it's for |
| --- | --- |
| `evt/go` | Aggregate, command, event, metadata, transaction, repository and rebuild contracts |
| `evt/go/backends/mem` | In-memory repository and store for unit tests |
| `evt/go/backends/dynamo` | DynamoDB event log, snapshots, views, stream helpers and transaction groups |
| `evt/go/backends/postgres` | PostgreSQL event log, snapshots and views on the same contract |
| `evt/go/snapshots` | The store most code uses: snapshot loading, write thresholds, outcomes and retry |
| `evt/go/projectors` | Runtime for read models fed from the DynamoDB Stream, SNS or SQS, with idempotency and retry classification |
| `evt/go/stream` | The stream envelope and the DynamoDB Streams handler |
| `evt/go/viewcodec` | Typed JSON helper around `evt.ViewRepository` |
| `evt/go/policy` | Failure classification and retry helpers for handlers |
| `evt/go/result` | The `result.Result` value the repository's stream channels carry |
| `evt/go/conformance` | The backend-neutral suite a custom `evt.Repository` should pass |
| `evt/go/test` | A test aggregate, commands, events and helpers |
| `evt/go/cmd/evt-doctor` | CLI that inspects an event log and runs the stream-heads migration |

The backend pages cover wiring and the stored layout: [DynamoDB](../docs/guide/dynamodb.md) and
[PostgreSQL](../docs/guide/postgres.md). [Projections and rebuilds](../docs/guide/projections.md)
and [Streams and projectors](../docs/guide/streams.md) cover read models, and
[Command outcomes and retries](../docs/guide/commands.md) covers idempotent commands. The
[storage format](../docs/reference/storage-format.md) is the exact contract, shared with the Rust
crate.

## Versions

Releases are tagged `go/vX.Y.Z`, because Go's module proxy requires the directory prefix for a
module that lives in a subdirectory. The tags from before the move (`v0.1.0` through `v0.8.0`)
belong to the old module path, `github.com/photon-grove/evt`. Moving to this module is a
find-and-replace on the import path plus the v0.9 renames, which
[Upgrading to v0.9](../docs/guide/upgrading-to-v0.9.md) lists. The Rust crate,
[`photon-evt`](../crates/evt/README.md), is versioned separately on crates.io and reads and writes
the same log. The changelog is [`CHANGELOG.md`](CHANGELOG.md).
