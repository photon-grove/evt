# Examples

Each example is a small program that shows one part of evt, runs against the in-memory backend, and
has tests. CI builds and tests all of them, so they track the current API.
Run the commands from the repository root.

## Go

The Go examples live in `go/examples/`, one package per example.

- **Banking** ([source](../../go/examples/banking/main.go)). An account aggregate on the in-memory
  store: commands, events, `Execute`, conflict and not-found errors, and reloading an aggregate
  from its stream.

  ```sh
  go -C go run ./examples/banking
  ```

- **Todo** ([source](../../go/examples/todo/main.go)). A commit-wide projector writes each todo
  list's open and done counts as a view row in the same commit as the events. The example then
  replays the commits from the log with `evt.GroupCommits` and `evt.ReplayViews` and checks that
  the replayed rows equal the live ones. That replay is an in-memory check that needs an
  uncompacted log, not a durable rebuild. See [Projections and rebuilds](projections.md).

  ```sh
  go -C go run ./examples/todo
  ```

- **Transfer** ([source](../../go/examples/transfer/main.go)). A transfer debits one account and
  credits another in one commit across both streams, through `ExecuteTransaction` on a store with
  command outcomes enabled. A retry with the same command ID replays the stored outcome, the same
  ID with a different request is refused as idempotency-key reuse, and an overdraft is a stored
  rejection. See [Command outcomes and retries](commands.md).

  ```sh
  go -C go run ./examples/transfer
  ```

Run the tests of all three with `go -C go test ./examples/...`.

## Rust

The Rust examples are Cargo examples of the crate they use.

- **Banking** ([source](../../crates/evt/examples/banking.rs)). The Rust port of the Go banking
  example, extended with a two-account transfer as a multi-stream transaction, a read-fenced
  `bank` stream, a repeated command ID, and given/when/then tests from `photon_evt::testing`.

  ```sh
  cargo run -p photon-evt --example banking --features testing
  ```

- **Journal** ([source](../../crates/evt/examples/journal.rs)). The sync journal: map events to
  scopes, journal a store's commits, and poll a scope page by page with gap detection.

  ```sh
  cargo run -p photon-evt --example journal --features journal,testing
  ```

- **Todo** ([source](../../crates/evt/examples/todo.rs)). The Rust port of the Go todo example: a
  `CommitProjector` writes each list's summary row in the commit, and `group_commits` and
  `replay_views` replay the rows from the log, with the same caveat.

  ```sh
  cargo run -p photon-evt --example todo
  ```

- **Stream handler** ([source](../../crates/evt-lambda/examples/stream_handler.rs)). A DynamoDB
  Streams batch for an event-log table, as Lambda delivers it, run through `process_events` without
  a Lambda runtime. An idempotent projector fails on one record, the partial batch failure response
  names it, and the redelivered records and a duplicate delivery are applied once.

  ```sh
  cargo run -p photon-evt-lambda --example stream_handler
  ```

Run their tests with `cargo test -p photon-evt --all-features --examples` and
`cargo test -p photon-evt-lambda --examples`.
