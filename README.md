# evt

**Event sourcing for Go without a framework runtime.**

[Documentation](https://photon-grove.github.io/evt/) · [Concepts](docs/concepts.md) · [Getting Started](docs/getting-started.md) · [Invariants](BEHAVIORAL_INVARIANTS.md) · [Storage Format](docs/spec/storage-format.md) · [Feature Matrix](docs/parity/feature-matrix.md)

`evt` provides append-only logs, optimistic concurrency, snapshots, replayable
read models, and DynamoDB or PostgreSQL persistence as small Go packages. There
is no runtime to adopt or base application to inherit from. Use only the pieces
your application needs.

## Why evt

- **Events are the source of truth.** Domain events are immutable facts. Every
  view, projection, and read model is derived state you can delete and rebuild by
  replaying the log.
- **The same aggregate runs across backends.** Test command and event logic against
  an in-memory store, then use DynamoDB or PostgreSQL without changing the
  aggregate. A backend-neutral conformance suite checks every backend against the
  same storage contract.
- **Concurrency and ordering are protected.** Conditional writes preserve
  per-entity sequence ordering. Stable command IDs prevent retries from duplicating
  facts.
- **Long logs can be rebuilt efficiently.** Inline snapshots, versioned upcasters,
  and snapshot-verified compaction limit replay work without rewriting history. An
  optional entity-heads table tracks each entity's latest sequence so rebuilds can
  target changed entities.
- **DynamoDB Streams support.** A publisher fans committed events out over SNS to
  idempotent projectors with retry classification.
- **A Rust crate shares the log.** [`photon-evt`](crates/evt) reads and writes the
  same DynamoDB items, PostgreSQL rows, and stream envelopes, so Go and Rust
  services can append to the same streams.

## Install

```sh
go get github.com/photon-grove/evt
```

## Quick Start

An aggregate handles commands, emits events, and folds those events into state.
Here is the shape of one — see [`examples/banking`](examples/banking) for the full
runnable version:

```go
func (a *Account) Handle(_ context.Context, cmd evt.Command) (evt.CommandResult, error) {
    switch c := cmd.(type) {
    case *OpenAccount:
        if a.Opened {
            return evt.CommandResult{}, evt.NewConflictError("account already opened")
        }
        return evt.CommandResult{Events: []evt.Event{
            AccountOpened{AccountID: c.AccountID, Balance: c.InitialBalance},
        }}, nil
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

Drive it with an in-memory store in tests:

```go
store := mem.NewStore()

// Execute replays history onto the instance you pass, so hand it a fresh one per command.
acct := NewAccount("acct-1")
store.Execute(ctx, acct, "acct-1", &OpenAccount{AccountID: "acct-1", InitialBalance: 100}, evt.Metadata{})

acct = NewAccount("acct-1")
store.Execute(ctx, acct, "acct-1", &Deposit{AccountID: "acct-1", Amount: 25}, evt.Metadata{})

fmt.Println(acct.Balance) // 125
```

Use the same aggregate with DynamoDB in production:

```go
repo := dynamo.NewRepository(dynamoClient, "event-log")
store := snapshots.NewStore(repo, 25) // snapshot every 25 events
```

When aggregates carry injected dependencies, construct them and pass the instance to `Execute`:

```go
entity := NewAccount("acct-1", projectors...)
err := store.Execute(ctx, entity, "acct-1", command, metadata)
// entity now holds the post-command state
```

## Packages

| Package | Purpose |
| --- | --- |
| `evt` | Core aggregate, command, event, metadata, transaction, repository, and rebuild contracts |
| `evt/mem` | In-memory repository and store for unit tests |
| `evt/dynamo` | DynamoDB event log, snapshots, views, stream helpers, and transaction groups |
| `evt/postgres` | PostgreSQL event log and snapshots — the same `evt.Repository` contract on a relational store |
| `evt/snapshots` | Store with snapshot loading and write thresholds |
| `evt/stream` | Stream handler and publisher helpers for event fanout |
| `evt/projectors` | Projector runtime (SNS/SQS fan-out, or direct stream) with idempotency and retry classification |
| `evt/publishers` | DynamoDB Streams publisher handler that fans events out to SNS, with ingress and retry budgets (deprecated) |
| `evt/policy` | Backend-neutral retry helpers for commit paths |
| `evt/policy/dynamodb` | DynamoDB-specific retry classification |
| `evt/viewstore` | Typed JSON helper around `evt.ViewRepository` |
| `evt/test` | Test aggregate, commands, events, and helpers for adopters and framework tests |

All packages live under `github.com/photon-grove/evt`.

## Deprecations (v0.9.0)

The v0.9.0 core reshape deprecates the features cut by plan decision D6
([ADR 0006](docs/adr/0006-parity-policy-and-feature-cuts.md)); they keep compiling but are
deleted in the next minor release. Each carries a `Deprecated:` godoc notice pointing at its
replacement, so adopting the replacement today builds warning-free:

- `CommitStream` (K1) — commit batched commands through the normal commit path
- snapshot freshness observer / replay estimate / budget (K2) — removed without a Go
  replacement; wrap `Load` with your own metrics (Rust gains structured load telemetry, P9)
- `StreamEntitiesByQuery`, its workers/`HeadSource` options, and `RebuildProjectionsByQuery` (K3) —
  `StreamEntities` plus `evt.RebuildProjectionsFromStream`
- `HeadStore` (heads table) and incremental/delta rebuild (K4) — scan-derived `StreamEntityHeads`
- `evt/publishers`, `BudgetController`, and `stream.SNSPublisher`/FIFO (K5) — command outcomes and
  the stream envelope (which stays: it is the cross-language wire format)
- `policy.Config.Classify` / `policy.Classifier` (K6) — leave `Classify` unset; the built-in
  `policy.Classify` mapping applies
- the `WithFactory` helpers (including `snapshots.Store`'s), `ByCommandID`, `dynamo.Delete`,
  `dynamo/mock`, the `do.go` DI providers, `dynamo.ExpressionStreamer` (and the concrete
  `*dynamo.Repository` `...ByExpression` methods), and handler-attached
  `CommandResult.Transaction` (K7) — direct store calls, `CompactBelow`, `StreamFilter`,
  projector-produced transaction groups, and (planned) commit-wide projectors

Three more D6-cut surfaces have **no warning-free migration path yet**, so they carry no
`Deprecated:` notice in v0.9.0. They are planned for removal once the core reshape (C1–C11)
replaces the contracts that require them:

- `evt.BaseEntity` / `evt.NewEntity` — `Entity.Base` still requires `BaseEntity`
- `evt/result` — `Repository.StreamAllEvents`, `Repository.StreamEntities`,
  `SnapshotStreamer`, and `RebuildProjectionsFromStream` still return `result.Result` channels
- `policy.Hooks` / `policy.Attempt` — `policy.Execute` and `policy.ExecuteWithRetry` still take
  a `Hooks` argument

## Rust

The [`photon-evt`](crates/evt) crate is a native Rust implementation of the same
event log: aggregates, a snapshotting store, upcasters, compaction, in-band
projectors, and DynamoDB and PostgreSQL repositories. It is not a binding to the Go
code. The [storage format](docs/spec/storage-format.md) is the contract between the
two. Golden fixtures and a live run where Go and Rust take turns writing the same
streams enforce it (see [`compat/`](compat) and
[ADR 0003](docs/adr/0003-rust-implementation-and-cross-language-compatibility.md)).

## Core Discipline

**Event rows are the source of truth. View rows are derived state.** Any projection
or view table must be safe to wipe and rebuild from the immutable log. Record human
decisions, external signals, publish flags, and review verdicts as events before a
projector writes them to a view.

Event logs are append-only and retained in full by default. Two opt-in mechanisms
trim them safely:

- **Compaction** (`evt.Compactor.CompactBelow`) deletes only events a durable
  snapshot already covers, then rebuilds seed from that snapshot rather than
  replaying from event 1. See
  [ADR 0001](docs/adr/0001-event-compaction-and-snapshot-truncation.md).
- **Per-type retention** (`dynamo.Repository.WithRetention`) sets DynamoDB `ttl`
  to `committedAt + duration` for terminal, short-lived, transient streams. TTL
  expires rows individually, so use it only when a stream's lifetime is much
  shorter than the duration and no events are appended after it becomes terminal.
  Otherwise an older prefix can expire while newer events remain, and loading the
  stream then fails with a conflict. For accumulating streams, keep a snapshot and
  compact instead.

The raw `dynamo.Delete` is snapshot-unsafe, for local fixtures only, and excluded
from production builds (`-tags prod`). See
[`BEHAVIORAL_INVARIANTS.md`](BEHAVIORAL_INVARIANTS.md) for the exact serialization
and DynamoDB schema guarantees.

## Local Development

```sh
pnpm install
go test ./...
moon run evt:test
```

The Rust toolchain is pinned in `rust-toolchain.toml`, and `rustup` installs it on first use:

```sh
cargo test --workspace
moon run evt-rust:check evt-rust:test
```

DynamoDB integration tests run against a local AWS emulator (Moto), not real AWS:

```sh
docker run -d --name evt-moto -p 4566:5000 motoserver/moto:5.1.22
terraform -chdir=infra/local init
terraform -chdir=infra/local apply -auto-approve
AWS_ENDPOINT_URL=http://localhost:4566 moon run evt:integration
```

PostgreSQL integration tests run against a local PostgreSQL server. Terraform
provisions the database; `postgres.Repository.EnsureSchema` owns the tables:

```sh
docker run -d --name evt-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:17
terraform -chdir=infra/local-postgres init
terraform -chdir=infra/local-postgres apply -auto-approve
moon run evt:integration-postgres
```

With both stacks running, `moon run evt-rust:integration evt-rust:integration-postgres`
runs the Rust conformance suite and the Go/Rust cross-language suite against each
backend.

## Documentation

The [docs site](https://photon-grove.github.io/evt/) covers architecture, the
integration cookbook, and runnable examples. Static docs live in [`docs/`](docs/);
the interactive site lives in [`website/`](website/).

Architecture decision records live in [`docs/adr/`](docs/adr/): event-log compaction
([ADR 0001](docs/adr/0001-event-compaction-and-snapshot-truncation.md)), backend-neutral storage
contracts ([ADR 0002](docs/adr/0002-backend-neutral-storage-contracts.md)), the Rust implementation
and cross-language compatibility ([ADR 0003](docs/adr/0003-rust-implementation-and-cross-language-compatibility.md)),
the vNext core reshape — multi-stream transactions, command outcomes, and the rebuild fence
([ADR 0004](docs/adr/0004-core-reshape-multi-stream-transactions-and-outcomes.md)), Rust-first
extensions ([ADR 0005](docs/adr/0005-rust-first-extensions-journal-and-lambda.md)), and the parity
policy ([ADR 0006](docs/adr/0006-parity-policy-and-feature-cuts.md)). The
[feature parity matrix](docs/parity/feature-matrix.md) tracks every capability across languages.

```sh
moon run website:dev
moon run website:build
```

## License

Apache-2.0. See [`LICENSE`](LICENSE).

---

Built by [Photon Grove](https://photon-grove.com), a Colorado software studio.
