# evt

[Documentation site](https://photon-grove.github.io/evt/)

`evt` is a Go event-sourcing framework for systems that want immutable domain
events, DynamoDB-backed persistence, deterministic read models, and practical
operational tooling without a large application framework.

It was extracted from production patterns, but this repository is intentionally
public-safe: examples use neutral table names, local emulator credentials, and no
private application environment details.

## Packages

| Package | Purpose |
| --- | --- |
| `github.com/photon-grove/evt` | Core aggregate, command, event, metadata, transaction, repository, and rebuild contracts |
| `github.com/photon-grove/evt/mem` | In-memory repository and store for unit tests |
| `github.com/photon-grove/evt/dynamo` | DynamoDB event log, snapshots, views, stream helpers, and transaction groups |
| `github.com/photon-grove/evt/snapshots` | Store implementation with snapshot loading and snapshot write thresholds |
| `github.com/photon-grove/evt/stream` | Stream handler and publisher helpers for event fanout |
| `github.com/photon-grove/evt/projectors` | DynamoDB Streams Lambda projector runtime with idempotency and retry classification |
| `github.com/photon-grove/evt/publishers` | DynamoDB Streams publisher handler with ingress and retry budgets |
| `github.com/photon-grove/evt/policy` | Backend-neutral retry helpers for commit paths |
| `github.com/photon-grove/evt/policy/dynamodb` | DynamoDB-specific retry classification |
| `github.com/photon-grove/evt/viewstore` | Typed JSON helper around `evt.ViewRepository` |
| `github.com/photon-grove/evt/test` | Test aggregate, commands, events, and helpers for adopters and framework tests |

## Install

```sh
go get github.com/photon-grove/evt
```

## Quick Start

Start with the memory store in tests:

```go
repo := mem.NewRepository()
store := mem.NewStoreFromRepo(repo)

entity := account.NewEntity("acct-1")
err := store.Execute(ctx, entity, "acct-1", &account.Open{InitialBalance: 100}, evt.Metadata{})
```

Move production storage to DynamoDB without changing aggregate contracts:

```go
repo := dynamo.NewRepository(dynamoClient, "event-log")
store := snapshots.NewStore(repo, 25)
```

Use factory helpers when aggregates carry injected dependencies:

```go
entity, err := evt.ExecuteWithFactory(ctx, store, func() evt.Entity {
	return account.NewEntity("acct-1", projectors...)
}, "acct-1", command, metadata)
```

## Local Development

```sh
pnpm install
go test ./...
moon run evt:test
```

DynamoDB integration tests run against a local AWS emulator:

```sh
docker run -d --name evt-moto -p 4566:5000 motoserver/moto:5.1.22
terraform -chdir=infra/local init
terraform -chdir=infra/local apply -auto-approve
AWS_ENDPOINT_URL=http://localhost:4566 moon run evt:integration
```

## Documentation

The docs site includes architecture diagrams, examples, and an integration
cookbook:

```sh
moon run website:dev
moon run website:build
```

Static docs live in [`docs/`](docs/), and the interactive site lives in
[`website/`](website/).

## Operational Invariants

The central invariant is that event rows are the source of truth and view rows
are derived state. Any projection/view table should be safe to wipe and rebuild
from immutable events. Human decisions, external signals, publish flags, and
review verdicts should be event-sourced before they appear in a view.

Event logs are append-only by default and retained in full. Streams may opt into
**compaction** (`evt.Compactor.CompactBelow`) to truncate events that a durable
snapshot already captures. Once a stream is compacted, rebuilds must seed from
its snapshot (`RebuildConfig.SeedEntity` / `evt.SnapshotStreamer`) rather than
replay from event 1. The raw `dynamo.Delete` is for local/staging fixtures only
and is excluded from production builds (`-tags prod`); use `CompactBelow` for
principled truncation. See
[ADR 0001](docs/adr/0001-event-compaction-and-snapshot-truncation.md).

For **terminal, short-lived, fully transient** entity types — process/scaffolding
streams that drive a one-time side effect and that no projection rebuild ever
replays — a per-entity-type retention policy
(`dynamo.Repository.WithRetention(dynamo.Retention{...})`) stamps a DynamoDB `ttl`
attribute on those rows so the table auto-expires them after the configured
duration. This is selective by design: only policy'd entity types ever carry a
`ttl`; durable types are written without one and are never expired. Because
DynamoDB TTL expires items individually (it cannot atomically drop a whole stream),
each event is stamped `committedAt + duration` — so retention is safe **only** when
a stream's entire lifetime is much shorter than the duration and it is not appended
to after going terminal; otherwise an older prefix can expire while newer events
survive and a load replays a partial suffix. Use it only when a wipe-and-replay does
not depend on the events; for streams that accumulate events over time, keep a
snapshot and use compaction instead. See the `dynamo.Retention` doc comment.

See [`BEHAVIORAL_INVARIANTS.md`](BEHAVIORAL_INVARIANTS.md) for the exact
serialization and DynamoDB schema guarantees.

## License

Apache-2.0. See [`LICENSE`](LICENSE).

---

Built with care by [Photon Grove](https://photon-grove.com) — a Colorado
software studio.
