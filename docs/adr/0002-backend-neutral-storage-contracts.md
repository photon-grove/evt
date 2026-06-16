# ADR 0002: Backend-neutral storage contracts

## Status

Accepted (2026-06-16)

## Context

- `evt` ships an in-memory backend (`mem`) and a DynamoDB backend (`dynamo`). A future PostgreSQL
  backend is on the roadmap. To add a backend without reworking the framework, the core
  `evt.Repository` contract must be expressible by any store, not just DynamoDB.
- The contract leaked a DynamoDB type. `Repository.StreamAllEvents` and `Repository.StreamEntities`
  accepted `*expression.Expression` from `github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression`.
  This forced every backend — and every caller of the interface — to import the AWS SDK, even the
  in-memory repository (which ignored the argument). A PostgreSQL backend would have had to depend on
  the DynamoDB SDK purely to satisfy the interface.
- In practice the framework only ever built **one** filter through that parameter: an
  `entityType == X` equality, constructed in `RebuildProjections`. The full generality of a DynamoDB
  expression was never used by the core.
- The streaming APIs return `result.Result[T]` over a channel. Before adding another backend we
  wanted to decide whether to keep that shape or move to an evt-owned stream/callback type.
- There was no shared, cross-backend test suite. `mem` and `dynamo` each had bespoke tests, so
  "does this backend honor the contract?" had no single executable answer — and a new backend would
  start from nothing.

## Decision

Make the storage contracts backend-neutral, keep `result.Result` channels, and add a conformance
suite that every backend runs.

### 1. `evt.StreamFilter` replaces `*expression.Expression`

A backend-neutral filter type lives in the core package:

```go
type StreamFilter struct {
    EntityType EntityType // empty matches every entity
}

func (f StreamFilter) Matches(entityType EntityType) bool
```

`Repository.StreamAllEvents` and `Repository.StreamEntities` now take a `StreamFilter`. Each backend
translates it into its own query mechanism:

- **dynamo** compiles a non-empty filter into a Scan `FilterExpression`.
- **mem** applies the filter client-side.
- a future **postgres** backend would translate it into a `WHERE` clause.

The core package no longer imports the AWS SDK.

### 2. Vendor filtering moves to a backend extension interface

Callers that genuinely need a raw DynamoDB filter (richer than entity type) use a DynamoDB-specific
extension, detected by type assertion — the same additive-capability pattern as `Compactor`,
`SnapshotStreamer`, and `EntityHeadVisitor`:

```go
type dynamo.ExpressionStreamer interface {
    StreamAllEventsByExpression(ctx, *expression.Expression) <-chan result.Result[[]evt.SerializedEvent]
    StreamEntitiesByExpression(ctx, *expression.Expression, applyEvent) <-chan result.Result[evt.Entity]
}
```

The neutral `StreamAllEvents`/`StreamEntities` are thin wrappers that build the expression from the
`StreamFilter` and delegate to the `*ByExpression` methods, so no behavior is lost.

### 3. Keep `result.Result` channels as the streaming shape

`result.Result[T]` is already backend-neutral (it lives in `evt/result`, imports nothing
vendor-specific) and is the established shape across `StreamEntitiesFromSnapshots`,
`StreamEntitiesByQuery`, and `CommitStream`. Moving to an evt-owned iterator/callback type would be a
large breaking change across every backend and caller for no contract-neutrality gain. We keep
`result.Result` channels and document them as the canonical streaming contract. A future move to a
`func(yield ...) error` iterator can be reconsidered, but is explicitly out of scope here.

### 4. Backend conformance suite

`conformance.RunRepositorySuite(t, newRepo, opts)` runs a backend-neutral battery of contract tests
against any `evt.Repository`. Backends wire it from their own test packages:

- `mem` runs it in the standard unit test job.
- `dynamo` runs it in the integration job against the local emulator.
- a future `postgres` backend wires the same suite.

`SuiteOptions` gates guarantees that not every backend provides (e.g. the in-memory test double does
not enforce optimistic concurrency). The suite namespaces its data per run so it is safe against a
shared durable table. The required storage invariants it checks are documented in
`BEHAVIORAL_INVARIANTS.md` ("Backend Storage Contract").

## Consequences

- **Breaking, intentional, documented**: the signatures of `Repository.StreamAllEvents` and
  `Repository.StreamEntities` changed from `*expression.Expression` to `evt.StreamFilter`. Callers
  that passed `nil` pass `evt.StreamFilter{}`; callers that built an `entityType` expression set
  `StreamFilter{EntityType: ...}`. Callers that pushed a custom DynamoDB expression type-assert to
  `dynamo.ExpressionStreamer` and use the `*ByExpression` methods.
- The core `evt` module no longer depends on the AWS SDK for its streaming contract; only the
  `dynamo` package does.
- A new backend now has an executable definition of "correct": implement `evt.Repository`, wire
  `conformance.RunRepositorySuite`, and make it pass.
- No DynamoDB key formats, serialized event/snapshot formats, or `result.Result` shapes changed.
