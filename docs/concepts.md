# Concepts

`evt` is small on purpose. A handful of interfaces carry the whole model, and one
rule ties them together: **immutable events are the source of truth, and
everything else is derived state you can rebuild.** This page defines the
vocabulary and shows the contract behind each type. The [getting-started
guide](getting-started.md) puts them together end to end.

## Entity

An entity is an aggregate root whose state is reconstructed from events. It owns
business invariants and applies facts. It implements:

```go
type Entity interface {
    Type() EntityType
    GetID() EntityID
    Base() BaseEntity
    Handle(context.Context, Command) (CommandResult, error)
    Apply(Event) error
    DeserializeEvent(SerializedEvent) (Event, error)
    EventUpcasters() []EventUpcaster
    Projectors() []EventProjector
}
```

Embed `evt.BaseEntity` (and construct it with `evt.NewEntity(id)`) to get the
`ID`, `IsActive`, `CreatedAt`, and `UpdatedAt` bookkeeping for free. The critical
discipline is the split between `Handle` and `Apply`: **`Handle` validates and
decides; `Apply` mutates and trusts.** `Handle` produces events without changing
state; `Apply` folds an already-recorded event into state without re-validating
it. Replay only ever calls `Apply`, so any check left in `Apply` would re-run
against history.

## Command

A command is a request to change state. It implements:

```go
type Command interface {
    Type() CommandType
    EntityType() EntityType
}
```

`Handle` turns a command into a `CommandResult`:

```go
type CommandResult struct {
    Events      []Event     // the immutable facts to commit
    Transaction Transaction // optional related writes ([]TransactionGroup)
}
```

Returning zero events is valid — a command can legitimately decide nothing
happened. A command carries [metadata](#metadata); a stable `CommandID` makes a
retried command fail as a `DuplicateCommandError` rather than recording the same
fact twice.

## Event

An event is an immutable fact — something that already happened, named in the past
tense. It implements:

```go
type Event interface {
    Type() EventType
    Version() EventVersion
    EntityType() EntityType
    EntityID() EntityID
}
```

Events are **versioned** so payload schemas can evolve without breaking old data
(see [Upcaster](#upcaster)). On the wire they become a `SerializedEvent` — the
durable record the repository stores and replays:

```go
type SerializedEvent struct {
    ID         EventID       // entityID + sequence
    Sequence   EventSequence // per-entity ordinal, 1, 2, 3, …
    Type       EventType
    Version    EventVersion
    EntityID   EntityID
    EntityType EntityType
    Payload    []byte        // your event's JSON
    Metadata   Metadata
}
```

The `Type` string and `Payload` JSON are a durable contract: once events are
stored, changing either requires an upcaster so old rows stay readable.

## Metadata

`evt.Metadata` travels with every command and is stamped onto every event it
produces. It carries an optional `CommandID`, OpenTelemetry `Trace`, `Origin`
(source + endpoint), client `Address`, `Region`, and `Timestamp`. Build it with
options rather than by hand:

```go
md := evt.NewMetadata(ctx, region,
    evt.WithCommandID("idempotency-key-123"),
    evt.WithTrace(ctx),
    evt.WithOrigin(evt.Origin{Source: "api", Endpoint: "POST /deposits"}),
)
```

## Repository

A repository persists serialized events and snapshots and replays them back. It
is backend-neutral: the same `evt.Repository` contract is implemented by `mem`
(in-memory, for tests) and `dynamo` (production). The DynamoDB repository stores
event rows under stable `pk`/`sk` keys and uses **conditional writes** to protect
per-entity ordering, so two writers racing on the same entity can't both win the
same sequence. See [DynamoDB integration](dynamodb.md) for the exact key layout.

Some capabilities are optional and detected by type assertion rather than baked
into the core interface — `Compactor` (truncate covered events), `SnapshotStreamer`
(seed rebuilds from snapshots), and `EntityHeadVisitor` (constant-memory
enumeration for [incremental rebuilds](projections.md)). Backends advertise what
they support; callers feature-detect.

## Store

A store coordinates the load → handle → serialize → commit → apply cycle. Its
contract is small:

```go
type Store interface {
    LoadEntity(ctx, entity, entityID) (Context, error)
    Commit(ctx, result, context, metadata) ([]SerializedEvent, error)
    Execute(ctx, entity, entityID, command, metadata) error
}
```

`Execute` is the everyday entry point: it loads the entity from the log, handles
the command, and commits the result in one call. When an aggregate needs injected
dependencies, construct it through `evt.ExecuteWithFactory(ctx, store, factory,
…)` so the store can build a fresh instance per call.

## Snapshot

A snapshot is a serialized checkpoint of entity state at a known sequence. It is a
**performance optimization, never a source of truth** — `snapshots.NewStore(repo,
size)` writes one roughly every `size` events so loading a long stream reads the
latest snapshot plus the tail instead of replaying from sequence 1. On DynamoDB
the snapshot lives inline in the event log at `sk = 0`.

## Projector

A projector turns event-sourced state into deterministic read models:

```go
type EventProjector interface {
    Project(context.Context, Entity, []Event) (TransactionGroup, error)
}
```

Projectors must be **idempotent and safe to replay** — the same events must always
produce the same view rows — because a view is rebuilt by re-running its projector
over the log. That is what makes a projection table disposable. See
[Projections and rebuilds](projections.md).

## Transaction

A `Transaction` is `[]TransactionGroup`: a command can return related writes
alongside its events so they commit together. A `TransactionGroup` is
backend-specific (a DynamoDB view projector returns a DynamoDB group), and the
framework merges compatible groups before committing. This is how a single command
appends facts and updates derived rows atomically where the backend supports it.

## Upcaster

When an event's JSON shape changes, older stored payloads must still load. An
upcaster transforms a historical `SerializedEvent` forward to the current shape
before `DeserializeEvent` runs:

```go
type EventUpcaster interface {
    CanUpcast(EventType, EventVersion) bool
    Upcast(SerializedEvent) (SerializedEvent, error)
}
```

Bump the event's `Version()` and register an upcaster (returned from
`EventUpcasters()`) plus a fixture test for every historical version. Chained
upcasters walk a payload from v1 → v2 → current. **Never assume every stored event
already has the latest shape.**

## Rebuild

A rebuild replays the log to regenerate read models — the operational expression
of "views are derived." `evt.RebuildProjections` streams entities, reconstitutes
each one, runs its projectors, and commits the resulting groups (or, in dry-run
mode, reports what it would write). Streams that have been
[compacted](dynamodb.md#compaction) must rebuild from their snapshot via
`RebuildConfig.SeedEntity`, never by replaying from sequence 1. Full details in
[Projections and rebuilds](projections.md).
