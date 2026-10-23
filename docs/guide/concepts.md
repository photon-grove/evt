# Concepts

`evt` is small on purpose. A handful of interfaces carry the whole model, and one
rule ties them together: **immutable events are the source of truth, and
everything else is derived state you can rebuild.** This page defines the
vocabulary and shows the contract behind each type. The [getting-started
guide](getting-started.md) puts them together end to end.

## Aggregate

An aggregate is an object whose state is reconstructed from events. It owns
business invariants and applies facts. Each aggregate instance is one stream in the log,
identified by an `EntityType` and an `EntityID`. Those two names describe the stream's identity,
which is why they keep the word "entity" while the object itself is an `Aggregate` (the same
trait name the Rust crate uses). An aggregate implements:

```go
type Aggregate interface {
    Type() EntityType
    GetID() EntityID
    Handle(context.Context, Command) (CommandResult, error)
    Apply(Event) error
    DeserializeEvent(SerializedEvent) (Event, error)
    EventUpcasters() []EventUpcaster
    Projectors() []EventProjector
}
```

An aggregate defines its own identity and lifecycle fields, such as an `ID` that `GetID()` returns.
The critical discipline is the split between `Handle` and `Apply`: **`Handle` validates and
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
    Events   []Event // the immutable facts to commit
    Response []byte  // stored with the command's outcome when outcomes are enabled
}
```

Returning zero events is valid — a command can legitimately decide nothing
happened. A command carries [metadata](#metadata); a stable `CommandID` makes a
retried command fail as a `DuplicateCommandError` rather than recording the same
fact twice. A store with [command outcomes](../../README.md#command-outcomes-and-rejections-v09-c3c4)
enabled goes further: it records every command (accepted, accepted with no events, or rejected)
on a durable `command-outcome#<id>` row and answers a retry with the stored outcome, even when
the original events were folded into a snapshot long ago. A handler error that implements
`evt.Rejectioner` (such as `NotFoundError`) becomes a persisted rejection.

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
(source + endpoint), client `Address`, `Region`, and `Timestamp`, plus `PrincipalID`
(the caller's identity), `CausationID` (the command that caused this one), and
`Extensions` (application-specific string keys and values). Build it with options
rather than by hand:

```go
md := evt.NewMetadata(ctx, region,
    evt.WithCommandID("idempotency-key-123"),
    evt.WithPrincipalID("user:42"),
    evt.WithCausationID("upstream-command-456"),
    evt.WithExtension("requestSource", "cli"),
    evt.WithTrace(ctx),
    evt.WithOrigin(evt.Origin{Source: "api", Endpoint: "POST /deposits"}),
)
```

`WithCausationID` overrides the default for commands triggered by another command.
In framework storage and stream encodings, an unset `causationId` defaults to
`CommandID`; with neither set, it is `null`. `PrincipalID` is optional and encodes
as `null` when unset.
`WithPrincipalID("")` and `WithCausationID("")` clear their fields. `Extensions`
always encodes as an object (`{}` when nil or empty); call `WithExtension` once
per key. Decoding preserves stored values without filling in defaults.

`evt.SerializedEvent` owns its JSON encoding: `json.Marshal(event)` and
`evt.MarshalEvent(event)` both write the canonical storage and stream format,
including these metadata defaults. `json.Unmarshal(data, &event)` and
`evt.UnmarshalEvent(data, &event)` preserve stored values without filling defaults.

`Metadata` itself has no JSON methods. Direct `encoding/json` calls on standalone
metadata use the struct's field values (including nil values), so embedding
`evt.Metadata` in an application DTO preserves sibling fields.

Use `md.Equal(other)` to compare metadata: it compares effective causation IDs
and treats nil and empty extension maps as equal, so encoded round trips remain
equal. `Metadata` contains a map and cannot be compared with `==`. See the
[storage format](../reference/storage-format.md#metadata) for the full encoding contract.

## Repository

A repository persists serialized events and snapshots and replays them back. It
is backend-neutral: the same `evt.Repository` contract is implemented by `mem`
(in-memory, for tests), `dynamo` (production), and `postgres` (production). The
DynamoDB repository stores event rows under stable `pk`/`sk` keys and uses
**conditional writes** to protect per-entity ordering; the PostgreSQL repository
keys the log on `(entity_id, sequence)` and relies on that primary key for the
same guarantee, so two writers racing on the same entity can't both win the same
sequence. See [DynamoDB integration](dynamodb.md) and
[PostgreSQL integration](postgres.md) for the exact layouts.

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
    LoadAggregate(ctx, aggregate, entityID) (CommandContext, error)
    Commit(ctx, result, cc, metadata) ([]SerializedEvent, error)
    Execute(ctx, aggregate, entityID, command, metadata) error
}
```

`CommandContext` is what `LoadAggregate` returns and `Commit` takes back: the loaded aggregate, its
`EntityID`, the sequence and snapshot the stream stood at when it was read, and the command IDs seen
during replay. It travels beside `context.Context` as its own explicit argument, so cancellation and
deadlines stay on `ctx` while the stream's state stays on `cc`.

`Execute` is the everyday entry point: it loads the aggregate from the log, handles
the command, and commits the result in one call. When an aggregate needs injected
dependencies, construct a fresh instance yourself and pass it to `store.Execute(ctx, aggregate,
entityID, command, metadata)`.

## Snapshot

A snapshot is a serialized checkpoint of aggregate state at a known sequence. It is a
**performance optimization, never a source of truth** — `snapshots.NewStore(repo,
size)` writes one roughly every `size` events so loading a long stream reads the
latest snapshot plus the tail instead of replaying from sequence 1. On DynamoDB
the snapshot lives inline in the event log at `sk = 0`.

## Projector

A projector turns event-sourced state into deterministic read models:

```go
type EventProjector interface {
    Project(context.Context, Aggregate, []Event) (TransactionGroup, error)
}
```

Projectors must be **idempotent and safe to replay** — the same events must always
produce the same view rows — because a view is rebuilt by re-running its projector
over the log. That is what makes a projection table disposable. See
[Projections and rebuilds](projections.md).

### Commit-wide projectors

An `EventProjector` sees one aggregate. A **commit-wide projector** (v0.9, C7) sees the whole commit
— every stream it wrote, with each stream's events and its state after the commit — and returns
**view operations**: puts and deletes of rows at any `pk`/`sk`, with optional conditions and
mapped errors. The rows commit in the same transaction as the events, so a cross-entity read model
(a unique-name directory, a per-tenant ledger) is never out of step with the events behind it.

```go
type CommitProjector interface {
    Name() string
    History() []EntityType // the entity types whose events it reads; none: current-state
    ProjectCommit(context.Context, CommitView) ([]ViewOperation, error)
}

store := snapshots.NewStore(repo, 5).WithCommitProjectors(directory{})
```

A commit-wide projector must be a **pure function of its `CommitView`**: no clock, random source,
environment, or store reads, and no state kept between calls. That is what lets a rebuild
reproduce its rows by regrouping the stored events by commit ID (`evt.GroupCommits`). One that
reads events declares their entity types in `History()`: the store durably marks them
history-dependent so they are never compacted, and it is given events, state, and the commit ID of
those types only. A projector that declares none is current-state and is given streams' state, with
no events and no commit ID. Every operation names the stream that owns its row, and the repository
refuses any write to the row by another stream; a row's owner never changes, because a stream's
sequence is the only commit order the log keeps. It runs once
per commit that writes at least one event, again each time fence-aware retry re-prepares the
command, and not for commands that write no event. When an operation's condition fails the commit
fails with a typed `*evt.ProjectorOperationError` naming the projector and the operation; it is not
retried. See [Commit-wide projectors](projections.md#commit-wide-projectors) and the
[storage format](../reference/storage-format.md#views).

## Transaction

A `Transaction` is `[]TransactionGroup`: a set of related writes committed together. A
`TransactionGroup` is backend-specific (a DynamoDB view projector returns a DynamoDB group), and
the framework merges compatible groups before committing.

The supported way to produce those groups is a projector: an `EventProjector` runs inside the
commit and its `TransactionGroup` is merged into the same atomic write as the command's events.

A command handler can't attach related writes to its `CommandResult`. Return zero transactions and let
projectors produce the view writes. [Commit-wide projectors](#commit-wide-projectors) ([ADR
0004](../adr/0004-core-reshape-multi-stream-transactions-and-outcomes.md), C7) emit backend-neutral
operations for every event in a commit, not just one entity's. The groups of `EventProjector` come
first in a commit, then the commit-wide projectors'.

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
`RebuildConfig.SeedAggregate`, never by replaying from sequence 1. Full details in
[Projections and rebuilds](projections.md).
