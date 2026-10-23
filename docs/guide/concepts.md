# Concepts

evt is small on purpose. A handful of interfaces carry the whole model, and one rule ties them
together: immutable events are the source of truth, and everything else is derived state you can
rebuild. This page walks through the vocabulary and the contract behind each type. The
[getting-started guide](getting-started.md) puts them together end to end.

## Aggregate

An aggregate is an object whose state is rebuilt from events. It owns the business invariants and
applies the facts. Each aggregate instance is one stream in the log, identified by an `EntityType`
and an `EntityID`. Those two names describe the stream's identity, which is why they keep the word
"entity" while the object itself is an `Aggregate` (the same trait name the Rust crate uses). An
aggregate implements this interface:

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

The aggregate defines its own identity and lifecycle fields, such as the `ID` that `GetID()`
returns. The discipline that matters most is the split between `Handle` and `Apply`. `Handle`
validates and decides. It produces events without changing state. `Apply` mutates and trusts. It
folds an event that was already recorded into state, without checking it again. Replay only ever
calls `Apply`, so any check left in `Apply` would run again against history.

## Command

A command is a request to change state:

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

Returning zero events is fine, because a command can decide that nothing happened. A command
carries [metadata](#metadata), and a stable `CommandID` there makes a retried command fail as a
`DuplicateCommandError` instead of recording the same fact twice.

A store with [command outcomes](commands.md) enabled goes further. It records every command
(accepted, accepted with no events, or rejected) on a durable `command-outcome#<id>` row, and it
answers a retry with the stored outcome, even when the original events were folded into a snapshot
long ago. A handler error that implements `evt.Rejectioner` (such as `NotFoundError`) becomes a
persisted rejection.

## Event

An event is an immutable fact, something that already happened, named in the past tense:

```go
type Event interface {
    Type() EventType
    Version() EventVersion
    EntityType() EntityType
    EntityID() EntityID
}
```

Events are versioned so payload schemas can change without breaking old data (see
[Upcaster](#upcaster)). On the wire an event becomes a `SerializedEvent`, the durable record the
repository stores and replays:

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

The `Type` string and the `Payload` JSON are a durable contract. Once events are stored, changing
either one needs an upcaster so the old rows stay readable.

## Metadata

`evt.Metadata` travels with every command and is stamped onto every event the command produces. It
carries an optional `CommandID`, the OpenTelemetry `Trace`, an `Origin` (source and endpoint), the
client `Address`, `Region` and `Timestamp`. It also carries `PrincipalID` (the caller's identity),
`CausationID` (the command that caused this one) and `Extensions` (application-specific string keys
and values). Build it with options rather than by hand:

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

`WithCausationID` overrides the default for a command triggered by another command. In storage and
stream encodings an unset `causationId` defaults to the `CommandID`, and with neither set it's
`null`. `PrincipalID` is optional and encodes as `null` when unset. `WithPrincipalID("")` and
`WithCausationID("")` clear their fields. `Extensions` always encodes as an object (`{}` when nil or
empty), and you call `WithExtension` once per key. Decoding keeps the stored values as they are and
fills in no defaults.

`evt.SerializedEvent` owns its JSON encoding. `json.Marshal(event)` and `evt.MarshalEvent(event)`
both write the canonical storage and stream format, defaults included. `json.Unmarshal(data,
&event)` and `evt.UnmarshalEvent(data, &event)` keep the stored values without filling defaults.

`Metadata` itself has no JSON methods. A direct `encoding/json` call on standalone metadata uses the
struct's field values (nil values included), so embedding `evt.Metadata` in an application DTO
keeps its sibling fields intact.

Compare metadata with `md.Equal(other)`. It compares the effective causation IDs and treats nil and
empty extension maps as equal, so encoded round trips stay equal. `Metadata` holds a map, so `==`
doesn't compile. The [storage format](../reference/storage-format.md#metadata) has the full
encoding contract.

## Repository

A repository persists serialized events and snapshots and replays them. It's backend-neutral: the
same `evt.Repository` contract is implemented by `mem` (in memory, for tests), `dynamo` and
`postgres`. The DynamoDB repository stores event rows under stable `pk`/`sk` keys and uses
conditional writes to protect per-entity ordering. The PostgreSQL repository keys the log on
`(entity_id, sequence)` and relies on that primary key for the same guarantee. Either way, two
writers racing on one entity can't both win the same sequence. [DynamoDB](dynamodb.md) and
[PostgreSQL](postgres.md) have the exact layouts.

Some capabilities are optional and detected by type assertion instead of living in the core
interface: `Compactor` (truncate covered events), `SnapshotStreamer` (seed rebuilds from snapshots)
and `EntityHeadVisitor` (constant-memory enumeration for [incremental rebuilds](projections.md)).
Backends advertise what they support, and callers check.

Every durable Go repository enforces the
[feature marker](../reference/storage-format.md#feature-marker) by default, with no supported
extensions. The marker is a reserved row that declares the extensions a store uses. An unsupported
declaration refuses commits and standalone snapshot writes atomically.
`repo.WithFeatureMarker(repo.FeatureMarker(supported))` changes the supported set, not whether
enforcement runs. Create markers from the repository, so reads and transactional checks share its
client and events table. Attaching a marker from another client or table is a typed configuration
error.

## Store

A store runs the load, handle, serialize, commit and apply cycle. Its contract is small:

```go
type Store interface {
    LoadAggregate(ctx, aggregate, entityID) (CommandContext, error)
    Commit(ctx, result, cc, metadata) ([]SerializedEvent, error)
    Execute(ctx, aggregate, entityID, command, metadata) error
}
```

`CommandContext` is what `LoadAggregate` returns and `Commit` takes back: the loaded aggregate, its
`EntityID`, the sequence and snapshot the stream stood at when it was read, and the command IDs seen
during replay. It travels beside `context.Context` as its own argument, so cancellation and
deadlines stay on `ctx` while the stream's state stays on `cc`.

`Execute` is the everyday entry point. It loads the aggregate from the log, handles the command and
commits the result in one call. When an aggregate needs injected dependencies, construct a fresh
instance yourself and pass it to `store.Execute(ctx, aggregate, entityID, command, metadata)`.

## Snapshot

A snapshot is a serialized checkpoint of aggregate state at a known sequence. It's a performance
optimization and never a source of truth. `snapshots.NewStore(repo, size)` writes one roughly every
`size` events, so loading a long stream reads the latest snapshot plus the tail instead of replaying
from sequence 1. On DynamoDB the snapshot lives inline in the event log at `sk = 0`.

## Projector

A projector turns event-sourced state into deterministic read models:

```go
type EventProjector interface {
    Project(context.Context, Aggregate, []Event) (TransactionGroup, error)
}
```

Projectors have to be idempotent and safe to replay. The same events must always produce the same
view rows, because a view is rebuilt by running its projector over the log again. That's what makes
a projection table disposable. [Projections and rebuilds](projections.md) covers it in depth.

### Commit-wide projectors

An `EventProjector` sees one aggregate. A commit-wide projector sees the whole commit: every stream
it wrote, with each stream's events and its state after the commit. It returns view operations,
meaning puts and deletes of rows at any `pk`/`sk`, with optional conditions and mapped errors. The
rows commit in the same transaction as the events, so a cross-entity read model (a unique-name
directory, a per-tenant ledger) is never out of step with the events behind it.

```go
type CommitProjector interface {
    Name() string
    History() []EntityType // the entity types whose events it reads; none: current-state
    ProjectCommit(context.Context, CommitView) ([]ViewOperation, error)
}

store := snapshots.NewStore(repo, 5).WithCommitProjectors(directory{})
```

A commit-wide projector must be a pure function of its `CommitView`. That means no clock, random
source, environment or store reads, and no state kept between calls. It's what lets a rebuild
reproduce the rows by regrouping the stored events by commit ID (`evt.GroupCommits`).

A projector that reads events declares their entity types in `History()`. The store durably marks
those types history-dependent so they're never compacted, and it hands the projector the events,
state and commit ID for those types only. A projector that declares none is current-state, and it
gets each stream's state with no events and no commit ID. Every operation names the stream that owns
its row, and the repository refuses a write to that row from any other stream. A row's owner never
changes, because a stream's sequence is the only commit order the log keeps.

The projector runs once per commit that writes at least one event, again each time fence-aware retry
re-prepares the command, and not at all for a command that writes no event. When an operation's
condition fails, the commit fails with a typed `*evt.ProjectorOperationError` naming the projector
and the operation, and it isn't retried. See [Commit-wide
projectors](projections.md#commit-wide-projectors) and the
[storage format](../reference/storage-format.md#views).

## Transaction

A `Transaction` is a `[]TransactionGroup`, a set of related writes committed together. A
`TransactionGroup` is backend-specific (a DynamoDB view projector returns a DynamoDB group), and
the framework merges compatible groups before committing.

The supported way to produce those groups is a projector. An `EventProjector` runs inside the commit,
and its `TransactionGroup` joins the same atomic write as the command's events. A command handler
can't attach related writes to its `CommandResult`, so let projectors produce the view writes.
[Commit-wide projectors](#commit-wide-projectors) (from
[ADR 0004](../adr/0004-core-reshape-multi-stream-transactions-and-outcomes.md)) emit backend-neutral
operations for every event in a commit, not just one entity's. In a commit the `EventProjector`
groups come first, then the commit-wide projectors' operations.

## Upcaster

When an event's JSON shape changes, the older stored payloads still have to load. An upcaster
transforms a historical `SerializedEvent` forward to the current shape before `DeserializeEvent`
runs:

```go
type EventUpcaster interface {
    CanUpcast(EventType, EventVersion) bool
    Upcast(SerializedEvent) (SerializedEvent, error)
}
```

Bump the event's `Version()`, return an upcaster from `EventUpcasters()`, and add a fixture test
for every historical version. Chained upcasters walk a payload from v1 to v2 to current. Never
assume every stored event already has the latest shape.

## Rebuild

A rebuild replays the log to regenerate read models. It's the working form of "views are
derived". `evt.RebuildProjections` streams entities, rebuilds each one, runs its projectors and
commits the resulting groups. In dry-run mode it reports what it would write instead. A stream that
has been [compacted](dynamodb.md#compaction) has to rebuild from its snapshot through
`RebuildConfig.SeedAggregate`, never by replaying from sequence 1. [Projections and
rebuilds](projections.md) has the details.

## Keeping the log

Event rows are the source of truth and view rows are derived state. So any projection or view table
must be safe to wipe and rebuild from the log. Human decisions, external signals, publish flags and
review verdicts get recorded as events before a projector writes them to a view.

Event logs are append-only and kept in full by default. Two opt-in mechanisms trim them safely.

Compaction (`evt.Compactor.CompactBelow`) deletes only events that a durable snapshot already
covers, and a rebuild then seeds from that snapshot instead of replaying from event 1 (see
[ADR 0001](../adr/0001-event-compaction-and-snapshot-truncation.md)). Compaction checks the feature
marker's declarations across handles. PostgreSQL holds one transaction lock for the whole run.
DynamoDB fences each delete batch, with no whole-operation lease and no rollback of batches already
committed. So a history-dependent declaration that races a compaction can leave a stream partially
compacted, and its history-dependent rebuild then reports `CompactionUncovered`. Declare history
dependence before you write data of that entity type.

Per-type retention (`dynamo.Repository.WithRetention`) sets the DynamoDB `ttl` attribute to
`committedAt + duration`, for terminal, short-lived, transient streams. TTL expires rows one by
one, so use it only when a stream's lifetime is much shorter than the duration and no events are
appended after the stream becomes terminal. Otherwise an older prefix can expire while newer events
remain, and loading the stream then fails with a conflict. For streams that keep accumulating,
keep a snapshot and compact instead.
