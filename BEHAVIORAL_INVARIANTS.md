# Go evt Library - Behavioral Invariants

This document captures the behavioral guarantees, serialization formats, error conditions, and key
invariants of the `evt` library.

## Core Types

### EntityID, EntityType, EventID

- `EntityID`: String type representing unique entity identifiers
- `EntityType`: String type representing the entity kind (e.g., "User", "Order")
- `EventID`: Formatted as `{entityID}:{sequence}` - use `GetEventID(entityID, sequence)` to generate

### EventSequence, EventVersion

- `EventSequence`: Integer starting from 1, increments with each event for an entity
- `EventVersion`: Integer representing schema version, starts at 1 for new event types

### Event ID Format

```go
GetEventID(entityID, sequence) → "{entityID}:{sequence}"
// Examples:
// GetEventID("user-123", 1) → "user-123:1"
// GetEventID("order-456", 42) → "order-456:42"
```

## DynamoDB Schema

### Events Table

| Field      | DynamoDB Type | JSON Key     | Description                   |
| ---------- | ------------- | ------------ | ----------------------------- |
| pk         | S             | `pk`         | Partition key = EntityID      |
| sk         | N             | `sk`         | Sort key = EventSequence      |
| type       | S             | `type`       | EventType                     |
| version    | N             | `version`    | EventVersion (schema version) |
| entityType | S             | `entityType` | EntityType                    |
| payload    | S             | `payload`    | JSON-encoded event data       |
| metadata   | S             | `metadata`   | JSON-encoded Metadata struct  |

**Key Invariants:**

- `pk` + `sk` combination must be unique (enforced by conditional write)
- `sk` (sequence) starts at 1 and increments monotonically per entity
- `sk` = 0 is reserved for inline snapshots (filtered out by event queries)

### Inline Snapshots (sk=0)

Snapshots are stored inline in the events table at `sk=0` for each entity:

| Field      | DynamoDB Type | JSON Key     | Description                              |
| ---------- | ------------- | ------------ | ---------------------------------------- |
| pk         | S             | `pk`         | Partition key = EntityID                 |
| sk         | N             | `sk`         | Sort key = 0 (reserved for snapshots)    |
| seq        | N             | `seq`        | Snapshot sequence (starts at 1)          |
| eventSeq   | N             | `eventSeq`   | Last event sequence included in snapshot |
| entityType | S             | `entityType` | EntityType                               |
| payload    | S             | `payload`    | JSON-encoded entity state                |

**Key Invariants:**

- One snapshot per entity (overwritten in place at `sk=0`)
- `seq` represents how many snapshots have been taken (1, 2, 3...)
- `eventSeq` is the last event sequence included in the snapshot
- `eventSeq >= seq` always (usually `eventSeq >> seq`)
- Conditional write checks `seq` to prevent race conditions
- Event queries filter out `sk=0` rows automatically

### Views Table

| Field      | DynamoDB Type | JSON Key     | Description                  |
| ---------- | ------------- | ------------ | ---------------------------- |
| pk         | S             | `pk`         | Partition key (configurable) |
| entityID   | S             | `entityID`   | EntityID                     |
| entityType | S             | `entityType` | EntityType (for GSI queries) |
| payload    | S             | `payload`    | JSON-encoded view data       |

## Serialization Formats

### SerializedEvent

```json
{
  "id": "entity-123:5",
  "sequence": 5,
  "type": "UserCreated",
  "version": 1,
  "entityId": "entity-123",
  "entityType": "User",
  "payload": "base64-or-json-bytes",
  "metadata": { ... }
}
```

### Metadata Structure

```json
{
  "commandId": "cmd-uuid",
  "trace": { "traceparent": "...", ... },
  "origin": { "source": "api", "endpoint": "/users" },
  "address": "192.168.1.1",
  "region": "us-east-1",
  "timestamp": "2024-01-01T00:00:00Z"
}
```

**Key Fields:**

- `commandId`: Optional command correlation ID
- `trace`: OpenTelemetry trace context (propagation.MapCarrier)
- `origin`: Request source and endpoint
- `address`: Client IP address (from context)
- `region`: AWS region
- `timestamp`: RFC3339 format, UTC timezone

## Error Types and Messages

### NotFoundError

```go
NewNotFoundError(msg string) → *NotFoundError
IsNotFoundErr(err error) → bool  // Uses errors.As
```

### ConflictError

```go
NewConflictError(msg string) → *ConflictError
IsConflictErr(err error) → bool  // Uses errors.As
```

### BadCommandError

```go
NewBadCommandError(command Command) → *BadCommandError
Error() → "Command not recognized: {command}"
```

### BadEventError

```go
NewBadEventError(event Event) → *BadEventError
Error() → "Event not recognized: {event}"
```

### UpcastError

```go
NewUpcastError(err error, event SerializedEvent) → *UpcastError
Error() → err.Error()  // Returns underlying error message
```

### DynamoDB Conditional Check Failure

- Detected via `HasConditionalCheckFailure(err)` function
- Returns `(bool, int)` where int is the index of the failed item
- Race condition message: `"race condition - sequence number updated since it was read, try again"`

## Transaction System

### TransactionGroup Interface

```go
type TransactionGroup interface {
    Len() int
    Merge(with TransactionGroup) (TransactionGroup, error)
    StorageType() StorageType
    TransactionType() TransactionType
    HandleError(error, int) error
}
```

### DynamoDB TransactionGroup Extension

```go
type TransactionGroup interface {
    evt.TransactionGroup
    ToWriteItems() []types.TransactWriteItem
    MergeDynamo(with TransactionGroup) (TransactionGroup, error)
}
```

### Transaction Limits

- DynamoDB supports up to 100 items per transaction
- `CommitStream` batches items to stay under this limit

## Snapshot Logic

### Snapshot Timing

- Controlled by `snapshotSize` parameter (default: 5); a size of 0 or less disables snapshots
- `CalculateAdditionalEvents(currentSequence, numEvents, maxSize)` decides whether a commit takes a
  snapshot: it returns 0 when the batch does not reach the next snapshot boundary, otherwise the
  full batch length
- When a snapshot is taken it captures the entity state through the **last** event in the batch, so
  `eventSeq` always equals that last event's sequence and the payload is consistent with it

### Snapshot Creation

1. Apply the batch's events to the entity
2. Increment snapshot sequence (`seq`)
3. Record the last event sequence (`eventSeq`)
4. Marshal entity state to JSON payload

### Entity Loading Priority

1. Check for snapshot first
2. If found: unmarshal snapshot, then load events after `eventSeq`
3. If not found: load all events from sequence 1
4. Apply all loaded events to rebuild state

## Compaction

Compaction is the opt-in mechanism for bounding event-log growth. See
[ADR 0001](docs/adr/0001-event-compaction-and-snapshot-truncation.md) for the full rationale and
its coordination with consumer wipe-and-replay guarantees.

### Compactor capability

`CompactBelow(ctx, entityID, throughSequence) (deleted int, err error)` is an optional capability
(interface `evt.Compactor`, detected via type assertion; implemented by `dynamo` and `mem`).

- Deletes events with sequence in `[1, throughSequence]` for the entity.
- **Refuses unless covered:** reads the inline snapshot and requires
  `snapshot.EventSequence >= throughSequence`. With no snapshot, or an uncovered range, it deletes
  nothing and returns `evt.ErrCompactionUncovered` (wrapped; matchable with `errors.Is`).
- Never deletes the `sk = 0` snapshot row. `throughSequence < 1` is a no-op. Idempotent.

### Compaction invariants

- A stream's events below its latest durable snapshot's `eventSeq` are **not required for rebuild**;
  the authoritative start of a compacted stream is its snapshot, not event 1.
- After compaction, correct rebuild **requires** the snapshot-aware path; legacy full-replay-from-1
  over a compacted stream reconstructs incorrect state.

### Snapshot-aware rebuild

`SnapshotStreamer.StreamEntitiesFromSnapshots(ctx, expr, seedEntity, applyEvent)` (optional
capability) seeds each entity from its `sk = 0` snapshot, then applies only events with
`sequence > snapshot.EventSequence`. Streams without a snapshot fall back to full replay from
sequence 1. `RebuildProjections` uses this path when `RebuildConfig.SeedEntity` is set, and errors
if `SeedEntity` is set on a repository that is not a `SnapshotStreamer`.

## Projector System

### EventProjector Interface

```go
type EventProjector interface {
    Project(ctx context.Context, entity Entity, events []Event) (TransactionGroup, error)
}
```

### ViewProjector Behavior

- Creates `ViewPutGroup` transactions
- Nil entity or empty entity ID returns `nil, nil`
- JSON marshal errors are propagated
- View PK defaults to `string(entityID)`

## Upcasting

### EventUpcaster Interface

```go
type EventUpcaster interface {
    CanUpcast(EventType, EventVersion) bool
    Upcast(SerializedEvent) (SerializedEvent, error)
}
```

### Upcasting Flow

1. Check each upcaster with `CanUpcast(type, version)`
2. If true, call `Upcast` to transform the serialized event
3. Multiple upcasters can chain transformations
4. Final event is passed to `entity.DeserializeEvent`

## Sorting

### ByCommandID Sort

- Sorts `[]SerializedEvent` by `Metadata.CommandID` if both events have one
- Falls back to sorting by `ID` (EventID) if either is missing CommandID
- Used for deterministic event ordering in batch operations

## Testing Patterns

### Mock Entity Requirements

Implement all `Entity` interface methods:

- `Type() EntityType`
- `GetID() EntityID`
- `Base() BaseEntity`
- `Handle(context.Context, Command) (CommandResult, error)`
- `Apply(Event) error`
- `DeserializeEvent(SerializedEvent) (Event, error)`
- `EventUpcasters() []EventUpcaster`
- `Projectors() []EventProjector`

### Test Metadata

Standard test metadata format:

```go
evt.Metadata{
    Region: "us-east-1",
    Origin: &evt.Origin{Source: "TestSuite", Endpoint: "Testing"},
}
```

## Repository Operations

### Commit

- Empty events: no-op, returns nil
- Builds `TransactWriteItem` with conditional expression `attribute_not_exists(sk)`
- Includes any transaction groups from projectors

### CommitWithSnapshot

- Same as Commit, plus snapshot item
- Snapshot conditional: `attribute_not_exists(seq) OR (seq = :previousSeq)`

### GetEvents

- Returns `nil` for empty result (not empty slice)
- Skips items where `sk = 0` (snapshots in legacy data)
- Uses pagination for large result sets

### GetLatestEvents

- Same as GetEvents but with `sk > :lastSequence` condition

### GetSnapshot

- Returns `nil, nil` if not found
- Single item lookup by `pk` only

### StreamAllEvents

- Returns channel of `result.Result[[]SerializedEvent]`
- Skips items with `snap` attribute (snapshots)
- Uses scan pagination

### StreamEntities

- Yields complete entities after all their events are processed
- Skips `sequence = 0` events (snapshot markers)
- Errors during `applyEvent` are yielded to channel, then continues

### StreamEntitiesFromSnapshots (optional: `evt.SnapshotStreamer`)

- Like `StreamEntities`, but seeds each entity from its `sk = 0` snapshot via `seedEntity`
- Skips events with `sequence <= snapshot.EventSequence` (already captured by the seed)
- Entities without a snapshot fall back to full replay from sequence 1 via `applyEvent`
- A `seedEntity` error is yielded to the channel and that entity's remaining events are skipped

### CompactBelow (optional: `evt.Compactor`)

- Deletes events with `sequence` in `[1, throughSequence]`
- Refuses with `evt.ErrCompactionUncovered` unless a snapshot covers the range
  (`snapshot.EventSequence >= throughSequence`)
- Never deletes the `sk = 0` snapshot; `throughSequence < 1` is a no-op; idempotent
- DynamoDB: key-only range query + `BatchWriteItem` deletes (25/batch) with bounded
  `UnprocessedItems` retries

### Delete (build-tagged `//go:build !prod`)

- Raw, snapshot-unsafe point-delete by `(pk, sk)`; for local/staging fixtures only
- Excluded from production builds (`-tags prod`); released binaries set this tag
- Use `CompactBelow` for principled, snapshot-verified truncation instead
