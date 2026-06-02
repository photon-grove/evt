# Go evt Library - Behavioral Invariants

This document captures the behavioral guarantees, serialization formats, error conditions, and key
invariants of the `go/evt` library.

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

- Controlled by `snapshotSize` parameter (default: 5)
- `CalculateAdditionalEvents(currentSequence, numEvents, maxSize)` determines when to snapshot

### Snapshot Creation

1. Apply events up to snapshot point
2. Increment snapshot sequence (`seq`)
3. Record current event sequence (`eventSeq`)
4. Marshal entity state to JSON payload

### Entity Loading Priority

1. Check for snapshot first
2. If found: unmarshal snapshot, then load events after `eventSeq`
3. If not found: load all events from sequence 1
4. Apply all loaded events to rebuild state

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
