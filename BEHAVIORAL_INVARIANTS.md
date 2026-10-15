# Go evt Library - Behavioral Invariants

This document captures the behavioral guarantees, serialization formats, error conditions, and key
invariants of the `evt` library. The byte-level storage contract shared with the Rust `photon-evt`
crate is specified in [`docs/spec/storage-format.md`](docs/spec/storage-format.md).

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
| ttl        | N             | `ttl`        | Optional TTL expiry (Unix s)  |

**Key Invariants:**

- `pk` + `sk` combination must be unique (enforced by conditional write)
- `sk` (sequence) starts at 1 and increments monotonically per entity
- `sk` = 0 is reserved for inline snapshots (filtered out by event queries)
- `ttl` is written **only** for entity types covered by a `Repository` retention policy
  (`WithRetention`); every other row omits the attribute entirely (`json:"ttl,omitempty"`) and is
  never auto-expired. It is a no-op unless the table has DynamoDB TTL enabled on the `ttl` attribute.
  Policy a type **only** when its events are transient and no projection rebuild replays them —
  otherwise DynamoDB would silently delete history a wipe-and-replay depends on.
- Each event is stamped `committedAt + duration`, and DynamoDB TTL expires items individually, so
  retention cannot atomically drop a multi-event stream. It is safe only for terminal, short-lived
  streams (lifetime ≪ duration, no appends after terminal); a stream appended across a span near the
  duration can lose its older prefix while newer events survive. Store loads detect the gap and fail
  with a transient conflict on every attempt, because the gap never heals. Rebuild scans do not
  detect it. Use compaction (snapshot + `CompactBelow`) for streams that accumulate events over time.

### Inline Snapshots (sk=0)

Snapshots are stored inline in the events table at `sk=0` for each entity:

| Field      | DynamoDB Type | JSON Key     | Description                                  |
| ---------- | ------------- | ------------ | -------------------------------------------- |
| pk         | S             | `pk`         | Partition key = EntityID                     |
| sk         | N             | `sk`         | Sort key = 0 (reserved for snapshots)        |
| seq        | N             | `seq`        | Snapshot sequence (starts at 1)              |
| eventSeq   | N             | `eventSeq`   | Last event sequence included in snapshot     |
| entityType | S             | `entityType` | EntityType                                   |
| payload    | S             | `payload`    | JSON-encoded entity state                    |
| ttl        | N             | `ttl`        | Optional TTL expiry (Unix s), mirrors policy |

**Key Invariants:**

- One snapshot per entity (overwritten in place at `sk=0`)
- `ttl` mirrors the entity type's retention policy (see Events Table) so a snapshot never outlives the
  events it summarizes; omitted for un-policed types
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

In v0.9 (C7), commit-wide projectors may put or delete rows at any `pk`/`sk` — not only the
derived `{entityType}#{entityId}` / `VIEW` — with per-operation error mapping and shared
key-length validation (`pk` ≤ 2,048 and `sk` ≤ 1,024 UTF-8 bytes, enforced before any write on
both backends). The row shape and rules are specified in the
[storage format](docs/spec/storage-format.md#views).

## Serialization Formats

### SerializedEvent

`SerializedEvent` owns its JSON encoding. `json.Marshal` / `json.Unmarshal` and
`evt.MarshalEvent` / `evt.UnmarshalEvent` use the same wire codec: encoding applies effective
causation and empty-extension defaults, while decoding preserves stored values. `Metadata` itself
has no JSON methods; embedding it in application DTOs must preserve sibling fields.

```json
{
  "id": "entity-123:5",
  "sequence": 5,
  "type": "UserCreated",
  "version": 1,
  "entityId": "entity-123",
  "entityType": "User",
  "payload": "<base64 of the payload JSON>",
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

In v0.9 ([ADR 0004](docs/adr/0004-core-reshape-multi-stream-transactions-and-outcomes.md), C6),
metadata gains `principalId` (string or null), `causationId` (string or null; defaults to
`commandId` when unset), `extensions` (string-keyed map, `{}` when empty), and `commitId`
(string; engine-written, identical across every event of a commit, defaulting to the command ID
when outcomes are enabled, else a generated ULID) — appended after the
existing keys. The byte-level shape — including defaults for old readers — is specified in the
[storage format](docs/spec/storage-format.md#metadata).

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
- A commit without a snapshot adds one `ConditionCheck` on the entity's `sk = 0` item (the floor
  check). It counts toward the 100 items and is billed as a write
- `CommitStream` (deprecated in v0.9.0, K1) batches items to stay under this limit, counting one
  floor `ConditionCheck` per
  entity, at the lowest sequence the batch commits for it, whatever order results arrive in

The v0.9 core reshape adds further transaction items — read-fence checks, the command-outcome row,
and the feature-marker and rebuild-lock checks — with a pinned item ordering and budget formula in
the [storage format](docs/spec/storage-format.md#writes). An over-budget commit fails with a typed
error before any write and is never split.

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
- In v0.9 ([ADR 0004](docs/adr/0004-core-reshape-multi-stream-transactions-and-outcomes.md), C7,
  C9) it also reads the feature manifest's durable `historyDependent` set (consistent read) and
  refuses with a typed error when the entity's type is declared history-dependent — in every
  process that opens the log, not only those that registered the projector. The byte-level rules
  are specified in the [storage format](docs/spec/storage-format.md#feature-marker).
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

_Deprecated in v0.9.0 ([ADR 0006](docs/adr/0006-parity-policy-and-feature-cuts.md), K7):_
`evt.ByCommandID` is cut and removed in the next minor release. It has no in-library
replacement — write your own comparator when you need deterministic batch ordering. The
pre-v0.9 behavior:

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
- Rejects events whose sequences are not contiguous for an entity, before writing
- Builds `TransactWriteItem` with conditional expression `attribute_not_exists(sk)`
- Adds a `ConditionCheck` on the entity's `sk = 0` item: `attribute_not_exists(eventSeq) OR
eventSeq < :first`, so a commit cannot write at or below a snapshot's floor after compaction
  deleted those keys
- Includes any transaction groups from projectors

### CommitWithSnapshot

- Same as Commit, plus snapshot item
- Snapshot conditional for the first snapshot (sequence 1): `attribute_not_exists(seq)`
- Snapshot conditional for later snapshots: `seq = :seq AND eventSeq < :first`, where `:seq` is the
  new sequence minus one. The floor check is folded into the snapshot put because a transaction
  cannot touch `sk = 0` twice

### GetEvents

- Returns `nil` for empty result (not empty slice)
- Skips items where `sk = 0` (snapshots in legacy data)
- Uses pagination for large result sets

### GetLatestEvents

- Same as GetEvents but with `sk > :lastSequence` condition

### GetRecentEvents

- Bounded, newest-first read of an entity's last `limit` events (storage spec, latest(n); ADR-0004
  C8) — for "recent activity" reads that must not replay a long stream. No item shape changes.
  Invalid limits return `*evt.InvalidArgumentError`, matchable with `errors.Is(err, evt.ErrInvalidArgument)`, before I/O.
- DynamoDB: one descending query (`ScanIndexForward = false`, `Limit = n`) whose `sk > 0` key
  condition excludes the snapshot item; when a page closes early (large items), the reader keeps
  paging until it has collected `n` items or the stream runs out. PostgreSQL: `ORDER BY sequence
  DESC LIMIT n`. The in-memory backend walks its stream backwards.
- A `limit` below 1 is a programmer error returned before any request, in every backend.

### Reserved Namespaces (ADR-0004 C8)

- The partition prefixes `SYSTEM#` and `command-outcome#` are reserved for engine-managed rows
  (matching is a case-sensitive byte prefix on the entity ID).
- Every entity-addressed write path refuses a reserved entity ID before any request, with a typed
  `evt.ReservedEntityIdError`: `Commit` (one reserved stream fails a multi-stream commit whole),
  `CommitWithSnapshot`, `CommitStream`, out-of-band snapshot writes (`dynamo.PutSnapshot`), and
  `CompactBelow` (which writes no new rows but is refused all the same).
- Enumeration (`StreamAllEvents`/`StreamEntities`), entity heads (`StreamEntityHeads`), and rebuild
  scans (`StreamEntitiesFromSnapshots`) skip every reserved partition.
- Entity IDs were never restricted before, so a pre-existing stream such as `SYSTEM#account` would
  become silently invisible after upgrading. `evt-doctor` reports pre-existing streams in reserved
  namespaces — on DynamoDB by scanning the table (exempting engine-written rows: the feature
  marker, the rebuild fence, and command outcomes), on PostgreSQL with the two spec queries against
  the configured events and snapshots tables — and the upgrade note requires renaming (migrating)
  such streams before enabling the v0.9 features.

### Feature Marker (ADR-0004 C9)

- A store declares the extensions it uses on the reserved `SYSTEM#FEATURES` marker row (storage
  spec, Feature Marker): `version` (bumped by exactly one per declaration transaction), `features`
  (JSON object, feature name → stored-format version, keys sorted), and two sorted, deduplicated JSON-array sets,
  `historyDependent` and `retention`, each omitted when empty. On PostgreSQL the row lives in the
  derived `{events}__evt_features` table and holds the same JSON text.
- Any writer that does not support a declared `(name, formatVersion)` pair refuses to write, with a
  typed `evt.UnsupportedFeatureError`.
- Every DynamoDB commit, including each `CommitStream` batch, carries a mandatory `ConditionCheck` on the marker
  (`attribute_not_exists(pk) OR version = :v`; a store that never declared features validates
  against `attribute_not_exists(pk)`), ordered first in the transaction. A writer validates the
  feature set once, at first write, and caches the version; a declaration bumps the version in the
  same conditional write that updates the feature set, so every writer cached at the old version
  fails, re-validates, and retries (typed via `evt.ErrFeatureManifestChanged`, classified
  transient); a re-validation that finds an unsupported pair refuses outright.
- Enforcement is unconditional. Default repositories support no extensions;
  `WithFeatureMarker` customizes the supported set, never disables the check. PostgreSQL holds
  the shared feature advisory lock across validation and writes in the same transaction.
  Unquoted table names are lowercase-canonicalized, including the feature-lock key.
- Declaration invalidates cached writer validation; declaring a feature does not imply support.
  `PutSnapshot` obeys the same refusal atomically: a DynamoDB marker check and snapshot put in
  one transaction, or PostgreSQL validation under the shared feature lock in the write transaction.
- `CompactBelow` reads `historyDependent` (strongly consistent on DynamoDB, regardless of
  `WithConsistentRead`) and refuses an entity type in the set with a typed
  `evt.HistoryDependentProjectionError` before deleting anything — in every process that opens the
  log, whether or not it registered the projector. A table with no marker row declares nothing and
  compaction proceeds. PostgreSQL retains the shared feature lock through all deletes; DynamoDB
  uses transactional batches of at most 99 deletes plus a check on the observed marker version
  (or absence). A changed marker is re-read and re-validated before retrying a delete batch.
  A completed history-dependent declaration therefore fences in-flight compaction.

### GetSnapshot

- Returns `nil, nil` if not found
- Single item lookup by `pk` only

### StreamAllEvents

- Returns channel of `result.Result[[]SerializedEvent]`
- Takes a backend-neutral `evt.StreamFilter`; a zero filter streams every event, a non-empty
  `EntityType` narrows to that type. The core contract carries no DynamoDB types.
- DynamoDB compiles the filter into a Scan `FilterExpression`; the in-memory repository applies it
  client-side. Skips items with the inline-snapshot marker (`sk = 0`). Uses scan pagination.
- Callers needing a raw DynamoDB filter beyond entity type historically type-asserted the
  repository to `dynamo.ExpressionStreamer` and called `StreamAllEventsByExpression`. That
  extension is deprecated in v0.9.0 (K7) and removed in the next minor — new code narrows with
  `evt.StreamFilter` (entity type) and filters the rest client-side, or scans directly with the
  AWS SDK.

### StreamEntities

- Takes a backend-neutral `evt.StreamFilter` with the same semantics as `StreamAllEvents`
- Yields complete entities after all their events are processed
- Skips `sequence = 0` events (snapshot markers)
- Errors during `applyEvent` are yielded to channel, then continues
- DynamoDB raw-expression variant: `dynamo.ExpressionStreamer.StreamEntitiesByExpression`
  (deprecated in v0.9.0, K7 — use `evt.StreamFilter`)

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

## Backend Storage Contract

The `evt.Repository` interface is backend-neutral: it carries no DynamoDB (or other vendor) types,
so the same contract is satisfied by the in-memory backend, DynamoDB, and PostgreSQL (`evt/postgres`).
Any backend that implements `evt.Repository` MUST uphold the following invariants. They are
exercised by the backend-neutral suite in `conformance` (`conformance.RunRepositorySuite`); each
backend wires that suite and declares the optional guarantees it provides via `conformance.SuiteOptions`.
DynamoDB and PostgreSQL both run it with `SupportsSnapshots` and `EnforcesOptimisticConcurrency` on.

- **Per-entity sequence uniqueness** — for a given entity, each `EventSequence` is written at most
  once. Sequences start at 1 and increase by 1 per committed event.
- **Optimistic concurrency on commit** — a commit that reuses an already-committed `(EntityID,
EventSequence)` is rejected, not silently overwritten. DynamoDB enforces this with a conditional
  write (`attribute_not_exists(sk)`); PostgreSQL with the `(entity_id, sequence)` primary key, whose
  unique violation surfaces as an `evt.ConflictError`, classified as transient so
  `policy.ExecuteWithRetry` retries it. (The in-memory backend is a permissive test
  double and does not enforce this — `SuiteOptions.EnforcesOptimisticConcurrency` is false for it.)
- **Stable event ordering** — `GetEvents` returns an entity's events in ascending sequence order,
  and `GetLatestEvents(lastSequence)` returns only events with `sequence > lastSequence`.
  `GetRecentEvents(limit)` returns at most `limit` events in descending sequence order.
- **Reserved namespaces** — every entity-addressed write path refuses a reserved entity ID
  (`SYSTEM#`, `command-outcome#`) with a typed `evt.ReservedEntityIdError` before any request, and
  enumeration, heads, and rebuild scans skip every reserved partition (ADR-0004 C8).
- **Read isolation by entity** — `GetEvents`/`GetLatestEvents`/`GetSnapshot` return only the
  requested entity's data; an unknown entity yields an empty result (no error).
- **Snapshot consistency** — a snapshot written by `CommitWithSnapshot` is read back intact by
  `GetSnapshot` (same `EntityType`, `EntityID`, `EventSequence`, and payload). `GetSnapshot` returns
  `nil, nil` when none exists. The snapshot's `EventSequence` is the authoritative floor after
  compaction (see ADR-0001), and every backend, including the in-memory one, rejects a commit at
  or below it. The suite checks this whenever `SupportsSnapshots` is on.
- **Event/view transaction behavior** — events and any projector-produced view writes in a single
  `SerializedResult` are committed atomically. On DynamoDB this is a `TransactWriteItems` call; on
  PostgreSQL it is a single SQL transaction; partial commits must not be observable.
- **Backend-neutral filtering** — `StreamAllEvents`/`StreamEntities` honor `evt.StreamFilter`
  (entity-type narrowing today). The legacy backend extension interface
  (`dynamo.ExpressionStreamer`) offered richer vendor-specific server-side filtering but is
  deprecated in v0.9.0 (K7) and removed in the next minor; richer filtering must not enter the
  core contract.

The PostgreSQL backend stores the log as an append-only `evt_events` table keyed by `(entity_id,
sequence)` and durable snapshots in a sibling `evt_snapshots` table keyed by `entity_id`. That
schema is owned by `postgres.Repository.EnsureSchema` (idempotent DDL) so it stays in lockstep with
the Go types that read and write it; the local Terraform stack under `infra/local-postgres`
provisions only the database. Compaction deletes event rows in `[1, throughSequence]` once a covering
snapshot exists and never touches the snapshot row, so it preserves the snapshot-as-floor invariant.

See `docs/adr/0002-backend-neutral-storage-contracts.md` for the rationale and the decision to keep
`result.Result` channels as the streaming shape.

## Cross-Language Compatibility

The Rust crate `photon-evt` (`crates/evt`) implements the same storage format natively. Go and Rust
services can append to and load the same streams in DynamoDB or PostgreSQL. See
[ADR 0003](docs/adr/0003-rust-implementation-and-cross-language-compatibility.md).

- **One format, two implementations.** DynamoDB items, PostgreSQL rows, serialized event, snapshot,
  and metadata JSON, SNS envelopes and message attributes, conditional-write expressions, and the
  snapshot policy are identical in both languages, byte for byte except for the differences below.
- **Fixtures enforce the contract.** `go test ./internal/compat -run TestFixtures -update` regenerates
  `compat/fixtures/` from the Go implementation. `go test ./...` and `cargo test --workspace` both
  fail when either language drifts from them.
- **Format changes regenerate fixtures and pass in both languages**, including the live
  cross-language run (`scripts/compat-integration.sh dynamo|postgres`), and keep older stored data
  readable through upcasters.
- **Documented differences**:
  - A JSON `null` event payload round-trips as `null` in Go and `""` in Rust. Stores never write
    one.
  - Floats are not normalized across languages, so cross-language payloads should use integers.
  - PostgreSQL `metadata` is `jsonb` and must be compared as JSON.

  The duplicate-command window (events after the latest snapshot) is the same in both languages.
  In v0.9 it is superseded by command outcomes, but on an upgraded log the window is the only
  guard for pre-v0.9 command IDs, which have no outcome rows — a pre-v0.9 duplicate beyond the
  latest snapshot is not detected (a documented upgrade limitation; no backfill).
