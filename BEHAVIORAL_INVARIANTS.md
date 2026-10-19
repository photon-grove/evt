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
- `sk` = -1 is reserved for the stream's **head item** (ADR-0007): `pk`, `sk`, `head` (the highest
  event sequence ever committed to the stream), and `ttl` for retained types only. It is written
  only by the commit transaction that writes the events it counts (and by the migration), it only
  increases, and it equals the stream's derived head (the larger of its highest event and its
  snapshot's `eventSeq`). No event or snapshot query, `latest(n)`, or compaction range returns it,
  and every scan, enumeration, heads read, and stream consumer skips `sk < 1`
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
- A snapshot never covers more than its stream's head item: a commit raises `head` to at least
  every snapshot it writes, and an out-of-band snapshot write (`PutSnapshot`) adds the head item to
  its transaction (`head >= :new`, or the head item absent for `:new = 0`) and refuses a snapshot
  ahead of the stream with `*StreamHeadDriftError`

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
[storage format](docs/spec/storage-format.md#views); the invariants are under
[Commit-Wide Projectors](#commit-wide-projectors-v09-c7).

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

### StreamHeadDriftError and StreamHeadsNotMigratedError (ADR-0007)

- `*StreamHeadDriftError` (`evt.ErrStreamHeadDrift`; Rust `Error::StreamHeadDrift`) carries the
  stream ID, the stored head (or absent), and the derived head. It means a stream's head item
  disagrees with its log: a writer appended without maintaining the head, events were deleted, or a
  snapshot is ahead of the head. It is **not** a conflict and not transient; fence-aware retry
  returns it at once, writes no outcome row, and never records a `stale-fence` rejection for it
- Failures rank: feature-marker check, rebuild-lock check, outcome put (`OutcomeConflictError`),
  `StreamHeadDriftError`, `SequenceGapError`, then any other conflict (a head update, an event put,
  a snapshot put, or a fence check, then projector conditions), identically on every backend. A
  failed head update or fence is classified by reading the stream (derive the head, read the head
  item, re-derive when the head is ahead); a head update that passed beside a failed event put is
  drift
- `*StreamHeadsNotMigratedError` (`evt.ErrStreamHeadsNotMigrated`; Rust
  `Error::StreamHeadsNotMigrated`) refuses a write to a store whose feature marker does not declare
  `stream-heads` while the store holds data: every write path that validates the marker (commits,
  including outcome-only and rejection commits, `PutSnapshot`, `CompactBelow`) returns it. A store
  with no data is declared by its first writer. It is not a conflict and not transient

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
- Every written stream adds one `Update` of its head item (`sk = -1`) to the commit, whether it
  creates the stream or appends to it, and whether or not it writes a snapshot. It counts toward
  the 100 items and is billed as a write
- The DynamoDB `CommitStream` (deprecated in v0.9.0, K1) is refused with
  `*dynamo.CommitStreamRefusedError`: its batches would write events with no head update

The v0.9 core reshape adds further transaction items — read-fence checks, the command-outcome row,
and the feature-marker and rebuild-lock checks — with a pinned item ordering and budget formula in
the [storage format](docs/spec/storage-format.md#writes). An over-budget commit fails with a typed
error before any write and is never split.

### Multi-Stream Commits (v0.9, C1)

- One `Repository.Commit` may append to several streams at once (`SerializedResult.Events` and
  `.Snapshots` for more than one entity ID), plus projector operations, in a single transaction:
  all streams land or none does
- The engine pins the write order bytewise by stream ID regardless of declaration order; each
  stream keeps its own ascending sequences, preceded by its own head update and followed by its
  snapshot put when it writes one
- Every event of the commit carries one `commitId` (see Metadata Structure)
- Each written stream continues exactly from its stored head: a first sequence that skips ahead
  is refused with `*SequenceGapError` / `Error::SequenceGap`, and on DynamoDB the head item is
  conditioned in the transaction (`head = first - 1`, or `attribute_not_exists(head)` for a stream
  that starts at 1), so nothing is read before the commit. The gap is told apart from a conflict
  only when that condition fails, by the failure-path probe
- Multi-stream transactions require a repository that declares `evt.TransactionalRepository`;
  `snapshots.Store.Transaction` refuses one that does not with `*TransactionsUnsupportedError`
  before dispatching anything
- Go PostgreSQL executes a commit's view operations through its `ViewWriter`, and refuses the
  operations of a transaction group built for another storage type with
  `*ProjectorOperationsUnsupportedError` before any write; it never drops them
- A commit that carries projector operations but writes no event is refused before any write
  (`ProjectorOnlyCommitError` / `Error::ProjectorWithoutEvents`): views are projections of
  committed events (ADR-0012). An empty command ID is treated as absent when deriving the commit ID
  in both languages
- The shared budget counts the whole commit — every stream's events, head update and snapshot,
  fence checks, projector operations, and the control checks — and a commit over any limit fails with
  `CommitTooLargeError` before any write, identically on every backend and in both languages

### Read Fences (v0.9, C2)

- A read-only stream observed during a transaction is fenced with one `ConditionCheck` on its head
  item (`head = :head`, or `attribute_not_exists(head)` for a stream the reader saw empty). The head
  only increases, so the append → snapshot → compact ABA cannot pass a stale fence: there is no
  snapshot companion
- A fence conflict fails the whole commit and surfaces as `*FenceConflictError` carrying the
  fenced stream ID, its fenced head, and the fence's provenance — never a transaction item
  index, which is internal and differs by backend. It unwraps to a conflict. An engine-filled
  fence conflict is classified transient (`policy.Classify`) and matches
  `evt.IsOptimisticConflict`, so fence-aware retry re-prepares it exactly like an event-put
  conflict; a caller-stamped one is a stale-fence rejection and is not retried
- Fences for entity types in the feature manifest's durable `retention` set are refused with
  `*RetentionConflictError` — the refusal consults the manifest, so it binds writers that
  configured no retention themselves
- A written stream's head update supersedes its own fence; no fence check is emitted for it

### Command Outcomes (v0.9, C3)

- A store with outcomes enabled requires a command ID on every command (a missing one is an
  `ErrInvalidArgument` returned before any read or write) of at most 512 UTF-8 bytes
  (`CommandIDTooLongError` / `Error::CommandIdTooLong`, before even the outcome key is built)
- The outcome row `command-outcome#<id>` (`sk = 1`; PostgreSQL `{events}__evt_command_outcomes`) is
  written once, in its final state, inside the commit transaction as its last action, conditioned
  on `attribute_not_exists(pk)`. It has no snapshot and no `ttl`, never compacts, and is the one
  engine-written row the reserved `command-outcome#` namespace exempts from the entity-write refusal
- Every attempt reads the outcome row first with a strongly consistent read (DynamoDB
  `ConsistentRead = true` whatever the repository's read-consistency setting): the same identity
  digest returns the stored outcome (events, response, rejection) without running the handler; a
  different digest is `IdempotencyKeyReuse` carrying the stored outcome, and is never stored. The
  stored digest is computed before execution from the command's identity alone (SHA-256 over the
  type, the sorted target streams the request names, the canonical payload JSON, and the
  principal) — the caller's intent, never the streams the command goes on to read or write
- Before any outcome is recorded — accepted, rejected, or stale-fence — the load-time duplicate
  window of the streams the command read is applied: a pre-v0.9 event that carries this command ID
  and was replayed since the latest snapshot is a duplicate (`DuplicateCommandError` /
  `Error::DuplicateCommand`), and no terminal outcome is written for a command that already ran.
  The window only sees events after the latest snapshot; a pre-v0.9 command ID folded into a
  snapshot or compacted away is not detected (the documented upgrade limitation, no backfill)
- A command may not name a reserved target stream (`SYSTEM#`, `command-outcome#`): the identity
  is refused with the typed reserved-ID error before the outcome row is read, so no outcome is read
  or recorded for a command aimed at engine-managed rows. An outcome read with an empty,
  non-UTF-8, or oversized command ID is the same typed error on every backend, never "absent"
- Go's error-returning `Execute`/`ExecuteWithFactory` keep their contract on an outcomes store: when
  the outcome is replayed (a duplicate, or a lost race settled by the winner's row) the supplied
  entity is restored and loaded again, so it reflects durable state. Rust's outcome methods return
  a `CommandOutcome`, not an aggregate
- A multi-stream command begins with the outcome pre-check (`BeginCommand` /
  `Transaction::begin_command`) before any read, append, or fence; a transaction whose pre-check
  returned a stored outcome is finished and accepts no further work
- The outcome-row check covers commands executed with outcomes enabled, including IDs folded into
  snapshots and compacted away; on an upgraded log the load-time event window remains the only
  guard for pre-v0.9 IDs, and it sees only those after the latest snapshot
- A lost outcome put is never a generic optimistic-concurrency conflict and is never resolved by
  re-preparing the command: it is `OutcomeConflict`, and it outranks every other failed item in the
  commit's classification, so the engine resolves it by re-reading the row. An ordinary conflict
  of an outcome-bearing commit also re-reads the row once, so a lost race with the same command
  returns the winner's outcome
- Only a genuinely indeterminate failure of an outcome-bearing commit is recovered — a timeout or
  cancelled context, a dispatch or connection failure, a truncated response, a 5xx or server fault,
  a connection dropped during PostgreSQL's `COMMIT` (Go `*AmbiguousWriteError`, Rust
  `Error::Indeterminate`). A definitive failure (validation, a missing table or relation,
  authorization, a constraint error other than the outcome-row conflict) is returned as is
- U+0000 in a command ID, a digest, any persisted metadata string (principal, causation, extension
  keys and values, ...), or a rejection's code, message, details strings, or details keys is
  `ErrInvalidArgument` / `Error::InvalidArgument` on every backend (PostgreSQL cannot store it),
  before any handler side effect or write
- The write set is not part of a command's identity: a command may read, fence, and append to any
  stream, named or not, because the streams it touches can depend on the state it reads and differ
  between attempts. The outcome's `events` list records what the committing attempt wrote, and a
  replay returns it without running the command
- Recovery polls the outcome row with bounded backoff (50 ms doubling to 1 s, 10 s by default) on a
  context detached from the caller's cancellation, for every indeterminate failure (including
  DynamoDB's `TransactionInProgressException`): a row that appears is resolved, and one still
  absent when the window ends is a retryable ambiguous write, never "not applied"
- A concurrent same-ID, same-digest loser returns only the winner's stored outcome, never its own
  handler error or local decision
- Outcome rows read from storage are validated (known status, digest, rejection/code consistency,
  well-formed event IDs, on a rejection too); a malformed row is a typed corruption error and
  is never replayed. A custom digest must be non-empty, checked before anything is read or run
- After an indeterminate commit failure (timeout, lost connection, server fault) the engine
  re-reads the row: present with the same digest returns the stored outcome, a different digest is
  `IdempotencyKeyReuse`, and an absent row after the window is a retryable `AmbiguousWrite`. The
  handler never re-runs during recovery, and the reads do not depend on the caller's cancelled
  context
- The response (raw bytes, stored as padded base64) and the encoded rejection are each at most
  65,536 bytes (`OutcomeTooLarge`, before any write); an empty response is stored as no response
- The outcome item counts in the commit budget (one action and its encoded size); an outcome-only
  commit is the feature-marker check plus the outcome put
- Stores without outcomes keep today's behavior, and `CommitStream` / bare `Store.Commit` refuse
  to carry or skip an outcome

### Rejections and the Accepted No-Op (v0.9, C4)

- A domain rejection is `Rejection { code, message, details }`, stored as the JSON
  `{"code":…,"message":…,"details":…}` (`details` an object or `null`) with the code repeated in
  the row's `code` attribute. Engine codes are `not-found`, `bad-command`, and `stale-fence`;
  application codes are free-form. A rejected outcome is terminal for its digest and a retried
  command replays it verbatim without running the handler
- Numbers in `details` are stored in the canonical text PostgreSQL's `jsonb` prints (no exponent,
  the literal's scale less its exponent, no sign on zero), so every backend returns the same number
  text and a first execution equals a replay; a number whose canonical text PostgreSQL's numeric
  type cannot hold is an invalid-argument error before any write, checked after U+0000 and before
  the size bound
- Only domain rejections are durable. Bad events, encoding errors, and storage failures are errors
  that write no outcome, so a retry after the fix starts clean. An oversized rejection fails with
  `OutcomeTooLarge` and writes nothing
- A command that produces zero events is an accepted outcome with no events (still storing any
  response), not an error; if it carries projector operations (the deprecated
  `CommandResult.Transaction`) it is refused with `ProjectorOnlyCommitError` and nothing is
  written. Outcome-only commits carry the feature-marker check and, for a
  multi-stream transaction, the fence checks of its reads; the single-aggregate shortcut has no
  fences to check
- A rejection may carry domain events that commit atomically with it. Go marks the handler or
  `Prepare` error with `evt.RejectWithEvents` (events from the returned `CommandResult`, or the ones
  staged with `Transaction.Append`; `Transaction.RejectWithEvents` for a transaction finished by
  hand); Rust implements `Aggregate::rejection_events` (single aggregate) or returns
  `Decision::reject_with_events` / `Transaction::reject_with_events`. The unmarked forms
  (`Reject`, `Decision::reject`) keep discarding staged events, and a rejection without events is
  byte-for-byte the plain rejection
- The events and the rejected row are one atomic commit under the rules of an accepted commit:
  the head condition, snapshot checks, read fences, reserved streams refused, projector
  operations only with an event behind them, the budget counting the outcome item. The rejected
  row lists the event IDs it wrote (same order as an accepted row), a conflict writes neither, and
  no response is stored with a rejection
- A replay of a rejection with events returns the stored rejection and event IDs, runs nothing,
  and never commits the events again. A conflict re-prepares the command (it may now decide
  differently, and then writes no failure event); exhausted attempts persist the `stale-fence`
  rejection without the caller's events, and so does a failed caller-stamped fence
- A failed caller-stamped fence is persisted as a `stale-fence` rejection by an outcome-only
  transaction that omits the failed fences (the old ID stays terminal; the remedy is a new command
  ID). A single engine-filled fence conflict or event, floor, or snapshot conflict is never a
  rejection; it re-prepares the command, and only the exhaustion of the attempts records one
  (see Fence-Aware Retry below)

### Fence-Aware Retry (v0.9, C5)

- On a store with outcomes a command runs under a bounded policy (default 5 attempts in total,
  25 ms doubling to 1 s, jittered to `[delay/2, delay]`; a value below 1 attempt means 1; unset
  delay fields take the defaults, and waiting is switched off only explicitly with
  `ImmediateRetryPolicy` / `RetryPolicy::immediate`). An optional deadline stops new attempts
  once it has elapsed and is treated as exhausted attempts. Worst case is the attempts, about
  0.4 s of waits with the defaults, and a recovery window per attempt for ambiguous writes. Go:
  `snapshots.RetryPolicy` and `Store.WithRetry`; Rust: `RetryPolicy` and
  `OutcomeConfig::with_retry`. Both implementations assert the defaults against
  `compat/fixtures/outcomes.json`. There is no wait after the last attempt, and a Go wait ends with
  the context's error
- A failure is classified by the transaction item that failed. An optimistic-concurrency conflict
  at commit time (head update, event put, snapshot condition, engine-filled fence,
  DynamoDB `TransactionConflict`) re-prepares the command: the outcome row is read again, the
  aggregates are loaded again, and the handler or prepare step runs again from scratch. Throttling,
  an ambiguous write whose row never appeared, and a load that found a sequence gap are retried
  after a backoff too, but are transient, not conflicts: they never become a rejection. Everything
  else is returned at once, including an error a multi-stream prepare step makes up itself (only a
  failed `Read` it passes on is a load failure) and, in both languages, a command ID, digest, and
  targets that are validated and computed once before the first attempt
- A failed caller-stamped fence fails fast: the engine records the `stale-fence` rejection
  immediately, without re-preparing or waiting, because it cannot know the caller's decision still
  holds. A caller-stamped fence that passed does not turn another item's conflict into a rejection
- Exhausted attempts on a conflict persist a `stale-fence` rejection (message `the state this
  command was decided on kept changing; retries were exhausted`, details `{"attempts": N}`) as the
  command's outcome through the outcome-only transaction, so a replay of the command ID returns
  the same rejection and the remedy is a new command ID. Exhausted attempts on a transient
  non-conflict return the error and record nothing
- Only the attempt that commits writes anything, and a stored outcome found by any attempt's
  pre-check ends the loop. The handler or prepare step may run up to `MaxAttempts` times for one
  command and must be safe to repeat; a handler's own error, including a domain `ConflictError`, is
  never retried
- The identity digest is computed once, before the first attempt, and never recomputed from the
  live command or metadata. Everything that distinguishes one request from another must be in the
  targets or the payload: two requests with the same ID, type, targets, payload, and principal are
  the same command (a transfer whose destination comes only from the decision step would replay
  the first destination's outcome)
- The write set may differ between attempts without changing the command: identity is the request's
  intent, computed once before any attempt, so re-preparation, a client retry after the command
  landed, and recovery all digest to the same value (never `IdempotencyKeyReuse`)
- Go resets the caller's entity before each re-preparation (exported fields from a deep copy,
  unexported fields shallowly, so state `Apply` changes belongs in exported fields) and attempts an
  entity it cannot reset (not a pointer to a struct) once; the command value is reused by every
  attempt and must not be mutated by a handler. After a replayed or exhausted result the entity is
  reloaded so it reflects durable state. Rust loads a fresh aggregate and clones the command per
  attempt
- `policy.ExecuteWithRetry` (deprecated, K6) and Rust's `execute_with_retry` (a store without
  outcomes) remain blind whole-command retry for code that does not use outcomes

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
  C9) it also validates all declared feature/version pairs against the writer's supported set,
  refusing unsupported features before deletion. It reads the durable `historyDependent` set and
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

## Commit-Wide Projectors (v0.9, C7)

A `CommitProjector` is a pure function over one commit; its view operations commit in the same
transaction as the events. The byte-level rules are in the
[storage format](docs/spec/storage-format.md#views).

**Contract**

- It runs once per commit that writes at least one event, after every stream is staged and its
  snapshot decided, over an `evt.CommitView`: the commit ID, and every written stream ascending by
  entity-ID bytes with its entity type, this commit's events ascending by sequence, and its state
  after the commit. Streams that were only read (fenced) are not in the view.
- A command that writes no event (a plain rejection, an accepted no-op, an outcome-only commit)
  runs no projector; a rejection that carries events projects them like an accepted command's. The
  view carries no outcome context.
- It runs again on every fence-aware re-preparation; the operations of a failed attempt are
  discarded with it. A projector's own error (`*ProjectorError`) fails the command before any
  write, records no outcome, and is not retried.
- **Determinism.** The same `CommitView` must produce the same operations in the same
  order, byte for byte: no clock, random source, environment, store reads, state kept between
  calls, or map-iteration order. Views are derived state, so a wipe-and-replay — `GroupCommits` of
  the stored events plus the state each stream had after that commit — must reproduce the rows. A
  row that records something that happened only outside the event log (a mutable decision, an
  external signal, a verdict) is a latent data-loss bug: event-source it first, then project it.

**Kinds, history, and ownership**

- `History()` names the entity types whose events the projector reads. None: current-state, given
  views without events. Otherwise history-dependent: given the events of those types only, and the
  store durably adds them to the manifest's `historyDependent` set before the first commit that runs
  it (`CompactBelow` then refuses them in every process). A repository that cannot declare
  (`evt.HistoryDeclarer`) refuses the commit (`HistoryDeclarationUnsupportedError`).
- Every operation of a history-dependent projector names an owner stream the commit wrote. A
  stream's sequence is the only commit order the log keeps (commit IDs are not causal), so writes
  to a row are ordered by its owner's sequence; `GroupCommits` returns an order every stream agrees
  with, and `ReplayViews` refuses a row two streams write (`SharedViewKeyError`).
- A repository must declare `evt.ViewWritingRepository` (Rust `Repository::writes_views`) or the
  store refuses a commit-wide projector (`ViewsUnsupportedError`); `CheckProjectorOperationsSupported`
  stays fail-closed for view operations.

**Operations**

- A put (`pk`, `sk`, `entityID`, `entityType`, `payload`) overwrites unless it is `IfAbsent`; a delete
  of an absent row succeeds unless it is `IfPresent`. `OnConflict(err)` names the error for a failed
  condition and is never stored. View items never carry `ttl`.
- Order: projector declaration order, each projector's operations in emission order, after the
  operations of any transaction group, after the marker check, before the written streams. On
  PostgreSQL the statements run after the events and before the snapshot upserts.
- Before any write, on every backend: `InvalidViewOperation` (empty `pk` or `sk`, text that is not
  UTF-8 or contains U+0000, an unknown kind, a condition the kind does not allow), then
  `ViewKeyTooLong` (`pk` over 2,048 or `sk` over 1,024 UTF-8 bytes), then `DuplicateViewKey` (a
  row an earlier operation of the commit already targets). Operations with no event behind them are
  `ProjectorOnlyCommit` / `ProjectorWithoutEvents` first. A repository with nowhere to write the
  operations refuses the commit with `ViewsNotConfigured`.
- Each operation is one action in the 100-action budget and is sized as DynamoDB sizes it (a put its
  item, a delete its key plus its condition expression); PostgreSQL sizes a statement by its SQL
  text, so its byte estimate is a lower bound while its action count is exact.

**Failure**

- A failed condition (DynamoDB `ConditionalCheckFailed`, a unique violation of an `IfAbsent`
  put or a delete of no row for an `IfPresent` delete on PostgreSQL, a failed in-memory check) is
  `*ProjectorOperationError` / `Error::ProjectorOperation`, naming the projector (name and
  declaration position) and the operation (index among its emitted operations, kind, `pk`, `sk`),
  with the reason and the operation's mapped error. DynamoDB `ValidationError` and
  `ItemCollectionSizeLimitExceeded` on a view item map the same way; `TransactionConflict` and
  throttling keep their transient classification.
- It is not an optimistic conflict and not transient: it is returned at once, never re-prepared,
  and records no outcome. It ranks last: the marker check, the outcome put, `StreamHeadDrift`, a
  gap, and any conflict on a written stream or a fence are reported first, so it surfaces only when
  nothing above it failed. The first failed operation in transaction order is the one named.
- Nothing from the commit is written: not its events, not its other rows.

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
- Builds `TransactWriteItem` with conditional expression `attribute_not_exists(sk)` per event put
- Starts each written stream's group with an `Update` of its head item (`sk = -1`):
  `attribute_not_exists(head)` when the stream starts at 1, `head = :prev` (the first sequence minus
  one) otherwise, so a commit cannot write at or below a snapshot's floor after compaction deleted
  those keys, and cannot skip a sequence. Nothing is read before the commit
- Includes any transaction groups from projectors

### CommitWithSnapshot

- Same as Commit, plus snapshot item
- Snapshot conditional for the first snapshot (sequence 1): `attribute_not_exists(seq)`
- Snapshot conditional for later snapshots: `seq = :seq`, where `:seq` is the new sequence minus
  one. Continuity and the floor are the head update's, so the snapshot put carries the sequence
  chain only

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
- DynamoDB: descending queries (`ScanIndexForward = false`, `Limit = min(n, 100)`) whose `sk > 0` key
  condition excludes the snapshot item; when a page closes early (large items), the reader keeps
  paging until it has collected `n` items or the stream runs out. PostgreSQL: `ORDER BY sequence
DESC LIMIT n`. The in-memory backend walks its stream backwards.
- A `limit` below 1 is a programmer error returned before any request, in every backend.
  Initial buffer capacity is bounded by a page or the available events, even for the largest limit.

### Reserved Namespaces (ADR-0004 C8)

- The partition prefixes `SYSTEM#` and `command-outcome#` are reserved for engine-managed rows
  (matching is a case-sensitive byte prefix on the entity ID).
- Every entity-addressed write path refuses a reserved entity ID before any request, with a typed
  `evt.ReservedEntityIdError`: `Commit` (one reserved stream fails a multi-stream commit whole),
  `CommitWithSnapshot`, `CommitStream`, out-of-band snapshot writes (`dynamo.PutSnapshot`), and
  `CompactBelow` (which writes no new rows but is refused all the same).
- Enumeration (`StreamAllEvents`/`StreamEntities`), entity heads (`StreamEntityHeads`), and rebuild
  scans (`StreamEntitiesFromSnapshots`) skip every reserved partition, including the deprecated
  `HeadStore` map and visitor APIs. `ReservedNamespacePrefixes()` returns a copy; callers cannot
  change the authoritative namespace checks.
- Entity IDs were never restricted before, so a pre-existing stream such as `SYSTEM#account` would
  disappear from enumeration after upgrading. Explicit event and snapshot reads by ID still
  return legacy reserved-prefix streams so migration tooling can read them. `evt-doctor` reports
  pre-existing streams in reserved namespaces — on DynamoDB by scanning the table (exempting engine-written rows: the feature
  marker, the rebuild fence, and command outcomes), on PostgreSQL with the two spec queries against
  the configured events and snapshots tables — and the upgrade note requires renaming (migrating)
  such streams before enabling the v0.9 features.
- `evt-doctor migrate-heads` (Go only) migrates a v0.8 store to per-stream head items and is the
  only code that writes head items outside a commit ([runbook](docs/migrating-to-stream-heads.md)):
  - It refuses without `-confirm-writers-stopped` (nothing is written, `-dry-run` aside), and
    refuses a store whose marker declares `journal` or any feature it does not model
    (`*UnsupportedFeatureError`), or that holds a non-engine stream in a reserved namespace
    (`*ReservedStreamsError`, before any write and again before declaring).
  - DynamoDB: strongly consistent scan accumulating per partition across pages; expired-but-
    undeleted items are ignored; reserved namespaces, a partition with nothing left, and a derived
    head of 0 are skipped; each head item is written `attribute_not_exists(head)` (never
    overwritten) with the largest `ttl` of the partition when every item has one; idempotent and
    resumable. It verifies after the backfill, immediately before declaring, and after declaring,
    and declares only after a clean verification (`*HeadsNotVerifiedError` otherwise).
  - PostgreSQL: backfill, verification, and declaration are one transaction under the exclusive
    feature lock (and a `SHARE` lock on the events and snapshots tables); any missing or unequal row
    aborts it and leaves the database as it was.
  - `-verify` is read-only; `-repair` raises, creates, and fixes a low `ttl` (each write conditioned
    on the observed head, and on the observed `ttl` when it changes one); only `-repair -allow-lower`
    lowers a head; none of them declares. A repair whose writes raced verifies again and fails if
    any stream still disagrees with its log.

### Feature Marker (ADR-0004 C9)

- Create markers with `repo.FeatureMarker(supported)`, binding reads and transactional checks to
  that repository's client and events table. `WithFeatureMarker` panics with
  `*evt.FeatureMarkerClientMismatchError` for a different client handle (even with the same table
  name), or `*evt.FeatureMarkerTableMismatchError` for another table. Writes return the typed
  mismatch error if the binding no longer matches; snapshot writes and compaction enforce the
  same rule.

  Rust markers are repository-owned rather than attachable: DynamoDB's table is immutable, and
  PostgreSQL `with_tables` rebinds the marker together with the events table and clears validation.

- A store declares the extensions it uses on the reserved `SYSTEM#FEATURES` marker row (storage
  spec, Feature Marker): `version` (bumped by exactly one per declaration transaction), `features`
  (JSON object, feature name → stored-format version, keys sorted), and two sorted, deduplicated JSON-array sets,
  `historyDependent` and `retention`, each omitted when empty. On PostgreSQL the row lives in the
  derived `{events}__evt_features` table and holds the same JSON text.
- Any writer that does not support a declared `(name, formatVersion)` pair refuses to write, with a
  typed `evt.UnsupportedFeatureError`.
- Every DynamoDB commit carries a mandatory `ConditionCheck` on the marker
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
- Validation and construction of a commit's marker condition use one locked cache snapshot;
  concurrent declaration cannot leave a commit with an unvalidated condition.
- PostgreSQL probes feature-table existence with `to_regclass` before reading a legacy log whose
  marker table is absent, so successful writes never continue inside an aborted transaction.
  Schema setup is serialized across Go and Rust by a shared advisory lock.
- Declaration invalidates cached writer validation; declaring a feature does not imply support.
  `PutSnapshot` obeys the same refusal atomically: a DynamoDB marker check and snapshot put in
  one transaction, or PostgreSQL validation under the shared feature lock in the write transaction.
- `CompactBelow` validates every declared feature/version pair against the writer's supported set
  and returns `evt.UnsupportedFeatureError` before deletion when any pair is unsupported. It also
  reads `historyDependent` (strongly consistent on DynamoDB, regardless of
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
- Never deletes the `sk = 0` snapshot or the stream's head item, and never moves the head, so a
  writer whose load is still current never conflicts with a compaction; `throughSequence < 1` is a
  no-op; idempotent
- Refuses a store that does not declare `stream-heads` with `*StreamHeadsNotMigratedError`
- Refuses unsupported feature declarations and history-dependent entity types from the durable
  feature marker, including declarations made by other repository handles
- DynamoDB: strongly consistent key-only range query + `TransactWriteItems` batches of at most
  99 deletes and one marker check; marker changes trigger revalidation before retrying
- Fencing is per batch, not a whole-operation lease: a history-dependent declaration racing
  multi-batch compaction can leave a partially compacted stream; committed deletes are not rolled
  back. A history-dependent rebuild reports `CompactionUncovered`. Declare history dependence
  before writing data of that entity type
- PostgreSQL holds the shared feature advisory lock through validation and all deletes in one
  transaction

### Delete (build-tagged `//go:build !prod`)

- Raw, snapshot-unsafe point-delete by `(pk, sk)`; for local/staging fixtures only. It also deletes
  the head item of every stream it names, or the stream could not be created again
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
EventSequence)` is rejected, not silently overwritten. DynamoDB enforces this with the head item's
  conditional update (`head = first - 1`, or `attribute_not_exists(head)`) and the event puts'
  `attribute_not_exists(sk)`; PostgreSQL with its head statement under the entity lock and the
  `(entity_id, sequence)` primary key, whose conflict surfaces as an `evt.ConflictError`, classified
  as transient so retry re-prepares it. A backend must report every such conflict so that `evt.IsOptimisticConflict`
  matches (`evt.ErrOptimisticConflict` or a `ConflictError`): fence-aware retry keys off it. (The in-memory backend is a permissive test
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
  In v0.9 it is superseded by command outcomes ([above](#command-outcomes-v09-c3)), but on an upgraded log the window is the only
  guard for pre-v0.9 command IDs, which have no outcome rows — a pre-v0.9 duplicate beyond the
  latest snapshot is not detected (a documented upgrade limitation; no backfill).
