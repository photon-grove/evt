# Storage Format

This is the storage contract shared by the Go `evt` packages and the Rust `photon-evt` crate. Both
implementations read and write the same DynamoDB items, PostgreSQL rows, serialized JSON, and
stream envelopes, so services in either language can share one event log.

The golden fixtures in [`compat/fixtures/`](../../compat/fixtures) are the executable form of this
document. The Go implementation generates them, and both test suites check against them. If this
document and the fixtures disagree, the fixtures are what the code enforces. Fix whichever is wrong.
See [`compat/README.md`](../../compat/README.md) and
[ADR 0003](../adr/0003-rust-implementation-and-cross-language-compatibility.md).

The v0.9 core reshape
([ADR 0004](../adr/0004-core-reshape-multi-stream-transactions-and-outcomes.md), capabilities
C1–C10) spans two kinds of change: the new sections [Read Fences](#read-fences),
[Command Outcomes](#command-outcomes), [Fence-Aware Retry](#fence-aware-retry),
[Reserved Namespaces](#reserved-namespaces), [Feature Marker](#feature-marker), and
[Rebuild Fence](#rebuild-fence), and deltas inside the existing [Writes](#writes) (C1),
[Metadata](#metadata) (C6), and [Views](#views) (C7). Every v0.9 paragraph is tagged `(v0.9, Cn)`
inline, so a reader implementing pre-v0.9 behavior follows exactly the paragraphs without a tag
and skips the new sections. All of it is normative for both implementations and lands with Go evt
v0.9.0 and `photon-evt` 0.2. No code lands before this document carries the byte-level shapes.

## Identifiers

| Name                    | Type   | Notes                                                                                  |
| ----------------------- | ------ | -------------------------------------------------------------------------------------- |
| entity ID               | string | One aggregate instance, one stream. The DynamoDB `pk` and PostgreSQL `entity_id`.      |
| entity type             | string | The aggregate type, such as `account`.                                                 |
| event sequence          | int64  | 1-based position in the stream. Contiguous: each commit continues from the last event. |
| event ID                | string | `{entityId}:{sequence}`, such as `acct-1:7`.                                           |
| event type              | string | The event kind, such as `account:deposited`.                                           |
| event version           | int64  | The payload schema version, starting at 1.                                             |
| snapshot sequence       | int64  | 1 for an entity's first snapshot, then incremented by one per snapshot.                |
| snapshot event sequence | int64  | The last event sequence folded into the snapshot.                                      |

## JSON Encoding

Every JSON document in this format is produced by Go's `encoding/json` (`json.Marshal`). The Rust
crate reproduces it byte for byte (`photon_evt::json`):

- No insignificant whitespace and no trailing newline.
- Object keys appear in struct declaration order. Map keys are sorted by their encoded string, so
  integer keys sort as text (`10` before `2`). Rust maps with non-string keys use
  `photon_evt::json::go_key_order` to match.
- `<`, `>`, and `&` are escaped as `\u003c`, `\u003e`, and `\u0026`; U+2028 and U+2029 as `\u2028`
  and `\u2029`. Other non-ASCII text is written as UTF-8. Control characters, `"`, and `\` use the
  standard JSON escapes.
- Byte fields (`payload` in serialized events and snapshots) are standard, padded base64.
- Floating-point numbers are **not** normalized: Go and Rust format some floats differently. Payloads
  that must be identical across languages should use integers (for example, cents).

Decoding is lenient in both languages: missing fields and `null` decode to the zero value, and
unknown fields are ignored.

### Payloads

An event payload is the JSON encoding of the domain event, and a snapshot payload is the JSON
encoding of the aggregate state. Payload bytes are stored verbatim: as a DynamoDB string, as
PostgreSQL `bytea`, or base64-encoded inside serialized JSON. Neither implementation re-encodes a
stored payload except through an upcaster.

## Serialized Event

The JSON form of one stored event. It is the `detail` of a stream envelope.

```json
{
  "id": "acct-1:1",
  "sequence": 1,
  "type": "account:opened",
  "version": 1,
  "entityId": "acct-1",
  "entityType": "account",
  "payload": "eyJvd25lciI6IkFkYSIsIm9wZW5lZEF0IjoiMjAyNi0wMS0wMlQwMzowNDowNS4xMjNaIn0=",
  "metadata": {
    "commandId": null,
    "trace": null,
    "origin": null,
    "address": null,
    "region": "us-west-2",
    "timestamp": "2026-10-12T00:00:00Z",
    "principalId": null,
    "causationId": null,
    "extensions": {},
    "commitId": "01JDXK7Q3W9B5N2M8P4R6TAVHY"
  }
}
```

Keys, in order: `id`, `sequence`, `type`, `version`, `entityId`, `entityType`, `payload` (base64),
`metadata` (an object carrying every key from [Metadata](#metadata) — the example shows the
defaults a command with none of the optional metadata produces: `principalId` and `causationId`
`null`, `extensions` `{}`; the event's `commandId` is `null`, so its engine-generated `commitId`
is a ULID).

Go's `evt.SerializedEvent` owns this JSON encoding: `encoding/json` uses the same canonical
codec as the public `evt.MarshalEvent` / `evt.UnmarshalEvent` helpers. Encode-time metadata
defaults apply to the event envelope, not to standalone `evt.Metadata`, which has no JSON methods
and can be embedded in application DTOs without hiding sibling fields.

## Metadata

Every key is always written; absent values are `null` (or `""` for `region` and `timestamp`).

```json
{
  "commandId": "cmd-0001",
  "trace": {
    "baggage": "tenant=example",
    "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    "tracestate": "vendor=value"
  },
  "origin": { "source": "billing-api", "endpoint": "POST /accounts" },
  "address": "203.0.113.7",
  "region": "us-west-2",
  "timestamp": "2026-10-12T00:00:00Z",
  "principalId": "user:42",
  "causationId": "cmd-0001",
  "extensions": { "requestSource": "cli" },
  "commitId": "cmd-0001"
}
```

| Key           | Type           | Notes                                                                                  |
| ------------- | -------------- | -------------------------------------------------------------------------------------- |
| `commandId`   | string or null | The command that produced the event. Used to reject duplicate commands.                |
| `trace`       | object or null | W3C trace-context carrier. Keys sorted.                                                |
| `origin`      | object or null | `{"source": ..., "endpoint": ...}`, both strings.                                      |
| `address`     | string or null | Optional caller address. An empty address is written as `null`.                        |
| `region`      | string         | The region the command ran in.                                                         |
| `timestamp`   | string         | RFC 3339 UTC with second precision: `2026-01-02T03:04:05Z`.                            |
| `principalId` | string or null | Who issued the command (the caller's identity), for audit and per-principal checks.    |
| `causationId` | string or null | The command that caused this command. Defaults to `commandId` when unset (see below).  |
| `extensions`  | object         | String-keyed application-specific metadata. Keys sorted. `{}` when unset or empty.     |
| `commitId`    | string         | The commit that wrote the event. Set by the engine; identical across the whole commit. |

The last four keys are new in v0.9 (ADR 0004, C6) and are appended after the existing keys, so
stored events remain readable without upcasters — decoding is lenient in both languages. They are
always written:

- `principalId` is the caller's identity as a string, `null` when the command carries no principal.
- `causationId` is written with its effective value: the explicitly set causation, otherwise the
  `commandId`, otherwise `null`. Two events written by the same command therefore carry the same
  `causationId` even when neither set it explicitly.
- `extensions` is always an object, `{}` when the command carries no extensions. Values are strings;
  keys are sorted by their encoded string, like every map.
- `commitId` identifies the commit that wrote the event: every event of one commit — across every
  stream a multi-stream commit touches — carries the same value, which is what lets a rebuild
  recover the commit's grouping ([Views](#views)). It is set by the engine, not the caller: the
  default is the command ID when the store records outcomes (outcomes require a command ID),
  otherwise a generated ULID, unique per commit.

A metadata object without the four keys (an event written before v0.9) decodes with `principalId`
and `causationId` `null`, `extensions` empty, and `commitId` empty; such events replay as
single-event commits ([Views](#views)).

## Serialized Snapshot

```json
{
  "entityType": "account",
  "entityID": "acct-1",
  "sequence": 2,
  "eventSequence": 10,
  "payload": "eyJvd25lciI6IkFkYSIsInN0YXR1cyI6Im9wZW4iLCJiYWxhbmNlQ2VudHMiOjEyMDB9"
}
```

Keys, in order: `entityType`, `entityID` (capital `ID`, unlike the event's `entityId`), `sequence`,
`eventSequence`, `payload` (base64).

## DynamoDB

One table holds every stream: a string partition key `pk` and a number sort key `sk`. Events use
`sk = 1, 2, 3, …`. The entity's single snapshot lives inline at `sk = 0`.

| Attribute    | Type | Event item                    | Snapshot item (`sk = 0`)        |
| ------------ | ---- | ----------------------------- | ------------------------------- |
| `pk`         | S    | entity ID                     | entity ID                       |
| `sk`         | N    | event sequence                | `0`                             |
| `type`       | S    | event type                    |                                 |
| `version`    | N    | event version                 |                                 |
| `entityType` | S    | entity type                   | entity type                     |
| `payload`    | S    | event payload JSON (raw text) | aggregate state JSON (raw text) |
| `metadata`   | S    | metadata JSON                 |                                 |
| `seq`        | N    |                               | snapshot sequence               |
| `eventSeq`   | N    |                               | snapshot event sequence         |
| `ttl`        | N    | optional, epoch seconds       | optional, epoch seconds         |

`ttl` is written only for entity types with a positive retention, on event items and on the
snapshot item alike. Its value is the commit time plus the retention, in whole Unix seconds,
rounded down after adding. View items never carry it. An event item with missing or empty
`metadata` is invalid. Readers ignore other attributes.

### Writes

A commit is one `TransactWriteItems` call, at most 100 actions. The actions appear in this order:

1. The feature-marker check (`SYSTEM#FEATURES`), present in every v0.9 commit
   ([Feature Marker](#feature-marker)).
2. The rebuild-lock check (`SYSTEM#REBUILD`), present in every v0.9 commit
   ([Rebuild Fence](#rebuild-fence)).
3. Projector operations, in projector declaration order; within one projector, in emission order.
4. One put per event. Streams are grouped by entity ID in ascending byte order, and each stream's
   events are in ascending sequence.
5. Per written stream, in the same order: either the snapshot put or the floor check on the
   stream's snapshot item.
6. The fence checks for each read-only fenced stream in the read set, in ascending entity-ID
   order: the slot check, then the snapshot companion ([Read Fences](#read-fences)). A stream that
   is both fenced and written contributes no fence checks.
7. The outcome row put, when the store records command outcomes
   ([Command Outcomes](#command-outcomes)).

Steps 6 and 7 apply only when the corresponding capability is enabled for the store. The
feature-marker and rebuild-lock checks (steps 1 and 2) are not optional: every commit in a v0.9
store carries both, and a store that never declares features still validates the marker against
`attribute_not_exists(pk)`. A store built without the v0.9 capabilities writes exactly the actions
the older paragraphs describe. The order is pinned so fixtures byte-match across implementations.

**Pre-v0.9 capacity:** the floor check counts toward the 100 actions and is billed as a write,
so a commit without a snapshot costs one more write unit than its events and projector puts,
and holds at most 99 of them. `CommitStream` adds one check per entity in each batch. This is
not the v0.9 capacity: the current Go C9 implementation reserves a marker check as well,
leaving 98 event/projector operations for a single-stream commit, with or without a snapshot
(the snapshot put replaces the floor check). The full v0.9 target below adds the C10 rebuild
check and leaves 97 for a basic single-stream commit with no read fences or outcome. C10 is
not implemented by the Go C9 change.

**Multi-stream commits (v0.9, C1).** A commit may append to any number of streams. Each written
stream keeps its existing protections — `attribute_not_exists(sk)` on every event put and the
snapshot floor — per stream, and the sequence-contiguity rule applies per written stream. Nothing
else about the item shapes changes.

The item budget is the same for the single-aggregate shortcut and a multi-stream commit:

```text
items = Σ over written streams (events_s + 1)     event puts + snapshot put or floor check
      + fenced read-only streams × 2              slot check + snapshot companion
      + 1                                         outcome put, when outcomes are enabled
      + 1                                         feature-marker check, always in v0.9 stores
      + 1                                         rebuild-lock check, always in v0.9 stores
      + projector puts and deletes
```

A commit whose budget exceeds 100 fails with a typed `CommitTooLarge` error **before any write**.
It is never split: splitting would break atomicity, because an error after an earlier chunk
commits would leave only part of the aggregate set updated.

The item count is not the only limit. `TransactWriteItems` also caps the whole request at
4,000,000 bytes in aggregate and every item at 400 KB, so a commit of individually valid items —
twenty 250 KB event items, say — would still be rejected by DynamoDB while PostgreSQL could
commit the same data. The preflight therefore covers bytes as well as items, and it runs in both
languages regardless of backend, so `CommitTooLarge` fires identically everywhere. The estimator
mirrors DynamoDB's item-size rules: an item's size is the sum over its attributes of the
attribute name's UTF-8 byte length plus the value's encoded byte length (strings as UTF-8,
numbers as their decimal text, binary as raw bytes), and the request's size is the sum of its
items' sizes. The preflight compares the estimates against 95% of the backend limits —
**3,800,000 bytes** in aggregate and **380,000 bytes** for any single item, a 5% safety margin
for key-encoding and rounding differences. It covers every item the transaction carries: the
event puts, the snapshot and floor items, the projector operations, the fenced checks, the
outcome row, and the two control checks. A commit over either byte budget fails with the same
typed `CommitTooLarge` error before any write, and is never split.

| Write                                  | Condition                                             |
| -------------------------------------- | ----------------------------------------------------- |
| event put                              | `attribute_not_exists(sk)`                            |
| floor check (no snapshot put)          | `attribute_not_exists(eventSeq) OR eventSeq < :first` |
| first snapshot put (`:seq` would be 0) | `attribute_not_exists(seq)`                           |
| later snapshot put in a commit         | `seq = :seq AND eventSeq < :first`                    |
| snapshot put outside a commit          | `attribute_not_exists(eventSeq) OR eventSeq <= :new`  |
| fenced slot check (v0.9, C2)           | `attribute_not_exists(sk)` on `(pk, sk = head + 1)`   |
| fenced snapshot companion (v0.9, C2)   | `attribute_not_exists(sk) OR eventSeq <= :head`       |
| outcome row put (v0.9, C3)             | `attribute_not_exists(pk)`                            |
| feature-marker check (v0.9, C9)        | `attribute_not_exists(pk) OR version = :v`            |
| rebuild-lock check (v0.9, C10)         | `attribute_not_exists(pk)`                            |

`:first` is the stream's first event sequence in this commit — each written stream carries its
own floor — `:seq` is the new snapshot sequence minus one, and
`:new` is the new `eventSeq`. The first snapshot must have sequence 1, and every later one the stored sequence plus
one. A later snapshot put carries the floor check in its own condition. The floor check is a `ConditionCheck` on `(pk, sk = 0)`. When the
commit writes a snapshot, the floor condition is part of the snapshot put instead, because a
transaction cannot touch one item twice.

The floor exists because compaction deletes the events a snapshot covers, which removes the keys
`attribute_not_exists(sk)` relies on. Without it, a writer that loaded the stream before the covering
snapshot could recreate a compacted sequence.

`TransactWriteItems` reports one cancellation reason per item, indexed by the item's position in
the transaction, and both implementations classify a failed commit by that index (v0.9):

- A fence-check failure means another writer moved the stream
  ([Read Fences](#read-fences)). The classification keys off the failing item itself: a failed
  event put, floor check, or snapshot condition is an ordinary conflict regardless of the
  commit's fences, and only a failed caller-stamped fence yields a stale-fence rejection — per
  its provenance, per [Fence-Aware Retry](#fence-aware-retry).
- A feature-marker check failure means the feature set changed under the writer: the writer
  re-validates the feature set, then retries the commit ([Feature Marker](#feature-marker)).
- A rebuild-lock check failure means a rebuild holds the lock: the commit fails with a
  `RebuildInProgress` error, retryable after the lock is taken over or force-released
  ([Rebuild Fence](#rebuild-fence)).

The two control rows are store-level state, unrelated to the caller's decision: neither
failure is ever routed through the fence path — neither produces a stale-fence rejection, and
neither consumes fence-aware retry, even when the commit carries caller-stamped fences. On
PostgreSQL the same classification applies to the feature-marker `SELECT` and the rebuild-lock
assertion.

A failed outcome put is **not** one of these conflicts. It means another writer already recorded
an outcome for this command ID, and re-preparing the command as a conflict can never resolve it:
every attempt begins by reading the outcome row by command ID, and a commit whose outcome put
failed resolves through that row — the winner of the race has by then written it, so the next
attempt's read returns the stored outcome (same digest) or `IdempotencyKeyReuse` (different
digest). See [Command Outcomes](#command-outcomes).

Both implementations reject a commit whose sequences are not contiguous for each entity (each one
more than the last), or start below 1, before writing anything. Sequence 0 is the DynamoDB snapshot
slot. Rust also rejects an event whose ID is not `{entityId}:{sequence}`, because DynamoDB does not
store the ID and rebuilds it from the key on read. The floor check reads the first event and the snapshot
records the last, so both depend on that order, and loads reject a stream with a gap. Repositories
do not check that a commit's first sequence follows the stored head: DynamoDB cannot express it in
one transaction once compaction has deleted the previous event. Stores always continue from the head
they loaded.

A transaction cancelled with a `ConditionalCheckFailed` reason on an event put, the floor check, or
the snapshot put is an optimistic-concurrency conflict: another writer committed the same sequence,
moved the snapshot, or compacted past the commit.
The caller reloads and retries. A failed out-of-band snapshot condition is not an error. The newer
snapshot stays.

The languages report the other cancellations differently:

| Cancellation                                         | Go                                    | Rust                            |
| ---------------------------------------------------- | ------------------------------------- | ------------------------------- |
| event or snapshot condition                          | `dynamo.ErrSnapshotRaceCondition`     | `Error::Conflict`               |
| projector condition                                  | the transaction group's `HandleError` | `Error::Projector`, not retried |
| `TransactionConflict`                                | classified as transient               | `Error::Conflict`, retried      |
| `ThrottlingError` or `ProvisionedThroughputExceeded` | classified as transient               | `Error::Transient`, retried     |

In Rust, a commit that times out, loses its response, or fails with an internal server error is
`Error::Backend`, not retried: the transaction may have been applied, and running the command again
could write its events twice. Reload the entity to find out.

### Reads

- Events: query `pk = :id AND sk > :after`, ascending, skipping `sk = 0`. Reads are strongly
  consistent by default.
- Snapshot: get item `(pk, sk = 0)`.
- Entity heads (rebuilds and diagnostics only): scan `pk`, `sk`, and `eventSeq`, optionally
  filtered by `entityType`. An entity's head is the larger of its highest `sk` and its snapshot's
  `eventSeq`. Enumeration, heads, and rebuild scans skip the reserved namespaces
  ([Reserved Namespaces](#reserved-namespaces)).

### Views

`ViewProjector` (Go `dynamo.NewViewProjector`, Rust `photon_evt::dynamodb::ViewProjector`) writes
one item per entity to a separate views table, in the same transaction as the events:

| Attribute    | Type | Value                     |
| ------------ | ---- | ------------------------- |
| `pk`         | S    | `{entityType}#{entityId}` |
| `sk`         | S    | `VIEW`                    |
| `entityID`   | S    | entity ID                 |
| `entityType` | S    | entity type               |
| `payload`    | S    | aggregate state JSON      |

**Commit-wide view rows (v0.9, C7).** A commit-wide projector is a pure function over every event
in the commit, and its rows are no longer constrained to the derived `pk` or `sk = "VIEW"`: a
projector may put — or delete — rows at **any `pk`/`sk` it chooses**, including partitions that do
not name an entity, which is what makes cross-entity read models atomic with the commit. The
attribute set is unchanged:

| Attribute    | Type | Notes                                          |
| ------------ | ---- | ---------------------------------------------- |
| `pk`         | S    | chosen by the projector                        |
| `sk`         | S    | chosen by the projector                        |
| `entityID`   | S    | projector-supplied; readers never interpret it |
| `entityType` | S    | projector-supplied; readers never interpret it |
| `payload`    | S    | view state JSON                                |

`entityID` and `entityType` are always written. For a row that belongs to no single entity — a
projection over the whole commit — the projector repeats the commit's primary stream's type and
ID, or any stable label it chooses; both are informational. Puts are unconditional. A delete
operation is a transaction `Delete`; it carries no condition unless the projector supplies one,
and deleting an absent row succeeds. Two operations in one commit may not target the same
`(pk, sk)` item: the engine rejects such a commit with a typed error before any write, since
DynamoDB would otherwise fail the whole transaction with an unclassified `ValidationException`.
The chosen keys are themselves bounded **before any write** in both languages: a `pk` over
**2,048 UTF-8 bytes** or an `sk` over **1,024 UTF-8 bytes** — DynamoDB's key limits — fails
with a typed `ViewKeyTooLong` error, so a projector whose keys fit PostgreSQL but not DynamoDB
fails identically on both backends instead of committing on one and raising a validation error
on the other. Each projector operation carries the cancellation reason it
maps to, replacing the single per-group `HandleError(error, int)` callback. The per-entity
`VIEW` row above remains the special case "one entity's events"; the commit-wide form is the
general API. View items never carry `ttl`.

```json
{
  "pk": "username-claims",
  "sk": "handle#ada",
  "entityID": "acct-1",
  "entityType": "account",
  "payload": "{\"status\":\"claimed\",\"accountId\":\"acct-1\"}"
}
```

A commit-wide row at a `pk` outside any `{entityType}#{entityId}` pattern, written in the same
transaction as the events that produced it.

**Commit boundaries (v0.9, C6, C7).** The only durable record of which events belong to one
commit is the events' metadata: every event of a commit carries the same `commitId`
([Metadata](#metadata)), including the events a multi-stream commit writes to different streams.

**Current-state rows (v0.9, C7).** A commit-wide projector produces **current-state rows**: every
row it puts is a deterministic function of that commit's events plus the post-commit aggregate
states — the state of each written stream after the commit — never of earlier commits' rows or
event history. A row encodes what the projection says _now_, not what happened once, so the same
rows can be produced again without replaying the events that preceded the commit.

**Two kinds of commit-wide projector (v0.9, C7, C9).** Every commit-wide projector declares which
of two kinds it is, and the declaration is durable — the entity types of history-dependent
projectors are recorded in the feature manifest's `historyDependent` set
([Feature Marker](#feature-marker)), not in one process's registration:

- **Current-state projectors** (the default) compute rows from the current state of the
  aggregates they read. Their rebuild never replays history, and compaction is irrelevant to
  them: no event a compaction might delete is ever an input to their rebuilt rows.
- **History-dependent projectors** need the commit stream itself — an append-only feed, say.
  Their entity types are declared in the `historyDependent` set, which forbids compaction for
  them (below), and their rebuild replays every commit grouped by `commitId`.

**State entry point (v0.9, C7).** Every commit-wide projector implements a second entry point
alongside the per-commit one: the **state entry point**, called with a lookup by stream ID over
the final state of every aggregate in scope and returning the projector's **complete current row
set**. "Final state" is what a load at the head returns — the snapshot plus the events after it,
for each stream. Because the entry point sees _all_ the final states at once, a row that combines
several aggregates' states is derived from the complete, consistent input, not from one stream's
snapshot: this is what keeps cross-entity rows reproducible when the commits that produced them
have been compacted away.

**Rebuilding a current-state projector (v0.9, C7).** A wipe-and-rebuild never replays history
for one. It loads the final state of every aggregate of the entity types in scope, calls the
projector's state entry point once with the by-stream-ID lookup over all of them, deletes the
rows the projector owns, and writes the returned set. Ownership is declared at registration:
each commit-wide projector declares the key space it writes (for the per-entity `VIEW` row, the
`{entityType}#{entityId}` partitions of its entity types), which is the space the rebuild wipes
before writing. The result is a pure function of the final
states, so it is identical whether or not any prefix of the log was compacted: no seed over a
single stream's snapshot is involved, and a compacted multi-stream commit contributes exactly
what its streams' final states say. (A per-stream snapshot seed cannot do this — a row derived
from several aggregates is not a function of any one of their snapshots — which is why the entry
point takes the complete state.) Compaction never invalidates such a rebuild, and `CompactBelow`
never refuses these entity types.

**Rebuilding a history-dependent projector (v0.9, C7, C9).** A wipe-and-rebuild replays every
commit grouped by `commitId` — each group's events, ordered by stream ID bytewise then by
sequence (the same pinned order as the commit's transaction), form one `CommitView`, and the
projector runs once per group, exactly as it did at commit time. Nothing may have deleted the
history, so compaction is **forbidden for the entity types in the manifest's `historyDependent`
set**, and the prohibition is durable ([Feature Marker](#feature-marker)):

- `CompactBelow` reads the manifest (consistent read) and refuses an entity type in
  `historyDependent` with a typed `HistoryDependentProjection` error before deleting anything —
  in every process that opens the log, whether or not it registered the projector.
- A wipe-and-rebuild that finds such a stream already compacted fails with `CompactionUncovered`
  ([Compaction](#compaction)): the replay cannot stand in for the deleted commits.

Pre-v0.9 events, whose metadata has no `commitId`, replay as single-event commits — one
`CommitView` per event, in stream and sequence order — which reproduces the per-entity projector
behaviour an old log was written under.

## Read Fences

A read-only stream in a transaction is fenced with two `ConditionCheck` items (v0.9, C2). The
fence fails the commit when any writer has moved the stream past the head the reader saw:

1. **Slot check** on `(pk, sk = head + 1)`:

   ```text
   ConditionExpression: attribute_not_exists(sk)
   ```

   The slot at `head + 1` is free on the reader's view: the head is the larger of the stream's
   highest event sequence and its snapshot's `eventSeq`, so any writer that appends past the head
   takes the slot and fails the check.

2. **Snapshot companion** on `(pk, sk = 0)`:

   ```text
   ConditionExpression: attribute_not_exists(sk) OR eventSeq <= :head
   ```

   `:head` is the fenced head. The companion closes the compaction ABA: another writer could
   append `head + 1`, snapshot it, and compact it away, leaving the slot empty again — the slot
   check alone would then pass. Compaction only deletes events covered by a snapshot, so any
   compaction past the reader's head raises the snapshot's `eventSeq` above `:head` and fails the
   companion. On a stream with no snapshot item, `attribute_not_exists(sk)` is true and the
   companion passes.

The companion is **always included** — there is no interval-0 exception. A reader's own snapshot
interval does not control compaction: any store instance snapshots the stream with its own
interval configuration, a snapshot can be written out of band, and `CompactBelow` compacts
whatever a durable snapshot covers. So an interval-0 stream can still be compacted past the
reader's head, and the companion closes the ABA regardless. When a fenced stream is **written in
the same commit**, both checks are omitted: the event put at `head + 1` and that stream's floor
or snapshot condition supersede them.

**Retention conflicts with fences (v0.9, C2, C9).** Fences are refused outright for entity types
in the feature manifest's durable `retention` set ([Feature Marker](#feature-marker)), with a
typed `RetentionConflict` error returned before any write — in both languages, regardless of
backend. The set is **durable**, not the calling instance's configuration: an instance that
configured no retention still refuses fences for a type another instance declared, so the
refusal does not depend on every writer being configured alike. Retention deletes event items by
TTL — possibly the item at `head + 1`, possibly an event and its TTL-bearing snapshot together —
without a newer snapshot ever covering them, and the deletion reopens the fenced slot: both
checks can pass while the stream has in fact moved. The snapshot companion closes the compaction
ABA but not this one, because TTL deletion leaves no successor snapshot behind. A
retention-enabled stream is therefore an append-only log with an expiring tail, not a fenced
input; a caller that needs a fenced read uses a stream without retention.

A `ConditionalCheckFailed` on either check is an optimistic-concurrency conflict — another writer
moved the stream — classified and retried like an event-put conflict. Whether the engine may
re-prepare the command depends on fence provenance; see [Fence-Aware Retry](#fence-aware-retry).

Worked example. A transaction loads `claim#ada` at head 3 and fences it. The two items are:

```json
{
  "ConditionCheck": {
    "Key": { "pk": "claim#ada", "sk": 4 },
    "ConditionExpression": "attribute_not_exists(sk)"
  }
}
```

```json
{
  "ConditionCheck": {
    "Key": { "pk": "claim#ada", "sk": 0 },
    "ConditionExpression": "attribute_not_exists(sk) OR eventSeq <= :head",
    "ExpressionAttributeValues": { ":head": 3 }
  }
}
```

Fences are declared per read; a fence the caller fills from outside the engine is a
**caller-stamped fence**, and [Fence-Aware Retry](#fence-aware-retry) treats it differently from
an engine-filled one. Fence provenance is in-memory commit metadata, never stored.

## Command Outcomes

A store with outcomes enabled (v0.9, C3) records every command on a global, reserved
`command-outcome#<commandId>` stream — one row per command, written inside the commit transaction
alongside the events. The row is written **once, in its final state**, with a `Put` conditioned on
`attribute_not_exists(pk)`: a committed transaction never leaves a half-written outcome, and
there is no reserved-then-resolved intermediate state.

| Attribute   | Type | Outcome item (`pk = command-outcome#<id>`, `sk = 1`)                |
| ----------- | ---- | ------------------------------------------------------------------- |
| `pk`        | S    | `command-outcome#<commandId>`                                       |
| `sk`        | N    | `1`                                                                 |
| `status`    | S    | `accepted` or `rejected`, final at write time                       |
| `digest`    | S    | identity digest (below)                                             |
| `response`  | S    | standard padded base64 of the response bytes; absent when none      |
| `events`    | S    | JSON array of event IDs the commit wrote; absent when none          |
| `rejection` | S    | serialized rejection (below); present only when `status = rejected` |
| `code`      | S    | the rejection code; present only when `status = rejected`           |

The row has no snapshot, no `ttl`, and never compacts. `response` is base64 rather than raw text
so a response of arbitrary bytes round-trips exactly in both languages; the decoded bytes are the
stored response returned to a retried command. `events` is a JSON array of event IDs
(`{entityId}:{sequence}` strings) encoded with the shared JSON rules, in ascending order by
stream ID bytewise, then by sequence — the same order as the transaction's event puts.

The stored values are bounded **before any write**: the `response` bytes and the encoded
`rejection` — the `rejection` attribute's JSON, its `message` and `details` included — are each
limited to **65536 bytes**. A command that exceeds the bound fails with a typed
`OutcomeTooLarge` error returned before any write, in both languages, so a valid-looking command
behaves identically regardless of backend instead of exceeding DynamoDB's 400 KB item limit in
one and succeeding in the other. The bound keeps the outcome row — whose other attributes are
short — far under that item limit.

The command ID itself is bounded **before any write**: it must be at most **512 UTF-8 bytes**.
A longer ID fails with a typed `CommandIdTooLong` error before any write — before the outcome key
`command-outcome#<id>` is even constructed — in both languages and on both backends, so a
command that PostgreSQL would accept is not rejected by DynamoDB's 2,048-byte partition-key
limit (the outcome `pk` is the 16-byte prefix plus the ID) while its events commit in the other.
The check precedes the response and rejection bounds, and a rejected ID writes nothing.

An **accepted** outcome with events and a stored response:

```json
{
  "pk": "command-outcome#cmd-0001",
  "sk": 1,
  "status": "accepted",
  "digest": "08f63bf06ef7167f79dda7f5de9307e1ffb626de015ecbc1c1482d1e67e5abd6",
  "response": "eyJhY2NvdW50SWQiOiJhY2N0LTEiLCJiYWxhbmNlQ2VudHMiOjB9",
  "events": "[\"acct-1:1\"]"
}
```

An **accepted multi-stream** outcome. The array's order is pinned: `claim#ada` sorts before
`profile#u-1`, so `claim#ada:1` comes first regardless of the order the command declared its
streams in. The digest is the [multi-stream digest example](#identity-digest) with the principal
`user:42` added:

```json
{
  "pk": "command-outcome#cmd-0004",
  "sk": 1,
  "status": "accepted",
  "digest": "56d01f5b9788b60e12a686e40807c28d9dfde802d82faf46888adda0acee7050",
  "events": "[\"claim#ada:1\",\"profile#u-1:1\"]"
}
```

An **accepted no-op** — a command that produced zero events, or an outcome-only commit — is the
same row without `events` (and without `response` when the command returned none). It is a
durable, observable outcome, not an error. An **outcome-only commit** writes only the outcome row
plus its checks: no event puts, no snapshot or floor items.

```json
{
  "pk": "command-outcome#cmd-0003",
  "sk": 1,
  "status": "accepted",
  "digest": "d24c7d5ce955a76c936a83e6eca2c3bc277d936c88a76460ed2d221e285a215a"
}
```

No `events` and no `response`. The `digest` here is `account:ping` on `acct-1` with an absent
payload (`lp("")` in the canonical bytes) and principal `user:42`.

A **rejected** outcome:

```json
{
  "pk": "command-outcome#cmd-0002",
  "sk": 1,
  "status": "rejected",
  "digest": "21c04bf818ab67db244dc184ae98197928309a4b3a3533acba69dacf992b6d79",
  "rejection": "{\"code\":\"username-taken\",\"message\":\"handle ada is taken\",\"details\":{\"handle\":\"ada\"}}",
  "code": "username-taken"
}
```

**Every attempt reads the outcome row first.** Before preparing each attempt — the first one
included — the engine gets `(command-outcome#<id>, sk = 1)`:

- present with the **same identity digest** → return the stored outcome, including the stored
  response and events, and write nothing;
- present with a **different digest** → `IdempotencyKeyReuse`;
- absent → prepare and commit normally.

Every outcome read — the pre-check above, the race resolution after a failed outcome put, and the
ambiguous-write recovery below — is strongly consistent: on DynamoDB, `GetItem` with
`ConsistentRead = true`, regardless of the repository's read-consistency setting. An eventually
consistent read can return absent after the transaction applied, and the absent branch would then
re-execute a command that already committed. PostgreSQL's reads are transactionally consistent, so
the rule needs no flag there.

A commit whose outcome put fails its condition is resolved by the same rule on the next attempt,
never by re-preparing the command as a generic conflict ([Writes](#writes)): the losing writer's
next attempt reads the winner's row and returns whichever branch applies.

### Identity digest

The default digest is SHA-256 over the **canonical bytes** of the command's identity, computed
**before the command executes**. The input is what the command _is_ — its type, the stream IDs it
declares it will write, its payload, its principal — never the streams the executed commit ended
up writing. A retry or a recovery re-read therefore computes the same digest without re-running
the command, even when intervening commits would make a re-preparation derive a different write
set. A repeat with the same digest returns the stored outcome; a repeat with a different digest is
`IdempotencyKeyReuse`. The canonical bytes are:

```text
"evt-cmd-digest-v1" || 0x00
|| lp(command type)
|| uint32-le(stream count) || lp(stream ID) …    declared target stream IDs, sorted by byte order, deduplicated
|| lp(canonical payload JSON)
|| lp(principal)

lp(x)  = uint32 little-endian byte length of x, followed by x
```

- `command type` is the command's type string.
- The declared target stream IDs are the streams the command declares it will write events to —
  fixed before execution, deduplicated and sorted by byte order. The executed commit may write a
  subset of them (state can change under it); the digest is unchanged either way, which is what
  makes retries and recovery compare equal. Fenced (read-only) streams are not part of the
  digest; a fenced conflict is caught by the fence checks themselves. An outcome-only commit
  declares an empty list.
- `canonical payload JSON` is the command payload encoded by the shared JSON rules (the same
  encoder as every JSON document in this format). A command with no payload contributes the
  zero-length byte string (`lp("")`); a payload of JSON `null` encodes as the four bytes `null`.
  Because floats are not normalized across languages, a float-bearing payload can digest
  differently in Go and Rust and surface as `IdempotencyKeyReuse`; digest-stable payloads use
  integers, as under [JSON Encoding](#json-encoding).
- `principal` is the command metadata's `principalId` as a string, `""` when unset.
- The `evt-cmd-digest-v1` tag pins the encoding; a future change to the input bumps the tag, and
  old command IDs then digest differently, which surfaces as `IdempotencyKeyReuse` rather than a
  silently matched outcome.

A custom digester stores its own opaque string in `digest`; ambiguous-write recovery compares
digests for equality only.

Worked example. Command type `account:open`, target stream `acct-1`, payload
`{"owner":"Ada"}`, principal `user:42`:

```text
evt-cmd-digest-v1 00
0c000000 6163636f756e743a6f70656e                    "account:open"
01000000 06000000 616363742d31                       1 stream: "acct-1"
0f000000 7b226f776e6572223a22416461227d              '{"owner":"Ada"}'
07000000 757365723a3432                              "user:42"
```

78 bytes; SHA-256 is
`08f63bf06ef7167f79dda7f5de9307e1ffb626de015ecbc1c1482d1e67e5abd6` — the `digest` in the accepted
example above.

A multi-stream example: command type `claim-username`, target streams `claim#ada` and
`profile#u-1` (sorted byte order), payload `{"handle":"ada"}`, no principal:

```text
evt-cmd-digest-v1 00
0e000000 636c61696d2d757365726e616d65                "claim-username"
02000000 09000000 636c61696d23616461                 "claim#ada"
         0b000000 70726f66696c6523752d31             "profile#u-1"
10000000 7b2268616e646c65223a22616461227d            '{"handle":"ada"}'
00000000                                             ""
```

SHA-256 is `21c04bf818ab67db244dc184ae98197928309a4b3a3533acba69dacf992b6d79` — the `digest` in
the rejected example above.

A store with outcomes enabled **requires a command ID** on every command; a missing ID is a
programmer error returned before the commit — no outcome row, no events. Stores built without
outcomes keep the older behavior.

### Ambiguous-write recovery

After an indeterminate commit error (a timeout, a lost connection — the caller cannot know
whether the transaction applied), re-read the outcome row by command ID (get
`(command-outcome#<id>, sk = 1)`, strongly consistent as above):

- present with the **same identity digest** → the commit won; return the stored outcome,
  including the stored response and events.
- present with a **different digest** → `IdempotencyKeyReuse`.
- absent → the commit did not apply; safe to retry.

The digest is the pre-execution digest defined above, computed from the command's identity alone,
so recovery never re-prepares the command to compute it.

`IdempotencyKeyReuse` is returned as an error and is **not stored**: the existing outcome row
belongs to the first command that claimed the ID, and nothing overwrites it.

### Rejections

A domain rejection — the aggregate's own refusal: not-found, invalid, conflict-by-rule — is a
durable rejected outcome (v0.9, C4):

```text
Rejection { code, message, details }
```

The `rejection` attribute is the JSON encoding of this struct, keys in order `code`, `message`,
`details`. `code` and `message` are strings; `details` is a JSON object of application details,
or `null` when the rejection carries none. A rejected outcome is terminal for that digest: a
retried command replays the original rejection verbatim from the stored row.

Only domain rejections are durable. Internal failures — event encoding errors, bad events,
storage errors — are returned as errors and **write no outcome**, so a retry after the underlying
bug is fixed starts clean. A domain rejection whose encoded form exceeds the 65536-byte outcome
bound ([Command Outcomes](#command-outcomes)) writes no outcome either: the command fails with
`OutcomeTooLarge` before any write, and the caller retries once the rejection is smaller.
Engine-provided rejection codes are `not-found` and `bad-command`
(Go's former `NotFoundError`/`BadCommandError`), and the engine's own stale-fence rejection
([Fence-Aware Retry](#fence-aware-retry)). Domain error enums map onto rejection codes in each
language; codes are free-form strings beyond the engine-provided ones.

A `stale-fence` rejection is durable even though the condition it records is transient: exactly
one outcome per command ID is the contract, so the old ID stays terminal and its rejection replays
verbatim. It is persisted by the outcome-only transaction pinned in
[Fence-Aware Retry](#fence-aware-retry), which deliberately omits the failed fences. The remedy is
a new command ID, not a retry of the old one.

## Fence-Aware Retry

On conflict, a failed commit is classified **by the transaction item that failed** — DynamoDB
reports one cancellation reason per item, indexed by position, and PostgreSQL maps the statement
that raised the error — and only then by fence provenance (v0.9, C5):

- A failed **event put, floor check, or snapshot condition** is an ordinary optimistic-concurrency
  conflict, re-prepared like any other ([Writes](#writes)) — the command re-runs against a fresh
  load — **whether or not the commit carried caller-stamped fences**. A caller-stamped fence that
  still passed says nothing about this failure: the commit's own write set moved, the fences are
  beside the point, and persisting a rejection would be false. Re-preparation re-derives the
  fences from the fresh load in the usual way.
- A failed **caller-stamped fence check** (a head the caller supplied) is a **stale-fence
  rejection**: the engine cannot know the caller's decision is still valid, so it does not retry.
  It returns the rejection persisted as the command's outcome
  ([Command Outcomes](#command-outcomes)) with the reserved code `stale-fence`. Only a
  caller-stamped fence that itself failed produces one; engine-filled fences re-prepare
  internally and never surface it. Although a stale fence is transient by nature, the rejection
  is a durable, terminal outcome — exactly one outcome per command ID is the contract — so the
  remedy is not a retry of the same command: the caller re-reads the fenced streams, re-decides,
  and submits a **new command ID**. The old ID replays its stored `stale-fence` rejection
  verbatim, like any rejected outcome.
- A failed **engine-filled fence check** re-prepares internally: the engine loaded the fenced
  streams itself, so it can reload them and re-decide.

This replaces blind whole-command retry as the default path for transactional commands; the
policy packages remain for backend-level transient classification. Control-row failures — the
feature-marker and rebuild-lock checks — are classified separately ([Writes](#writes)) and never
reach this rule: a stale-fence rejection is produced only by a caller-stamped fence that failed,
never by a control row, an event put, or a snapshot condition.

The rejection is persisted by an **outcome-only transaction** that carries none of the failed
commit's other items: no fences (they are stale by definition), no event puts, no snapshot or
floor items, no projector operations. Like every attempt, it begins with the outcome pre-check
read. On DynamoDB its item list is exactly:

```text
1. ConditionCheck (SYSTEM#FEATURES, sk = 0)   the feature-marker check, as in every commit
2. ConditionCheck (SYSTEM#REBUILD, sk = 0)    the rebuild-lock check, as in every commit
3. Put (command-outcome#<id>, sk = 1)         the rejected outcome, attribute_not_exists(pk)
```

On PostgreSQL the same shape is the feature-marker `SELECT`, the rebuild-lock assertion, and the
outcome `INSERT`, inside one transaction. If one of its two control checks fails, the ordinary
control-row classification applies ([Writes](#writes)): a feature-marker failure re-validates and
retries the rejection write, and a rebuild-lock failure is `RebuildInProgress`, retried after the
rebuild — neither turns the pending rejection into something else. If the outcome put loses its
condition, the standard
resolution applies: the next read of the row returns the winner's outcome (same digest) or
`IdempotencyKeyReuse` (different digest) ([Command Outcomes](#command-outcomes)). A persisted
`stale-fence` row — command `account:rename` on `acct-1`, payload `{"name":"Grace"}`, principal
`user:42`, whose caller-stamped fence on `referral#r-3` at head 2 went stale before the commit:

```json
{
  "pk": "command-outcome#cmd-0005",
  "sk": 1,
  "status": "rejected",
  "digest": "ba4a6a859dcf103a00261515d7e309b31e8f07ca21732a4b7872b7fdebb92c1b",
  "rejection": "{\"code\":\"stale-fence\",\"message\":\"a caller-stamped fence went stale\",\"details\":null}",
  "code": "stale-fence"
}
```

The command never wrote; the engine persisted the rejection instead of retrying it.

The same rule covers the outcome put itself: an attempt whose outcome put lost a race is never
re-prepared as a generic conflict. The next attempt's outcome pre-check resolves it — the
winner's row returns the stored outcome (same digest) or `IdempotencyKeyReuse` (different digest)
([Command Outcomes](#command-outcomes)).

Fence provenance (engine-filled vs caller-stamped) is in-memory commit metadata, not stored.

## Reserved Namespaces

Two partition prefixes are reserved for engine-managed rows (v0.9, C8): `SYSTEM#` (the feature
marker, the rebuild fence) and `command-outcome#` (outcomes). Matching is a case-sensitive byte
prefix on `pk`.

- Enumeration, entity heads, and rebuild scans **skip** every partition whose `pk` begins with a
  reserved prefix. Engine-managed rows are not ordinary aggregates and never compact.
- Explicit reads by entity ID (`GetEvents`, `GetLatestEvents`, `GetRecentEvents` / `latest(n)`,
  and `GetSnapshot`) do **not** filter reserved prefixes. Legacy streams and their snapshots remain
  readable for migration; only enumeration, heads, and rebuild scans hide them.
- **Every entity-addressed write path refuses** an entity ID that begins with a reserved prefix,
  before any request, with a typed `ReservedEntityId` error: commits (multi-stream included —
  one reserved stream fails the whole commit), out-of-band snapshot writes (`PutSnapshot`),
  snapshot writes by a rebuild's seeding, and `CompactBelow` (which writes no new rows but is
  refused all the same, so the engine never touches a reserved stream). Commit-time-only refusal
  is not enough: an out-of-band snapshot put at `(SYSTEM#FEATURES, sk = 0)` passes its
  `attribute_not_exists(eventSeq) OR eventSeq <= :new` condition against an engine row — which
  has no `eventSeq` — and would overwrite the feature marker or the rebuild lock.
- Entity IDs were never restricted before, so a pre-existing stream such as `SYSTEM#account`
  would become silently invisible to enumeration, heads, and rebuild after upgrading. `evt-doctor`
  reports pre-existing streams whose IDs fall in a reserved namespace — on DynamoDB by scanning
  the table, on PostgreSQL with the queries below against the configured events and snapshots
  tables — and the upgrade note requires renaming (migrating) such streams before enabling the
  v0.9 features. Real stored data is never silently skipped.

  ```sql
  SELECT DISTINCT entity_id FROM evt_events
  WHERE entity_id LIKE 'SYSTEM#%' OR entity_id LIKE 'command-outcome#%';

  SELECT entity_id FROM evt_snapshots
  WHERE entity_id LIKE 'SYSTEM#%' OR entity_id LIKE 'command-outcome#%';
  ```

  `evt_events` and `evt_snapshots` stand for the configured table names. `evt-doctor` runs the
  same two queries and reports every ID they return; the upgrade procedure migrates those streams
  to non-reserved IDs before the v0.9 features are enabled.

### latest(n)

`latest(n)` is a bounded, newest-first read of a stream's last `n` events (descending), for
"recent activity" reads that must not replay a long stream. No item shape changes.

- DynamoDB: query `pk = :id AND sk > :zero`, `ScanIndexForward = false`, `Limit = min(n, 100)`. The `sk > 0`
  key condition excludes the snapshot item, so the result is the `n` highest event sequences,
  descending.
- PostgreSQL: `SELECT … FROM evt_events WHERE entity_id = $1 ORDER BY sequence DESC LIMIT $2`.

`n < 1` is a programmer error returned before any request, in both languages — nothing is read.
The result is at most `n` events: DynamoDB uses bounded pages and follows `LastEvaluatedKey`
until it has collected `n` items or the stream runs out, including when large items close a page
early. Initial buffer allocation is bounded by a page or the available events, not by an
arbitrarily large `n`. PostgreSQL's `LIMIT n` returns the same at-most-`n` window in one statement.

## Feature Marker

A feature marker is created from its repository and bound to both its client handle and events
table at construction. Marker reads and transactional checks must use that same client and table;
matching table names alone do not identify a physical log. Attaching a marker for another client
or table is a typed configuration error; subsequent writes (including snapshots and compaction)
must fail with a typed mismatch error if the repository no longer matches the binding. Rust
repositories own marker access directly rather than accepting a separately constructed marker.

A store declares the extensions it uses on a single `SYSTEM#FEATURES` marker row (v0.9, C9). Any
writer that does not support a declared feature **refuses to write** to the table.

| Attribute          | Type | Feature-marker item (`pk = SYSTEM#FEATURES`, `sk = 0`)                                        |
| ------------------ | ---- | --------------------------------------------------------------------------------------------- |
| `pk`               | S    | `SYSTEM#FEATURES`                                                                             |
| `sk`               | N    | `0`                                                                                           |
| `version`          | N    | bumped on every change to the manifest                                                        |
| `features`         | S    | JSON object mapping declared feature name → its stored-format version (a number), keys sorted |
| `historyDependent` | S    | JSON array of entity types whose projectors are history-dependent, sorted; absent when empty  |
| `retention`        | S    | JSON array of entity types with retention (TTL), sorted; absent when empty                    |

```json
{
  "pk": "SYSTEM#FEATURES",
  "sk": 0,
  "version": 3,
  "features": "{\"journal\":1,\"journal-compaction\":1}",
  "historyDependent": "[\"audit-feed\"]",
  "retention": "[\"session\"]"
}
```

The manifest has three parts. The `features` map declares extensions by name; each maps to the
stored-format version of that feature, a number starting at 1 — the features this spec defines
are `journal` and `journal-compaction` (the Rust-first extensions, ADR 0005), both at format
version 1. The `historyDependent` set declares the entity types whose commit-wide projectors are
history-dependent ([Views](#views)) — compaction is forbidden for them; the `retention` set
declares the entity types configured with retention (a positive `ttl`) — fences are refused for
them ([Read Fences](#read-fences)). Both sets are JSON arrays of entity-type names, sorted and deduplicated,
encoded with the shared JSON rules, stored as strings; an absent attribute is an empty set. Both
declarations are **durable, shared state**, not per-process registration: every writer and
maintenance tool that opens the log observes the same sets, which is what makes the compaction
and fence refusals hold across independently configured instances.

A writer may commit only if it supports **every declared `(name, formatVersion)` pair** — it
models the feature and reads and writes its stored format at exactly the declared version. A
writer that encounters a pair it does not support reads the feature set and refuses. The
per-feature version is what makes a format change detectable: bumping `journal` to `2` in the map
re-fails every older writer's revalidation, where a manifest of bare names would look unchanged
to a writer that already models `journal` at version 1.

The row is declared by the writer that first uses a feature, and updated — with a `version` bump
— by every later change to the manifest: adding a feature, bumping a feature's format version
when its stored format changes, or adding or removing an entity type from either set. Changing a
set is a declaration in the same protocol and re-fails every writer cached at the old `version`,
exactly as a feature change does.

Every commit on a v0.9 store includes one `ConditionCheck` on
`(SYSTEM#FEATURES, sk = 0)` — unconditional, not opt-in: the writer that most needs the refusal
is one that does not know a declared extension, and it cannot be relied on to have switched
enforcement on. The check is cheap when no features are declared (`attribute_not_exists(pk)`
passes), and it is a mandatory transaction item ([Writes](#writes)):

```text
ConditionExpression: attribute_not_exists(pk) OR version = :v
```

`:v` is the version the writer validated. A writer validates the feature set once, at first
write, and caches the version it validated: a table with no marker validates against
`attribute_not_exists(pk)` (cached as "no marker"), and a marker at version `v` validates against
`version = :v`. A declaration bumps `version` **in the same conditional write that updates the
feature set**, so every writer whose commits condition on the old version fails and re-validates;
at re-validation an unsupported writer reads the feature set, sees a `(name, formatVersion)` pair
it does not support, and refuses. The check costs one transaction item per commit
([Writes](#writes)). Standalone snapshot writes follow the same refusal protocol atomically:
DynamoDB includes the marker check and snapshot put in one transaction; PostgreSQL validates
under the shared feature advisory lock held through the snapshot-write transaction. Declaration
invalidates cached validation: declaring a feature is not proof that the declaring writer supports it.
Validation and per-commit condition construction use one locked cache snapshot, so concurrent
declarations cannot leave a commit with an unvalidated condition. PostgreSQL checks for a missing
feature relation with `to_regclass` before selecting it: a pre-marker log declares nothing, and
its transaction remains usable. Schema setup in both languages serializes DDL under
`pg_advisory_xact_lock(1702261762, hashtext(events_table))` to avoid catalog races on startup.

The declaration itself is a conditional write. Creating the row asserts `attribute_not_exists(pk)`
and writes **`version = 1`**; updating it asserts `version = :observed`, writes the merged feature
map with sorted keys, and sets `version = :observed + 1` — each declaration transaction adds
exactly one to `version`, no matter how many features it adds or bumps. (One row per feature with
distinct sort keys could not do this: adding a feature would neither change an already-validated
row nor fail a condition against another key.)

**Durable refusals read the manifest, not local configuration.** Both set-backed refusals consult
the row itself, so they hold for every process that opens the log:

- `CompactBelow` first validates every declared feature/version pair against the writer's
  supported set, refusing an unsupported pair with `UnsupportedFeature` before deleting anything.
  It also reads `historyDependent` — on DynamoDB with `ConsistentRead = true`, regardless
  of the repository's read-consistency setting, so the check cannot miss a just-written set — and
  refuses an entity whose type appears in the set with a typed `HistoryDependentProjection` error
  before deleting anything ([Views](#views)). A maintenance tool that never registered the
  projector refuses all the same. PostgreSQL holds the shared feature advisory lock across
  validation and all deletes in one transaction. DynamoDB deletes in transactions of at most
  99 events plus a check on the observed marker (`version = :observed`, or
  `attribute_not_exists(pk)` when absent); a failed check re-reads and re-validates before retrying.
  This is **per-batch fencing**, not a lease over the entire DynamoDB compaction. A
  history-dependent declaration racing an in-progress multi-batch compaction can stop later
  batches but cannot roll back the already committed prefix, leaving a partially compacted
  stream. A history-dependent rebuild then reports `CompactionUncovered`, rather than silently
  replaying incomplete history. History dependence must therefore be declared **before writing
  data of that entity type**. PostgreSQL retains its single-transaction guarantee under the
  shared feature advisory lock. On a table with no marker row there is nothing declared and
  compaction proceeds, as it always has; a pre-v0.9 writer that knows nothing of the manifest
  cannot be stopped, and the guarantee binds v0.9 writers as everywhere else in this section.
- A fenced read refuses an entity type in `retention` with a typed `RetentionConflict` error
  before any write ([Read Fences](#read-fences)) — keyed to the durable set, not to the writing
  instance's own retention configuration.
- On PostgreSQL the manifest row's `historyDependent` and `retention` columns hold the same JSON
  text, and the checks are ordinary reads inside the refusing operation's transaction — the
  strong-consistency distinction is DynamoDB-specific.

## Rebuild Fence

Rebuilds are fenced durably (v0.9, C10). The lock row and the generation counter are separate
items:

| Attribute        | Type | Rebuild-lock item (`pk = SYSTEM#REBUILD`, `sk = 0`) |
| ---------------- | ---- | --------------------------------------------------- |
| `pk`             | S    | `SYSTEM#REBUILD`                                    |
| `sk`             | N    | `0`                                                 |
| `owner`          | S    | owner token of the current lock holder              |
| `leaseExpiresAt` | N    | lease expiry, epoch seconds                         |

| Attribute       | Type | Generation item (`pk = SYSTEM#REBUILD#GENERATION`, `sk = 0`)                  |
| --------------- | ---- | ----------------------------------------------------------------------------- |
| `pk`            | S    | `SYSTEM#REBUILD#GENERATION`                                                   |
| `sk`            | N    | `0`                                                                           |
| `value`         | N    | monotonic counter, bumped per rebuild                                         |
| `forceReleased` | BOOL | optional; `true` after a force-release, cleared by the next completed release |

```json
{
  "pk": "SYSTEM#REBUILD",
  "sk": 0,
  "owner": "rebuild-4b1f0a2e",
  "leaseExpiresAt": 1760000000
}
```

```json
{
  "pk": "SYSTEM#REBUILD#GENERATION",
  "sk": 0,
  "value": 5
}
```

The lock row above and this generation are one active-rebuild state: the lock exists, so the
generation is odd.

The protocol. Acquire, takeover, release, and force-release are each **one `TransactWriteItems`**
that writes both items in a single transaction, so a crash can never leave the lock and the
generation disagreeing: the generation is **odd if and only if a rebuild is in progress** — odd
while a rebuild holds the lock, even when quiesced. The one exception is a **force-release**,
which deletes the lock but deliberately leaves the generation odd, recording `forceReleased`:
that odd value means "derived state untrustworthy", not "rebuild in progress", and readers treat
it the same way (below).

Every attribute name an expression touches is audited against DynamoDB's reserved-word list, which
is case-insensitive: `VALUE` and `OWNER` are reserved, so the expressions below reach them only
through the `#value` and `#owner` aliases, each shown with its `ExpressionAttributeNames` map. The
other attribute names in this spec's expressions — `pk`, `sk`, `seq`, `eventSeq`, `version`,
`leaseExpiresAt`, and `forceReleased` — are not reserved and appear literally, here and in the
write-condition table in [Writes](#writes).

- **Acquire** (a fresh lock — no lock row exists):

  ```text
  Put    (SYSTEM#REBUILD, sk = 0)  owner = :newOwner, leaseExpiresAt = :now + :lease
         ConditionExpression: attribute_not_exists(pk)
  Update (SYSTEM#REBUILD#GENERATION, sk = 0)
         SET #value = if_not_exists(#value, :zero) + :increment
         ConditionExpression: attribute_not_exists(#value) OR #value = :seenEven
                              OR (#value = :seenOdd AND forceReleased = :true)
         ExpressionAttributeNames: {"#value": "value"}
  ```

  The generation condition asserts the quiesced state the caller observed: absent-or-`0` before
  the first rebuild (`:seenEven = 0` covers the absent item), the stored even value after, or —
  after a force-release — an odd value carrying `forceReleased = true`, the one odd generation
  that can exist without a lock row. `:increment` is **one** when the observed generation was
  absent or even and **two** when it was odd and force-released: either way the generation ends
  odd, matching the lock row that now exists. An odd value without the flag never passes — that
  state cannot arise, because the only odd-generation-without-lock transition (force-release)
  sets the flag in the same transaction.

- **Takeover** (of an expired lease — one transaction, the same two items):

  ```text
  Put    (SYSTEM#REBUILD, sk = 0)  owner = :newOwner, leaseExpiresAt = :now + :lease
         ConditionExpression: leaseExpiresAt < :now AND #owner = :observedOwner
         ExpressionAttributeNames: {"#owner": "owner"}
  Update (SYSTEM#REBUILD#GENERATION, sk = 0)
         SET #value = #value + :two
         ConditionExpression: #value = :seenOdd
         ExpressionAttributeNames: {"#value": "value"}
  ```

  `:observedOwner` is the owner the caller observed when it decided to take over the expired
  lease. On takeover the lock row still exists, so the generation is necessarily odd: an
  acquire's even-value condition can never hold, and adding one would leave the generation even
  while a lock row exists. Takeover instead asserts the observed odd value and adds **two**: the
  generation stays odd — the lock is still held — but changes, so a reader that pinned the old
  epoch sees a new one and re-reads. A takeover racing another takeover fails one of the two
  conditions and retries from a fresh read.

- **Renew**: while it runs, the rebuild renews `leaseExpiresAt` with a conditional update
  asserting `#owner = :owner`, with `ExpressionAttributeNames` `{"#owner": "owner"}`.

- **Release** — one transaction:

  ```text
  Delete (SYSTEM#REBUILD, sk = 0)
         ConditionExpression: #owner = :me
         ExpressionAttributeNames: {"#owner": "owner"}
  Update (SYSTEM#REBUILD#GENERATION, sk = 0)
         SET #value = #value + :one REMOVE forceReleased
         ConditionExpression: #value = :seenOdd
         ExpressionAttributeNames: {"#value": "value"}
  ```

  A release restores the quiesced state — generation even — and clears `forceReleased`, so the
  next acquire sees a plain even value whether or not the rebuild started from a force-released
  generation.

- **Force-release** — one transaction, the operator's escape hatch for a lost or wedged worker:

  ```text
  Delete (SYSTEM#REBUILD, sk = 0)
         ConditionExpression: #owner = :requestedOwner
         ExpressionAttributeNames: {"#owner": "owner"}
  Update (SYSTEM#REBUILD#GENERATION, sk = 0)
         SET #value = #value + :two, forceReleased = :true
         ConditionExpression: #value = :seenOdd
         ExpressionAttributeNames: {"#value": "value"}
  ```

  Force-release **unblocks commits immediately** — the lock row is gone, so the commit-time check
  passes — but it deliberately leaves the generation **odd**: it adds **two**, not one, and sets
  `forceReleased`. Readers keep treating an odd generation as unavailable, so every view read
  stays fenced until a later rebuild acquires the lock, replays, and completes a release, whose
  even generation and cleared flag end the fence. The operator tradeoff is explicit: force-release
  trades read availability for write availability. Commits resume at once, but the force-released
  rebuild may have committed only some of its wipe and replay batches — its derived state is
  half-written and there is no replacement worker left to finish it — so readers keep retrying
  until a **successful** rebuild completes. A takeover cannot recover this state (there is no
  lock row to take over); only a fresh acquire from the force-released odd generation can.

- **Commit-time check**: every v0.9 commit includes a `ConditionCheck` asserting
  `attribute_not_exists(pk)` on the lock row — mandatory, like the feature-marker check, not
  opt-in per store — so a commit is refused whenever any rebuild holds the lock, **including an
  expired one**: until the lock is taken over or force-released, it blocks writes. That is
  deliberate: commits stay safe while any lock row exists, at the cost of waiting out a crashed
  rebuild. A commit that races a rebuild fails its check with `RebuildInProgress`
  ([Writes](#writes)), retryable after the lock is taken over or force-released — never as a
  fence conflict.

**Rebuild view writes (v0.9, C10).** While it replays, the rebuild worker writes its view rows in
`TransactWriteItems` batches of at most 99 view operations — the 100th item of every batch is a
`ConditionCheck` on the lock row:

```text
ConditionExpression: #owner = :me AND leaseExpiresAt > :now
ExpressionAttributeNames: {"#owner": "owner"}
```

The condition asserts transactionally, with every batch's writes, that the worker still holds an
unexpired lease. A worker that pauses past its lease and loses the lock to a takeover cannot
resume wiping or replaying views: its next batch fails the check and the worker aborts
immediately, so the new owner's release is never racing a stale worker's projection writes.
(Changing the generation protects readers; only this per-batch condition protects the views from
the _old_ worker.) On PostgreSQL the equivalent is per transaction: every rebuild write
transaction takes the lock row itself with `FOR UPDATE` and asserts ownership and lease inside
the same transaction that carries the view statements —

```sql
SELECT owner, lease_expires_at FROM evt_events__evt_rebuild_lock
WHERE id = 0 FOR UPDATE;  -- must return owner = $1 AND lease_expires_at > $2, else abort
```

(`evt_events__evt_rebuild_lock` stands for the configured derived name.) The row lock is held
for the length of the write transaction, so a takeover — which must `UPDATE` or `DELETE` that
row — blocks until the write transaction commits: the old worker either asserts the old owner
and commits first, or waits and then fails the assertion against the new owner and aborts. A
plain predicate read would hold no lock and leave exactly that window. A failed check aborts the
rebuild worker in both languages; nothing is ever written by a worker that no longer holds the
lock.

The rows live in the reserved `SYSTEM#` namespace, skipped by enumeration, heads, and rebuild
scans, and neither row has a snapshot or a `ttl`.

**Reader epoch check.** A reader that needs consistency reads the generation before and after its
read and treats an **odd or changed** value as "rebuild in progress or completed — retry": the
generation increments when a rebuild acquires the lock, changes again when a takeover replaces an
expired lease, and increments once more when it releases after replay completes, so state built
under a stale epoch can be rejected and re-read. A force-release leaves the generation odd and
flagged, so readers keep retrying through it — the fence ends only when a later rebuild
completes and releases (even). On DynamoDB both generation reads set
`ConsistentRead = true`, regardless of the repository's read-consistency setting: two eventually
consistent reads can return the same stale even value while a rebuild holds the lock, and the
reader would accept state produced mid-rebuild. PostgreSQL's reads are transactionally consistent,
so the rule needs no flag there.

## PostgreSQL

The schema is created by Go's `postgres.Repository.EnsureSchema` or Rust's
`PostgresRepository::ensure_schema`, which run the same idempotent DDL. Table names default to
`evt_events` and `evt_snapshots` and can be changed in both implementations. The v0.9 auxiliary
relations derive from the configured events table name through a **reserved infix** —
`{events}__evt_command_outcomes`, `{events}__evt_features`, `{events}__evt_rebuild_lock`,
`{events}__evt_rebuild_generation`, and the entity-type index `{events}__evt_entity_type_idx` —
so multiple event logs in one database never share engine rows: equal command IDs in unrelated
logs do not collide, and one log's feature marker or rebuild lock never gates another log
(`evt_events` yields `evt_events__evt_command_outcomes`, `evt_events__evt_features`,
`evt_events__evt_rebuild_lock`, `evt_events__evt_rebuild_generation`, and
`evt_events__evt_entity_type_idx`). The infix is what keeps two logs apart: a configured base
name of `orders` derives only names containing `orders__evt_`, and because **neither configured
table name may contain the infix `__evt_`** (case-insensitively; both implementations return a
validation error at construction otherwise), one log's events or snapshots table can never equal
another log's derived relation — the collision `orders` deriving `orders__evt_features` while a
second log configures `orders__evt_features` as its own events table is rejected outright. The
infix also renames the pre-v0.9 entity-type index, whose old `{events}_entity_type_idx` form
was collision-prone the same way; a schema upgraded from a pre-v0.9 release recreates the index
under the new name. Both implementations validate a derived name by the same rules as a
configured one. Names are interpolated unquoted, so both reject names that are not plain
identifiers or are PostgreSQL reserved keywords. Validated unquoted names are canonicalized to
lowercase before deriving engine advisory-lock keys, so differently cased handles share locks.
The snapshots table name is at most **58 bytes**, leaving room for its `_pkey` index.
The configured events table name must be at
most **34 bytes** — both implementations return a validation error at construction otherwise —
so every derived name fits PostgreSQL's 63-byte identifier limit without truncation. The longest
derived relation name, `{events}__evt_rebuild_generation_pkey`, adds 29 bytes to the base name
(`34 + 29 = 63`), ahead of `{events}__evt_rebuild_generation` (+24) and
`{events}__evt_entity_type_idx` (+21). Both also reject a pair of names where two of the
relations the schema creates (the six tables, the index, and each table's `{table}_pkey`) would
share a name after case folding, because `CREATE TABLE IF NOT EXISTS` would skip the second one.

```sql
CREATE TABLE IF NOT EXISTS evt_events (
    entity_id   text   NOT NULL,
    sequence    bigint NOT NULL,
    event_id    text   NOT NULL,
    event_type  text   NOT NULL,
    version     bigint NOT NULL,
    entity_type text   NOT NULL,
    payload     bytea  NOT NULL,
    metadata    jsonb  NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_id, sequence)
);
CREATE INDEX IF NOT EXISTS evt_events__evt_entity_type_idx ON evt_events (entity_type);

CREATE TABLE IF NOT EXISTS evt_snapshots (
    entity_id      text   PRIMARY KEY,
    entity_type    text   NOT NULL,
    sequence       bigint NOT NULL,
    event_sequence bigint NOT NULL,
    payload        bytea  NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS evt_events__evt_command_outcomes (
    command_id text        PRIMARY KEY,
    status     text        NOT NULL CHECK (status IN ('accepted', 'rejected')),
    digest     text        NOT NULL,
    response   bytea,
    events     jsonb       NOT NULL DEFAULT '[]'::jsonb,
    rejection  jsonb,
    code       text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'rejected' AND rejection IS NOT NULL AND code IS NOT NULL)
           OR (status = 'accepted' AND rejection IS NULL AND code IS NULL))
);

CREATE TABLE IF NOT EXISTS evt_events__evt_features (
    id                integer PRIMARY KEY,
    version           bigint NOT NULL,
    features          text   NOT NULL,
    history_dependent text   NOT NULL DEFAULT '[]',
    retention         text   NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS evt_events__evt_rebuild_lock (
    id               integer PRIMARY KEY,
    owner            text        NOT NULL,
    lease_expires_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS evt_events__evt_rebuild_generation (
    id             integer PRIMARY KEY,
    value          bigint   NOT NULL,
    force_released boolean  NOT NULL DEFAULT false
);
```

- A commit is one transaction: the entity lock, one insert per event, the floor check, projector
  statements, then the snapshot upsert. Projector statements run after the events, so a writer
  that lost a race fails on the event key (a conflict) before it reaches a view row. Commit-wide
  projector operations execute inside the commit transaction in **both** languages in v0.9:
  Go's `postgres.Repository` ignores projector transaction groups today, and executing them is a
  v0.9 behaviour change, implemented with the commit-wide projector (E2.6). A projector statement
  that the database rejects is a projector error (a lost
  connection is a backend error), and so is an integrity
  violation raised at `COMMIT`, which only a deferred constraint on a projector's table can cause.
- A unique violation (`23505`) on the event primary key is an optimistic-concurrency conflict. In
  Go, it and the floor-check conflict are `evt.ConflictError`s wrapped in a transient
  `policy.ClassifiedError`, so `policy.ExecuteWithRetry` retries them as it does on DynamoDB.
  In Rust, a serialization failure (`40001`) or deadlock (`40P01`), raised by any statement in
  the commit (projector, insert, floor check, or snapshot upsert), is `Error::Transient` and is
  retried.
- Commits and out-of-band snapshot writes first take a per-entity transaction lock,
  `SELECT pg_advisory_xact_lock(1702261760, hashtext($1))` with the entity ID. Both languages run
  this exact statement, so a snapshot write cannot raise the floor while a commit below it is in
  flight. Both languages deduplicate the computed key pairs and acquire the locks in ascending
  order of the pair `(1702261760, hashtext($entity))` — ascending `hashtext` value, ties broken
  by ascending entity ID bytes — so concurrent multi-stream commits cannot deadlock against each
  other: the acquisition order is a total order over the keys themselves, not over the IDs that
  hash to them.
- After the inserts, the commit checks the floor in the same transaction: a snapshot row with
  `event_sequence >= ` the commit's first sequence is a conflict.
- The snapshot upsert only replaces a snapshot whose `event_sequence` is lower or equal
  (`ON CONFLICT (entity_id) DO UPDATE … WHERE EXCLUDED.event_sequence >= evt_snapshots.event_sequence`).
  PostgreSQL does not check the snapshot sequence: a racing writer already fails on the event key.
- `metadata` is `jsonb`, which normalizes whitespace and key order. Compare it as JSON, not bytes.
  `payload` is `bytea` and keeps its exact bytes.
- Entity heads take `MAX` over event sequences and snapshot event sequences per entity.

**v0.9 rows.** The DynamoDB items of [Command Outcomes](#command-outcomes),
[Feature Marker](#feature-marker), and [Rebuild Fence](#rebuild-fence) are PostgreSQL rows:

- `{events}__evt_command_outcomes` holds one row per command (`command_id` is the command ID without
  the `command-outcome#` prefix). `digest` and `code` are text as on DynamoDB; `response` is
  `bytea`, the same raw bytes whose base64 the DynamoDB item stores; `events` is a `jsonb` array
  of event IDs — the same array, in the same stream-ID-then-sequence order — and `rejection` a
  `jsonb` object with keys `code`, `message`, `details`. Compare both as JSON, not bytes. The
  `CHECK` constraints mirror the once-in-final-state rule. The outcome insert is a plain `INSERT`
  into the commit transaction; a unique violation (`23505`) on the outcomes table means another
  writer recorded the outcome first — the commit is ambiguous, and the caller recovers by
  re-reading the row by command ID ([Ambiguous-write recovery](#ambiguous-write-recovery)). The
  outcome insert runs last, after the snapshot upserts.
- `{events}__evt_features` holds exactly one row, `id = 0`; `features` is the canonical JSON object
  text — feature name → format version — byte-identical to the DynamoDB `features` attribute, and
  `history_dependent` and `retention` hold the same sorted JSON-array text as the DynamoDB
  `historyDependent` and `retention` attributes. The
  commit-time check is a `SELECT` of
  `version` in the commit transaction — mandatory in every v0.9 commit, as on DynamoDB,
  including the no-marker case. A row whose `version` differs from the cached one, or a row that
  appeared after the writer validated an empty table, fails the check: the writer re-validates
  the feature set, then retries, as on DynamoDB
  ([Writes](#writes)). Declarations take the
  exclusive advisory lock below for their short read-modify-write; commit-time checks take the
  shared lock, so a declaration cannot slip between a writer's check and its commit.
- `{events}__evt_rebuild_lock` (`id = 0`) and `{events}__evt_rebuild_generation` (`id = 0`) mirror the lock
  and generation items; `lease_expires_at` is `timestamptz` rather than epoch seconds, and
  `force_released` mirrors the generation item's `forceReleased` flag.

**Advisory locks.** The per-entity commit lock stays `pg_advisory_xact_lock(1702261760,
hashtext($1))`, keyed by the entity ID alone: two event logs in one database that share an entity
ID merely serialize each other's commits on it, which is safe. The engine-row locks derive from
the configured events table name, so two logs never block each other's engine rows: class key
`1702261761`, with the second key `hashtext('{events}:features')` for the feature marker and
`hashtext('{events}:rebuild')` for the rebuild fence. (`{events}` is the configured events table
name.)

- A multi-stream commit takes the per-entity lock for **every stream in its read set** — written
  and fenced — in the order pinned above (ascending advisory key pair, ties by entity ID bytes,
  duplicate key pairs deduplicated since one lock covers every ID that hashes to it), then runs
  the fence assertions below, then the inserts. A concurrent writer blocks on the lock instead of
  slipping `head + 1` past a fenced read.
- **Fence assertions** (v0.9, C2) run inside the commit transaction after the locks:

  ```sql
  SELECT COALESCE(MAX(sequence), 0) FROM evt_events WHERE entity_id = $1;   -- must be <= :head
  SELECT event_sequence FROM evt_snapshots WHERE entity_id = $1;            -- must be <= :head
  ```

  Both assertions are upper bounds, not equality: compaction can shrink the visible event range
  below the reader's head (a snapshot whose `event_sequence` equals the head may have covered
  every event), and that is not movement past the head. A stream moved past the head always
  violates one conjunct — an appended event raises `MAX(sequence)` above `:head`, and a snapshot
  past the head raises `event_sequence` above it; an event appended and then compacted away is
  covered by such a snapshot. PostgreSQL cannot fence the absent slot at `head + 1` with row
  locks (`SELECT … FOR KEY SHARE` locks only rows that were actually returned), so the advisory
  lock plus these assertions are the fence.

- **Rebuild fence** (v0.9, C10): the durable `{events}__evt_rebuild_lock` **row** is what fences
  commits, exactly as the DynamoDB item does. After taking the shared advisory lock, every commit
  asserts the row is absent, in the same order as the DynamoDB transaction (the feature-marker
  `SELECT` first, then this assertion):

  ```sql
  SELECT 1 FROM evt_events__evt_rebuild_lock WHERE id = 0;  -- any row present, including an expired one, is a conflict
  ```

  A commit that finds a row fails its check with `RebuildInProgress` ([Writes](#writes)),
  retryable after the lock is taken over or force-released: a crashed rebuild keeps blocking
  commits, same as DynamoDB. The shared lock
  (`pg_advisory_xact_lock_shared(1702261761, hashtext('{events}:rebuild'))`, taken at commit
  start) makes the assertion safe without serializing commits against each other: acquire,
  takeover, and release take the exclusive lock
  (`pg_advisory_xact_lock(1702261761, hashtext('{events}:rebuild'))`) for their short
  transactions only, so no acquire or release can slip between a commit's assertion and its
  `COMMIT`. Acquire, takeover, release, and force-release are each one transaction with the same
  two statements as the DynamoDB transaction: the conditional lock insert, update, or delete
  (fresh acquire asserts absence — accepting the odd generation only when `force_released` is
  set, as on DynamoDB — takeover asserts `owner = :observedOwner AND lease_expires_at < :now`;
  release and force-release assert the owner) and the conditional generation bump
  (`attribute_not_exists(value) OR value = :seenEven OR (value = :seenOdd AND force_released)`
  to acquire, with the same `:increment` as on DynamoDB — one from an absent or even generation,
  two from a force-released odd one —
  `SET value = value + :two` against `value = :seenOdd` to take over,
  `SET value = value + :one REMOVE force_released` — cleared — against `value = :seenOdd` to
  release, and `SET value = value + :two, force_released = true` against `value = :seenOdd` to
  force-release, keeping the generation's parity and flag semantics exactly as pinned in
  [Rebuild Fence](#rebuild-fence)). A takeover moves odd to odd (changed), so readers pinned to
  the old epoch re-read; a force-release leaves the generation odd with the flag set, so commits
  resume while readers stay fenced until a later rebuild completes and releases. While it
  replays, the rebuild's view writes are transactions that each take the lock row `FOR UPDATE`
  and assert `owner = :me AND lease_expires_at > :now` in the same transaction, so a worker that
  lost the lock to a takeover blocks on it, then aborts instead of writing — the transactional
  equivalent of the DynamoDB per-batch `ConditionCheck`
  ([Rebuild Fence](#rebuild-fence)).

- **Feature marker**: declarations take
  `pg_advisory_xact_lock(1702261761, hashtext('{events}:features'))` — exclusive — around the
  read-modify-write of `{events}__evt_features`; commit-time checks take
  `pg_advisory_xact_lock_shared(1702261761, hashtext('{events}:features'))`, which is why the
  check-then-commit race above cannot happen: the commit holds the shared lock to `COMMIT`, and a
  declaration's exclusive lock waits for every commit holding the shared lock. Commits do not
  serialize against each other.

## Stream Envelope

The stream envelope is the cross-language wire format. Go's `stream.SNSPublisher` (deprecated in
v0.9.0, K5) wraps each committed event in an EventBridge-style envelope and publishes it to SNS;
a post-removal publisher emits the same envelope itself. Go decodes the envelope with
`projectors.StreamRecordFromEnvelope`; Rust encodes the same bytes
(`photon_evt::wire::envelope::encode`) and decodes both SNS messages and raw-delivery SQS bodies
(`decode`).

```json
{"version":"","id":"acct-1:1","detail-type":"account:opened","source":"evt-compat","account":"","time":"0001-01-01T00:00:00Z","region":"","resources":null,"detail":{...serialized event...}}
```

Message attributes, all strings:

| Attribute       | When                                              |
| --------------- | ------------------------------------------------- |
| `entityType`    | always                                            |
| `eventType`     | always                                            |
| `commandId`     | metadata has a non-empty command ID               |
| `traceparent`   | the trace carrier has a non-empty `traceparent`   |
| `correlationId` | the command ID if present, else the `traceparent` |

Decoding rejects an envelope whose event has no entity ID or event ID.

## Snapshot Policy

A store snapshots when a commit reaches or crosses the next multiple of the snapshot interval
(Go `evt.CalculateAdditionalEvents`, Rust `photon_evt::snapshot_events_due`):

```text
next = interval - (current_sequence mod interval)
snapshot if interval > 0 and events_in_commit >= next
```

- The snapshot always covers the whole commit: its `eventSequence` is the commit's last event.
- Its sequence is the previous snapshot sequence plus one (1 for the first).
- The default interval is 5 in both implementations. An interval of 0 disables snapshots.

For example, with interval 3, a 6-event commit at sequence 1 snapshots at event 7, and a 1-event
commit at sequence 7 does not snapshot because the next boundary is 9.

## Loading and Duplicate Commands

A store loads an entity from its snapshot plus events after the snapshot's `eventSequence`, or from
every event when there is no snapshot. An entity with no events loads at sequence 0.

Loaded events must continue the sequence without a gap: the first event after a snapshot is its
`eventSequence` plus one, and a stream without a snapshot starts at 1. A gap means another writer
snapshotted and compacted the stream between the snapshot read and the event read, or events
expired. Applying events across the gap would build the wrong state. Rust's `Store::load` reads the
stream again, up to three times, then returns `Error::Conflict`. Go's `snapshots.Store.LoadEntity`
returns a conflict error at once, because it has already modified the entity the caller passed in.
The error is a `policy.ClassifiedError` with class `transient`, so `policy.ExecuteWithRetry`
retries it with a fresh entity, and `evt.IsConflictErr` still matches it. A gap that does not heal,
such as a prefix that DynamoDB TTL expired under `WithRetention`, fails every load. Before this
check, such a stream loaded with the wrong state.

Like any read, a load can be one compaction behind. If another writer snapshots and compacts the
stream during the load, the event read can stop short without a gap: it is empty when the new
snapshot is exactly at the head, and a paginated DynamoDB query can return a contiguous first page
before the later events are deleted. The load then returns an older state. That state was real when
it was read, so this is ordinary staleness, not a wrong state. Neither language reads the snapshot
again to detect it, which would cost a read on every load. A commit from the stale load starts at or
below the new snapshot's `eventSequence`, so the floor check rejects it with a conflict, and the
retry policies reload and run the command again.

A command whose metadata carries a command ID already recorded by an event **replayed since the
latest snapshot** is a duplicate and writes nothing. Command IDs folded into a snapshot are no
longer checked, so the dedupe window is the events after the snapshot.

In v0.9 this window is superseded by [Command Outcomes](#command-outcomes): the durable
`command-outcome#` row checks every command ID globally, including IDs folded into snapshots. The
window remains as a cheap pre-check on load, and on an **upgraded log** it is what still catches
older duplicates: pre-v0.9 events carry command IDs but no outcome rows, so the load-time check
is retained for them.

That retention is a known upgrade limitation, documented here rather than papered over with a
backfill: no tool materializes outcome rows for pre-v0.9 commands. IDs already folded into a
snapshot at upgrade bypass the new global check immediately, and IDs still visible as events
after the latest snapshot keep only the load-time window — once those events fold into a snapshot
and compact, their IDs are no longer checked at all. A duplicate of a pre-v0.9 command beyond the
latest snapshot is therefore **not detected**. The once-per-command-ID guarantee applies to
commands executed after outcomes are enabled.

## Upcasting

Upcasters migrate stored events from older payload versions when they are read. They run in
declaration order, each once, and each sees the output of the ones before it. Stored events are
never rewritten. A failed upcast or decode fails the load.

## Compaction

`CompactBelow(entity, through)` (Go `evt.Compactor`, Rust `Repository::compact_below`):

- Deletes events with sequence in `[1, through]`, only when the stored snapshot covers them
  (`snapshot.eventSequence >= through`). Otherwise it deletes nothing and returns the
  compaction-uncovered error.
- Never deletes the snapshot. `through < 1` is a no-op. It is idempotent.
- On DynamoDB, the snapshot read and the key query are strongly consistent even when the repository
  is configured for eventually consistent reads, so compaction cannot miss recent events.
- A compacted stream must be loaded or rebuilt from its snapshot, never by replaying from sequence 1.
- A commit whose first event is at or below the snapshot's `eventSequence` is a conflict in both
  backends (see the floor checks above), so a writer that loaded before compaction cannot recreate
  compacted events.

See [ADR 0001](../adr/0001-event-compaction-and-snapshot-truncation.md).

## Known Differences

These differences are deliberate and covered by tests. Keep them documented here.

- **`null` payloads.** Go keeps a JSON `null` payload as a nil `[]byte` and re-encodes it as
  `null`; Rust decodes it to empty bytes and writes `""`. Only hand-built JSON can contain a `null`
  payload. Neither store writes one.
- **Event versions.** Rust refuses to decode a stored event whose version, after upcasting, is not
  the version its type declares (`PayloadError::VersionMismatch`). Go decodes it into the current
  struct. Deploy readers before writers when an event version changes.
- **Floats.** Not normalized across languages, as described under [JSON Encoding](#json-encoding).
- **Duplicate-command window.** Both languages only see command IDs in events after the latest
  snapshot. This is shared behavior, listed here because it surprises readers. The `dedupe-window`
  scenario covers it. Superseded in v0.9 by command outcomes
  ([Command Outcomes](#command-outcomes)); the window remains as a load-time pre-check, and on an
  upgraded log it is the only guard for pre-v0.9 command IDs, which have no outcome rows — a
  pre-v0.9 duplicate beyond the latest snapshot is not detected (a documented upgrade limitation,
  no backfill).
- **PostgreSQL metadata.** Stored as `jsonb`, so its bytes differ from the encoded JSON. Compare it
  as JSON.

## Compat Scenarios to Add

The v0.9 core reshape (C1–C10) extends `compat/scenarios.json` with the interleaved Go/Rust
scenarios below. Go generates the core golden fixtures for them
(`go test ./internal/compat -run TestFixtures -update`), Rust must reproduce them byte for byte,
and the live interleaved runs (`scripts/compat-integration.sh dynamo|postgres`) execute the same
steps on real backends. **E2.8** (Go conformance suite + fixtures) and **E3.8** (Rust conformance
suite + the live runs) must implement every scenario in this list; the names are the shared keys
in `compat/scenarios.json`. The `metadata-extensions` case is implemented for C6 (E2.1/E3.1)
in the current single-stream harness; it does not yet enable outcomes. Once C3 lands, both writers
run these scenarios with outcomes enabled and a command ID on every step. Some scenarios need
new ops in the harness (`declareFeature`, `acquireRebuildLock`,
`releaseRebuildLock`, `forceReleaseRebuildLock`, `resolveOutcome`, `readLatest`, `rebuildViews`, a
multi-stream execute, and `injectCommitFault`, which makes a commit apply but the caller observe an
indeterminate error, so `outcomes-ambiguous` exercises the real ambiguous path rather than only
the re-read branches); [`compat/README.md`](../../compat/README.md) grows the op table when they
land.

| Scenario               | Covers | What it pins                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ---------------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `outcomes-idempotency` | C3     | A retry with the same identity digest returns the stored outcome and writes nothing; a retry with a different digest is `IdempotencyKeyReuse` in both languages; a command ID over 512 UTF-8 bytes fails with `CommandIdTooLong` before any write.                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `outcomes-noop`        | C3     | A zero-event command and an outcome-only commit are stored as accepted with no `events` attribute.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `outcomes-rejection`   | C3, C4 | A domain rejection is persisted once in final state; the retried command replays it verbatim, cross-language.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `outcomes-ambiguous`   | C3     | After an indeterminate commit error — injected with `injectCommitFault` so the commit really applied — the re-read by command ID returns the stored outcome (same digest), `IdempotencyKeyReuse` (different digest), or absence (safe retry).                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `commit-too-large`     | C1     | A commit whose item budget exceeds 100, or whose estimated request exceeds the byte budgets (3,800,000 bytes in aggregate, 380,000 bytes for any single item), fails with `CommitTooLarge` before any write, in both languages and on both backends; nothing is stored and the commit is never split.                                                                                                                                                                                                                                                                                                                                                                                          |
| `multi-stream-commit`  | C1     | One commit appends to two streams atomically; the other language interleaves and must see both streams or neither. Per-stream floor checks apply.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `read-fences`          | C2     | A fenced read fails when the other language appends past the head; the compaction ABA (append, snapshot, compact past the head) still fails the fence; the snapshot companion is present on every fenced read, including one whose aggregate has snapshots disabled; a fenced-and-written stream emits no fence checks; a fence on a stream whose type is in the manifest's durable `retention` set is refused with `RetentionConflict` — including by a writer that configured no retention itself.                                                                                                                                                                                           |
| `fence-aware-retry`    | C2, C5 | Classification is per failing item: an event-put or floor failure in a commit that also carries caller-stamped fences still re-prepares and succeeds; a failed caller-stamped fence produces the persisted `stale-fence` rejection, whose only remedy is a new command ID — the old ID replays the rejection.                                                                                                                                                                                                                                                                                                                                                                                  |
| `metadata-extensions`  | C6     | `principalId`, `causationId`, and `extensions` round-trip in both directions, including the defaults: `causationId` written as the command ID when unset, `extensions` written as `{}`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `commit-grouping`      | C6, C7 | A multi-stream commit's events carry one `commitId` across all its streams. A rebuild of a **history-dependent** projector replays every commit grouped by `commitId`, each group's events in the pinned (stream ID, sequence) order, and pre-v0.9 events without `commitId` replay as single-event commits; a rebuild of a **current-state** projector never replays — it loads every aggregate's final state and derives the complete row set through the state entry point, producing the same rows over a compacted prefix as over a full log. `CompactBelow` refuses an entity type in the durable `historyDependent` set — including from a process that never registered the projector. |
| `commit-wide-views`    | C7     | A commit-wide projector writes rows at a `pk` outside `{entityType}#{entityId}` and deletes a row; the other language reads the rows after the commit; a `pk` over 2,048 or an `sk` over 1,024 UTF-8 bytes fails with `ViewKeyTooLong` before any write.                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `latest-n`             | C8     | With more than `n` events on a stream, both languages read the last `n` events newest-first, excluding the snapshot item.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `feature-marker`       | C9     | A declaration bumps `version`; the other language's cached version fails its condition and revalidates; a bump of a feature's format version fails the older writer the same way; a writer that does not support a declared `(name, formatVersion)` pair refuses to commit; adding or removing an entity type from `historyDependent` or `retention` bumps `version` the same way, and the refusals those sets drive hold across independently configured instances.                                                                                                                                                                                                                           |
| `rebuild-fence`        | C10    | While one language holds `SYSTEM#REBUILD`, the other's commit conflicts; release (and force-release) bumps the generation; takeover of an expired lease bumps it by two while it stays odd; the reader epoch check sees odd/changed generations; a replay batch or transaction whose lock-row condition fails aborts the worker without writing — on PostgreSQL the write transaction holds the lock row `FOR UPDATE`, so a takeover blocks until it commits. Force-release deletes the lock but leaves the generation odd and sets `forceReleased`: commits resume while readers stay fenced until a later rebuild completes and releases (even, flag cleared).                               |
| `reserved-namespaces`  | C8     | A commit whose entity ID begins with `SYSTEM#` or `command-outcome#` is refused before any write, and so is an out-of-band `PutSnapshot` with a reserved ID; enumeration and heads skip the engine rows the other language wrote.                                                                                                                                                                                                                                                                                                                                                                                                                                                              |

## Changing the Format

The format changes only on purpose. A change must:

1. Update the Go implementation and regenerate the fixtures:
   `go test ./internal/compat -run TestFixtures -update`.
2. Update the Rust implementation until `cargo test --workspace` passes against the new fixtures.
3. Pass the live cross-language suite on both backends (`scripts/compat-integration.sh`).
4. Keep older stored data readable: add upcasters for changed event payloads and fixture coverage
   for the old shape.
5. Update this document and `BEHAVIORAL_INVARIANTS.md`.
