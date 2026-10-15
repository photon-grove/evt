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
    "timestamp": "2026-10-12T00:00:00Z"
  }
}
```

Keys, in order: `id`, `sequence`, `type`, `version`, `entityId`, `entityType`, `payload` (base64),
`metadata` (an object).

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
  "extensions": { "requestSource": "cli" }
}
```

| Key           | Type           | Notes                                                                                 |
| ------------- | -------------- | ------------------------------------------------------------------------------------- |
| `commandId`   | string or null | The command that produced the event. Used to reject duplicate commands.               |
| `trace`       | object or null | W3C trace-context carrier. Keys sorted.                                               |
| `origin`      | object or null | `{"source": ..., "endpoint": ...}`, both strings.                                     |
| `address`     | string or null | Optional caller address. An empty address is written as `null`.                       |
| `region`      | string         | The region the command ran in.                                                        |
| `timestamp`   | string         | RFC 3339 UTC with second precision: `2026-01-02T03:04:05Z`.                           |
| `principalId` | string or null | Who issued the command (the caller's identity), for audit and per-principal checks.   |
| `causationId` | string or null | The command that caused this command. Defaults to `commandId` when unset (see below). |
| `extensions`  | object         | String-keyed application-specific metadata. Keys sorted. `{}` when unset or empty.    |

The last three keys are new in v0.9 (ADR 0004, C6) and are appended after the existing keys, so
stored events remain readable without upcasters — decoding is lenient in both languages. They are
always written:

- `principalId` is the caller's identity as a string, `null` when the command carries no principal.
- `causationId` is written with its effective value: the explicitly set causation, otherwise the
  `commandId`, otherwise `null`. Two events written by the same command therefore carry the same
  `causationId` even when neither set it explicitly.
- `extensions` is always an object, `{}` when the command carries no extensions. Values are strings;
  keys are sorted by their encoded string, like every map.

A metadata object without the three keys (an event written before v0.9) decodes with `principalId`
and `causationId` `null` and `extensions` empty.

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
2. The rebuild-lock check (`SYSTEM#REBUILD`), when the store enforces the rebuild fence
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

Steps 2, 6, and 7 apply only when the corresponding capability is enabled for the store. The
feature-marker check (step 1) is not optional: every commit in a v0.9 store carries it, and a
store that never declares features still validates against `attribute_not_exists(pk)`. A store
built without the v0.9 capabilities writes exactly the actions the older paragraphs describe. The
order is pinned so fixtures byte-match across implementations.

The floor check counts toward the 100 actions and is billed as a write, so a commit without a
snapshot costs one more write unit than its events and projector puts, and holds at most 99 of
them. `CommitStream` adds one check per entity in each batch.

**Multi-stream commits (v0.9, C1).** A commit may append to any number of streams. Each written
stream keeps its existing protections — `attribute_not_exists(sk)` on every event put and the
snapshot floor — per stream, and the sequence-contiguity rule applies per written stream. Nothing
else about the item shapes changes.

The item budget is the same for the single-aggregate shortcut and a multi-stream commit:

```text
items = Σ over written streams (events_s + 1)     event puts + snapshot put or floor check
      + fenced read-only streams × (1 or 2)       slot check, + snapshot companion unless omitted
      + 1                                         outcome put, when outcomes are enabled
      + 1                                         feature-marker check, always in v0.9 stores
      + 1                                         rebuild-lock check, when the fence is enabled
      + projector puts and deletes
```

A commit whose budget exceeds 100 fails with a typed `CommitTooLarge` error **before any write**.
It is never split: splitting would break atomicity, because an error after an earlier chunk
commits would leave only part of the aggregate set updated.

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

A `ConditionalCheckFailed` on a fence check, the feature-marker check, or the rebuild-lock check
is classified like the existing conflicts: a fence failure means another writer moved the stream
([Read Fences](#read-fences)); a feature-marker or rebuild-lock failure means the store's state
changed under the writer ([Feature Marker](#feature-marker), [Rebuild Fence](#rebuild-fence)). All
are retried with a fresh load, per [Fence-Aware Retry](#fence-aware-retry).

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
DynamoDB would otherwise fail the whole transaction with an unclassified `ValidationException`. Each projector operation carries the cancellation reason it
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

The companion is **omitted** when the fenced stream's aggregate has a snapshot interval of 0
(snapshots disabled): compaction requires a covering snapshot, so such a stream can never be
compacted and the ABA is impossible. Every other fenced stream carries both checks. When a
fenced stream is **written in the same commit**, both checks are omitted: the event put at
`head + 1` and that stream's floor or snapshot condition supersede them.

A `ConditionalCheckFailed` on either check is an optimistic-concurrency conflict — another writer
moved the stream — classified and retried like an event-put conflict. Whether the engine may
re-prepare the command depends on fence provenance; see [Fence-Aware Retry](#fence-aware-retry).

Worked example. A transaction loads `claim#ada` at head 3 and fences it (snapshot interval
greater than 0, so both checks apply). The two items are:

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
(`{entityId}:{sequence}` strings) encoded with the shared JSON rules.

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
`(command-outcome#<id>, sk = 1)`):

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
bug is fixed starts clean. Engine-provided rejection codes are `not-found` and `bad-command`
(Go's former `NotFoundError`/`BadCommandError`), and the engine's own stale-fence rejection
([Fence-Aware Retry](#fence-aware-retry)). Domain error enums map onto rejection codes in each
language; codes are free-form strings beyond the engine-provided ones.

A `stale-fence` rejection is durable even though the condition it records is transient: exactly
one outcome per command ID is the contract, so the old ID stays terminal and its rejection replays
verbatim. The remedy is a new command ID, not a retry of the old one — see
[Fence-Aware Retry](#fence-aware-retry).

## Fence-Aware Retry

On conflict, a transactional command is re-prepared — the command re-runs against a fresh load —
**only when every fence in the failed commit was engine-filled**: the engine loaded the fenced
streams itself, so it can reload them and re-decide (v0.9, C5). If any fence was caller-stamped
(a head the caller supplied), the engine cannot know the caller's decision is still valid, so it
does not retry: it returns a **stale-fence rejection**, persisted as the command's outcome
([Command Outcomes](#command-outcomes)) with the reserved code `stale-fence`. Only a
caller-stamped fence produces one; engine-filled fences re-prepare internally and never surface
it. Although a stale fence is transient by nature, the rejection is a durable, terminal outcome —
exactly one outcome per command ID is the contract — so the remedy is not a retry of the same
command: the caller re-reads the fenced streams, re-decides, and submits a **new command ID**. The
old ID replays its stored `stale-fence` rejection verbatim, like any rejected outcome. This
replaces blind whole-command retry as the default path for transactional commands; the policy
packages remain for backend-level transient classification.

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
  reserved prefix. Reserved rows are never ordinary aggregates: they have no snapshots and never
  compact.
- A writer **refuses** an entity ID that begins with a reserved prefix at commit time, before any
  write, with a typed error.
- Entity IDs were never restricted before, so a pre-existing stream such as `SYSTEM#account`
  would become silently invisible to enumeration, heads, and rebuild after upgrading. `evt-doctor`
  reports pre-existing streams whose IDs fall in a reserved namespace, and the upgrade note
  requires renaming (migrating) such streams before enabling the v0.9 features. Real stored data
  is never silently skipped.

### latest(n)

`latest(n)` is a bounded, newest-first read of a stream's last `n` events (descending), for
"recent activity" reads that must not replay a long stream. No item shape changes.

- DynamoDB: query `pk = :id AND sk > :zero`, `ScanIndexForward = false`, `Limit = n`. The `sk > 0`
  key condition excludes the snapshot item, so the result is the `n` highest event sequences,
  descending.
- PostgreSQL: `SELECT … FROM evt_events WHERE entity_id = $1 ORDER BY sequence DESC LIMIT $2`.

## Feature Marker

A store declares the extensions it uses on a single `SYSTEM#FEATURES` marker row (v0.9, C9). Any
writer that does not support a declared feature **refuses to write** to the table.

| Attribute  | Type | Feature-marker item (`pk = SYSTEM#FEATURES`, `sk = 0`)                         |
| ---------- | ---- | ------------------------------------------------------------------------------ |
| `pk`       | S    | `SYSTEM#FEATURES`                                                              |
| `sk`       | N    | `0`                                                                            |
| `version`  | N    | bumped on every declaration or change                                          |
| `features` | S    | JSON array of declared feature identifiers, sorted by byte order, deduplicated |

```json
{
  "pk": "SYSTEM#FEATURES",
  "sk": 0,
  "version": 2,
  "features": "[\"journal\",\"journal-compaction\"]"
}
```

Feature identifiers are strings. The identifiers this spec defines are `journal` and
`journal-compaction` (the Rust-first extensions, ADR 0005); a writer that encounters an
identifier it does not model reads the feature set and refuses. The row is declared by the writer
that first uses a feature, and updated — with a `version` bump — by every later declaration;
`version` is bumped again when an extension's stored format changes.

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
at re-validation an unsupported writer reads the feature set, sees the feature it does not model,
and refuses. The check costs one transaction item per commit ([Writes](#writes)).

The declaration itself is a conditional write: creating the row asserts `attribute_not_exists(pk)`;
updating it asserts `version = :observed`, writes the merged, sorted feature set, and sets
`version = :observed + 1`. (One row per feature with distinct sort keys could not do this: adding
a feature would neither change an already-validated row nor fail a condition against another key.)

## Rebuild Fence

Rebuilds are fenced durably (v0.9, C10). The lock row and the generation counter are separate
items:

| Attribute        | Type | Rebuild-lock item (`pk = SYSTEM#REBUILD`, `sk = 0`) |
| ---------------- | ---- | --------------------------------------------------- |
| `pk`             | S    | `SYSTEM#REBUILD`                                    |
| `sk`             | N    | `0`                                                 |
| `owner`          | S    | owner token of the current lock holder              |
| `leaseExpiresAt` | N    | lease expiry, epoch seconds                         |

| Attribute | Type | Generation item (`pk = SYSTEM#REBUILD#GENERATION`, `sk = 0`) |
| --------- | ---- | ------------------------------------------------------------ |
| `pk`      | S    | `SYSTEM#REBUILD#GENERATION`                                  |
| `sk`      | N    | `0`                                                          |
| `value`   | N    | monotonic counter, bumped per rebuild                        |

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
  "value": 4
}
```

The protocol. Acquire, release, and force-release are each **one `TransactWriteItems`** that
writes both items in a single transaction, so a crash can never leave the lock and the generation
disagreeing: the generation is **odd if and only if the lock row exists** — odd while a rebuild
holds the lock, even when quiesced.

- **Acquire** (a fresh lock, or takeover of an expired one):

  ```text
  Put    (SYSTEM#REBUILD, sk = 0)  owner = :newOwner, leaseExpiresAt = :now + :lease
         ConditionExpression:
           attribute_not_exists(pk)
           OR (leaseExpiresAt < :now AND owner = :observedOwner)
  Update (SYSTEM#REBUILD#GENERATION, sk = 0)
         SET value = if_not_exists(value, :zero) + :one
         ConditionExpression:
           attribute_not_exists(value) OR value = :seenEven
  ```

  `:observedOwner` is the owner the caller observed when it decided to take over an expired lease
  (any value when the lock is free). The generation condition asserts the quiesced value the
  caller observed: absent-or-`0` before the first rebuild (`:seenEven = 0` covers the absent
  item), or the stored even value after. A takeover racing another takeover fails one of the two
  conditions and retries from a fresh read.

- **Renew**: while it runs, the rebuild renews `leaseExpiresAt` with a conditional update
  asserting `owner = :owner`.

- **Release** — one transaction:

  ```text
  Delete (SYSTEM#REBUILD, sk = 0)
         ConditionExpression: owner = :me
  Update (SYSTEM#REBUILD#GENERATION, sk = 0)
         SET value = value + :one
         ConditionExpression: value = :seenOdd
  ```

- **Force-release** — one transaction, the same shape with the operator-named owner:

  ```text
  Delete (SYSTEM#REBUILD, sk = 0)
         ConditionExpression: owner = :requestedOwner
  Update (SYSTEM#REBUILD#GENERATION, sk = 0)
         SET value = value + :one
         ConditionExpression: value = :seenOdd
  ```

- **Commit-time check**: every commit on a store with the fence enabled includes a
  `ConditionCheck` asserting `attribute_not_exists(pk)` on the lock row — a commit is refused
  whenever any rebuild holds the lock, **including an expired one**: until the lock is taken over
  or force-released, it blocks writes. That is deliberate: commits stay safe while any lock row
  exists, at the cost of waiting out a crashed rebuild. A commit that races a rebuild is rejected
  as a conflict and retried with a fresh load ([Fence-Aware Retry](#fence-aware-retry)).

The rows live in the reserved `SYSTEM#` namespace, skipped by enumeration, heads, and rebuild
scans, and neither row has a snapshot or a `ttl`.

**Reader epoch check.** A reader that needs consistency reads the generation before and after its
read and treats an **odd or changed** value as "rebuild in progress or completed — retry": the
generation increments when a rebuild acquires the lock and again when it releases after replay
completes, so state built under a stale epoch can be rejected and re-read.

## PostgreSQL

The schema is created by Go's `postgres.Repository.EnsureSchema` or Rust's
`PostgresRepository::ensure_schema`, which run the same idempotent DDL. Table names default to
`evt_events` and `evt_snapshots` and can be changed in both implementations. Names are interpolated
unquoted, so both reject names that are not plain identifiers, are longer than 47 bytes, or are
PostgreSQL reserved keywords. PostgreSQL truncates identifiers to 63 bytes, and the longest derived
name, `{events}_entity_type_idx`, adds 16. Both also reject a pair of names where two of the
relations the schema creates (the tables, the index, and each table's `{table}_pkey`) would share a
name after case folding, because `CREATE TABLE IF NOT EXISTS` would skip the second one.

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
CREATE INDEX IF NOT EXISTS evt_events_entity_type_idx ON evt_events (entity_type);

CREATE TABLE IF NOT EXISTS evt_snapshots (
    entity_id      text   PRIMARY KEY,
    entity_type    text   NOT NULL,
    sequence       bigint NOT NULL,
    event_sequence bigint NOT NULL,
    payload        bytea  NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS evt_command_outcomes (
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

CREATE TABLE IF NOT EXISTS evt_features (
    id       integer PRIMARY KEY,
    version  bigint NOT NULL,
    features text   NOT NULL
);

CREATE TABLE IF NOT EXISTS evt_rebuild_lock (
    id               integer PRIMARY KEY,
    owner            text        NOT NULL,
    lease_expires_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS evt_rebuild_generation (
    id    integer PRIMARY KEY,
    value bigint NOT NULL
);
```

- A commit is one transaction: the entity lock, one insert per event, the floor check, projector
  statements, then the snapshot upsert. Projector statements run after the events, so a writer
  that lost a race fails on the event key (a conflict) before it reaches a view row. Projector statements are Rust-only: Go's `postgres.Repository` ignores projector
  transaction groups. A projector statement that the database rejects is a projector error (a lost
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
  flight. Both languages take these locks in ascending entity-key order, so concurrent
  multi-stream commits cannot deadlock against each other.
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

- `evt_command_outcomes` holds one row per command (`command_id` is the command ID without the
  `command-outcome#` prefix). `digest` and `code` are text as on DynamoDB; `response` is `bytea`,
  the same raw bytes whose base64 the DynamoDB item stores; `events` is a `jsonb` array of event
  IDs and `rejection` a `jsonb` object with keys `code`, `message`, `details` — compare both as
  JSON, not bytes. The `CHECK` constraints mirror the once-in-final-state rule. The outcome insert
  is a plain `INSERT` into the commit transaction; a unique violation (`23505`) on
  `evt_command_outcomes` means another writer recorded the outcome first — the commit is
  ambiguous, and the caller recovers by re-reading the row by command ID
  ([Ambiguous-write recovery](#ambiguous-write-recovery)). The outcome insert runs last, after the
  snapshot upserts.
- `evt_features` holds exactly one row, `id = 0`; `features` is the canonical JSON array text,
  byte-identical to the DynamoDB `features` attribute. The commit-time check is a `SELECT` of
  `version` in the commit transaction — mandatory in every v0.9 commit, as on DynamoDB,
  including the no-marker case. A row whose `version` differs from the cached one, or a row that
  appeared after the writer validated an empty table, is a conflict. Declarations take the
  exclusive advisory lock below for their short read-modify-write; commit-time checks take the
  shared lock, so a declaration cannot slip between a writer's check and its commit.
- `evt_rebuild_lock` (`id = 0`) and `evt_rebuild_generation` (`id = 0`) mirror the lock and
  generation items; `lease_expires_at` is `timestamptz` rather than epoch seconds.

**Advisory locks.** The per-entity commit lock stays `pg_advisory_xact_lock(1702261760,
hashtext($1))`. Engine rows use class key `1702261761`: objid `0` is the feature marker and objid
`1` the rebuild fence.

- A multi-stream commit takes the per-entity lock for **every stream in its read set** — written
  and fenced — in ascending byte order of entity ID, then runs the fence assertions below, then
  the inserts. A concurrent writer blocks on the lock instead of slipping `head + 1` past a
  fenced read.
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

- **Rebuild fence** (v0.9, C10): the durable `evt_rebuild_lock` **row** is what fences commits,
  exactly as the DynamoDB item does. After taking the shared advisory lock, every commit asserts
  the row is absent, in the same order as the DynamoDB transaction (the feature-marker `SELECT`
  first, then this assertion):

  ```sql
  SELECT 1 FROM evt_rebuild_lock WHERE id = 0;  -- any row present, including an expired one, is a conflict
  ```

  A commit that finds a row is a conflict retried with a fresh load, until the lock is taken over
  or force-released: a crashed rebuild keeps blocking commits, same as DynamoDB. The shared lock
  (`pg_advisory_xact_lock_shared(1702261761, 1)`, taken at commit start) makes the assertion
  safe without serializing commits against each other: acquire and release take the exclusive
  lock (`pg_advisory_xact_lock(1702261761, 1)`) for their short transactions only, so no acquire
  or release can slip between a commit's assertion and its `COMMIT`. Acquire, release, and
  force-release are each one transaction with the same two statements as the DynamoDB
  transaction: the conditional lock insert or delete (fresh acquire asserts absence; takeover
  asserts `owner = :observedOwner AND lease_expires_at < :now`; release and force-release assert
  the owner) and the conditional generation bump (`attribute_not_exists(value) OR value =
:seenEven` to acquire, `value = :seenOdd` to release), keeping the invariant that the
  generation is odd if and only if the lock row exists.

- **Feature marker**: declarations take `pg_advisory_xact_lock(1702261761, 0)` — exclusive —
  around the read-modify-write of `evt_features`; commit-time checks take
  `pg_advisory_xact_lock_shared(1702261761, 0)`, which is why the check-then-commit race above
  cannot happen: the commit holds the shared lock to `COMMIT`, and a declaration's exclusive
  lock waits for every commit holding the shared lock. Commits do not serialize against each
  other.

## Stream Envelope

Go's `stream.SNSPublisher` wraps each committed event in an EventBridge-style envelope and publishes
it to SNS. The Rust crate encodes the same bytes (`photon_evt::wire::envelope::encode`) and decodes
both SNS messages and raw-delivery SQS bodies (`decode`).

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
window remains as a cheap pre-check on load.

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
  ([Command Outcomes](#command-outcomes)); the window remains as a load-time pre-check.
- **PostgreSQL metadata.** Stored as `jsonb`, so its bytes differ from the encoded JSON. Compare it
  as JSON.

## Compat Scenarios to Add

The v0.9 core reshape (C1–C10) extends `compat/scenarios.json` with the interleaved Go/Rust
scenarios below. Go generates the core golden fixtures for them
(`go test ./internal/compat -run TestFixtures -update`), Rust must reproduce them byte for byte,
and the live interleaved runs (`scripts/compat-integration.sh dynamo|postgres`) execute the same
steps on real backends. **E2.8** (Go conformance suite + fixtures) and **E3.8** (Rust conformance
suite + the live runs) must implement every scenario in this list; the names are the shared keys
in `compat/scenarios.json`. In all of them both writers run with outcomes enabled and a command ID
on every step. Some scenarios need new ops in the harness (`declareFeature`, `acquireRebuildLock`,
`releaseRebuildLock`, `forceReleaseRebuildLock`, `resolveOutcome`, `readLatest`, a multi-stream
execute, and `injectCommitFault`, which makes a commit apply but the caller observe an
indeterminate error, so `outcomes-ambiguous` exercises the real ambiguous path rather than only
the re-read branches); [`compat/README.md`](../../compat/README.md) grows the op table when they
land.

| Scenario               | Covers | What it pins                                                                                                                                                                                                                                  |
| ---------------------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `outcomes-idempotency` | C3     | A retry with the same identity digest returns the stored outcome and writes nothing; a retry with a different digest is `IdempotencyKeyReuse` in both languages.                                                                              |
| `outcomes-noop`        | C3     | A zero-event command and an outcome-only commit are stored as accepted with no `events` attribute.                                                                                                                                            |
| `outcomes-rejection`   | C3, C4 | A domain rejection is persisted once in final state; the retried command replays it verbatim, cross-language.                                                                                                                                 |
| `outcomes-ambiguous`   | C3     | After an indeterminate commit error — injected with `injectCommitFault` so the commit really applied — the re-read by command ID returns the stored outcome (same digest), `IdempotencyKeyReuse` (different digest), or absence (safe retry). |
| `commit-too-large`     | C1     | A commit whose item budget exceeds 100 fails with `CommitTooLarge` before any write, in both languages; nothing is stored and the commit is never split.                                                                                      |
| `multi-stream-commit`  | C1     | One commit appends to two streams atomically; the other language interleaves and must see both streams or neither. Per-stream floor checks apply.                                                                                             |
| `read-fences`          | C2     | A fenced read fails when the other language appends past the head; the compaction ABA (append, snapshot, compact past the head) still fails the fence; a fenced-and-written stream emits no fence checks.                                     |
| `fence-aware-retry`    | C2, C5 | An engine-filled fence conflict re-prepares and succeeds; a caller-stamped fence produces the persisted `stale-fence` rejection, whose only remedy is a new command ID — the old ID replays the rejection.                                    |
| `metadata-extensions`  | C6     | `principalId`, `causationId`, and `extensions` round-trip in both directions, including the defaults: `causationId` written as the command ID when unset, `extensions` written as `{}`.                                                       |
| `commit-wide-views`    | C7     | A commit-wide projector writes rows at a `pk` outside `{entityType}#{entityId}` and deletes a row; the other language reads the rows after the commit.                                                                                        |
| `latest-n`             | C8     | With more than `n` events on a stream, both languages read the last `n` events newest-first, excluding the snapshot item.                                                                                                                     |
| `feature-marker`       | C9     | A declaration bumps `version`; the other language's cached version fails its condition and revalidates; a writer that does not model a declared feature refuses to commit.                                                                    |
| `rebuild-fence`        | C10    | While one language holds `SYSTEM#REBUILD`, the other's commit conflicts; release (and force-release) bumps the generation; the reader epoch check sees odd/changed generations.                                                               |
| `reserved-namespaces`  | C8     | A commit whose entity ID begins with `SYSTEM#` or `command-outcome#` is refused before any write; enumeration and heads skip the engine rows the other language wrote.                                                                        |

## Changing the Format

The format changes only on purpose. A change must:

1. Update the Go implementation and regenerate the fixtures:
   `go test ./internal/compat -run TestFixtures -update`.
2. Update the Rust implementation until `cargo test --workspace` passes against the new fixtures.
3. Pass the live cross-language suite on both backends (`scripts/compat-integration.sh`).
4. Keep older stored data readable: add upcasters for changed event payloads and fixture coverage
   for the old shape.
5. Update this document and `BEHAVIORAL_INVARIANTS.md`.
