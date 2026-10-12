# Storage Format

This is the storage contract shared by the Go `evt` packages and the Rust `photon-evt` crate. Both
implementations read and write the same DynamoDB items, PostgreSQL rows, serialized JSON, and
stream envelopes, so services in either language can share one event log.

The golden fixtures in [`compat/fixtures/`](../../compat/fixtures) are the executable form of this
document. The Go implementation generates them, and both test suites check against them. If this
document and the fixtures disagree, the fixtures are what the code enforces; fix whichever is wrong.
See [`compat/README.md`](../../compat/README.md) and
[ADR 0003](../adr/0003-rust-implementation-and-cross-language-compatibility.md).

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
- Object keys appear in struct declaration order. Map keys are sorted.
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
  "timestamp": "2026-10-12T00:00:00Z"
}
```

| Key         | Type           | Notes                                                                   |
| ----------- | -------------- | ----------------------------------------------------------------------- |
| `commandId` | string or null | The command that produced the event. Used to reject duplicate commands. |
| `trace`     | object or null | W3C trace-context carrier. Keys sorted.                                 |
| `origin`    | object or null | `{"source": ..., "endpoint": ...}`, both strings.                       |
| `address`   | string or null | Optional caller address. An empty address is written as `null`.         |
| `region`    | string         | The region the command ran in.                                          |
| `timestamp` | string         | RFC 3339 UTC with second precision: `2026-01-02T03:04:05Z`.             |

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
`sk = 1, 2, 3, …`; the entity's single snapshot lives inline at `sk = 0`.

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

`ttl` is written only for entity types with a retention policy, and only when positive. An event
item with missing or empty `metadata` is invalid. Readers ignore other attributes.

### Writes

A commit is one `TransactWriteItems` call, at most 100 actions, in this order: projector
operations, one put per event, then the snapshot put if the commit takes one.

| Write                         | Condition                                                                                        |
| ----------------------------- | ------------------------------------------------------------------------------------------------ |
| event put                     | `attribute_not_exists(sk)`                                                                       |
| snapshot put in a commit      | `attribute_not_exists(seq) OR (seq = :seq)`, where `:seq` is the new snapshot sequence minus one |
| snapshot put outside a commit | `attribute_not_exists(eventSeq) OR eventSeq <= :new`, where `:new` is the new `eventSeq`         |

A transaction cancelled with a `ConditionalCheckFailed` reason is an optimistic-concurrency
conflict: another writer committed the same sequence or moved the snapshot. The caller reloads and
retries. A failed condition on a projector operation is a projector error, not a conflict, so it is
not retried. A failed out-of-band snapshot condition is not an error; the newer snapshot stays.

### Reads

- Events: query `pk = :id AND sk > :after`, ascending, skipping `sk = 0`. Reads are strongly
  consistent by default.
- Snapshot: get item `(pk, sk = 0)`.
- Entity heads (rebuilds and diagnostics only): scan `pk`, `sk`, and `eventSeq`, optionally
  filtered by `entityType`. An entity's head is the larger of its highest `sk` and its snapshot's
  `eventSeq`.

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

## PostgreSQL

The schema is created by Go's `postgres.Repository.EnsureSchema` or Rust's
`PostgresRepository::ensure_schema`, which run the same idempotent DDL. Table names default to
`evt_events` and `evt_snapshots` and can be changed in both implementations.

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
```

- A commit is one transaction: projector statements, one insert per event, then the snapshot
  upsert.
- A unique violation (`23505`) on the event primary key is an optimistic-concurrency conflict.
- The snapshot upsert only replaces a snapshot whose `event_sequence` is lower or equal
  (`ON CONFLICT (entity_id) DO UPDATE … WHERE EXCLUDED.event_sequence >= evt_snapshots.event_sequence`).
  PostgreSQL does not check the snapshot sequence: a racing writer already fails on the event key.
- `metadata` is `jsonb`, which normalizes whitespace and key order. Compare it as JSON, not bytes.
  `payload` is stored byte for byte.
- Entity heads take `MAX` over event sequences and snapshot event sequences per entity.

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

A command whose metadata carries a command ID already recorded by an event **replayed since the
latest snapshot** is a duplicate and writes nothing. Command IDs folded into a snapshot are no
longer checked, so the dedupe window is the events after the snapshot.

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
- A compacted stream must be loaded or rebuilt from its snapshot, never by replaying from sequence 1.
- Known limitation, shared by both languages: compaction deletes the rows that the event-sequence
  condition relies on. A writer that loaded the stream before the covering snapshot and commits
  without a snapshot after compaction can write a sequence at or below the snapshot's
  `eventSequence`. Loads ignore such rows, but the commit's projector operations still apply. See
  ADR 0001, "stale-context sequence reuse".

See [ADR 0001](../adr/0001-event-compaction-and-snapshot-truncation.md).

## Known Differences

These differences are deliberate and covered by tests. Keep them documented here.

- **`null` payloads.** Go keeps a JSON `null` payload as a nil `[]byte` and re-encodes it as
  `null`; Rust decodes it to empty bytes and writes `""`. Only hand-built JSON can contain a `null`
  payload; neither store writes one.
- **Floats.** Not normalized across languages, as described under [JSON Encoding](#json-encoding).
- **Duplicate-command window.** Both languages only see command IDs in events after the latest
  snapshot. This is shared behavior, listed here because it surprises readers; the `dedupe-window`
  scenario covers it.
- **PostgreSQL metadata.** Stored as `jsonb`, so its bytes differ from the encoded JSON. Compare it
  as JSON.

## Changing the Format

The format changes only on purpose. A change must:

1. Update the Go implementation and regenerate the fixtures:
   `go test ./internal/compat -run TestFixtures -update`.
2. Update the Rust implementation until `cargo test --workspace` passes against the new fixtures.
3. Pass the live cross-language suite on both backends (`scripts/compat-integration.sh`).
4. Keep older stored data readable: add upcasters for changed event payloads and fixture coverage
   for the old shape.
5. Update this document and `BEHAVIORAL_INVARIANTS.md`.
