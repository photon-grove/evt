# ADR 0007: Cheaper commits with a per-stream head item

## Status

Accepted (2026-10-18). Tracking: matrix row C12 in
[`docs/parity/feature-matrix.md`](../parity/feature-matrix.md) and
[#159](https://github.com/photon-grove/evt/issues/159). The core commit path is implemented in Go
and Rust, and the migration tool (`evt-doctor migrate-heads`, Go only; see the
[runbook](../migrating-to-stream-heads.md)) is implemented; the real-DynamoDB billing measurement
(`go/backends/dynamo/awsmeasure_test.go`, run by a human) is still to be run. The normative text is the
[Stream Heads](../spec/storage-format.md#stream-heads) section of the storage spec, and the
sections of the spec that describe commits, fences, and failures follow it.

**Decided:** the migration from v0.8 storage is a stop-the-world procedure (stop writers, backfill,
verify, declare the marker feature, start the new writers). The project is greenfield, so there is
no dual-layout transition release. The defaults below stand: fences stay refused for retained types,
PostgreSQL gets a heads table, and `stream-heads` is a required marker feature.

It amends the commit layout of [ADR 0004](0004-core-reshape-multi-stream-transactions-and-outcomes.md)
(C1, C2; ADR 0004 carries a note pointing here) and adds one feature to the C9 marker. It does not change events, snapshots, outcomes,
metadata, or views. v0.9 (C1–C9) is unreleased, so the wire format can still change before release.

## Context

Every commit is one DynamoDB `TransactWriteItems`, capped at 100 actions and 4 MB. The v0.9 reshape
(ADR 0004) spent most of that budget on protection items that carry no data:

| Item                                                  | Per                              | Why it exists                                                      |
| ----------------------------------------------------- | -------------------------------- | ------------------------------------------------------------------ |
| continuation check on `(pk, first - 1)`               | written stream appending         | A stream continues exactly from its head; no sequence is skipped.  |
| floor check on the snapshot item `(pk, 0)`            | written stream with no snapshot  | Compaction deleted keys that `attribute_not_exists(sk)` relies on. |
| slot check on `(pk, head + 1)`                        | read-only fenced stream          | No writer appended past the head the reader saw.                   |
| snapshot companion on `(pk, 0)`                       | read-only fenced stream          | Closes the append, snapshot, compact ABA that empties the slot.    |
| feature-marker check, outcome put, rebuild-lock check | commit                           | Control rows.                                                      |

So a written stream costs `events + 2` when it appends (continuation plus floor or snapshot put) and
`events + 1` when it creates, and a fenced stream costs 2. Projector operations count against the
same 100.

A command that consumes ten ingredient items (ten single-event appends) and reads five reference
streams, with an outcome, sends 42 actions before projector operations: `1 + 10 × 3 + 5 × 2 + 1`.
A command that writes a dozen streams and reads nine lands at 56, or 57 with the
rebuild-lock check (C10, emitted since), which leaves 43 actions for projector operations. The read-fence pair is the
largest avoidable cost, and the continuation and floor checks are the next.

Two further costs are invisible in the action count:

- **Billing follows the checked item.** Every transactional action is two underlying writes of the
  item's size, rounded up to 1 KB ([DynamoDB capacity
  consumption](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html)).
  The floor check and the snapshot companion are checks on the snapshot item, which holds the whole
  aggregate state. If a check bills by the size of the item it reads, a 12 KB snapshot makes every
  floor check and every fence on that stream cost 24 write units for no data. See
  [WCU and RCU](#wcu-and-rcu) for the caveat.
- **Pre-commit reads.** To pin the head before it writes, a written stream whose first sequence is
  above 1 first reads its snapshot (strongly consistent, sized by the snapshot) and then, unless the
  snapshot ends exactly at the head, the head event
  (`go/backends/dynamo/continuity.go`, `observeContinuations`). Go does this one
  stream at a time, so a ten-stream append makes up to twenty round trips before the commit.

## Decision

Give every stream one small **head item** that records the stream's head, and make it the only item
a commit conditions on for concurrency. Compaction, snapshots, and retention keep working around it.

### The head item

| Attribute | Type | Head item (`sk = -1`)                                                         |
| --------- | ---- | ----------------------------------------------------------------------------- |
| `pk`      | S    | entity ID                                                                     |
| `sk`      | N    | `-1`                                                                          |
| `head`    | N    | the stream's head: the highest event sequence ever committed to it            |
| `ttl`     | N    | optional, epoch seconds; only for entity types with retention (see Retention) |

- `sk = -1` sits in the stream's own partition, below the snapshot at `0` and the events from `1`.
  Every events query already bounds `sk` from below at 0 or more (`sk > :after`, `sk > 0`,
  `sk BETWEEN :lo AND :hi` with `:lo ≥ 1`), so queries, `latest(n)`, and compaction never return or
  delete it, and event and snapshot reads by code that predates the item are unaffected. Full-table
  scans are not (see Readers). This was checked against the Go and
  Rust query sites. One needs a guard: Go's `GetLatestEvents` passes its `lastSequence` straight into
  `sk > :sk` (Rust clamps with `.max(0)`), so a negative argument would already return the snapshot
  and would now return the head item; the implementation clamps it at 0.
- It holds only `head` (and `ttl`). An `Update` is billed on the larger of the item before and
  after, so the item must stay under 1 KB. Adding attributes re-prices every commit; do not.
- `head` is audited against DynamoDB's reserved words and is not one. `ttl` is, so any expression
  that touches it goes through `#ttl`.
- It is not a view and not a projection. It is concurrency-control metadata for the log, written in
  the same transaction as the events it counts, and it is fully derivable from the log (the larger
  of the highest event sequence and the snapshot's `eventSeq`), so it can be wiped and recomputed
  without loss. It is unrelated to the cut `HeadStore` (K4), an asynchronous projection that
  nothing in a commit depended on.

Four invariants make it sound:

- **H1, monotone.** `head` increases and is never lowered, and a commit that writes events to a
  stream raises its head by at least one. The head item is never deleted (see Retention for the one
  declared exception).
- **H2, exact.** After a commit that writes a stream, `head` equals the stream's highest committed
  event sequence.
- **H3, covered.** A snapshot's `eventSeq` never exceeds `head`. Commits keep it by raising `head` to at
  least every snapshot they write. Out-of-band snapshot writes enforce it with a condition on the head
  item; see Snapshots and compaction.
- **H4, derivable.** The head a loader derives (the larger of the highest event `sk` and the
  snapshot's `eventSeq`) equals `head`. Compaction deletes events at or below a snapshot's
  `eventSeq`, and `eventSeq ≤ head`, so compaction cannot change the derived head.

### The commit layout

Per written stream, the group is: **head update**, event puts in ascending sequence, then the
snapshot put when the commit writes one. There is no continuation check and no floor check.

```text
Update  (pk, sk = -1)   SET head = :last
        create (first = 1):  ConditionExpression: attribute_not_exists(head)
        append (first > 1):  ConditionExpression: head = :prev        (:prev = first - 1)
Put     (pk, sk = n)    attribute_not_exists(sk)                       unchanged
Put     (pk, sk = 0)    snapshot; condition is the sequence chain only:
        first snapshot:  attribute_not_exists(seq)
        later snapshot:  seq = :seq
```

- **Continuity** is the head condition. `head = first - 1` means events `1..first-1` were committed
  (H2), so nothing is skipped, and everything from `first` up is free. A gap is a first sequence
  above the stored head plus one.
- **The floor** is implied. `head = first - 1` and H3 give `snapshot.eventSeq ≤ first - 1 < first`.
  A writer that loaded before another writer appended, snapshotted, and compacted has a stale
  `:prev`, and the head moved when the other writer appended, so it conflicts. Compaction itself
  never moves the head, so a writer whose load is still current never conflicts with a compaction.
- **The snapshot put** keeps the sequence chain, because an out-of-band snapshot write can bump the
  snapshot sequence without moving the head. It loses `eventSeq < :first`, and the pinned variants
  (`eventSeq = :prev`) disappear along with the pre-commit reads that chose them.
- **Event puts** keep `attribute_not_exists(sk)`. It costs no action, and it is the defense if a
  non-conforming writer ever appends without updating the head item.

A **read fence** is one `ConditionCheck` on the head item:

```text
ConditionCheck (pk, sk = -1)   head = :head              :head ≥ 1
ConditionCheck (pk, sk = -1)   attribute_not_exists(head)   :head = 0 (the reader saw no events)
```

The reader's `:head` is the head it derived when it loaded the stream (H4). The check passes exactly
when no commit has written the stream since. Because `head` only increases (H1), a value that matches
cannot have been reached again by a different history: **the compaction ABA does not exist**, so
there is no snapshot companion, and no reader-side distinction between interval-0 and snapshotting
streams. A stream that is both fenced and written contributes only its head update, as now.

### Action counts

With `e` events in a written stream, `c` = 1 when it appends (first sequence above 1), `σ` = 1 when
the commit writes its snapshot, and `F` fenced streams:

```text
old per written stream = c + e + 1            continuation (when appending) + events + (snapshot put or floor check)
new per written stream =     1 + e + σ        head update + events + snapshot put (when written)
old per fenced stream  = 2                    slot + companion
new per fenced stream  = 1                    head check
fixed                  = marker 1 + outcome 1 (when enabled) + rebuild lock 1 (C10) + projector ops
```

| Commit                                              | Old | New | Change |
| --------------------------------------------------- | --- | --- | ------ |
| create one stream, 1 event, no snapshot (+ outcome) | 4   | 4   | 0      |
| append 1 event, no snapshot (+ outcome)             | 5   | 4   | −1     |
| append 1 event that writes a snapshot (+ outcome)   | 5   | 5   | 0      |
| create with 5 events and a snapshot (+ outcome)     | 8   | 9   | +1     |
| 10 single-event appends + 5 fenced reads            | 42  | 27  | −15    |
| 12 single-event appends + 9 fenced reads            | 56  | 35  | −21    |
| 1 single-event append + 30 fenced reads             | 65  | 34  | −31    |

Every row includes the marker and the outcome put. C10 added the rebuild-lock check, so add one to
both columns: the dozen-and-nine command becomes 57 against 36, which leaves 64 actions for
projector operations instead of 43. Max fenced streams beside one single-event append, marker and
outcome counted: 47 old, 96 new.

Single-stream capacity for event and projector operations (marker counted, outcome not, before the
rebuild-lock check): create 98 → 98, append 97 → 98, create-with-snapshot 98 → 97,
append-with-snapshot 97 → 97. With the rebuild-lock check every number is one lower: 97 → 97,
96 → 97, 97 → 96, 96 → 96.
The one regression is a commit that **creates** a stream and writes its first snapshot in the same
commit (a first commit of `interval` or more events): the old layout let the snapshot put replace the
floor check, and now it sits beside the head update.

### WCU and RCU

DynamoDB documents that every transactional action is two underlying writes of the item, billed at
the item's size rounded up to 1 KB, and that an `Update` is billed on the larger of the item before
and after. The documentation does not say in so many words that a `ConditionCheck` bills by the size
of the item it reads, and the numbers below assume it does. **The implementation spike must confirm it
with `ReturnConsumedCapacity` against real DynamoDB** (the local emulator does not bill; run
`go/backends/dynamo/awsmeasure_test.go`). The action
counts above do not depend on it.

Worked example, the ten-writes-and-five-reads command, with 12 KB reference snapshots, 4 KB written
snapshots, and events and event-sized items under 1 KB. Event puts, the marker check, and the outcome
put are the same in both layouts and are left out:

| Overhead item (WCU)                  | Old                                       | New         |
| ------------------------------------ | ----------------------------------------- | ----------- |
| 5 fenced reads                       | 5 × (2 slot + 24 companion) = 130         | 5 × 2 = 10  |
| 10 appended streams                  | 10 × (2 continuation + 8 floor) = 100     | 10 × 2 = 20 |
| **Total**                            | **230**                                   | **30**      |
| Pre-commit strongly consistent reads | 10 × (1 snapshot + 1 head event) = 20 RCU | 0 RCU       |

If condition checks bill at a flat 2 WCU regardless of size, the old total is 60 and the new one 30,
so the saving is between 2× and roughly 8× depending on snapshot size, never negative. A cancelled
transaction still bills, so a conflict-heavy hot stream benefits at least as much as a quiet one.

Two other effects. The head `Update` bills 2 WCU against a floor check that billed `2 × ceil(snapshot KB)`,
so partition write load drops wherever snapshots exceed 1 KB, and the commit no longer reads a large
snapshot just to prove it is old enough. And a head `Update` writes a stream record per written stream
per commit, which consumers must skip (see [Readers](#readers-and-other-consumers)).

### Contention

Same partition, same hot key, fewer hot items. All writers to a stream already serialize on its
`head + 1` event key and its snapshot item, and fenced readers condition-check those same two items.
DynamoDB cancels two overlapping transactions that include the same item, including as a
`ConditionCheck`, with `TransactionConflict` ([transaction conflict
handling](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html#transaction-conflict-handling)).
Now everything converges on one item per stream, the head item. The set of commits that conflict is
unchanged: two writers of one stream still conflict (head condition, as the event put did), a writer
and a fencing reader still conflict, and two readers that only fence the same stream conflict only
while their transactions overlap, as they did on the slot and companion. Both fences of a pair
used to be hot items, so the number of hot items per popular reference stream halves. `TransactionConflict`
stays classified as a retryable conflict. There is no new hot partition: the head item is in the
stream's own partition.

One subtlety. A stream the commit fences and one it writes are different streams, so one transaction
never targets a head item twice. A stream that is fenced and written contributes only the update.

### Snapshots and compaction

- Commit snapshot puts keep `attribute_not_exists(seq)` and `seq = :seq` and lose `eventSeq < :first`.
- **Out-of-band snapshot writes enforce H3.** `PutSnapshot` (`go/backends/dynamo/snapshot.go`) only checks that
  `eventSeq` is monotone, which is not enough. With the stored head at 7, `PutSnapshot(eventSeq = 10)`
  would let a writer that loaded earlier pass `head = 7` and write events 8 to 10 beneath a snapshot
  that already covers them; loaders skip those events, and every later writer derives 10 and
  conflicts with the stored head forever. The old floor check refused that stale writer. So
  `PutSnapshot`, and any rebuild path that seeds a snapshot (`SeedEntity`), adds the head item to its
  existing transaction, which becomes marker check, head item, snapshot put:
  - for a type without retention, a `ConditionCheck` with `head >= :new` (`:new` is the snapshot's
    `eventSeq`);
  - for a type with retention, an `Update` of the same item with that condition and the never-lower
    `ttl` refresh (a transaction cannot touch an item twice; see Retention);
  - when `:new` is 0 (a snapshot of an empty stream, including a snapshot-only partition the
    migration left without a head item) the head item may be absent: the check is
    `attribute_not_exists(head) OR head >= :new`, and it is a `ConditionCheck` even for a retained
    type, because an `Update` would create a head item with no `head` attribute.

  A failed head condition means the snapshot is ahead of the stream, or the stream has no head item:
  it is `StreamHeadDrift`. It is not the existing no-op for "a newer snapshot already covers this",
  which keeps its meaning for a failed snapshot condition. PostgreSQL does the same under the entity
  lock (`SELECT head` must be at least the new `event_sequence`). Rust's `put_snapshot` does the
  same on all three backends.

- `CompactBelow(through)` still requires `through ≤ snapshot.eventSeq`, still deletes `[1, through]`,
  and still never deletes the snapshot. It also never deletes or writes the head item, which is
  outside the range. Compaction can delete the head **event** without consequence: continuity no
  longer depends on an event existing.
- A compacted stream's next commit conditions on `head = first - 1` and needs no read, where the
  old layout re-read the snapshot to choose a pinned condition.
- The head item makes issue [#155](https://github.com/photon-grove/evt/issues/155) (an oversized
  snapshot bricking a stream) cheaper to survive: the commit path only touches the snapshot when it
  writes one. It does not fix it, since a snapshot put of an oversized state still fails.

### Retention

The question was whether a head item without a TTL makes fences compatible with retention. It makes
the slot hazard in the spec go away: the head item survives the expiry of `head + 1`, and a fence on
`head = :head` fails correctly. But it opens two others, so **fences stay refused for retained entity
types (`RetentionConflict`) in this ADR**:

1. **Recreation.** With no TTL, a stream whose events and snapshot expired keeps `head = N`. A loader
   derives head 0, and every new commit conditions on `attribute_not_exists(head)` and conflicts
   forever. The entity ID can never be reused, and a tombstone stays per expired stream.
2. **The reader would have to read the head item.** A fence at the derived head fails whenever
   expiry made the derived head lag the stored one (`N` stored, tail expired, derived `< N`), so
   loads would have to take the head from the head item. That is a different load path, and it still
   cannot recreate a stream.

Giving the head item a TTL fixes recreation but not soundness: TTL deletion is lazy, so a reader can
load a stream that has expired but is not yet deleted at head 1, the head item and event can then
expire, another writer can create the stream afresh and reach head 1, and `head = 1` passes for a
different incarnation. Closing that needs an incarnation token in the head item, which is a format
addition for a feature nobody has asked for.

**Decision:** the head item of a type with retention carries a `ttl` that is **at least the `ttl` of
every other item of the stream**, and `RetentionConflict` stays.

- Every commit raises it and never lowers it (the rule below).
- Every out-of-band snapshot write refreshes it in the same transaction, by the same rule.
  `PutSnapshot` stamps its snapshot's `ttl` at call time, so without the refresh the snapshot would
  outlive both the events and the head item: a loader would derive the snapshot's `eventSeq` with
  no head item, and no `head = :prev` could ever hold.
- The backfill sets it to the maximum `ttl` across the partition, snapshot included.

**The never-lower rule.** A commit that stamped its own `ttl` on the head could shorten it, because
two writers can configure different retention durations (the manifest records only entity-type
names, not durations) and a writer's clock can step back. DynamoDB has no `max()` in an update
expression, so the head `ttl` is raised by value and guarded by a condition. This ADR does not
require a single durable retention duration. A retained stream's head update, on append, is:

```text
UpdateExpression:    SET head = :last, #ttl = :t
ConditionExpression: head = :prev AND (attribute_not_exists(#ttl) OR #ttl <= :t)
```

`:t` is the larger of the commit's own `ttl` and the largest `ttl` among the items the writer loaded
for the stream (the snapshot and the events after it). On create, the update is
`SET head = :last, #ttl = :t` under `attribute_not_exists(head)`, with `:t` the commit's `ttl`. The
events keep the commit's own `ttl`; only the head takes the maximum. The condition makes lowering
impossible: if an unseen writer (an out-of-band snapshot) raised the stored `ttl` above `:t`, the
update fails as an ordinary conflict and the retry reloads, sees the larger `ttl`, and passes. An
out-of-band snapshot write uses the same `ttl` condition with `:t` its snapshot's `ttl`. When it
fails, `PutSnapshot` reads the head item back: a head at or above `:new` with a `ttl` at least the
snapshot's needs no refresh and the write is retried as a `ConditionCheck` on `head >= :new`; one
with a lower or absent `ttl` (the head was behind when the update failed and a concurrent writer
advanced it with a shorter `ttl`) is retried as the refreshing update, so the snapshot never
outlives the head item.

A retained stream disappears whole once every one of its items has expired, and its ID is then
reusable. TTL deletion is lazy and unordered, so a partly deleted stream is possible and transient,
and the failure-path probe classifies it: a head item absent while events or a snapshot remain is
`StreamHeadDrift` **unless every remaining item's `ttl` has passed** (expired, awaiting deletion),
which is a conflict that heals once they are deleted. For a retained type, a head above the derived
head is the same expired-tail transient, not drift. If retention-compatible fences are wanted later,
the head item is the place to add an incarnation token without another migration.

### Interaction with the other capabilities

- **C3 outcomes.** Unchanged: the outcome put is last, still outranks every other failed item except
  the marker, and the row layout does not change. An outcome-only commit (a no-op, a rejection with
  no events, a stale-fence rejection) is still marker plus fence checks plus the outcome put, with
  one check per fence instead of two.
- **C4 rejections with events.** Same group layout as an accepted commit.
- **C5 fence-aware retry.** Provenance is in-memory commit metadata and does not change. A failed
  head **update** is an ordinary conflict, like a failed event put, and re-prepares whether or not
  the commit carries caller-stamped fences. Only a failed caller-stamped **fence check** fails fast
  into a `stale-fence` rejection. `StreamHeadDrift` is never retried and never exhausted into a
  rejection (see Failure classification). With one fence item per stream there is one index to map. The
  head update's condition is exactly an expected-version check, which is what
  [#154](https://github.com/photon-grove/evt/issues/154) wants for streams a command also writes;
  that design can build on it.
- **C9 marker.** The format change is one new feature, `stream-heads`, at format version 1. See
  Migration. The marker check itself is unchanged.
- **C10 rebuild fence.** The lock check is still a mandatory control item; nothing about the head
  item changes it. Rebuild view writes touch no stream items. A rebuild reads events, never head
  items. A rebuild path that seeds a snapshot goes through the `PutSnapshot` guard above, which reads
  the head item; it never raises a head.
- **Fences and the marker's `retention` set.** Unchanged, per Retention above.

### Failure classification

The commit is classified by the failing items' cancellation-reason indexes. Where several items
failed, the engine ranks them, and every backend reports the same one. The rank, highest first:

1. the feature-marker check (revalidate and retry, unchanged), then the rebuild-lock check
   (`RebuildInProgress`, unchanged);
2. the outcome put (`OutcomeConflict`, unchanged);
3. **`StreamHeadDrift`**;
4. **`SequenceGapError`**;
5. any other conflict: a head update, an event put, a snapshot put, or a fence check (a fence
   carries its provenance, unchanged), then projector conditions.

`TransactionConflict`, throttling, and indeterminate writes are unchanged. **A gap used to be
refused before any write**, ahead of everything, by the pre-commit reads. It is now whichever
transaction item fails, so it ranks below the outcome conflict: if the outcome row exists, the row is
the answer, and the next attempt returns the stored outcome. PostgreSQL pins the same rank (see
PostgreSQL).

**The failure-path probe.** A failed head update, or a failed fence check, is classified by reading
the stream, with no `ReturnValues` on the transaction: both implementations do exactly this, so the
classification is identical.

1. Derive the head `D1` with strongly consistent reads: the snapshot item's `eventSeq` and the
   highest event `sk` (a `Query` of `sk > 0`, descending, limit 1).
2. Read the head item (`GetItem`, strongly consistent) as `S`, absent as 0.
3. `S < D1`: an event or snapshot sits above the head. That is drift (a healthy stream cannot show
   it, because a commit writes the head and the events together and `D1` was read first).
4. `S = D1`: no drift. For a failed head update, `S < first - 1` is a **gap** and anything else a
   conflict. A failed fence is a fence conflict.
5. `S > D1`: derive again as `D2`. `D2 < S` is drift (events are missing below the head). Otherwise a
   commit landed between the reads, which is an ordinary conflict.

The probe costs a few reads on the failure path only; the happy path has none.

**`StreamHeadDrift`** (Go `*StreamHeadDriftError`, Rust `Error::StreamHeadDrift`) means H2, H3, or H4
is broken for a stream: a writer appended without maintaining the head, events were deleted, or a
snapshot is ahead of the head. It carries the stream ID, the stored head (or absent), and the
derived head. It is **not retryable and never exhausted into a rejection**: fence-aware retry
returns it at once, like any internal failure, writes no outcome row, and never records a
`stale-fence` rejection for it. Without this, a head that sits below the derived head would fail
`head = :prev` five times and persist a false `stale-fence` that replays forever. It is raised in
three cases:

- the **certain** case: the stream's head update passed and its event put failed, so an event exists
  above the head (the cancellation reasons show the head update as passing). For a type with
  retention this goes through the retention-aware probe first: TTL can delete the head before an
  expired event, so a create then passes the head condition and fails the event put on the residual
  row. The engine reads the stream's remaining items (the snapshot and a `sk > 0` query projecting
  `sk` and `ttl`). If every one has a `ttl` in the past, the failure is a transient conflict that
  heals when TTL deletes them. Otherwise it is drift. A type without retention is drift at once.
- the probe finds drift (steps 3 and 5 above);
- an out-of-band snapshot write finds the snapshot ahead of the head.

A failed **snapshot put** whose head update passed is not drift: an out-of-band snapshot can bump the
snapshot sequence without moving the head, and that is an ordinary conflict.

Repair is offline: `evt-doctor migrate-heads --repair` raises a head below the derived value and
creates a missing one. A head **above** the derived value is lowered only by
`--repair --allow-lower`, which must run with every writer stopped and writes
`SET head = :derived` conditioned on `head = :observed`. Lowering breaks H1, which is why it needs
writers stopped (an in-flight fence reads the head).

### The 4 MB byte budget

The head update's budgeted size follows the spec's existing rule for updates: key attributes plus its
expression strings, name placeholders (the placeholder and the name), and attribute values, which is
roughly 150 bytes plus twice the entity ID. It replaces a continuation check, a floor check, and a
companion, each of similar size, so the aggregate estimate moves by hundreds of bytes in a commit of
megabytes. The 3,800,000-byte aggregate and 380,000-byte per-item thresholds do not change, and the
head item never approaches the per-item limit.

### PostgreSQL

PostgreSQL has no action cap, so the case for a head table there is semantic parity and cheaper
queries, not capacity. It still takes the same model so that "head" means one thing on every backend
and tool:

```sql
CREATE TABLE IF NOT EXISTS evt_events__evt_stream_heads (
    entity_id   text   PRIMARY KEY,
    entity_type text   NOT NULL,
    head        bigint NOT NULL CHECK (head >= 1)
);
```

Under the per-entity advisory locks the commit already takes, in the same order:

- create: `INSERT … ON CONFLICT (entity_id) DO NOTHING`, which must affect one row;
- append: `UPDATE … SET head = $last WHERE entity_id = $1 AND head = $prev`, which must affect one row
  (a guarded version);
- fence: `SELECT head … WHERE entity_id = $1`, absent as 0, which must equal `:head`;
- a failure is classified by the same probe as DynamoDB (derive, read the head, re-derive when the
  head is ahead), run inside the transaction.

To report the same failure as DynamoDB, PostgreSQL pins its statement order and ranks before it
reports: locks; marker `SELECT`; rebuild assertion; a probe for an existing outcome row for the
command (`OutcomeConflict` ranks above gap and drift, as on DynamoDB; the final `INSERT`'s `23505`
still resolves a real race); every head statement and fence read, evaluated without stopping at the
first failure; then the ranking (drift, then gap, then conflicts); then the event inserts, snapshot
upserts, and outcome insert. A snapshot write also checks `head >=` its new `event_sequence`.

The continuity query (`GREATEST(MAX(sequence), MAX(event_sequence))`), the floor query, and the two
fence queries go away. The advisory locks stay: they serialize writers and make the absent-row fence
(head 0) safe, since PostgreSQL cannot lock a row that does not exist. `entity_heads` becomes a read
of this table instead of an aggregate over the whole events table, but only once `stream-heads` is
declared: `EnsureSchema` creates the table empty on a populated v0.8 database, and a head-aware
process reading it before the migration would report no entities and make an incremental rebuild
skip all of them. So `entity_heads` reads the marker first, and until `stream-heads` is declared it
keeps the existing aggregate over events and snapshots. Go and Rust do the same. The new relation name is
`{events}__evt_stream_heads`, 18 characters past the base, which fits the 63-byte identifier limit
with the existing 34-byte base cap.

PostgreSQL can remain conformant without this table (derived heads under the same locks are
equivalent), so it is the least essential part of the ADR; see the open questions.

### Every event-writing path

Each path that adds, removes, or snapshots events either maintains the head or is fenced off:

| Path                                                                                                    | Effect on the head                                                                                                                                                                                          |
| ------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Go `Commit`, `CommitWithSnapshot` (DynamoDB, PostgreSQL, in-memory); outcome-only and rejection commits | The head update is part of the commit. `CommitWithSnapshot` routes through `Commit`.                                                                                                                        |
| Go PostgreSQL and in-memory `CommitStream`                                                              | Loops over `Commit`; nothing to change.                                                                                                                                                                     |
| Go DynamoDB `CommitStream` (deprecated, K1)                                                             | Builds its own continuation and floor checks and writes events with no head update, so it would drift heads. It **refuses with a typed error once `stream-heads` is declared**, or is deleted first (#134). |
| Rust `commit` and `commit_transaction` (DynamoDB, PostgreSQL, in-memory)                                | The head update is part of the commit.                                                                                                                                                                      |
| `PutSnapshot`, `put_snapshot` (all backends), and a rebuild path that seeds a snapshot                  | Reads the head (`head >= :new`) and refreshes its `ttl` for retained types. Never raises it.                                                                                                                |
| `CompactBelow`, `compact_below`                                                                         | Deletes events at or below a covering snapshot. Never touches the head.                                                                                                                                     |
| `dynamo.Delete` (K7, `!prod`) and fixture or reset tooling                                              | Deletes events only. A deleted stream keeps its head, so the tool must delete the head item too.                                                                                                            |
| `evt-doctor migrate-heads`                                                                              | The only tool that writes head items outside a commit.                                                                                                                                                      |

### Readers and other consumers

- Event reads, `latest(n)`, snapshot reads, and compaction ranges are unchanged.
- **Full-table scans break on the head row, in v0.8 as well as v0.9.** Go's `decodeScanPage` skips
  only `sk == 0` and then decodes the row's metadata, which is empty, so `json.Unmarshal` fails; the
  v0.8.0 scanner is the same. Rust's `read_page` (`crates/evt/src/dynamodb/enumerate.rs`) hands any
  non-snapshot item to `event_from_item`, which also fails. Both are reachable through
  `StreamAllEvents(StreamFilter{})`, unfiltered `StreamEntities`, and a rebuild with no entity type
  (C10 rebuilds). A scan with an `entityType` filter is unaffected, because a head item has no
  `entityType`. So "readers that predate the item are unaffected" holds for queries only.
- **Required scanner changes**, in both languages:
  - `decodeScanPage` and `read_page` skip every row with `sk < 1`;
  - `enumerateEntityIDs` projects `sk` as well as `pk` and skips head rows (`sk < 0`), because an
    unfiltered scan would otherwise list an orphan head (a partition with no events or snapshot) as a
    ghost entity ID; snapshot rows still enumerate their entity;
  - heads scans (`pk, sk, eventSeq`) skip `sk < 0` explicitly: Go's `headSeqFromItem` routes every
    `sk ≤ 0` row to the snapshot branch and Rust's `item_head` returns any non-zero `sk` as the
    head, so `-1` would contribute `-1`.
    These ship, and scan jobs are upgraded, **before** the backfill (see Migration).
- Diagnostics that list a whole partition (Rust's `partition_items`) now return the head item.
- **DynamoDB Streams consumers** see an `INSERT` or `MODIFY` record for `sk = -1` per written stream
  per commit, and the backfill emits one per stream. A consumer must skip them. An ESM or Pipes
  filter does it on the keys, which every view type carries, including `KEYS_ONLY`:

  ```json
  {
    "dynamodb": { "Keys": { "sk": { "N": [{ "anything-but": ["-1", "0"] }] } } }
  }
  ```

  The pattern also drops the snapshot at `sk = 0`, which consumers already skip. The Lambda
  `NEW_IMAGE` decoder (X3, `photon-evt-lambda`) skips `sk < 1` itself, and its `EventFilter`
  pattern drops head items by requiring a `metadata` attribute. The journal's `KEYS_ONLY` wake-ups
  read the views table's stream instead, which holds no head items. Event-log consumers that do not
  filter pay for the extra records.

- `dynamo.Delete` (cut, K7) and fixture cleanups must delete the head item too, or a deleted stream
  keeps its head and rejects its own recreation.

## Alternatives considered

**(a) The head item (recommended).** Described above. Fences 2 → 1, appends without a snapshot lose
the continuation check, no pre-commit reads, no ABA, no dependence on what the snapshot weighs, and a
simpler spec (the pinned-snapshot machinery and the floor check go). Costs a new item per stream, a
format change with a migration, and one extra action for a commit that creates a stream and writes its
first snapshot.

**(b) Drop the companion for entity types the marker declares never-compacted.** A new marker set
(like `retention` and `historyDependent`) names types that are never compacted, and fences on them
carry only the slot check. This halves fence cost with no new item, no format change, and no
migration. It does nothing for written streams (each append still pays continuation plus floor), it
permanently gives up compaction for those types, it does not fix the snapshot-size billing of the
floor check, it makes correctness depend on the declaration being made before data is written, and it
leaves retention refused. It is the right fallback if the head item is rejected, and it is not
mutually exclusive in time: it could ship first. The head item subsumes it.

**(c) Leave fences alone and reduce projector cost.** Projector operations are the application's own
rows, and what evt can offer (writing them outside the commit) gives up the atomicity commit-wide
projectors exist for. It does nothing about the 21 avoidable actions in the dozen-and-nine example.
Complementary, not an alternative.

**(d) Key-preserving compaction.** Replace compacted events with empty stub items so the slot check
alone is ABA-proof. It keeps every key forever, so compaction stops shrinking the item count, and it
rewrites the compaction contract (ADR 0001). Rejected.

**(e) Put the head in the snapshot item (`sk = 0`).** One fewer item per stream. But every commit
would then rewrite the snapshot item, billed at its size, and every commit would contend with
out-of-band snapshot writers on the same item. This is what the separate item avoids. Rejected.

**(f) Decouple snapshots from the commit.** Write snapshots in a follow-up one-item transaction. It
would save the extra action on creates with a snapshot and every snapshot commit's `+1`, at the price
of the exact-interval and atomicity guarantees. Orthogonal; not pursued here.

**(g) Merge the marker check and the rebuild-lock check into one control row.** Saves one action per
commit since C10 landed. It couples two protocols (one already implemented), and it makes the
cancellation reason ambiguous between "revalidate" and "`RebuildInProgress`" without returned values.
Worth a follow-up; out of scope.

**(h) A dual-layout transition release (rejected).** v0.9 writers would keep the old continuation,
floor, slot, and companion checks until the `stream-heads` declaration flipped them to head
conditions, so v0.8 and v0.9 writers could coexist during a rolling deployment. It would keep both
layouts' code, the pinned-snapshot machinery, and the snapshot companion alive in two languages and
three backends for a release, double the conformance matrix for it, and still leave the flip racing
any v0.8 writer that does not read the marker. All of that buys the avoidance of a short quiet
window. The project is greenfield, so the quiet window is acceptable and the dual layout is not
worth building.

Recommendation: (a), with (b) as the fallback and (g) as a follow-up.

## Migration and compatibility

v0.9 is unreleased, so this is a change to the v0.9 format, not a v0.9-to-v0.10 upgrade. What needs
a path is **v0.8 storage**: a table of streams with no head items, written by v0.8 writers that know
nothing of the marker.

**Queries are compatible, scans and writes are not.** A v0.8 `Query` or `GetItem` never sees a head
item (every query bounds `sk` at 0 or above). A v0.8 **full-table scan** fails on it, as Readers
explains, so every unfiltered scan job and rebuild must be upgraded before head items exist. A v0.8
**writer** appends events without updating the head item, which leaves it stale (H2 and H4 broken):
head-aware writers then fail on that stream with `StreamHeadDrift`, and a fence on it passes although
the stream moved. The marker cannot stop a writer that does not read it. So v0.8 writers must be
stopped before heads are declared.

**Decision: the migration is stop-the-world.** There is no transition release in which old and new
writers coexist. The project is greenfield, a quiet window is acceptable, and the alternative (a
dual layout, rejected below) costs more than the window does.

**The procedure:**

1. **Stop writers and take a backup.** Stop every v0.8 writer and start no head-aware writer. Enforce
   it rather than trusting it: apply an access-control deny on the write actions (`PutItem`,
   `UpdateItem`, `DeleteItem`, `BatchWriteItem`, `TransactWriteItems`) for every principal except the
   migration role (an identity policy on each writer principal, a table resource policy, or a service
   control policy), and keep it in place until step 6. DynamoDB has no setting that stops writes by
   capacity: provisioned mode has a positive minimum and on-demand has no writable-capacity setting.
   Take an on-demand backup or confirm point-in-time recovery; a rollback needs it.
2. **Upgrade readers.** Deploy the scanner and consumer changes from Readers: scan jobs, rebuild
   jobs, `evt-doctor`, and every Streams, ESM, or Pipes consumer (with the filter above). The
   backfill itself emits `sk = -1` stream records, so consumers must be ready first.
3. **Backfill.** Run `evt-doctor migrate-heads`. `migrate-heads` refuses a table whose marker
   declares `journal` or any other feature the Go tool does not model (the Rust-first extensions
   keep rows it would misread). On DynamoDB it is an explicit scan (allowed: it is a migration),
   projecting `pk, sk, eventSeq, ttl`, accumulating per partition across the whole scan (a partition
   split across a page boundary is finalized only when the scan ends, memory is one entry per
   stream). Items whose `ttl` has passed but which are not yet deleted are ignored, and a partition
   with nothing left is skipped. For each remaining partition it computes the derived head (H4) and
   the **maximum `ttl` across the partition**, and **skips a partition whose derived head is 0** (a
   snapshot-only partition at `eventSeq` 0), because `head ≥ 1`. It writes the head item with
   `SET head = :h` (and `#ttl`) conditioned on `attribute_not_exists(head)`. It is idempotent and
   resumable, supports `--dry-run`, and never lowers an existing head: a head above the derived value
   is reported, and a head below is raised only by `--repair`.
4. **Verify.** `evt-doctor migrate-heads --verify` rescans and requires that every partition with a
   non-expired item has a head item equal to its derived head (items past their `ttl` but not yet
   deleted are ignored, as in the backfill), and exits non-zero otherwise.
5. **Declare.** After a clean verification in the same invocation, repeated immediately before the
   declaration, the tool declares `stream-heads: 1` in the marker by the ordinary declaration
   transaction. It requires `--confirm-writers-stopped`, and the access-control deny of step 1
   is the evidence that writers stayed stopped between verify and declare; the tool cannot prove it.
   It verifies once more after the declaration and reports any drift.
6. **Start the new writers,** then lift the step-1 deny.

On PostgreSQL, `EnsureSchema` creates the empty heads table, and steps 3 to 5 are one transaction
under the exclusive feature advisory lock: `INSERT INTO … SELECT` the per-stream maximum of
`sequence` and `event_sequence` from both tables (skipping a maximum of 0), a verification query that
finds no stream whose row is missing or unequal to the derived head, lower or higher (otherwise the
transaction aborts and `--repair` is the remedy), then the
declaration. The lock blocks head-aware commits for its duration; v0.8 writers must still be stopped
(step 1). PostgreSQL has no TTL, no Streams, and no scan jobs to upgrade.

**A required feature.** The marker today only stops a writer that does not know a declared feature.
This adds the other direction: a head-aware writer requires `stream-heads: 1` and otherwise refuses
with a typed `StreamHeadsNotMigrated` error, so a build pointed at a table nobody has migrated fails
loudly on its first write instead of failing on every stream. The refusal covers **every write path
that validates the marker**: commits (including outcome-only and rejection commits), `PutSnapshot`,
`CompactBelow`, and the mutating `evt-doctor` commands other than `migrate-heads` itself. Read-only
paths do not need the feature. On a table with no marker row, a writer may declare `stream-heads`
itself, but only after confirming, with a **strongly consistent** scan, that the table holds no
non-reserved items (it reads until it finds one or exhausts the table; a populated table stops it
immediately). A fresh table therefore needs no step, and a populated unmigrated table can never be
declared by accident.

**Writers that predate the feature.** A v0.9 build without `stream-heads` refuses a table that
declares it (the existing rule), so a mixed fleet of old and new v0.9 builds cannot corrupt heads
after the declaration. v0.8 builds cannot be stopped; a stray v0.8 writer after the declaration
produces `StreamHeadDrift` (stale heads) and, for fenced streams, missed fences. `migrate-heads
--repair` fixes the heads once it is stopped.

**Rollback is not supported in place.** The declaration is never removed, so a pre-heads v0.9 build
refuses the table, and v0.8 full-table scans fail on head rows. Returning to the old layout means
restoring the step-1 backup. Plan the migration as one-way.

**A dual-layout transition release** (rejected) is Alternative (h).

**Compatibility fixtures that change.** Fixtures are generated by Go
(`go -C go test ./internal/compat -run TestFixtures -update`), never hand-edited, and Rust must reproduce
them:

- `compat/fixtures/scenarios.json`: every scenario's stored DynamoDB items gain the head item, and
  PostgreSQL rows gain heads rows.
- `compat/fixtures/feature-marker.json`: the ordered commit requests (head update first in each
  group, no continuation or floor checks, one check per fence), the `stream-heads` feature in the
  canonical manifest vectors and declaration rows, and the schema-upgrade DDL.
- `compat/fixtures/codec.json`: head item vectors (plain and `ttl`-bearing under the fixed clock)
  and budgeted sizes.
- `compat/fixtures/outcomes.json`: the outcome-only and fenced transaction orders and any budgeted
  sizes that include fences.
- `compat/scenarios.json`: changes to `read-fences` (one check; the append, snapshot, compact ABA
  still fails; the interval-0 case no longer special), `multi-stream-commit` (action order),
  `commit-too-large` (new budget, including the create-with-snapshot case), `fence-aware-retry`
  (classification keys, and drift is never exhausted into a rejection), and `feature-marker`
  (declare, required refusal, fresh-table declaration); and new scenarios `stream-heads-migration`
  (an unmigrated table is refused, the migration runs, both languages then commit and fence),
  `stream-heads-compaction` (compacting past the head neither moves the head nor conflicts with a
  current writer), `stream-heads-snapshot-guard` (an out-of-band snapshot above the head is refused),
  and `stream-heads-drift` (an `appendRaw` that bypasses the head yields `StreamHeadDrift` in both
  languages).
- `compat/README.md` and the "Compat Scenarios to Add" table in the spec.

**Implementation surface beyond the fixtures** (so the implementing change does not miss them):

- `go/internal/compat/fakedynamo.go`: it has no `UpdateItem`/`Update` transaction action and matches
  conditions by exact string, so it needs the head `Update`, the new condition strings, and the new
  fence check.
- The in-memory backends (Go `go/backends/mem/repo.go`, Rust `crates/evt/src/mem.rs`) keep a head per stream in
  the commit.
- Both conformance suites (`go/conformance/repository.go`, `crates/evt/src/conformance.rs`).
- Docs: `BEHAVIORAL_INVARIANTS.md`, `docs/dynamodb.md`, `docs/streams.md`, the READMEs, and the
  spec.

## Consequences

- **Cheaper in the common cases.** Fences cost half, appends without a snapshot lose a check, and
  the pre-commit reads disappear. The computed headroom for the dozen-and-nine command goes from 43
  to 64 projector operations. Billing no longer scales with snapshot size on the commit path.
- **A simpler protocol.** The continuation check, the floor check, the pinned conditions, the
  pre-commit observation, and the snapshot companion are deleted from both languages. The compaction
  ABA is not mitigated; it cannot occur.
- **A format change and a one-way migration.** One new item per stream, one new feature, one new
  required refusal, one new error (`StreamHeadDrift`), one migration tool. Existing v0.8 tables need
  a quiet window, a backup, and upgraded scanners and consumers first.
- **One regression.** Creating a stream and writing its first snapshot in one commit costs one more
  action.
- **A new thing to keep true.** H1–H4 are invariants the engine must maintain and `evt-doctor` must
  check (head below derived, head above derived, orphan head item, missing head item). Anything that
  deletes or writes events outside the commit path (fixtures, repair tools, the deprecated
  DynamoDB `CommitStream`) must maintain the head or be refused.
- **More stream records.** A head update per written stream per commit reaches DynamoDB Streams
  consumers.
- **Retention is unchanged.** Fences stay refused for retained types.

## Risks and open questions

1. **Non-rolling upgrade from v0.8: resolved.** Stop-the-world is accepted (see Migration and
   compatibility); no dual-layout release. The remaining risk is operational: a v0.8 writer left
   running after the declaration leaves stale heads, and `migrate-heads --repair` fixes them.
2. **Condition-check billing: unconfirmed.** The WCU numbers assume a `ConditionCheck` bills by the
   size of the item it reads. They stay marked as unconfirmed until measured on real DynamoDB with
   `ReturnConsumedCapacity`; that measurement is on the
   [#159](https://github.com/photon-grove/evt/issues/159) checklist. `go/backends/dynamo/awsmeasure_test.go` is the
   script for it (behind the `awsmeasure` build tag; a human runs it with AWS credentials that can
   create a scratch table, and pastes its table here). The action-count result stands either way.
3. **Retention: decided.** The head item carries `ttl` for retained types and fences stay refused
   (`RetentionConflict`). Lifting the refusal would need an incarnation token and is out of scope.
4. **PostgreSQL: decided.** PostgreSQL gets the heads table, so the spec has one definition of head.
   It can still land as a separate change from the DynamoDB one.
5. **Drift is loud but not self-healing.** A writer that appends without maintaining the head
   produces `StreamHeadDrift` (never a retry loop, never a recorded rejection), except for fences,
   which can pass until the probe runs. That is why the v0.8 writers must be stopped, and why the
   migration needs enforced evidence that they stayed stopped.
6. **The extra action on create-with-snapshot.** Cheap to accept, but it is the one place the new
   layout is worse, so say so in the release notes.
7. **Required-feature refusal is new marker behavior** (a writer refusing because a feature is
   missing). It needs its own conformance cases in `feature-marker`.
8. **Failure-path probe cost and races.** The probe adds a few strongly consistent reads to a failed
   commit. The read order (derive, then read the head, then re-derive only when the head is ahead)
   is what keeps a concurrent commit from reading as drift; the conformance suite needs a case that
   interleaves a commit with the probe.

See [ADR 0004](0004-core-reshape-multi-stream-transactions-and-outcomes.md) for the layout this
amends and [ADR 0001](0001-event-compaction-and-snapshot-truncation.md) for compaction.
