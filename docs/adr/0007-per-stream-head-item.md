# ADR 0007: Cheaper commits with a per-stream head item

## Status

Proposed (2026-10-18). Design only: no code lands until this ADR is accepted. Tracking: matrix row
C12 in [`docs/parity/feature-matrix.md`](../parity/feature-matrix.md) and
[#159](https://github.com/photon-grove/evt/issues/159). The normative draft is the
[Stream Heads](../spec/storage-format.md#stream-heads-proposed-adr-0007) section of the storage
spec, which is marked proposed and leaves every existing paragraph untouched.

It amends the commit layout of [ADR 0004](0004-core-reshape-multi-stream-transactions-and-outcomes.md)
(C1, C2) and adds one feature to the C9 marker. It does not change events, snapshots, outcomes,
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
| feature-marker check, outcome put, rebuild-lock check | commit (the last once C10 lands) | Control rows.                                                      |

So a written stream costs `events + 2` when it appends (continuation plus floor or snapshot put) and
`events + 1` when it creates, and a fenced stream costs 2. Projector operations count against the
same 100.

A command that consumes ten ingredient items (ten single-event appends) and reads five reference
streams, with an outcome, sends 42 actions before projector operations: `1 + 10 × 3 + 5 × 2 + 1`.
A command that writes a dozen streams and reads nine lands at 56, or 57 once C10 emits the
rebuild-lock check, which leaves 43 actions for projector operations. The read-fence pair is the
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
  ([`dynamo/continuity.go`](../../dynamo/continuity.go), `observeContinuations`). Go does this one
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
  delete it, and readers that predate the item are unaffected. This was checked against the Go and
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
- **H3, covered.** A snapshot's `eventSeq` never exceeds `head`.
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
fixed                  = marker 1 + outcome 1 (when enabled) + rebuild lock 1 (once C10 emits it) + projector ops
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

Every row includes the marker and the outcome put. Add one to both columns once C10 emits the
rebuild-lock check: the dozen-and-nine command becomes 57 against 36, which leaves 64 actions for
projector operations instead of 43. Max fenced streams beside one single-event append, marker and
outcome counted: 47 old, 96 new.

Single-stream capacity for event and projector operations (marker counted, outcome not, as in the
spec today): create 98 → 98, append 97 → 98, create-with-snapshot 98 → 97, append-with-snapshot 97 → 97.
The one regression is a commit that **creates** a stream and writes its first snapshot in the same
commit (a first commit of `interval` or more events): the old layout let the snapshot put replace the
floor check, and now it sits beside the head update.

### WCU and RCU

DynamoDB documents that every transactional action is two underlying writes of the item, billed at
the item's size rounded up to 1 KB, and that an `Update` is billed on the larger of the item before
and after. The documentation does not say in so many words that a `ConditionCheck` bills by the size
of the item it reads, and the numbers below assume it does. **The implementation spike must confirm it
with `ReturnConsumedCapacity` against real DynamoDB** (the local emulator does not bill). The action
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

- Snapshot puts keep `attribute_not_exists(seq)` and `seq = :seq` and lose `eventSeq < :first`.
  Out-of-band `PutSnapshot` keeps `attribute_not_exists(eventSeq) OR eventSeq <= :new` and does not
  touch the head. It does not need to: a snapshot is taken from a loaded state at or below the head
  (H3), and a snapshot above the head is already a poisoned stream today.
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

**Decision:** for an entity type with retention, the head item carries `ttl` equal to the `ttl` of the
commit that last wrote the stream (refreshed by every commit, so it expires with the stream's newest
event and snapshot), and `RetentionConflict` stays. This keeps today's lifecycle: a retained stream
that expires vanishes whole, and its ID is reusable. A head item that outlives its events by TTL
deletion order (or the reverse) gives a conflict that heals when the other items are deleted, which
is the same transient the partial-expiry gap already produces for loads. If retention-compatible
fences are wanted later, the head item is the place to add an incarnation token without another
migration.

### Interaction with the other capabilities

- **C3 outcomes.** Unchanged: the outcome put is last, still outranks every other failed item except
  the marker, and the row layout does not change. An outcome-only commit (a no-op, a rejection with
  no events, a stale-fence rejection) is still marker plus fence checks plus the outcome put, with
  one check per fence instead of two.
- **C4 rejections with events.** Same group layout as an accepted commit.
- **C5 fence-aware retry.** Provenance is in-memory commit metadata and does not change. A failed
  head **update** is an ordinary conflict, like a failed event put, and re-prepares whether or not
  the commit carries caller-stamped fences. Only a failed caller-stamped **fence check** fails fast
  into a `stale-fence` rejection. With one fence item per stream there is one index to map. The
  head update's condition is exactly an expected-version check, which is what
  [#154](https://github.com/photon-grove/evt/issues/154) wants for streams a command also writes;
  that design can build on it.
- **C9 marker.** The format change is one new feature, `stream-heads`, at format version 1. See
  Migration. The marker check itself is unchanged.
- **C10 rebuild fence.** The lock check is still a mandatory control item; nothing about the head
  item changes it. Rebuild view writes touch no stream items. A rebuild reads events, never head
  items. The rebuild path that seeds snapshots (`SeedEntity`) writes snapshots only, and does not
  touch heads.
- **Fences and the marker's `retention` set.** Unchanged, per Retention above.

### Failure classification

The commit is classified by the failing item's cancellation-reason index, as now. The mapping
changes in the stream items:

| Failed item                                      | Classification                                                                                                    |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------- |
| feature-marker check                             | re-validate and retry (unchanged)                                                                                 |
| outcome put                                      | `OutcomeConflict`, outranks all but the marker (unchanged)                                                        |
| a stream's head update                           | ordinary conflict; a **gap** (`SequenceGapError`) when the stored head is below `first - 1`, otherwise a conflict |
| an event put or snapshot put                     | ordinary conflict (unchanged). If the same stream's head update passed, an event sits above the head: head drift  |
| a fence's head check                             | fence conflict carrying the stream, head, and provenance (unchanged contract)                                     |
| `TransactionConflict`, throttling, indeterminate | unchanged                                                                                                         |

The gap is not detected before the write any more. It is detected on the failure path: after a
cancellation with a failed head update, the engine reads that stream's head item (`GetItem`, strongly
consistent, 1 RCU) and returns `SequenceGapError` if the stored head is below `first - 1`, else a
conflict. Where the SDK returns it, `ReturnValuesOnConditionCheckFailure = ALL_OLD` on the head update
carries the same answer without a read. The observable result is the same typed error with nothing
written, and the happy path loses its reads. Head drift (an event above the head, from a writer that
does not maintain heads) is reported as a conflict and flagged by `evt-doctor`; the engine cannot
repair it mid-commit.

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
  (a guarded version: zero rows is a conflict, or a gap if a follow-up read finds `head < $prev`);
- fence: `SELECT head … WHERE entity_id = $1`, absent as 0, which must equal `:head`.

The continuity query (`GREATEST(MAX(sequence), MAX(event_sequence))`), the floor query, and the two
fence queries go away. The advisory locks stay: they serialize writers and make the absent-row fence
(head 0) safe, since PostgreSQL cannot lock a row that does not exist. `entity_heads` becomes a read
of this table instead of an aggregate over the whole events table. The new relation name is
`{events}__evt_stream_heads`, 18 characters past the base, which fits the 63-byte identifier limit
with the existing 34-byte base cap.

PostgreSQL can remain conformant without this table (derived heads under the same locks are
equivalent), so it is the least essential part of the ADR; see the open questions.

### Readers and other consumers

- Event reads, `latest(n)`, snapshot reads, and compaction ranges are unchanged.
- Enumeration and rebuild scans already filter by `entityType`; a head item has none, so it is never
  enumerated. Heads scans (`pk, sk, eventSeq`) must skip `sk < 1` rows explicitly: Go's
  `headSeqFromItem` currently routes every `sk ≤ 0` row to the snapshot branch, and Rust's
  `item_head` returns any non-zero `sk` as the head, so `-1` would contribute `-1`. Both are
  harmless today and must be made explicit.
- Diagnostics that list a whole partition (Rust's `partition_items`) now return the head item.
- DynamoDB Streams consumers see a record per written stream per commit for `sk = -1`. A consumer
  that already filters on the presence of `type` (to skip the snapshot item at `sk = 0`) is
  unaffected; the Lambda `NEW_IMAGE` decoder (X3) and the ESM or Pipes filter helpers must skip
  `sk < 1`. Consumers that do not filter pay for the extra records.
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
commit once C10 lands. It couples two protocols (one already implemented), and it makes the
cancellation reason ambiguous between "revalidate" and "`RebuildInProgress`" without returned values.
Worth a follow-up; out of scope.

Recommendation: (a), with (b) as the fallback and (g) as a follow-up.

## Migration and compatibility

v0.9 is unreleased, so this is a change to the v0.9 format, not a v0.9-to-v0.10 upgrade. What needs
a path is **v0.8 storage**: a table of streams with no head items, written by v0.8 writers that know
nothing of the marker.

**Reading is compatible both ways.** A v0.8 reader never sees a head item (all its queries bound
`sk` at 0 or above) and its scans tolerate the row. **Writing is not.** A v0.8 writer appends events
without updating the head item. After that, the head item is stale (H2, H4 broken) and a v0.9 head
writer conflicts on the stream until it is repaired, and a fence on it passes although the stream
moved. The first is a liveness problem, the second a correctness one, and the marker cannot stop a
writer that does not read it. So v0.8 writers must be stopped before heads are declared. v0.9 itself
already asks for a quiet window (the reserved-namespace renames), so this adds a step to it rather
than a new kind of upgrade.

**The protocol:**

1. Deploy no v0.9 head writers yet, and stop every v0.8 writer.
2. Run `evt-doctor migrate-heads` (Go only, per ADR 0006). On DynamoDB it is an explicit scan
   (allowed: it is a migration), projecting `pk, sk, eventSeq, ttl`. Per non-reserved partition it
   computes the derived head (H4) and the `ttl` of the highest item, then writes the head item with
   `SET head = :h` conditioned on `attribute_not_exists(head)`. It never lowers an existing head: a
   head above the derived value is reported (events were deleted), a head below is repaired upward
   in `--repair` mode. It is idempotent and resumable, and `--dry-run` reports without writing.
3. `evt-doctor migrate-heads --verify` rescans and checks that every non-reserved partition's
   `head` equals its derived head.
4. The tool declares `stream-heads: 1` in the marker, through the ordinary declaration transaction
   (the `version` bump re-fails every cached writer).
5. Start v0.9 writers.

On PostgreSQL, `EnsureSchema` creates the empty heads table, and the migration is one transaction
under the exclusive feature advisory lock: `INSERT INTO … SELECT` the per-stream maximum of
`sequence` and `event_sequence` from both tables, then the declaration. The lock blocks v0.9 commits
for its duration; v0.8 writers must still be stopped.

**A required feature.** The marker today only stops a writer that does not know a declared feature.
This adds the other direction: a head-aware writer requires `stream-heads: 1` and otherwise refuses
to commit with a typed `StreamHeadsNotMigrated` error, so a v0.9 build pointed at a table nobody has
migrated fails loudly on its first commit instead of conflicting on every stream. On a table with no
marker row, a writer may declare `stream-heads` itself, but only after confirming the table holds no
non-reserved items (it reads until it finds one or exhausts the table; a populated table stops it
immediately). A fresh table therefore needs no step, and a populated unmigrated table can never be
declared by accident.

**Writers that predate the feature.** A v0.9 build without `stream-heads` refuses a table that
declares it (the existing rule), so a mixed fleet of old and new v0.9 builds cannot corrupt heads
after the declaration. v0.8 builds cannot be stopped; after declaration a stray v0.8 writer causes
conflicts (stale heads) and, for fenced streams, missed fences. `migrate-heads --repair` fixes the
heads once it is stopped.

**Rollback.** Stop the head writers, run v0.8 writers (they ignore head items), and to return, run
`migrate-heads --repair` before restarting head writers. The declaration is not removed.

**Rolling upgrade (not chosen).** A dual layout would let v0.9 writers keep the old checks until
the flip, so v0.8 writers could coexist during a deployment. It keeps both layouts' code, the
pinned-snapshot machinery, and the companion in two languages and three backends for a release, in
exchange for avoiding a short quiet window that v0.9 already needs. If a consumer cannot stop its v0.8
writers, this is the path; see the open questions.

**Compatibility fixtures that change.** Fixtures are generated by Go
(`go test ./internal/compat -run TestFixtures -update`), never hand-edited, and Rust must reproduce
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
  (classification keys), and `feature-marker` (declare, required refusal, fresh-table declaration);
  and new scenarios `stream-heads-migration` (an unmigrated table is refused, the migration runs,
  both languages then commit and fence) and `stream-heads-compaction` (compacting past the head
  neither moves the head nor conflicts with a current writer), plus a drift case with `appendRaw`.
- `compat/README.md` and the "Compat Scenarios to Add" table in the spec.

## Consequences

- **Cheaper in the common cases.** Fences cost half, appends without a snapshot lose a check, and
  the pre-commit reads disappear. The computed headroom for the dozen-and-nine command goes from 43
  to 64 projector operations. Billing no longer scales with snapshot size on the commit path.
- **A simpler protocol.** The continuation check, the floor check, the pinned conditions, the
  pre-commit observation, and the snapshot companion are deleted from both languages. The compaction
  ABA is not mitigated; it cannot occur.
- **A format change and a migration.** One new item per stream, one new feature, one new required
  refusal, one migration tool. Existing v0.8 tables need a quiet window.
- **One regression.** Creating a stream and writing its first snapshot in one commit costs one more
  action.
- **A new thing to keep true.** H1–H4 are invariants the engine must maintain and `evt-doctor` must
  check (head below derived, head above derived, orphan head item, missing head item). Anything that
  deletes or writes events outside the commit path (fixtures, repair tools) must maintain the head.
- **More stream records.** A head update per written stream per commit reaches DynamoDB Streams
  consumers.
- **Retention is unchanged.** Fences stay refused for retained types.

## Risks and open questions

1. **Non-rolling upgrade from v0.8.** Is there a deployment that cannot stop its v0.8 writers for the
   migration window? If so, the dual layout is needed. This is the main question for approval.
2. **Condition-check billing.** The WCU numbers assume a `ConditionCheck` bills by the size of the
   item it reads. The implementation must confirm it on real DynamoDB before the numbers are quoted
   anywhere else. The action-count result stands either way.
3. **Retention.** Do we accept `ttl` on the head item for retained types and keep fences refused, or
   should this ADR carry an incarnation token to lift the refusal now?
4. **PostgreSQL.** Include the heads table now (parity, cheaper `entity_heads`) or leave PostgreSQL
   on derived heads, which is conformant? The recommendation is to include it so the spec has one
   definition of head, but it can be split into a follow-up without affecting DynamoDB.
5. **Drift is loud but not self-healing.** A writer that appends without maintaining the head fails
   closed (conflicts, `evt-doctor` flags it), except for fences, which can pass. That is why the
   v0.8 writers must be stopped.
6. **The extra action on create-with-snapshot.** Cheap to accept, but it is the one place the new
   layout is worse, so say so in the release notes.
7. **Required-feature refusal is new marker behavior** (a writer refusing because a feature is
   missing). It needs its own conformance cases in `feature-marker`.

See [ADR 0004](0004-core-reshape-multi-stream-transactions-and-outcomes.md) for the layout this
amends and [ADR 0001](0001-event-compaction-and-snapshot-truncation.md) for compaction.
