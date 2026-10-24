# Migrating a v0.8 store to stream heads

This is the operator runbook for `evt-doctor migrate-heads`. A table or database written by v0.8
has streams with no [head items](../reference/storage-format.md#stream-heads), the small per-stream
row a commit conditions on. A head-aware writer refuses such a store
(`*evt.StreamHeadsNotMigratedError`, or `Error::StreamHeadsNotMigrated` in Rust) until the heads
are backfilled and verified and the required `stream-heads` feature is declared. The rules this
procedure implements are in the storage format's
[Migration](../reference/storage-format.md#migration) section and
[ADR 0007](../adr/0007-per-stream-head-item.md#migration-and-compatibility).

You only need this for a store that already holds streams written by v0.8. A fresh table or
database needs no step, because its first head-aware writer declares the feature. A store that
already declares `stream-heads` is migrated, and `-verify` checks it.

The migration is stop-the-world and one-way. No v0.8 writer may run against the store once its
first head-aware writer has run, and there's no rollback in place. The way back is the backup you
take in step 1.

The tool is Go only. Rust services need nothing from it, since they run against a migrated store
like any head-aware writer, and Rust has no `migrate-heads` command.

## Before you start

### 1. Stop writers, enforce it, and take a backup

Stop every v0.8 writer and start no head-aware writer. Then enforce that nothing writes except
the migration role. The tool cannot see who else writes to the store, so this deny is your evidence
that writers stayed stopped between the verification and the declaration.

On DynamoDB, deny the write actions for every principal except the role that runs the migration,
and keep the deny until step 6. An identity policy on each writer principal, a table resource
policy, or a service control policy all work. DynamoDB has no setting that stops writes by capacity.
Transactional writes are authorized by the item-level actions, so denying those covers
`TransactWriteItems` too:

```json
{
  "Effect": "Deny",
  "Action": [
    "dynamodb:PutItem",
    "dynamodb:UpdateItem",
    "dynamodb:DeleteItem",
    "dynamodb:BatchWriteItem",
    "dynamodb:ConditionCheckItem"
  ],
  "Resource": "arn:aws:dynamodb:REGION:ACCOUNT_ID:table/EVENT_LOG_TABLE",
  "Condition": {
    "StringNotEquals": { "aws:PrincipalArn": "arn:aws:iam::ACCOUNT_ID:role/MIGRATION_ROLE" }
  }
}
```

Take an on-demand backup, or confirm point-in-time recovery is on and note the time. A rollback
needs it. On PostgreSQL, take a backup the way you take any (`pg_dump`, a volume snapshot) and
revoke writer access or stop the writers.

### 2. Upgrade readers

On DynamoDB, head items are rows at `sk = -1` in the stream's own partition. Every reader that
reads the table without a key filter has to skip them before the first head item exists, because
the backfill writes one per stream:

- Scan, rebuild and enumeration jobs need a build that skips `sk < 1`, which this module does.
- Stream consumers skip `sk < 1`, or filter at the event source mapping or pipe
  (`{ "dynamodb": { "Keys": { "sk": { "N": [{ "anything-but": ["-1", "0"] }] } } } }`). The
  backfill emits one stream record per stream. See
  [Head items are not events](streams.md#head-items-are-not-events).
- For `evt-doctor` itself, use the build from the release you're migrating to.

PostgreSQL has no scan jobs or streams to upgrade.

### 3. Permissions and cost

On DynamoDB the migration role needs `dynamodb:GetItem`, `Scan`, `UpdateItem` (for the head
items) and `PutItem` (for the marker declaration) on the event-log table. On PostgreSQL it needs to create the engine tables (the migration runs `EnsureSchema`) and to `LOCK
TABLE` the events and snapshots tables, so use the table owner or a superuser.

The DynamoDB tool reads the whole table with strongly consistent scans: a full backfill pass and
three verification passes (after the backfill, immediately before declaring, and after declaring).
So budget four scans of the table. Each head item is one write of under 1 KB. On a large
table, run it in a quiet window and raise `-page-size` or `-concurrency` only with capacity to
match. The tool holds one small entry per stream in memory.

### 4. Rehearse

Run the migration against a restored copy of the backup first, with the same build. `-dry-run`
scans and counts without writing anything:

```sh
evt-doctor migrate-heads -dry-run -events-table EVENT_LOG_TABLE
```

```text
migrate-heads: backfill
migrate-heads: 48210 stream(s): would write 47960 head item(s); 0 already had one, 250 empty or expired, 0 at head 0, 0 raced, 12 without a ttl because their items mix ttls
migrate-heads: dry run; nothing was written
```

## Run it

### DynamoDB

```sh
export AWS_REGION=us-west-2
evt-doctor migrate-heads -events-table EVENT_LOG_TABLE -confirm-writers-stopped
```

`-confirm-writers-stopped` is required. It's your statement that step 1 holds. The tool prints the
checklist and then runs, in order:

1. Backfill. Scan the table, accumulating per partition across the whole scan (a partition
   split by a page boundary is finalized when the scan ends). Ignore items whose `ttl` has passed
   but which are not yet deleted. Skip reserved namespaces (`SYSTEM#`, `command-outcome#`), a
   partition with nothing left, and a partition whose derived head is 0 (head items start at 1).
   For every other partition write the head item `SET head = :h` conditioned on
   `attribute_not_exists(head)`. The derived head is the larger of the highest event sequence and
   the snapshot's `eventSeq`, so a stream compacted down to its snapshot gets the snapshot's
   sequence. It never overwrites a head item.
2. Verify. Rescan and require, for every partition with a non-expired item, a head item equal
   to the derived head.
3. Verify before declaring. The same check once more, immediately before the declaration.
4. Declare. The ordinary declaration transaction adds `stream-heads` (format 1) to the feature
   marker and bumps its version by one.
5. Verify after declaring. One more verification, to catch a writer that was not really stopped.

The run stops without declaring the moment a verification finds a stream whose head doesn't match
its log, and it prints those streams. It's idempotent and resumable. After an interruption (a
crash, a throttle, a lost credential), rerun the same command. Head items already written are left as
they are, and the rerun writes the rest.

Retained types need one more rule. A head item for a stream whose items all carry a `ttl` gets the
largest of them, so the head item is never deleted before the stream. A stream with a `ttl` on some
items and none on others (because a retention policy was added or removed after the stream began)
gets a head item with no `ttl` at all. A head item that expired first would leave the rest of the
stream without one.
The summary line counts these as "without a ttl because their items mix ttls".

The tool refuses some stores and runs before writing anything:

- A table whose feature marker declares `journal`, or any feature the Go tool doesn't model, fails
  with `store declares unsupported features`. The Rust-first extensions keep rows it would misread.
- A run without `-confirm-writers-stopped` is refused.
- A store holding a stream in a reserved namespace is refused. That's an entity ID starting
  `SYSTEM#` or `command-outcome#` that isn't an engine row (entity IDs were never restricted before
  v0.9). Head-aware code hides those partitions from enumeration, heads and rebuild scans, so the
  stream would be left without a head item under a declaration. Rename (migrate) it first, which
  is the same upgrade step the plain `evt-doctor` reserved-namespace check asks for, then rerun.
  The check runs again before the declaration, and `-verify` and `-dry-run` report it too.
- A store that already declares `stream-heads` has nothing to migrate, so the command verifies the
  store instead and exits with the verification's status. A rerun after an interruption that came
  after the declaration ends here.

### PostgreSQL

```sh
export DATABASE_URL='postgres://USER:PASSWORD@HOST:5432/DATABASE'
evt-doctor migrate-heads -backend postgres -events-table evt_events -snapshots-table evt_snapshots -confirm-writers-stopped
```

The tool runs `EnsureSchema` (which creates the empty head table), then does the backfill, the
verification and the declaration in one transaction under the exclusive feature advisory lock:
it inserts each stream's head (the larger of its highest event sequence and its snapshot's event
sequence, skipping 0, `ON CONFLICT DO NOTHING`), requires that no stream's row is missing, lower or
higher than the derived head, and declares `stream-heads`. Any mismatch aborts the transaction and
leaves the database exactly as it was. The transaction also takes a `SHARE` lock on the events and
snapshots tables, so a v0.8 writer with a transaction open finishes first and one that arrives
meanwhile waits. That is a courtesy for the moment of the migration. It cannot stop a v0.8 writer
that starts afterwards, so step 1 still applies. `-dry-run` and `-verify` take no lock. They read
one consistent snapshot and neither wait for writers nor make them wait.

## Verify and repair

`-verify` is read-only and exits non-zero on any mismatch. Run it after the migration, and any time
you suspect a stray writer:

```sh
evt-doctor migrate-heads -verify -events-table EVENT_LOG_TABLE
```

| Finding    | Meaning                                                                                          | Fix                                                |
| ---------- | ------------------------------------------------------------------------------------------------ | -------------------------------------------------- |
| `missing`  | The stream has events or a snapshot and no head item (a stream created by a v0.8 writer).         | `-repair`                                          |
| `lower`    | The head is below the derived head (a v0.8 writer appended without it).                           | `-repair`                                          |
| `higher`   | The head is above the derived head (events were deleted, or the item was edited).                 | `-repair -allow-lower`, writers stopped            |
| `ttl-low`  | DynamoDB only: the head item's `ttl` is below an item of its stream, or the stream never expires. | `-repair`                                          |
| `orphan`   | A head item with no events or snapshot left. On DynamoDB a head item with a `ttl` is only a warning (a retained stream's head outlives the rest until it expires). One without a `ttl` never expires and would make a create of that stream ID fail with drift, so it is a failure. Any orphan row on PostgreSQL is a failure. | Wait for the `ttl`, or delete the item or row by hand (`-repair` leaves it). |

A head over a snapshot of event sequence 0 has no derived head to lower to (heads start at 1), so
`-repair` reports it and leaves it for you.

```sh
evt-doctor migrate-heads -repair -confirm-writers-stopped -events-table EVENT_LOG_TABLE
evt-doctor migrate-heads -repair -allow-lower -confirm-writers-stopped -events-table EVENT_LOG_TABLE
```

`-repair` raises a low head, creates a missing one, and fixes a low `ttl`. Each write is
conditioned on the head it observed (`head = :observed`), and on the observed `ttl` too when it
changes the `ttl`. So a head or `ttl` that changed since the scan, say because a writer refreshed
the `ttl` without moving the head, is never overwritten. A write that loses its condition is left
alone and counted as raced. The repair then verifies again and exits 1 if any stream still
disagrees with its log, in which case rerun it.

`-allow-lower` also writes `SET head = :derived` for a head above the derived value. Lowering breaks
the rule that a head never decreases, and an in-flight fence reads the head, which is why it needs
every writer stopped. A plain `-repair` never lowers anything. Both modes take `-dry-run`. Repair
doesn't declare the feature, so rerun the migration afterwards if the store isn't declared yet.

On PostgreSQL the lists in a report are capped at 1000 entries, but the counts behind the exit
status aren't. A finding past the cap still fails `-verify` and a repair dry run.

A plain `evt-doctor` run (no subcommand) reports head drift too, once the store declares
`stream-heads`: a head below, above, or missing, a `ttl` that is too low, and an orphan head item
without a `ttl`. It exits 1 when it finds any of them, and when it cannot run the check (a denied
read, say). Orphan head items that have a `ttl` are listed as warnings and do not fail the run. A
store that declares a feature the check does not model (`journal`) is skipped.

## Start the new writers

6. Start the head-aware writers. Then lift the step-1 deny.

From here on, no v0.8 writer may run against the store again. A v0.8 writer that runs after
the declaration appends without the head item, which leaves it stale. The head-aware writers then
fail that stream with `*evt.StreamHeadDriftError`, and a fence on it can pass although the stream
moved. Pre-heads v0.9 builds refuse the store (they do not model the feature). If a stray writer
did run, stop it and use `-repair`.

## Rollback

There is no rollback in place. The declaration is never removed once a writer has committed under
it, a pre-heads build refuses the table, and v0.8 full-table scans fail on head rows. To return to
the old layout, restore the backup from step 1. On DynamoDB, restore the on-demand backup or the
point-in-time recovery to a new table and point your v0.8 services at it. On PostgreSQL, restore the
dump. Anything the new writers committed after the migration is not in that backup. Plan the
migration as one-way.

## Flags

| Flag                        | Meaning                                                                                  |
| --------------------------- | ---------------------------------------------------------------------------------------- |
| `-backend`                  | `dynamodb` (default) or `postgres`.                                                      |
| `-events-table`             | Event-log table (`evt-local-event-log` / `evt_events` by default).                        |
| `-snapshots-table`          | PostgreSQL snapshots table (`evt_snapshots`).                                            |
| `-region`, `-endpoint`      | DynamoDB region and an emulator endpoint.                                                |
| `-dsn`                      | PostgreSQL connection string (also `DATABASE_URL`).                                      |
| `-confirm-writers-stopped`  | Required to write: step 1 holds.                                                         |
| `-dry-run`                  | Scan and count, and write nothing.                                                       |
| `-verify`                   | Read-only verification.                                                                  |
| `-repair`, `-allow-lower`   | Repair, and also lower a high head.                                                      |
| `-page-size`                | DynamoDB scan page size in items (0 lets DynamoDB fill a 1 MB page).                     |
| `-concurrency`              | Concurrent DynamoDB head writes (default 8).                                             |
| `-max-findings`             | Findings to list before summarizing the rest (default 50).                               |

Exit status: 0 success, 1 a failure or a mismatch, 2 a refused invocation (a missing confirmation or
conflicting flags).
