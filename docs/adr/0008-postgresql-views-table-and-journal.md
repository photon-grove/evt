# ADR 0008: A PostgreSQL views table, and the journal on PostgreSQL

## Status

Accepted (2026-10-29). Supersedes the "Postgres view store: do not build" verdict of
[ADR 0006](0006-parity-policy-and-feature-cuts.md#feature-verdicts). Tracking: the matrix rows
"PostgreSQL views table" (P12), "Journal and journal compaction on PostgreSQL" (X4) and
"PostgreSQL journal wake-ups" (X5) in
[`docs/reference/feature-matrix.md`](../reference/feature-matrix.md). The normative text
is the [PostgreSQL views table](../reference/storage-format.md#postgresql-views-table) and
[Journal on PostgreSQL](../reference/storage-format.md#journal-on-postgresql) sections of the
storage spec. The views table and `PgViewRepository` are implemented; the commit target, the
journal, compaction and wake-ups follow, all in `photon-evt` 0.3.

### Amendment (2026-10-29): the encoding check and the repository's key refusal

Implementing the view repository changed two details of the decision below:

- **Encoding.** `ensure_schema` creates the views table on any database encoding, so a store
  that never uses its views is unaffected by them. The view repository and `with_views_table()`
  check that the encoding is `UTF8` once, on first use, and refuse any other with
  `InvalidConfig`.
- **Refusals outside the commit path.** The view repository refuses an oversized key with
  `InvalidArgument` (`ViewKeyTooLong` names a projector and an operation, which a repository write
  does not have), and it refuses U+0000 in any key, owner, entity type or payload with
  `InvalidArgument`, because PostgreSQL `text` cannot hold it. Both are listed under the storage
  spec's known differences.

## Context

ADR 0006 cut a PostgreSQL view store at 10%: no consumer, and views could live in the commit path
through an application's own tables (C7). Since then:

- **There is a consumer.** A downstream service runs on DynamoDB in production and wants the same
  code on PostgreSQL for local development and self-hosted deployments. It reads views through
  `ViewRepository`, journals its commits (X1), compacts its journal (X2), and wakes realtime
  delivery from journal writes (X3). On PostgreSQL today it gets the event log and nothing else.
- **The journal cannot run on an application's tables.** On PostgreSQL, commit-wide view
  operations go through a view writer the application supplies
  (`PostgresRepository::with_view_writer`), because evt defines no PostgreSQL view schema. That is
  enough for projector rows the application reads itself. It is not enough for the journal:
  - Its rows, counters and compaction rows are view rows evt itself must read back (`page`,
    `high_watermark`, the floor, the lease).
  - Its counter write is a compare-and-set on the exact payload the commit read. A view writer
    turns one operation into one statement and cannot express it, so the repository refuses it
    (`InvalidViewOperation`, and `applies_expected_payload()` is false).
  - PostgreSQL implements none of `JournalStore`, `JournalCompactionStore` and `CompactionStore`,
    so a `JournalRepository` over it has no `Repository` impl (that impl needs
    `FeatureMarker + JournalCompactionStore`) and cannot commit, page or compact.
- **Wake-ups are DynamoDB-shaped.** A consumer learns that a scope has new rows from the views
  table's DynamoDB stream (`KEYS_ONLY` wake-ups, `photon-evt-lambda`). PostgreSQL has no stream,
  so there is no wake-up at all.

Under ADR 0006's thresholds a feature with a consumer in the window is a port. Hand-rolling these
pieces in the consumer would break the rule ADR 0004 and ADR 0005 share: evolve evt, never
hand-roll it in the app.

## Decision

### P12: an evt-owned PostgreSQL views table

`ensure_schema` creates one more derived relation, `{events}__evt_views`, with the five view
attributes and `ttl` as columns (the database must be `UTF8`; see the amendment above):

- `pk` and `sk` are `text COLLATE "C"`. The primary key `(pk, sk)` then orders by bytes, which is
  DynamoDB's sort-key order, so the journal's 20-digit positions page in numeric order on both
  backends.
- `payload` is `text`, not `jsonb`. The journal's digests and the compare-and-set bind exact
  payload bytes, and `jsonb` normalizes them.
- An index on `(entity_type, pk, sk)` serves lists by entity type (DynamoDB's `entityType-index`),
  and a partial index on `ttl` serves `purge_expired` without a table scan.
- The derived names fit the 34-byte limit on the events table name: the longest,
  `{events}__evt_views_type_idx`, adds 20 bytes, under the existing 29.
- One deliberate difference: a btree entry holds at most 2,704 bytes, so a row whose entity
  type and keys together exceed 2,600 bytes is refused (`ViewKeyTooLong` on the commit path,
  `InvalidArgument` from the view repository), where DynamoDB would accept a 2,048-byte `pk` with a
  1,024-byte `sk`.

`PgViewRepository` implements `ViewRepository` over it, with the DynamoDB repository's semantics:
unconditional puts and deletes outside the commit path, `get_view_at`, lists by partition and by
entity type, and the same cursor encoding (the last row's keys as a sorted JSON object in padded
base64url). Reads are strongly consistent, including lists by entity type (stronger than
DynamoDB's global secondary index, which the contract allows). `ttl` is stored and returned as
on DynamoDB. Rows are not expired automatically: `purge_expired` deletes rows whose `ttl` has
passed, run by an operator or a scheduled job, the way DynamoDB's TTL sweep runs on its own.

`PostgresRepository::with_views_table()` makes this table the commit's view target, next to the
existing `with_view_writer` for application-owned tables (a repository has one or the other).
With it, commit-wide view operations get exactly the DynamoDB owner rules, built by evt rather
than by a writer, and the repository applies the expected-payload condition
(`applies_expected_payload()` becomes true). The application view writer is unchanged and still
refuses the condition. Each evt-built statement is sized in the commit budget as the DynamoDB item
it mirrors, so `CommitTooLarge` fires at the same commit on both backends.

These transactions run at `READ COMMITTED`, set explicitly: their fence reads follow
advisory-lock waits, and a `REPEATABLE READ` snapshot taken before the wait would read a stale
rebuild generation or counter.

The repository declares a `postgres-views` feature on the marker, so a writer that would put
commit-wide rows somewhere else (a Go writer, or a Rust repository with `with_view_writer`)
refuses the store instead of splitting its views across two tables.

### X4: the journal and its compaction on PostgreSQL

With the views table, `PostgresRepository` implements `JournalStore`, `JournalCompactionStore` and
`CompactionStore`, so `JournalRepository<PostgresRepository>` gets its `Repository` impl and behaves
as it does over DynamoDB:

- **Rows are the same rows.** Journal rows, counters and compaction rows have the same `pk`, `sk`,
  owner, `entity_type` and payload bytes as the DynamoDB items, column for attribute. The
  Rust-generated `compat/fixtures/journal.json` therefore pins both backends, and no new fixture
  format is needed.
- **The commit.** Row puts are owner-guarded inserts (`absent`). The counter put is the
  compare-and-set: an insert when the commit read no counter, otherwise an update guarded by owner
  and `payload = :expected`. A count of zero fails the condition, which is contention, exactly as
  a failed `ConditionCheck` is on DynamoDB.
- **Reads** are single statements: the counter, the highest row, and a page above a position,
  ordered by `sk`.
- **Compaction writes** (`write_compaction`, at most 99) are one transaction. It takes the shared
  rebuild advisory lock, checks the epoch against the generation row, locks each key it touches,
  checks its `Expect`, and guards each write again in its own statement (an `Absent` put is a
  plain insert, a delete carries its owner). Any failure rolls the whole batch back, the
  transactional equivalent of DynamoDB's `TransactWriteItems`. Batches of one scope serialize on
  the scope's lease row, which is what makes an absent key safe to check.

### X5: wake-ups through `NOTIFY`

The commit transaction runs `pg_notify('{events}__evt_journal', <scope key>)` once per scope it
wrote, in ascending scope order. PostgreSQL delivers notifications only at `COMMIT`, in commit
order, and drops them on rollback, so a wake-up never names a write that did not happen. A
`JournalListener` holds a dedicated connection with `LISTEN` and yields scope wake-ups.
Notifications are on by default with `with_views_table()`; `with_wakeups(false)` turns them off for a
deployment that only polls.

As on DynamoDB, a wake-up is not a delivery: the consumer pages the scope from its own cursor.
Notifications are lost while a listener is disconnected, so the listener reports every reconnect,
and the consumer pages each of its scopes then and on a polling interval as a backstop. A scope
key longer than `NOTIFY`'s payload limit is sent as an empty payload, meaning "any scope may have
changed".

### Rollout

- All three are Rust-first extensions under ADR 0005's rules (the matrix lists them in the `ext`
  tier). The spec sections and the existing
  Rust-generated fixtures come first, then the implementation. The backend conformance suite runs
  its view, journal and compaction scenarios on PostgreSQL as well as in memory and on DynamoDB.
- Go keeps application-owned view writers. A Go writer already refuses a store that declares
  `journal`, so a mixed Go/Rust deployment cannot write a journal without its counter.
  Porting P12 and X4 to Go follows the deferral rule: on a Go consumer, from this spec and the
  fixtures.
- Planned order: the spec (this ADR's PR), P12 with the compare-and-set, X4 reads, X4 compaction,
  then X5, released together as `photon-evt` 0.3.

## Consequences

- PostgreSQL becomes a full backend for the Rust extensions, not only an event log. A service can
  run the same commands, views, journal and compaction on DynamoDB, in memory, and on PostgreSQL.
- evt now owns a PostgreSQL view schema. Applications that keep their own view tables lose
  nothing (`with_view_writer` stays), but the journal is available only with `with_views_table()`.
- The views table and the events share one database, so a commit's view rows are atomic with its
  events through a single transaction rather than through `TransactWriteItems`' 100-action limit.
  The engine still enforces the DynamoDB budget, counting evt-built view statements as their
  DynamoDB items, so a commit refused on one backend for size is refused on the other too.
- `NOTIFY` adds one statement per written scope to a commit. Commits that journal nothing add none.

## Alternatives considered

- **Keep app-owned view tables and give the view writer a compare-and-set hook.** It would make
  the counter write possible, but every application would still have to implement the journal's
  reads, the floor and the compaction batch over its own schema, and keep their semantics in step
  with the spec. That is hand-rolling with extra steps.
- **Store views as `jsonb`.** Queryable, but it rewrites payload bytes, which breaks the journal's
  digests and the compare-and-set. An application that wants queryable JSON can still project
  into its own tables with a view writer.
- **A polling-only consumer, without `NOTIFY`.** Simpler, but every idle scope costs a query per
  interval, and latency is the interval. `NOTIFY` makes the common case immediate, and polling
  stays as the backstop for what `NOTIFY` cannot guarantee.
- **Logical replication as the wake-up source** (closest to a DynamoDB stream). It needs a
  replication slot, extra privileges and server configuration, too much for local and small
  self-hosted deployments, which are the reason for this backend.

## Risks and open questions

- **`NOTIFY` and commit throughput.** A transaction that notifies takes a cluster-wide lock at
  `COMMIT` to queue its notifications, which serializes notifying commits across every database
  of the server. That is fine at the consumer's scale. A busy deployment can turn wake-ups off
  (`with_wakeups(false)`) and poll.
- **A stalled listener fills the queue.** PostgreSQL keeps notifications until every listening
  session has consumed them. A listener that stays connected but stops reading lets the queue
  (8 GB by default) fill, and then every notifying commit fails at `COMMIT`. `JournalListener`
  must keep draining, and a consumer that cannot keep up should disconnect and rely on polling.
- **TTL is no longer best-effort-by-default.** On DynamoDB expired rows disappear on their own;
  here they stay until `purge_expired` runs. The guide will say so, and readers do not filter
  expired rows on either backend, so the observable contract is unchanged.
