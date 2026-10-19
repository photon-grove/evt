# ADR 0004: Core reshape — multi-stream transactions, command outcomes, and rebuild fences

## Status

Accepted (2026-10-13). Capabilities C1–C11 of the vNext plan: the core reshape — multi-stream
transactions, read fences, command outcomes, rejections, fence-aware retry, metadata additions,
commit-wide projectors, `latest(n)` and reserved namespaces, the feature marker, the rebuild
fence, and shared conformance. Tracking:
[`docs/parity/feature-matrix.md`](../parity/feature-matrix.md).

_Amended by [ADR 0007](0007-per-stream-head-item.md): a per-stream head item replaces the
continuation check, the floor check, and the fence's snapshot companion in C1 and C2._

## Context

Two consumers of evt have grown capabilities the framework never offered, and each grew them by
hand:

- **A downstream Rust game service** built its own command engine on raw evt writes:
  per-aggregate counter rows to sequence every aggregate's writes, per-entity dedupe, and no
  shared conformance story between its memory and DynamoDB stores.
- **A downstream Go service on evt** hand-rolls command reservations keyed by an identity digest,
  canonical receipts, ambiguous-write recovery, and a replay guarded by a hand-passed
  writers-quiesced flag.

Meanwhile the framework itself only executes one aggregate per commit:

- Go: `evt.Repository.Commit` takes a single entity's `SerializedResult`
  ([`repository.go`](../../repository.go)); `CommitStream` batches many single-entity commits, but
  each is its own transaction ([`dynamo/commit.go`](../../dynamo/commit.go)).
- Rust: `photon_evt::repository::Commit::new(entity_id, events)` is likewise single-entity
  ([`crates/evt/src/repository.rs`](../../crates/evt/src/repository.rs)).

At the same time the duplicate-command check only covers events replayed since the latest snapshot
(`Loaded::has_seen`, `crates/evt/src/store.rs`; the dedupe window is documented in
[`docs/spec/storage-format.md`](../spec/storage-format.md)), so a command whose first attempt also
wrote a snapshot can run twice. Rebuilds have no fence: a rebuild and a live writer can interleave
unchecked. And retry is blind: `policy.ExecuteWithRetry` (Go,
[`policy/commit.go`](../../policy/commit.go)) and `execute_with_retry` (Rust,
`crates/evt/src/store.rs`) re-run the whole command on every conflict, even though a conflict on a
fence the engine filled — rather than a fence the command read — means the command's decision is
still valid.

The fix is one rule: **when an event-sourced app needs a capability evt lacks, evolve evt — never
hand-roll it in the app.** This ADR records what the core gains and what it costs in the storage
format. Plan IDs C1–C11 name the capabilities; they are used verbatim below, in the feature matrix,
and in PR titles.

## Decision

The core gains multi-stream transactions, read fences, durable command outcomes with standard
rejections, fence-aware retry, richer metadata, commit-wide projectors, bounded newest-first reads,
a feature-marker row, and a rebuild fence. Every one of these is **core**: it lands in Go (`evt`
v0.9.0) and Rust (`photon-evt` 0.2), in the same release window, behind the same storage format.

Per plan decision D1 (the commit path and the storage contract are dual-language obligations),
anything in the commit path or storage contract lands in **both** languages:

- Go generates the core golden fixtures (`go test ./internal/compat -run TestFixtures -update`),
  as it does today.
- Both implementations run the shared conformance suite, extended to cover C1–C10, and both pass
  the live interleaved cross-language run (`scripts/compat-integration.sh dynamo|postgres`).
- The spec ([`docs/spec/storage-format.md`](../spec/storage-format.md)) gains byte-level examples
  for every new item shape before either implementation ships.

Single-aggregate execution stays: Go's `Store`/`Execute` and Rust's `Store::execute` remain the
shortcut over the general transaction, not a second code path.

### C1 — Multi-stream transactions

**Current state.** Go commits one entity per `Commit` call; `CommitStream` flushes batches of
independent single-entity transactions (`dynamo/commit.go`). Rust's `Commit` builder carries one
entity's events (`crates/evt/src/repository.rs`). A command that must append to two aggregates —
claiming a unique handle while updating the owning profile, where `user#<subject>` and
`username#<handle>` change together — cannot commit atomically today.

**Decision.** A caller loads a **typed read set** (a set of aggregate instances with their
sequences and heads), appends events to any subset of them, and commits once. Every written stream
keeps its existing protections: `attribute_not_exists(sk)` on each event put plus the snapshot
floor check from ADR-0001, per stream. Unwritten streams in the read set contribute **read fences**
(C2) instead of event puts. The commit is atomic per backend: one `TransactWriteItems` (≤ 100
items, existing limit accounting applies) on DynamoDB, one SQL transaction on PostgreSQL, and
sequential consistency on the in-memory backends.

**Storage format.** No new item shape; the existing event and snapshot item shapes are unchanged.
The commit gains a second kind of transaction item (the C2 fence `ConditionCheck`), and the
per-stream sequence-contiguity and floor rules apply per written stream.

**API shape (illustrative, not final).**

```go
// Go: a transaction reads aggregates, mutates some, and commits once.
outcome, err := store.Execute(ctx, store.Txn().
    Read(account),          // read + write: appends events
    Read(claim).Fence(),    // read-only: fenced
    cmd, metadata)
```

```rust
// Rust: the same shape through a builder. Each read is typed by its own aggregate, so one
// transaction mixes aggregate types.
let mut tx = store.transaction();
let mut account = tx.read::<Account>(&id).await?;       // read + write
let mut claim = tx.read::<Claim>(&claim_id).await?;       // written with the account
tx.read::<Username>(&username_id).await?;                    // read-only: engine-filled fence
tx.append(&mut account, account_events)?;
tx.append(&mut claim, claim_events)?;
let committed = tx.commit(&metadata).await?;            // one atomic commit, one commitId
```

### C2 — Read fences

**Current state.** Nothing in either implementation fences a read-only aggregate. A downstream
service kept per-aggregate counter rows to sequence every aggregate's writes — mutable state with
no backing event, exactly what the core discipline forbids.

**Decision.** A read-only stream in a transaction is fenced with a `ConditionCheck` on
`(pk, head + 1)` asserting `attribute_not_exists(sk)`: the check fails if any writer has appended
past the head the reader saw. The slot check alone is not enough once compaction exists: another
writer could append `head + 1`, snapshot it and compact it away, leaving the slot empty again (an
ABA). So the fence pairs the slot check with a second `ConditionCheck` on the stream's `sk = 0`
snapshot item asserting `attribute_not_exists(sk) OR eventSeq <= :head`; compaction only deletes
events covered by a snapshot, so any compaction past the reader's head shows up there. A fresh
load re-derives the head as the larger of the highest event sequence and the snapshot's
`eventSeq`, so `head + 1` is a free slot on the reader's view. A fenced stream can also be written in the same commit; then the event put at
`head + 1` makes the fence redundant and it is omitted.

**Storage format.** The fence is two `ConditionCheck` transaction items — the slot
`(pk, sk = head+1)` and the snapshot item `(pk, sk = 0)`. The snapshot companion is always
included: any store instance can snapshot the stream and `CompactBelow` compacts whatever a
durable snapshot covers, so no interval configuration durably disables compaction (E1.1 pins the
exact rule). Each bills as a write and
counts toward the 100-item limit, like the existing floor check
([`dynamo/commit.go`](../../dynamo/commit.go), `buildFloorChecks`). A `ConditionalCheckFailed` on a
fence is a conflict (another writer moved the stream), classified and retried like an event-put
conflict. PostgreSQL fences cannot rely on row locks: the slot at `head + 1` does not exist at fence time,
and `SELECT … FOR KEY SHARE` locks only rows that were actually returned, so it cannot protect an
absent key. Instead, every fenced stream takes the same per-entity transaction-scoped advisory
lock the writer path already takes (`pg_advisory_xact_lock(1702261760, hashtext($1))` with the
entity ID), then asserts `max(sequence) <= head` over the stream's events — and the snapshot's
`eventSeq <= head` — inside the commit transaction. _Errata (E1.1):_ the events predicate is an
upper bound, not equality, matching the storage spec — a fully compacted stream whose snapshot
sits at the fenced head has `max(sequence) < head` and is unchanged, not moved, so equality would
wrongly reject it. A concurrent writer blocks on the advisory
lock rather than slipping `head + 1` past a fenced read. The in-memory backends check
sequentially. Fences are declared per read; a fence the caller fills from outside the engine is a
caller-stamped fence (see C5).

### C3 — Command outcomes

**Current state.** The dedupe window is "events replayed since the latest snapshot"
(`crates/evt/src/store.rs` `Loaded::has_seen`; Go's snapshot-aware load), so a command ID folded
into a snapshot is no longer checked and the repeat applies twice — documented in
`crates/evt/README.md` and the spec's known differences. Downstream services hand-roll
reservations, receipts, and per-entity dedupe instead.

**Decision.** The framework owns durable, global command outcomes. A commit on a store with
outcomes enabled writes a row on a global `command-outcome#<id>` stream in the same transaction as
the events:

- **Command ID required**: a store with outcomes enabled requires a command ID on every command.
  A missing ID is a programmer error returned before the commit — no outcome row, no events.
  Stores built without outcomes keep today's behaviour.
- **Identity digest**: pluggable — the caller may supply a digest function over the command's
  identity. A repeat with the same digest returns the stored outcome; a repeat with a different
  digest is an `IdempotencyKeyReuse` rejection. The default digest is SHA-256 over a canonical
  encoding of (command type, target stream IDs, canonical command payload JSON, principal), so two
  different requests that reuse one command ID carry different digests and the
  `IdempotencyKeyReuse` branch is reachable without a custom digester.
- **Stored response**: the outcome row stores the command's response payload so a retried command
  returns the original answer, not just "accepted".
- **Status**: `accepted` or `rejected` (C4), written **once, in its final state** — digest,
  response, event IDs, and the rejection when applicable — inside the commit transaction, with a
  `Put` conditioned on `attribute_not_exists(pk)`. There is no intermediate "reserved" state:
  DynamoDB's `TransactWriteItems` allows only one action per item, so a reserve-then-resolve pair
  inside one transaction is not expressible, and a single final-state write needs no resolution
  step. A rejected outcome is terminal for that digest.
- **Ambiguous-write recovery**: after an indeterminate commit error (timeout, lost connection —
  the caller cannot know whether the transaction applied), re-read the outcome row by command ID:
  present with the **same identity digest** → the commit won; return the stored response. Present
  with a **different digest** → `IdempotencyKeyReuse`. **Absent** → the commit did not apply;
  safe to retry.
- **Outcome-only commits**: a command can record an outcome without appending events, for commands
  whose effect is a decision about state rather than a change to it.
- **Accepted no-op**: a command that produces zero events is recorded as **accepted with no
  events**, not an error. Go's current "empty commit is a no-op" (`Commit` returns nil on empty
  events) becomes an observable, durable outcome.

This replaces per-entity dedupe: the `SeenCommandIDs`-style window (both languages) is superseded,
though it remains as a cheap pre-check on load.

**Storage format.** New item shape on the shared table (same `pk`/`sk` pattern as every stream):

| Attribute   | Type | Outcome item (`pk = command-outcome#<id>`)                                                                                                |
| ----------- | ---- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `pk`        | S    | `command-outcome#<commandId>`                                                                                                             |
| `sk`        | N    | `1` (single row per command)                                                                                                              |
| `status`    | S    | `accepted` or `rejected` (written once, final)                                                                                            |
| `digest`    | S    | pluggable identity digest                                                                                                                 |
| `response`  | S    | the response bytes, standard padded base64 (Errata (E1.1): the storage spec pins base64, so arbitrary bytes round-trip in both languages) |
| `events`    | S    | event IDs the commit wrote, when any                                                                                                      |
| `rejection` | S    | serialized `Rejection` when `status = rejected`                                                                                           |
| `code`      | S    | rejection code, when rejected                                                                                                             |

The row is written once, in its final state, inside the commit transaction, conditioned on
`attribute_not_exists(pk)`. A committed transaction therefore never leaves a half-written outcome.
The row's byte-level shape is pinned in the storage spec (E1.1). The `command-outcome#` namespace
is reserved:
repository scans that walk streams skip it (C8), and it has no snapshot and never compacts.

**API shape.**

```go
// Go
outcome, err := store.Execute(ctx, account, cmd, metadata.WithCommandID(id))
// outcome.Accepted() bool, outcome.NoOp() bool, outcome.Response, outcome.Rejection
// ErrIdempotencyKeyReuse and the ambiguous-write recovery error matchable with errors.Is
```

```rust
// Rust
enum Outcome<A> {
    Accepted { events: Vec<A::Event>, response: Option<Bytes>, noop: bool },
    Rejected(Rejection),
}
// Error::IdempotencyKeyReuse — a repeat carrying a different identity digest.
// After an indeterminate commit error, re-read the outcome by command ID:
// present → return it (same digest) or reject (different digest); absent → safe to retry.
```

_Errata (E2.4/E3.4):_ the API shapes above are illustrative; the shipped ones keep the existing
entry points compiling. Go adds `Store.ExecuteWithOutcome` (returning `*CommandOutcome`, with
`Accepted`/`Rejected`/`NoOp`, `Response`, `Rejection`, and `Replayed`) rather than changing
`Store.Execute`, which on an outcomes store reports a rejection as `*RejectionError`;
`prior_outcome` is `IdempotencyKeyReuseError.Prior`, and multi-stream commands use
`Store.BeginCommand` then `Transaction.Complete`/`Reject`. Rust adds
`Store::execute_with_outcome`/`execute_with_response` and `Transaction::begin_command`
→ `commit_with_outcome`/`reject`, returning `CommandOutcome<E>` rather than a bare enum. An
outcome-only commit (a persisted rejection that carries no events, or an accepted no-op) is the feature-marker check plus,
for a transaction, the fence checks of its reads, then the outcome put; the stale-fence
rejection omits the failed fences. A failed outcome put outranks every other failed item except the
feature-marker check. Ambiguous-write recovery polls the row for a bounded window and treats a
row still absent at the end as a retryable ambiguous write rather than proof that the commit did
not apply ("absent → safe to retry" above is superseded: a transaction can land after any single
read). See the storage spec's Command Outcomes section.

### C4 — Rejections

**Current state.** Go's command errors are ad-hoc sentinel types (`NotFoundError`,
`BadCommandError`, `BadEventError` in [`errors.go`](../../errors.go)); Rust commands return their
own `Aggregate::Error`. Neither is durable, and neither survives a retry — the caller of a retried
command cannot know why the first attempt failed.

**Decision.** A standard, persisted rejection:

```text
Rejection { code, message, details }
```

Only **domain rejections** — the aggregate's own refusal: not-found, invalid, conflict-by-rule —
become durable rejected outcomes. Internal failures — event encoding errors, bad events
(`BadEventError`), storage errors — are returned as errors and **write no outcome**: after the
underlying bug or deployment mismatch is fixed, a retry starts clean instead of replaying a
durable rejection forever. Domain error enums map onto rejection codes (Rust:
`From<DomainError> for Rejection`; Go: a `Rejectioner` interface or a code registry). Go's
existing `NotFound`/`BadCommand` become rejection codes; `BadEvent` stays an error for exactly
this reason; Go's `NoOp` becomes accepted-without-events (C3). Rejections are stored by C3, so a
retried command replays the original rejection verbatim.

**Storage format.** The `rejection` attribute on the outcome row (above), with a fixed JSON shape
pinned in the storage spec (E1.1). No other format change.

**API shape.**

```go
// Go
evt.Rejection{Code: "username-taken", Message: "...", Details: map[string]any{...}}
evt.RejectionCode("not-found") // replaces NotFoundError as a persisted outcome
```

```rust
// Rust
pub struct Rejection { pub code: RejectionCode, pub message: String, pub details: Option<serde_json::Value> }
impl From<DomainError> for Rejection { ... }
```

### C5 — Fence-aware retry

**Current state.** Go's `policy.ExecuteWithRetry` (`policy/commit.go`) and Rust's
`execute_with_retry` (`crates/evt/src/store.rs`) re-run the whole command on every conflict. When
the conflict is on a fence the _engine_ filled (C2), the command's decision is still valid and
re-running it is wasted work — and when the command is not deterministic, re-running it is wrong.

**Decision.** On conflict, re-prepare (re-run the command) **only when every fence in the failed
commit was engine-filled** — that is, the engine loaded the fenced streams and can reload them and
re-decide. If any fence was caller-stamped (a head the caller supplied), the engine cannot know the
caller's decision is still valid, so it does not retry: it returns a **stale-fence rejection**
persisted via C3 when retries are exhausted. This replaces blind `ExecuteWithRetry` /
`execute_with_retry` as the default retry path for transactional commands; the policy packages
remain for backend-level transient classification.

**Storage format.** None beyond C3 (a stale rejection is an outcome row). Fence provenance
(engine-filled vs caller-stamped) is in-memory commit metadata, not stored. _Errata (E1.1):_ the
classification keys off the **transaction item that failed**, not the commit's contents: a failed
event put, floor check, or snapshot condition is an ordinary conflict that re-prepares even when
the commit also carried caller-stamped fences, and only a failed caller-stamped fence produces
the stale-fence rejection. The storage spec pins this in "Fence-aware retry".

_Errata (E2.5/E3.5):_ the shipped retry is built into the store, not a wrapper around it. On a
store with outcomes, Go's `Store.ExecuteWithOutcome` (and `Execute`) and the new
`Store.ExecuteTransaction`, and Rust's `Store::execute_with_outcome` and the new
`Store::execute_transaction`, run the command under a bounded, configurable policy (defaults: 5
attempts, 25 ms doubling to 1 s, jittered; `snapshots.RetryPolicy` / `OutcomeConfig::with_retry`).
A conflict on anything the engine can re-derive re-prepares the command: reload, then run the
handler or prepare step again. A failed caller-stamped fence fails fast with the stale-fence
rejection instead. When the last attempt also conflicts, the engine persists the stale-fence
rejection (`details: {"attempts": N}`) as the outcome, so a replay gets the same answer; a
transient failure that is not a conflict is returned as an error. `policy.ExecuteWithRetry` is
deprecated (K6) and Rust's blind `execute_with_outcome_retry` is gone. The multi-stream retry takes
the caller's decision step as a function (`Prepare` in Go, a boxed-future closure returning a
`Decision` in Rust) because only the caller can say how to decide again.

The same change fixes a design flaw in C3's identity digest. A command's write set can depend on
state it reads (a rename releases whichever handle the entity currently holds), so it cannot be part
of an identity that must be computed before any state is read and must survive re-preparation,
client retries, and recovery. **The identity is the caller's intent** — principal, command type,
the streams the request names (`Targets`, no longer "the streams it declares it will write"), and
payload. The streams a command reads and writes are decided per attempt and recorded in the
outcome's `events` list; appending to a stream outside the identity is no longer refused. The
canonical bytes and the `evt-cmd-digest-v1` tag are unchanged, so this is not a wire-format
change; the storage spec's "Identity digest" and "Fence-aware retry" sections pin it.

### C6 — Metadata: `principalId`, `causationId`, `extensions`

**Current state.** `Metadata` is `commandId`, `trace`, `origin`, `address`, `region`, `timestamp`
([`metadata.go`](../../metadata.go); Rust `crates/evt/src/metadata.rs`; spec "Metadata").

**Decision.** Three additions:

- `principalId` — who issued the command (identity of the caller), for audit and per-principal
  authorization checks.
- `causationId` — the command that caused this command; defaults to the command ID when unset.
- `extensions` — a string-keyed map for application-specific metadata, so apps stop overloading
  `origin` or stashing data out of band.

**Storage format.** The metadata JSON gains three keys, always written (absent = `null` /
empty map), appended after the existing keys so stored events remain readable without upcasters —
decoding is lenient in both languages, and the existing fixture vectors still decode. New fixtures
cover the added keys. Rust's `photon_evt::metadata` mirrors the field order. _Errata (E1.1):_ the
metadata JSON gains a fourth key, `commitId` (a string the engine writes, not the caller): every
event of one commit carries the same value — identical across all streams of a multi-stream
commit — defaulting to the command ID when outcomes are enabled, else a generated ULID. It is the
only durable commit boundary, and a wipe-and-rebuild regroups on it (see C7).

**API shape.** Go: `Metadata.PrincipalID *PrincipalID`, `Metadata.CausationID *CommandID`,
`Metadata.Extensions map[string]string` plus `WithPrincipalID`/`WithCausationID`/`WithExtension`
options. Rust: `Metadata::with_principal_id`, `with_causation_id`, `with_extension`.

### C7 — Commit-wide projectors

**Current state.** Projectors are per-entity and puts-only: Go's `EventProjector.Project(ctx,
entity, events)` runs per entity ([`types.go`](../../types.go)) and its `TransactionGroup` merges
view puts into the commit ([`dynamo/view_projector.go`](../../dynamo/view_projector.go)); errors go
through `TransactionGroup.HandleError` ([`transactions.go`](../../transactions.go)), which sees
only its own group. Rust's `Projector` trait is likewise per-aggregate
(`crates/evt/src/aggregate.rs`), and the DynamoDB `ViewProjector` writes exactly one `VIEW` row
per entity at `pk = {entityType}#{entityId}` (spec "Views"). Cross-entity views — a projection
over every event in a commit — are impossible, and deletes require hand-rolled `DeleteView` calls
outside the commit.

**Decision.** A projector becomes a **pure function over every event in the commit**, not per
entity. It emits puts **and deletes**, and view rows may live at **any `pk`/`sk`** the projector
chooses (not just `{entityType}#{entityId}` / `VIEW`), enabling cross-entity read models written
atomically with the commit. Per-operation error mapping replaces the single
`HandleError(error, int)`: an operation carries the cancellation reason it maps to, replacing the
single per-group callback with per-operation intent. `EventProjector`/`Projector` per-entity
projectors remain supported as the special case "one entity's events"; the commit-wide form is the
new general API. This also retires the handler-attached `CommandResult.Transaction` pattern (K7).

**Storage format.** View items keep the existing attribute set (`pk`, `sk`, `entityID`,
`entityType`, `payload`) but are no longer constrained to `sk = "VIEW"` or the derived pk; the
spec's Views section is relaxed accordingly, and fixtures gain a multi-pk/sk vector. Delete
operations are transaction deletes with their own conditions. _Errata (E1.1):_ because the log
stores only per-stream events, the spec pins how a wipe-and-rebuild reproduces the grouping: it
regroups events by `commitId` (C6) and runs the commit-wide projector once per commit, with that
commit's events in the pinned (stream ID, sequence) order; the projector's output may depend only
on the events of that commit plus the post-commit aggregate states, never on cross-commit
ordering; pre-v0.9 events without `commitId` replay as single-event commits. On PostgreSQL, evt defines no view
schema — apps own their view tables, and the "do not build" verdict on a Postgres view store
stands ([ADR 0006](0006-parity-policy-and-feature-cuts.md)). C7 on PostgreSQL therefore means the
backend executes the projector's operations inside the commit transaction, as the Rust backend
already does with `PgStatement` ([`crates/evt/src/postgres.rs`](../../crates/evt/src/postgres.rs));
the Go PostgreSQL backend today ignores `res.Transaction`
([`postgres/repository.go`](../../postgres/repository.go)), so this is new backend work — a
view-row execution path, not a port of an existing one. _Errata (E1.1):_ the spec pins the
**current-state contract**: a commit-wide projector's rows are a deterministic function of the
commit's events plus the post-commit aggregate states, and every commit-wide projector implements
a second, **state entry point** called with a lookup by stream ID over the final state of every
aggregate in scope. Rebuild splits by projector kind: **current-state** projectors (the default)
never replay history — the rebuild loads every aggregate's final state (snapshot + tail), calls
the state entry point once over all of them, and writes the complete returned row set, so
cross-entity rows derive from complete, consistent state and compaction is irrelevant to them;
**history-dependent** projectors replay every commit grouped by `commitId`, and compaction is
forbidden for their entity types — durably, via the feature manifest's `historyDependent` set
(C9), which `CompactBelow` checks with a consistent read and refuses with a typed error in every
process. Projections that need the full history (for example feeds) must declare it; a rebuild
that finds such a stream already compacted fails with `CompactionUncovered`.

**API shape.**

```go
// Go: pure function over the whole commit
type CommitProjector interface {
    ProjectCommit(ctx context.Context, commit CommitView) (ViewTransaction, error)
}
// ViewTransaction: puts and deletes, each at an explicit pk/sk, each with an error mapping
```

```rust
// Rust: one hook per commit
pub trait CommitProjector {
    fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, Rejection>;
}
pub enum ViewOperation { Put { pk: String, sk: String, payload: ... }, Delete { pk: String, sk: String } }
```

_Implementation note (E2.6/E3.6)._ The commit path shipped with these shapes. Go: `evt.CommitProjector`
(`Name`, `ProjectCommit(ctx, CommitView) ([]ViewOperation, error)`), `evt.ViewOperation` (`PutView`,
`DeleteView`, `IfAbsent`, `IfPresent`, `OnConflict`), registered with
`snapshots.Store.WithCommitProjectors` and carried to the repository as `SerializedResult.Views`.
Rust: `CommitProjector`, `ViewOperation`, `CommitView`, `Store::with_commit_projector`. Operations are
backend-neutral and the repository owns their execution (DynamoDB `WithViewsTable`, PostgreSQL
`WithViewWriter`, in-memory); per-operation errors are `*evt.ProjectorOperationError` /
`Error::ProjectorOperation`. The state entry point, the rebuilds that call it, and the
history-dependent registration check are part of the rebuild ports (P4).

### C8 — Repository additions: `latest(n)` and reserved namespaces

**Current state.** Go has `GetEvents`/`GetLatestEvents(lastSequence)` — unbounded, and
"latest" is a floor, not a count (`repository.go`). Rust has `events`/`events_after`
(`crates/evt/src/repository.rs`). Enumeration (`StreamAllEvents`, `StreamEntities`) and the heads
scan (`dynamo/heads.go`, `entity_heads` in Rust) read **every** partition, so today a global
outcome stream would pollute enumerations, heads, and rebuilds.

**Decision.**

- `latest(n)`: a bounded, newest-first read of an entity's last `n` events (descending), for
  "recent activity" reads that must not replay a long stream.
- **Reserved namespaces**: the `SYSTEM#` partition namespace (C9, C10) and the
  `command-outcome#` namespace (C3) are reserved for engine-managed rows; enumeration, heads, and
  rebuild scans skip them, and they are never ordinary aggregates.
  - **Commit-time refusal**: a writer refuses an entity ID that starts with a reserved prefix
    (`SYSTEM#`, `command-outcome#`) at commit time, before any write.
  - **Upgrade check**: entity IDs were never restricted before, so a pre-existing stream such as
    `SYSTEM#account` would become silently invisible to enumeration, heads, and rebuild after
    upgrading. `evt-doctor` gains a check that reports pre-existing streams whose IDs fall in a
    reserved namespace, and the upgrade note requires renaming (migrating) such streams before
    enabling the v0.9 features. Real stored data is never silently skipped.

**Storage format.** The namespaces themselves are the format change (C3, C9, C10 define the row
shapes). `latest(n)` is a read-pattern addition (a descending query bounded by `n` on DynamoDB, an
`ORDER BY … DESC LIMIT n` on PostgreSQL); no item shape changes.

### C9 — Feature-marker row

**Current state.** Nothing tells a writer which extensions a store's data uses. A Go tool that
does not know about a Rust-first extension (D2: extensions are Rust-first; D4: the feature marker
enforces declared extensions) would happily write to a table that depends on
it, silently violating its invariants (for example, compacting a journal without its manifest).

**Decision.** A store declares the extensions it uses on a single `SYSTEM#FEATURES` marker row.
Any writer that does not support a declared feature **refuses to write** to the table. The marker
row carries the feature set and a numeric `version`; a writer validates the declared feature set
once, at first write, and caches the version it validated. **Every commit then includes a
`ConditionCheck` on the marker** — the version must still equal the cached one, or
`attribute_not_exists(pk)` when no marker row existed at validation. Declaring a feature — or
changing one — **increments the `version` in the same conditional write that updates the feature
set**, so every writer whose commits condition on the old version fails and re-validates; a writer
that validated before the declaration cannot keep committing. (One row per feature with distinct
sort keys could not do this: adding a feature would neither change an already-validated row nor
fail a condition against another key.) At re-validation an unsupported writer reads the feature
set, sees the feature it does not model, and refuses. The cost is one extra transaction item per
commit.

**Storage format.**

| Attribute  | Type | Feature-marker item (`pk = SYSTEM#FEATURES`)                                                                                                               |
| ---------- | ---- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pk`       | S    | `SYSTEM#FEATURES`                                                                                                                                          |
| `sk`       | N    | `0` (the events table's `sk` is numeric — the feature set is an attribute, not a sort key)                                                                 |
| `version`  | N    | `1` when the row is created; each declaration transaction then adds one (Errata (E1.1): the storage spec pins the initial value and the unit of increment) |
| `features` | S    | JSON object mapping declared feature name → its stored-format version, keys sorted (Errata (E1.1): per-feature format versions, matching the storage spec) |

One manifest row, `pk = SYSTEM#FEATURES`, `sk = 0` — declared by the writer that first uses a
feature, updated (with a `version` bump) by every later declaration; `version` is bumped again
when an extension's storage format changes. A writer that encounters an undeclared-to-it feature
reads the feature set and refuses. The exact attributes, the per-feature format versions, and the
ConditionCheck shape are pinned in the storage spec (E1.1). _Errata (E1.1):_ the manifest is
also the durable home of two policy declarations the original decision left to registration:
the `historyDependent` set (entity types whose commit-wide projectors need the full commit
stream, so compaction is forbidden for them — `CompactBelow` reads the set with a consistent read
and refuses with a typed error in every process that opens the log) and the `retention` set
(entity types with TTL retention, for which read fences are refused). Both are sorted JSON arrays
of entity types stored on the same row, and changing either bumps `version` — and re-fails every
cached writer — exactly as a feature change does. The sets are durable, shared state, so the
refusals hold across independently configured instances; in-memory-only declarations could not.

### C10 — Rebuild fence

**Current state.** Rebuilds are unconstrained: `RebuildProjections`
([`rebuild.go`](../../rebuild.go)) runs whenever a job feels like it, and a live writer can commit
while the rebuild replays, interleaving projector output. A downstream service fences rebuilds by
hand with a writers-quiesced flag passed through its call graph — a promise, not a fence.

**Decision.** The framework provides a durable rebuild fence, per plan decision D3 (rebuilds are
fenced by the framework, not by app-level flags):

- **The lock row and the generation counter are separate items.** The lock row (`SYSTEM#REBUILD`,
  `sk = 0`) carries the **owner token** and a lease expiry (`leaseExpiresAt`); the generation
  counter (`SYSTEM#REBUILD#GENERATION`, `sk = 0`) is a separate item holding a numeric monotonic
  `value`.
- **Rebuilds** acquire the lock conditionally (`attribute_not_exists` on the lock row), bump the
  generation, replay, then release the lock.
- **Lease and recovery**: the rebuild renews the lease (`leaseExpiresAt`) while it runs. A failed
  rebuild's lock is recoverable: another rebuild may **take over** only with a conditional write
  that asserts the observed `owner` and an expired `leaseExpiresAt`; an operator **force-release**
  path also exists and bumps the generation. Until then, commits are still refused while any lock
  row exists — an expired lock blocks writes until taken over or force-released. That is the
  deliberate tradeoff: commits stay safe while any lock row exists, at the cost of waiting out a
  crashed rebuild until takeover or force-release. _Errata (E1.1):_ force-release is reshaped to
  protect readers from a partially rebuilt projection: it deletes the lock (commits resume
  immediately) but bumps the generation by **two**, leaving it **odd**, and records a
  `forceReleased` flag on the generation item. Readers keep treating odd as unavailable — every
  view read stays fenced — until a later rebuild acquires from the flagged odd generation,
  replays, and completes a release, whose even generation and cleared flag end the fence. The
  operator tradeoff: force-release trades read availability for write availability, because the
  force-released rebuild may have committed only some of its wipe and replay batches and no
  replacement worker remains to finish them.
- **Commit-time check**: every commit includes a `ConditionCheck` asserting
  `attribute_not_exists(pk)` on the lock row — a commit is refused whenever any rebuild holds the
  lock, regardless of generation. _Errata (E1.1):_ a commit that races a rebuild fails its check
  with `RebuildInProgress`, a control-row classification like the feature-marker check: it is
  **never** routed through the fence path — it does not produce a stale-fence rejection and is
  **not** retried with a fresh load under C5 — but is retried after the lock is taken over or
  force-released. The storage spec pins this in "Writes" and "Fence-aware retry".
- **Reader epoch check**: the generation increments when a rebuild **acquires** the lock and
  **again when it releases** after replay completes, so the value is odd while a rebuild is in
  progress and even when quiesced. Readers that need consistency read the generation before and
  after the read and treat an **odd or changed** value as "rebuild in progress or completed —
  retry"; view readers can reject state built under a stale epoch.

**Storage format.**

| Attribute        | Type | Rebuild-lock item (`pk = SYSTEM#REBUILD`) |
| ---------------- | ---- | ----------------------------------------- |
| `pk`             | S    | `SYSTEM#REBUILD`                          |
| `sk`             | N    | `0`                                       |
| `owner`          | S    | owner token of the current lock holder    |
| `leaseExpiresAt` | N    | lease expiry, epoch seconds               |

| Attribute | Type | Generation item (`pk = SYSTEM#REBUILD#GENERATION`) |
| --------- | ---- | -------------------------------------------------- |
| `pk`      | S    | `SYSTEM#REBUILD#GENERATION`                        |
| `sk`      | N    | `0`                                                |
| `value`   | N    | monotonic counter, bumped per rebuild              |

The rows live in the reserved `SYSTEM#` namespace (C8), skipped by enumeration and heads. On
PostgreSQL the same rows are table rows checked in the commit transaction.

### C11 — Conformance and compat for the new core

**Current state.** Each backend runs its own conformance suite (`conformance.RunRepositorySuite`
in Go; `photon_evt::conformance::Suite` in Rust), and the cross-language fixtures cover
serialization and the scenario set in `compat/scenarios.json` (see
[`compat/README.md`](../../compat/README.md)). Bespoke in-app stores never shared a suite — the
gap this aims to close permanently.

**Decision.** The conformance suites (both languages) grow cases for every capability above:
multi-stream commits, fences, outcomes (including `IdempotencyKeyReuse`, ambiguous-write
recovery, accepted no-op), rejections, fence-aware retry, metadata fields, commit-wide projectors,
`latest(n)`, the feature marker, and the rebuild fence. Every backend — `mem`, DynamoDB,
PostgreSQL — passes the same suite in both languages. The shared scenario list in
`compat/scenarios.json` grows interleaved Go/Rust steps for the new writes, and Go regenerates the
core golden fixtures as today. The suite, not the ADR, is the executable contract.

## Motivating consumers

- **A downstream Rust game service**: needs atomic multi-aggregate commands — claiming a unique
  handle while updating the owning profile — which C1+C2 provide directly. Its hand-rolled
  command engine (per-aggregate counter rows, per-entity dedupe, accepted-no-op handling)
  maps onto C3+C4, and the port to the framework capabilities (plan E6) deletes the engine.
- **A downstream Go service on evt**: hand-rolled command reservations with identity digests,
  canonical receipts, ambiguous-write recovery, and a replay guarded by a hand-passed
  writers-quiesced flag. C3+C4 replace the reservations and receipts; the outcome re-read (C3)
  replaces the hand-rolled recovery; C10 replaces the manual quiesce flag; C7's per-operation
  error mapping replaces the per-group error plumbing.

## Consequences

- **Breaking changes are confined to one release.** Go v0.9.0 and photon-evt 0.2 reshape the
  commit path together; the deprecated features (K1–K7, see
  [ADR 0006](0006-parity-policy-and-feature-cuts.md)) are deprecated in the same release and
  deleted in the next minor.
- **The transaction budget gets tighter.** Fences, floor checks, outcome rows, and the
  feature-marker condition all count against the 100-item DynamoDB transaction limit — the item
  budget is: events + fences + snapshot checks + the outcome row + the feature-marker and
  rebuild-lock checks + projector operations. An atomic multi-stream commit that needs more than
  100 items fails with a typed `CommitTooLarge` error **before any write**; it is **never split** —
  splitting would break C1's atomicity guarantee, because an error after an earlier chunk commits
  would leave only part of the aggregate set updated.
- **PostgreSQL commits gain a view-write path.** evt defines no PostgreSQL view schema — apps own
  their view tables — so C7 means the Go PostgreSQL backend starts executing projector operations
  inside the commit transaction (it ignores `res.Transaction` today), as the Rust backend already
  does with `PgStatement`.
- **Every write now has an outcome.** Commands that previously "succeeded silently" (no events)
  become observable, which is the point — but callers that treat empty commits as free no-ops will
  now pay an outcome row write.
- **Reserved namespaces are permanent.** `SYSTEM#` and `command-outcome#` partitions must never
  collide with entity IDs. Entity IDs beginning with these prefixes are rejected at commit, and
  pre-existing streams in a reserved namespace must be renamed (migrated) before enabling the
  v0.9 features — `evt-doctor` reports them (C8), so no stored data is silently skipped.
- **Spec and fixtures first.** No code lands before `docs/spec/storage-format.md` carries the
  byte-level shapes (plan E1.1); both implementations then pass the regenerated fixtures and the
  shared conformance suite.

See [ADR 0005](0005-rust-first-extensions-journal-and-lambda.md) for the extensions that build on
this core, and [ADR 0006](0006-parity-policy-and-feature-cuts.md) for what is cut to pay for it.
