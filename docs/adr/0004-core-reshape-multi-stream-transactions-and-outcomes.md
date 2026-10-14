# ADR 0004: Core reshape — multi-stream transactions, command outcomes, and rebuild fences

## Status

Accepted (2026-10-13). Plan: [evt vNext and the Storyverse Rust
port](https://github.com/photon-grove/apps/blob/bkonkle/evt-vnext-storyverse-rust-plan/docs/plans/evt-vnext-storyverse-rust/README.md)
(ids C1–C11). Tracking: [`docs/parity/feature-matrix.md`](../parity/feature-matrix.md).

## Context

Two consumers of evt have grown capabilities the framework never offered, and each grew them by
hand:

- **Storyverse v2** (`photon-grove/apps`, `apps/storyverse-api/internal/v2`) built its own command
  engine on raw evt writes: a hand-rolled counter row (`COUNTER#`) to fence username claims
  (apps ADR-0033), per-entity dedupe, and no shared conformance story for its memory and DynamoDB
  stores.
- **munc-api** (`apps/munc-api`) runs on evt but hand-rolls command reservations
  (`CommandReservationEntity`, `requestHash`), canonical receipts, ambiguous-write recovery
  (`resolveCommitmentAfterCancellation`), and a replay guarded by a hand-passed `writersQuiesced`
  flag (`apps/munc-api/internal/memory/rebuild.go`).

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

Per plan decision D1, anything in the commit path or storage contract lands in **both** languages:

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
the username-claim case, where `user#<subject>` and `username#<handle>` change together — cannot
commit atomically today.

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
// Rust: the same shape through a builder.
let outcome = store
    .transaction()
    .read(load::<Account>(&id).await?)   // read + write
    .read(load::<Claim>(&handle).await?) // read-only, fenced
    .fence(load::<Username>(&handle).await?)
    .command(cmd)
    .execute(metadata)
    .await?;
```

### C2 — Read fences

**Current state.** Nothing in either implementation fences a read-only aggregate. Storyverse v2
invented `COUNTER#` rows to make username claims race-safe (apps ADR-0033) — mutable state with no
backing event, exactly what the core discipline forbids.

**Decision.** A read-only stream in a transaction is fenced with a `ConditionCheck` on
`(pk, head + 1)` asserting `attribute_not_exists(sk)`: the check fails if any writer has appended
past the head the reader saw. This works with `sk = 0` snapshots and with compaction: compaction
never deletes at or above the head, and a fresh load always re-derives the head as the larger of
the highest event sequence and the snapshot's `eventSeq`, so `head + 1` is always a free slot on
the reader's view. A fenced stream can also be written in the same commit; then the event put at
`head + 1` makes the fence redundant and it is omitted.

**Storage format.** The fence is a `ConditionCheck` transaction item on `(pk, sk = head+1)`. It
bills as a write and counts toward the 100-item limit, like the existing floor check
([`dynamo/commit.go`](../../dynamo/commit.go), `buildFloorChecks`). A `ConditionalCheckFailed` on a
fence is a conflict (another writer moved the stream), classified and retried like an event-put
conflict. PostgreSQL expresses the same check as a `SELECT … FOR KEY SHARE`-strength existence
assert on the row at `head + 1` inside the commit transaction; the in-memory backends check
sequentially. Fences are declared per read; a fence the caller fills from outside the engine is a
caller-stamped fence (see C5).

### C3 — Command outcomes

**Current state.** The dedupe window is "events replayed since the latest snapshot"
(`crates/evt/src/store.rs` `Loaded::has_seen`; Go's snapshot-aware load), so a command ID folded
into a snapshot is no longer checked and the repeat applies twice — documented in
`crates/evt/README.md` and the spec's known differences. munc-api hand-rolls reservations and
receipts; v2 hand-rolls per-entity dedupe.

**Decision.** The framework owns durable, global command outcomes. A commit that carries a command
ID **reserves** a row on a global `command-outcome#<id>` stream in the same transaction as the
events, then records the outcome:

- **Identity digest**: pluggable — the caller supplies a digest of the command's identity
  (canonical request bytes). A repeat with the same digest returns the stored outcome; a repeat
  with a different digest is an `IdempotencyKeyReuse` rejection. The default digest is the command
  ID alone.
- **Stored response**: the outcome row stores the command's response payload so a retried command
  returns the original answer, not just "accepted".
- **Status**: `accepted` or `rejected` (C4). A rejected outcome is terminal for that digest.
- **`prior_outcome`**: a retried commit that finds a reserved-but-unresolved outcome links to it
  (`prior_outcome`) instead of failing, giving ambiguous-write recovery a durable chain — the
  general form of munc's `resolveCommitmentAfterCancellation`.
- **Outcome-only commits**: a command can record an outcome without appending events, for commands
  whose effect is a decision about state rather than a change to it.
- **Accepted no-op**: a command that produces zero events is recorded as **accepted with no
  events**, not an error. This is the framework form of apps ADR-0033's `ACCEPTS_NO_OP`. Go's
  current "empty commit is a no-op" (`Commit` returns nil on empty events) becomes an observable,
  durable outcome.

This replaces per-entity dedupe: the `SeenCommandIDs`-style window (both languages) is superseded,
though it remains as a cheap pre-check on load.

**Storage format.** New item shape on the shared table (same `pk`/`sk` pattern as every stream):

| Attribute   | Type | Outcome item (`pk = command-outcome#<id>`)                                  |
| ----------- | ---- | --------------------------------------------------------------------------- |
| `pk`        | S    | `command-outcome#<commandId>`                                               |
| `sk`        | N    | `1` (single row per command; the stream is reserved, then updated in place) |
| `status`    | S    | `reserved`, `accepted`, or `rejected`                                       |
| `digest`    | S    | pluggable identity digest                                                   |
| `response`  | S    | stored response JSON (base64 or raw text — fixed in E1.1)                   |
| `rejection` | S    | serialized `Rejection` when `status = rejected`                             |
| `prior`     | S    | prior reserved outcome's pk, when linking                                   |
| `code`      | S    | rejection code, when rejected                                               |

The row is reserved (written with `attribute_not_exists(sk)`) inside the event commit and resolved
(status flipped) in the same transaction when possible; the reservation-to-resolution detail is
specified byte-for-byte in E1.1. The `command-outcome#` namespace is reserved: repository scans
that walk streams skip it (C8), and it has no snapshot and never compacts.

**API shape.**

```go
// Go
outcome, err := store.Execute(ctx, account, cmd, metadata.WithCommandID(id))
// outcome.Accepted() bool, outcome.NoOp() bool, outcome.Response, outcome.Rejection
// ErrIdempotencyKeyReuse, ErrPriorOutcome matchable with errors.Is
```

```rust
// Rust
enum Outcome<A> {
    Accepted { events: Vec<A::Event>, response: Option<Bytes>, noop: bool },
    Rejected(Rejection),
}
// Error::IdempotencyKeyReuse { prior: OutcomeRef }, Error::AmbiguousWrite { prior: OutcomeRef }
```

### C4 — Rejections

**Current state.** Go's command errors are ad-hoc sentinel types (`NotFoundError`,
`BadCommandError`, `BadEventError` in [`errors.go`](../../errors.go)); Rust commands return their
own `Aggregate::Error`. Neither is durable, and neither survives a retry — the caller of a retried
command cannot know why the first attempt failed.

**Decision.** A standard, persisted rejection:

```text
Rejection { code, message, details }
```

Domain error enums map onto rejection codes (Rust: `From<DomainError> for Rejection`; Go: a
`Rejectioner` interface or a code registry). Go's existing `NotFound`/`BadCommand`/`BadEvent`
become rejection codes; Go's `NoOp` becomes accepted-without-events (C3). Rejections are stored by
C3, so a retried command replays the original rejection verbatim.

**Storage format.** The `rejection` attribute on the outcome row (above), with a fixed JSON shape
specified in E1.1. No other format change.

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
(engine-filled vs caller-stamped) is in-memory commit metadata, not stored.

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
cover the added keys. Rust's `photon_evt::metadata` mirrors the field order.

**API shape.** Go: `Metadata.PrincipalID *PrincipalID`, `Metadata.CausationID *CommandID`,
`Metadata.Extensions map[string]string` plus `WithPrincipalID`/`WithCausationID`/`WithExtension`
options. Rust: `Metadata::with_principal_id`, `with_causation_id`, `with_extension`.

### C7 — Commit-wide projectors

**Current state.** Projectors are per-entity and puts-only: Go's `EventProjector.Project(ctx,
entity, events)` runs per entity and its `TransactionGroup` merges view puts into the commit
([`repository.go`](../../repository.go), [`dynamo/view_projector.go`](../../dynamo/view_projector.go));
Rust's `Projector` trait is likewise per-aggregate (`crates/evt/src/aggregate.rs`), and the
DynamoDB `ViewProjector` writes exactly one `VIEW` row per entity at
`pk = {entityType}#{entityId}` (spec "Views"). Errors go through
`TransactionGroup.HandleError`, which sees only its own group. Cross-entity views — a projection
over every event in a commit — are impossible, and deletes require hand-rolled `DeleteView` calls
outside the commit.

**Decision.** A projector becomes a **pure function over every event in the commit**, not per
entity. It emits puts **and deletes**, and view rows may live at **any `pk`/`sk`** the projector
chooses (not just `{entityType}#{entityId}` / `VIEW`), enabling cross-entity read models written
atomically with the commit. Per-operation error mapping replaces the single
`HandleError(error, int)`: an operation carries the cancellation reason it maps to, in the shape
munc's `HandleError` hand-rolls today. `EventProjector`/`Projector` per-entity projectors remain
supported as the special case "one entity's events"; the commit-wide form is the new general API.
This also retires the handler-attached `CommandResult.Transaction` pattern (K7).

**Storage format.** View items keep the existing attribute set (`pk`, `sk`, `entityID`,
`entityType`, `payload`) but are no longer constrained to `sk = "VIEW"` or the derived pk; the
spec's Views section is relaxed accordingly, and fixtures gain a multi-pk/sk vector. Delete
operations are transaction deletes with their own conditions.

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
  `command-outcome#` namespace (C3) are skipped by enumeration, heads, and rebuild scans by
  default. Streams in a reserved namespace are engine-managed; they are never ordinary aggregates.

**Storage format.** The namespaces themselves are the format change (C3, C9, C10 define the row
shapes). `latest(n)` is a read-pattern addition (a descending query bounded by `n` on DynamoDB, an
`ORDER BY … DESC LIMIT n` on PostgreSQL); no item shape changes.

### C9 — Feature-marker row

**Current state.** Nothing tells a writer which extensions a store's data uses. A Go tool that
does not know about a Rust-first extension (D2/D4) would happily write to a table that depends on
it, silently violating its invariants (for example, compacting a journal without its manifest).

**Decision.** A store declares the extensions it uses on a `SYSTEM#FEATURES` marker row. Any
writer that does not support a declared feature **refuses to write** to the table. The marker is
checked once per repository instance at first write (cheap, cached), not per commit.

**Storage format.**

| Attribute | Type | Feature-marker item (`pk = SYSTEM#FEATURES`)                                 |
| --------- | ---- | ---------------------------------------------------------------------------- |
| `pk`      | S    | `SYSTEM#FEATURES`                                                            |
| `sk`      | S    | the feature identifier, one row per declared feature (for example `journal`) |
| `version` | N    | feature format version                                                       |

One row per feature, `pk = SYSTEM#FEATURES`, `sk = <feature>` — declared by the writer that first
uses the feature. A writer that encounters an undeclared-to-it feature reads the row set and
refuses. The exact attributes are fixed in E1.1.

### C10 — Rebuild fence

**Current state.** Rebuilds are unconstrained: `RebuildProjections`
([`rebuild.go`](../../rebuild.go)) runs whenever a job feels like it, and a live writer can commit
while the rebuild replays, interleaving projector output. munc-api fences rebuilds by hand with a
`writersQuiesced bool` passed through the call graph
(`apps/munc-api/internal/memory/rebuild.go`) — a promise, not a fence.

**Decision.** The framework provides a durable rebuild fence, per plan decision D3:

- A `SYSTEM#REBUILD` lock row with an **owner token** and a **generation counter**. A rebuild
  acquires the lock (conditional write), bumps the generation, and holds the owner token.
- **Commit-time check**: every commit `ConditionCheck`s the lock row — either it does not exist or
  the generation matches the value the reader observed at load. A commit that loaded before the
  rebuild began is rejected as a conflict and retried with a fresh load (C5 semantics).
- **Reader epoch check**: loads capture the current generation; a rebuild uses it to know which
  pre-rebuild readers are still in flight, and view readers can reject state built under a stale
  epoch.

**Storage format.**

| Attribute    | Type | Rebuild-lock item (`pk = SYSTEM#REBUILD`)           |
| ------------ | ---- | --------------------------------------------------- |
| `pk`         | S    | `SYSTEM#REBUILD`                                    |
| `sk`         | S    | `LOCK` (or one row per fence scope — fixed in E1.1) |
| `owner`      | S    | owner token of the current lock holder              |
| `generation` | N    | monotonic counter, bumped per rebuild               |
| `expires`    | N    | lease expiry, epoch seconds                         |

The row lives in the reserved `SYSTEM#` namespace (C8), skipped by enumeration and heads. On
PostgreSQL the same row is a table row checked in the commit transaction.

### C11 — Conformance and compat for the new core

**Current state.** Each backend runs its own conformance suite (`conformance.RunRepositorySuite`
in Go; `photon_evt::conformance::Suite` in Rust), and the cross-language fixtures cover
serialization and the scenario set in `compat/scenarios.json` (see
[`compat/README.md`](../../compat/README.md)). Storyverse v2's memory and DynamoDB stores never
shared a suite — the gap this aims to close permanently.

**Decision.** The conformance suites (both languages) grow cases for every capability above:
multi-stream commits, fences, outcomes (including `IdempotencyKeyReuse`, `prior_outcome`,
accepted no-op), rejections, fence-aware retry, metadata fields, commit-wide projectors,
`latest(n)`, the feature marker, and the rebuild fence. Every backend — `mem`, DynamoDB,
PostgreSQL — passes the same suite in both languages. The shared scenario list in
`compat/scenarios.json` grows interleaved Go/Rust steps for the new writes, and Go regenerates the
core golden fixtures as today. The suite, not the ADR, is the executable contract.

## Motivating consumers

- **Storyverse v2** (`apps/storyverse-api/internal/v2`, apps ADR-0033 username claims): C1+C2
  replace the `COUNTER#` rows, C3+C4 replace hand-rolled dedupe and verdict handling, C3's
  accepted-no-op is `ACCEPTS_NO_OP`. The Storyverse Rust port (plan E6) deletes all of it in favor
  of the framework capabilities.
- **munc-api** receipts and replay: C3+C4 replace `CommandReservationEntity`/`requestHash` and the
  canonical receipts (plan E7.1); C3's `prior_outcome` replaces
  `resolveCommitmentAfterCancellation`; C10 replaces the hand-passed `writersQuiesced`; C7's
  per-operation error mapping replaces `HandleError` plumbing.

## Consequences

- **Breaking changes are confined to one release.** Go v0.9.0 and photon-evt 0.2 reshape the
  commit path together; the deprecated features (K1–K7, see
  [ADR 0006](0006-parity-policy-and-feature-cuts.md)) are deprecated in the same release and
  deleted in the next minor.
- **The transaction budget gets tighter.** Fences, floor checks, and outcome reservations all
  count against the 100-item DynamoDB transaction limit. The commit planner must account for them
  and split or reject oversized commits deliberately, not accidentally.
- **Every write now has an outcome.** Commands that previously "succeeded silently" (no events)
  become observable, which is the point — but callers that treat empty commits as free no-ops will
  now pay an outcome row write.
- **Reserved namespaces are permanent.** `SYSTEM#` and `command-outcome#` partitions must never
  collide with entity IDs. Entity IDs beginning with these prefixes are rejected at commit.
- **Spec and fixtures first.** No code lands before `docs/spec/storage-format.md` carries the
  byte-level shapes (plan E1.1); both implementations then pass the regenerated fixtures and the
  shared conformance suite.

See [ADR 0005](0005-rust-first-extensions-journal-and-lambda.md) for the extensions that build on
this core, and [ADR 0006](0006-parity-policy-and-feature-cuts.md) for what is cut to pay for it.
