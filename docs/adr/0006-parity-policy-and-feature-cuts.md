# ADR 0006: Parity policy and feature cuts

## Status

Accepted (2026-10-13). Plan decision D6, defined below (port/defer/cut thresholds); rows
P1–P10, F1–F5, K1–K7. Tracking:
[`docs/reference/feature-matrix.md`](../reference/feature-matrix.md).

## Context

The Rust `photon-evt` crate implements the event-log core — log, snapshots, upcasting, compaction,
entity heads, in-band projectors, the stream envelope ([ADR 0003](0003-rust-implementation-and-cross-language-compatibility.md))
— but Go's `evt` has years of accumulated tooling the Rust crate lacks: view repositories, rebuild
runtime, publisher and projector fan-out, test utilities. The vNext program moves a downstream
service to Rust and migrates a Go service onto evt core outcomes, so every gap is a porting
decision: which Go features does Rust need in the port window, which can wait, and which should
stop existing anywhere?

Deciding per feature ad hoc produces half-finished ports. Plan decision D6 fixes the policy, and
the [feature matrix](../reference/feature-matrix.md) is where the decisions live.

## Decision

### Thresholds (D6)

Estimate the probability the feature is needed in the port window (≈ next quarter: the Rust port
and the outcomes migration), then:

| Probability                        | Verdict   | Meaning                                                                                                                                                                          |
| ---------------------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ≥ 50%, or required by an invariant | **Port**  | Build in Rust now, in the parity window (photon-evt 0.2/0.3).                                                                                                                    |
| 20–50%                             | **Defer** | Stays single-language; specified in the matrix; ported on first consumer, from the spec and fixtures ([ADR 0005](0005-rust-first-extensions-journal-and-lambda.md) rules apply). |
| < 20%                              | **Cut**   | Deprecated in Go in the core-reshape release (Go v0.9.0), deleted in the next minor. Never ported.                                                                               |

The invariant escape hatch matters: the wipe-and-replay guarantee (see
[ADR 0001](0001-event-compaction-and-snapshot-truncation.md)) and the new core reshape capabilities
(C1–C11) are port regardless of probability, because the storage contract requires them in both
languages.

### Feature verdicts

Go features `photon-evt` lacks, and their verdicts. The matrix
([`docs/reference/feature-matrix.md`](../reference/feature-matrix.md)) carries the authoritative current
state; the reasoning lives here.

| ID  | Feature                                                                                                                                                        | Near-term need | Verdict                   | Why                                                                                                                                                                                                                                                                                                              |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P1  | `ViewRepository`: get, put (TTL), delete, list by pk, list by entity type (GSI), paged with base64url cursor                                                   | 95%            | **Port**                  | Both downstream services read views through it; rebuild (P4) depends on it.                                                                                                                                                                                                                                      |
| P2  | `ViewCodec<T>` (merges `viewstore` Codec/Single + the `GetJSONView` family)                                                                                    | 85%            | **Port**                  | Typed view reads are how both consumers use P1.                                                                                                                                                                                                                                                                  |
| P3  | Event-log enumeration: `stream_all_events` / `stream_entities` + filter (`futures_core::Stream`)                                                               | 90%            | **Port**                  | Rebuild, doctoring, and differential testing all replay the log.                                                                                                                                                                                                                                                 |
| P4  | Full rebuild: snapshot-seeded enumeration (P11), commit-wide projector replay, batch view writes, honours the C10 rebuild fence                                | 95%            | **Port**                  | Wipe-and-replay is the load-bearing invariant. Rust already ships compaction (B5), so the port must be snapshot-aware from the start (see F3's promotion to P11).                                                                                                                                                |
| P5  | Catch-up snapshot on read-only loads                                                                                                                           | 55%            | **Port**                  | Keeps cold streams from replaying unboundedly during the port window.                                                                                                                                                                                                                                            |
| P6  | Per-aggregate snapshot interval (`Aggregate::SNAPSHOT_INTERVAL`)                                                                                               | 80%            | **Port**                  | Go's `WithSnapshotSize` per-type override is used by real aggregates.                                                                                                                                                                                                                                            |
| P7  | Test utilities (given/when/then over `MemRepository`)                                                                                                          | 70%            | **Port**                  | Every Rust port epic (E6.1+) writes domain tests against these.                                                                                                                                                                                                                                                  |
| P8  | `examples/banking` in Rust                                                                                                                                     | 50%            | **Port**                  | The runnable example is the docs contract for new adopters.                                                                                                                                                                                                                                                      |
| P9  | `tracing` fields on load (`events_replayed`, `snapshot_seq`) replacing freshness observers                                                                     | —              | **Port (replacement)**    | Structured load telemetry replaces the observer API (K2).                                                                                                                                                                                                                                                        |
| P10 | `PutViews` batch writer                                                                                                                                        | 70%            | **Port (internal to P4)** | Rebuild batches view writes; users never touch it directly.                                                                                                                                                                                                                                                      |
| P11 | Snapshot-seeded enumeration (`StreamEntitiesFromSnapshots`, promoted from F3 into the P4 rebuild scope)                                                        | 95%            | **Port**                  | Part of the P4 rebuild scope: Rust already ships compaction (B5), so a rebuild of a compacted stream must seed from snapshots; without it, rebuild fails loudly with `CompactionUncovered` and projections cannot be wiped and rebuilt.                                                                          |
| F1  | Segmented parallel scan                                                                                                                                        | 20%            | **Defer**                 | Enumeration (P3) is fast enough at current log sizes.                                                                                                                                                                                                                                                            |
| F2  | Constant-memory heads visitor                                                                                                                                  | 20%            | **Defer**                 | `entity_heads` returning a map is fine at current entity counts.                                                                                                                                                                                                                                                 |
| F3  | `StreamEntitiesFromSnapshots` (compaction-aware rebuild seeding)                                                                                               | —              | **Port (promoted → P11)** | Promoted into the P4 rebuild scope as P11: Rust already ships compaction (B5), so the rebuild port must be snapshot-aware from the start — a rebuild of a compacted stream without snapshot seeding fails with `CompactionUncovered`, and projections cannot be wiped and rebuilt for a supported storage state. |
| F4  | `BatchGetViews` / `AtSK`                                                                                                                                       | 30%            | **Defer**                 | Single gets cover the port window's view reads.                                                                                                                                                                                                                                                                  |
| F5  | SNS/SQS projector runtime + in-memory idempotency guard                                                                                                        | 25%            | **Defer**                 | C3 outcomes replace the idempotency guard; the runtime itself ports when a Rust app fans out.                                                                                                                                                                                                                    |
| K1  | `CommitStream`                                                                                                                                                 | 15%            | **Cut**                   | Bypasses outcomes, the journal, and projections — the one code path that must not exist after the reshape. Seeding uses batched commands instead.                                                                                                                                                                |
| K2  | Snapshot freshness observer / replay estimate / budget                                                                                                         | 15%            | **Cut**                   | Replaced by P9 `tracing` fields on load.                                                                                                                                                                                                                                                                         |
| K3  | `StreamEntitiesByQuery` + workers + `HeadSource`; `RebuildProjectionsByQuery`                                                                                  | 10%            | **Cut**                   | Served one hosted multi-tenant deployment only, since removed.                                                                                                                                                                                                                                                   |
| K4  | `HeadStore` (heads table) + incremental/delta rebuild                                                                                                          | 10%            | **Cut**                   | The heads-table rebuild path never earned its complexity; `entity_heads` scans suffice.                                                                                                                                                                                                                          |
| K5  | `publishers` + `BudgetController` + `stream.SNSPublisher`/FIFO                                                                                                 | 10%            | **Cut**                   | Cut after the legacy Go consumer is deleted. The envelope encode/decode stays (it is the cross-language wire format).                                                                                                                                                                                            |
| K6  | `policy` hooks / attempt callbacks / pluggable classifier                                                                                                      | 15%            | **Cut**                   | C5 fence-aware retry plus `tracing` replace the hooks.                                                                                                                                                                                                                                                           |
| K7  | `BaseEntity`, factories, `ByCommandID`, `dynamo.Delete`, `dynamo/mock`, `result/`, `do.go`, `ExpressionStreamer`, handler-attached `CommandResult.Transaction` | < 10%          | **Cut**                   | Legacy ergonomics and escape hatches the reshape obsoletes; the core rule is "evolve evt, never hand-roll in the app."                                                                                                                                                                                           |
| —   | `evt-doctor`                                                                                                                                                   | —              | **Keep, Go-only**         | Diagnostic CLI stays Go-only; it must learn the new rows (outcomes, `SYSTEM#FEATURES`, `SYSTEM#REBUILD`).                                                                                                                                                                                                        |
| —   | Postgres view store                                                                                                                                            | 10%            | **Do not build**          | No consumer; PostgreSQL views can live in the events-table projector path (C7). Reversed by [ADR 0008](0008-postgresql-views-table-and-journal.md) (proposed) once a consumer appeared.                                                                                                                                                                                                                                  |

### Deprecation-then-delete policy for Go

Cuts are two-step, so no consumer breaks on upgrade:

1. **Deprecate in Go v0.9.0** (the core-reshape release, alongside C1–C11): the deprecated
   packages and APIs are marked `// Deprecated:` with a pointer to the replacement, kept compiling,
   and still covered by tests. New code fails lint if it uses them.
2. **Delete in the next minor release**, once consumers have had a release cycle to migrate. The
   deletion PR is small by construction: it removes what v0.9.0 already documented.

K5's envelope encode/decode is the exception to "cut means gone": the SNS envelope is part of the
cross-language storage format, so the format lives on even as the publisher runtime is deleted.

### Matrix upkeep (D7)

[`docs/reference/feature-matrix.md`](../reference/feature-matrix.md) is the tracking source of truth,
not this ADR. The rules:

- **Every capability-changing PR updates its row** in the matrix — status, tier, or a new row for
  a new capability. Review rejects PRs that change a capability and leave the matrix stale.
- The plan IDs (C1–C11, X1–X3, P1–P10, F1–F5, K1–K7) are stable and are used in the matrix, in
  issues, and in PR titles.
- A row's `Go`/`Rust`/`TS` cells reflect the code on the default branch, not aspiration.

### TypeScript will join

TypeScript will become a third implementation column once there is a TS consumer (the plan's
tracking section lists `lang:ts`). The matrix already carries the `TS` column, so the parity
policy applies unchanged: a TS implementation follows the same spec, fixtures, and conformance
suite, and its rows move from ⏳ to ✅ only when the shared suite passes.

## Consequences

- The Rust parity work (plan E4) is bounded by the table above: eleven ports (P1–P11), four
  defers, seven cuts — not "port everything."
- Cuts shrink Go's surface at the same moment the reshape grows it, keeping the framework
  reviewable. Consumers migrating to v0.9.0 read the matrix's ✅ → ✂️ v0.9 rows as the migration list.
- Defers are tracked, not forgotten: a deferred row names its consumer trigger, and the first
  consumer ports it under the ADR-0005 rules (spec + fixtures, never hand-rolled).
- The matrix becomes the first file every evt PR checks, and the first file a new adopter reads to
  see what exists in their language.

See [ADR 0004](0004-core-reshape-multi-stream-transactions-and-outcomes.md) for the core reshape
and [ADR 0005](0005-rust-first-extensions-journal-and-lambda.md) for the Rust-first extensions.
