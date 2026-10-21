# ADR 0005: Rust-first extensions — the journal and the Lambda toolkit

## Status

Accepted (2026-10-13). Capabilities X1–X3 of the vNext plan; plan decision D2 (extensions are
Rust-first) and plan decision D4 (the feature-marker row enforces declared extensions, see
[ADR 0004](0004-core-reshape-multi-stream-transactions-and-outcomes.md) C9). Tracking:
[`docs/parity/feature-matrix.md`](../parity/feature-matrix.md).

## Context

[ADR 0004](0004-core-reshape-multi-stream-transactions-and-outcomes.md) reshapes the core in both
languages, because the commit path and the storage contract cannot tolerate drift. Beyond the core,
the next capabilities on the plan are:

- **X1 — the journal**: per-scope ordered, replayable delivery rows written in the commit — scope
  allocations persisted in event metadata (so rebuild is order-independent), scope counters,
  `high_watermark` / `page` reads with gap detection, a configurable row-size guard with a
  pre-check, and storage-contention retry.
- **X2 — journal compaction**: canonical chunks, a manifest, a lease with force release, an audit
  trail, range delete, and a `410 Gone` handoff descriptor so readers discover a moved page.
- **X3 — `photon-evt-lambda`**: a crate for the Lambda side of an evt deployment — DynamoDB Streams
  `NEW_IMAGE` decode, event-type filter helpers matching EventBridge Pipes / ESM filter patterns,
  `KEYS_ONLY` journal wake-up decode, and partial-batch-failure helpers.

Who needs these first? The first known consumer (plan E6) is a Rust service: the writers and
consumers of the new rows are all in Rust. No Go consumer needs the journal in
the port window.
Plan decision D2 says: **extensions are Rust-first.** This ADR records what that trade costs and
how the framework defends against it.

## Decision

The journal (+ its compaction) and `photon-evt-lambda` are built in Rust first — as
`photon-evt` 0.3 features (`journal`) and a new `photon-evt-lambda` crate — and Go ports them on
first need, **from the spec and Rust-generated fixtures, never by hand-rolling in a Go app**.

Rust-first is justified because:

1. **The first consumer is Rust.** The Rust port (plan E6) is the journal's only near-term user.
   Building in Go first would mean writing the spec twice — once in Go code, then translating it
   to Rust — for no consumer in between.
2. **The extensions are additive, not core.** Per plan decision D1, only the commit path and the
   storage contract are dual-language obligations. The journal rides on top of the commit (C1/C3
   transactions plus extra rows) and the Lambda crate is pure consumption; neither changes how a
   plain event is stored.

### What is sacrificed

- **No second implementation checking the spec.** ADR-0003's core safety net is two independent
  implementations held together by fixtures: if Go misreads the format, Rust's tests catch it.
  For the extensions there is only one implementation, so a Rust bug can ship silently.
- **Go apps must port before use.** A Go service that later needs the journal has to port it into
  `evt` from the spec and fixtures before it can use the table — a real delay when "first need"
  arrives mid-incident. The alternative (hand-rolling journal writes inside a Go app) is exactly
  the failure mode this program exists to eliminate.
- **Go tooling vs extension tables.** Go-side tooling that walks the event log — `evt-doctor`
  ([`cmd/evt-doctor`](../../cmd/evt-doctor)), enumeration, heads, rebuild — does not understand
  journal rows, manifests, or lease rows. Run against an extension-using table, it can misreport
  or, worse, act on rows it does not model.
- **Split fixture authority.** Today Go generates every golden fixture
  ([`compat/README.md`](../../compat/README.md)). Under Rust-first, the **core** fixtures stay
  Go-generated while the **extension** fixtures are Rust-generated. Authority depends on which
  implementation shipped the feature — a rule to remember when regenerating.

### Mitigations

1. **Rust-generated fixtures with byte-level spec examples.** The journal, its compaction, and the
   Lambda wire formats get fixtures (`compat/fixtures/`, extension namespace) generated from the
   Rust implementation, and `docs/spec/storage-format.md` gains byte-level examples for every new
   row — the same discipline the core follows, with the generator role swapped. A future Go port
   writes code until it passes the Rust-generated fixtures byte for byte, exactly as Rust does
   today for Go-generated core fixtures.
2. **The feature-marker row makes unsupported writers refuse.** The `SYSTEM#FEATURES` marker row
   (plan decision D4 — extension features are declared on the table and enforced at commit time;
   [ADR 0004](0004-core-reshape-multi-stream-transactions-and-outcomes.md) C9)
   is the enforcement backstop: a writer that first uses an extension declares the feature on the
   marker row — every declaration bumps the marker's `version` in the same conditional write — and
   any writer that does not support a declared feature **refuses to write to the table**.
   Enforcement is per commit, not cached-and-forgotten: every commit carries a condition on the
   marker `version`, so a writer cannot write past a declaration it has not validated. A Go
   repository that does not know the journal, pointed at a journal-using table, fails on its first
   commit after the declaration exists instead of corrupting the extension tables.
3. **Missing capabilities are ported into evt, never hand-rolled in apps.** The standing rule of
   the program (plan "Why", item 3): when an event-sourced app needs a capability evt lacks, the
   capability is ported into the framework — Go apps port Rust-first extensions from the spec and
   fixtures, with the conformance suite extended to cover them — and never reimplemented inside an
   application. Review rejects hand-rolled copies the same way it rejects mutable state in views.

## Consequences

- **`photon-evt` grows a `journal` feature flag**, so the core crate stays dependency-light; the
  journal's storage helpers sit behind it like `dynamodb` and `postgres` do today
  ([`crates/evt/README.md`](../../crates/evt/README.md)).
- **`photon-evt-lambda` is a new crate** in the workspace, released separately (release-please
  package), depending on the core. As built (#133) it needs no AWS SDK: stream records are
  DynamoDB JSON, which the core's `wire::dynamo` codecs already read, so it does not enable the
  `dynamodb` feature. Its journal wake-ups sit behind its own `journal` feature, off by default,
  because the core's `journal` feature is build-wide (below).
- **The Go port has a defined entry point.** When a Go consumer appears, the work is: port from
  spec + Rust-generated fixtures, extend `conformance` and the shared scenario list, and flip the
  Go column in [`docs/parity/feature-matrix.md`](../parity/feature-matrix.md). Until the port
  lands, the matrix records the extension as Rust ⏳ / Go —; once it ships, it moves to
  Rust ✅ / Go ⏸ deferred.
- **Fixtures gain a second generator.** `internal/compat` (Go) keeps generating core fixtures;
  the Rust side gains a generator for extension fixtures. Both live in `compat/fixtures/` and the
  `compat/README.md` table is updated to say which side writes which file.
- **The `journal` cargo feature is build-wide.** It makes the build model `journal` at format
  version 1, so every repository in the build accepts a table that declares it, wrapped in the
  journal's repository or not; a writer that bypasses the journal leaves rows missing with no gap
  to show it. The marker refuses builds without the feature, which is the protection the ADR
  promises against Go and unaware Rust builds; it cannot tell journaling writers from bypassing
  ones in a build that has the feature. Making acceptance per repository means touching every
  backend's marker validation, a commit-path change; it is tracked as a follow-up.
- **Journal compaction (X2) declares `journal-compaction`.** Its rows live in the views table under
  `journal-compaction#<scope>`, engine-owned like the counters; they are not projections, but hold
  no domain state (a snapshot of stored rows, a lease, an audit log), so wiping them only
  un-compacts the journal. A rebuild restores compacted rows rather than reading the floor, which
  keeps the journal a pure function of the events; compacting again prunes them. See the storage
  format, "Compaction (X2)".
- **Extension tables opt in explicitly.** A table is either journal-enabled (marker row declares
  it, Rust writes, Go refuses) or not (both languages behave as today). There is no partial state
  where a Go writer could half-understand the format.

See [ADR 0006](0006-parity-policy-and-feature-cuts.md) for the port/defer/cut policy this
Rust-first stance applies to, and the feature matrix for the current Go/Rust column state.
