# ADR 0003: Rust implementation and cross-language compatibility

## Status

Accepted (2026-10-12)

## Context

- Some services that share an event log are written in Rust. They need to append to and load the
  same streams that Go services write, publish the same stream envelopes, and run the same
  compaction rules, without a translation layer between them.
- The storage format was defined by the Go code: DynamoDB item layout, PostgreSQL schema, the
  `encoding/json` output of serialized events, metadata, and snapshots, the SNS envelope, and the
  snapshot policy. Most of it was written down in `BEHAVIORAL_INVARIANTS.md`, but byte-level details
  (key order, HTML escaping, base64 payloads, null handling) existed only in the code.
- There are three ways to give Rust access to the log:
  1. **Shared core through FFI or WASM.** Compile one implementation and call it from the other
     language.
  2. **Generated code** from a schema shared by both languages.
  3. **Two native implementations** held together by a shared, executable specification.
- A shared core makes every Rust build depend on a Go toolchain (or every Go build on a Rust one),
  complicates cross-compilation and static linking, and puts an FFI boundary on the commit path,
  where async I/O, cancellation, and error types do not map cleanly across languages. Generated
  code covers data shapes but not behavior: concurrency conditions, the snapshot policy, upcasting
  order, compaction safety, and duplicate-command detection.

## Decision

Maintain two native implementations, Go's `evt` and the Rust `photon-evt` crate (`crates/evt`),
each with idiomatic APIs, and enforce compatibility with shared fixtures and a live
cross-language test run instead of shared code.

### 1. The storage format is a written contract

[`docs/spec/storage-format.md`](../spec/storage-format.md) documents every byte both
implementations depend on, including the deliberate differences between them.

### 2. Go generates golden fixtures; both languages assert them

`internal/compat` holds a Go implementation of a small shared domain (an account aggregate with
upcasting, maps, optional fields, and multi-event commands). Its fixture generator writes
`compat/fixtures/codec.json` (encode/decode vectors, DynamoDB items, SNS messages and attributes,
and the snapshot policy table) and `compat/fixtures/scenarios.json` (the stored items, events,
snapshot, and loaded state after each scenario in `compat/scenarios.json`).

`go test ./...` fails if the Go output drifts from the fixtures. `cargo test --workspace` runs the
same scenarios through the Rust crate and fails if its output differs by a single byte.

### 3. A live run interleaves both languages on the same streams

`scripts/compat-integration.sh` builds a Go and a Rust driver. For each scenario step, the named
writer (`go` or `rust`) executes it against the local DynamoDB emulator or PostgreSQL, so each
language extends streams, snapshot chains, and compacted streams that the other started. Both
drivers then verify every stream against the fixtures: raw items or rows, events and snapshots read
through the repository, and the loaded state.

### 4. Each implementation runs its own conformance suite

Go's `conformance` package and Rust's `photon_evt::conformance` module check each backend against
the repository contract. They are not shared code. The fixtures and the live run cover
cross-language agreement.

## Consequences

- Each language has an API that fits it: Rust uses traits, `async fn`, typed event enums, and
  `Result` errors. Go keeps its existing packages unchanged.
- Neither build depends on the other language's toolchain. Only the compatibility checks need both.
- Behavior can drift between the implementations, so the fixtures are the guard. Any change to the
  storage format must regenerate the fixtures and pass in both languages, as described in the
  spec's "Changing the Format" section. CI runs the Rust checks when Rust or shared fixture files
  change, and the live cross-language run when the storage format may have changed.
- Features are ported deliberately, not automatically. The Rust crate covers the event log,
  snapshots, upcasting, compaction, entity heads, in-band projectors, and the stream envelope.
  Go-only features (stream publishers, projector runtimes, rebuild tooling) stay Go-only until
  needed.
- The documented differences (`null` payloads, float formatting, `jsonb` metadata) are tested and
  must stay documented rather than silently changed.
