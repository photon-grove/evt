# Cross-language compatibility

This directory holds the shared inputs and golden outputs that keep the Go `evt` packages and the
Rust `photon-evt` crate wire-compatible. The contract itself is
[`docs/spec/storage-format.md`](../docs/spec/storage-format.md), and the reasoning is in
[ADR 0003](../docs/adr/0003-rust-implementation-and-cross-language-compatibility.md).

| Path                      | Written by   | Contents                                                                                                                                                                                              |
| ------------------------- | ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scenarios.json`          | hand         | Scenarios: a snapshot interval and steps, each naming its writer and expected outcome. The v0.9 scenarios (C1–C10) that E2.8/E3.8 must add are listed in the [storage format](../docs/spec/storage-format.md#compat-scenarios-to-add). |
| `fixtures/codec.json`     | Go generator | Encode/decode vectors: serialized JSON, DynamoDB items, SNS messages and attributes, view items, `ttl` items under a fixed clock, lenient decoding, integer map-key order, the snapshot policy table. |
| `fixtures/scenarios.json` | Go generator | For each scenario: stored events and snapshot, DynamoDB items, SNS messages, sequences, and loaded state.                                                                                             |

Both sides implement the same account domain: `internal/compat/account.go` and
`crates/evt-compat/src/account.rs`. Keep them identical, field for field and rule for rule.

## Tests

| Command                                  | Checks                                                                                                         |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `go test ./internal/compat`              | The Go output still matches the fixtures.                                                                      |
| `cargo test -p evt-compat`               | The Rust output matches the fixtures byte for byte, running every step in Rust against `MemRepository`.        |
| `scripts/compat-integration.sh dynamo`   | Go and Rust take turns writing the same streams in the local DynamoDB emulator, then both verify every stream. |
| `scripts/compat-integration.sh postgres` | The same against local PostgreSQL.                                                                             |

The live runs need the local stacks from the root [README](../README.md#local-development). Moon
runs them, together with the Rust conformance suite, as `evt-rust:integration` and
`evt-rust:integration-postgres`.

## Regenerating fixtures

Fixtures change only when the storage format changes on purpose:

```sh
go test ./internal/compat -run TestFixtures -update
cargo test --workspace
```

Review the fixture diff. Every changed byte is a format change that existing data and the other
language must handle. Then update the Rust side until it passes, add upcasters if stored payloads
changed, and update the spec.

## Adding a scenario

1. Add an entry to `scenarios.json`. Steps run in order, and `by` is `go` or `rust`. Ops:

   | `op`            | Fields                                                           | Outcome `result`                                             |
   | --------------- | ---------------------------------------------------------------- | ------------------------------------------------------------ |
   | `execute`       | `command`, optional `commandId`, `trace`, `address`, `timestamp` | `ok` (with `events`), `rejected`, `duplicate`, or `conflict` |
   | `appendRaw`     | `event` (`type`, `version`, `payload`), optional `commandId`     | `ok` (with `events`) or `conflict`                           |
   | `compact`       | `through`                                                        | `compacted` (with `deleted`) or `uncovered`                  |
   | `conflictProbe` | `sequence`                                                       | `conflict`, or `ok` when the sequence is free                |

2. If the scenario needs a new command or event, add it to both account domains.
3. Regenerate the fixtures and run both test suites and both live runs.

Use neutral names and local values only. This repository is public.
