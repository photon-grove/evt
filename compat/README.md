# Cross-language compatibility

This directory holds the shared inputs and golden outputs that keep the Go `evt` packages and the
Rust `photon-evt` crate wire-compatible. The contract itself is
[`docs/spec/storage-format.md`](../docs/spec/storage-format.md), and the reasoning is in
[ADR 0003](../docs/adr/0003-rust-implementation-and-cross-language-compatibility.md).

| Path                      | Written by   | Contents                                                                                                                                                                                              |
| ------------------------- | ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scenarios.json`          | hand         | Scenarios: a snapshot interval and steps, each naming its writer and expected outcome. Includes the C6 `metadata-extensions` handoff; remaining v0.9 scenarios for E2.8/E3.8 are listed in the [storage format](../docs/spec/storage-format.md#compat-scenarios-to-add). Includes the C10 `rebuild-fence` scenario, which is `exclusive`. |
| `fixtures/codec.json`     | Go generator | Encode/decode vectors: serialized JSON, DynamoDB items, SNS messages and attributes, view items, `ttl` items under a fixed clock, lenient decoding, integer map-key order, the snapshot policy table. |
| `fixtures/outcomes.json`  | Go generator | C3/C4 identity-digest vectors (canonical bytes and SHA-256, keyed by `targets`, including a rename whose write set comes from state), the encodings of outcome rows (DynamoDB item, budgeted item size, PostgreSQL row, rejection and event-ID JSON, including both stale-fence rejections and a rejection that lists the events it committed), the canonical text of rejection-detail numbers, and the C5 retry-policy defaults both languages assert. |
| `fixtures/feature-marker.json` | Go generator | C9 canonical manifest JSON, complete DynamoDB marker items, PostgreSQL declaration rows, ordered commit requests, stream-head requests, the C7 `commitViews` requests (the exact actions, tables, conditions, and values of a commit with commit-wide view operations, and their budget sizes), the C10 `rebuildFence` vectors (every request the DynamoDB rebuild lifecycle sends, seeded from a stored fence state under a fixed clock: the consistent reads, the acquire, takeover, release, and force-release transactions, the lease renewal, and a guarded view write, with the items the table holds before and after), and schema-upgrade DDL. |
| `fixtures/scenarios.json` | Go generator | For each scenario: stored events and snapshot, DynamoDB items (including the stream head item at `sk = -1`), the stream's `head` (the PostgreSQL heads row), SNS messages, sequences, and loaded state; the view rows commit-wide projectors left in the scenario's partitions (`views`); and the `TransactWriteItems` action order of each transaction step, where an action on the views table carries `table: "views"` and its string sort key in `skText`. |

Both sides implement the same account domain: `internal/compat/account.go` and
`crates/evt-compat/src/account.rs`. Keep them identical, field for field and rule for rule.

## Tests

| Command                                  | Checks                                                                                                         |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `go test ./internal/compat`              | The Go output still matches the fixtures.                                                                      |
| `cargo test -p evt-compat`               | The Rust output matches the fixtures byte for byte, running every step in Rust against `MemRepository`.        |
| `scripts/compat-integration.sh dynamo`   | Go and Rust take turns writing the same streams in the local DynamoDB emulator, then both verify every stream. |
| `scripts/compat-integration.sh postgres` | The same against local PostgreSQL.                                                                             |

The marker vectors include duplicate-input set canonicalization, the complete transaction order
(marker check, rebuild-lock check, projector operations, then per written stream its head update,
event puts, and snapshot put), the marker row a writer stores when it declares `stream-heads` on a fresh table, and, under
`streamHeads`, the exact actions, expressions, name placeholders, and values of every head request
shape (create, append, later snapshot, fences, retained streams, and the out-of-band snapshot guard)
with the commit budget's item sizes. Rust consumes these
vectors in its fixture tests and DynamoDB transaction tests. With `--features drivers`, the live
PostgreSQL tests also compare declaration-row bytes and execute both languages' schema DDL in
either creation/upgrade order.

Live runs write view rows to the DynamoDB views table (`EVT_COMPAT_DYNAMO_VIEWS_TABLE`, default
`evt-local-entity-views`, string `pk` and `sk`) or, on PostgreSQL, to the `evt_compat_views` table
that each driver creates itself — evt defines no PostgreSQL view schema, so the harness owns this
one, with the statements both drivers issue (a plain `INSERT` for an `absent` put, an upsert
otherwise, and a `DELETE`).

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
   | `execute`       | `command`, optional `commandId`, `principalId`, `causationId`, `extensions`, `trace`, `address`, `timestamp` | `ok` (with `events`), `rejected`, `duplicate`, or `conflict` |
   | `appendRaw`     | `event` (`type`, `version`, `payload`), optional metadata as above     | `ok` (with `events`) or `conflict`                           |
   | `compact`       | `through`                                                        | `compacted` (with `deleted`) or `uncovered`                  |
   | `putSnapshot`   | `snapshot` (`sequence`, `payload`), `eventSequence`, optional `suffix` | `ok` (also a no-op regression) or `drift` (the snapshot is ahead of the head, or the stream has no head item) |
   | `appendRawBypassingHead` | `event` (`type`, `version`, `payload`), optional `suffix`, `commitId` | `ok`: the event is stored and the stream's head item is not updated |
   | `conflictProbe` | `sequence`                                                       | `conflict`, or `ok` when the sequence is free                |
   | `outcomeOnly`   | `identity` (`type`, `payload` object), `response`, `commandId`   | outcomes scenarios: an accepted outcome with no events       |
   | `resolveOutcome`| `command`, `commandId`                                           | outcomes scenarios: `absent`, a replayed outcome, or `reuse` |
   | `rebuild`       | `action` (`begin`, `renew`, `end`, `forceRelease`, `generation`, `writeViews`, `wait`), `owner`, `leaseSeconds`, `seconds` | `ok`, `held`, `leaseLost`, `settled`, or `unavailable`, with `delta` (the generation after the step minus before), `forceReleased`, and (`writeViews`) `rows`; an `execute` or `transaction` step during a rebuild expects `rebuildInProgress` |

   A `transaction` step may also carry `projectors` (C7): a list of `{name, operations}` whose
   operations (`op` `put` or `delete`, `pk`, `sk`, `owner`, `entityType`, `payload`, `condition`
   `absent` or `present`, `conflict` for a mapped error message, and `pkPad`/`skPad` to extend a key
   with `x` to that many bytes) are the commit-wide view operations of the commit, in declaration
   then emission order. `{id}` in a key, owner, or payload is the scenario's entity ID, so rows
   are scoped to the run. The step's `expect` adds the results `projector` (a failed condition,
   with the `projector` name, the `operation` index, and the `mapped` message), `projectorOnly`,
   `viewKeyTooLong`, and `duplicateViewKey`.

   A scenario with `"projector": true` registers the shared ledger projector (`ledger`, in both
   account domains) on its stores, so every commit that writes events puts or deletes the
   stream's row at `pk = "ledger"`, `sk` = the stream ID.

   A scenario with `"exclusive": true` (C10's `rebuild-fence`) holds store-wide state across its
   steps: a rebuild lock refuses every other scenario's commits. The interleaved run skips it, and
   `scripts/compat-integration.sh` runs it alone afterwards, one step at a time (`evt-compat steps
   -only NAME`, `step -only NAME`, and `exclusive` in the Go driver; the Rust driver takes the same
   arguments). `rebuild` owner tokens are scoped to the run and scenario like command IDs, and a
   `writeViews` step writes one view row in the partition `rebuild-<entity ID>`. The fixture runs
   advance an injected clock for `wait`; the live runs sleep.

   A scenario with `"outcomes": true` runs on stores with command outcomes enabled (C3/C4): every
   step needs a `commandId`, and `execute` steps record and replay outcomes. Outcomes steps
   expect `ok` (with `events`, 0 for a no-op), `rejected` (with `code`), `reuse`, `idTooLong`,
   `ambiguous` (a row still absent after the recovery window), or `absent`, plus `"replayed": true` when the stored outcome is returned. An
   `execute` step may set `injectCommitFault` (`applyThenFail` or `failWithoutApplying`) to make
   its commit apply, or drop, while the caller observes an indeterminate error. The harness scopes
   command IDs to the run and scenario (`compat-<run>-<scenario>-<id>`) and uses a digest that
   maps the run name in the target stream IDs to the fixture run's, so digests are run
   independent; the digested payload is the step's `command` object with sorted keys.
2. If the scenario needs a new command or event, add it to both account domains.
3. Regenerate the fixtures and run both test suites and both live runs.

Use neutral names and local values only. This repository is public.
