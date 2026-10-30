# ADR 0009: TypeScript implementation (`@photon-grove/evt`)

## Status

Proposed (2026-10-30). Tracking: the `TS` column of
[`docs/reference/feature-matrix.md`](../reference/feature-matrix.md).

## Context

- evt has two native implementations, the Go module and the Rust `photon-evt` crate, held together
  by a written storage format, golden fixtures and a live interleaved run
  ([ADR 0003](0003-rust-implementation-and-cross-language-compatibility.md)). A Go writer and a
  Rust writer can take turns on the same DynamoDB or PostgreSQL stream.
- A TypeScript implementation would give Node, Bun, Deno, edge runtimes and eventually browser
  clients one data layer that reads and writes the same log. No consumer needs it yet. The port
  is for reach, so a small, correct, well-specified core is worth more than breadth.
- The options are the ones ADR 0003 weighed. A shared core compiled to WASM (from the Rust crate)
  would put a Rust toolchain behind every TS build and an async WASM boundary on the commit path,
  and it would cover only the codecs: the rest of a backend is SDK and driver calls, which have to
  be TypeScript anyway. Generated code covers shapes, not behavior. So TS joins as a third native
  implementation, held to the same fixtures.
- JavaScript makes the format harder to reproduce than Go or Rust do:
  - `number` is a float64, so `JSON.parse` rounds integers past 2^53. `codec.json` has a stored
    event at sequence 9007199254740993 (`large-numbers`), and `JSON.parse` reads it as 9007199254740992.
  - `JSON.stringify` doesn't escape `<`, `>`, `&`, U+2028 or U+2029. It writes lone surrogates as
    `\udXXX`, where Go writes `\ufffd`. It has no notion of a sorted map.
  - JavaScript objects put integer-like keys first, in numeric order, whatever order they were
    inserted in. `{"10": …, "2": …}` stringifies as `{"2":…,"10":…}`, the reverse of the Go order
    that `codec.json`'s `intKeyMap` pins.
  - `string.length` counts UTF-16 code units, but the per-item budgets count UTF-8 bytes.
  - The common runtime surface has no `Buffer`, and Node 24 lacks `Uint8Array.prototype.toBase64`.

### Spike

A throwaway encoder of about 120 lines (Go-compatible string escaping, a struct/map split, base64
without `Buffer`, `TextEncoder` byte counts, `bigint` sequences) checked the `events`, `snapshots`,
`metadataDecode` and `intKeyMap` vectors of `codec.json` on Node 24. Every serialized event,
snapshot and metadata vector matched byte for byte, including `escaping`, `unicode-entity-id` and
`large-numbers`. Web Crypto's `crypto.subtle.digest` reproduced all nine identity digests in
`outcomes.json`. Two metadata rules failed the first attempt. The prose states both, but it was
the vectors that caught them:

- The canonical encoder writes the effective `causationId`, which is the `commandId` when none was
  set. Decoding leaves it `null` (Go applies the default at encode time). A `metadataDecode`
  vector decodes and then re-encodes, so a legacy object with a command ID comes back with
  `causationId` filled in.
- A decoded empty `address` stays `""`. Only the write path turns an empty address into `null`.

The `eventDecode` vectors, which the spike didn't run, add a third rule: a `null` event payload
stays `null`. That is the spec's known Go/Rust difference (Rust writes `""`), and TS takes Go's
side because Go generates the vector.

So the fixtures, as much as the prose, are the specification TS is built against. Reading the fixtures
themselves needs a lossless JSON reader, since `JSON.parse` already rounds `codec.json`.

## Decision

Build `@photon-grove/evt`, a native TypeScript implementation that shares no code with Go or Rust,
holds itself to the Go- and Rust-generated fixtures, and joins the live interleaved runs. Go keeps
generating the core fixtures and Rust the Rust-first extension fixtures. TS generates none. If TS
finds a real ambiguity in the spec, the spec and the Go or Rust generator are fixed and the fixtures
regenerated ([`compat/README.md`](https://github.com/photon-grove/evt/blob/main/compat/README.md)), never
hand-edited.

### 1. Layout

```text
packages/
  evt/                  @photon-grove/evt, published (moon project evt-ts)
    src/                core, wire codecs, and one directory per subpath
    examples/           getting-started.ts, checked against docs/guide/getting-started.md
  evt-compat/           private: the fixture runner and, from phase 2, the live-run driver
  react-flow-diagrams/  unchanged
```

`packages/*` is already in `pnpm-workspace.yaml` and in moon's project globs, so the layout adds no
workspace configuration. JavaScript's convention puts packages under `packages/`, as Cargo's
puts crates under `crates/`. `evt-compat` mirrors `crates/evt-compat`. It's a separate private
package that imports `@photon-grove/evt` through its `exports` map, so the fixtures exercise the
published entry points, not source paths. A later `@photon-grove/evt-lambda` goes in
`packages/evt-lambda`.

The moon project sets `id: evt-ts` explicitly, because without it `packages/evt` falls back to
the id `evt`, which `go/moon.yml` already uses. The tasks are `evt-ts:check`
(`tsc --noEmit`, oxlint, and a check that `src/` imports no Node built-in), `evt-ts:test`
(Vitest unit tests plus the fixture runner), `evt-ts:build`, and `evt-ts:runtimes`. From phase 2
there are also `evt-ts:integration` and `evt-ts:integration-postgres`. Oxlint lints without
depending on the TypeScript compiler API, which the repo's TypeScript 7 toolchain changed.

`ci.yml`'s `changes` job gains a `ts` output. It's set by `packages/evt/*`, `packages/evt-compat/*`,
`compat/*`, `docs/guide/getting-started.md`, `.github/*`, `.moon/*`, `package.json`,
`pnpm-lock.yaml` and `pnpm-workspace.yaml`. The two package paths also set `compat` from phase 2,
when TS joins the live runs. They get their own `case` arm, ahead of the catch-all, so a TS-only
change doesn't run the Go jobs. A new `typescript` job runs when `ts` is true.

### 2. Packaging

One package with subpath exports, ESM only:

| Entry point                       | Contents                                                                        | Peer dependency (optional) |
| --------------------------------- | ------------------------------------------------------------------------------- | -------------------------- |
| `@photon-grove/evt`               | Types, codecs, aggregates, `Store`, upcasting, snapshot policy, retry, envelope | none                       |
| `@photon-grove/evt/memory`        | `MemRepository`                                                                 | none                       |
| `@photon-grove/evt/testing`       | given/when/then helpers over `MemRepository`                                    | none                       |
| `@photon-grove/evt/dynamodb`      | `DynamoRepository` and the DynamoDB item codec                                  | `@aws-sdk/client-dynamodb` |
| `@photon-grove/evt/postgres`      | `PgRepository`                                                                  | none (see below)           |
| `@photon-grove/evt/journal` later | The journal (ADR 0005)                                                          | none                       |

- **One package, not one per backend.** Subpaths keep the core free of SDKs (nothing imports the
  AWS SDK unless it imports `/dynamodb`), and they keep one version number, which matches the
  backend-neutral contract
  ([ADR 0002](0002-backend-neutral-storage-contracts.md)): a release is one set of behaviors across
  every backend. `"sideEffects": false` lets bundlers drop unused subpaths.
- **DynamoDB** uses the low-level `@aws-sdk/client-dynamodb` client (`AttributeValue` maps), not
  `lib-dynamodb`'s document client. The format pins exact attribute types (`N` versus `S`), and
  the document client's marshalling would choose them for us. The item codec is pure and imports
  no SDK runtime code, so phase 1 can assert every `dynamoItem` vector without the SDK installed.
- **PostgreSQL** takes a structural client interface shaped like `pg`'s `query(text, values)`
  plus a way to hold one connection for a transaction. `pg`, Neon's serverless driver and PGlite
  all fit it with no adapter, so `/postgres` imports no driver at all. `pg` is an optional peer
  dependency for its types only. Reads select `metadata::text` and `int8` columns as text, so
  the driver's `JSON.parse` and its `number` parsing never see stored values.
- **ESM only.** Node 22.12 and later can `require()` an ES module whose graph has no top-level
  await, so CommonJS callers still load the package. The package therefore never uses top-level
  await. A dual build would add the dual-package hazard (two copies of the error classes, so
  `instanceof` fails across them) for no reader who needs it.
- No runtime dependencies. ULIDs come from `crypto.getRandomValues`, SHA-256 from
  `crypto.subtle.digest`, and base64 from our own code.
- `tsc` (TypeScript 7, the repo's version) emits `dist/` with declarations, and `exports` maps each
  subpath to its `types` and `default`. Publishing to JSR is left for later. Deno imports the npm
  package with `npm:` specifiers.

### 3. Runtime targets

The core and `/memory` use only the web-standard globals that every target runtime has:
`TextEncoder`, `TextDecoder`, `crypto.getRandomValues`, `crypto.subtle.digest`, `AbortSignal`,
`setTimeout` and `queueMicrotask`. The type system enforces this. `src/` compiles with
`lib: ["ES2022"]` and `types: []`, and a `globals.d.ts` declares exactly those globals, so
`Buffer`, `process` or `document` in core code is a type error before lint sees it. The import
check rejects `node:*` and bare built-in names. ES2022 is the language floor: it has `bigint`,
`Object.hasOwn` and `Error.cause`, and Safari 16.4 and current edge runtimes support it.

| Runtime                  | CI runs                                                                                                        |
| ------------------------ | -------------------------------------------------------------------------------------------------------------- |
| Node 24                  | Everything: typecheck, lint, build, unit tests, the fixture runner, and the live runs                          |
| Node 22, Bun, Deno       | The fixture runner and the memory backend's scenarios, against the built `dist/`                               |
| Edge (workerd), browsers | None at first. The globals contract and the import check guard them, and a Miniflare smoke test is a follow-up |

The fixture runner in `evt-compat` is a plain program with its own assertions, not a Vitest
suite, so all three runtimes run it unchanged (Deno with `--allow-read`). Wire fidelity is what a
runtime can break (`TextEncoder`, `bigint`, Web Crypto, JSON), and the runner is where wire
fidelity is checked. Unit tests run under Vitest on Node 24. `engines.node` is `>=22.12`, the
oldest Node the runner covers. The CI job pins exact Bun and Deno versions
(`oven-sh/setup-bun`, `denoland/setup-deno`).

### 4. Wire-fidelity hazards

Every hazard gets a vector-backed test in `evt-compat`. Where no vector exists yet, the gap is
closed in the Go generator first, never with a TS-only expectation.

| Hazard            | Decision                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | Vectors                                                                                                                                                                                                                                                                               |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 64-bit integers   | Unbounded counters are `bigint` everywhere in the API and on the wire: event sequence, snapshot sequence, snapshot event sequence, stream head, journal position, rebuild generation, marker version. Small declared integers are `number`, checked with `Number.isSafeInteger` where they're decoded: event version, snapshot interval, `latest(n)`, budgets, TTL seconds. Every decoder range-checks int64 and refuses `1e3`, `1.0` or `"1"` where Go and Rust refuse them.       | `codec.json` `large-numbers` (2^53 + 1). Later `views.json` `codecDecode` (`count-overflow` at 2^63 refused, `big` at 2^64 − 1)                                                                                                                                                       |
| Canonical JSON    | evt's own writer, never `JSON.stringify`. Plain objects are structs and encode in own-property order. `Map` is a Go map and encodes sorted by the UTF-8 bytes of its keys. `Uint8Array` is `[]byte` (base64), `bigint` is an integer. The writer refuses a plain object with an integer-like key, because the engine has already reordered it, and it says to use a `Map`. Engine-owned shapes (metadata `trace` and `extensions`) are `Map`s inside the codec.                     | `codec.json` `events`, `snapshots`, `views`, `intKeyMap`, `metadataDecode`                                                                                                                                                                                                            |
| String escaping   | Go's rules: `<`, `>`, `&`, U+2028 and U+2029 as `\u00XX`/`\u20XX`, the short escapes for `\b \f \n \r \t`, lowercase `\u00XX` for other controls, and `\ufffd` for a lone surrogate.                                                                                                                                                                                                                                                                                                | `codec.json` `escaping`, `unicode-entity-id`. No vector has `\b` or `\f` yet, so phase 1 adds them in the Go generator. A lone surrogate has no Go or Rust input (Go strings are bytes, Rust strings are valid UTF-8), so it gets a TS unit test that mirrors Go's invalid-UTF-8 rule |
| Number text       | Engine documents carry only integers. Rejection `details` keep each number's lexeme: the lossless reader keeps the literal's text, and the canonical rewrite works on that text with decimal digits, never through float64 (16,000-digit scales, 2^53 + 1 kept exact). `Number#toString` is close to Go's float64 text (both shortest round-trip, with exponents below 1e-6 and from 1e21; negative zero differs), but floats stay unnormalized across languages, as the spec says. | `outcomes.json` `rejectionNumbers` (phase 4)                                                                                                                                                                                                                                          |
| Byte sizes        | Every budget counts UTF-8 bytes with a counting loop that doesn't allocate, and a lone surrogate counts as 3 (the U+FFFD `TextEncoder` writes). `.length` never appears in a budget.                                                                                                                                                                                                                                                                                                | `codec.json` `snapshotBudget` (379,923 / 379,924 bytes), `scenarios.json` `commit-too-large` and `outcomes.json` budgeted row sizes (phase 4)                                                                                                                                         |
| Base64 and binary | Our own standard padded encoder and Go-compatible decoder. Payloads are `Uint8Array` at the repository boundary (DynamoDB `S` through a fatal `TextDecoder`, PostgreSQL `bytea`, base64 in serialized JSON). The format stores no `B` attribute, and a decoder refuses one where it expects `S`. View cursors are padded base64url.                                                                                                                                                 | `codec.json` `events` and `eventDecode`, `outcomes.json` `rows` and `malformedRows`, `views.json` `cursors`, `lambda.json` records                                                                                                                                                    |
| Lenient decoding  | Engine documents decode by Go's rules as the vectors pin them: a missing field or `null` gives the zero value, unknown fields are ignored, a wrong JSON type is an error, a missing `causationId` stays `null` until the encoder writes its effective value, and a `null` payload stays `null`. Field names match exactly, as in Rust. Go's case-insensitive matching isn't reproduced, and the spec's known differences gain a TS entry for it.                                    | `codec.json` `metadataDecode`, `eventDecode`, `views.json` `decodes`                                                                                                                                                                                                                  |
| Time              | The clock is injected (`() => number`, Unix milliseconds). Metadata timestamps are truncated to whole seconds. TTL is `floor((clockMs + retentionMs) / 1000)` in integers, which equals Go's nanosecond rule for whole-millisecond retentions. A sub-millisecond retention is refused.                                                                                                                                                                                              | `codec.json` `ttl` (clock and retention 0.9 s and 0.5 s past a second)                                                                                                                                                                                                                |

Application payloads are the application's business. An event type supplies a codec
(`encode(data): Uint8Array`, `decode(bytes): data`). The default `json()` codec uses evt's writer
and accepts an optional [Standard Schema](https://standardschema.dev) validator (Zod, Valibot and
ArkType implement it) without depending on any of them. A TS domain that has to match a Go or Rust
domain byte for byte, as the compat account domain must, uses `Map` wherever Go uses a map.

### 5. API shape

The concepts and names stay recognizably the same as Rust and Go (`Aggregate`, `Store`,
`Repository`, `MemRepository`, `Metadata`, `Upcaster`, `compactBelow`, `latest`, `transaction`,
`Rejection`, `CommandIdentity`), in camelCase, so the docs can show all three side by side. The
idioms are TypeScript's:

```ts
import { defineAggregate, defineEvents, json, Store } from "@photon-grove/evt";
import { MemRepository } from "@photon-grove/evt/memory";

const events = defineEvents({
  "account:deposited": { version: 1, codec: json<{ amountCents: number }>() },
});

type Deposit = { type: "deposit"; amountCents: number };

const account = defineAggregate({
  type: "account",
  events,
  state: json<{ balanceCents: number }>(),
  initial: () => ({ balanceCents: 0 }),
  handle(_state, command: Deposit) {
    if (command.amountCents <= 0) {
      return {
        kind: "reject",
        rejection: { code: "invalid", message: "amount must be positive" },
      };
    }
    return {
      kind: "accept",
      events: [
        {
          type: "account:deposited",
          data: { amountCents: command.amountCents },
        },
      ],
    };
  },
  apply: (state, event) => ({
    balanceCents: state.balanceCents + event.data.amountCents,
  }),
});

const store = new Store(new MemRepository(), { region: "us-west-2" });
const result = await store.execute(
  account,
  "acct-1",
  { type: "deposit", amountCents: 100 },
  {
    metadata: { commandId: "cmd-1" },
    signal: AbortSignal.timeout(5_000),
  },
);
```

The sketch shows the shape. Exact names are settled in the phase 1 PRs.

- **Typed events and commands.** `defineEvents` maps each event type to its version and codec, and
  the event union (`{ type: 'account:deposited'; data: … }`) is inferred from it, so `apply`
  narrows on `event.type`. Upcasters are declared beside the event type and run in declaration
  order, as in the spec.
- **`apply` is a reducer** that returns the next state. Every attempt starts from a freshly loaded
  state, which is Rust's behavior and avoids Go's reset-between-attempts difference.
- **Decisions and results are discriminated unions.** `handle` returns `accept` (events, possibly
  none) or `reject` (a `Rejection`, optionally with events, per ADR 0004). `execute` resolves to
  `committed`, `duplicate`, `rejected` or `replayed`. Failures throw `EvtError` subclasses with a
  `code` discriminant (`conflict`, `commitTooLarge`, `streamHeadDrift`, `reserved`, `unsupported`,
  `storeNotInitialized`, `rebuildInProgress`, `indeterminate`, …) and the cause in `Error.cause`.
  Domain outcomes are values and infrastructure failures are exceptions, the same split as Rust's
  `Ok(outcome)` versus `Err`.
- **`AbortSignal` everywhere.** Every async call takes `{ signal }`. An abort before a request is
  sent leaves nothing written and rejects with the signal's reason. An abort after the commit
  request is sent may race a commit that applied, so it surfaces as `indeterminate`, never as
  "not written". Command outcomes depend on that distinction. The DynamoDB backend passes the
  signal to `client.send(command, { abortSignal })`. The PostgreSQL client interface takes an
  optional signal, and a client that can't cancel is checked between statements.
- **Retry** is `executeWithRetry` with a `RetryPolicy` whose defaults are the ones the fixtures
  pin, sleeping with `setTimeout`, or with an injected `sleep` in tests.
- **No tracing dependency.** The Rust load `tracing` fields (`eventsReplayed`, `snapshotSeq`)
  become an optional observer hook when a consumer asks for one.

### 6. Phasing

[ADR 0006](0006-parity-policy-and-feature-cuts.md)'s thresholds, applied to TS. With no consumer
yet, "needed in the window" is read against the goal of a shared data layer. Here one rule decides
most rows: **a TS writer on a shared table must honor every store-wide guard that Go and Rust
writers rely on**, so whatever the commit path of a shared table requires is "required by
invariant" and ported. TS refuses whatever it doesn't implement: a table that declares
`journal`, `journal-compaction` or `postgres-views` is refused through the feature marker, as Go
refuses it today.

| Phase                      | Ships                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | Gate                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. Core and memory         | Wire codecs (serialized event, metadata with every v0.9 key, snapshot, DynamoDB item codec, stream envelope, TTL), aggregates and `Store`, upcasting, `MemRepository` with per-stream head items, optimistic concurrency, inline snapshots, the snapshot policy and the oversized-snapshot guard, the commit budget (control checks counted), the duplicate-command window, plain (unpersisted) rejections, reserved-namespace refusal, `compactBelow`, `latest(n)`, `RetryPolicy`, per-aggregate snapshot interval, `/testing`, and the conformance suite on mem | Every `codec.json` vector and the `outcomes.json` `retry` defaults, plus the `scenarios.json` scenarios that need nothing beyond phase 1, byte for byte on the memory backend, on every CI runtime. Those scenarios are `metadata-extensions`, `go-writes-rust-reads`, `rust-writes-go-reads`, `interleaved`, `compaction-handoff`, `upcast-legacy`, `conflict-probe`, `dedupe-window`, `batch-crosses-snapshot`, `snapshots-disabled`, `stream-heads-snapshot-guard` and `latest-n`. Their DynamoDB items and SNS messages are asserted through the pure codecs, and their PostgreSQL `head` rows wait for phase 3. |
| 2. DynamoDB                | `DynamoRepository`. The feature marker, in memory and on DynamoDB: validation, the per-commit check, explicit initialization and lazy initialization, refusal of unsupported features. The rebuild-lock check in every commit (refusal only, without the lifecycle). Retention TTL, the in-band per-entity view projector, and the conformance suite on DynamoDB                                                                                                                                                                                                  | `scripts/compat-integration.sh dynamo` with TS writing (below), plus the `feature-marker.json` single-stream commit, head and marker request vectors (the fence shapes wait for phase 4)                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 3. PostgreSQL              | `PgRepository`: the schema and DDL vectors, `READ COMMITTED`, canonical lock keys, the heads table                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `scripts/compat-integration.sh postgres` with TS writing, plus `postgresSchemas`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 4. Core reshape (ADR 0004) | Multi-stream transactions, read fences, command outcomes and outcome lookup, rejections (with events), fence-aware retry, commit-wide projectors, marker declarations, the rebuild-fence lifecycle, then enumeration, snapshot-seeded full rebuild and entity heads, which a TS-hosted projector needs for wipe-and-replay                                                                                                                                                                                                                                        | The remaining `scenarios.json` scenarios, `outcomes.json`, `feature-marker.json` (`commitViews`, `rebuildFence`), in memory and live                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 5. Reads and extensions    | The view repository and the typed view codec (`views.json`), then the Rust-first extensions from their Rust-generated fixtures: journal, journal compaction, commit receipts, the journal on PostgreSQL, wake-ups, the PostgreSQL views table, and `@photon-grove/evt-lambda` (`lambda.json`)                                                                                                                                                                                                                                                                     | `views.json`, `journal.json`, `lambda.json`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |

Phase 1 reaches past the base tier where the wire makes it necessary. Every stored event carries
the v0.9 metadata keys, and every commit counts the marker and rebuild-lock checks in its budget,
on every backend. `compactBelow` and `latest(n)` are in phase 1 because the phase-1 scenarios call
them and the memory implementation is small.

Until phase 4, a TS writer must not write an entity type that has commit-wide projectors
registered in a Go or Rust service, because it would commit events without their view rows. This
rule already binds Go and Rust: every writer of a type registers that type's projectors. For the
same reason, a TS writer must not share a store whose Go or Rust writers record command outcomes
until phase 4: its commits would carry no outcome rows, so a command retried through TS could run
twice. The TS README states both rules.

**Joining the live runs.** Each scenario step names its writer (`go` or `rust`). Each driver
executes only its own writer's steps, and the harness stamps the name into the event's
`origin.endpoint` and into its commit ID (`compat-<by>-<commandId>`). A TS driver takes `--as go`
or `--as rust`, executes the steps of the language it stands in for, and stamps the same names, so
the fixtures and `scenarios.json` don't change. `compat-integration.sh` gains two passes: TS
standing in for Go, then for Rust.

The passes widen as phases land. The TS driver lists the scenarios it supports. The Go and Rust
drivers gain a flag that limits them to a list of scenarios, since today `-only` names a single
exclusive scenario. In a pass, the language TS stands in for still runs the steps of the
scenarios TS doesn't support. After each pass, Go and Rust verify every stream, and TS verifies
the streams of the scenarios it supports. TS also joins the enumeration check, where the script
diffs each language's `enumerate` output.

A third writer name in `scenarios.json` would change the stamped values and regenerate every
fixture, for no coverage the substitution passes lack.

The matrix's `TS` cells record each row's phase as `⏳ (phase N)`. Three rows are deferred, as
`⏸ (ADR 0009: reason)`: the Rust load `tracing` fields (an observer hook on first need), the
catch-up snapshot on read-only loads (off by default in Go and Rust), and the Rust
`examples/banking` (the getting-started example comes first). The Rust `PutViews` batch writer is
`—`, because it's internal to Rust's rebuild. Rows whose `TS` cell is already `—` stay that way:
the Go-only tools, the deferred Go-only runtimes and the cut features.

### 7. Release and publishing

- **release-please.** A `packages/evt` entry with `release-type: node`,
  `package-name: @photon-grove/evt`, `component: evt-ts`, `include-component-in-tag: true`,
  `initial-version: 0.1.0`, `bump-minor-pre-major: true` and
  `bump-patch-for-minor-pre-major: true`, the crates' pre-1.0 policy. Tags are `evt-ts-vX.Y.Z`,
  versioned independently of the crates and the Go module. Conventional commits scoped `ts` (`feat(ts): …`) land in its
  changelog. Before the config merges, a dry run
  (`npx release-please@17 release-pr --dry-run`) confirms that only the new package picks up
  history, and `bootstrap-sha` is set if it doesn't.
- **Publishing.** A `publish-npm` job in `release.yml` runs when the release-please output
  `packages/evt--release_created` is true. It runs in the `npm` environment, which requires
  approval and allows only `main`, with `permissions: { contents: read, id-token: write }`. It
  builds, runs the fixture runner against `dist/`, upgrades npm to at least 11.5.1 (the first
  release with trusted publishing), and runs `npm publish --access public --provenance` from
  `packages/evt`. Trusted publishing exchanges the job's OIDC token for a short-lived publish
  credential, so there's no `NPM_TOKEN` secret, and the provenance attestation links each version
  to its workflow run.
- **Bootstrap.** npm adds a trusted publisher only to a package that already exists, so before the
  first release a `0.0.0` placeholder is published by hand. The placeholder, rather than a
  hand-published `0.1.0`, keeps every real version OIDC-published with provenance, and it lets
  the trusted publisher be configured and tested before any release. The maintainer's steps are
  below.

#### npm bootstrap (maintainer)

1. On npmjs.com, open the `@photon-grove` organization's **Members** page and confirm your account
   is an **Owner**. The registry shows that the organization exists and has no packages yet, but
   only you can see who owns it. If it belongs to someone else, stop: the scope decision reopens.
2. Make sure your npm account has two-factor authentication enabled.
3. In a scratch directory outside the repository, write this `package.json` and a one-line
   `README.md`, then run `npm publish --access public` there. Scoped packages are private unless
   `--access public` is passed.

   ```json
   {
     "name": "@photon-grove/evt",
     "version": "0.0.0",
     "description": "Reserved for the evt TypeScript implementation.",
     "license": "Apache-2.0",
     "repository": {
       "type": "git",
       "url": "git+https://github.com/photon-grove/evt.git",
       "directory": "packages/evt"
     }
   }
   ```

4. On the package's **Settings** page, add a trusted publisher: GitHub Actions, organization
   `photon-grove`, repository `evt`, workflow filename `release.yml`, environment `npm`.
5. Still on **Settings**, set **Publishing access** to "Require two-factor authentication and
   disallow tokens". Trusted publishing keeps working, and no token can publish.
6. In the GitHub repository settings, create the `npm` environment with yourself as a required
   reviewer and a deployment branch rule for `main` only, as `crates-io` has.
7. After the first real release, deprecate the placeholder:
   `npm deprecate @photon-grove/evt@0.0.0 "Placeholder; use 0.1.0 or later."`

## Consequences

- Three implementations hold each other to one spec. A misreading in any one shows up as a fixture
  diff in the other two, and TS's lenient-decoding failures in the spike already showed that the
  vectors catch what prose misses.
- TS adds a third language to every storage-format change: regenerate the fixtures, then pass Go,
  Rust and TS, and both live runs with the TS passes. `AGENTS.md`, `CONTRIBUTING.md`,
  `compat/README.md` and the storage format's introduction say so from the scaffolding PR on, and
  `ci.yml` keeps its path lists in step.
- `bigint` sequences cost ergonomics: `5n` literals in tests, and `JSON.stringify` throws on them.
  That's the price of reading every valid stored item exactly. A bounded `number` would have made
  TS the one implementation that refuses the `large-numbers` vector, and every decode boundary
  would have needed a range check anyway, since `JSON.parse`, the `pg` driver and DynamoDB `N`
  strings all hand over text or rounded floats. evt's own JSON writer and the serialized event
  encode `bigint`, and EventStoreDB's Node client made the same choice for revisions.
- The core can't use Node conveniences (`Buffer`, `node:crypto`), so it carries a small base64
  codec, a UTF-8 length counter and a lossless JSON reader. In return it runs unchanged on every
  runtime that has the web-standard globals.
- The TS live-run passes roughly triple the cross-language run time on each backend. If that
  becomes a CI problem, the passes can split into a separate job.
- No consumer exists, so phases ship when ready, not on a deadline, and the matrix shows the
  truth at every step.
