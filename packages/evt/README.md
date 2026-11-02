# @photon-grove/evt

Event sourcing for TypeScript, wire-compatible with evt's Go module and its Rust crate. Event
sourcing means the log of events is the record of truth, and an aggregate's state is whatever you
get by folding its events in order. This package isn't a binding to the Go or Rust code. It's a
third native implementation of the same
[storage format](https://github.com/photon-grove/evt/blob/main/docs/reference/storage-format.md),
held to the fixtures Go and Rust generate, byte for byte.

```sh
npm install @photon-grove/evt
```

So far it has the core, the in-memory backend and the testing helpers. The DynamoDB and
PostgreSQL backends, command outcomes and multi-stream transactions come in later releases, and the
[feature matrix](https://github.com/photon-grove/evt/blob/main/docs/reference/feature-matrix.md)
tracks each one. The
[getting started guide](https://github.com/photon-grove/evt/blob/main/docs/guide/getting-started.md#the-same-model-in-typescript)
builds a bank account aggregate with it, end to end.

## Entry points

| Import                       | Contents                                                                    |
| ---------------------------- | --------------------------------------------------------------------------- |
| `@photon-grove/evt`          | Aggregates, codecs, upcasters, the `Store`, retry, and the wire codecs      |
| `@photon-grove/evt/memory`   | `MemRepository`, the in-memory backend for tests and local development      |
| `@photon-grove/evt/testing`  | `scenario` and `Fixture`, the given/when/then helpers over `MemRepository`  |
| `@photon-grove/evt/dynamodb` | The DynamoDB item codec, with no SDK. The repository comes with the backend |

## Runtimes

The package is ESM only and has no runtime dependencies. CI runs it on Node 24, Node 22.12, Bun and
Deno. Its code uses only web-standard globals (`TextEncoder`, `TextDecoder`, Web Crypto,
`AbortSignal`, timers), never Node's built-ins, so it should run on edge runtimes and in browsers
too, though CI doesn't run them yet. It never uses top-level await, so CommonJS code on Node 22.12
and later can `require()` it. Its type declarations need TypeScript 5.7 or later and a declaration
of `AbortSignal`, which the DOM library, `@types/node`, and Bun's and Deno's types all provide.

## The model

- **Events.** `defineEvents` gives each event type a version, a codec and, optionally, upcasters.
  The type string, the version and the payload bytes are the durable contract. `EventOf<typeof
  events>` is the union of `{type, data}` objects the aggregate works with.
- **Codecs.** `json()` writes plain objects with evt's own JSON writer, in property order, and
  decodes them back. It refuses an integer past `Number.MAX_SAFE_INTEGER` rather than round it,
  when encoding as well as decoding. It takes an optional synchronous
  [Standard Schema](https://standardschema.dev) validator (Zod, Valibot, ArkType), checked on both
  sides. Any `{encode, decode}` pair of bytes works as a codec.
- **Aggregates.** `defineAggregate` names the type, the events, the state's codec and its initial
  value. `handle(state, command)` returns a decision, `{kind: 'accept', events}` or
  `{kind: 'reject', rejection: {code, message}}`, and may be async. `apply(state, event)` returns
  the next state and never validates.
- **The store.** `new Store(repository, options)` loads an aggregate from its latest snapshot and
  the events after it, runs a command, and commits the decided events with optimistic concurrency:
  the commit fails with a `ConflictError` if another writer moved the stream first.
  `executeWithRetry` loads and runs the command again after a conflict, with a jittered backoff
  (`DEFAULT_RETRY_POLICY`, the same as Go's and Rust's). A rejection commits nothing.
- **Snapshots.** The store writes a snapshot every five events by default (the `snapshotInterval`
  option, or the aggregate's own). A snapshot too large for one DynamoDB item is skipped rather
  than failing the commit, and later loads replay from the one before.
- **Duplicate commands.** A command whose metadata carries a command ID already seen since the
  latest snapshot doesn't run, and `execute` resolves to `{kind: 'duplicate'}`. The window ends at
  the snapshot, so once a snapshot covers the first run, a retry runs the command again.
- **Upcasting.** An event type's upcasters migrate a stored payload from older versions to the
  current one as it's read, in the order they're declared. `jsonUpcaster(from, to, migrate)` does
  it on the parsed JSON, losslessly. A stored version the upcasters don't bring to the declared one
  fails the load with a `ReplayError` rather than decoding into the wrong shape.
- **Sequences** are `bigint`s, because Go and Rust store them as 64-bit integers.

Errors are `EvtError`s with a `code`. `isConflict` and `isRetryable` tell them apart.

## Testing

`scenario` runs one command through a real `Store` over `MemRepository` and checks what it did:

```ts
await scenario(account)
  .given({type: 'bank_account.opened', data: {accountId: 'acct-1', balance: 100}})
  .when({type: 'deposit', amount: 25})
  .thenEvents({type: 'bank_account.money_deposited', data: {accountId: 'acct-1', amount: 25}})
```

The other checks are `thenNoEvents`, `thenRejected` (with a code or a predicate), `thenDuplicate`,
`thenError` and `thenState`. `run()` resolves to the outcome itself instead. `.entity()`,
`.metadata()` and `.options()` change the stream, the command's metadata and the store's options.
Each returns a new scenario, so one can be the base of several tests. `.on(fixture)` runs on a
shared `Fixture`, where every run seeds its given events and runs its command again.

`Fixture` seeds streams of any aggregate (`given`), reads them back decoded (`events`,
`assertEvents`) and loads them (`load`). After a snapshot and a compaction, seeding continues from
the snapshot. A failed check throws an `AssertionError` with `actual` and `expected`, so it works in
any test runner, and Vitest and Jest show a diff.

## Sharing streams with Go and Rust

A TypeScript writer and a Go or Rust writer can share a stream once the TypeScript backends ship, as
long as they agree on these:

- The event type strings, versions and payload JSON, and the aggregate state's JSON, because each
  language reads the others' snapshots. `json()` writes an object's properties in order, so declare
  them in the order of the Go struct's fields.
- Maps whose keys Go writes sorted. A Go map is a `Map` here, and it needs a codec of its own that
  decodes it back as a `Map`, since `json()` decodes plain objects.
- Integral payload numbers (cents, for example), because the languages format some floats
  differently.
- Event versions. Like Rust, TypeScript refuses a stored version it can't upcast to the declared
  one, and Go decodes it anyway, so deploy readers before writers when you bump a version.

Until the TypeScript package has commit-wide projectors and command outcomes, two more rules apply:

- Don't write an entity type that has commit-wide projectors registered in a Go or Rust service.
  Its commits would land without their view rows.
- Don't share a store whose Go or Rust writers record command outcomes. A command retried through
  TypeScript would find no outcome and could run twice.

The [known differences](https://github.com/photon-grove/evt/blob/main/docs/reference/storage-format.md#known-differences)
between the languages are in the storage format, and
[ADR 0009](https://github.com/photon-grove/evt/blob/main/docs/adr/0009-typescript-implementation.md)
explains how this package is built and tested.

## License

Apache-2.0.
