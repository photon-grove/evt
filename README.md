# evt

**Event sourcing for Go without a framework runtime.**

[Documentation](https://photon-grove.github.io/evt/) · [Concepts](docs/concepts.md) · [Getting Started](docs/getting-started.md) · [Invariants](BEHAVIORAL_INVARIANTS.md) · [Storage Format](docs/spec/storage-format.md) · [Feature Matrix](docs/parity/feature-matrix.md)

`evt` provides append-only logs, optimistic concurrency, snapshots, replayable
read models, and DynamoDB or PostgreSQL persistence as small Go packages. There
is no runtime to adopt or base application to inherit from. Use only the pieces
your application needs.

## Why evt

- **Events are the source of truth.** Domain events are immutable facts. Every
  view, projection, and read model is derived state you can delete and rebuild by
  replaying the log.
- **The same aggregate runs across backends.** Test command and event logic against
  an in-memory store, then use DynamoDB or PostgreSQL without changing the
  aggregate. A backend-neutral conformance suite checks every backend against the
  same storage contract.
- **Concurrency and ordering are protected.** Conditional writes preserve
  per-entity sequence ordering. Stable command IDs prevent retries from duplicating
  facts.
- **Long logs can be rebuilt efficiently.** Inline snapshots, versioned upcasters,
  and snapshot-verified compaction limit replay work without rewriting history.
- **DynamoDB Streams support.** Projectors consume the stream directly
  (`projectors.NewLambdaHandler`), with idempotency and retry classification included. The
  library no longer ships an SNS publisher. If you fan events out, do it from your own handler.
- **A Rust crate shares the log.** [`photon-evt`](crates/evt) reads and writes the
  same DynamoDB items, PostgreSQL rows, and stream envelopes, so Go and Rust
  services can append to the same streams. [`photon-evt-lambda`](crates/evt-lambda)
  is its Lambda side: DynamoDB Streams event decode, filter patterns, journal
  wake-ups, and partial batch failures.

## Install

```sh
go get github.com/photon-grove/evt
```

## Quick Start

An aggregate handles commands, emits events, and folds those events into state.
Here is the shape of one — see [`examples/banking`](examples/banking) for the full
runnable version:

```go
func (a *Account) Handle(_ context.Context, cmd evt.Command) (evt.CommandResult, error) {
    switch c := cmd.(type) {
    case *OpenAccount:
        if a.Opened {
            return evt.CommandResult{}, evt.NewConflictError("account already opened")
        }
        return evt.CommandResult{Events: []evt.Event{
            AccountOpened{AccountID: c.AccountID, Balance: c.InitialBalance},
        }}, nil
    // ...
    }
}

func (a *Account) Apply(event evt.Event) error {
    switch e := event.(type) {
    case AccountOpened:
        a.ID, a.Balance, a.Opened = e.AccountID, e.Balance, true
    // ...
    }
    return nil
}
```

Drive it with an in-memory store in tests:

```go
store := mem.NewStore()

// Execute replays history onto the instance you pass, so hand it a fresh one per command.
acct := NewAccount("acct-1")
store.Execute(ctx, acct, "acct-1", &OpenAccount{AccountID: "acct-1", InitialBalance: 100}, evt.Metadata{})

acct = NewAccount("acct-1")
store.Execute(ctx, acct, "acct-1", &Deposit{AccountID: "acct-1", Amount: 25}, evt.Metadata{})

fmt.Println(acct.Balance) // 125
```

Use the same aggregate with DynamoDB in production:

```go
repo := dynamo.NewRepository(dynamoClient, "event-log")
store := snapshots.NewStore(repo, 25) // snapshot every 25 events
```

Initialize the table once at deploy time with `evt-doctor init` (or `repo.Initialize(ctx)`); writers
refuse an uninitialized table rather than scan it.

When aggregates carry injected dependencies, construct them and pass the instance to `Execute`:

```go
entity := NewAccount("acct-1", projectors...)
err := store.Execute(ctx, entity, "acct-1", command, metadata)
// entity now holds the post-command state
```

## Packages

| Package | Purpose |
| --- | --- |
| `evt` | Core aggregate, command, event, metadata, transaction, repository, and rebuild contracts |
| `evt/mem` | In-memory repository and store for unit tests |
| `evt/dynamo` | DynamoDB event log, snapshots, views, stream helpers, and transaction groups |
| `evt/postgres` | PostgreSQL event log and snapshots — the same `evt.Repository` contract on a relational store |
| `evt/snapshots` | Store with snapshot loading and write thresholds |
| `evt/stream` | Stream envelope and DynamoDB Streams handler |
| `evt/projectors` | Projector runtime (SNS/SQS envelope decode, or direct stream) with idempotency and retry classification |
| `evt/policy` | Backend-neutral retry helpers for commit paths |
| `evt/viewstore` | Typed JSON helper around `evt.ViewRepository` |
| `evt/test` | Test aggregate, commands, events, and helpers for adopters and framework tests |

All packages live under `github.com/photon-grove/evt`.

## Removals

The features cut by plan decision D6 ([ADR 0006](docs/adr/0006-parity-policy-and-feature-cuts.md))
are deleted. The [parity matrix](docs/parity/feature-matrix.md) records each one (K1 to K7). In
short:

- `Repository.CommitStream` is gone. Commit batched commands through the normal commit path.
- The snapshot freshness observer, replay estimate and replay budget are gone. Wrap `Load` with your
  own metrics.
- `dynamo.StreamEntitiesByQuery`, its `HeadSource` option and `RebuildProjectionsByQuery` are gone.
  `StreamEntitiesFromSnapshots` stays. `StreamEntities` buffers every matched event, so for very
  large tables page your own DynamoDB Query or Scan with `LastEvaluatedKey`.
- `dynamo.HeadStore` and the entity-heads table are gone. `Repository.StreamEntityHeads` derives
  heads from the log and stays.
- The `evt/publishers` package and `stream.SNSPublisher` are gone. The stream envelope stays because
  it is the cross-language wire format, so read the DynamoDB Stream with `stream.DynamoDBHandler` or
  `projectors.NewLambdaHandler` and publish onward from your own handler.
- `policy.ExecuteWithRetry`, its hooks and the pluggable classifier, plus `evt/policy/dynamodb`, are
  gone. Fence-aware retry (below) replaces blind whole-command retry.
- `evt.BaseEntity`, `evt.NewEntity`, `Entity.Base`, the `WithFactory` helpers, `evt.ByCommandID`,
  `CommandResult.Transaction`, the `do.go` providers, `dynamo.Repository.Delete`,
  `dynamo.ExpressionStreamer` and the public `...ByExpression` methods are gone, and `dynamo/mock`
  is no longer importable. Entities define their own `ID` field and `GetID()`. Use `StreamFilter`
  for entity-type filtering and `snapshots.Store.WithCommitProjectors` for commit-wide projectors.
  The `result` package stays, because the repository stream channels still carry `result.Result`.

Three v0.9.0 changes are source-breaking without a deprecation window:

- `evt.Metadata` is no longer comparable with `==` — the new `Extensions` map (C6) makes the
  struct non-comparable. Use `evt.Metadata.Equal` instead; it treats nil and empty maps as equal.
- `evt.SerializedResult` gained `Snapshots` and `Fences` (C1/C2) and `Outcome` (C3), growing from two
  exported fields to five, so an unkeyed struct literal such as
  `evt.SerializedResult{events, transaction}` no longer compiles. (`evt.CommandResult` also gained
  a `Response` field; use keyed literals there too.) Use keyed fields; the zero value of the new ones keeps the single-stream shape:

  ```go
  result := evt.SerializedResult{
      Events:      events,
      Transaction: transaction,
  }
  ```

- `evt.Repository` now requires `GetRecentEvents(ctx context.Context, entityID evt.EntityID,
  limit int) ([]evt.SerializedEvent, error)` (C8). Add it to custom backends and mocks. Return
  a typed `*evt.InvalidArgumentError` for `limit < 1` before any I/O (match it with
  `errors.Is(err, evt.ErrInvalidArgument)`); return at most `limit` events newest-first by
  sequence, regardless of insertion order, excluding snapshot rows. Bound the storage query
  itself — do not call `GetEvents` and slice a full replay.

Multi-stream transactions and read fences (C1/C2) land in v0.9.0 without a new interface method:
`Repository.Commit` already accepts a `SerializedResult`, so a commit may append to several
streams and snapshots at once and the engine pins their order bytewise by stream ID. A
`SerializedResult.Fences` entry pins a read-only stream to the head it was observed at — the
commit fails with `*evt.FenceConflictError` when another writer (or a compaction) moves it —
and fences for entity types with retention are refused (the union of the repository's own
`WithRetention` configuration and the feature marker's durable retention set; declare retained
types in the marker so every process refuses them). Every commit, single
or multi-stream, is checked against the shared 100-item / 3.8 MB / 380 KB budget and fails with
`*evt.CommitTooLargeError` before any write, on every backend. The budget counts the actions a
commit actually emits. Every written stream is protected by one conditional update of its **head
item** (ADR 0007), so a single-stream commit leaves room for 97 events and projector operations
whether it creates its stream or appends to it, and 96 when it also writes a snapshot (the
feature-marker check and the rebuild-lock check are in every commit). A fence is one check of the
same head item. A commit whose first sequence skips past the stream's stored head is refused with
`*evt.SequenceGapError`, and a stream whose head item disagrees with its log fails with
`*evt.StreamHeadDriftError` (never retried). A store must declare the `stream-heads` feature before
a writer writes to it: an empty store is **initialized** once (`evt-doctor init`, or
`StoreInitializer.Initialize`), and a store written before head items is migrated first
(`*evt.StreamHeadsNotMigratedError` until then) with `evt-doctor migrate-heads`, a stop-the-world,
one-way procedure documented in the [migration runbook](docs/migrating-to-stream-heads.md).

**Behaviour change: a fresh DynamoDB table needs one initialization step.** A DynamoDB writer no
longer declares `stream-heads` on a fresh table itself, because that took a strongly consistent
`Scan` of the table on the first write, so every writer role needed `dynamodb:Scan`. It now refuses
an uninitialized table with `*evt.StoreNotInitializedError` (Rust `Error::StoreNotInitialized`),
writing nothing, and the table is initialized once at deploy time by a role that may Scan:
`evt-doctor init -events-table <table>`, or `repo.Initialize(ctx)` (Rust `initialize()`). It is
idempotent, so it can run on every deploy, concurrent deploys included: two jobs initializing the
same empty table let exactly one of them write. Tables that already declare `stream-heads` need nothing,
and writer roles can drop `dynamodb:Scan` (keep it only on roles that run rebuilds, enumeration, or
`evt-doctor`). Tests and local emulators can keep the old behaviour with
`WithLazyInitialization(true)` (Rust `with_lazy_initialization(true)`). PostgreSQL and in-memory
writers still initialize a fresh store themselves; both implement `Initialize` too, so one deploy
step can serve every backend. The exact contract is specified in the [storage
format](docs/spec/storage-format.md#writes).

### Commit-wide projectors (v0.9, C7)

A commit-wide projector (`evt.CommitProjector`) is a pure function over everything one commit wrote
— every stream's events and its state after the commit — that returns view operations: puts and
deletes of rows at **any** `pk`/`sk`, with optional `IfAbsent`/`IfPresent` conditions and mapped
errors. Register it with `snapshots.Store.WithCommitProjectors`; the rows commit in the same
transaction as the events, so a cross-entity read model is never out of step with the streams behind
it. Give the DynamoDB repository a views table (`WithViewsTable`) or the PostgreSQL repository a
view writer (`WithViewWriter`); the in-memory repository keeps the rows for tests.

```go
store := snapshots.NewStore(repo, 5).WithCommitProjectors(directory{}) // evt.CommitProjector
```

- A projector declares the entity types whose events it reads (`History()`). With none it is
  current-state and is given each stream's state, with no events and no commit ID. With types it is
  history-dependent and is given the commit ID and the events and state of those types only; the
  store durably adds them to the manifest's `historyDependent` set before the first commit that
  runs it, so `CompactBelow` refuses them.
- Every operation names the stream that owns its row (`PutView(pk, sk, owner, ...)`,
  `DeleteView(pk, sk, owner)`): a stream the commit wrote and, for a history-dependent projector,
  of a declared type. The repository stores the owner and refuses any write to the row by another
  stream, so **a row's owner must never change**; to reuse a key, rewrite the row with a tombstone
  as its owner rather than deleting it. A PostgreSQL view writer must enforce the owner in its SQL
  ([PostgreSQL](docs/postgres.md#views)).
- A repository must declare `evt.ViewWritingRepository` (its `WritesViews()` reports the capability,
  so a decorator reports its wrapped repository's) and `evt.HistoryDeclarer` for history-dependent
  projectors, or the commit is refused.
- Operations land after the feature-marker and rebuild-lock checks and before the written streams, in projector
  declaration order then emission order, and count against the shared budget (a single-stream
  commit leaves room for 97 events and view operations together, 96 with a snapshot).
- A failed condition returns `*evt.ProjectorOperationError`, which names the projector and the
  operation and wraps the operation's mapped error. It writes nothing, is not a conflict and not
  transient, and is never retried or recorded as an outcome; stream conflicts, drift, gaps, and a
  lost outcome row are reported ahead of it. A projector function's own error is
  `*evt.ProjectorError`.
- Before any write, on every backend: an empty, non-UTF-8, or NUL-bearing key is
  `*evt.InvalidViewOperationError`, a `pk` over 2,048 or an `sk` over 1,024 UTF-8 bytes is
  `*evt.ViewKeyTooLongError`, a repeated row is `*evt.DuplicateViewKeyError`, operations with no
  event behind them are `*evt.ProjectorOnlyCommitError`, and a repository with nowhere to write
  them is `*evt.ViewsNotConfiguredError` — operations are never dropped.
- Projectors run again on every fence-aware re-preparation of a command and never for a command that
  writes no event; a rejection that carries events projects them. A projector must be
  deterministic in its `evt.CommitView`, so a rebuild can reproduce its rows by regrouping the
  stored events with `evt.GroupCommits` (ordered by each stream's sequence) and `evt.ReplayViews`,
  which refuses a row two streams write. See [Commit-wide
  projectors](docs/projections.md#commit-wide-projectors) and the [storage
  format](docs/spec/storage-format.md#views).

A custom `Repository` receives these operations as `SerializedResult.Views`; one that cannot write
them in the commit's transaction **must return an error** rather than ignore them.

### Rebuild fence (v0.9, C10)

Rebuilds are fenced by the framework, not by a flag your jobs pass around. A rebuild worker takes a
durable lock, and while it holds the lock **every commit is refused** with `*evt.RebuildInProgressError`
(`errors.Is(err, evt.ErrRebuildInProgress)`): live writers can never interleave with the view rows a
rebuild is rewriting. Every repository provides the fence (`evt.RebuildFenceProvider`):

```go
fence := repo.(evt.RebuildFenceProvider).RebuildFence()

lease, err := fence.Begin(ctx, "rebuild-2026-10", 5*time.Minute) // acquire, or take over an expired lease
// ... wipe and replay, renewing the lease while it runs:
lease, err = fence.Renew(ctx, lease, 5*time.Minute)
// ... write view batches under the lease guard (at most 99 operations each):
err = fence.WriteViews(ctx, lease, batch) // *evt.RebuildLeaseLostError: stop, you were replaced
// ... and release it:
_, err = fence.End(ctx, lease)
```

- **Refusal is transient, never a conflict.** A commit that meets the lock is retried with a backoff
  by fence-aware retry and returned as `*evt.RebuildInProgressError` when the attempts run out; it
  is never exhausted into a `stale-fence` rejection, records no outcome (a rejection decided
  during a rebuild is not recorded either), and the same command ID runs again once the rebuild
  ends. `policy.Classify` reports it transient. An expired lock still blocks commits until a
  rebuild that takes it over ends: waiting out a crashed rebuild is the price of never writing
  beside one.
- **Generation.** The fence keeps a counter that is odd while a rebuild is in progress and even when
  it is quiesced. `Begin` takes it up by one (two for a takeover or after a force-release) and `End`
  by one more. `ForceRelease(ctx, owner)` is the operator's escape hatch for a wedged worker: it
  ends the worker's lease at once, so a new rebuild can begin without waiting for it to lapse, but
  leaves the generation odd and flagged, so readers stay fenced **and commits stay refused** until
  a later rebuild begins and ends — the force-released rebuild may have written only some of its
  batches, and a commit (a journaled one above all) could build on that half-written state. To
  resume writes, run the rebuild again to its end.
- **Readers fence a decision against the generation.** `evt.ReadAtRebuildEpoch(ctx, fence, read)`
  (or `ObserveRebuildEpoch` and `epoch.Verify`) reads the generation before and after the read and
  fails with `*evt.RebuildUnavailableError` (odd: a rebuild is running, or a force-release left
  derived state untrustworthy) or `*evt.RebuildEpochChangedError` (a rebuild began and ended while
  the read ran). A command handler that read a derived view returns that error as its own: the
  engine returns it at once and records no outcome, so a decision made on a half-rebuilt view is
  never persisted as an accepted or rejected outcome.
- A lease is the owner token **and** the generation `Begin` returned, its fencing token: `Renew`,
  `End`, and `WriteViews` take the whole `evt.RebuildLease`, so a worker whose lease lapsed cannot
  renew, write for, or end the lease of a later incarnation that took over under the same owner
  token (`*evt.RebuildLeaseLostError`).
- Leases are whole seconds on the repository's clock (`WithClock` in tests); the owner token is any
  string up to 512 bytes without U+0000. The rows are the reserved `SYSTEM#REBUILD` and `SYSTEM#REBUILD#GENERATION`
  items on DynamoDB, and the `{events}__evt_rebuild_lock` and `{events}__evt_rebuild_generation`
  tables on PostgreSQL (`EnsureSchema` creates them). See the [storage
  format](docs/spec/storage-format.md#rebuild-fence) for the exact transactions.

### Command outcomes and rejections (v0.9, C3/C4)

Enable durable command outcomes on a store with `WithOutcomes`. Every command then needs a command
ID and is recorded in the same transaction as its events, on a reserved `command-outcome#<id>`
row written once, in its final state. A retried command returns the original answer — including
the response bytes your handler set in `CommandResult.Response` — instead of running twice, and
the check covers IDs that were long since folded into a snapshot:

```go
store := snapshots.NewStore(repo, 5).WithOutcomes()

outcome, err := store.ExecuteWithOutcome(ctx, account, id, &Deposit{Cents: 500},
    evt.NewMetadata(ctx, nil, evt.WithCommandID("req-123")))
switch {
case errors.Is(err, evt.ErrIdempotencyKeyReuse):
    // "req-123" already belongs to a different command (a different identity digest)
case err != nil:
    // an internal failure: nothing was recorded, so the same call can be retried
case outcome.Rejected():
    _ = outcome.Rejection // {Code, Message, Details}, replayed verbatim on a retry
default:
    _ = outcome.Response // the stored response, whether just produced or replayed
    _ = outcome.Replayed // true when the stored outcome was returned and nothing ran
}
```

- **Identity.** A command's identity digest (SHA-256 over its type, the target streams the
  request names, canonical payload JSON, and principal) is computed before it runs; the same ID
  with the same digest replays, a different digest is `*evt.IdempotencyKeyReuseError` (carrying
  the stored outcome). The identity is the caller's intent: the streams a command goes on to read
  and write are not part of it, because they can depend on state and change between attempts.
  Supply your own with `snapshots.WithDigester`.
- **Rejections (C4).** A handler error that implements `evt.Rejectioner` (`NotFoundError` and
  `BadCommandError` do, with the engine codes `not-found` and `bad-command`) is persisted as a
  rejected outcome. Any other error — a bad event, an encoding or storage failure — is an internal
  failure: returned, nothing recorded, so a retry after the fix starts clean. The error-returning
  `Execute` reports a rejection as `*evt.RejectionError` (`errors.Is(err, evt.ErrRejected)`).
- **Rejections that carry events.** Some refusals still have to be recorded. A proposal whose apply
  finds its base revision stale should answer the client with a rejection *and* leave an
  `apply-failed` event on the proposal, so the aggregate is no longer "applicable" and the history
  shows the attempt. Return the events beside the error and mark it with `evt.RejectWithEvents`:
  they commit in the same atomic commit as the rejected row, which lists them.

  ```go
  func (p *Proposal) Handle(ctx context.Context, command evt.Command) (evt.CommandResult, error) {
      // ... the base revision is stale:
      return evt.CommandResult{Events: []evt.Event{ApplyFailed{Base: apply.Base}}},
          evt.RejectWithEvents(&BaseStale{Base: apply.Base}) // a Rejectioner
  }
  ```

  In a `Prepare` function, stage the events with `tx.Append` and return the marked error (or finish
  a transaction by hand with `Transaction.RejectWithEvents`); a plain rejection still discards what
  was staged. Everything an accepted commit enforces applies: continuity and fences, reserved
  streams, projectors, snapshots, and the commit budget. A replay returns the stored rejection
  without running the handler or writing the events again. A conflict re-prepares the command, and
  it may then decide differently and record no failure at all; if the attempts run out, the
  `stale-fence` rejection is recorded without your events. A rejection stores no response.
- **Accepted no-op.** A command that produces no events (or returns `NoOpError`) is recorded as
  accepted with no events — a durable, observable outcome rather than an error.
- **Ambiguous writes.** If a commit's result is unknown (a timeout, a lost connection) the store
  re-reads the outcome row instead of re-running the command: a stored outcome is returned; an
  absent row is polled for a bounded window (`snapshots.WithRecoveryWait`, 10 s, on a context detached
  from the caller's cancellation); a row that never appears is a retryable `*evt.AmbiguousWriteError`
  (`errors.Is(err, evt.ErrAmbiguousWrite)`, transient) and never "not applied", because a
  transaction can land after any single read. Retrying the same command ID is safe. A lost
  race on the outcome row is resolved the same way, never as a generic conflict, and the loser
  returns only the winner's stored outcome. A first execution returns its rejection in the stored
  (decoded) form, so its `Details` equal a replay's. A command ID, principal, and command type must
  be valid UTF-8, and a custom digest must be non-empty: both are refused before anything is read. U+0000 is refused
  alike on every backend in a command ID, a digest, and a rejection's code, message, and details.
  Numbers in a rejection's details are stored in the canonical text PostgreSQL's `jsonb` prints
  (no exponent, no sign on zero), so they read back the same on every backend; one PostgreSQL's
  numeric type cannot hold is refused as an invalid argument.
  `Store.ResolveCommand` runs the same read for a caller that lost the result entirely.
- **Multi-stream commands** use `Store.ExecuteTransaction`, which runs your decision step
  (`snapshots.Prepare`) under fence-aware retry (below), or `Store.BeginCommand`, which reads the
  outcome row first, then the usual `Read`/`Append`/`Fence`, then `Transaction.Complete`
  (accepted, or a no-op when nothing was appended) or `Transaction.Reject`, attempting once. A
  failed caller-stamped fence is persisted as a `stale-fence` rejection; the remedy is a new
  command ID.
- **Fence-aware retry (C5).** `ExecuteWithOutcome`, `Execute` on an outcomes store, and
  `ExecuteTransaction` retry a command that conflicts with another writer, up to
  `snapshots.RetryPolicy` (`Store.WithRetry`; the default is 5 attempts, 25 ms doubling to 1 s,
  jittered; unset delays take those defaults, `snapshots.ImmediateRetryPolicy` opts out of waiting
  for tests, and `Deadline` bounds the whole command; with the defaults the waits total about
  0.4 s, but an ambiguous write that never resolves costs a recovery window per attempt). A conflict on anything the engine can re-derive — an event put, a snapshot or
  continuity check, an engine-filled fence — **re-prepares** the command: the entity is reset and
  loaded again, the handler (or your `Prepare`) runs again against fresh state, and only the
  attempt that commits writes anything. A failed **caller-stamped** fence fails fast with the
  `stale-fence` rejection instead, because the engine cannot know your decision still holds. When
  the last attempt also conflicts, the engine persists a `stale-fence` rejection
  (`evt.RetriesExhaustedRejection`) as the command's outcome, so a replay of the ID gets the same
  answer; submit a new command ID to try again. Handlers and `Prepare` functions may therefore
  run more than once and must be safe to repeat. A transient failure that is not a conflict
  (throttling, an ambiguous write that never resolved) is retried the same way but returned as an
  error when attempts run out.

  A command whose write set depends on state — a rename that releases whichever handle the
  entity currently holds — keeps one identity across attempts, because only the targets in its
  `evt.CommandIdentity` are digested. The flip side is a contract: **everything that distinguishes
  the request must be in `Targets` or the payload.** A transfer whose destination is chosen only
  inside `Prepare` has the same identity whichever destination it picks, so a second request with
  the same command ID and a different destination replays the first's outcome instead of running;
  put the destination in the payload (or the targets). The handler resets the entity between
  attempts from a deep copy of its exported fields, so keep the state `Apply` changes in exported
  fields, and do not mutate the command in a handler.

  ```go
  identity := evt.CommandIdentity{
      Type: "profile:rename", Targets: []evt.EntityID{profileID}, Payload: rename,
  }

  outcome, err := store.ExecuteTransaction(ctx, identity, metadata,
      func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
          profile := NewProfile(profileID)
          if err := tx.Read(ctx, profile, profileID); err != nil {
              return nil, err
          }

          // The handle this profile holds now decides which claim stream is released.
          released, taken := ClaimID(profile.Handle), ClaimID(rename.Handle)
          // ... Read both claims, check the new handle is free (return a Rejectioner if not),
          // then Append to the profile and to both claims.
          return nil, nil
      })
  ```
- Bounds checked before any write: a command ID is at most 512 UTF-8 bytes
  (`*evt.CommandIDTooLongError`), and the response and the encoded rejection (its numbers in
  canonical text) are each at most 64 KiB (`*evt.OutcomeTooLargeError`). The outcome row counts in the commit budget.
- A repository opts in by implementing `evt.OutcomeRepository`; the in-memory, DynamoDB, and
  PostgreSQL repositories do (PostgreSQL keeps the rows in `{events}__evt_command_outcomes`, created
  by `EnsureSchema`). On an upgraded log, command IDs recorded before outcomes were enabled keep
  only the old window check; see the [storage format](docs/spec/storage-format.md#command-outcomes).
  The bare `Store.Commit` does not record outcomes and refuses them.

A custom `Repository` written before v0.9 compiles unchanged but now receives
`SerializedResult.Snapshots` and `.Fences`. An implementation that cannot honor them atomically
**must return an error** rather than ignore them: ignoring a fence commits a decision made on a
stale read with no isolation guarantee, and ignoring a snapshot loses one the engine believes is
durable. See the contract on `evt.Repository.Commit`; `evt.NormalizeCommit` and
`evt.PreflightCommit` give a backend the shared validation and budget checks.

Atomic multi-stream support is also an explicit capability. A repository declares it by
implementing `evt.TransactionalRepository` (a marker method, `AtomicTransactions()`), and does so
only once its `Commit` honors the full contract above. `snapshots.Store.Transaction` fails closed:
`Read` and `Commit` refuse a repository without the capability with
`*evt.TransactionsUnsupportedError` (`errors.Is(err, evt.ErrTransactionsUnsupported)`) before
anything is loaded or written. The in-memory, DynamoDB, and PostgreSQL repositories declare it.
The Go PostgreSQL repository executes commit-wide view operations through a view writer
(`WithViewWriter`; see [PostgreSQL](docs/postgres.md#views)), and refuses the operations of a
transaction group built for another storage type with `*evt.ProjectorOperationsUnsupportedError`
before writing, instead of dropping the view writes.
To use transactions on a custom backend, make `Commit` atomic over every stream, snapshot, and
fence, then add the marker method. Wrapping a repository in a decorator type hides the capability
unless the wrapper declares it too.

For example, a SQL backend with a separate events table can implement the method below.
Here `queryEvents` is the backend's existing query-and-decode helper; it executes the supplied
SQL and decodes event rows, not a full-stream replay:

```go
func (r *Repository) GetRecentEvents(
    ctx context.Context, entityID evt.EntityID, limit int,
) ([]evt.SerializedEvent, error) {
    if limit < 1 {
        return nil, &evt.InvalidArgumentError{Argument: "limit", Message: "must be at least 1"}
    }

    return r.queryEvents(ctx,
        `SELECT * FROM events WHERE entity_id = $1 AND sequence > 0
         ORDER BY sequence DESC LIMIT $2`, string(entityID), int64(limit))
}
```

Use your backend's event-column list and decoder in place of `SELECT *`; snapshots must remain
outside this result. Mocks must implement the same signature and bounded-read contract.

## Rust

The [`photon-evt`](crates/evt) crate is a native Rust implementation of the same
event log: aggregates, a snapshotting store, upcasters, compaction, in-band
projectors, and DynamoDB and PostgreSQL repositories. It is not a binding to the Go
code. The [storage format](docs/spec/storage-format.md) is the contract between the
two. Golden fixtures and a live run where Go and Rust take turns writing the same
streams enforce it (see [`compat/`](compat) and
[ADR 0003](docs/adr/0003-rust-implementation-and-cross-language-compatibility.md)).

## Core Discipline

**Event rows are the source of truth. View rows are derived state.** Any projection
or view table must be safe to wipe and rebuild from the immutable log. Record human
decisions, external signals, publish flags, and review verdicts as events before a
projector writes them to a view.

Event logs are append-only and retained in full by default. Two opt-in mechanisms
trim them safely:

- **Compaction** (`evt.Compactor.CompactBelow`) deletes only events a durable
  snapshot already covers, then rebuilds seed from that snapshot rather than
  replaying from event 1. See
  [ADR 0001](docs/adr/0001-event-compaction-and-snapshot-truncation.md).
- **Per-type retention** (`dynamo.Repository.WithRetention`) sets DynamoDB `ttl`
  to `committedAt + duration` for terminal, short-lived, transient streams. TTL
  expires rows individually, so use it only when a stream's lifetime is much
  shorter than the duration and no events are appended after it becomes terminal.
  Otherwise an older prefix can expire while newer events remain, and loading the
  stream then fails with a conflict. For accumulating streams, keep a snapshot and
  compact instead.

All durable Go repositories enforce the [feature marker](docs/spec/storage-format.md#feature-marker)
by default, with no supported extensions. Unsupported declarations refuse commits and standalone
snapshot writes atomically. Compaction checks declarations across handles: PostgreSQL holds one
transaction lock; DynamoDB fences each delete batch, with no whole-operation lease or rollback of
already committed batches. A racing history-dependent declaration can therefore leave a partially
compacted stream; its history-dependent rebuild reports `CompactionUncovered`. Declare history
dependence before writing data of that entity type.
`repo.WithFeatureMarker(repo.FeatureMarker(supported))` customizes the supported set, not whether
enforcement runs. Create markers from the repository so reads and transactional checks share its
client and events table; attaching a marker from another client or table is a typed configuration
error.

See
[`BEHAVIORAL_INVARIANTS.md`](BEHAVIORAL_INVARIANTS.md) for the exact serialization
and DynamoDB schema guarantees.

## Local Development

```sh
pnpm install
go test ./...
moon run evt:test
moon run evt:lint
```

The Rust toolchain is pinned in `rust-toolchain.toml`, and `rustup` installs it on first use:

```sh
cargo test --workspace
moon run evt-rust:check evt-rust:test
```

DynamoDB integration tests run against a local AWS emulator (Moto), not real AWS:

```sh
docker run -d --name evt-moto -p 4566:5000 motoserver/moto:5.1.22
terraform -chdir=infra/local init
terraform -chdir=infra/local apply -auto-approve
AWS_ENDPOINT_URL=http://localhost:4566 moon run evt:integration
```

PostgreSQL integration tests run against a local PostgreSQL server. Terraform
provisions the database; `postgres.Repository.EnsureSchema` owns the tables:

```sh
docker run -d --name evt-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:17
terraform -chdir=infra/local-postgres init
terraform -chdir=infra/local-postgres apply -auto-approve
moon run evt:integration-postgres
```

With both stacks running, `moon run evt-rust:integration evt-rust:integration-postgres`
runs the Rust conformance suite and the Go/Rust cross-language suite against each
backend.

## Documentation

The [docs site](https://photon-grove.github.io/evt/) covers architecture, the
integration cookbook, the [stream-heads migration runbook](docs/migrating-to-stream-heads.md),
and runnable examples. Static docs live in [`docs/`](docs/);
the interactive site lives in [`website/`](website/).

Architecture decision records live in [`docs/adr/`](docs/adr/): event-log compaction
([ADR 0001](docs/adr/0001-event-compaction-and-snapshot-truncation.md)), backend-neutral storage
contracts ([ADR 0002](docs/adr/0002-backend-neutral-storage-contracts.md)), the Rust implementation
and cross-language compatibility ([ADR 0003](docs/adr/0003-rust-implementation-and-cross-language-compatibility.md)),
the vNext core reshape — multi-stream transactions, command outcomes, and the rebuild fence
([ADR 0004](docs/adr/0004-core-reshape-multi-stream-transactions-and-outcomes.md)), Rust-first
extensions ([ADR 0005](docs/adr/0005-rust-first-extensions-journal-and-lambda.md)), and the parity
policy ([ADR 0006](docs/adr/0006-parity-policy-and-feature-cuts.md)). The
[feature parity matrix](docs/parity/feature-matrix.md) tracks every capability across languages.

```sh
moon run website:dev
moon run website:build
```

## License

Apache-2.0. See [`LICENSE`](LICENSE).

---

Built by [Photon Grove](https://photon-grove.com), a Colorado software studio.
