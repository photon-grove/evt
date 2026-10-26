# Getting Started

This guide builds a replayable aggregate, tests it with in-memory storage, and
then switches it to DynamoDB. The complete runnable example lives in
[`go/examples/banking`](https://github.com/photon-grove/evt/tree/main/go/examples/banking).

## Install

```sh
go get github.com/photon-grove/evt/go
```

The core `evt` and `mem` packages have no cloud dependencies, so unit tests stay
fast and offline. The AWS SDK is pulled in only through the `dynamo`, `stream`,
and `projectors` subpackages where you actually need it.

## 1. Define an aggregate

An aggregate implements [`evt.Aggregate`](concepts.md#aggregate). It does three jobs:
**handle** commands into new events, **apply** events to its own state, and
**deserialize** historical events back into current Go types.

```go
const entityType evt.EntityType = "bank_account"

type Account struct {
    ID      evt.EntityID
    Balance int
    Opened  bool
}

func NewAccount(id evt.EntityID) *Account {
    return &Account{ID: id}
}

func (a *Account) Type() evt.EntityType { return entityType }
func (a *Account) GetID() evt.EntityID  { return a.ID }
```

Keep **command validation and invariant checks in `Handle`**, and keep **state
mutation in `Apply`**. `Handle` never mutates the aggregate; it only decides which
facts to record. `Apply` never validates; it trusts that the fact already
happened.

```go
func (a *Account) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
    switch cmd := command.(type) {
    case *OpenAccount:
        if a.Opened {
            return evt.CommandResult{}, evt.NewConflictError("account already opened")
        }
        return evt.CommandResult{Events: []evt.Event{AccountOpened{
            AccountID: cmd.AccountID,
            Balance:   cmd.InitialBalance,
            At:        time.Now().UTC(),
        }}}, nil

    case *Deposit:
        if !a.Opened {
            return evt.CommandResult{}, evt.NewNotFoundError("account not opened")
        }
        return evt.CommandResult{Events: []evt.Event{MoneyDeposited{
            AccountID: cmd.AccountID,
            Amount:    cmd.Amount,
            At:        time.Now().UTC(),
        }}}, nil

    default:
        return evt.CommandResult{}, evt.NewBadCommandError(command)
    }
}

func (a *Account) Apply(event evt.Event) error {
    switch e := event.(type) {
    case AccountOpened:
        a.ID, a.Balance, a.Opened = e.AccountID, e.Balance, true
    case MoneyDeposited:
        a.Balance += e.Amount
    default:
        return evt.NewBadEventError(event)
    }
    return nil
}
```

`DeserializeEvent` is how stored JSON becomes a typed event again on replay. It
keys off the event `Type` string, so that string is part of your durable
contract — read [schema evolution](concepts.md#upcaster) before you ever change
it.

```go
func (a *Account) DeserializeEvent(s evt.SerializedEvent) (evt.Event, error) {
    switch s.Type {
    case "bank_account.opened":
        var e AccountOpened
        return e, json.Unmarshal(s.Payload, &e)
    case "bank_account.money_deposited":
        var e MoneyDeposited
        return e, json.Unmarshal(s.Payload, &e)
    default:
        return nil, fmt.Errorf("unrecognized event: %s", s.Type)
    }
}

// No schema migrations or in-band projectors yet.
func (a *Account) EventUpcasters() []evt.EventUpcaster { return nil }
func (a *Account) Projectors() []evt.EventProjector    { return nil }
```

Commands implement [`evt.Command`](concepts.md#command) (`Type` + `EntityType`);
events implement [`evt.Event`](concepts.md#event) (`Type`, `Version`, `EntityID`,
`EntityType`). Start every event at `Version()` of `1` — that number is what lets
you [upcast](concepts.md#upcaster) old payloads later.

## 2. Drive it with `mem` in a test

`mem.NewStore()` gives you the full [`evt.Store`](concepts.md#store) contract with
zero infrastructure, so behavior tests run in microseconds. The store loads a
**fresh aggregate from the log on every `Execute`**, so give each command its own
instance — reusing an already-applied aggregate double-applies events.

```go
func TestOpenThenDepositAndReplay(t *testing.T) {
    ctx := context.Background()
    store := mem.NewStore()

    account := NewAccount("acct-1")
    if err := store.Execute(ctx, account, "acct-1",
        &OpenAccount{AccountID: "acct-1", InitialBalance: 100}, evt.Metadata{}); err != nil {
        t.Fatalf("open: %v", err)
    }

    account = NewAccount("acct-1")
    if err := store.Execute(ctx, account, "acct-1",
        &Deposit{AccountID: "acct-1", Amount: 25}, evt.Metadata{}); err != nil {
        t.Fatalf("deposit: %v", err)
    }

    // Replay from the log into a fresh instance — state is fully reconstructed.
    reloaded := NewAccount("acct-1")
    if _, err := store.LoadAggregate(ctx, reloaded, "acct-1"); err != nil {
        t.Fatalf("reload: %v", err)
    }
    if reloaded.Balance != 125 {
        t.Fatalf("balance = %d, want 125", reloaded.Balance)
    }
}
```

The error constructors round-trip through typed predicates, so tests assert on
intent rather than strings: `evt.IsNotFoundErr(err)`, `evt.IsConflictErr(err)`,
`evt.IsDuplicateCommandErr(err)`.

## 3. Make retries safe with metadata

[`evt.Metadata`](concepts.md#metadata) rides along with every command and carries
an optional command ID, trace context, origin, caller identity (`PrincipalID`),
causation (`CausationID`), and application-specific `Extensions`. The command ID is the
practical key to **idempotent retries**: set it from a request ID, job ID, or
message ID, and a duplicate attempt fails as a
[`DuplicateCommandError`](concepts.md#command) instead of recording the fact
twice. For durable, global idempotency that also stores the response, see
[command outcomes](../README.md#command-outcomes-and-rejections-v09-c3c4).

```go
md := evt.NewMetadata(ctx, nil,
    evt.WithCommandID("idempotency-key-123"),
    evt.WithPrincipalID("user:42"),
    evt.WithExtension("requestSource", "cli"),
)
err := store.Execute(ctx, account, "acct-1", &Deposit{AccountID: "acct-1", Amount: 25}, md)
```

This example leaves `CausationID` unset, so the stored `causationId` defaults to
`"idempotency-key-123"`. For a command triggered by another command, add
`evt.WithCausationID("upstream-command-456")` to override it. With neither a
command ID nor a causation ID, `causationId` encodes as `null`. An unset principal
also encodes as `null`; nil or empty extensions encode as `{}`. Use
`evt.WithExtension(key, value)` for each application-specific string entry.

Use `evt.WithTrace(ctx)` to propagate an OpenTelemetry context and
`evt.WithAddress(addr)` when the caller has parsed a client address worth
recording.

## 4. Move the same model to DynamoDB

The aggregate does not change. Swap the store's backing repository:

```go
repo := dynamo.NewRepository(dynamoClient, "evt-local-event-log")
store := snapshots.NewStore(repo, 25) // write a snapshot every ~25 events
```

Initialize a new event-log table once, before its writers start, with `evt-doctor init` (or
`repo.Initialize(ctx)` in a deploy step). Writers refuse an uninitialized table instead of scanning
it, so their roles need no `dynamodb:Scan`. For a local emulator you can skip the step with
`.WithLazyInitialization(true)`, and the first write initializes the table itself.

`snapshots.NewStore` keeps replay fast by checkpointing aggregate state inline in the
event log, so loading a long-lived stream reads the latest snapshot plus the tail
rather than every event from sequence 1. You provision two tables — an event log
and a views table — both detailed in the [DynamoDB guide](dynamodb.md). Run them
locally against the Moto emulator with the `infra/local` Terraform stack before
touching real AWS.

Prefer a relational store? The same swap targets PostgreSQL — `postgres.NewStore`
(or `postgres.NewRepository` + `snapshots.NewStore`) over a `pgxpool.Pool`. The
aggregate still does not change. See the [PostgreSQL guide](postgres.md) for the
schema and local setup.

## 5. Project views deliberately

Views are derived state. A projection table must always be safe to wipe and
rebuild by replaying events — see [Projections and rebuilds](projections.md). The
corollary is the framework's central rule: **if a piece of state cannot be
reconstructed from the log, it has to be recorded as an event first.** Human
decisions, external signals, and publish flags belong in events, never written
straight to a view.

## Where to go next

- [Concepts](concepts.md) — the vocabulary and the contracts behind each type.
- [DynamoDB integration](dynamodb.md) — table shapes, snapshots, and compaction.
- [PostgreSQL integration](postgres.md) — the relational schema and local setup.
- [Projections and rebuilds](projections.md) — building and rebuilding read models.
- [Streams and projectors](streams.md) — async fan-out on Lambda.
