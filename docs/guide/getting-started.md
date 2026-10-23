# Getting Started

This guide builds a replayable aggregate, tests it against in-memory storage, and then moves it to
DynamoDB without changing the aggregate. The complete runnable example lives in
[`go/examples/banking`](https://github.com/photon-grove/evt/tree/main/go/examples/banking).

## Install

```sh
go get github.com/photon-grove/evt/go
```

The core `evt` and `mem` packages have no cloud dependencies, so unit tests stay fast and offline.
The AWS SDK comes in only through the `dynamo`, `stream` and `projectors` subpackages, where you
actually need it.

## 1. Define an aggregate

An aggregate implements [`evt.Aggregate`](concepts.md#aggregate). It does three jobs. It handles
commands into new events, applies events to its own state, and deserializes historical events back
into current Go types.

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

Keep command validation and invariant checks in `Handle`, and keep state mutation in `Apply`.
`Handle` never mutates the aggregate. It only decides which facts to record. `Apply` never
validates, because it trusts that the fact already happened.

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

`DeserializeEvent` is how stored JSON becomes a typed event again on replay. It keys off the event's
`Type` string, so that string is part of your durable contract. Read about
[schema evolution](concepts.md#upcaster) before you ever change it.

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

Commands implement [`evt.Command`](concepts.md#command) (`Type` and `EntityType`), and events
implement [`evt.Event`](concepts.md#event) (`Type`, `Version`, `EntityID` and `EntityType`). Start
every event at a `Version()` of `1`. That number is what lets you [upcast](concepts.md#upcaster) old
payloads later.

## 2. Drive it with `mem` in a test

`mem.NewStore()` gives you the full [`evt.Store`](concepts.md#store) contract with no
infrastructure, so behavior tests stay fast. The store loads a fresh aggregate from the log on every
`Execute`, so give each command its own instance. Reusing an aggregate that already applied events
would apply them twice.

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

    // Replay from the log into a fresh instance to rebuild the state.
    reloaded := NewAccount("acct-1")
    if _, err := store.LoadAggregate(ctx, reloaded, "acct-1"); err != nil {
        t.Fatalf("reload: %v", err)
    }
    if reloaded.Balance != 125 {
        t.Fatalf("balance = %d, want 125", reloaded.Balance)
    }
}
```

The error constructors round-trip through typed predicates, so tests assert on intent instead of
strings: `evt.IsNotFoundErr(err)`, `evt.IsConflictErr(err)` and `evt.IsDuplicateCommandErr(err)`.

## 3. Make retries safe with metadata

[`evt.Metadata`](concepts.md#metadata) rides along with every command. It carries an optional
command ID, trace context, origin, caller identity (`PrincipalID`), causation (`CausationID`) and
application-specific `Extensions`. The command ID is what makes retries idempotent. Set it from a
request ID, job ID or message ID, and a duplicate attempt fails as a
[`DuplicateCommandError`](concepts.md#command) instead of recording the fact twice. For durable
idempotency that also stores the response, see [command outcomes](commands.md).

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
`evt.WithCausationID("upstream-command-456")` to override it. With neither a command ID nor a
causation ID, `causationId` encodes as `null`. An unset principal also encodes as `null`, and nil or
empty extensions encode as `{}`. Call `evt.WithExtension(key, value)` once for each
application-specific string entry.

Use `evt.WithTrace(ctx)` to carry an OpenTelemetry context along, and `evt.WithAddress(addr)` when
the caller has parsed a client address worth recording.

## 4. Move the same model to DynamoDB

The aggregate does not change. Swap the store's backing repository:

```go
repo := dynamo.NewRepository(dynamoClient, "evt-local-event-log")
store := snapshots.NewStore(repo, 25) // write a snapshot every ~25 events
```

`snapshots.NewStore` keeps replay fast by checkpointing aggregate state inline in the event log. So
loading a long-lived stream reads the latest snapshot plus the tail, not every event from sequence
1. You provision two tables, an event log and a views table, and the [DynamoDB guide](dynamodb.md)
describes both. Run them locally against the Moto emulator with the `infra/local` Terraform stack
before you touch real AWS.

The same swap works for PostgreSQL: `postgres.NewStore` (or `postgres.NewRepository` with
`snapshots.NewStore`) over a `pgxpool.Pool`. The aggregate still doesn't change. The
[PostgreSQL guide](postgres.md) has the schema and local setup.

## 5. Project views deliberately

Views are derived state. A projection table must always be safe to wipe and rebuild by replaying
events (see [Projections and rebuilds](projections.md)). That leads to the framework's central
rule. If a piece of state can't be rebuilt from the log, record it as an event first. Human
decisions, external signals and publish flags belong in events, and never get written straight to a
view.

## Where to go next

[Concepts](concepts.md) covers the vocabulary and the contract behind each type. The
[DynamoDB](dynamodb.md) and [PostgreSQL](postgres.md) guides cover table shapes, snapshots,
compaction and local setup. [Projections and rebuilds](projections.md) is about building read
models and rebuilding them, [Command outcomes and retries](commands.md) is about idempotency, and
[Streams and projectors](streams.md) is about async fan-out on Lambda.
