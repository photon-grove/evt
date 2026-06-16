# Getting Started

This guide takes you from `go get` to a replayable aggregate with passing tests,
then shows the one-line switch from in-memory storage to DynamoDB. The complete,
runnable version of everything below lives in
[`examples/banking`](https://github.com/photon-grove/evt/tree/main/examples/banking).

## Install

```sh
go get github.com/photon-grove/evt
```

The core `evt` and `mem` packages have no cloud dependencies, so unit tests stay
fast and offline. The AWS SDK is pulled in only through the `dynamo`, `stream`,
`projectors`, and `publishers` subpackages, where you actually need it.

## 1. Define an aggregate

An aggregate implements [`evt.Entity`](concepts.md#entity). It does three jobs:
**handle** commands into new events, **apply** events to its own state, and
**deserialize** historical events back into current Go types.

```go
const entityType evt.EntityType = "bank_account"

type Account struct {
    evt.BaseEntity
    Balance int
    Opened  bool
}

func NewAccount(id evt.EntityID) *Account {
    return &Account{BaseEntity: evt.NewEntity(id)}
}

func (a *Account) Type() evt.EntityType { return entityType }
func (a *Account) GetID() evt.EntityID  { return a.ID }
func (a *Account) Base() evt.BaseEntity { return a.BaseEntity }
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
instance — reusing an already-applied entity double-applies events.

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
    if _, err := store.LoadEntity(ctx, reloaded, "acct-1"); err != nil {
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
an optional command ID, trace context, and origin. The command ID is the
practical key to **idempotent retries**: set it from a request ID, job ID, or
message ID, and a duplicate attempt fails as a
[`DuplicateCommandError`](concepts.md#command) instead of recording the fact
twice.

```go
md := evt.NewMetadata(ctx, nil, evt.WithCommandID("idempotency-key-123"))
err := store.Execute(ctx, account, "acct-1", &Deposit{AccountID: "acct-1", Amount: 25}, md)
```

Use `evt.WithTrace(ctx)` to propagate an OpenTelemetry context and
`evt.WithAddress(addr)` when the caller has parsed a client address worth
recording.

## 4. Move the same model to DynamoDB

Nothing about the aggregate changes. You swap the store's backing repository:

```go
repo := dynamo.NewRepository(dynamoClient, "evt-local-event-log")
store := snapshots.NewStore(repo, 25) // write a snapshot every ~25 events
```

`snapshots.NewStore` keeps replay fast by checkpointing entity state inline in the
event log, so loading a long-lived stream reads the latest snapshot plus the tail
rather than every event from sequence 1. You provision two tables — an event log
and a views table — both detailed in the [DynamoDB guide](dynamodb.md). Run them
locally against the Moto emulator with the `infra/local` Terraform stack before
touching real AWS.

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
- [Projections and rebuilds](projections.md) — building and rebuilding read models.
- [Streams, projectors, and publishers](streams.md) — async fanout on Lambda.
