package main

import (
	"context"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/mem"
)

func TestOpenThenDepositAndReplay(t *testing.T) {
	ctx := context.Background()
	store := mem.NewStore()

	// Execute loads a fresh aggregate from the event log on every call, so each
	// command needs its own instance.
	account := NewAccount("acct-1")
	if err := store.Execute(ctx, account, "acct-1", &OpenAccount{AccountID: "acct-1", InitialBalance: 100}, evt.Metadata{}); err != nil {
		t.Fatalf("open: %v", err)
	}

	account = NewAccount("acct-1")
	if err := store.Execute(ctx, account, "acct-1", &Deposit{AccountID: "acct-1", Amount: 25}, evt.Metadata{}); err != nil {
		t.Fatalf("deposit: %v", err)
	}

	if account.Balance != 125 {
		t.Fatalf("balance after deposit = %d, want 125", account.Balance)
	}

	// Replay from the event log into a fresh instance and confirm the state matches.
	reloaded := NewAccount("acct-1")
	if _, err := store.LoadEntity(ctx, reloaded, "acct-1"); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !reloaded.Opened {
		t.Fatal("reloaded account should be opened")
	}
	if reloaded.Balance != 125 {
		t.Fatalf("reloaded balance = %d, want 125", reloaded.Balance)
	}
}

func TestDepositBeforeOpenIsNotFound(t *testing.T) {
	ctx := context.Background()
	store := mem.NewStore()

	account := NewAccount("acct-2")
	err := store.Execute(ctx, account, "acct-2", &Deposit{AccountID: "acct-2", Amount: 10}, evt.Metadata{})
	if err == nil {
		t.Fatal("expected an error depositing into an unopened account")
	}
	if !evt.IsNotFoundErr(err) {
		t.Fatalf("expected NotFoundError, got %T: %v", err, err)
	}
}

func TestDoubleOpenConflicts(t *testing.T) {
	ctx := context.Background()
	store := mem.NewStore()

	account := NewAccount("acct-3")
	if err := store.Execute(ctx, account, "acct-3", &OpenAccount{AccountID: "acct-3", InitialBalance: 5}, evt.Metadata{}); err != nil {
		t.Fatalf("first open: %v", err)
	}

	account = NewAccount("acct-3")
	err := store.Execute(ctx, account, "acct-3", &OpenAccount{AccountID: "acct-3", InitialBalance: 5}, evt.Metadata{})
	if err == nil {
		t.Fatal("expected a conflict opening an already-open account")
	}
	if !evt.IsConflictErr(err) {
		t.Fatalf("expected ConflictError, got %T: %v", err, err)
	}
}

func TestUnknownCommandIsRejected(t *testing.T) {
	account := NewAccount("acct-4")
	_, err := account.Handle(context.Background(), unknownCommand{})
	if err == nil {
		t.Fatal("expected an error for an unrecognized command")
	}
}

func TestEventRoundTripsThroughDeserialize(t *testing.T) {
	account := NewAccount("acct-5")

	opened := AccountOpened{AccountID: "acct-5", Balance: 50}
	serialized, err := evt.SerializeEvents([]evt.Event{opened}, 0, "acct-5", evt.Metadata{})
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	decoded, err := account.DeserializeEvent(serialized[0])
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	got, ok := decoded.(AccountOpened)
	if !ok {
		t.Fatalf("expected AccountOpened, got %T", decoded)
	}
	if got.AccountID != "acct-5" || got.Balance != 50 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

// unknownCommand is a command the Account does not handle, used to exercise the bad-command path.
type unknownCommand struct{}

func (unknownCommand) Type() evt.CommandType { return "bank_account.unknown" }

func (unknownCommand) EntityType() evt.EntityType { return entityType }
