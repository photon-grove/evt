// Command transfer is a runnable example of a multi-stream command with a durable outcome.
//
// A transfer debits one account and credits another in one atomic commit across both streams,
// through snapshots.Store.ExecuteTransaction on a store with command outcomes enabled. Every
// command carries a stable command ID, and the store records its outcome (accepted with a
// response, or rejected) in the same commit as its events. So a client that retries with the same
// ID gets the stored answer back and nothing runs twice; the same ID sent with a different request
// is refused as idempotency-key reuse; and a domain rejection such as insufficient funds is stored
// and replayed like an acceptance.
//
// Run it with `go -C go run ./examples/transfer`.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/mem"
	"github.com/photon-grove/evt/go/snapshots"
)

const entityType evt.EntityType = "account"

// transferType is the command type of a transfer, part of its identity digest.
const transferType evt.CommandType = "account.transfer"

// ---- Aggregate ----

// Account is the aggregate. A store with outcomes resets it from a deep copy of its exported
// fields between attempts, so every field Apply changes is exported.
type Account struct {
	ID      evt.EntityID `json:"id"`
	Balance int64        `json:"balance"`
	Opened  bool         `json:"opened"`
}

// NewAccount returns an empty account; each command or read needs a fresh one.
func NewAccount(id evt.EntityID) *Account {
	return &Account{ID: id}
}

// Type returns the account's entity type.
func (a *Account) Type() evt.EntityType { return entityType }

// GetID returns the account's ID.
func (a *Account) GetID() evt.EntityID { return a.ID }

// Handle decides the events of the single-stream commands. A transfer is decided by Transfer.
func (a *Account) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
	switch cmd := command.(type) {
	case *OpenAccount:
		if a.Opened {
			return evt.CommandResult{}, evt.NewConflictError("account already opened")
		}

		return evt.CommandResult{Events: []evt.Event{AccountOpened{AccountID: a.ID, Balance: cmd.InitialBalance}}}, nil

	default:
		return evt.CommandResult{}, evt.NewBadCommandError(command)
	}
}

// Apply folds a recorded event into state.
func (a *Account) Apply(event evt.Event) error {
	switch e := event.(type) {
	case AccountOpened:
		a.ID = e.AccountID
		a.Balance = e.Balance
		a.Opened = true

	case MoneyDebited:
		a.Balance -= e.Amount

	case MoneyCredited:
		a.Balance += e.Amount

	default:
		return evt.NewBadEventError(event)
	}

	return nil
}

// DeserializeEvent decodes a stored event of this account.
func (a *Account) DeserializeEvent(serialized evt.SerializedEvent) (evt.Event, error) {
	switch serialized.Type {
	case AccountOpened{}.Type():
		return decode[AccountOpened](serialized.Payload)

	case MoneyDebited{}.Type():
		return decode[MoneyDebited](serialized.Payload)

	case MoneyCredited{}.Type():
		return decode[MoneyCredited](serialized.Payload)

	default:
		return nil, fmt.Errorf("unrecognized event: %s", serialized.Type)
	}
}

func decode[E evt.Event](payload []byte) (evt.Event, error) {
	var event E
	err := json.Unmarshal(payload, &event)

	return event, err
}

// EventUpcasters returns none: every stored event is at its current version.
func (a *Account) EventUpcasters() []evt.EventUpcaster { return nil }

// Projectors returns none.
func (a *Account) Projectors() []evt.EventProjector { return nil }

// ---- Commands and events ----

// OpenAccount opens an account with an initial balance.
type OpenAccount struct {
	InitialBalance int64
}

// Type returns the command type.
func (c *OpenAccount) Type() evt.CommandType { return "account.open" }

// EntityType returns the account's entity type.
func (c *OpenAccount) EntityType() evt.EntityType { return entityType }

// AccountOpened records an opened account.
type AccountOpened struct {
	AccountID evt.EntityID `json:"accountId"`
	Balance   int64        `json:"balance"`
}

// Type returns the stored event type.
func (e AccountOpened) Type() evt.EventType { return "account.opened" }

// Version returns the payload version.
func (e AccountOpened) Version() evt.EventVersion { return 1 }

// EntityID returns the account's ID.
func (e AccountOpened) EntityID() evt.EntityID { return e.AccountID }

// EntityType returns the account's entity type.
func (e AccountOpened) EntityType() evt.EntityType { return entityType }

// MoneyDebited records the sending side of a transfer.
type MoneyDebited struct {
	AccountID evt.EntityID `json:"accountId"`
	To        evt.EntityID `json:"to"`
	Amount    int64        `json:"amount"`
}

// Type returns the stored event type.
func (e MoneyDebited) Type() evt.EventType { return "account.money_debited" }

// Version returns the payload version.
func (e MoneyDebited) Version() evt.EventVersion { return 1 }

// EntityID returns the account's ID.
func (e MoneyDebited) EntityID() evt.EntityID { return e.AccountID }

// EntityType returns the account's entity type.
func (e MoneyDebited) EntityType() evt.EntityType { return entityType }

// MoneyCredited records the receiving side of a transfer.
type MoneyCredited struct {
	AccountID evt.EntityID `json:"accountId"`
	From      evt.EntityID `json:"from"`
	Amount    int64        `json:"amount"`
}

// Type returns the stored event type.
func (e MoneyCredited) Type() evt.EventType { return "account.money_credited" }

// Version returns the payload version.
func (e MoneyCredited) Version() evt.EventVersion { return 1 }

// EntityID returns the account's ID.
func (e MoneyCredited) EntityID() evt.EntityID { return e.AccountID }

// EntityType returns the account's entity type.
func (e MoneyCredited) EntityType() evt.EntityType { return entityType }

// ---- Transfer ----

// TransferRequest is a transfer as the client sends it. All of it goes into the command's
// identity digest, so the same command ID with a different request is idempotency-key reuse.
type TransferRequest struct {
	From   evt.EntityID `json:"from"`
	To     evt.EntityID `json:"to"`
	Amount int64        `json:"amount"`
}

// TransferResponse is the response stored with an accepted transfer and replayed on a retry.
type TransferResponse struct {
	FromBalance int64 `json:"fromBalance"`
	ToBalance   int64 `json:"toBalance"`
}

// InsufficientFundsCode is the rejection code of a transfer the balance does not cover.
const InsufficientFundsCode evt.RejectionCode = "insufficient-funds"

// InsufficientFundsError is a domain refusal. It implements evt.Rejectioner, so the store records
// it as a rejected outcome rather than returning it as an internal failure.
type InsufficientFundsError struct {
	Balance   int64
	Requested int64
}

func (e *InsufficientFundsError) Error() string {
	return fmt.Sprintf("insufficient funds: balance %d, requested %d", e.Balance, e.Requested)
}

// Rejection maps the error onto the stored rejection.
func (e *InsufficientFundsError) Rejection() evt.Rejection {
	return evt.Rejection{
		Code:    InsufficientFundsCode,
		Message: e.Error(),
		Details: map[string]any{"balance": e.Balance, "requested": e.Requested},
	}
}

// InvalidTransferCode is the rejection code of a malformed transfer.
const InvalidTransferCode evt.RejectionCode = "invalid-transfer"

// InvalidTransferError refuses a transfer of a non-positive amount or to the same account.
type InvalidTransferError struct {
	Reason string
}

func (e *InvalidTransferError) Error() string { return "invalid transfer: " + e.Reason }

// Rejection maps the error onto the stored rejection.
func (e *InvalidTransferError) Rejection() evt.Rejection {
	return evt.Rejection{Code: InvalidTransferCode, Message: e.Error()}
}

// Transfer moves money between two accounts in one commit across both streams. The command ID
// makes it idempotent: a retry with the same ID and request returns the stored outcome.
func Transfer(
	ctx context.Context,
	store *snapshots.Store,
	commandID evt.CommandID,
	request TransferRequest,
) (*evt.CommandOutcome, error) {
	identity := evt.CommandIdentity{
		Type:    transferType,
		Targets: []evt.EntityID{request.From, request.To},
		Payload: request,
	}

	metadata := evt.NewMetadata(ctx, nil, evt.WithCommandID(commandID))

	// The decision step can run more than once (a conflict re-prepares it against fresh state), so
	// it decides only from what the transaction reads.
	return store.ExecuteTransaction(ctx, identity, metadata, func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
		if request.Amount <= 0 || request.From == request.To {
			return nil, &InvalidTransferError{Reason: "a transfer moves a positive amount between two accounts"}
		}

		from, to := NewAccount(request.From), NewAccount(request.To)

		if err := tx.Read(ctx, from, request.From); err != nil {
			return nil, err
		}

		if err := tx.Read(ctx, to, request.To); err != nil {
			return nil, err
		}

		if err := checkFunds(from, to, request.Amount); err != nil {
			return nil, err
		}

		if err := tx.Append(request.From, MoneyDebited{AccountID: request.From, To: request.To, Amount: request.Amount}); err != nil {
			return nil, err
		}

		if err := tx.Append(request.To, MoneyCredited{AccountID: request.To, From: request.From, Amount: request.Amount}); err != nil {
			return nil, err
		}

		// Append applied the events, so the accounts hold their post-commit balances.
		return json.Marshal(TransferResponse{FromBalance: from.Balance, ToBalance: to.Balance})
	})
}

// checkFunds returns a Rejectioner for every refusal, so each is recorded as an outcome.
func checkFunds(from, to *Account, amount int64) error {
	switch {
	case !from.Opened:
		return evt.NewNotFoundError(fmt.Sprintf("account %s is not open", from.ID))

	case !to.Opened:
		return evt.NewNotFoundError(fmt.Sprintf("account %s is not open", to.ID))

	case from.Balance < amount:
		return &InsufficientFundsError{Balance: from.Balance, Requested: amount}
	}

	return nil
}

// ---- Wiring ----

// newStore returns an in-memory store with command outcomes enabled. The DynamoDB and PostgreSQL
// repositories record outcomes the same way.
func newStore() *snapshots.Store {
	return snapshots.NewStore(mem.NewRepository(), 5).WithOutcomes()
}

// open opens an account under its own command ID: a store with outcomes requires one.
func open(ctx context.Context, store *snapshots.Store, id evt.EntityID, balance int64) error {
	metadata := evt.NewMetadata(ctx, nil, evt.WithCommandID(evt.CommandID("open-"+string(id))))

	return store.Execute(ctx, NewAccount(id), id, &OpenAccount{InitialBalance: balance}, metadata)
}

// balance loads an account and returns its balance.
func balance(ctx context.Context, store *snapshots.Store, id evt.EntityID) (int64, error) {
	account := NewAccount(id)
	if _, err := store.LoadAggregate(ctx, account, id); err != nil {
		return 0, err
	}

	return account.Balance, nil
}

// describe renders an outcome on one line.
func describe(outcome *evt.CommandOutcome) string {
	if outcome.Rejected() {
		return fmt.Sprintf("rejected (%s) replayed=%t", outcome.Rejection.Code, outcome.Replayed)
	}

	return fmt.Sprintf("accepted %s replayed=%t", outcome.Response, outcome.Replayed)
}

// run opens two accounts, transfers between them, retries, and prints what each call returned.
func run(ctx context.Context, out io.Writer) error {
	store := newStore()

	if err := open(ctx, store, "acct-1", 100); err != nil {
		return err
	}

	if err := open(ctx, store, "acct-2", 20); err != nil {
		return err
	}

	request := TransferRequest{From: "acct-1", To: "acct-2", Amount: 30}

	calls := []struct {
		label     string
		commandID evt.CommandID
		request   TransferRequest
	}{
		{"transfer-1", "transfer-1", request},
		{"transfer-1 retried", "transfer-1", request},
		{"transfer-2 overdraws", "transfer-2", TransferRequest{From: "acct-2", To: "acct-1", Amount: 500}},
		{"transfer-2 retried", "transfer-2", TransferRequest{From: "acct-2", To: "acct-1", Amount: 500}},
	}

	for _, call := range calls {
		outcome, err := Transfer(ctx, store, call.commandID, call.request)
		if err != nil {
			return fmt.Errorf("%s: %w", call.label, err)
		}

		if _, err := fmt.Fprintf(out, "%s: %s\n", call.label, describe(outcome)); err != nil {
			return err
		}
	}

	// The same command ID with a different request is refused before anything runs.
	request.Amount = 31

	_, err := Transfer(ctx, store, "transfer-1", request)
	if !errors.Is(err, evt.ErrIdempotencyKeyReuse) {
		return fmt.Errorf("transfer-1 with another amount: expected idempotency-key reuse, got %w", err)
	}

	if _, err := fmt.Fprintln(out, "transfer-1 with another amount: idempotency key reuse"); err != nil {
		return err
	}

	for _, id := range []evt.EntityID{"acct-1", "acct-2"} {
		balance, err := balance(ctx, store, id)
		if err != nil {
			return err
		}

		if _, err := fmt.Fprintf(out, "%s: balance=%d\n", id, balance); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		panic(err)
	}
}
