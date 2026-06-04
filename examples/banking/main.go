// Command banking is a runnable example of an evt-based event-sourced aggregate.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/mem"
)

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

func (a *Account) GetID() evt.EntityID { return a.ID }

func (a *Account) Base() evt.BaseEntity { return a.BaseEntity }

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
		a.ID = e.AccountID
		a.Balance = e.Balance
		a.Opened = true

	case MoneyDeposited:
		a.Balance += e.Amount

	default:
		return evt.NewBadEventError(event)
	}

	return nil
}

func (a *Account) DeserializeEvent(serialized evt.SerializedEvent) (evt.Event, error) {
	switch serialized.Type {
	case "bank_account.opened":
		var event AccountOpened
		if err := json.Unmarshal(serialized.Payload, &event); err != nil {
			return nil, err
		}

		return event, nil

	case "bank_account.money_deposited":
		var event MoneyDeposited
		if err := json.Unmarshal(serialized.Payload, &event); err != nil {
			return nil, err
		}

		return event, nil

	default:
		return nil, fmt.Errorf("unrecognized event: %s", serialized.Type)
	}
}

func (a *Account) EventUpcasters() []evt.EventUpcaster { return nil }

func (a *Account) Projectors() []evt.EventProjector { return nil }

type OpenAccount struct {
	AccountID      evt.EntityID
	InitialBalance int
}

func (c *OpenAccount) Type() evt.CommandType { return "bank_account.open" }

func (c *OpenAccount) EntityType() evt.EntityType { return entityType }

type Deposit struct {
	AccountID evt.EntityID
	Amount    int
}

func (c *Deposit) Type() evt.CommandType { return "bank_account.deposit" }

func (c *Deposit) EntityType() evt.EntityType { return entityType }

type AccountOpened struct {
	AccountID evt.EntityID `json:"accountId"`
	Balance   int          `json:"balance"`
	At        time.Time    `json:"at"`
}

func (e AccountOpened) Type() evt.EventType { return "bank_account.opened" }

func (e AccountOpened) Version() evt.EventVersion { return 1 }

func (e AccountOpened) EntityID() evt.EntityID { return e.AccountID }

func (e AccountOpened) EntityType() evt.EntityType { return entityType }

type MoneyDeposited struct {
	AccountID evt.EntityID `json:"accountId"`
	Amount    int          `json:"amount"`
	At        time.Time    `json:"at"`
}

func (e MoneyDeposited) Type() evt.EventType { return "bank_account.money_deposited" }

func (e MoneyDeposited) Version() evt.EventVersion { return 1 }

func (e MoneyDeposited) EntityID() evt.EntityID { return e.AccountID }

func (e MoneyDeposited) EntityType() evt.EntityType { return entityType }

func main() {
	ctx := context.Background()
	store := mem.NewStore()
	account := NewAccount("acct-1")

	err := store.Execute(ctx, account, "acct-1", &OpenAccount{AccountID: "acct-1", InitialBalance: 100}, evt.Metadata{})
	if err != nil {
		panic(err)
	}

	err = store.Execute(ctx, account, "acct-1", &Deposit{AccountID: "acct-1", Amount: 25}, evt.Metadata{})
	if err != nil {
		panic(err)
	}

	fmt.Printf("balance=%d\n", account.Balance)
}
