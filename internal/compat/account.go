// Package compat is the Go half of the cross-language compatibility harness between this module
// and the Rust photon-evt crate. It defines a small account domain that both languages implement
// identically, runs the shared scenarios in compat/scenarios.json, and generates the golden
// fixtures in compat/fixtures that the Rust test suite asserts against byte for byte.
//
// The domain deliberately keeps entity IDs out of payloads and state so fixtures can be replayed
// under per-run entity IDs during live integration runs.
package compat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/photon-grove/evt"
)

// AccountType is the entity type of the shared account aggregate.
const AccountType evt.EntityType = "account"

// Event types and their current versions.
const (
	OpenedEvent    evt.EventType = "account:opened"
	DepositedEvent evt.EventType = "account:deposited"
	WithdrawnEvent evt.EventType = "account:withdrawn"
	TaggedEvent    evt.EventType = "account:tagged"
	LabeledEvent   evt.EventType = "account:labeled"
	RenamedEvent   evt.EventType = "account:renamed"
	ClosedEvent    evt.EventType = "account:closed"
)

// Account is the shared aggregate. Its JSON encoding is the snapshot format, so the Rust struct
// declares the same fields in the same order.
type Account struct {
	id           evt.EntityID
	Owner        string            `json:"owner"`
	Status       string            `json:"status"`
	BalanceCents int64             `json:"balanceCents"`
	Deposits     int64             `json:"deposits"`
	OpenedAt     time.Time         `json:"openedAt"`
	Tags         []string          `json:"tags"`
	Labels       map[string]string `json:"labels"`
	LastMemo     *string           `json:"lastMemo"`
}

// NewAccount returns an empty account for the given entity ID.
func NewAccount(id evt.EntityID) *Account {
	return &Account{id: id}
}

// AccountOpened is the payload of account:opened (v1).
type AccountOpened struct {
	Owner    string    `json:"owner"`
	OpenedAt time.Time `json:"openedAt"`
}

// MoneyDeposited is the payload of account:deposited (v1).
type MoneyDeposited struct {
	AmountCents int64   `json:"amountCents"`
	Memo        *string `json:"memo"`
}

// MoneyWithdrawn is the payload of account:withdrawn (v1).
type MoneyWithdrawn struct {
	AmountCents int64 `json:"amountCents"`
}

// AccountTagged is the payload of account:tagged (v1).
type AccountTagged struct {
	Tags []string `json:"tags"`
}

// AccountLabeled is the payload of account:labeled (v1).
type AccountLabeled struct {
	Labels map[string]string `json:"labels"`
}

// AccountRenamed is the payload of account:renamed (v2). Version 1 stored the owner as "name".
type AccountRenamed struct {
	Owner string `json:"owner"`
}

// AccountClosed is the payload of account:closed (v1).
type AccountClosed struct {
	Reason *string `json:"reason"`
}

func (AccountOpened) Type() evt.EventType  { return OpenedEvent }
func (MoneyDeposited) Type() evt.EventType { return DepositedEvent }
func (MoneyWithdrawn) Type() evt.EventType { return WithdrawnEvent }
func (AccountTagged) Type() evt.EventType  { return TaggedEvent }
func (AccountLabeled) Type() evt.EventType { return LabeledEvent }
func (AccountRenamed) Type() evt.EventType { return RenamedEvent }
func (AccountClosed) Type() evt.EventType  { return ClosedEvent }

func (AccountOpened) Version() evt.EventVersion  { return 1 }
func (MoneyDeposited) Version() evt.EventVersion { return 1 }
func (MoneyWithdrawn) Version() evt.EventVersion { return 1 }
func (AccountTagged) Version() evt.EventVersion  { return 1 }
func (AccountLabeled) Version() evt.EventVersion { return 1 }
func (AccountRenamed) Version() evt.EventVersion { return 2 }
func (AccountClosed) Version() evt.EventVersion  { return 1 }

func (AccountOpened) EntityType() evt.EntityType  { return AccountType }
func (MoneyDeposited) EntityType() evt.EntityType { return AccountType }
func (MoneyWithdrawn) EntityType() evt.EntityType { return AccountType }
func (AccountTagged) EntityType() evt.EntityType  { return AccountType }
func (AccountLabeled) EntityType() evt.EntityType { return AccountType }
func (AccountRenamed) EntityType() evt.EntityType { return AccountType }
func (AccountClosed) EntityType() evt.EntityType  { return AccountType }

// Events carry no entity ID. The store assigns it when serializing.
func (AccountOpened) EntityID() evt.EntityID  { return "" }
func (MoneyDeposited) EntityID() evt.EntityID { return "" }
func (MoneyWithdrawn) EntityID() evt.EntityID { return "" }
func (AccountTagged) EntityID() evt.EntityID  { return "" }
func (AccountLabeled) EntityID() evt.EntityID { return "" }
func (AccountRenamed) EntityID() evt.EntityID { return "" }
func (AccountClosed) EntityID() evt.EntityID  { return "" }

// Command is the account command, decoded from a scenario step. Kind selects the operation.
type Command struct {
	Kind        string            `json:"type"`
	Owner       string            `json:"owner"`
	OpenedAt    string            `json:"openedAt"`
	AmountCents int64             `json:"amountCents"`
	Amounts     []int64           `json:"amounts"`
	Memo        *string           `json:"memo"`
	Tags        []string          `json:"tags"`
	Labels      map[string]string `json:"labels"`
	Reason      *string           `json:"reason"`
}

// Type returns the command type.
func (c *Command) Type() evt.CommandType { return evt.CommandType("account:" + c.Kind) }

// EntityType returns the account entity type.
func (c *Command) EntityType() evt.EntityType { return AccountType }

// RejectedError is a business-rule rejection. Both languages reject the same commands.
type RejectedError struct {
	Reason string
}

func (e *RejectedError) Error() string { return "rejected: " + e.Reason }

func reject(reason string) (evt.CommandResult, error) {
	return evt.CommandResult{}, &RejectedError{Reason: reason}
}

// Type returns the account entity type.
func (a *Account) Type() evt.EntityType { return AccountType }

// GetID returns the entity ID.
func (a *Account) GetID() evt.EntityID { return a.id }

// SetID sets the entity ID. The store calls it after loading.
func (a *Account) SetID(id evt.EntityID) { a.id = id }

// Base returns the base entity fields.
func (a *Account) Base() evt.BaseEntity { //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	return evt.BaseEntity{ID: a.id, IsActive: a.Status == "open"} //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
}

// Projectors returns no in-band projectors.
func (a *Account) Projectors() []evt.EventProjector { return nil }

// EventUpcasters migrates account:renamed v1 to v2.
func (a *Account) EventUpcasters() []evt.EventUpcaster {
	return []evt.EventUpcaster{renamedV1{}}
}

// Handle decides the events for a command. The rules must match the Rust implementation.
func (a *Account) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
	c, ok := command.(*Command)
	if !ok {
		return evt.CommandResult{}, evt.NewBadCommandError(command)
	}

	if c.Kind == "open" {
		if a.Status != "" {
			return reject("account already opened")
		}

		var openedAt time.Time
		if c.OpenedAt != "" {
			parsed, err := time.Parse(time.RFC3339Nano, c.OpenedAt)
			if err != nil {
				return reject("invalid openedAt")
			}
			openedAt = parsed
		}

		return emit(AccountOpened{Owner: c.Owner, OpenedAt: openedAt})
	}

	if a.Status != "open" {
		return reject("account is not open")
	}

	switch c.Kind {
	case "deposit":
		if c.AmountCents <= 0 {
			return reject("amount must be positive")
		}
		return emit(MoneyDeposited{AmountCents: c.AmountCents, Memo: c.Memo})

	case "batchDeposit":
		if len(c.Amounts) == 0 {
			return reject("no amounts")
		}
		result := make([]evt.Event, 0, len(c.Amounts))
		for _, amount := range c.Amounts {
			if amount <= 0 {
				return reject("amount must be positive")
			}
			result = append(result, MoneyDeposited{AmountCents: amount})
		}
		return evt.CommandResult{Events: result}, nil

	case "withdraw":
		if c.AmountCents <= 0 {
			return reject("amount must be positive")
		}
		if c.AmountCents > a.BalanceCents {
			return reject("insufficient funds")
		}
		return emit(MoneyWithdrawn{AmountCents: c.AmountCents})

	case "tag":
		if len(c.Tags) == 0 {
			return reject("no tags")
		}
		return emit(AccountTagged{Tags: c.Tags})

	case "setLabels":
		if len(c.Labels) == 0 {
			return reject("no labels")
		}
		return emit(AccountLabeled{Labels: c.Labels})

	case "rename":
		if c.Owner == "" {
			return reject("owner is required")
		}
		return emit(AccountRenamed{Owner: c.Owner})

	case "close":
		return emit(AccountClosed{Reason: c.Reason})
	}

	return evt.CommandResult{}, evt.NewBadCommandError(command)
}

func emit(list ...evt.Event) (evt.CommandResult, error) {
	return evt.CommandResult{Events: list}, nil
}

// Apply folds an event into state.
func (a *Account) Apply(event evt.Event) error {
	switch e := event.(type) {
	case AccountOpened:
		a.Owner = e.Owner
		a.OpenedAt = e.OpenedAt
		a.Status = "open"
	case MoneyDeposited:
		a.BalanceCents += e.AmountCents
		a.Deposits++
		if e.Memo != nil {
			a.LastMemo = e.Memo
		}
	case MoneyWithdrawn:
		a.BalanceCents -= e.AmountCents
	case AccountTagged:
		a.Tags = append(a.Tags, e.Tags...)
	case AccountLabeled:
		if a.Labels == nil {
			a.Labels = make(map[string]string, len(e.Labels))
		}
		for key, value := range e.Labels {
			a.Labels[key] = value
		}
	case AccountRenamed:
		a.Owner = e.Owner
	case AccountClosed:
		a.Status = "closed"
	default:
		return evt.NewBadEventError(event)
	}

	return nil
}

// DeserializeEvent decodes a stored payload into its typed event.
func (a *Account) DeserializeEvent(serialized evt.SerializedEvent) (evt.Event, error) {
	switch serialized.Type {
	case OpenedEvent:
		return decode[AccountOpened](serialized.Payload)
	case DepositedEvent:
		return decode[MoneyDeposited](serialized.Payload)
	case WithdrawnEvent:
		return decode[MoneyWithdrawn](serialized.Payload)
	case TaggedEvent:
		return decode[AccountTagged](serialized.Payload)
	case LabeledEvent:
		return decode[AccountLabeled](serialized.Payload)
	case RenamedEvent:
		return decode[AccountRenamed](serialized.Payload)
	case ClosedEvent:
		return decode[AccountClosed](serialized.Payload)
	}

	return nil, fmt.Errorf("unknown event type: %s", serialized.Type)
}

func decode[T evt.Event](payload []byte) (evt.Event, error) {
	var event T
	if err := json.Unmarshal(payload, &event); err != nil {
		return nil, err
	}

	return event, nil
}

// renamedV1 upcasts account:renamed v1 ({"name": ...}) to v2 ({"owner": ...}).
type renamedV1 struct{}

func (renamedV1) CanUpcast(eventType evt.EventType, version evt.EventVersion) bool {
	return eventType == RenamedEvent && version == 1
}

func (renamedV1) Upcast(event evt.SerializedEvent) (evt.SerializedEvent, error) {
	var payload map[string]any
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return event, err
	}

	name, ok := payload["name"]
	if !ok {
		return event, errors.New("account:renamed v1 payload has no name")
	}

	delete(payload, "name")
	payload["owner"] = name

	upcasted, err := json.Marshal(payload)
	if err != nil {
		return event, err
	}

	event.Payload = upcasted
	event.Version = 2

	return event, nil
}
