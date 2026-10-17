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
	"strings"
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
	accountEvent

	Owner    string    `json:"owner"`
	OpenedAt time.Time `json:"openedAt"`
}

// MoneyDeposited is the payload of account:deposited (v1).
type MoneyDeposited struct {
	accountEvent

	AmountCents int64   `json:"amountCents"`
	Memo        *string `json:"memo"`
}

// MoneyWithdrawn is the payload of account:withdrawn (v1).
type MoneyWithdrawn struct {
	accountEvent

	AmountCents int64 `json:"amountCents"`
}

// AccountTagged is the payload of account:tagged (v1).
type AccountTagged struct {
	accountEvent

	Tags []string `json:"tags"`
}

// AccountLabeled is the payload of account:labeled (v1).
type AccountLabeled struct {
	accountEvent

	Labels map[string]string `json:"labels"`
}

// AccountRenamed is the payload of account:renamed (v2). Version 1 stored the owner as "name".
type AccountRenamed struct {
	accountEvent

	Owner string `json:"owner"`
}

// AccountClosed is the payload of account:closed (v1).
type AccountClosed struct {
	accountEvent

	Reason *string `json:"reason"`
}

// accountEvent supplies the evt.Event methods every account event shares. It has no fields, so
// embedding it leaves the JSON payloads unchanged.
type accountEvent struct{}

func (accountEvent) EntityType() evt.EntityType { return AccountType }

// EntityID is empty: events carry no entity ID, and the store assigns it when serializing.
func (accountEvent) EntityID() evt.EntityID { return "" }

// Type implements evt.Event.
func (AccountOpened) Type() evt.EventType { return OpenedEvent }

// Version implements evt.Event.
func (AccountOpened) Version() evt.EventVersion { return 1 }

// Type implements evt.Event.
func (MoneyDeposited) Type() evt.EventType { return DepositedEvent }

// Version implements evt.Event.
func (MoneyDeposited) Version() evt.EventVersion { return 1 }

// Type implements evt.Event.
func (MoneyWithdrawn) Type() evt.EventType { return WithdrawnEvent }

// Version implements evt.Event.
func (MoneyWithdrawn) Version() evt.EventVersion { return 1 }

// Type implements evt.Event.
func (AccountTagged) Type() evt.EventType { return TaggedEvent }

// Version implements evt.Event.
func (AccountTagged) Version() evt.EventVersion { return 1 }

// Type implements evt.Event.
func (AccountLabeled) Type() evt.EventType { return LabeledEvent }

// Version implements evt.Event.
func (AccountLabeled) Version() evt.EventVersion { return 1 }

// Type implements evt.Event.
func (AccountRenamed) Type() evt.EventType { return RenamedEvent }

// Version implements evt.Event.
func (AccountRenamed) Version() evt.EventVersion { return 2 }

// Type implements evt.Event.
func (AccountClosed) Type() evt.EventType { return ClosedEvent }

// Version implements evt.Event.
func (AccountClosed) Version() evt.EventVersion { return 1 }

// Command is the account command, decoded from a scenario step. Kind selects the operation.
//
// Its JSON encoding is the step's own command object, keys sorted and compact, so the identity
// digest of a command (which hashes its payload JSON) is the same in both languages and does not
// depend on which optional fields a struct happens to declare.
type Command struct {
	raw map[string]json.RawMessage

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

// UnmarshalJSON decodes the typed fields and remembers the step's own keys.
func (c *Command) UnmarshalJSON(data []byte) error {
	type fields Command

	var typed fields
	if err := json.Unmarshal(data, &typed); err != nil {
		return err
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	*c = Command(typed)
	c.raw = raw

	return nil
}

// MarshalJSON encodes the step's command object with sorted keys.
func (c *Command) MarshalJSON() ([]byte, error) {
	if c.raw != nil {
		return json.Marshal(c.raw)
	}

	type fields Command

	return json.Marshal((*fields)(c))
}

// Type returns the command type.
func (c *Command) Type() evt.CommandType { return evt.CommandType("account:" + c.Kind) }

// EntityType returns the account entity type.
func (c *Command) EntityType() evt.EntityType { return AccountType }

// RejectedError is a business-rule rejection. Both languages reject the same commands.
type RejectedError struct {
	Reason  string
	Details map[string]any
}

func (e *RejectedError) Error() string { return "rejected: " + e.Reason }

// Rejection maps the rule onto the standard persisted rejection: the code is the reason in
// kebab-case ("insufficient funds" is "insufficient-funds"), the message the reason itself.
func (e *RejectedError) Rejection() evt.Rejection {
	return evt.Rejection{
		Code:    evt.RejectionCode(strings.ReplaceAll(e.Reason, " ", "-")),
		Message: e.Reason,
		Details: e.Details,
	}
}

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
		return emitWithBalance(a.BalanceCents+c.AmountCents, MoneyDeposited{AmountCents: c.AmountCents, Memo: c.Memo})

	case "batchDeposit":
		if len(c.Amounts) == 0 {
			return reject("no amounts")
		}
		result := make([]evt.Event, 0, len(c.Amounts))
		balance := a.BalanceCents
		for _, amount := range c.Amounts {
			if amount <= 0 {
				return reject("amount must be positive")
			}
			result = append(result, MoneyDeposited{AmountCents: amount})
			balance += amount
		}
		return emitWithBalance(balance, result...)

	case "withdraw":
		if c.AmountCents <= 0 {
			return reject("amount must be positive")
		}
		if c.AmountCents > a.BalanceCents {
			return evt.CommandResult{}, &RejectedError{
				Reason:  "insufficient funds",
				Details: map[string]any{"balanceCents": a.BalanceCents, "requestedCents": c.AmountCents},
			}
		}
		return emitWithBalance(a.BalanceCents-c.AmountCents, MoneyWithdrawn{AmountCents: c.AmountCents})

	case "ping":
		// Accepted with no events and no response.
		return evt.CommandResult{}, nil

	case "balance":
		// Accepted with no events; the response is the balance.
		return emitWithBalance(a.BalanceCents)

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

// emitWithBalance returns the events and a response carrying the balance after them. Outcome
// stores record the response; others ignore it.
func emitWithBalance(balance int64, list ...evt.Event) (evt.CommandResult, error) {
	return evt.CommandResult{Events: list, Response: fmt.Appendf(nil, `{"balanceCents":%d}`, balance)}, nil
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
