package test

// WARNING: Not intended for use in Production

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	evt "github.com/photon-grove/evt/go"
)

// EntityType is a unique entity type
const EntityType evt.EntityType = "test"

// Example error that might be returned by a command handler that fails a business rule
var errBusiness = errors.New("business logic error")

// The Aggregate handles commands and applies events for testing
type Aggregate struct {
	ID    evt.EntityID `json:"id"`
	Value string       `json:"value"`
	Other *string      `json:"other"`
}

// NewAggregate returns an Aggregate instance for the given id
func NewAggregate(id evt.EntityID) *Aggregate {
	return &Aggregate{ID: id}
}

// Type returns the Entity type constant
func (aggregate *Aggregate) Type() evt.EntityType {
	return EntityType
}

// GetID returns the ID for this Aggregate instance
func (aggregate *Aggregate) GetID() evt.EntityID {
	return aggregate.ID
}

// EventUpcasters returns the upcasters for events within this Aggregate
func (aggregate *Aggregate) EventUpcasters() []evt.EventUpcaster {
	return nil
}

// Projectors returns the projectors for this Aggregate
func (aggregate *Aggregate) Projectors() []evt.EventProjector {
	return nil
}

// Command Handling
// ----------------

// Handle handles a command, yielding Events or an error
func (aggregate *Aggregate) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
	switch c := command.(type) {

	case *CreateAggregate:
		if c.Value == "" {
			return evt.CommandResult{}, errBusiness
		}
		return evt.CommandResult{
			Events: []evt.Event{
				&AggregateCreated{
					ID:    aggregate.GetID(),
					Value: c.Value,
					Other: c.Other,
				},
			},
		}, nil

	case *ReplaceAggregate:
		return evt.CommandResult{
			Events: []evt.Event{
				&AggregateUpdated{
					ID:    aggregate.GetID(),
					Value: c.Value,
					Other: c.Other,
				},
			},
		}, nil

	}

	return evt.CommandResult{}, fmt.Errorf("unrecognized Command: %v", command)
}

// Event Handling
// --------------

// Apply takes an event and applies it to the current state of this entity instance.
// Unknown events return a BadEventError (fail-closed).
func (aggregate *Aggregate) Apply(event evt.Event) error {
	switch e := event.(type) {

	case *AggregateCreated:
		aggregate.ID = e.ID
		aggregate.Value = e.Value
		aggregate.Other = e.Other

	case *AggregateUpdated:
		aggregate.Value = e.Value
		aggregate.Other = e.Other

	default:
		return evt.NewBadEventError(event)
	}

	return nil
}

// DeserializeEvent allows the event store to deserialize an event for this entity
func (aggregate *Aggregate) DeserializeEvent(serializedEvent evt.SerializedEvent) (evt.Event, error) {
	switch serializedEvent.Type {

	case CreatedEvent:
		var event AggregateCreated
		err := json.Unmarshal(serializedEvent.Payload, &event)
		if err != nil {
			return nil, err
		}

		return &event, nil

	case UpdatedEvent:
		var event AggregateUpdated
		err := json.Unmarshal(serializedEvent.Payload, &event)
		if err != nil {
			return nil, err
		}

		return &event, nil

	}

	return nil, fmt.Errorf("unknown event type: %s", serializedEvent.Type)
}
