package test

// WARNING: Not intended for use in Production

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/photon-grove/evt"
)

// EntityType is a unique entity type
const EntityType evt.EntityType = "test"

// Example error that might be returned by a command handler that fails a business rule
var errBusiness = errors.New("business logic error")

// The Entity handles commands and applies events for testing
type Entity struct {
	evt.BaseEntity
	Value string  `json:"value"`
	Other *string `json:"other"`
}

// NewEntity returns an Entity instance for the given id
func NewEntity(id evt.EntityID) *Entity {
	return &Entity{
		BaseEntity: evt.NewEntity(id),
	}
}

// Type returns the Entity type constant
func (entity *Entity) Type() evt.EntityType {
	return EntityType
}

// GetID returns the ID for this Entity instance
func (entity *Entity) GetID() evt.EntityID {
	return entity.ID
}

// Base returns the base entity fields
func (entity *Entity) Base() evt.BaseEntity {
	return entity.BaseEntity
}

// EventUpcasters returns the upcasters for events within this Entity
func (entity *Entity) EventUpcasters() []evt.EventUpcaster {
	return nil
}

// Projectors returns the projectors for this Entity
func (entity *Entity) Projectors() []evt.EventProjector {
	return nil
}

// Command Handling
// ----------------

// Handle handles a command, yielding Events or an error
func (entity *Entity) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
	switch c := command.(type) {

	case *CreateEntity:
		if c.Value == "" {
			return evt.CommandResult{}, errBusiness
		}
		return evt.CommandResult{
			Events: []evt.Event{
				&EntityCreated{
					ID:    entity.GetID(),
					Value: c.Value,
					Other: c.Other,
				},
			},
		}, nil

	case *ReplaceEntity:
		return evt.CommandResult{
			Events: []evt.Event{
				&EntityUpdated{
					ID:    entity.GetID(),
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
func (entity *Entity) Apply(event evt.Event) error {
	switch e := event.(type) {

	case *EntityCreated:
		entity.ID = e.ID
		entity.Value = e.Value
		entity.Other = e.Other

	case *EntityUpdated:
		entity.Value = e.Value
		entity.Other = e.Other

	default:
		return evt.NewBadEventError(event)
	}

	return nil
}

// DeserializeEvent allows the event store to deserialize an event for this entity
func (entity *Entity) DeserializeEvent(serializedEvent evt.SerializedEvent) (evt.Event, error) {
	switch serializedEvent.Type {

	case CreatedEvent:
		var event EntityCreated
		err := json.Unmarshal(serializedEvent.Payload, &event)
		if err != nil {
			return nil, err
		}

		return &event, nil

	case UpdatedEvent:
		var event EntityUpdated
		err := json.Unmarshal(serializedEvent.Payload, &event)
		if err != nil {
			return nil, err
		}

		return &event, nil

	}

	return nil, fmt.Errorf("unknown event type: %s", serializedEvent.Type)
}
