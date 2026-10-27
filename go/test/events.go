package test

import evt "github.com/photon-grove/evt/go"

// CreatedEvent is an event type
const CreatedEvent evt.EventType = evt.EventType(string(EntityType) + ":created")

// AggregateCreated is the "created" event
type AggregateCreated struct {
	ID    evt.EntityID `json:"id"`
	Value string       `json:"value"`
	Other *string      `json:"other"`
}

// EntityType returns the entity type constant for this event
func (e AggregateCreated) EntityType() evt.EntityType {
	return EntityType
}

// EntityID returns the ID for this entity
func (e AggregateCreated) EntityID() evt.EntityID {
	return e.ID
}

// Type returns the event type constant
func (e AggregateCreated) Type() evt.EventType {
	return CreatedEvent
}

// Version returns the version of the event schema - when changed, increment this
func (e AggregateCreated) Version() evt.EventVersion {
	return 1
}

// UpdatedEvent represents the event type with a string unique to the entity type
const UpdatedEvent evt.EventType = evt.EventType(string(EntityType) + ":updated")

// AggregateUpdated is the "updated" event
type AggregateUpdated struct {
	ID    evt.EntityID `json:"id"`
	Value string       `json:"value"`
	Other *string      `json:"other"`
}

// EntityType returns the Entity type constant for this event
func (e AggregateUpdated) EntityType() evt.EntityType {
	return EntityType
}

// EntityID returns the ID of the aggregate's stream
func (e AggregateUpdated) EntityID() evt.EntityID {
	return e.ID
}

// Type returns the event type constant
func (e AggregateUpdated) Type() evt.EventType {
	return UpdatedEvent
}

// Version returns the version of the event schema - when changed, increment this
func (e AggregateUpdated) Version() evt.EventVersion {
	return 1
}

// FakeEvent is a dummy event that should not be recognized by the entity.
// It implements the Event interface to test fail-closed Apply behavior.
type FakeEvent struct{}

// Type returns a fake event type
func (f *FakeEvent) Type() evt.EventType {
	return "test:fake"
}

// Version returns the version
func (f *FakeEvent) Version() evt.EventVersion {
	return 1
}

// EntityType returns the entity type
func (f *FakeEvent) EntityType() evt.EntityType {
	return EntityType
}

// EntityID returns an empty entity ID
func (f *FakeEvent) EntityID() evt.EntityID {
	return ""
}
