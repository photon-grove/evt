package test

import (
	"encoding/json"

	"github.com/photon-grove/evt"
)

// NewTestStore creates a new MockStore for testing
func NewTestStore() *MockStore {
	return NewMockStore()
}

// NewTestEntity creates a new test Entity with the given ID
func NewTestEntity(id string) *Entity {
	return NewEntity(evt.EntityID(id))
}

// NewTestEventContext creates a new test event context with default values
func NewTestEventContext() evt.Context {
	seq := evt.EventSequence(0)
	return evt.Context{
		Entity:          nil,
		EntityID:        "",
		CurrentSequence: &seq,
		CurrentSnapshot: nil,
	}
}

// SimpleEvent is a simple event for integration testing
type SimpleEvent struct {
	eventType evt.EventType
	payload   map[string]any
}

// NewTestEvent creates a new SimpleEvent with the given type and payload
func NewTestEvent(eventType string, payload map[string]any) *SimpleEvent {
	return &SimpleEvent{
		eventType: evt.EventType(eventType),
		payload:   payload,
	}
}

// EntityType returns the entity type for this event
func (e *SimpleEvent) EntityType() evt.EntityType {
	return EntityType
}

// EntityID returns an empty entity ID (set during serialization)
func (e *SimpleEvent) EntityID() evt.EntityID {
	return ""
}

// Type returns the event type
func (e *SimpleEvent) Type() evt.EventType {
	return e.eventType
}

// Version returns the event version
func (e *SimpleEvent) Version() evt.EventVersion {
	return 1
}

// MarshalJSON implements json.Marshaler for SimpleEvent
func (e *SimpleEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.payload)
}
