package evt

import "fmt"

// EventType identifies an event kind.
type EventType string

// EventID combines the entity ID and event sequence.
type EventID string

// EventSequence is a unique sequence number within an entity instance.
type EventSequence int

// EventVersion is the schema version of an event.
type EventVersion int

// Event is implemented by all event types.
type Event interface {
	Type() EventType
	Version() EventVersion
	EntityType() EntityType
	EntityID() EntityID
}

// Context carries entity context for the EventStore.
type Context struct {
	Entity          Entity
	EntityID        EntityID
	CurrentSequence *EventSequence
	CurrentSnapshot *EventSequence
	SeenCommandIDs  map[CommandID]struct{} // CommandIDs observed during event replay (dedupe guard)
}

// HasCommandID reports whether the given CommandID was already seen during event replay.
func (c Context) HasCommandID(id CommandID) bool {
	if c.SeenCommandIDs == nil {
		return false
	}
	_, ok := c.SeenCommandIDs[id]
	return ok
}

// RecordCommandID adds a CommandID to the seen set (initializing the map if needed).
func (c *Context) RecordCommandID(id CommandID) {
	if c.SeenCommandIDs == nil {
		c.SeenCommandIDs = make(map[CommandID]struct{})
	}
	c.SeenCommandIDs[id] = struct{}{}
}

// GetEventID builds a stable event ID from the entity ID and sequence.
func GetEventID(entityID EntityID, sequence EventSequence) EventID {
	return EventID(fmt.Sprintf("%s:%d", entityID, sequence))
}
