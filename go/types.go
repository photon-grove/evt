package evt

import (
	"context"
	"fmt"
)

// CommandType is a unique identifier for the type of a Command.
type CommandType string

// CommandID is a unique identifier for a Command instance.
type CommandID string

// Command is implemented by all command types.
type Command interface {
	Type() CommandType
	EntityType() EntityType
}

// CommandResult is the outcome of executing a Command.
type CommandResult struct {
	Events []Event `json:"events"`

	// Response is stored with the command's outcome on a store with outcomes enabled (ADR 0004,
	// C3), so a retried command returns the original answer. Nil means the command returned none.
	Response []byte `json:"response,omitempty"`
}

// EventType identifies an event kind.
type EventType string

// EventID combines the entity ID and event sequence.
type EventID string

// EventSequence is a unique sequence number within an aggregate instance.
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
	Aggregate       Aggregate
	EntityID        EntityID
	CurrentSequence *EventSequence
	CurrentSnapshot *EventSequence
	SeenCommandIDs  map[CommandID]struct{} // CommandIDs observed during event replay (dedupe guard)
}

// HasCommandID reports whether the given CommandID was already seen during event replay. An empty
// CommandID is absent, never a duplicate, as in CommitMetadata.
func (c Context) HasCommandID(id CommandID) bool {
	if id == "" || c.SeenCommandIDs == nil {
		return false
	}

	_, ok := c.SeenCommandIDs[id]

	return ok
}

// RecordCommandID adds a CommandID to the seen set (initializing the map if needed). An empty
// CommandID is absent and is not recorded.
func (c *Context) RecordCommandID(id CommandID) {
	if id == "" {
		return
	}

	if c.SeenCommandIDs == nil {
		c.SeenCommandIDs = make(map[CommandID]struct{})
	}

	c.SeenCommandIDs[id] = struct{}{}
}

// GetEventID builds a stable event ID from the entity ID and sequence.
func GetEventID(entityID EntityID, sequence EventSequence) EventID {
	return EventID(fmt.Sprintf("%s:%d", entityID, sequence))
}

// EntityType names the type of a stream's Aggregate, such as "account".
type EntityType string

// EntityID is the unique identifier of an Aggregate instance, which is also the identity of its stream.
type EntityID string

// Aggregate is an object whose state is rebuilt by folding its immutable Events. Each instance is
// one stream, identified by its EntityType and EntityID.
type Aggregate interface {
	Type() EntityType
	GetID() EntityID
	Handle(context.Context, Command) (CommandResult, error)
	Apply(Event) error
	DeserializeEvent(serializedEvent SerializedEvent) (Event, error)
	EventUpcasters() []EventUpcaster
	Projectors() []EventProjector
}

// EventProjector generates transactional view operations that should be committed alongside events.
type EventProjector interface {
	// Project produces a transaction group for the given aggregate state. Returning nil
	// indicates no operations are required.
	Project(context.Context, Aggregate, []Event) (TransactionGroup, error)
}
