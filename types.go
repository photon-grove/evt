package evt

import (
	"context"
	"fmt"
	"time"
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
	Events      []Event     `json:"events"`
	Transaction Transaction `json:"transaction"`
}

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

// EntityType is the type of an aggregated Entity.
type EntityType string

// EntityID is a unique identifier for an aggregated Entity instance.
type EntityID string

// EntityFactory constructs a fresh zero-state aggregate instance for load/execute operations.
type EntityFactory func() Entity

// Entity is an object whose state is made up of aggregated immutable change Events.
type Entity interface {
	Type() EntityType
	GetID() EntityID
	Base() BaseEntity
	Handle(context.Context, Command) (CommandResult, error)
	Apply(Event) error
	DeserializeEvent(serializedEvent SerializedEvent) (Event, error)
	EventUpcasters() []EventUpcaster
	Projectors() []EventProjector
}

// BaseEntity provides common fields for all entities.
type BaseEntity struct {
	ID        EntityID  `json:"id"`        // A unique identifier
	IsActive  bool      `json:"isActive"`  // Whether this record has been deleted
	CreatedAt time.Time `json:"createdAt"` // The date this record was originally created
	UpdatedAt time.Time `json:"updatedAt"` // The date this record was last updated
}

// NewEntity creates a new BaseEntity with the given ID, setting IsActive to true and timestamps to now.
func NewEntity(id EntityID) BaseEntity {
	return BaseEntity{
		ID:        id,
		IsActive:  true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

func entityFromFactory(factory EntityFactory) (Entity, error) {
	if factory == nil {
		return nil, fmt.Errorf("entity factory is nil")
	}

	entity := factory()
	if entity == nil {
		return nil, fmt.Errorf("entity factory returned nil")
	}

	return entity, nil
}

// LoadEntityWithFactory creates a fresh entity instance and loads it from the store.
func LoadEntityWithFactory(
	ctx context.Context,
	store Store,
	factory EntityFactory,
	entityID EntityID,
) (Entity, Context, error) {
	entity, err := entityFromFactory(factory)
	if err != nil {
		return nil, Context{}, err
	}

	eventContext, err := store.LoadEntity(ctx, entity, entityID)
	if err != nil {
		return entity, eventContext, err
	}

	return entity, eventContext, nil
}

// ExecuteWithFactory creates a fresh entity instance, executes the command, and returns it.
func ExecuteWithFactory(
	ctx context.Context,
	store Store,
	factory EntityFactory,
	entityID EntityID,
	command Command,
	metadata Metadata,
) (Entity, error) {
	entity, err := entityFromFactory(factory)
	if err != nil {
		return nil, err
	}

	if err := store.Execute(ctx, entity, entityID, command, metadata); err != nil {
		return nil, err
	}

	return entity, nil
}

// EventProjector generates transactional view operations that should be committed alongside events.
type EventProjector interface {
	// Project produces a transaction group for the given entity state. Returning nil
	// indicates no operations are required.
	Project(context.Context, Entity, []Event) (TransactionGroup, error)
}
