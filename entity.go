package evt

import (
	"context"
	"time"
)

// EntityType is the type of an aggregated Entity
type EntityType string

// EntityID is a unique identifier for an aggregated Entity instance
type EntityID string

// EntityFactory constructs a fresh zero-state aggregate instance for load/execute operations.
type EntityFactory func() Entity

// An Entity is an object whose state is made up of aggregated immutable change Events
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

// BaseEntity provides common fields for all entities
type BaseEntity struct {
	ID        EntityID  `json:"id"`        // A unique identifier
	IsActive  bool      `json:"isActive"`  // Whether this record has been deleted
	CreatedAt time.Time `json:"createdAt"` // The date this record was originally created
	UpdatedAt time.Time `json:"updatedAt"` // The date this record was last updated
}

// NewEntity creates a new BaseEntity with the given ID, setting IsActive to true and timestamps to now
func NewEntity(id EntityID) BaseEntity {
	return BaseEntity{
		id,
		true,       // IsActive
		time.Now(), // CreatedAt
		time.Now(), // UpdatedAt
	}
}
