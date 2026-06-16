package evt

import (
	"context"

	"github.com/photon-grove/evt/result"
)

// StorageType identifies the backend storage system a Repository uses.
type StorageType string

// StreamFilter narrows which entities a table-wide stream or projection rebuild visits. It is a
// backend-neutral description of the filter: each Repository translates it into its own query
// mechanism (the DynamoDB backend, for example, compiles it into a Scan FilterExpression; a future
// SQL backend would translate it into a WHERE clause). The zero value imposes no constraint and
// matches every entity.
//
// It deliberately exposes only entity-type filtering — the single predicate the framework's rebuild
// paths require — so the core Repository contract carries no backend-specific query types. Backends
// that support richer server-side filtering may offer it through their own extension interface (for
// example the dynamo package's ExpressionStreamer) without widening this type.
type StreamFilter struct {
	// EntityType, when non-empty, restricts the stream to entities of that type.
	EntityType EntityType
}

// Matches reports whether an entity of the given type passes the filter. A zero filter (empty
// EntityType) matches every entity. Backends without server-side filtering can use this to filter
// client-side and still honor the contract.
func (f StreamFilter) Matches(entityType EntityType) bool {
	return f.EntityType == "" || f.EntityType == entityType
}

// Store handles executing Commands, committing Events yielded by those Commands,
// and loading Entity instances from a Repository.
type Store interface {
	// LoadEntity loads the given Entity instance with the given id, retrieving Snapshots
	// and Events from the Repository.
	LoadEntity(
		ctx context.Context,
		entity Entity,
		entityID EntityID,
	) (Context, error)

	// Commit new Events to the Entity within the given context, with optional Metadata.
	Commit(
		ctx context.Context,
		result CommandResult,
		context Context,
		metadata Metadata,
	) ([]SerializedEvent, error)

	// Execute takes an empty Entity instance, loads it from the Repository, handles the given
	// Command, commits the resulting Events using the given Metadata, and applies those Events to the
	// current Entity instance.
	Execute(
		ctx context.Context,
		entity Entity,
		entityID EntityID,
		command Command,
		metadata Metadata,
	) error
}

// Repository manages events for entities consistently and persists snapshots to improve load time.
type Repository interface {
	// Commit new Events for an Entity instance.
	Commit(ctx context.Context, result SerializedResult) error

	// Commit a stream of Events using whatever batch size works best for the Repository.
	CommitStream(ctx context.Context, channel <-chan result.Result[SerializedResult]) []error

	// Commit new Events for an Entity instance while also saving the given Snapshot.
	CommitWithSnapshot(
		ctx context.Context,
		result SerializedResult,
		entityType EntityType,
		entityID EntityID,
		payload []byte,
		currentSnapshot EventSequence,
	) error

	// Get all Events for an Entity instance.
	GetEvents(
		ctx context.Context,
		entityID EntityID,
	) ([]SerializedEvent, error)

	// Get the latest Events beyond the given sequence number for an Entity, used for
	// finding Events after a particular Snapshot.
	GetLatestEvents(
		ctx context.Context,
		entityID EntityID,
		lastSequence EventSequence,
	) ([]SerializedEvent, error)

	// Get the latest Snapshot for an Entity instance.
	GetSnapshot(
		ctx context.Context,
		entityID EntityID,
	) (*SerializedSnapshot, error)

	// StreamAllEvents returns a channel of all Events in the Repository, optionally narrowed by the
	// backend-neutral StreamFilter. A zero filter streams every event.
	StreamAllEvents(
		ctx context.Context,
		filter StreamFilter,
	) <-chan result.Result[[]SerializedEvent]

	// StreamEntities returns a channel of all Entities in the Repository, optionally narrowed by the
	// backend-neutral StreamFilter, with the given function applied to each Entity to fold the
	// application-specific Events into each instance. A zero filter streams every entity.
	StreamEntities(
		ctx context.Context,
		filter StreamFilter,
		applyEvent func(context.Context, SerializedEvent, Entity) (Entity, error),
	) <-chan result.Result[Entity]
}
