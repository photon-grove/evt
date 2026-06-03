package evt

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"

	"github.com/photon-grove/evt/result"
)

// StorageType identifies the backend storage system a Repository uses.
type StorageType string

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

	// StreamAllEvents returns a channel of all Events in the Repository, optionally filtered by the
	// given DynamoDB expression.
	StreamAllEvents(
		ctx context.Context,
		expr *expression.Expression,
	) <-chan result.Result[[]SerializedEvent]

	// StreamEntities returns a channel of all Entities in the Repository, optionally filtered by
	// the given DynamoDB expression, and with the given function applied to each Entity to apply
	// the application-specific Events to each instance.
	StreamEntities(
		ctx context.Context,
		expr *expression.Expression,
		applyEvent func(context.Context, SerializedEvent, Entity) (Entity, error),
	) <-chan result.Result[Entity]
}
