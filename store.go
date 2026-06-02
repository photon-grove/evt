package evt

import "context"

// A Store handles executing Commands, committing Events that are yielded by those Commands,
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
