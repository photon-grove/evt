package evt

import (
	"context"
	"fmt"
)

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
