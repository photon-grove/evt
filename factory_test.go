package evt

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type factoryStoreStub struct {
	load func(context.Context, Entity, EntityID) (Context, error)
	exec func(context.Context, Entity, EntityID, Command, Metadata) error
}

func (s factoryStoreStub) LoadEntity(ctx context.Context, entity Entity, entityID EntityID) (Context, error) {
	return s.load(ctx, entity, entityID)
}

func (s factoryStoreStub) Commit(context.Context, CommandResult, Context, Metadata) ([]SerializedEvent, error) {
	return nil, nil
}

func (s factoryStoreStub) Execute(ctx context.Context, entity Entity, entityID EntityID, command Command, metadata Metadata) error {
	return s.exec(ctx, entity, entityID, command, metadata)
}

type factoryEntity struct {
	BaseEntity
	Value string `json:"value"`
}

func newFactoryEntity(id EntityID) *factoryEntity {
	return &factoryEntity{BaseEntity: NewEntity(id)}
}

func (e *factoryEntity) Type() EntityType { return "factory-test" }

func (e *factoryEntity) GetID() EntityID { return e.ID }

func (e *factoryEntity) Base() BaseEntity { return e.BaseEntity }

func (e *factoryEntity) Handle(context.Context, Command) (CommandResult, error) {
	return CommandResult{}, nil
}

func (e *factoryEntity) Apply(Event) error { return nil }

func (e *factoryEntity) DeserializeEvent(serialized SerializedEvent) (Event, error) {
	var event factoryEvent
	if err := json.Unmarshal(serialized.Payload, &event); err != nil {
		return nil, err
	}
	return &event, nil
}

func (e *factoryEntity) EventUpcasters() []EventUpcaster { return nil }

func (e *factoryEntity) Projectors() []EventProjector { return nil }

type factoryEvent struct {
	Value string `json:"value"`
}

func (e *factoryEvent) Type() EventType        { return "factory-test:event" }
func (e *factoryEvent) Version() EventVersion  { return 1 }
func (e *factoryEvent) EntityType() EntityType { return "factory-test" }
func (e *factoryEvent) EntityID() EntityID     { return "factory-id" }

func TestLoadEntityWithFactoryCreatesFreshEntity(t *testing.T) {
	t.Parallel()

	factoryCalls := 0
	store := factoryStoreStub{
		load: func(_ context.Context, entity Entity, entityID EntityID) (Context, error) {
			seq := EventSequence(0)
			return Context{Entity: entity, EntityID: entityID, CurrentSequence: &seq}, nil
		},
		exec: func(context.Context, Entity, EntityID, Command, Metadata) error { return nil },
	}

	entityOne, ctxOne, err := LoadEntityWithFactory(context.Background(), store, func() Entity {
		factoryCalls++
		return newFactoryEntity("e1")
	}, "e1")
	require.NoError(t, err)

	entityTwo, ctxTwo, err := LoadEntityWithFactory(context.Background(), store, func() Entity {
		factoryCalls++
		return newFactoryEntity("e1")
	}, "e1")
	require.NoError(t, err)

	require.NotSame(t, entityOne, entityTwo)
	require.Equal(t, entityOne, ctxOne.Entity)
	require.Equal(t, entityTwo, ctxTwo.Entity)
	require.Equal(t, 2, factoryCalls)
}

func TestExecuteWithFactoryReturnsMutatedEntity(t *testing.T) {
	t.Parallel()

	store := factoryStoreStub{
		load: func(context.Context, Entity, EntityID) (Context, error) { return Context{}, nil },
		exec: func(_ context.Context, entity Entity, _ EntityID, _ Command, _ Metadata) error {
			testEntity, ok := entity.(*factoryEntity)
			require.True(t, ok)
			testEntity.Value = "applied"
			return nil
		},
	}

	entity, err := ExecuteWithFactory(context.Background(), store, func() Entity {
		return newFactoryEntity("e1")
	}, "e1", nil, Metadata{})
	require.NoError(t, err)
	testEntity, ok := entity.(*factoryEntity)
	require.True(t, ok)
	require.Equal(t, "applied", testEntity.Value)
}

func TestLoadEntityWithFactoryRejectsNilFactory(t *testing.T) {
	t.Parallel()

	store := factoryStoreStub{
		load: func(context.Context, Entity, EntityID) (Context, error) { return Context{}, nil },
		exec: func(context.Context, Entity, EntityID, Command, Metadata) error { return nil },
	}

	_, _, err := LoadEntityWithFactory(context.Background(), store, nil, "e1")
	require.ErrorContains(t, err, "entity factory is nil")

	_, err = ExecuteWithFactory(context.Background(), store, func() Entity {
		return nil
	}, "e1", nil, Metadata{})
	require.ErrorContains(t, err, "entity factory returned nil")
}

func TestLoadEntityWithFactoryReturnsEntityOnLoadError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("load failed")
	store := factoryStoreStub{
		load: func(_ context.Context, entity Entity, entityID EntityID) (Context, error) {
			seq := EventSequence(0)
			return Context{Entity: entity, EntityID: entityID, CurrentSequence: &seq}, expectedErr
		},
		exec: func(context.Context, Entity, EntityID, Command, Metadata) error { return nil },
	}

	entity, eventContext, err := LoadEntityWithFactory(context.Background(), store, func() Entity {
		return newFactoryEntity("e1")
	}, "e1")
	require.ErrorIs(t, err, expectedErr)
	require.NotNil(t, entity)
	require.Same(t, entity, eventContext.Entity)
}
