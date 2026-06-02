package test

import (
	"context"
	"fmt"

	"github.com/photon-grove/evt"
	"github.com/stretchr/testify/mock"
)

// MockStore is a mock store that satisfies the `EventStore` interface
type MockStore struct {
	mock.Mock
}

// NewMockStore automatically initializes a testify mock on creating a new instance
func NewMockStore() *MockStore {
	return new(MockStore)
}

// LoadEntity mocks the corresponding method on the Store
func (store *MockStore) LoadEntity(ctx context.Context, entity evt.Entity, entityID evt.EntityID) (evt.Context, error) {
	args := store.Called(ctx, entity, entityID)

	arg := args.Get(0)
	if arg == nil {
		return evt.Context{}, args.Error(1)
	}

	eventContext, ok := arg.(evt.Context)
	if !ok {
		// Also support pointer for backward compatibility
		if ptr, ok := arg.(*evt.Context); ok && ptr != nil {
			return *ptr, args.Error(1)
		}
		return evt.Context{}, fmt.Errorf("expected evt.Context, got %T", arg)
	}

	return eventContext, args.Error(1)
}

// Commit mocks the corresponding method on the Store
func (store *MockStore) Commit(ctx context.Context, result evt.CommandResult, eventContext evt.Context, metadata evt.Metadata) ([]evt.SerializedEvent, error) {
	args := store.Called(ctx, result, eventContext, metadata)

	arg := args.Get(0)
	if arg == nil {
		return nil, args.Error(1)
	}

	serializedEvents, ok := arg.([]evt.SerializedEvent)
	if !ok {
		return nil, fmt.Errorf("expected []evt.SerializedEvent, got %T", arg)
	}

	return serializedEvents, args.Error(1)
}

// Execute mocks the corresponding method on the Store
func (store *MockStore) Execute(ctx context.Context, entity evt.Entity, entityID evt.EntityID, command evt.Command, metadata evt.Metadata) error {
	args := store.Called(ctx, entity, entityID, command, metadata)

	return args.Error(0)
}
