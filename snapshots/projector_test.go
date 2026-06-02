package snapshots_test

import (
	"context"
	"errors"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

// Test Execute method - projector execution
func TestExecute_ProjectorCalled(t *testing.T) {
	setup := newTestSetup(t, "test-execute-projector", 5)
	ctx := context.Background()
	metadata := newTestMetadata()

	projector := &MockProjector{}
	entity := &MockEntity{
		BaseEntity: evt.NewEntity(setup.entityID),
		projectors: []evt.EventProjector{projector},
	}
	command := &test.CreateEntity{Value: "test-value"}

	err := setup.store.Execute(ctx, entity, setup.entityID, command, metadata)
	require.NoError(t, err)

	require.True(t, projector.called)
	require.Len(t, projector.lastEvents, 1)
}

// Test Execute method - projector error
func TestExecute_ProjectorError(t *testing.T) {
	setup := newTestSetup(t, "test-execute-projector-error", 5)
	ctx := context.Background()
	metadata := newTestMetadata()

	projector := &MockProjector{projectError: errors.New("projector failed")}
	entity := &MockEntity{
		BaseEntity: evt.NewEntity(setup.entityID),
		projectors: []evt.EventProjector{projector},
	}
	command := &test.CreateEntity{Value: "test-value"}

	err := setup.store.Execute(ctx, entity, setup.entityID, command, metadata)
	require.Error(t, err)
	require.Contains(t, err.Error(), "projector failed")

	// Verify events were NOT committed
	events := getEvents(t, setup.repo, setup.entityID)
	require.Empty(t, events)
}
