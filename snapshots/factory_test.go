package snapshots_test

import (
	"context"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/snapshots"
	evttest "github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

func TestStoreExecuteWithFactory(t *testing.T) {
	repo := &MockRepository{}
	store := snapshots.NewStore(repo, 5)

	entity, err := store.ExecuteWithFactory(context.Background(), func() evt.Entity {
		return evttest.NewEntity("factory-id")
	}, "factory-id", &evttest.CreateEntity{Value: "factory-value"}, evt.Metadata{})
	require.NoError(t, err)
	testEntity, ok := entity.(*evttest.Entity)
	require.True(t, ok)
	require.Equal(t, "factory-value", testEntity.Value)
}
