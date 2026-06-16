package snapshots

import (
	"context"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
	do "github.com/samber/do/v2"
	"github.com/stretchr/testify/require"
)

type stubRepo struct{}

func (stubRepo) Commit(context.Context, evt.SerializedResult) error { return nil }
func (stubRepo) CommitStream(context.Context, <-chan result.Result[evt.SerializedResult]) []error {
	return nil
}
func (stubRepo) CommitWithSnapshot(
	context.Context,
	evt.SerializedResult,
	evt.EntityType,
	evt.EntityID,
	[]byte,
	evt.EventSequence,
) error {
	return nil
}
func (stubRepo) GetEvents(context.Context, evt.EntityID) ([]evt.SerializedEvent, error) {
	return nil, nil
}
func (stubRepo) GetLatestEvents(context.Context, evt.EntityID, evt.EventSequence) ([]evt.SerializedEvent, error) {
	return nil, nil
}
func (stubRepo) GetSnapshot(context.Context, evt.EntityID) (*evt.SerializedSnapshot, error) {
	return nil, nil
}
func (stubRepo) StreamAllEvents(context.Context, evt.StreamFilter) <-chan result.Result[[]evt.SerializedEvent] {
	ch := make(chan result.Result[[]evt.SerializedEvent])
	close(ch)
	return ch
}
func (stubRepo) StreamEntities(context.Context, evt.StreamFilter, func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error)) <-chan result.Result[evt.Entity] {
	ch := make(chan result.Result[evt.Entity])
	close(ch)
	return ch
}

func Test_ProvideStore_Success(t *testing.T) {
	i := do.New()
	do.ProvideValue[evt.Repository](i, stubRepo{})

	store, err := ProvideStore(i)
	require.NoError(t, err)
	require.NotNil(t, store)

	// Ensure we actually got a snapshots.Store back.
	_, ok := store.(*Store)
	require.True(t, ok)
}

func Test_ProvideStore_MissingRepository(t *testing.T) {
	i := do.New()

	store, err := ProvideStore(i)
	require.Error(t, err)
	require.Nil(t, store)
}
