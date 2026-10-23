package snapshots_test

import (
	"context"
	"errors"
	"testing"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/mem"
	"github.com/photon-grove/evt/go/snapshots"
	"github.com/photon-grove/evt/go/test"
	"github.com/stretchr/testify/require"
)

// Test Execute method - projector execution
func TestExecute_ProjectorCalled(t *testing.T) {
	setup := newTestSetup(t, "test-execute-projector", 5)
	ctx := context.Background()
	metadata := newTestMetadata()

	projector := &MockProjector{}
	entity := &MockEntity{
		ID:         setup.entityID,
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
		ID:         setup.entityID,
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

// txGroup is a projector transaction group of one operation.
type txGroup struct{}

func (txGroup) Len() int                             { return 1 }
func (txGroup) StorageType() evt.StorageType         { return "test" }
func (txGroup) TransactionType() evt.TransactionType { return "test" }
func (txGroup) HandleError(err error, _ int) error   { return err }
func (g txGroup) Merge(evt.TransactionGroup) (evt.TransactionGroup, error) {
	return g, nil
}

// groupProjector returns a real transaction group for every event it sees.
type groupProjector struct{}

func (groupProjector) Project(context.Context, evt.Entity, []evt.Event) (evt.TransactionGroup, error) {
	return txGroup{}, nil
}

// recordingRepo records each committed result and does not write it, so a projector group that the
// backend would refuse still shows what the store handed to the commit.
type recordingRepo struct {
	evt.Repository
	committed []evt.SerializedResult
}

func (r *recordingRepo) Commit(_ context.Context, result evt.SerializedResult) error {
	r.committed = append(r.committed, result)
	return nil
}

func (r *recordingRepo) CommitWithSnapshot(
	_ context.Context, result evt.SerializedResult, _ evt.EntityType, _ evt.EntityID, _ []byte, _ evt.EventSequence,
) error {
	r.committed = append(r.committed, result)
	return nil
}

// An in-band projector's transaction group travels with the events in the same commit, on both the
// plain commit path and the path that also writes a snapshot.
func TestExecute_ProjectorGroupIsCommittedWithTheEvents(t *testing.T) {
	for name, snapshotSize := range map[string]int{"plain commit": 25, "commit with snapshot": 1} {
		t.Run(name, func(t *testing.T) {
			repo := &recordingRepo{Repository: mem.NewRepository()}
			store := snapshots.NewStore(repo, snapshotSize)
			id := evt.EntityID("group-1")
			entity := &MockEntity{ID: id, projectors: []evt.EventProjector{groupProjector{}}}

			require.NoError(t, store.Execute(context.Background(), entity, id, &test.CreateEntity{Value: "v"}, newTestMetadata()))

			require.Len(t, repo.committed, 1)
			require.Len(t, repo.committed[0].Events, 1)
			require.Equal(t, evt.Transaction{txGroup{}}, repo.committed[0].Transaction)
		})
	}
}
