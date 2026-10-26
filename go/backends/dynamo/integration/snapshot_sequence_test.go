//go:build integration

package integration

import (
	"context"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/test"
	"github.com/stretchr/testify/require"
)

// Test_CommitWithSnapshot_FirstSnapshotMustBeSequenceOne verifies that a commit cannot write an
// entity's first snapshot with a sequence other than 1, and that a later snapshot must follow the
// stored one.
func (s *DynamoEventsIntegrationSuite) Test_CommitWithSnapshot_FirstSnapshotMustBeSequenceOne() {
	ctx := context.Background()
	id := evt.EntityID(newID())

	events := func(from evt.EventSequence) evt.SerializedResult {
		serialized, err := evt.SerializeEvents([]evt.Event{&test.AggregateCreated{ID: id, Value: "v"}}, from, id, evt.Metadata{})
		require.NoError(s.T(), err)

		return evt.SerializedResult{Events: serialized}
	}

	require.Error(s.T(), s.repo.CommitWithSnapshot(ctx, events(0), test.EntityType, id, []byte(`{}`), 2))

	stored, err := s.repo.GetEvents(ctx, id)
	require.NoError(s.T(), err)
	require.Empty(s.T(), stored, "the rejected commit writes nothing")

	require.NoError(s.T(), s.repo.CommitWithSnapshot(ctx, events(0), test.EntityType, id, []byte(`{}`), 1))
	require.Error(s.T(), s.repo.CommitWithSnapshot(ctx, events(1), test.EntityType, id, []byte(`{}`), 3))
	require.NoError(s.T(), s.repo.CommitWithSnapshot(ctx, events(1), test.EntityType, id, []byte(`{}`), 2))
}
