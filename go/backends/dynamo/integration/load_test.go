//go:build integration

package integration

import (
	"context"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/test"
	"github.com/stretchr/testify/require"
)

func (s *DynamoEventsIntegrationSuite) Test_Load_WithEvents() {
	ctx := context.Background()

	s.SetupAggregate(evt.EntityID(newID()), 5)

	metadata := s.getMetadata(ctx)

	updatedValue := "updated-value"
	otherValue := "test-other-value"

	// Add with an initial value
	result, err := s.aggregate.Handle(ctx, &test.CreateAggregate{
		Value: "test-value",
		Other: &otherValue,
	})
	require.NoError(s.T(), err)

	// Then update the value
	updateResult, err := s.aggregate.Handle(ctx, &test.ReplaceAggregate{
		Value: updatedValue,
		Other: &otherValue,
	})
	require.NoError(s.T(), err)

	result.Events = append(result.Events, updateResult.Events...)

	_, err = s.store.Commit(ctx, result, s.cc, metadata)
	require.NoError(s.T(), err)

	// Load the entity again to retrieve events from memory
	s.SetupAggregate(s.entityID, 5)

	// The hydrated entity should have the updated value
	require.EqualValues(s.T(), test.Aggregate{
		ID:    s.aggregate.ID,
		Value: "updated-value",
		Other: &otherValue,
	}, *s.aggregate)
}
