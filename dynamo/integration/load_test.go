//go:build integration

package integration

import (
	"context"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

func (s *DynamoEventsIntegrationSuite) Test_Load_WithEvents() {
	ctx := context.Background()

	s.SetupEntity(evt.EntityID(newID()), 5)

	metadata := s.getMetadata(ctx)

	updatedValue := "updated-value"
	otherValue := "test-other-value"

	// Add with an initial value
	result, err := s.entity.Handle(ctx, &test.CreateEntity{
		Value: "test-value",
		Other: &otherValue,
	})
	require.NoError(s.T(), err)

	// Then update the value
	updateResult, err := s.entity.Handle(ctx, &test.ReplaceEntity{
		Value: updatedValue,
		Other: &otherValue,
	})
	require.NoError(s.T(), err)

	result.Events = append(result.Events, updateResult.Events...)
	result.Transaction = append(result.Transaction, updateResult.Transaction...)

	_, err = s.store.Commit(ctx, result, s.eventContext, metadata)
	require.NoError(s.T(), err)

	// Load the entity again to retrieve events from memory
	s.SetupEntity(s.entityID, 5)

	// The hydrated entity should have the updated value
	require.EqualValues(s.T(), test.Entity{
		BaseEntity: s.entity.BaseEntity,
		Value:      "updated-value",
		Other:      &otherValue,
	}, *s.entity)
}
