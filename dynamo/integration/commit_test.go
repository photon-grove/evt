//go:build integration

package integration

import (
	"context"
	"encoding/json"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

func (s *DynamoEventsIntegrationSuite) Test_Commit_Success() {
	ctx := context.Background()

	s.SetupEntity(evt.EntityID(newID()), 2)

	metadata := s.getMetadata(ctx)
	otherValue := "test-other-value"

	result, err := s.entity.Handle(ctx, &test.CreateEntity{
		Value: "test-value",
		Other: &otherValue,
	})
	require.NoError(s.T(), err)

	type expectedPayload struct {
		ID    string  `json:"id"`
		Value string  `json:"value"`
		Other *string `json:"other,omitempty"`
	}

	payload, err := json.Marshal(expectedPayload{
		ID:    string(s.entityID),
		Value: "test-value",
		Other: &otherValue,
	})
	require.NoError(s.T(), err)

	expected := []evt.SerializedEvent{
		{
			ID:         evt.GetEventID(s.entityID, 1),
			Type:       test.CreatedEvent,
			Version:    1,
			EntityType: s.entityType,
			EntityID:   s.entityID,
			Sequence:   1,
			Payload:    payload,
			Metadata:   metadata,
		},
	}

	_, err = s.store.Commit(ctx, result, s.eventContext, metadata)
	require.NoError(s.T(), err)

	committedEvents, err := s.repo.GetEvents(ctx, s.entityID)
	require.NoError(s.T(), err)

	require.Equal(s.T(), expected, committedEvents)
}

func (s *DynamoEventsIntegrationSuite) Test_Commit_Empty() {
	ctx := context.Background()

	s.SetupEntity(evt.EntityID(newID()), 2)

	metadata := s.getMetadata(ctx)
	resultEvents := make([]evt.Event, 0)

	result := evt.CommandResult{Events: resultEvents}

	_, err := s.store.Commit(ctx, result, s.eventContext, metadata)
	require.NoError(s.T(), err)

	committedEvents, err := s.repo.GetEvents(ctx, s.entityID)
	require.NoError(s.T(), err)

	require.Empty(s.T(), committedEvents)
}
