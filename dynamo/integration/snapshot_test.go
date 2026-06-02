//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"time"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

func (s *DynamoEventsIntegrationSuite) Test_Commit_Snapshot() {
	ctx := context.Background()

	s.SetupEntity(evt.EntityID(newID()), 2)

	metadata := s.getMetadata(ctx)

	updatedValue := "updated-value"
	intermediateOther := "intermediate-other-value"
	updatedOther := "updated-other-value"
	otherValue := "test-other-value"

	result, err := s.entity.Handle(ctx, &test.CreateEntity{
		Value: "test-value",
		Other: &otherValue,
	})
	require.NoError(s.T(), err)

	updateValueResult, err := s.entity.Handle(ctx, &test.ReplaceEntity{
		Value: updatedValue,
		Other: &otherValue,
	})
	require.NoError(s.T(), err)

	result.Events = append(result.Events, updateValueResult.Events...)
	result.Transaction = append(result.Transaction, updateValueResult.Transaction...)

	// Set "other" to an intermediate value
	updateOtherResult, err := s.entity.Handle(ctx, &test.ReplaceEntity{
		Value: updatedValue,
		Other: &intermediateOther,
	})
	require.NoError(s.T(), err)

	result.Events = append(result.Events, updateOtherResult.Events...)
	result.Transaction = append(result.Transaction, updateOtherResult.Transaction...)

	_, err = s.store.Commit(ctx, result, s.eventContext, metadata)
	require.NoError(s.T(), err)

	// Retrieve the initial snapshot
	snapshot, err := s.repo.GetSnapshot(ctx, s.entityID)
	require.NoError(s.T(), err)

	// The sequence should match the last event captured in the snapshot, and the snapshot
	// number itself should be 1, because it was the first taken.
	require.Equal(s.T(), s.entityType, snapshot.EntityType)
	require.Equal(s.T(), s.entityID, snapshot.EntityID)
	require.Equal(s.T(), evt.EventSequence(1), snapshot.Sequence)
	require.Equal(s.T(), evt.EventSequence(3), snapshot.EventSequence)

	// Unmarshal snapshot payload and assert semantics, not exact timestamps
	var snap1 struct {
		ID        string    `json:"id"`
		IsActive  bool      `json:"isActive"`
		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`
		Value     string    `json:"value"`
		Other     *string   `json:"other"`
	}
	require.NoError(s.T(), json.Unmarshal(snapshot.Payload, &snap1))
	require.Equal(s.T(), string(s.entityID), snap1.ID)
	require.True(s.T(), snap1.IsActive)
	require.False(s.T(), snap1.CreatedAt.IsZero())
	require.False(s.T(), snap1.UpdatedAt.IsZero())
	require.Equal(s.T(), updatedValue, snap1.Value)
	require.Equal(s.T(), intermediateOther, *snap1.Other)

	// Set "other" to the intermediate value again
	newResult, err := s.entity.Handle(ctx, &test.ReplaceEntity{
		Value: updatedValue,
		Other: &intermediateOther,
	})
	require.NoError(s.T(), err)

	// Set "other" to the final expected value
	newOtherResult, err := s.entity.Handle(ctx, &test.ReplaceEntity{
		Value: updatedValue,
		Other: &updatedOther,
	})
	require.NoError(s.T(), err)

	newResult.Events = append(newResult.Events, newOtherResult.Events...)
	newResult.Transaction = append(newResult.Transaction, newOtherResult.Transaction...)

	_, err = s.store.Commit(ctx, newResult, s.eventContext, metadata)
	require.NoError(s.T(), err)

	// Retrieve the updated snapshot
	snapshot, err = s.repo.GetSnapshot(ctx, s.entityID)
	require.NoError(s.T(), err)

	// The sequence should increase to 5, which is the event with the final expected value
	// for the "other" field. The snapshot itself should be 2, because it was the second taken.
	require.Equal(s.T(), s.entityType, snapshot.EntityType)
	require.Equal(s.T(), s.entityID, snapshot.EntityID)
	require.Equal(s.T(), evt.EventSequence(2), snapshot.Sequence)
	require.Equal(s.T(), evt.EventSequence(5), snapshot.EventSequence)

	// Unmarshal snapshot payload and assert semantics, not exact timestamps
	var snap2 struct {
		ID        string    `json:"id"`
		IsActive  bool      `json:"isActive"`
		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`
		Value     string    `json:"value"`
		Other     *string   `json:"other"`
	}
	require.NoError(s.T(), json.Unmarshal(snapshot.Payload, &snap2))
	require.Equal(s.T(), string(s.entityID), snap2.ID)
	require.True(s.T(), snap2.IsActive)
	require.False(s.T(), snap2.CreatedAt.IsZero())
	require.False(s.T(), snap2.UpdatedAt.IsZero())
	require.Equal(s.T(), updatedValue, snap2.Value)
	require.Equal(s.T(), updatedOther, *snap2.Other)

	committedSerializedEvents, err := s.repo.GetEvents(ctx, s.entityID)
	require.NoError(s.T(), err)

	// Both the initial and the subsequent events should be captured in the repository
	expected := append(result.Events, newResult.Events...)

	// Convert the serialized committed events to domain events
	committedEvents := make([]evt.Event, 0, len(committedSerializedEvents))
	for _, serializedEvent := range committedSerializedEvents {
		event, err := evt.DeserializeEvent(serializedEvent, s.eventContext.Entity)
		require.NoError(s.T(), err)

		committedEvents = append(committedEvents, event)
	}

	// Check to make sure they were all captured
	require.Equal(s.T(), expected, committedEvents)
}

func (s *DynamoEventsIntegrationSuite) Test_Load_WithSnapshot() {
	ctx := context.Background()

	s.SetupEntity(evt.EntityID(newID()), 2)

	metadata := s.getMetadata(ctx)

	updatedValue := "updated-value"
	intermediateOther := "intermediate-other-value"
	updatedOther := "updated-other-value"
	otherValue := "test-other-value"

	result, err := s.entity.Handle(ctx, &test.CreateEntity{
		Value: "test-value",
		Other: &otherValue,
	})
	require.NoError(s.T(), err)

	updateValueResult, err := s.entity.Handle(ctx, &test.ReplaceEntity{
		Value: updatedValue,
		Other: &otherValue,
	})
	require.NoError(s.T(), err)

	result.Events = append(result.Events, updateValueResult.Events...)
	result.Transaction = append(result.Transaction, updateValueResult.Transaction...)

	// Set "other" to an intermediate value
	updateOtherResult, err := s.entity.Handle(ctx, &test.ReplaceEntity{
		Value: updatedValue,
		Other: &intermediateOther,
	})
	require.NoError(s.T(), err)

	result.Events = append(result.Events, updateOtherResult.Events...)
	result.Transaction = append(result.Transaction, updateOtherResult.Transaction...)

	_, err = s.store.Commit(ctx, result, s.eventContext, metadata)
	require.NoError(s.T(), err)

	// Set "other" to the final expected value
	newResult, err := s.entity.Handle(ctx, &test.ReplaceEntity{
		Value: updatedValue,
		Other: &updatedOther,
	})
	require.NoError(s.T(), err)

	// This sequence of events should leave 1 trailing event after the last snapshot
	_, err = s.store.Commit(ctx, newResult, s.eventContext, metadata)
	require.NoError(s.T(), err)

	// Load the entity again to retrieve events from memory
	s.SetupEntity(s.entityID, 2)

	// The hydrated entity should have the updated values
	require.EqualValues(s.T(), test.Entity{
		BaseEntity: s.entity.BaseEntity,
		Value:      "updated-value",
		Other:      &updatedOther,
	}, *s.entity)
}
