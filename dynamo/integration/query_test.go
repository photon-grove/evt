//go:build integration

package integration

import (
	"context"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *DynamoEventsIntegrationSuite) Test_Query_GetEvents_NoEvents() {
	ctx := context.Background()
	// Use an entity ID that doesn't exist
	entityID := evt.EntityID(newID())

	events, err := s.repo.GetEvents(ctx, entityID)
	require.NoError(s.T(), err)
	assert.Empty(s.T(), events)
}

func (s *DynamoEventsIntegrationSuite) Test_Query_GetLatestEvents_Pagination() {
	ctx := context.Background()
	entityID := evt.EntityID(newID())
	s.SetupEntity(entityID, 100) // Large snapshot size so we get many events
	metadata := s.getMetadata(ctx)

	// Generate enough events to trigger pagination if page size is small.
	// DynamoDB Query page size limit is 1MB. We won't hit that easily.
	// But the Paginator handles it.
	// We can't force pagination without mocking or sending LOTS of data.
	// We'll trust the SDK paginator, but verify we get ALL events.

	numEvents := 10
	for i := 0; i < numEvents; i++ {
		err := s.store.Execute(ctx, s.entity, entityID, &test.CreateEntity{Value: "v"}, metadata)
		require.NoError(s.T(), err)
	}

	// Fetch all events
	events, err := s.repo.GetEvents(ctx, entityID)
	require.NoError(s.T(), err)
	assert.Len(s.T(), events, numEvents)

	// Fetch latest events after seq 5
	latest, err := s.repo.GetLatestEvents(ctx, entityID, 5)
	require.NoError(s.T(), err)
	assert.Len(s.T(), latest, 5) // 6, 7, 8, 9, 10
	assert.Equal(s.T(), evt.EventSequence(6), latest[0].Sequence)
}

func (s *DynamoEventsIntegrationSuite) Test_Query_GetSnapshot_NoneExists() {
	ctx := context.Background()
	entityID := evt.EntityID(newID())

	snap, err := s.repo.GetSnapshot(ctx, entityID)
	require.NoError(s.T(), err)
	assert.Nil(s.T(), snap)
}

func (s *DynamoEventsIntegrationSuite) Test_Query_StreamEntities() {
	ctx := context.Background()
	metadata := s.getMetadata(ctx)

	// Create two distinct entities
	id1 := evt.EntityID(newID())
	err := s.store.Execute(ctx, test.NewEntity(id1), id1, &test.CreateEntity{Value: "s1"}, metadata)
	require.NoError(s.T(), err)

	id2 := evt.EntityID(newID())
	err = s.store.Execute(ctx, test.NewEntity(id2), id2, &test.CreateEntity{Value: "s2"}, metadata)
	require.NoError(s.T(), err)

	// Stream all entities
	// Note: StreamEntities scans the table.
	// Since we are running in a shared local environment, there might be other entities from other tests.
	// We should just check that OUR entities are present in the stream.

	applier := func(_ context.Context, se evt.SerializedEvent, e evt.Entity) (evt.Entity, error) {
		if e == nil {
			// Create new instance of correct type
			e = test.NewEntity(se.EntityID)
		}
		event, err := e.DeserializeEvent(se)
		if err != nil {
			return nil, err
		}
		if err := e.Apply(event); err != nil {
			return nil, err
		}
		return e, nil
	}

	stream := s.repo.StreamEntities(ctx, nil, applier)

	found1 := false
	found2 := false

	// Consume stream
	for result := range stream {
		entity, err := result.Unwrap()
		if err != nil {
			// Ignore errors from other random data if any
			continue
		}
		if entity.GetID() == id1 {
			found1 = true
			testEntity, ok := entity.(*test.Entity)
			require.True(s.T(), ok)
			assert.Equal(s.T(), "s1", testEntity.Value)
		}
		if entity.GetID() == id2 {
			found2 = true
			testEntity, ok := entity.(*test.Entity)
			require.True(s.T(), ok)
			assert.Equal(s.T(), "s2", testEntity.Value)
		}
	}

	assert.True(s.T(), found1, "Entity 1 not found in stream")
	assert.True(s.T(), found2, "Entity 2 not found in stream")
}
