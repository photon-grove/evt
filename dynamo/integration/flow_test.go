//go:build integration

package integration

import (
	"context"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *DynamoEventsIntegrationSuite) Test_Flow_ExecuteCycle() {
	ctx := context.Background()
	entityID := evt.EntityID(newID())
	s.SetupEntity(entityID, 100)
	metadata := s.getMetadata(ctx)

	// 1. First command (Create)
	cmd1 := &test.CreateEntity{Value: "v1"}
	err := s.store.Execute(ctx, s.entity, entityID, cmd1, metadata)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "v1", s.entity.Value)
	assert.NotEmpty(s.T(), s.entity.CreatedAt)
	assert.True(s.T(), s.entity.IsActive)

	// 2. Second command (Update)
	// Create a FRESH entity to simulate loading from scratch
	entity2 := test.NewEntity(entityID)
	cmd2 := &test.ReplaceEntity{Value: "v2"}

	err = s.store.Execute(ctx, entity2, entityID, cmd2, metadata)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "v2", entity2.Value)
	assert.Equal(s.T(), entityID, entity2.ID)

	// Verify state persistence by loading a third instance
	entity3 := test.NewEntity(entityID)
	_, err = s.store.LoadEntity(ctx, entity3, entityID)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "v2", entity3.Value)
}

func (s *DynamoEventsIntegrationSuite) Test_Flow_Snapshotting() {
	ctx := context.Background()
	entityID := evt.EntityID(newID())
	s.SetupEntity(entityID, 2) // Snapshot every 2 events
	metadata := s.getMetadata(ctx)

	// 1. Create (Seq 1)
	err := s.store.Execute(ctx, s.entity, entityID, &test.CreateEntity{Value: "init"}, metadata)
	require.NoError(s.T(), err)

	// 2. Update (Seq 2) -> Snapshot 1
	err = s.store.Execute(ctx, s.entity, entityID, &test.ReplaceEntity{Value: "snap1"}, metadata)
	require.NoError(s.T(), err)

	// Verify snapshot exists
	snap, err := s.repo.GetSnapshot(ctx, entityID)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), snap)
	assert.Equal(s.T(), evt.EventSequence(2), snap.EventSequence)

	// 3. Update (Seq 3)
	err = s.store.Execute(ctx, s.entity, entityID, &test.ReplaceEntity{Value: "after1"}, metadata)
	require.NoError(s.T(), err)

	// Load from scratch - should use snapshot + 1 event
	loaded := test.NewEntity(entityID)
	context, err := s.store.LoadEntity(ctx, loaded, entityID)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "after1", loaded.Value)
	assert.Equal(s.T(), evt.EventSequence(3), *context.CurrentSequence)
	assert.Equal(s.T(), evt.EventSequence(1), *context.CurrentSnapshot)
	// Note: CurrentSnapshot tracks snapshot SEQUENCE (1st snapshot), not event sequence.
}

func (s *DynamoEventsIntegrationSuite) Test_Flow_ConcurrentConflict() {
	ctx := context.Background()
	entityID := evt.EntityID(newID())
	s.SetupEntity(entityID, 100)
	metadata := s.getMetadata(ctx)

	// Create initial state
	err := s.store.Execute(ctx, s.entity, entityID, &test.CreateEntity{Value: "init"}, metadata)
	require.NoError(s.T(), err)

	// Load two separate instances (simulating two threads/processes)
	entityA := test.NewEntity(entityID)
	ctxA, err := s.store.LoadEntity(ctx, entityA, entityID)
	require.NoError(s.T(), err)

	entityB := test.NewEntity(entityID)
	ctxB, err := s.store.LoadEntity(ctx, entityB, entityID)
	require.NoError(s.T(), err)

	// A modifies
	resA, err := entityA.Handle(ctx, &test.ReplaceEntity{Value: "A"})
	require.NoError(s.T(), err)
	_, err = s.store.Commit(ctx, resA, ctxA, metadata)
	require.NoError(s.T(), err)

	// B modifies (using STALE context)
	resB, err := entityB.Handle(ctx, &test.ReplaceEntity{Value: "B"})
	require.NoError(s.T(), err)
	_, err = s.store.Commit(ctx, resB, ctxB, metadata)

	// Should fail
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "race condition")

	// Verify state is A
	final := test.NewEntity(entityID)
	_, err = s.store.LoadEntity(ctx, final, entityID)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "A", final.Value)
}

func (s *DynamoEventsIntegrationSuite) Test_Flow_TimeTravel() {
	// Verify we can load state at a point in time (using GetEvents/GetSnapshot manually if needed,
	// though Store doesn't support "LoadAt").
	// But we can verify sequence numbers.

	ctx := context.Background()
	entityID := evt.EntityID(newID())
	s.SetupEntity(entityID, 10)
	metadata := s.getMetadata(ctx)

	err := s.store.Execute(ctx, s.entity, entityID, &test.CreateEntity{Value: "1"}, metadata)
	require.NoError(s.T(), err)
	err = s.store.Execute(ctx, s.entity, entityID, &test.ReplaceEntity{Value: "2"}, metadata)
	require.NoError(s.T(), err)
	err = s.store.Execute(ctx, s.entity, entityID, &test.ReplaceEntity{Value: "3"}, metadata)
	require.NoError(s.T(), err)

	// Get events manually
	events, err := s.repo.GetEvents(ctx, entityID)
	require.NoError(s.T(), err)
	require.Len(s.T(), events, 3)

	assert.Equal(s.T(), evt.EventSequence(1), events[0].Sequence)
	assert.Equal(s.T(), evt.EventSequence(2), events[1].Sequence)
	assert.Equal(s.T(), evt.EventSequence(3), events[2].Sequence)
}
