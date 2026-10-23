//go:build integration

package integration

import (
	"context"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *DynamoEventsIntegrationSuite) Test_Flow_ExecuteCycle() {
	ctx := context.Background()
	entityID := evt.EntityID(newID())
	s.SetupAggregate(entityID, 100)
	metadata := s.getMetadata(ctx)

	// 1. First command (Create)
	cmd1 := &test.CreateAggregate{Value: "v1"}
	err := s.store.Execute(ctx, s.aggregate, entityID, cmd1, metadata)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "v1", s.aggregate.Value)
	assert.Equal(s.T(), entityID, s.aggregate.ID)

	// 2. Second command (Update)
	// Create a FRESH entity to simulate loading from scratch
	aggregate2 := test.NewAggregate(entityID)
	cmd2 := &test.ReplaceAggregate{Value: "v2"}

	err = s.store.Execute(ctx, aggregate2, entityID, cmd2, metadata)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "v2", aggregate2.Value)
	assert.Equal(s.T(), entityID, aggregate2.ID)

	// Verify state persistence by loading a third instance
	aggregate3 := test.NewAggregate(entityID)
	_, err = s.store.LoadAggregate(ctx, aggregate3, entityID)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "v2", aggregate3.Value)
}

func (s *DynamoEventsIntegrationSuite) Test_Flow_Snapshotting() {
	ctx := context.Background()
	entityID := evt.EntityID(newID())
	s.SetupAggregate(entityID, 2) // Snapshot every 2 events
	metadata := s.getMetadata(ctx)

	// 1. Create (Seq 1)
	err := s.store.Execute(ctx, s.aggregate, entityID, &test.CreateAggregate{Value: "init"}, metadata)
	require.NoError(s.T(), err)

	// 2. Update (Seq 2) -> Snapshot 1
	err = s.store.Execute(ctx, s.aggregate, entityID, &test.ReplaceAggregate{Value: "snap1"}, metadata)
	require.NoError(s.T(), err)

	// Verify snapshot exists
	snap, err := s.repo.GetSnapshot(ctx, entityID)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), snap)
	assert.Equal(s.T(), evt.EventSequence(2), snap.EventSequence)

	// 3. Update (Seq 3)
	err = s.store.Execute(ctx, s.aggregate, entityID, &test.ReplaceAggregate{Value: "after1"}, metadata)
	require.NoError(s.T(), err)

	// Load from scratch - should use snapshot + 1 event
	loaded := test.NewAggregate(entityID)
	context, err := s.store.LoadAggregate(ctx, loaded, entityID)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "after1", loaded.Value)
	assert.Equal(s.T(), evt.EventSequence(3), *context.CurrentSequence)
	assert.Equal(s.T(), evt.EventSequence(1), *context.CurrentSnapshot)
	// Note: CurrentSnapshot tracks snapshot SEQUENCE (1st snapshot), not event sequence.
}

func (s *DynamoEventsIntegrationSuite) Test_Flow_ConcurrentConflict() {
	ctx := context.Background()
	entityID := evt.EntityID(newID())
	s.SetupAggregate(entityID, 100)
	metadata := s.getMetadata(ctx)

	// Create initial state
	err := s.store.Execute(ctx, s.aggregate, entityID, &test.CreateAggregate{Value: "init"}, metadata)
	require.NoError(s.T(), err)

	// Load two separate instances (simulating two threads/processes)
	aggregateA := test.NewAggregate(entityID)
	ctxA, err := s.store.LoadAggregate(ctx, aggregateA, entityID)
	require.NoError(s.T(), err)

	aggregateB := test.NewAggregate(entityID)
	ctxB, err := s.store.LoadAggregate(ctx, aggregateB, entityID)
	require.NoError(s.T(), err)

	// A modifies
	resA, err := aggregateA.Handle(ctx, &test.ReplaceAggregate{Value: "A"})
	require.NoError(s.T(), err)
	_, err = s.store.Commit(ctx, resA, ctxA, metadata)
	require.NoError(s.T(), err)

	// B modifies (using STALE context)
	resB, err := aggregateB.Handle(ctx, &test.ReplaceAggregate{Value: "B"})
	require.NoError(s.T(), err)
	_, err = s.store.Commit(ctx, resB, ctxB, metadata)

	// Should fail
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "race condition")

	// Verify state is A
	final := test.NewAggregate(entityID)
	_, err = s.store.LoadAggregate(ctx, final, entityID)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "A", final.Value)
}

func (s *DynamoEventsIntegrationSuite) Test_Flow_TimeTravel() {
	// Verify we can load state at a point in time (using GetEvents/GetSnapshot manually if needed,
	// though Store doesn't support "LoadAt").
	// But we can verify sequence numbers.

	ctx := context.Background()
	entityID := evt.EntityID(newID())
	s.SetupAggregate(entityID, 10)
	metadata := s.getMetadata(ctx)

	err := s.store.Execute(ctx, s.aggregate, entityID, &test.CreateAggregate{Value: "1"}, metadata)
	require.NoError(s.T(), err)
	err = s.store.Execute(ctx, s.aggregate, entityID, &test.ReplaceAggregate{Value: "2"}, metadata)
	require.NoError(s.T(), err)
	err = s.store.Execute(ctx, s.aggregate, entityID, &test.ReplaceAggregate{Value: "3"}, metadata)
	require.NoError(s.T(), err)

	// Get events manually
	events, err := s.repo.GetEvents(ctx, entityID)
	require.NoError(s.T(), err)
	require.Len(s.T(), events, 3)

	assert.Equal(s.T(), evt.EventSequence(1), events[0].Sequence)
	assert.Equal(s.T(), evt.EventSequence(2), events[1].Sequence)
	assert.Equal(s.T(), evt.EventSequence(3), events[2].Sequence)
}
