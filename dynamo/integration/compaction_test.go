//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/snapshots"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

// seedIntegrationEntity reconstructs a test.Entity from a durable snapshot payload.
func seedIntegrationEntity(_ context.Context, snapshot evt.SerializedSnapshot) (evt.Entity, error) {
	entity := test.NewEntity(snapshot.EntityID)
	if err := json.Unmarshal(snapshot.Payload, entity); err != nil {
		return nil, err
	}

	return entity, nil
}

// applyIntegrationEvent deserializes and applies one serialized event.
func applyIntegrationEvent(_ context.Context, serialized evt.SerializedEvent, entity evt.Entity) (evt.Entity, error) {
	if entity == nil {
		entity = test.NewEntity(serialized.EntityID)
	}

	event, err := entity.DeserializeEvent(serialized)
	if err != nil {
		return nil, err
	}
	if err := entity.Apply(event); err != nil {
		return nil, err
	}

	return entity, nil
}

func (s *DynamoEventsIntegrationSuite) sequencesFor(ctx context.Context, id evt.EntityID) []evt.EventSequence {
	events, err := s.repo.GetEvents(ctx, id)
	require.NoError(s.T(), err)

	seqs := make([]evt.EventSequence, 0, len(events))
	for _, e := range events {
		seqs = append(seqs, e.Sequence)
	}

	return seqs
}

// Test_CompactBelow_RebuildAfterCompaction commits a stream with snapshots, compacts the events
// below the latest snapshot, and verifies that both snapshot-aware load and snapshot-aware
// rebuild still reproduce the correct final state.
func (s *DynamoEventsIntegrationSuite) Test_CompactBelow_RebuildAfterCompaction() {
	ctx := context.Background()

	id := evt.EntityID(newID())
	s.SetupEntity(id, 2) // snapshot every 2 events
	metadata := s.getMetadata(ctx)

	require.NoError(s.T(), s.store.Execute(ctx, s.entity, id, &test.CreateEntity{Value: "v1"}, metadata))
	for i := 2; i <= 7; i++ {
		require.NoError(s.T(), s.store.Execute(ctx, s.entity, id, &test.ReplaceEntity{Value: "v" + strconv.Itoa(i)}, metadata))
	}

	snapshot, err := s.repo.GetSnapshot(ctx, id)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), snapshot)
	require.Equal(s.T(), evt.EventSequence(6), snapshot.EventSequence)

	// Compact everything captured by the snapshot (events 1..6); the tail (7) survives.
	deleted, err := s.repo.CompactBelow(ctx, id, snapshot.EventSequence)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 6, deleted)
	require.Equal(s.T(), []evt.EventSequence{7}, s.sequencesFor(ctx, id))

	// Snapshot-aware load reconstructs v7 from snapshot(v6) + event 7.
	loaded := test.NewEntity(id)
	_, err = s.store.LoadEntity(ctx, loaded, id)
	require.NoError(s.T(), err)
	require.Equal(s.T(), "v7", loaded.Value)

	// Snapshot-aware rebuild reproduces the same state from snapshot + tail.
	var rebuilt evt.Entity
	count := 0
	for res := range s.repo.StreamEntitiesFromSnapshots(ctx, "", seedIntegrationEntity, applyIntegrationEvent) {
		entity, uerr := res.Unwrap()
		require.NoError(s.T(), uerr)
		if entity.GetID() == id {
			rebuilt = entity
			count++
		}
	}
	require.Equal(s.T(), 1, count)
	rebuiltEntity, ok := rebuilt.(*test.Entity)
	require.True(s.T(), ok)
	require.Equal(s.T(), "v7", rebuiltEntity.Value)
}

// Test_CompactBelow_RefusesWithoutCoveringSnapshot verifies the safety guard against an
// emulator-backed table: compacting past the snapshot boundary deletes nothing.
func (s *DynamoEventsIntegrationSuite) Test_CompactBelow_RefusesWithoutCoveringSnapshot() {
	ctx := context.Background()

	id := evt.EntityID(newID())
	s.SetupEntity(id, 2)
	metadata := s.getMetadata(ctx)

	require.NoError(s.T(), s.store.Execute(ctx, s.entity, id, &test.CreateEntity{Value: "v1"}, metadata))
	require.NoError(s.T(), s.store.Execute(ctx, s.entity, id, &test.ReplaceEntity{Value: "v2"}, metadata))
	require.NoError(s.T(), s.store.Execute(ctx, s.entity, id, &test.ReplaceEntity{Value: "v3"}, metadata))

	snapshot, err := s.repo.GetSnapshot(ctx, id)
	require.NoError(s.T(), err)
	require.Equal(s.T(), evt.EventSequence(2), snapshot.EventSequence)

	// Requesting compaction past the covered range is refused; events remain intact.
	deleted, err := s.repo.CompactBelow(ctx, id, 3)
	require.ErrorIs(s.T(), err, evt.ErrCompactionUncovered)
	require.Equal(s.T(), 0, deleted)
	require.Equal(s.T(), []evt.EventSequence{1, 2, 3}, s.sequencesFor(ctx, id))
}

// Test_CompactBelow_ConcurrentWithAppends runs compaction concurrently with new commits and
// verifies the stream stays loadable and correct. Compaction only removes already-snapshotted
// low events, which concurrent appenders never touch.
func (s *DynamoEventsIntegrationSuite) Test_CompactBelow_ConcurrentWithAppends() {
	ctx := context.Background()

	id := evt.EntityID(newID())
	s.SetupEntity(id, 2)
	metadata := s.getMetadata(ctx)

	require.NoError(s.T(), s.store.Execute(ctx, s.entity, id, &test.CreateEntity{Value: "v1"}, metadata))
	for i := 2; i <= 6; i++ {
		require.NoError(s.T(), s.store.Execute(ctx, s.entity, id, &test.ReplaceEntity{Value: "v" + strconv.Itoa(i)}, metadata))
	}

	snapshot, err := s.repo.GetSnapshot(ctx, id)
	require.NoError(s.T(), err)
	coverThrough := snapshot.EventSequence // 6

	// A dedicated entity/store handle for the appender so the two goroutines do not share a
	// mutable entity instance.
	appendStore := snapshots.NewStore(s.repo, 2)
	appendEntity := test.NewEntity(id)

	var wg sync.WaitGroup
	wg.Add(2)

	var compactErr error
	var deleted int
	go func() {
		defer wg.Done()
		deleted, compactErr = s.repo.CompactBelow(ctx, id, coverThrough)
	}()

	var appendErr error
	go func() {
		defer wg.Done()
		for i := 7; i <= 9; i++ {
			if execErr := appendStore.Execute(ctx, appendEntity, id, &test.ReplaceEntity{Value: "v" + strconv.Itoa(i)}, metadata); execErr != nil {
				appendErr = execErr
				return
			}
		}
	}()

	wg.Wait()
	require.NoError(s.T(), compactErr)
	require.NoError(s.T(), appendErr)
	require.LessOrEqual(s.T(), deleted, 6)

	// The compacted range is gone and the tail (events > 6) is intact.
	for _, seq := range s.sequencesFor(ctx, id) {
		require.Greater(s.T(), int(seq), int(coverThrough))
	}

	// The stream still loads to the final appended value.
	loaded := test.NewEntity(id)
	_, err = s.store.LoadEntity(ctx, loaded, id)
	require.NoError(s.T(), err)
	require.Equal(s.T(), "v9", loaded.Value)
}
