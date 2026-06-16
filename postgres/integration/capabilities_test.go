//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/postgres"
	"github.com/photon-grove/evt/test"
)

// Test_Capabilities exercises the optional capabilities the conformance suite does not cover —
// Compactor, SnapshotStreamer, and EntityHeadStreamer/EntityHeadVisitor — against a real database.
// It uses a per-run namespace so it is safe on a table shared with the conformance suite.
func Test_Capabilities(t *testing.T) {
	ctx := context.Background()
	repo := postgres.NewRepository(newPool(ctx, t))

	ns := fmt.Sprintf("cap-%d", time.Now().UnixNano())
	entityType := evt.EntityType(ns)

	// Entity A: two events plus a durable snapshot covering through event 2.
	idA := evt.EntityID(ns + "-a")
	withSnapshot := serializeAs(t, idA, entityType, 0,
		&test.EntityCreated{ID: idA, Value: "one"},
		&test.EntityUpdated{ID: idA, Value: "two"},
	)
	payload := []byte(fmt.Sprintf(`{"id":%q,"value":"two"}`, idA))
	require.NoError(t, repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: withSnapshot}, entityType, idA, payload, 1))

	t.Run("HeadsReflectStoredEvents", func(t *testing.T) {
		heads, err := repo.StreamEntityHeads(ctx, entityType)
		require.NoError(t, err)
		require.Equal(t, evt.EventSequence(2), heads[idA])

		// The streaming visitor reports the same heads without materializing a map upstream.
		visited := make(map[evt.EntityID]evt.EventSequence)
		require.NoError(t, repo.StreamEntityHeadsFunc(ctx, entityType, func(id evt.EntityID, head evt.EventSequence) error {
			visited[id] = head
			return nil
		}))
		require.Equal(t, heads, visited)
	})

	t.Run("CompactBelowRequiresCoveringSnapshot", func(t *testing.T) {
		idB := evt.EntityID(ns + "-b")
		require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
			Events: serializeAs(t, idB, entityType, 0, &test.EntityCreated{ID: idB, Value: "b"}),
		}))

		// No snapshot covers entity B, so compaction is refused and nothing is deleted.
		deleted, err := repo.CompactBelow(ctx, idB, 1)
		require.ErrorIs(t, err, evt.ErrCompactionUncovered)
		require.Zero(t, deleted)

		events, err := repo.GetEvents(ctx, idB)
		require.NoError(t, err)
		require.Len(t, events, 1, "refused compaction must not delete events")
	})

	t.Run("CompactBelowTruncatesUnderSnapshot", func(t *testing.T) {
		deleted, err := repo.CompactBelow(ctx, idA, 2)
		require.NoError(t, err)
		require.Equal(t, 2, deleted)

		// Events are gone, but the durable snapshot remains the authoritative floor.
		events, err := repo.GetEvents(ctx, idA)
		require.NoError(t, err)
		require.Empty(t, events)

		snapshot, err := repo.GetSnapshot(ctx, idA)
		require.NoError(t, err)
		require.NotNil(t, snapshot)
		require.Equal(t, evt.EventSequence(2), snapshot.EventSequence)

		// The head still reports 2 from the snapshot even though every event row is gone.
		heads, err := repo.StreamEntityHeads(ctx, entityType)
		require.NoError(t, err)
		require.Equal(t, evt.EventSequence(2), heads[idA])
	})

	t.Run("StreamEntitiesFromSnapshotsSeedsCompactedStreams", func(t *testing.T) {
		seed := func(_ context.Context, snapshot evt.SerializedSnapshot) (evt.Entity, error) {
			return test.NewEntity(snapshot.EntityID), nil
		}

		var seen []evt.EntityID
		for r := range repo.StreamEntitiesFromSnapshots(ctx, entityType, seed, applyTestEvent) {
			entity, err := r.Unwrap()
			require.NoError(t, err)
			if entity != nil {
				seen = append(seen, entity.GetID())
			}
		}

		require.Contains(t, seen, idA, "a compacted stream must rebuild from its snapshot")
	})
}

// serializeAs serializes events for an entity and overrides the stored EntityType so a single Go
// event type can stand in for a namespaced logical type, isolating a run on a shared table.
func serializeAs(
	t *testing.T,
	id evt.EntityID,
	entityType evt.EntityType,
	fromSequence evt.EventSequence,
	events ...evt.Event,
) []evt.SerializedEvent {
	t.Helper()

	serialized, err := evt.SerializeEvents(events, fromSequence, id, evt.Metadata{})
	require.NoError(t, err)

	for i := range serialized {
		serialized[i].EntityType = entityType
	}

	return serialized
}

// applyTestEvent reconstitutes a test.Entity from serialized events, deserializing by event Type so
// it works regardless of the overridden EntityType.
func applyTestEvent(_ context.Context, se evt.SerializedEvent, current evt.Entity) (evt.Entity, error) {
	if current == nil {
		current = test.NewEntity(se.EntityID)
	}

	event, err := current.DeserializeEvent(se)
	if err != nil {
		return nil, err
	}

	if err := current.Apply(event); err != nil {
		return nil, err
	}

	return current, nil
}
