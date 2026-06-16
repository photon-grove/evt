package mem

import (
	"context"
	"fmt"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/stretchr/testify/require"
)

func headEvent(entityID evt.EntityID, entityType evt.EntityType, seq evt.EventSequence) evt.SerializedEvent {
	return evt.SerializedEvent{
		ID:         evt.EventID(fmt.Sprintf("%s:%d", entityID, seq)),
		Sequence:   seq,
		Type:       "Tested",
		EntityID:   entityID,
		EntityType: entityType,
		Payload:    []byte("{}"),
	}
}

func commitEvents(t *testing.T, repo evt.Repository, events ...evt.SerializedEvent) {
	t.Helper()
	require.NoError(t, repo.Commit(context.Background(), evt.SerializedResult{Events: events}))
}

func asHeadStreamer(t *testing.T, repo evt.Repository) evt.EntityHeadStreamer {
	t.Helper()
	hs, ok := repo.(evt.EntityHeadStreamer)
	require.True(t, ok)

	return hs
}

func TestStreamEntityHeads_MaxSequencePerEntity(t *testing.T) {
	repo := NewRepository()

	commitEvents(t, repo,
		headEvent("widget-1", "widget", 1),
		headEvent("widget-1", "widget", 2),
		headEvent("widget-1", "widget", 3),
	)
	commitEvents(t, repo, headEvent("widget-2", "widget", 1))

	heads, err := asHeadStreamer(t, repo).StreamEntityHeads(context.Background(), "")
	require.NoError(t, err)
	require.Equal(t, map[evt.EntityID]evt.EventSequence{
		"widget-1": 3,
		"widget-2": 1,
	}, heads)
}

func TestStreamEntityHeads_FiltersByType(t *testing.T) {
	repo := NewRepository()

	commitEvents(t, repo, headEvent("widget-1", "widget", 2))
	commitEvents(t, repo, headEvent("gadget-1", "gadget", 5))

	heads, err := asHeadStreamer(t, repo).StreamEntityHeads(context.Background(), "widget")
	require.NoError(t, err)
	require.Equal(t, map[evt.EntityID]evt.EventSequence{"widget-1": 2}, heads)
}

// A compacted stream keeps only its snapshot (events below the floor are deleted), so the head must
// come from the snapshot's recorded EventSequence, not the now-absent events.
func TestStreamEntityHeads_SnapshotFloorAfterCompaction(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository()

	result := evt.SerializedResult{Events: []evt.SerializedEvent{
		headEvent("widget-1", "widget", 1),
		headEvent("widget-1", "widget", 2),
		headEvent("widget-1", "widget", 3),
		headEvent("widget-1", "widget", 4),
		headEvent("widget-1", "widget", 5),
	}}
	require.NoError(t, repo.CommitWithSnapshot(ctx, result, "widget", "widget-1", []byte("{}"), 1))

	// Compact away events 1..5; only the snapshot (EventSequence=5) remains.
	compactor, ok := repo.(evt.Compactor)
	require.True(t, ok)
	deleted, err := compactor.CompactBelow(ctx, "widget-1", 5)
	require.NoError(t, err)
	require.Equal(t, 5, deleted)

	heads, err := asHeadStreamer(t, repo).StreamEntityHeads(ctx, "")
	require.NoError(t, err)
	require.Equal(t, evt.EventSequence(5), heads["widget-1"])
}
