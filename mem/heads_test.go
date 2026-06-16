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

func asHeadVisitor(t *testing.T, repo evt.Repository) evt.EntityHeadVisitor {
	t.Helper()
	hv, ok := repo.(evt.EntityHeadVisitor)
	require.True(t, ok)

	return hv
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

// StreamEntityHeadsFunc must visit every entity exactly once with the same head StreamEntityHeads
// reports — including a snapshot-only (compacted) entity that lives only in the snapshot map.
func TestStreamEntityHeadsFunc_VisitsEachEntityOnce(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository()

	commitEvents(t, repo, headEvent("widget-1", "widget", 1), headEvent("widget-1", "widget", 2))
	commitEvents(t, repo, headEvent("gadget-1", "gadget", 4))

	// A compacted, snapshot-only entity: events deleted, snapshot floor at 7.
	require.NoError(t, repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: []evt.SerializedEvent{
		headEvent("widget-2", "widget", 6),
		headEvent("widget-2", "widget", 7),
	}}, "widget", "widget-2", []byte("{}"), 1))
	compactor, ok := repo.(evt.Compactor)
	require.True(t, ok)
	_, err := compactor.CompactBelow(ctx, "widget-2", 7)
	require.NoError(t, err)

	visits := map[evt.EntityID]int{}
	collected := map[evt.EntityID]evt.EventSequence{}
	err = asHeadVisitor(t, repo).StreamEntityHeadsFunc(ctx, "", func(id evt.EntityID, head evt.EventSequence) error {
		visits[id]++
		collected[id] = head
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, map[evt.EntityID]evt.EventSequence{"widget-1": 2, "widget-2": 7, "gadget-1": 4}, collected)
	for id, n := range visits {
		require.Equalf(t, 1, n, "entity %s visited more than once", id)
	}

	// The visitor and the map convenience method agree.
	heads, err := asHeadStreamer(t, repo).StreamEntityHeads(ctx, "")
	require.NoError(t, err)
	require.Equal(t, heads, collected)
}

func TestStreamEntityHeadsFunc_FiltersByType(t *testing.T) {
	repo := NewRepository()

	commitEvents(t, repo, headEvent("widget-1", "widget", 2))
	commitEvents(t, repo, headEvent("gadget-1", "gadget", 5))

	collected := map[evt.EntityID]evt.EventSequence{}
	err := asHeadVisitor(t, repo).StreamEntityHeadsFunc(context.Background(), "widget", func(id evt.EntityID, head evt.EventSequence) error {
		collected[id] = head
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, map[evt.EntityID]evt.EventSequence{"widget-1": 2}, collected)
}

// A visit error stops enumeration and propagates unchanged.
func TestStreamEntityHeadsFunc_StopsOnVisitError(t *testing.T) {
	repo := NewRepository()

	commitEvents(t, repo, headEvent("widget-1", "widget", 1))
	commitEvents(t, repo, headEvent("widget-2", "widget", 1))
	commitEvents(t, repo, headEvent("widget-3", "widget", 1))

	boom := fmt.Errorf("visitor boom")
	visits := 0
	err := asHeadVisitor(t, repo).StreamEntityHeadsFunc(context.Background(), "", func(_ evt.EntityID, _ evt.EventSequence) error {
		visits++
		return boom
	})

	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, visits, "enumeration must stop at the first visit error")
}
