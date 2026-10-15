package mem

import (
	"context"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/stretchr/testify/require"
)

// Test_SkipsReservedNamespaces asserts enumeration, heads, and snapshot-aware rebuilds skip the
// reserved namespaces. Commits refuse reserved IDs, so the rows are seeded directly the way an
// engine row (or a pre-v0.9 stream) would appear in the maps.
func Test_SkipsReservedNamespaces(t *testing.T) {
	repo, ok := NewRepository().(*Repository)
	require.True(t, ok)

	require.NoError(t, repo.Commit(context.Background(), evt.SerializedResult{Events: []evt.SerializedEvent{
		headEvent("acct-1", "test", 1),
	}}))

	// An engine row (command-outcome#) and a pre-v0.9 stream in the SYSTEM# namespace.
	repo.events["command-outcome#cmd-1"] = []evt.SerializedEvent{headEvent("command-outcome#cmd-1", "engine", 1)}
	repo.events["SYSTEM#legacy"] = []evt.SerializedEvent{headEvent("SYSTEM#legacy", "legacy", 1)}

	var ids []evt.EntityID
	for res := range repo.StreamAllEvents(context.Background(), evt.StreamFilter{}) {
		batch, err := res.Unwrap()
		require.NoError(t, err)
		for _, event := range batch {
			ids = append(ids, event.EntityID)
		}
	}
	require.Equal(t, []evt.EntityID{"acct-1"}, ids)

	heads, err := repo.StreamEntityHeads(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, heads, 1)
	require.Contains(t, heads, evt.EntityID("acct-1"))

	rebuilt := 0
	for range repo.StreamEntitiesFromSnapshots(context.Background(), "engine", nil, nil) { //nolint:errcheck // the nil callbacks are never invoked for the filtered rows
		rebuilt++
	}
	require.Equal(t, 0, rebuilt, "the reserved rows are not aggregates")
}

// Test_WritePathsRefuseReservedEntityIds asserts the in-memory write paths refuse reserved entity
// IDs like the durable backends do.
func Test_WritePathsRefuseReservedEntityIds(t *testing.T) {
	repo := NewRepository().(*Repository)

	for _, id := range []evt.EntityID{"SYSTEM#acct-1", "command-outcome#cmd-1"} {
		err := repo.Commit(context.Background(), evt.SerializedResult{Events: []evt.SerializedEvent{
			headEvent(id, "test", 1),
		}})
		require.Error(t, err)
		require.True(t, evt.IsReservedEntityIdErr(err), "expected ReservedEntityIdError, got %v", err)

		err = repo.CommitWithSnapshot(context.Background(),
			evt.SerializedResult{Events: []evt.SerializedEvent{headEvent(id, "test", 1)}},
			"test", id, []byte("{}"), 1)
		require.Error(t, err)
		require.True(t, evt.IsReservedEntityIdErr(err))

		_, err = repo.CompactBelow(context.Background(), id, 1)
		require.Error(t, err)
		require.True(t, evt.IsReservedEntityIdErr(err))
	}

	require.Empty(t, repo.events, "the refused writes stored nothing")
}
