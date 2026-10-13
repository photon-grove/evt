package snapshots_test

import (
	"context"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/mem"
	"github.com/photon-grove/evt/snapshots"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

// A sequence gap after the snapshot (here, sequence 3 missing) must fail the load rather than
// apply later events to the wrong state. Compaction racing a load produces this gap.
func TestLoadEntityRejectsSequenceGap(t *testing.T) {
	ctx := context.Background()
	repo := mem.NewRepository()
	store := snapshots.NewStore(repo, 2)
	id := evt.EntityID("gap-1")

	for _, value := range []string{"one", "two"} {
		require.NoError(t, store.Execute(ctx, test.NewEntity(id), id, &test.ReplaceEntity{Value: value}, newTestMetadata()))
	}

	skipped, err := evt.SerializeEvents([]evt.Event{&test.EntityUpdated{ID: id, Value: "four"}}, 3, id, newTestMetadata())
	require.NoError(t, err)
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: skipped}))

	_, err = store.LoadEntity(ctx, test.NewEntity(id), id)
	require.True(t, evt.IsConflictErr(err), "want a conflict, got %v", err)
}
