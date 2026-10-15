//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

func Test_LegacyReservedStreamsRemainReadable(t *testing.T) {
	ctx := context.Background()
	repo, _, table := newIsolatedEventLog(ctx, t)
	pool := newPool(ctx, t)

	for _, id := range []evt.EntityID{"SYSTEM#account", "command-outcome#account"} {
		require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: serializeAs(t, "account", test.EntityType, 0,
			&test.EntityCreated{ID: "account", Value: "v"}, &test.EntityUpdated{ID: "account", Value: "v2"})}))
		require.NoError(t, repo.PutSnapshot(ctx, test.EntityType, "account", []byte(`{"value":"v"}`), 1, 1))
		for _, name := range []string{table, table + "_snapshots"} {
			_, err := pool.Exec(ctx, "UPDATE "+name+" SET entity_id = $1 WHERE entity_id = 'account'", string(id))
			require.NoError(t, err)
		}

		all, err := repo.GetEvents(ctx, id)
		require.NoError(t, err)
		require.Len(t, all, 2)
		require.Equal(t, id, all[0].EntityID)
		latest, err := repo.GetLatestEvents(ctx, id, 1)
		require.NoError(t, err)
		require.Len(t, latest, 1)
		require.Equal(t, evt.EventSequence(2), latest[0].Sequence)
		recent, err := repo.GetRecentEvents(ctx, id, 2)
		require.NoError(t, err)
		require.Len(t, recent, 2)
		require.Equal(t, evt.EventSequence(2), recent[0].Sequence)
		require.Equal(t, evt.EventSequence(1), recent[1].Sequence)
		snapshot, err := repo.GetSnapshot(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, snapshot)
		require.Equal(t, id, snapshot.EntityID)
		require.Equal(t, evt.EventSequence(1), snapshot.EventSequence)
	}
}
