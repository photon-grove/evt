//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/test"
)

func rebuildCommit(t *testing.T, repo evt.Repository, id evt.EntityID, sequence evt.EventSequence) error {
	t.Helper()

	events, err := evt.SerializeEvents([]evt.Event{&test.EntityUpdated{ID: id, Value: "v"}}, sequence-1, id, evt.Metadata{})
	require.NoError(t, err)

	for i := range events {
		events[i].EntityType = "test"
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	return repo.Commit(ctx, evt.SerializedResult{Events: events})
}

// The emulator reports one cancellation reason per action, so a commit that fails its marker check
// and its lock check at once is classified by the ranking: the marker first, and only the retry,
// against the re-validated manifest, meets the lock.
func Test_Rebuild_MarkerCheckOutranksTheLockCheckOnTheEmulator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*callTimeout)
	defer cancel()

	repo, marker := newIsolatedEventLog(ctx, t)

	require.NoError(t, rebuildCommit(t, repo, "marker-first", 1))

	_, err := marker.Declare(ctx, func(manifest *evt.FeatureManifest) {
		manifest.Retention = append(manifest.Retention, "other")
	})
	require.NoError(t, err)

	_, err = repo.RebuildFence().Begin(ctx, "job", time.Minute)
	require.NoError(t, err)

	err = rebuildCommit(t, repo, "marker-first", 2)
	require.ErrorIs(t, err, evt.ErrFeatureManifestChanged)
	require.NotErrorIs(t, err, evt.ErrRebuildInProgress)

	err = rebuildCommit(t, repo, "marker-first", 2)
	require.ErrorIs(t, err, evt.ErrRebuildInProgress)

	// The failed head update beside the lock check (sequence 1 exists) still reports the lock.
	err = rebuildCommit(t, repo, "marker-first", 1)
	require.ErrorIs(t, err, evt.ErrRebuildInProgress)
	require.False(t, evt.IsOptimisticConflict(err))
}

// Two repository handles over one table share the fence, and a handle opened after a force-release
// sees the odd, flagged generation.
func Test_Rebuild_HandlesShareTheFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*callTimeout)
	defer cancel()

	first, _ := newIsolatedEventLog(ctx, t)
	second := first.WithConsistentRead(false)

	started, err := first.RebuildFence().Begin(ctx, "job", time.Minute)
	require.NoError(t, err)

	generation, err := second.RebuildFence().Generation(ctx)
	require.NoError(t, err)
	require.Equal(t, started.Generation, generation.Value, "an eventually consistent repository still reads the generation consistently")

	_, err = second.RebuildFence().ForceRelease(ctx, "job")
	require.NoError(t, err)

	generation, err = first.RebuildFence().Generation(ctx)
	require.NoError(t, err)
	require.Equal(t, evt.RebuildGeneration{Value: started.Generation + 2, ForceReleased: true}, generation)
}
