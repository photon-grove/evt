package snapshots_test

import (
	"context"
	"testing"
	"time"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/mem"
	"github.com/photon-grove/evt/policy"
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

// gapOnce drops the first event of the first post-snapshot read, the way a compaction that lands
// between the snapshot read and the event read would.
type gapOnce struct {
	evt.Repository
	fired bool
}

func (r *gapOnce) GetLatestEvents(ctx context.Context, id evt.EntityID, after evt.EventSequence) ([]evt.SerializedEvent, error) {
	events, err := r.Repository.GetLatestEvents(ctx, id, after)
	if err != nil || r.fired || len(events) < 2 {
		return events, err
	}
	r.fired = true

	return events[1:], nil
}

// policy.ExecuteWithRetry retries a load gap, because the conflict is classified as transient.
func TestExecuteWithRetryRetriesALoadGap(t *testing.T) {
	ctx := context.Background()
	repo := mem.NewRepository()
	id := evt.EntityID("gap-retry-1")

	// Snapshot at 3, then events 4 and 5 after it.
	store := snapshots.NewStore(repo, 3)
	for _, value := range []string{"one", "two", "three", "four", "five"} {
		require.NoError(t, store.Execute(ctx, test.NewEntity(id), id, &test.ReplaceEntity{Value: value}, newTestMetadata()))
	}

	retries := 0
	entity, err := policy.ExecuteWithRetry(ctx,
		snapshots.NewStore(&gapOnce{Repository: repo}, 3),
		func() evt.Entity { return test.NewEntity(id) },
		id,
		&test.ReplaceEntity{Value: "six"},
		newTestMetadata(),
		policy.Config{MaxAttempts: 3, Sleep: func(context.Context, time.Duration) error { return nil }},
		policy.Hooks{OnRetry: func(policy.Attempt) { retries++ }}, //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	)

	require.NoError(t, err)
	require.Equal(t, 1, retries, "the gap is retried once")
	require.Equal(t, "six", entity.(*test.Entity).Value)
}

// compactDuringLoad snapshots and compacts the stream between the load's snapshot read and its
// event read, leaving no event after the old snapshot and so no gap to detect.
type compactDuringLoad struct {
	evt.Repository
	advance func()
	fired   bool
}

func (r *compactDuringLoad) GetLatestEvents(ctx context.Context, id evt.EntityID, after evt.EventSequence) ([]evt.SerializedEvent, error) {
	if !r.fired {
		r.fired = true
		r.advance()
	}

	return r.Repository.GetLatestEvents(ctx, id, after)
}

// A load that falls behind a compaction returns the older state, like any read that another writer
// overtakes. The floor check rejects a commit from it.
func TestFloorRejectsACommitFromALoadBehindACompaction(t *testing.T) {
	ctx := context.Background()
	repo := mem.NewRepository()
	id := evt.EntityID("advance-1")

	store := snapshots.NewStore(repo, 3)
	execute := func(values ...string) {
		for _, value := range values {
			require.NoError(t, store.Execute(ctx, test.NewEntity(id), id, &test.ReplaceEntity{Value: value}, newTestMetadata()))
		}
	}
	execute("one", "two", "three")

	racing := &compactDuringLoad{Repository: repo, advance: func() {
		execute("four", "five", "six")
		_, err := repo.(evt.Compactor).CompactBelow(ctx, id, 6)
		require.NoError(t, err)
	}}

	err := snapshots.NewStore(racing, 3).Execute(ctx, test.NewEntity(id), id, &test.ReplaceEntity{Value: "stale"}, newTestMetadata())
	require.True(t, evt.IsConflictErr(err), "want a conflict, got %v", err)
}
