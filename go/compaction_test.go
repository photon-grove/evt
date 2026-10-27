package evt_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/mem"
	"github.com/photon-grove/evt/go/snapshots"
	evttest "github.com/photon-grove/evt/go/test"
)

// snapshotSize is small so a single-event-per-command flow snapshots quickly.
const compactionSnapshotSize = 5

// mustCompactor asserts that the repository supports the Compactor capability.
func mustCompactor(t *testing.T, repo evt.Repository) evt.Compactor {
	t.Helper()

	compactor, ok := repo.(evt.Compactor)
	require.True(t, ok, "repository must implement evt.Compactor")

	return compactor
}

// entityValue extracts the Value field from a streamed test entity with a checked assertion.
func aggregateValue(t *testing.T, aggregate evt.Aggregate) string {
	t.Helper()

	typed, ok := aggregate.(*evttest.Aggregate)
	require.True(t, ok, "entity must be *test.Entity")

	return typed.Value
}

// seedTestAggregate reconstructs a test.Aggregate from a durable snapshot payload.
func seedTestAggregate(_ context.Context, snapshot evt.SerializedSnapshot) (evt.Aggregate, error) {
	aggregate := evttest.NewAggregate(snapshot.EntityID)
	if err := json.Unmarshal(snapshot.Payload, aggregate); err != nil {
		return nil, err
	}

	return aggregate, nil
}

// applyTestEvent deserializes and applies a serialized event, creating the entity on first use.
func applyTestEvent(_ context.Context, serialized evt.SerializedEvent, aggregate evt.Aggregate) (evt.Aggregate, error) {
	if aggregate == nil {
		aggregate = evttest.NewAggregate(serialized.EntityID)
	}

	event, err := aggregate.DeserializeEvent(serialized)
	if err != nil {
		return nil, err
	}
	if err := aggregate.Apply(event); err != nil {
		return nil, err
	}

	return aggregate, nil
}

// buildStream commits one create + (count-1) replace events for entityID, taking snapshots along
// the way, and returns the repository plus the entity's fully-replayed final state.
func buildStream(t *testing.T, entityID evt.EntityID, count int) (evt.Repository, *evttest.Aggregate) {
	t.Helper()

	ctx := context.Background()
	repo := mem.NewRepository()
	store := snapshots.NewStore(repo, compactionSnapshotSize)
	metadata := evt.Metadata{Region: "us-west-2"}

	require.NoError(t, store.Execute(ctx, evttest.NewAggregate(entityID), entityID,
		&evttest.CreateAggregate{Value: "v1"}, metadata))

	for i := 2; i <= count; i++ {
		value := "v" + strconv.Itoa(i)
		require.NoError(t, store.Execute(ctx, evttest.NewAggregate(entityID), entityID,
			&evttest.ReplaceAggregate{Value: value}, metadata))
	}

	loaded := evttest.NewAggregate(entityID)
	_, err := store.LoadAggregate(ctx, loaded, entityID)
	require.NoError(t, err)

	return repo, loaded
}

// rebuildOne drains a SnapshotStreamer for a single entity and returns it.
func rebuildOne(t *testing.T, repo evt.Repository) evt.Aggregate {
	t.Helper()

	streamer, ok := repo.(evt.SnapshotStreamer)
	require.True(t, ok, "repository must implement SnapshotStreamer")

	var got evt.Aggregate
	count := 0
	for res := range streamer.StreamEntitiesFromSnapshots(context.Background(), "", seedTestAggregate, applyTestEvent) {
		aggregate, err := res.Unwrap()
		require.NoError(t, err)
		got = aggregate
		count++
	}
	require.Equal(t, 1, count, "expected exactly one entity")

	return got
}

func remainingSequences(t *testing.T, repo evt.Repository, entityID evt.EntityID) []evt.EventSequence {
	t.Helper()

	events, err := repo.GetEvents(context.Background(), entityID)
	require.NoError(t, err)

	seqs := make([]evt.EventSequence, 0, len(events))
	for _, e := range events {
		seqs = append(seqs, e.Sequence)
	}

	return seqs
}

func TestCompactBelow_RebuildCorrectAfterCompaction(t *testing.T) {
	const entityID evt.EntityID = "acct-1"
	repo, expected := buildStream(t, entityID, 7)

	compactor, ok := repo.(evt.Compactor)
	require.True(t, ok)

	// A snapshot covers through event 5; compact everything at or below it.
	deleted, err := compactor.CompactBelow(context.Background(), entityID, 5)
	require.NoError(t, err)
	require.Equal(t, 5, deleted)

	// Events 1..5 are gone; only the post-snapshot tail (6, 7) remains.
	require.Equal(t, []evt.EventSequence{6, 7}, remainingSequences(t, repo, entityID))

	// A snapshot-aware load still reconstructs the correct state.
	loaded := evttest.NewAggregate(entityID)
	store := snapshots.NewStore(repo, compactionSnapshotSize)
	_, err = store.LoadAggregate(context.Background(), loaded, entityID)
	require.NoError(t, err)
	require.Equal(t, expected.Value, loaded.Value)

	// A snapshot-aware rebuild reproduces the same state from snapshot + tail.
	rebuilt := rebuildOne(t, repo)
	require.Equal(t, expected.Value, aggregateValue(t, rebuilt))
	require.Equal(t, expected.GetID(), rebuilt.GetID())
}

func TestCompactBelow_BoundaryEqualsSnapshot(t *testing.T) {
	const entityID evt.EntityID = "acct-boundary"
	repo, _ := buildStream(t, entityID, 5) // exactly one snapshot at event 5, no tail

	compactor := mustCompactor(t, repo)

	// Compacting up to the snapshot boundary is allowed; one past it is refused.
	_, err := compactor.CompactBelow(context.Background(), entityID, 6)
	require.ErrorIs(t, err, evt.ErrCompactionUncovered)

	deleted, err := compactor.CompactBelow(context.Background(), entityID, 5)
	require.NoError(t, err)
	require.Equal(t, 5, deleted)
	require.Empty(t, remainingSequences(t, repo, entityID))

	// The snapshot row survives and still loads the entity.
	snap, err := repo.GetSnapshot(context.Background(), entityID)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Equal(t, evt.EventSequence(5), snap.EventSequence)

	rebuilt := rebuildOne(t, repo)
	require.Equal(t, "v5", aggregateValue(t, rebuilt))
}

func TestCompactBelow_RefusesWithoutSnapshot(t *testing.T) {
	const entityID evt.EntityID = "acct-nosnap"
	repo, _ := buildStream(t, entityID, 3) // fewer than snapshotSize events => no snapshot yet

	snap, err := repo.GetSnapshot(context.Background(), entityID)
	require.NoError(t, err)
	require.Nil(t, snap)

	compactor := mustCompactor(t, repo)
	deleted, err := compactor.CompactBelow(context.Background(), entityID, 2)
	require.ErrorIs(t, err, evt.ErrCompactionUncovered)
	require.Equal(t, 0, deleted)

	// Nothing was deleted.
	require.Equal(t, []evt.EventSequence{1, 2, 3}, remainingSequences(t, repo, entityID))
}

func TestCompactBelow_ZeroIsNoOp(t *testing.T) {
	const entityID evt.EntityID = "acct-zero"
	repo, _ := buildStream(t, entityID, 7)

	compactor := mustCompactor(t, repo)
	deleted, err := compactor.CompactBelow(context.Background(), entityID, 0)
	require.NoError(t, err)
	require.Equal(t, 0, deleted)
	require.Equal(t, []evt.EventSequence{1, 2, 3, 4, 5, 6, 7}, remainingSequences(t, repo, entityID))
}

func TestCompactBelow_Idempotent(t *testing.T) {
	const entityID evt.EntityID = "acct-idem"
	repo, _ := buildStream(t, entityID, 7)
	compactor := mustCompactor(t, repo)

	first, err := compactor.CompactBelow(context.Background(), entityID, 5)
	require.NoError(t, err)
	require.Equal(t, 5, first)

	// Running it again deletes nothing (the range is already empty) and does not error.
	second, err := compactor.CompactBelow(context.Background(), entityID, 5)
	require.NoError(t, err)
	require.Equal(t, 0, second)
	require.Equal(t, []evt.EventSequence{6, 7}, remainingSequences(t, repo, entityID))
}

func TestRebuildProjections_SeedAggregateRequiresSnapshotStreamer(t *testing.T) {
	// fakeRepo (defined in rebuild_test.go) does not implement SnapshotStreamer.
	repo := &fakeRepo{}

	_, err := evt.RebuildProjections(context.Background(), repo, applyTestEvent, evt.RebuildConfig{
		Projectors:    []evt.EventProjector{&stubProjector{}},
		DryRun:        true,
		SeedAggregate: seedTestAggregate,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "SnapshotStreamer")
}

func TestRebuildProjections_SnapshotAwarePathAfterCompaction(t *testing.T) {
	const entityID evt.EntityID = "acct-rebuild"
	repo, expected := buildStream(t, entityID, 7)

	compactor := mustCompactor(t, repo)
	_, err := compactor.CompactBelow(context.Background(), entityID, 5)
	require.NoError(t, err)

	projector := &stubProjector{group: &stubTransactionGroup{size: 1}}

	res, err := evt.RebuildProjections(context.Background(), repo, applyTestEvent, evt.RebuildConfig{
		Projectors:    []evt.EventProjector{projector},
		DryRun:        true,
		SeedAggregate: seedTestAggregate,
	})
	require.NoError(t, err)
	require.Equal(t, 1, res.Processed)

	calls := projector.getCalls()
	require.Len(t, calls, 1)
	require.Equal(t, expected.Value, aggregateValue(t, calls[0]))
}

// Sanity check that the test helpers and a clean (uncompacted) rebuild agree.
func TestStreamEntitiesFromSnapshots_NoSnapshotFallsBackToFullReplay(t *testing.T) {
	const entityID evt.EntityID = "acct-fallback"
	repo, expected := buildStream(t, entityID, 3) // no snapshot taken yet

	rebuilt := rebuildOne(t, repo)
	require.Equal(t, expected.Value, aggregateValue(t, rebuilt))
}

// Guard: the wrapped compaction error remains identifiable via errors.Is.
func TestErrCompactionUncovered_IsWrapped(t *testing.T) {
	const entityID evt.EntityID = "acct-wrap"
	repo, _ := buildStream(t, entityID, 7)
	compactor := mustCompactor(t, repo)

	_, err := compactor.CompactBelow(context.Background(), entityID, 9999)
	require.True(t, errors.Is(err, evt.ErrCompactionUncovered))
}
