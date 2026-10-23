package mem_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/mem"
	"github.com/photon-grove/evt/go/test"
)

func newRepo(t *testing.T) *mem.Repository {
	t.Helper()

	repo, ok := mem.NewRepository().(*mem.Repository)
	require.True(t, ok)

	return repo
}

func seeded(t *testing.T, repo *mem.Repository, id evt.EntityID) {
	t.Helper()

	require.NoError(t, repo.Commit(context.Background(), evt.SerializedResult{Events: []evt.SerializedEvent{{
		ID:         evt.GetEventID(id, 1),
		Sequence:   1,
		Type:       test.CreatedEvent,
		Version:    1,
		EntityID:   id,
		EntityType: test.EntityType,
		Payload:    []byte(`{}`),
	}}}))
}

func TestManifest_DeclareBumpsTheVersionAndReturnsACopy(t *testing.T) {
	ctx := context.Background()
	repo := newRepo(t)

	manifest, err := repo.Manifest(ctx)
	require.NoError(t, err)
	require.Nil(t, manifest, "nothing is declared yet")

	declared, err := repo.Declare(ctx, func(m *evt.FeatureManifest) { m.Retention = append(m.Retention, "session") })
	require.NoError(t, err)
	require.Equal(t, int64(1), declared.Version)

	declared, err = repo.Declare(ctx, func(m *evt.FeatureManifest) { m.HistoryDependent = append(m.HistoryDependent, "audit") })
	require.NoError(t, err)
	require.Equal(t, int64(2), declared.Version, "every declaration bumps the version by exactly one")
	require.Equal(t, []evt.EntityType{"session"}, declared.Retention, "declarations merge into the stored manifest")

	stored, err := repo.Manifest(ctx)
	require.NoError(t, err)
	stored.Retention[0] = "mutated"
	again, err := repo.Manifest(ctx)
	require.NoError(t, err)
	require.Equal(t, []evt.EntityType{"session"}, again.Retention, "callers never alias the stored manifest")
}

func TestCommit_RefusesFencesForRetentionTypesOnlyThroughTheManifest(t *testing.T) {
	ctx := context.Background()
	repo := newRepo(t)

	fence := evt.ReadFence{EntityID: "fenced-stream", EntityType: "session"}
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Fences: []evt.ReadFence{fence}}),
		"nothing is declared, so the fence is accepted")

	_, err := repo.Declare(ctx, func(m *evt.FeatureManifest) { m.Retention = append(m.Retention, "session") })
	require.NoError(t, err)

	err = repo.Commit(ctx, evt.SerializedResult{Fences: []evt.ReadFence{fence}})
	var refused *evt.RetentionConflictError
	require.ErrorAs(t, err, &refused)
	require.Equal(t, evt.EntityType("session"), refused.EntityType)

	other := evt.ReadFence{EntityID: "fenced-stream", EntityType: "account"}
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Fences: []evt.ReadFence{other}}),
		"a type outside the retention set is still fenceable")
}

func TestCommit_RefusesToWriteWhenTheManifestDeclaresAnUnsupportedFeature(t *testing.T) {
	ctx := context.Background()
	repo := newRepo(t)
	seeded(t, repo, "marker-stream")

	_, err := repo.Declare(ctx, func(m *evt.FeatureManifest) { m.Features["journal"] = 1 })
	require.NoError(t, err)

	err = repo.Commit(ctx, evt.SerializedResult{Events: []evt.SerializedEvent{{
		ID: evt.GetEventID("marker-stream", 2), Sequence: 2, Type: test.UpdatedEvent, Version: 1,
		EntityID: "marker-stream", EntityType: test.EntityType, Payload: []byte(`{}`),
	}}})
	require.True(t, evt.IsUnsupportedFeatureErr(err), "got %v", err)

	events, err := repo.GetEvents(ctx, "marker-stream")
	require.NoError(t, err)
	require.Len(t, events, 1, "the refused commit wrote nothing")

	_, err = repo.CompactBelow(ctx, "marker-stream", 1)
	require.True(t, evt.IsUnsupportedFeatureErr(err), "compaction is refused too, got %v", err)
}

func TestCompactBelow_RefusesHistoryDependentTypes(t *testing.T) {
	ctx := context.Background()
	repo := newRepo(t)

	events := []evt.SerializedEvent{
		{ID: evt.GetEventID("history-stream", 1), Sequence: 1, Type: test.CreatedEvent, Version: 1,
			EntityID: "history-stream", EntityType: test.EntityType, Payload: []byte(`{}`)},
		{ID: evt.GetEventID("history-stream", 2), Sequence: 2, Type: test.UpdatedEvent, Version: 1,
			EntityID: "history-stream", EntityType: test.EntityType, Payload: []byte(`{}`)},
	}
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: events,
		Snapshots: []evt.SerializedSnapshot{{
			EntityType: test.EntityType, EntityID: "history-stream", Sequence: 1, EventSequence: 2, Payload: []byte(`{}`),
		}},
	}))

	_, err := repo.Declare(ctx, func(m *evt.FeatureManifest) {
		m.HistoryDependent = append(m.HistoryDependent, test.EntityType)
	})
	require.NoError(t, err)

	_, err = repo.CompactBelow(ctx, "history-stream", 2)
	require.True(t, evt.IsHistoryDependentProjectionErr(err), "got %v", err)

	stored, err := repo.GetEvents(ctx, "history-stream")
	require.NoError(t, err)
	require.Len(t, stored, 2, "the refused compaction deleted nothing")
}
