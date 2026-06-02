package snapshots_test

import (
	"context"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/snapshots"
	"github.com/photon-grove/evt/test"
)

func TestLoadEntityEmitsFreshnessSample(t *testing.T) {
	repo := &MockRepository{
		events: []evt.SerializedEvent{
			{Sequence: 1, EntityID: "e1", EntityType: "test", Type: "test:created", Payload: []byte(`{"id":"e1","value":"v1"}`)},
			{Sequence: 2, EntityID: "e1", EntityType: "test", Type: "test:updated", Payload: []byte(`{"id":"e1","value":"v2"}`)},
		},
	}

	var got snapshots.FreshnessSample
	store := snapshots.
		NewStore(repo, 25).
		WithReplayEstimatePerEventMS(10).
		WithReplayBudgetEvents(1).
		WithFreshnessObserver(func(_ context.Context, sample snapshots.FreshnessSample) {
			got = sample
		})

	_, err := store.LoadEntity(context.Background(), test.NewEntity("e1"), "e1")
	if err != nil {
		t.Fatalf("LoadEntity() error = %v", err)
	}

	if got.EntityID != "e1" {
		t.Fatalf("EntityID = %q, want e1", got.EntityID)
	}
	if got.EventsSinceSnapshot != 2 {
		t.Fatalf("EventsSinceSnapshot = %d, want 2", got.EventsSinceSnapshot)
	}
	if got.EstimatedReplayMS != 20 {
		t.Fatalf("EstimatedReplayMS = %d, want 20", got.EstimatedReplayMS)
	}
	if !got.ReplayBudgetExceeded {
		t.Fatal("expected ReplayBudgetExceeded to be true")
	}
}

func TestCatchUpSnapshotAppliedWhenThresholdExceeded(t *testing.T) {
	repo := &MockRepository{
		events: []evt.SerializedEvent{
			{Sequence: 1, EntityID: "e1", EntityType: "test", Type: "test:created", Payload: []byte(`{"id":"e1","value":"v1"}`)},
			{Sequence: 2, EntityID: "e1", EntityType: "test", Type: "test:updated", Payload: []byte(`{"id":"e1","value":"v2"}`)},
		},
	}

	var got snapshots.FreshnessSample
	store := snapshots.
		NewStore(repo, 25).
		WithCatchUpSnapshotThreshold(2).
		WithFreshnessObserver(func(_ context.Context, sample snapshots.FreshnessSample) {
			got = sample
		})

	_, err := store.LoadEntity(context.Background(), test.NewEntity("e1"), "e1")
	if err != nil {
		t.Fatalf("LoadEntity() error = %v", err)
	}

	if repo.putSnapshotCalls != 1 {
		t.Fatalf("putSnapshotCalls = %d, want 1", repo.putSnapshotCalls)
	}
	if !got.CatchUpApplied {
		t.Fatal("expected CatchUpApplied to be true")
	}
}
