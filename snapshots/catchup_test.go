package snapshots_test

import (
	"context"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/snapshots"
	"github.com/photon-grove/evt/test"
)

func TestCatchUpSnapshotAppliedWhenThresholdExceeded(t *testing.T) {
	repo := &MockRepository{
		events: []evt.SerializedEvent{
			{Sequence: 1, EntityID: "e1", EntityType: "test", Type: "test:created", Payload: []byte(`{"id":"e1","value":"v1"}`)},
			{Sequence: 2, EntityID: "e1", EntityType: "test", Type: "test:updated", Payload: []byte(`{"id":"e1","value":"v2"}`)},
		},
	}

	store := snapshots.NewStore(repo, 25).WithCatchUpSnapshotThreshold(2)

	_, err := store.LoadEntity(context.Background(), test.NewEntity("e1"), "e1")
	if err != nil {
		t.Fatalf("LoadEntity() error = %v", err)
	}

	if repo.putSnapshotCalls != 1 {
		t.Fatalf("putSnapshotCalls = %d, want 1", repo.putSnapshotCalls)
	}
}
