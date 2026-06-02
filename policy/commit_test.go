package policy_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/mem"
	"github.com/photon-grove/evt/policy"
	evttest "github.com/photon-grove/evt/test"
)

func noSleep(_ context.Context, _ time.Duration) error { return nil }

func TestDefaultConfig(t *testing.T) {
	cfg := policy.DefaultConfig()

	if cfg.MaxAttempts != policy.DefaultMaxAttempts {
		t.Fatalf("expected MaxAttempts=%d, got %d", policy.DefaultMaxAttempts, cfg.MaxAttempts)
	}
	if cfg.BaseDelay != policy.DefaultBaseDelay {
		t.Fatalf("expected BaseDelay=%s, got %s", policy.DefaultBaseDelay, cfg.BaseDelay)
	}
	if cfg.MaxDelay != policy.DefaultMaxDelay {
		t.Fatalf("expected MaxDelay=%s, got %s", policy.DefaultMaxDelay, cfg.MaxDelay)
	}
	if cfg.Jitter == nil {
		t.Fatal("expected Jitter to be set")
	}
	if cfg.Sleep == nil {
		t.Fatal("expected Sleep to be set")
	}
}

func TestExecuteWithRetry_SucceedsFirstAttempt(t *testing.T) {
	store := mem.NewStore()
	factory := func() evt.Entity { return evttest.NewEntity("test-1") }

	cfg := policy.Config{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		Sleep:       noSleep,
	}

	entity, err := policy.ExecuteWithRetry(
		context.Background(), store, factory, "test-1",
		&evttest.CreateEntity{Value: "hello"},
		evt.Metadata{},
		cfg, policy.Hooks{},
	)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
	}

	testEntity, ok := entity.(*evttest.Entity)
	if !ok {
		t.Fatalf("expected *test.Entity, got %T", entity)
	}
	if testEntity.Value != "hello" {
		t.Fatalf("expected Value=hello, got %s", testEntity.Value)
	}
}

func TestExecuteWithRetry_ReplayAfterCommit(t *testing.T) {
	store := mem.NewStore()
	factory := func() evt.Entity { return evttest.NewEntity("test-1") }

	cfg := policy.Config{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		Sleep:       noSleep,
	}

	// First execute creates the entity
	_, err := policy.ExecuteWithRetry(
		context.Background(), store, factory, "test-1",
		&evttest.CreateEntity{Value: "v1"},
		evt.Metadata{},
		cfg, policy.Hooks{},
	)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Second execute updates it — entity is replayed from stored events
	entity, err := policy.ExecuteWithRetry(
		context.Background(), store, factory, "test-1",
		&evttest.ReplaceEntity{Value: "v2"},
		evt.Metadata{},
		cfg, policy.Hooks{},
	)
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	testEntity, ok := entity.(*evttest.Entity)
	if !ok {
		t.Fatalf("expected *evttest.Entity, got %T", entity)
	}
	if testEntity.Value != "v2" {
		t.Fatalf("expected Value=v2, got %s", testEntity.Value)
	}

	// Verify replay: load the entity fresh and check state
	fresh := evttest.NewEntity("test-1")
	_, loadErr := store.LoadEntity(context.Background(), fresh, "test-1")
	if loadErr != nil {
		t.Fatalf("load failed: %v", loadErr)
	}
	if fresh.Value != "v2" {
		t.Fatalf("replayed Value=%s, want v2", fresh.Value)
	}
}

func TestExecuteWithRetry_GiveUpReturnsClassifiedError(t *testing.T) {
	store := mem.NewStore()
	factory := func() evt.Entity { return evttest.NewEntity("test-1") }

	cfg := policy.Config{
		MaxAttempts: 1,
		BaseDelay:   time.Millisecond,
		IsRetryable: func(error) bool { return false },
		Sleep:       noSleep,
	}

	// Empty Value triggers business error in test entity
	_, err := policy.ExecuteWithRetry(
		context.Background(), store, factory, "test-1",
		&evttest.CreateEntity{Value: ""},
		evt.Metadata{},
		cfg, policy.Hooks{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var ce *policy.ClassifiedError
	if !errors.As(err, &ce) {
		t.Fatalf("expected ClassifiedError, got %T: %v", err, err)
	}
	if ce.Err == nil {
		t.Fatal("expected wrapped error, got nil")
	}
}

func TestExecuteWithRetry_UnknownEventReplayFailsClosed(t *testing.T) {
	// Commit an unknown event directly to simulate legacy data in the store
	repo := mem.NewRepository()
	unknownEvent := evt.SerializedEvent{
		ID:         evt.GetEventID("test-1", 1),
		Sequence:   1,
		Type:       "test:never_existed",
		Version:    1,
		EntityID:   "test-1",
		EntityType: evttest.EntityType,
		Payload:    []byte(`{}`),
		Metadata:   evt.Metadata{},
	}
	if commitErr := repo.Commit(context.Background(), evt.SerializedResult{Events: []evt.SerializedEvent{unknownEvent}}); commitErr != nil {
		t.Fatalf("raw commit failed: %v", commitErr)
	}

	repoStore := mem.NewStoreFromRepo(repo)

	// Attempt to replay — should fail closed
	fresh := evttest.NewEntity("test-1")
	_, loadErr := repoStore.LoadEntity(context.Background(), fresh, "test-1")
	if loadErr == nil {
		t.Fatal("expected replay to fail closed on unknown event")
	}
	if !evt.IsReplayStrictnessErr(loadErr) {
		t.Fatalf("expected ReplayStrictnessError, got %T: %v", loadErr, loadErr)
	}
}
