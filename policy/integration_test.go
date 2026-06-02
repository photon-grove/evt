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

// TestRetryReplayRoundTrip_ChatIntentPattern validates retry + replay
// semantics using the pattern from Chat Intent: create an entity, update it,
// then replay and verify state.
func TestRetryReplayRoundTrip_ChatIntentPattern(t *testing.T) {
	store := mem.NewStore()
	factory := func() evt.Entity { return evttest.NewEntity("msg-1") }

	cfg := policy.Config{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		Sleep:       noSleep,
	}

	// Step 1: Create entity (mimics Chat Intent message creation)
	entity, err := policy.ExecuteWithRetry(
		context.Background(), store, factory, "msg-1",
		&evttest.CreateEntity{Value: "initial-content"},
		evt.Metadata{},
		cfg, policy.Hooks{},
	)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	testEntity, ok := entity.(*evttest.Entity)
	if !ok {
		t.Fatalf("expected *evttest.Entity, got %T", entity)
	}
	if testEntity.Value != "initial-content" {
		t.Fatalf("expected initial-content, got %s", testEntity.Value)
	}

	// Step 2: Update entity (mimics message status update)
	_, err = policy.ExecuteWithRetry(
		context.Background(), store, factory, "msg-1",
		&evttest.ReplaceEntity{Value: "interpreted"},
		evt.Metadata{},
		cfg, policy.Hooks{},
	)
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	// Step 3: Replay — load fresh entity and verify state
	fresh := evttest.NewEntity("msg-1")
	_, loadErr := store.LoadEntity(context.Background(), fresh, "msg-1")
	if loadErr != nil {
		t.Fatalf("replay failed: %v", loadErr)
	}
	if fresh.Value != "interpreted" {
		t.Fatalf("replayed Value=%s, want interpreted", fresh.Value)
	}
}

// TestRetryReplayRoundTrip_EventSourcedPattern validates retry + replay semantics
// using the pattern from event-sourced services: multiple command cycles with deduplication
// and factory-based reload.
func TestRetryReplayRoundTrip_EventSourcedPattern(t *testing.T) {
	store := mem.NewStore()
	factory := func() evt.Entity { return evttest.NewEntity("portfolio-1") }

	cfg := policy.Config{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		Sleep:       noSleep,
	}

	// Step 1: Create portfolio entity
	_, err := policy.ExecuteWithRetry(
		context.Background(), store, factory, "portfolio-1",
		&evttest.CreateEntity{Value: "BTC-USD"},
		evt.Metadata{},
		cfg, policy.Hooks{},
	)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Step 2: Update portfolio (mimics order placement)
	other := "order-123"
	entity, err := policy.ExecuteWithRetry(
		context.Background(), store, factory, "portfolio-1",
		&evttest.ReplaceEntity{Value: "BTC-USD-updated", Other: &other},
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
	if testEntity.Value != "BTC-USD-updated" {
		t.Fatalf("expected BTC-USD-updated, got %s", testEntity.Value)
	}
	if testEntity.Other == nil || *testEntity.Other != "order-123" {
		t.Fatal("expected Other=order-123")
	}

	// Step 3: Command deduplication — same CommandID should not produce new events
	cmdID := evt.CommandID("dedup-test-1")
	_, err = policy.ExecuteWithRetry(
		context.Background(), store, factory, "portfolio-1",
		&evttest.ReplaceEntity{Value: "should-dedup"},
		evt.Metadata{CommandID: &cmdID},
		cfg, policy.Hooks{},
	)
	if err != nil {
		t.Fatalf("first command failed: %v", err)
	}

	// Retry with same CommandID — should return DuplicateCommandError
	_, err = policy.ExecuteWithRetry(
		context.Background(), store, factory, "portfolio-1",
		&evttest.ReplaceEntity{Value: "duplicate"},
		evt.Metadata{CommandID: &cmdID},
		cfg, policy.Hooks{},
	)
	if err == nil {
		t.Fatal("expected DuplicateCommandError, got nil")
	}
	if !evt.IsDuplicateCommandErr(err) {
		t.Fatalf("expected DuplicateCommandError, got %T: %v", err, err)
	}

	// Verify final state via replay
	fresh := evttest.NewEntity("portfolio-1")
	_, loadErr := store.LoadEntity(context.Background(), fresh, "portfolio-1")
	if loadErr != nil {
		t.Fatalf("replay failed: %v", loadErr)
	}
	if fresh.Value != "should-dedup" {
		t.Fatalf("replayed Value=%s, want should-dedup", fresh.Value)
	}
}

// TestReplayStrictness_UnknownEventBlocksLoad verifies that unknown events
// in the event log prevent entity loading (fail-closed).
func TestReplayStrictness_UnknownEventBlocksLoad(t *testing.T) {
	repo := mem.NewRepository()
	store := mem.NewStoreFromRepo(repo)

	// Commit a valid event
	validEvent := evt.SerializedEvent{
		ID:         evt.GetEventID("entity-1", 1),
		Sequence:   1,
		Type:       evttest.CreatedEvent,
		Version:    1,
		EntityID:   "entity-1",
		EntityType: evttest.EntityType,
		Payload:    []byte(`{"id":"entity-1","value":"ok"}`),
		Metadata:   evt.Metadata{},
	}
	if err := repo.Commit(context.Background(), evt.SerializedResult{Events: []evt.SerializedEvent{validEvent}}); err != nil {
		t.Fatalf("valid commit: %v", err)
	}

	// Commit an unknown event type
	unknownEvent := evt.SerializedEvent{
		ID:         evt.GetEventID("entity-1", 2),
		Sequence:   2,
		Type:       "test:unknown_event_type",
		Version:    1,
		EntityID:   "entity-1",
		EntityType: evttest.EntityType,
		Payload:    []byte(`{}`),
		Metadata:   evt.Metadata{},
	}
	if err := repo.Commit(context.Background(), evt.SerializedResult{Events: []evt.SerializedEvent{unknownEvent}}); err != nil {
		t.Fatalf("unknown commit: %v", err)
	}

	// Attempt replay — should fail closed
	entity := evttest.NewEntity("entity-1")
	_, err := store.LoadEntity(context.Background(), entity, "entity-1")
	if err == nil {
		t.Fatal("expected replay to fail closed on unknown event")
	}

	if !evt.IsReplayStrictnessErr(err) {
		t.Fatalf("expected ReplayStrictnessError, got %T: %v", err, err)
	}

	var replayErr *evt.ReplayStrictnessError
	if !errors.As(err, &replayErr) {
		t.Fatalf("expected ReplayStrictnessError, got %T", err)
	}
	if replayErr.Phase != "deserialize" {
		t.Fatalf("expected phase=deserialize, got %q", replayErr.Phase)
	}
}
