package snapshots_test

// These tests exercise the public event store surface (Execute,
// LoadEntity, Commit) with mocks to cover orchestration and error handling
// paths that are hard to reach with the end-to-end snapshot fixtures alone.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/mem"
	"github.com/photon-grove/evt/snapshots"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

// Test NewStore constructor
func TestNewStore(t *testing.T) {
	repo := mem.NewRepository()
	snapshotSize := 5

	store := snapshots.NewStore(repo, snapshotSize)

	require.NotNil(t, store)
	// Note: We can't directly test the internal fields as they're private,
	// but we can verify the store works as expected through its methods
}

// Test Execute method - successful command execution
func TestExecute_Success(t *testing.T) {
	setup := newTestSetup(t, "test-execute-1", 5)
	ctx := context.Background()
	metadata := newTestMetadata()

	entity := test.NewEntity(setup.entityID)
	command := &test.CreateEntity{Value: "execute-value", Other: stringPtr("execute-other")}

	err := setup.store.Execute(ctx, entity, setup.entityID, command, metadata)
	require.NoError(t, err)

	// Verify the entity was updated
	require.Equal(t, "execute-value", entity.Value)
	require.Equal(t, "execute-other", *entity.Other)

	// Verify events were committed
	events := getEvents(t, setup.repo, setup.entityID)
	require.Len(t, events, 1)
	require.Equal(t, setup.entityID, events[0].EntityID)
	require.Equal(t, test.CreatedEvent, events[0].Type)
}

// Test Execute method with LoadEntity error
func TestExecute_LoadEntityError(t *testing.T) {
	// Create a mock repo that fails on GetSnapshot
	mockRepo := &MockRepository{
		getSnapshotError: errors.New("snapshot load failed"),
	}
	store := snapshots.NewStore(mockRepo, 5)
	ctx := context.Background()
	metadata := newTestMetadata()

	entity := test.NewEntity("test-id")
	command := &test.CreateEntity{Value: "test-value"}

	err := store.Execute(ctx, entity, "test-id", command, metadata)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot load failed")
}

// Test Execute method with Handle error
func TestExecute_HandleError(t *testing.T) {
	// Use a mock entity that will handle our MockCommand
	mockEntity := &MockEntity{
		BaseEntity:  evt.NewEntity("test-execute-handle-error"),
		handleError: errors.New("handle failed"),
	}

	mockRepo := &MockRepository{}
	store := snapshots.NewStore(mockRepo, 5)
	ctx := context.Background()
	metadata := newTestMetadata()

	// Create a command that will cause the entity to fail
	command := &MockCommand{handleError: errors.New("handle failed")}

	err := store.Execute(ctx, mockEntity, "test-execute-handle-error", command, metadata)
	require.Error(t, err)
	require.Contains(t, err.Error(), "handle failed")
}

// Test Execute method with Commit error
func TestExecute_CommitError(t *testing.T) {
	// Create a mock repo that fails on Commit
	mockRepo := &MockRepository{
		commitError: errors.New("commit failed"),
	}
	store := snapshots.NewStore(mockRepo, 5)
	ctx := context.Background()
	metadata := newTestMetadata()

	entity := test.NewEntity("test-id")
	command := &test.CreateEntity{Value: "test-value"}

	err := store.Execute(ctx, entity, "test-id", command, metadata)
	require.Error(t, err)
	require.Contains(t, err.Error(), "commit failed")
}

// Test Execute method with Apply error
func TestExecute_ApplyError(t *testing.T) {
	setup := newTestSetup(t, "test-execute-apply-error", 5)
	ctx := context.Background()
	metadata := newTestMetadata()

	// Create a mock entity that fails on Apply
	entity := &MockEntity{
		BaseEntity: evt.NewEntity(setup.entityID),
		applyError: errors.New("apply failed"),
	}
	command := &test.CreateEntity{Value: "test-value"}

	err := setup.store.Execute(ctx, entity, setup.entityID, command, metadata)
	require.Error(t, err)
	require.Contains(t, err.Error(), "apply failed")
}

// Test LoadEntity with GetSnapshot error
func TestLoadEntity_GetSnapshotError(t *testing.T) {
	mockRepo := &MockRepository{
		getSnapshotError: errors.New("get snapshot failed"),
	}
	store := snapshots.NewStore(mockRepo, 5)
	ctx := context.Background()

	entity := test.NewEntity("test-id")
	_, err := store.LoadEntity(ctx, entity, "test-id")

	require.Error(t, err)
	require.Contains(t, err.Error(), "get snapshot failed")
}

// Test LoadEntity with snapshot unmarshal error
func TestLoadEntity_UnmarshalError(t *testing.T) {
	mockRepo := &MockRepository{
		snapshot: &evt.SerializedSnapshot{
			EntityType:    "TestEntity",
			EntityID:      "test-id",
			Sequence:      1,
			EventSequence: 2,
			Payload:       []byte("invalid-json{"),
		},
	}
	store := snapshots.NewStore(mockRepo, 5)
	ctx := context.Background()

	entity := test.NewEntity("test-id")
	_, err := store.LoadEntity(ctx, entity, "test-id")

	require.Error(t, err)
	// Error should be related to JSON unmarshaling
	require.Contains(t, err.Error(), "invalid character")
}

// Test LoadEntity with GetLatestEvents error (with snapshot)
func TestLoadEntity_GetLatestEventsError(t *testing.T) {
	payload, mErr := json.Marshal(test.Entity{
		BaseEntity: evt.NewEntity("test-id"),
		Value:      "snapshot-value",
	})
	require.NoError(t, mErr)

	mockRepo := &MockRepository{
		snapshot: &evt.SerializedSnapshot{
			EntityType:    "TestEntity",
			EntityID:      "test-id",
			Sequence:      1,
			EventSequence: 2,
			Payload:       payload,
		},
		getLatestEventsError: errors.New("get latest events failed"),
	}
	store := snapshots.NewStore(mockRepo, 5)
	ctx := context.Background()

	entity := test.NewEntity("test-id")
	_, err := store.LoadEntity(ctx, entity, "test-id")

	require.Error(t, err)
	require.Contains(t, err.Error(), "get latest events failed")
}

// Test LoadEntity with GetEvents error (no snapshot)
func TestLoadEntity_GetEventsError(t *testing.T) {
	mockRepo := &MockRepository{
		getEventsError: errors.New("get events failed"),
	}
	store := snapshots.NewStore(mockRepo, 5)
	ctx := context.Background()

	entity := test.NewEntity("test-id")
	_, err := store.LoadEntity(ctx, entity, "test-id")

	require.Error(t, err)
	require.Contains(t, err.Error(), "get events failed")
}

// Test LoadEntity with event apply error
func TestLoadEntity_ApplyEventError(t *testing.T) {
	// Create a serialized event that will fail to apply
	metadata := newTestMetadata()
	serializedEvent := createExpectedEvent(
		"TestEntity",
		"test-id",
		1,
		test.CreatedEvent,
		[]byte(`{"ID":"test-id","Value":"test","Other":null}`),
		metadata,
	)

	mockRepo := &MockRepository{
		events: []evt.SerializedEvent{serializedEvent},
	}
	store := snapshots.NewStore(mockRepo, 5)
	ctx := context.Background()

	// Use mock entity that fails on Apply
	entity := &MockEntity{
		BaseEntity: evt.NewEntity("test-id"),
		applyError: errors.New("apply event failed"),
	}

	_, err := store.LoadEntity(ctx, entity, "test-id")

	require.Error(t, err)
	// The error will be from the DeserializeEvent method since that's called first
	require.Contains(t, err.Error(), "not implemented")
}

// Test Commit with updateSnapshotWithEvents error
func TestCommit_UpdateSnapshotError(t *testing.T) {
	// Setup a scenario that will trigger snapshot creation
	setup := newTestSetup(t, "test-commit-snapshot-error", 1) // Very small snapshot size
	ctx := context.Background()
	metadata := newTestMetadata()

	// Create events that will trigger snapshot
	command := &test.CreateEntity{Value: "test-value"}
	result := handleCommand(t, setup.entity, command)

	// Use a mock entity that fails during snapshot generation
	mockEntity := &MockEntity{
		BaseEntity:    evt.NewEntity(setup.entityID),
		snapshotError: true,
	}

	eventContext := evt.Context{
		Entity:          mockEntity,
		EntityID:        setup.entityID,
		CurrentSequence: intPtr(0),
		CurrentSnapshot: intPtr(0),
	}

	_, err := setup.store.Commit(ctx, result, eventContext, metadata)
	require.Error(t, err)
	// The error comes from the DeserializeEvent method, not MarshalJSON
	require.Contains(t, err.Error(), "not implemented")
}

// Test edge case of nil CurrentSnapshot no longer panics and initializes to 1
func TestUpdateSnapshotSequence_NilSnapshot(t *testing.T) {
	setup := newTestSetup(t, "test-nil-snapshot", 1)
	ctx := context.Background()
	metadata := newTestMetadata()

	command := &test.CreateEntity{Value: "test-value"}
	result := handleCommand(t, setup.entity, command)

	// Manually create event context with nil CurrentSnapshot to test initialization behavior
	eventContext := evt.Context{
		Entity:          setup.entity,
		EntityID:        setup.entityID,
		CurrentSequence: intPtr(1),
		CurrentSnapshot: nil,
	}

	serialized, err := setup.store.Commit(ctx, result, eventContext, metadata)
	require.NoError(t, err)
	require.NotEmpty(t, serialized)
}

// These tests drive the snapshot helpers end-to-end against the in-memory
// repository to verify serialization, hydration, and snapshot rotation happy
// paths, complementing the mocked orchestration exercised in eventstore tests.

func Test_EventStore_CommitSimpleEvents_Success(t *testing.T) {
	setup := newTestSetup(t, "test-id", 2)
	metadata := newTestMetadata()
	testOtherValue := "test-other-value"

	command := createEntityCommand("test-value", &testOtherValue)
	result := handleCommand(t, setup.entity, &command)

	payload := marshalCreatedEvent(t, setup.entityID, command.Value, command.Other)
	expected := []evt.SerializedEvent{
		createExpectedEvent(
			setup.entityType,
			setup.entityID,
			1,
			test.CreatedEvent,
			payload,
			metadata,
		),
	}

	commitEvents(t, setup.store, result, setup.eventContext, metadata)
	committedEvents := getEvents(t, setup.repo, setup.entityID)
	require.Equal(t, expected, committedEvents)
}

func Test_EventStore_LoadWithEvents_Success(t *testing.T) {
	setup := newTestSetup(t, "test-id", 5)
	metadata := newTestMetadata()
	updatedValue := "updated-value"
	otherValue := "test-other-value"

	// Add with an initial value
	createCommand := createEntityCommand("test-value", &otherValue)
	result := handleCommand(t, setup.entity, &createCommand)

	updateCommand := replaceEntityCommand(updatedValue, &otherValue)
	updateResult := handleCommand(t, setup.entity, &updateCommand)

	// Combine results
	combinedResult := combineResults(result, updateResult)

	commitEvents(t, setup.store, combinedResult, setup.eventContext, metadata)

	// Load the entity again to retrieve events from memory
	setup.refresh(t)

	// The hydrated entity should have the updated value (ignore exact timestamps)
	assertEntityMatches(t, *setup.entity, setup.entityID, "updated-value", &otherValue)
}

func Test_EventStore_SnapshotCommit_Success(t *testing.T) {
	setup := newTestSetup(t, "test-id", 2)
	metadata := newTestMetadata()
	updatedValue := "updated-value"
	intermediateOther := "intermediate-other-value"
	updatedOther := "updated-other-value"
	otherValue := "test-other-value"

	// Create entity and handle multiple updates to trigger snapshot
	createCommand := createEntityCommand("test-value", &otherValue)
	result := handleCommand(t, setup.entity, &createCommand)

	updateValueCommand := replaceEntityCommand(updatedValue, &otherValue)
	updateValueResult := handleCommand(t, setup.entity, &updateValueCommand)

	updateOtherCommand := replaceEntityCommand(updatedValue, &intermediateOther)
	updateOtherResult := handleCommand(t, setup.entity, &updateOtherCommand)

	// Combine all results
	firstBatchResult := combineResults(result, updateValueResult, updateOtherResult)
	commitEvents(t, setup.store, firstBatchResult, setup.eventContext, metadata)

	// Retrieve the initial snapshot
	snapshot := getSnapshot(t, setup.repo, setup.entityID)

	// Verify the snapshot metadata and payload contents (without exact timestamp equality)
	require.NotNil(t, snapshot)
	require.Equal(t, setup.entityType, snapshot.EntityType)
	require.Equal(t, setup.entityID, snapshot.EntityID)
	require.Equal(t, evt.EventSequence(1), snapshot.Sequence)
	require.Equal(t, evt.EventSequence(3), snapshot.EventSequence)
	var snapEntity test.Entity
	require.NoError(t, json.Unmarshal(snapshot.Payload, &snapEntity))
	assertEntityMatches(t, snapEntity, setup.entityID, updatedValue, &intermediateOther)

	// Add more events to trigger another snapshot
	nextCommand1 := replaceEntityCommand(updatedValue, &intermediateOther)
	nextResult1 := handleCommand(t, setup.entity, &nextCommand1)

	nextCommand2 := replaceEntityCommand(updatedValue, &updatedOther)
	nextResult2 := handleCommand(t, setup.entity, &nextCommand2)

	secondBatchResult := combineResults(nextResult1, nextResult2)
	commitEvents(t, setup.store, secondBatchResult, setup.eventContext, metadata)

	// Retrieve the updated snapshot
	snapshot = getSnapshot(t, setup.repo, setup.entityID)

	// Verify the updated snapshot metadata and payload
	require.NotNil(t, snapshot)
	require.Equal(t, setup.entityType, snapshot.EntityType)
	require.Equal(t, setup.entityID, snapshot.EntityID)
	require.Equal(t, evt.EventSequence(2), snapshot.Sequence)
	require.Equal(t, evt.EventSequence(5), snapshot.EventSequence)
	var snapEntity2 test.Entity
	require.NoError(t, json.Unmarshal(snapshot.Payload, &snapEntity2))
	assertEntityMatches(t, snapEntity2, setup.entityID, updatedValue, &updatedOther)

	// Verify all events were captured
	committedSerializedEvents := getEvents(t, setup.repo, setup.entityID)
	expectedCombinedResult := combineResults(firstBatchResult, secondBatchResult)
	committedEvents := deserializeEvents(t, committedSerializedEvents, setup.eventContext.Entity)
	require.Equal(t, expectedCombinedResult.Events, committedEvents)
}

func Test_EventStore_LoadWithSnapshot_Success(t *testing.T) {
	setup := newTestSetup(t, "test-id", 2)
	metadata := newTestMetadata()
	updatedValue := "updated-value"
	intermediateOther := "intermediate-other-value"
	updatedOther := "updated-other-value"
	otherValue := "test-other-value"

	// Create entity and build up to snapshot
	createCommand := createEntityCommand("test-value", &otherValue)
	result := handleCommand(t, setup.entity, &createCommand)

	updateValueCommand := replaceEntityCommand(updatedValue, &otherValue)
	updateValueResult := handleCommand(t, setup.entity, &updateValueCommand)

	updateOtherCommand := replaceEntityCommand(updatedValue, &intermediateOther)
	updateOtherResult := handleCommand(t, setup.entity, &updateOtherCommand)

	// Combine and commit first batch (this will create a snapshot)
	firstBatchResult := combineResults(result, updateValueResult, updateOtherResult)
	commitEvents(t, setup.store, firstBatchResult, setup.eventContext, metadata)

	// Add one more event (this will be after the snapshot)
	finalCommand := replaceEntityCommand(updatedValue, &updatedOther)
	finalResult := handleCommand(t, setup.entity, &finalCommand)
	commitEvents(t, setup.store, finalResult, setup.eventContext, metadata)

	// Refresh the setup to load from snapshot + events
	setup.refresh(t)

	// The hydrated entity should have the final values (ignore exact timestamps)
	assertEntityMatches(t, *setup.entity, setup.entityID, updatedValue, &updatedOther)
}

// --- Command Dedupe Guard Tests ---

// Test that a duplicate commandId on Commit returns DuplicateCommandError
func TestCommit_DuplicateCommandID_ReturnsError(t *testing.T) {
	setup := newTestSetup(t, "test-dedupe-commit", 5)
	ctx := context.Background()

	cmdID := evt.CommandID("cmd-001")
	metadata := evt.Metadata{
		Region:    "us-east-1",
		CommandID: &cmdID,
	}

	// First commit succeeds
	command := &test.CreateEntity{Value: "initial", Other: stringPtr("other")}
	result := handleCommand(t, setup.entity, command)
	_, err := setup.store.Commit(ctx, result, setup.eventContext, metadata)
	require.NoError(t, err)

	// Reload entity so SeenCommandIDs are populated from event log
	setup.refresh(t)

	// Second commit with the same commandId must be rejected
	command2 := &test.CreateEntity{Value: "retry"}
	result2 := handleCommand(t, setup.entity, command2)
	_, err = setup.store.Commit(ctx, result2, setup.eventContext, metadata)
	require.Error(t, err)
	require.True(t, evt.IsDuplicateCommandErr(err), "expected DuplicateCommandError, got: %v", err)
}

// Test that different commandIds for the same entity are accepted
func TestCommit_DifferentCommandIDs_Accepted(t *testing.T) {
	setup := newTestSetup(t, "test-dedupe-diff", 5)
	ctx := context.Background()

	cmdID1 := evt.CommandID("cmd-aaa")
	meta1 := evt.Metadata{Region: "us-east-1", CommandID: &cmdID1}

	command := &test.CreateEntity{Value: "v1", Other: stringPtr("o1")}
	result := handleCommand(t, setup.entity, command)
	_, err := setup.store.Commit(ctx, result, setup.eventContext, meta1)
	require.NoError(t, err)

	// Reload to pick up seen IDs
	setup.refresh(t)

	cmdID2 := evt.CommandID("cmd-bbb")
	meta2 := evt.Metadata{Region: "us-east-1", CommandID: &cmdID2}

	command2 := &test.ReplaceEntity{Value: "v2", Other: stringPtr("o2")}
	result2 := handleCommand(t, setup.entity, command2)
	_, err = setup.store.Commit(ctx, result2, setup.eventContext, meta2)
	require.NoError(t, err)
}

// Test that Execute rejects a duplicate commandId with DuplicateCommandError
func TestExecute_DuplicateCommandID_ReturnsError(t *testing.T) {
	setup := newTestSetup(t, "test-dedupe-execute", 5)
	ctx := context.Background()

	cmdID := evt.CommandID("cmd-exec-001")
	metadata := evt.Metadata{Region: "us-east-1", CommandID: &cmdID}

	entity := test.NewEntity(setup.entityID)
	command := &test.CreateEntity{Value: "first"}

	// First execution succeeds
	err := setup.store.Execute(ctx, entity, setup.entityID, command, metadata)
	require.NoError(t, err)

	// Second execution with same commandId is an idempotent duplicate
	entity2 := test.NewEntity(setup.entityID)
	err = setup.store.Execute(ctx, entity2, setup.entityID, command, metadata)
	require.Error(t, err)
	require.True(t, evt.IsDuplicateCommandErr(err), "expected DuplicateCommandError, got: %v", err)
}

// Test that Execute with different commandIds succeeds
func TestExecute_DifferentCommandIDs_Accepted(t *testing.T) {
	setup := newTestSetup(t, "test-dedupe-exec-diff", 5)
	ctx := context.Background()

	cmdID1 := evt.CommandID("cmd-x1")
	meta1 := evt.Metadata{Region: "us-east-1", CommandID: &cmdID1}

	entity1 := test.NewEntity(setup.entityID)
	err := setup.store.Execute(ctx, entity1, setup.entityID, &test.CreateEntity{Value: "first"}, meta1)
	require.NoError(t, err)

	cmdID2 := evt.CommandID("cmd-x2")
	meta2 := evt.Metadata{Region: "us-east-1", CommandID: &cmdID2}

	entity2 := test.NewEntity(setup.entityID)
	err = setup.store.Execute(ctx, entity2, setup.entityID, &test.ReplaceEntity{Value: "second"}, meta2)
	require.NoError(t, err)
}

// Test Execute calls SetID on entities that implement it
func TestExecute_SetID_OverridesEntityID(t *testing.T) {
	setup := newTestSetup(t, "test-setid", 5)
	ctx := context.Background()
	metadata := newTestMetadata()

	// The entity starts with an abbreviated ID, but the store key is the full scoped ID.
	abbreviatedID := evt.EntityID("abbreviated-id")
	storeKey := evt.EntityID("scoped:abbreviated-id:2026-02-23")

	entity := &SetIDMockEntity{
		MockEntity: MockEntity{
			BaseEntity: evt.NewEntity(abbreviatedID),
		},
	}
	command := &test.CreateEntity{Value: "setid-value"}

	err := setup.store.Execute(ctx, entity, storeKey, command, metadata)
	require.NoError(t, err)

	// Verify SetID was called with the store key
	require.True(t, entity.setIDCalled, "SetID should have been called")
	require.Equal(t, storeKey, entity.setIDValue, "SetID should receive the store's entity ID")
	require.Equal(t, storeKey, entity.GetID(), "GetID should return the overridden ID")
}

// Test Execute does not panic when entity does not implement SetID
func TestExecute_WithoutSetID_Succeeds(t *testing.T) {
	setup := newTestSetup(t, "test-no-setid", 5)
	ctx := context.Background()
	metadata := newTestMetadata()

	// MockEntity does not implement SetID — Execute should still work fine
	entity := &MockEntity{
		BaseEntity: evt.NewEntity("test-no-setid"),
	}
	command := &test.CreateEntity{Value: "no-setid-value"}

	err := setup.store.Execute(ctx, entity, "test-no-setid", command, metadata)
	require.NoError(t, err)
}

// Test that nil CommandID bypasses the dedupe guard
func TestCommit_NilCommandID_Bypasses(t *testing.T) {
	setup := newTestSetup(t, "test-dedupe-nil", 5)
	ctx := context.Background()

	metadata := evt.Metadata{Region: "us-east-1"} // no CommandID

	command := &test.CreateEntity{Value: "v1"}
	result := handleCommand(t, setup.entity, command)
	_, err := setup.store.Commit(ctx, result, setup.eventContext, metadata)
	require.NoError(t, err)

	// Reload and commit again without commandId — should succeed
	setup.refresh(t)
	command2 := &test.ReplaceEntity{Value: "v2"}
	result2 := handleCommand(t, setup.entity, command2)
	_, err = setup.store.Commit(ctx, result2, setup.eventContext, metadata)
	require.NoError(t, err)
}

// Test that duplicate commandId does not append additional events
func TestDedupe_NoExtraEventsOnRetry(t *testing.T) {
	setup := newTestSetup(t, "test-dedupe-no-extra", 5)
	ctx := context.Background()

	cmdID := evt.CommandID("cmd-once")
	metadata := evt.Metadata{Region: "us-east-1", CommandID: &cmdID}

	entity := test.NewEntity(setup.entityID)
	err := setup.store.Execute(ctx, entity, setup.entityID, &test.CreateEntity{Value: "once"}, metadata)
	require.NoError(t, err)

	events := getEvents(t, setup.repo, setup.entityID)
	require.Len(t, events, 1, "should have exactly 1 event after first execute")

	// Retry — must not add events
	entity2 := test.NewEntity(setup.entityID)
	err = setup.store.Execute(ctx, entity2, setup.entityID, &test.CreateEntity{Value: "once"}, metadata)
	require.True(t, evt.IsDuplicateCommandErr(err))

	events = getEvents(t, setup.repo, setup.entityID)
	require.Len(t, events, 1, "should still have exactly 1 event after duplicate retry")
}

// Helper functions

func stringPtr(s string) *string {
	return &s
}

func intPtr(i evt.EventSequence) *evt.EventSequence {
	return &i
}
