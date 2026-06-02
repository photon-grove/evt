package snapshots_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/mem"
	"github.com/photon-grove/evt/snapshots"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

// testSetup represents a test setup with store, entity, and context
type testSetup struct {
	store        evt.Store
	repo         evt.Repository
	entityID     evt.EntityID
	entity       *test.Entity
	eventContext evt.Context
	entityType   evt.EntityType
}

// newTestSetup creates a new test setup with the given entity ID and snapshot size
func newTestSetup(t *testing.T, entityID evt.EntityID, snapshotSize int) *testSetup {
	t.Helper()
	ctx := context.Background()

	repo := mem.NewRepository()
	store := snapshots.NewStore(repo, snapshotSize)
	entity := test.NewEntity(entityID)
	entityType := entity.Type()

	eventContext, err := store.LoadEntity(ctx, entity, entityID)
	if err != nil {
		t.Fatalf("Error loading Entity: %v", err)
	}

	return &testSetup{
		store:        store,
		repo:         repo,
		entityID:     entityID,
		entity:       entity,
		eventContext: eventContext,
		entityType:   entityType,
	}
}

// newTestMetadata creates standard test metadata
func newTestMetadata() evt.Metadata {
	return evt.Metadata{
		Region: "us-east-1",
		Origin: &evt.Origin{Source: "EventStoreSuite", Endpoint: "Testing"},
	}
}

// createEntityCommand creates a CreateEntity command with the given value and other
func createEntityCommand(value string, other *string) test.CreateEntity {
	return test.CreateEntity{
		Value: value,
		Other: other,
	}
}

// replaceEntityCommand creates a ReplaceEntity command with the given value and other
func replaceEntityCommand(value string, other *string) test.ReplaceEntity {
	return test.ReplaceEntity{
		Value: value,
		Other: other,
	}
}

// handleCommand handles a command and returns the result, failing on error
func handleCommand(t *testing.T, entity *test.Entity, command evt.Command) evt.CommandResult {
	t.Helper()
	ctx := context.Background()

	result, err := entity.Handle(ctx, command)
	require.NoError(t, err, "Error handling command")
	return result
}

// combineResults combines multiple command results into one
func combineResults(results ...evt.CommandResult) evt.CommandResult {
	combined := evt.CommandResult{}
	for _, result := range results {
		combined.Events = append(combined.Events, result.Events...)
		combined.Transaction = append(combined.Transaction, result.Transaction...)
	}
	return combined
}

// commitEvents commits events and returns serialized events
func commitEvents(t *testing.T, store evt.Store, result evt.CommandResult, eventContext evt.Context, metadata evt.Metadata) []evt.SerializedEvent {
	t.Helper()
	ctx := context.Background()

	serializedEvents, err := store.Commit(ctx, result, eventContext, metadata)
	require.NoError(t, err, "Error committing events")
	return serializedEvents
}

// getEvents retrieves events from repository
func getEvents(t *testing.T, repo evt.Repository, entityID evt.EntityID) []evt.SerializedEvent {
	t.Helper()
	ctx := context.Background()

	events, err := repo.GetEvents(ctx, entityID)
	require.NoError(t, err, "Error retrieving events")
	return events
}

// getSnapshot retrieves snapshot from repository
func getSnapshot(t *testing.T, repo evt.Repository, entityID evt.EntityID) *evt.SerializedSnapshot {
	t.Helper()
	ctx := context.Background()

	snapshot, err := repo.GetSnapshot(ctx, entityID)
	require.NoError(t, err, "Error retrieving snapshot")
	return snapshot
}

// marshalCreatedEvent marshals an EntityCreated event to JSON (matches SerializeEvents behavior)
func marshalCreatedEvent(t *testing.T, entityID evt.EntityID, value string, other *string) []byte {
	t.Helper()
	payload, err := json.Marshal(test.EntityCreated{ID: entityID, Value: value, Other: other})
	require.NoError(t, err, "Error marshaling created event payload")
	return payload
}

// assertEntityMatches checks core fields and that timestamps are non-zero without requiring exact equality
func assertEntityMatches(t *testing.T, e test.Entity, id evt.EntityID, value string, other *string) {
	t.Helper()
	require.Equal(t, id, e.ID)
	require.True(t, e.IsActive)
	require.Equal(t, value, e.Value)
	require.Equal(t, other, e.Other)
	require.False(t, e.CreatedAt.IsZero())
	require.False(t, e.UpdatedAt.IsZero())
}

// createExpectedEvent creates an expected serialized event for assertions
func createExpectedEvent(entityType evt.EntityType, entityID evt.EntityID, sequence evt.EventSequence, eventType evt.EventType, payload []byte, metadata evt.Metadata) evt.SerializedEvent {
	return evt.SerializedEvent{
		ID:         evt.GetEventID(entityID, sequence),
		EntityType: entityType,
		EntityID:   entityID,
		Sequence:   sequence,
		Type:       eventType,
		Version:    1,
		Payload:    payload,
		Metadata:   metadata,
	}
}

// deserializeEvents converts serialized events to domain events using the entity
func deserializeEvents(t *testing.T, serializedEvents []evt.SerializedEvent, entity evt.Entity) []evt.Event {
	t.Helper()

	domainEvents := make([]evt.Event, 0, len(serializedEvents))
	for _, serializedEvent := range serializedEvents {
		event, err := evt.DeserializeEvent(serializedEvent, entity)
		require.NoError(t, err, "Error deserializing events")
		domainEvents = append(domainEvents, event)
	}
	return domainEvents
}

// refreshTestSetup reloads the entity from the repository
func (s *testSetup) refresh(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	entity := test.NewEntity(s.entityID)
	eventContext, err := s.store.LoadEntity(ctx, entity, s.entityID)
	require.NoError(t, err, "Error refreshing entity")

	s.entity = entity
	s.eventContext = eventContext
}
