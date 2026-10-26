package snapshots_test

import (
	"context"
	"encoding/json"
	"testing"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/mem"
	"github.com/photon-grove/evt/go/snapshots"
	"github.com/photon-grove/evt/go/test"
	"github.com/stretchr/testify/require"
)

// testSetup represents a test setup with store, entity, and context
type testSetup struct {
	store      evt.Store
	repo       evt.Repository
	entityID   evt.EntityID
	aggregate  *test.Aggregate
	cc         evt.CommandContext
	entityType evt.EntityType
}

// newTestSetup creates a new test setup with the given entity ID and snapshot size
func newTestSetup(t *testing.T, entityID evt.EntityID, snapshotSize int) *testSetup {
	t.Helper()
	ctx := context.Background()

	repo := mem.NewRepository()
	store := snapshots.NewStore(repo, snapshotSize)
	aggregate := test.NewAggregate(entityID)
	entityType := aggregate.Type()

	cc, err := store.LoadAggregate(ctx, aggregate, entityID)
	if err != nil {
		t.Fatalf("Error loading Entity: %v", err)
	}

	return &testSetup{
		store:      store,
		repo:       repo,
		entityID:   entityID,
		aggregate:  aggregate,
		cc:         cc,
		entityType: entityType,
	}
}

// newTestMetadata creates standard test metadata
func newTestMetadata() evt.Metadata {
	return evt.Metadata{
		Region: "us-east-1",
		Origin: &evt.Origin{Source: "EventStoreSuite", Endpoint: "Testing"},
	}
}

// createAggregateCommand creates a CreateAggregate command with the given value and other
func createAggregateCommand(value string, other *string) test.CreateAggregate {
	return test.CreateAggregate{
		Value: value,
		Other: other,
	}
}

// replaceAggregateCommand creates a ReplaceAggregate command with the given value and other
func replaceAggregateCommand(value string, other *string) test.ReplaceAggregate {
	return test.ReplaceAggregate{
		Value: value,
		Other: other,
	}
}

// handleCommand handles a command and returns the result, failing on error
func handleCommand(t *testing.T, aggregate *test.Aggregate, command evt.Command) evt.CommandResult {
	t.Helper()
	ctx := context.Background()

	result, err := aggregate.Handle(ctx, command)
	require.NoError(t, err, "Error handling command")
	return result
}

// combineResults combines multiple command results into one
func combineResults(results ...evt.CommandResult) evt.CommandResult {
	combined := evt.CommandResult{}
	for _, result := range results {
		combined.Events = append(combined.Events, result.Events...)
	}
	return combined
}

// commitEvents commits events and returns serialized events.
//
// Per the Store.Commit contract, the entity must already reflect result.Events before Commit (in
// production, Execute applies them first). These tests construct results without going through
// Execute, so apply the events to the context entity here before committing.
func commitEvents(t *testing.T, store evt.Store, result evt.CommandResult, cc evt.CommandContext, metadata evt.Metadata) []evt.SerializedEvent {
	t.Helper()
	ctx := context.Background()

	for _, event := range result.Events {
		require.NoError(t, cc.Aggregate.Apply(event), "Error applying event before commit")
	}

	serializedEvents, err := store.Commit(ctx, result, cc, metadata)
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

// marshalCreatedEvent marshals an AggregateCreated event to JSON (matches SerializeEvents behavior)
func marshalCreatedEvent(t *testing.T, entityID evt.EntityID, value string, other *string) []byte {
	t.Helper()
	payload, err := json.Marshal(test.AggregateCreated{ID: entityID, Value: value, Other: other})
	require.NoError(t, err, "Error marshaling created event payload")
	return payload
}

// assertAggregateMatches checks the entity ID, value and other fields
func assertAggregateMatches(t *testing.T, e test.Aggregate, id evt.EntityID, value string, other *string) {
	t.Helper()
	require.Equal(t, id, e.ID)
	require.Equal(t, value, e.Value)
	require.Equal(t, other, e.Other)
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
func deserializeEvents(t *testing.T, serializedEvents []evt.SerializedEvent, aggregate evt.Aggregate) []evt.Event {
	t.Helper()

	domainEvents := make([]evt.Event, 0, len(serializedEvents))
	for _, serializedEvent := range serializedEvents {
		event, err := evt.DeserializeEvent(serializedEvent, aggregate)
		require.NoError(t, err, "Error deserializing events")
		domainEvents = append(domainEvents, event)
	}
	return domainEvents
}

// refreshTestSetup reloads the entity from the repository
func (s *testSetup) refresh(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	aggregate := test.NewAggregate(s.entityID)
	cc, err := s.store.LoadAggregate(ctx, aggregate, s.entityID)
	require.NoError(t, err, "Error refreshing entity")

	s.aggregate = aggregate
	s.cc = cc
}
