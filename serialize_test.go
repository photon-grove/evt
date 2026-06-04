package evt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializeEvents(t *testing.T) {
	// Setup
	entityID := EntityID("test-entity-1")
	sequence := EventSequence(10)
	metadata := NewMetadata(context.Background(), nil)

	mockEvent := MockEvent{
		EventType:    "TestEvent",
		EventVersion: 1,
		EntType:      "TestEntity",
		EntID:        entityID,
	}

	events := []Event{mockEvent}

	// Execute
	serialized, err := SerializeEvents(events, sequence, entityID, metadata)

	// Verify
	require.NoError(t, err)
	require.Len(t, serialized, 1)

	sEvent := serialized[0]
	assert.Equal(t, "test-entity-1:11", string(sEvent.ID)) // Sequence incremented
	assert.Equal(t, EventSequence(11), sEvent.Sequence)
	assert.Equal(t, EventType("TestEvent"), sEvent.Type)
	assert.Equal(t, EventVersion(1), sEvent.Version)
	assert.Equal(t, EntityType("TestEntity"), sEvent.EntityType)
	assert.Equal(t, entityID, sEvent.EntityID)
	assert.Equal(t, metadata, sEvent.Metadata)

	// Verify payload is correct JSON
	var unmarshaled MockEvent
	err = json.Unmarshal(sEvent.Payload, &unmarshaled)
	require.NoError(t, err)
	assert.Equal(t, mockEvent, unmarshaled)
}

func TestSerializeEventsWithContext(t *testing.T) {
	// Setup
	entityID := EntityID("test-entity-1")
	seq := EventSequence(5)
	snapshot := EventSequence(0)

	ctx := &Context{
		Entity:          nil,
		EntityID:        entityID,
		CurrentSequence: &seq,
		CurrentSnapshot: &snapshot,
	}

	metadata := NewMetadata(context.Background(), nil)

	mockEvent := MockEvent{
		EventType:    "TestEvent",
		EventVersion: 1,
		EntType:      "TestEntity",
		EntID:        entityID,
	}
	events := []Event{mockEvent}

	// Execute
	serialized, err := SerializeEventsWithContext(events, ctx, metadata)

	// Verify
	require.NoError(t, err)
	require.Len(t, serialized, 1)

	sEvent := serialized[0]
	// Should start incrementing from the context's sequence (5) -> 6
	assert.Equal(t, EventSequence(6), sEvent.Sequence)
	assert.Equal(t, entityID, sEvent.EntityID)
}

func TestSerializeEventsWithContext_NilContext(t *testing.T) {
	events := []Event{MockEvent{}}
	metadata := NewMetadata(context.Background(), nil)

	_, err := SerializeEventsWithContext(events, nil, metadata)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context not found")
}

func TestDeserializeEvent(t *testing.T) {
	// Setup
	entityID := EntityID("test-entity-1")
	mockPayload, err := json.Marshal(MockEvent{
		EventType: "TestEvent",
	})
	require.NoError(t, err)

	serialized := SerializedEvent{
		Type:     "TestEvent",
		Version:  1,
		EntityID: entityID,
		Payload:  mockPayload,
	}

	// Mock Entity that implements simple deserialization
	entity := &MockTestEntity{}

	// Execute
	event, err := DeserializeEvent(serialized, entity)

	// Verify
	require.NoError(t, err)
	require.NotNil(t, event)
	assert.Equal(t, EventType("TestEvent"), event.Type())
}

func TestDeserializeEvent_WithUpcaster(t *testing.T) {
	// Setup
	entityID := EntityID("test-entity-1")
	// Old version event payload
	mockPayload, err := json.Marshal(MockEvent{
		EventType: "TestEventV1",
	})
	require.NoError(t, err)

	serialized := SerializedEvent{
		Type:     "TestEventV1",
		Version:  1,
		EntityID: entityID,
		Payload:  mockPayload,
	}

	// Mock Entity with Upcaster
	// The upcaster should convert V1 to V2
	entity := &MockTestEntity{
		Upcasters: []EventUpcaster{
			&MockUpcaster{
				TargetType: "TestEventV1",
				TargetVer:  1,
				NewType:    "TestEventV2",
				NewVer:     2,
			},
		},
	}

	// Execute
	event, err := DeserializeEvent(serialized, entity)

	// Verify
	require.NoError(t, err)
	require.NotNil(t, event)
	assert.Equal(t, EventType("TestEventV2"), event.Type())
	assert.Equal(t, EventVersion(2), event.Version())
}

func TestDeserializeEvent_FailClosedWrapsDeserializeError(t *testing.T) {
	serialized := SerializedEvent{
		Type:    "UnknownEvent",
		Version: 1,
		Payload: []byte(`{}`),
	}

	_, err := DeserializeEvent(serialized, &failingDeserializeEntity{})
	require.Error(t, err)
	require.True(t, IsReplayStrictnessErr(err))

	var replayErr *ReplayStrictnessError
	require.True(t, errors.As(err, &replayErr))
	assert.Equal(t, EventType("UnknownEvent"), replayErr.EventType)
	assert.Equal(t, EventVersion(1), replayErr.Version)
	assert.Equal(t, "deserialize", replayErr.Phase)
	assert.Contains(t, err.Error(), "unknown event type")
}

func TestCalculateAdditionalEvents(t *testing.T) {
	tests := []struct {
		name           string
		currentSeq     EventSequence
		numEvents      int
		maxSize        int
		expectedResult int
	}{
		{
			name:           "No snapshot needed",
			currentSeq:     0,
			numEvents:      2,
			maxSize:        5,
			expectedResult: 0,
		},
		{
			name:           "Snapshot needed exactly at boundary",
			currentSeq:     0,
			numEvents:      5,
			maxSize:        5,
			expectedResult: 5,
		},
		{
			name:       "Boundary reached inside batch snapshots the whole batch",
			currentSeq: 3,
			numEvents:  3, // seq 4,5,6; boundary at 5 is reached
			maxSize:    5,
			// nextSnapshotAt = 5 - (3 % 5) = 2; numEvents (3) >= 2, so snapshot the full batch.
			expectedResult: 3,
		},
		{
			name:       "Batch spanning multiple intervals snapshots the whole batch",
			currentSeq: 0,
			numEvents:  12,
			maxSize:    5,
			// nextSnapshotAt = 5; numEvents (12) >= 5. One snapshot captures full state at seq 12.
			expectedResult: 12,
		},
		{
			name:       "Non-power-of-2 size snapshots the whole batch once a boundary is crossed",
			currentSeq: 2,
			numEvents:  20,
			maxSize:    5,
			// nextSnapshotAt = 5 - (2 % 5) = 3; numEvents (20) >= 3.
			expectedResult: 20,
		},
		{
			name:           "maxSize of zero disables snapshots",
			currentSeq:     0,
			numEvents:      100,
			maxSize:        0,
			expectedResult: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := CalculateAdditionalEvents(tt.currentSeq, tt.numEvents, tt.maxSize)
			assert.Equal(t, tt.expectedResult, actual)
		})
	}
}

// Helpers for testing

type MockTestEntity struct {
	Upcasters []EventUpcaster
	BaseEntity
}

func (e *MockTestEntity) Type() EntityType { return "TestEntity" }
func (e *MockTestEntity) GetID() EntityID  { return "id" }
func (e *MockTestEntity) Base() BaseEntity { return e.BaseEntity }
func (e *MockTestEntity) Handle(context.Context, Command) (CommandResult, error) {
	return CommandResult{}, nil
}
func (e *MockTestEntity) Apply(Event) error               { return nil }
func (e *MockTestEntity) EventUpcasters() []EventUpcaster { return e.Upcasters }
func (e *MockTestEntity) Projectors() []EventProjector    { return nil }
func (e *MockTestEntity) DeserializeEvent(se SerializedEvent) (Event, error) {
	// Simple mock implementation
	mockEvent := MockEvent{
		EventType:    se.Type,
		EventVersion: se.Version,
		EntType:      se.EntityType,
		EntID:        se.EntityID,
	}
	return mockEvent, nil
}

type failingDeserializeEntity struct {
	MockTestEntity
}

func (e *failingDeserializeEntity) DeserializeEvent(se SerializedEvent) (Event, error) {
	return nil, fmt.Errorf("unknown event type: %s", se.Type)
}

type MockUpcaster struct {
	TargetType EventType
	TargetVer  EventVersion
	NewType    EventType
	NewVer     EventVersion
}

func (u *MockUpcaster) CanUpcast(t EventType, v EventVersion) bool {
	return t == u.TargetType && v == u.TargetVer
}

func (u *MockUpcaster) Upcast(se SerializedEvent) (SerializedEvent, error) {
	se.Type = u.NewType
	se.Version = u.NewVer
	return se, nil
}
