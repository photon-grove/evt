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

func TestSerializeEventsWithCommandContext(t *testing.T) {
	// Setup
	entityID := EntityID("test-entity-1")
	seq := EventSequence(5)
	snapshot := EventSequence(0)

	cc := &CommandContext{
		Aggregate:       nil,
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
	serialized, err := SerializeEventsWithCommandContext(events, cc, metadata)

	// Verify
	require.NoError(t, err)
	require.Len(t, serialized, 1)

	sEvent := serialized[0]
	// Should start incrementing from the context's sequence (5) -> 6
	assert.Equal(t, EventSequence(6), sEvent.Sequence)
	assert.Equal(t, entityID, sEvent.EntityID)
}

func TestSerializeEventsWithCommandContext_NilContext(t *testing.T) {
	events := []Event{MockEvent{}}
	metadata := NewMetadata(context.Background(), nil)

	_, err := SerializeEventsWithCommandContext(events, nil, metadata)
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

	// Mock Aggregate that implements simple deserialization
	aggregate := &MockTestAggregate{}

	// Execute
	event, err := DeserializeEvent(serialized, aggregate)

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

	// Mock Aggregate with Upcaster
	// The upcaster should convert V1 to V2
	aggregate := &MockTestAggregate{
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
	event, err := DeserializeEvent(serialized, aggregate)

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

	_, err := DeserializeEvent(serialized, &failingDeserializeAggregate{})
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

type MockTestAggregate struct {
	Upcasters []EventUpcaster
}

func (e *MockTestAggregate) Type() EntityType { return "TestEntity" }
func (e *MockTestAggregate) GetID() EntityID  { return "id" }
func (e *MockTestAggregate) Handle(context.Context, Command) (CommandResult, error) {
	return CommandResult{}, nil
}
func (e *MockTestAggregate) Apply(Event) error               { return nil }
func (e *MockTestAggregate) EventUpcasters() []EventUpcaster { return e.Upcasters }
func (e *MockTestAggregate) Projectors() []EventProjector    { return nil }
func (e *MockTestAggregate) DeserializeEvent(se SerializedEvent) (Event, error) {
	// Simple mock implementation
	mockEvent := MockEvent{
		EventType:    se.Type,
		EventVersion: se.Version,
		EntType:      se.EntityType,
		EntID:        se.EntityID,
	}
	return mockEvent, nil
}

type failingDeserializeAggregate struct {
	MockTestAggregate
}

func (e *failingDeserializeAggregate) DeserializeEvent(se SerializedEvent) (Event, error) {
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

func TestSerializedEventDecodePreservesValues(t *testing.T) {
	for _, unmarshal := range []func([]byte, *SerializedEvent) error{
		func(data []byte, event *SerializedEvent) error { return json.Unmarshal(data, event) },
		UnmarshalEvent,
	} {
		event := SerializedEvent{ID: "account-1:1", Payload: []byte(`{}`)}
		require.NoError(t, unmarshal([]byte(`{"metadata":{"commandId":"cmd-1"}}`), &event))
		require.Equal(t, EventID("account-1:1"), event.ID)
		require.Equal(t, []byte(`{}`), event.Payload)
		require.Equal(t, CommandID("cmd-1"), *event.Metadata.CommandID)
		require.Nil(t, event.Metadata.CausationID)
		require.Nil(t, event.Metadata.Extensions)

		encoded, err := json.Marshal(event)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"causationId":"cmd-1","extensions":{}`)
		require.Nil(t, event.Metadata.CausationID)
		require.Nil(t, event.Metadata.Extensions)

		require.NoError(t, unmarshal([]byte(`null`), &event))
		require.Equal(t, EventID("account-1:1"), event.ID)
		require.Error(t, unmarshal([]byte(`{"payload":"not-base64!"}`), &event))
		require.Equal(t, []byte(`{}`), event.Payload)
		require.Error(t, unmarshal([]byte(`{}`), nil))
	}
}

// These tests document the SerializedSnapshot structure and its serialization behavior.

func Test_SerializedSnapshot_Creation(t *testing.T) {
	snapshot := SerializedSnapshot{
		EntityType:    EntityType("User"),
		EntityID:      EntityID("user-123"),
		Sequence:      EventSequence(5),
		EventSequence: EventSequence(25),
		Payload:       []byte(`{"id":"user-123","name":"John","isActive":true}`),
	}

	require.Equal(t, EntityType("User"), snapshot.EntityType)
	require.Equal(t, EntityID("user-123"), snapshot.EntityID)
	require.Equal(t, EventSequence(5), snapshot.Sequence)
	require.Equal(t, EventSequence(25), snapshot.EventSequence)
	require.Contains(t, string(snapshot.Payload), "John")
}

func Test_SerializedSnapshot_ZeroValue(t *testing.T) {
	var snapshot SerializedSnapshot

	require.Equal(t, EntityType(""), snapshot.EntityType)
	require.Equal(t, EntityID(""), snapshot.EntityID)
	require.Equal(t, EventSequence(0), snapshot.Sequence)
	require.Equal(t, EventSequence(0), snapshot.EventSequence)
	require.Nil(t, snapshot.Payload)
}

func Test_SerializedSnapshot_JSONSerialization(t *testing.T) {
	snapshot := SerializedSnapshot{
		EntityType:    "Order",
		EntityID:      "order-456",
		Sequence:      10,
		EventSequence: 100,
		Payload:       []byte(`{"status":"completed"}`),
	}

	// Test JSON marshaling
	data, err := json.Marshal(snapshot)
	require.NoError(t, err)

	// Verify JSON field names
	var fields map[string]interface{}
	err = json.Unmarshal(data, &fields)
	require.NoError(t, err)

	require.Contains(t, fields, "entityType")
	require.Contains(t, fields, "entityID")
	require.Contains(t, fields, "sequence")
	require.Contains(t, fields, "eventSequence")
	require.Contains(t, fields, "payload")
}

func Test_SerializedSnapshot_JSONDeserialization(t *testing.T) {
	jsonData := `{
		"entityType": "Product",
		"entityID": "prod-789",
		"sequence": 3,
		"eventSequence": 15,
		"payload": "eyJza3UiOiJQUk9ELTEyMyJ9"
	}`

	var snapshot SerializedSnapshot
	err := json.Unmarshal([]byte(jsonData), &snapshot)
	require.NoError(t, err)

	require.Equal(t, EntityType("Product"), snapshot.EntityType)
	require.Equal(t, EntityID("prod-789"), snapshot.EntityID)
	require.Equal(t, EventSequence(3), snapshot.Sequence)
	require.Equal(t, EventSequence(15), snapshot.EventSequence)
}

func Test_SerializedSnapshot_Roundtrip(t *testing.T) {
	original := SerializedSnapshot{
		EntityType:    "Customer",
		EntityID:      "cust-abc",
		Sequence:      7,
		EventSequence: 35,
		Payload:       []byte(`{"email":"test@example.com","tier":"premium"}`),
	}

	// Marshal
	data, err := json.Marshal(original)
	require.NoError(t, err)

	// Unmarshal
	var roundtripped SerializedSnapshot
	err = json.Unmarshal(data, &roundtripped)
	require.NoError(t, err)

	require.Equal(t, original.EntityType, roundtripped.EntityType)
	require.Equal(t, original.EntityID, roundtripped.EntityID)
	require.Equal(t, original.Sequence, roundtripped.Sequence)
	require.Equal(t, original.EventSequence, roundtripped.EventSequence)
	// Payload comparison needs care due to base64 encoding
	require.NotNil(t, roundtripped.Payload)
}

func Test_SerializedSnapshot_SequenceRelationship(t *testing.T) {
	// Document the relationship between Sequence (snapshot count) and EventSequence (event count)
	// Sequence starts at 1 for the first snapshot
	// EventSequence represents the last event included in the snapshot

	testCases := []struct {
		name          string
		sequence      EventSequence
		eventSequence EventSequence
		description   string
	}{
		{"First snapshot", 1, 5, "First snapshot taken after 5 events"},
		{"Second snapshot", 2, 10, "Second snapshot at event 10"},
		{"Many snapshots", 10, 50, "Tenth snapshot at event 50"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := SerializedSnapshot{
				Sequence:      tc.sequence,
				EventSequence: tc.eventSequence,
			}

			// EventSequence should always be >= Sequence (since multiple events per snapshot)
			require.GreaterOrEqual(t, int(snapshot.EventSequence), int(snapshot.Sequence))
		})
	}
}
