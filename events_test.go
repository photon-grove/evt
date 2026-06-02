package evt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_EventTypes_TypeDefinitions(t *testing.T) {
	// Test that our type definitions work correctly
	var eventType EventType = "UserCreated"
	var eventID EventID = "user-123:5"
	var eventSequence EventSequence = 42
	var eventVersion EventVersion = 3

	require.Equal(t, "UserCreated", string(eventType))
	require.Equal(t, "user-123:5", string(eventID))
	require.Equal(t, 42, int(eventSequence))
	require.Equal(t, 3, int(eventVersion))
}

func Test_GetEventID(t *testing.T) {
	testCases := []struct {
		name       string
		entityID   EntityID
		sequence   EventSequence
		expectedID EventID
	}{
		{
			name:       "Simple ID and sequence",
			entityID:   "user-123",
			sequence:   1,
			expectedID: "user-123:1",
		},
		{
			name:       "UUID entity ID",
			entityID:   "550e8400-e29b-41d4-a716-446655440000",
			sequence:   42,
			expectedID: "550e8400-e29b-41d4-a716-446655440000:42",
		},
		{
			name:       "Zero sequence",
			entityID:   "test-entity",
			sequence:   0,
			expectedID: "test-entity:0",
		},
		{
			name:       "Large sequence number",
			entityID:   "entity-1",
			sequence:   9999999,
			expectedID: "entity-1:9999999",
		},
		{
			name:       "Entity ID with special characters",
			entityID:   "user-with-dashes_and_underscores",
			sequence:   10,
			expectedID: "user-with-dashes_and_underscores:10",
		},
		{
			name:       "Empty entity ID",
			entityID:   "",
			sequence:   1,
			expectedID: ":1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GetEventID(tc.entityID, tc.sequence)
			require.Equal(t, tc.expectedID, result)
		})
	}
}

func Test_GetEventID_ConsistentFormatting(t *testing.T) {
	// Test that GetEventID always produces the same format for the same inputs
	entityID := EntityID("test-entity")
	sequence := EventSequence(5)

	result1 := GetEventID(entityID, sequence)
	result2 := GetEventID(entityID, sequence)

	require.Equal(t, result1, result2)
	require.Equal(t, "test-entity:5", string(result1))
}

func Test_Context_StructFields(t *testing.T) {
	// Test Context struct creation and field access
	entityID := EntityID("test-entity-123")
	currentSeq := EventSequence(10)
	snapshotSeq := EventSequence(5)

	// Create a mock entity (we'll use nil since we're just testing the struct)
	var entity Entity

	ctx := Context{
		Entity:          entity,
		EntityID:        entityID,
		CurrentSequence: &currentSeq,
		CurrentSnapshot: &snapshotSeq,
	}

	require.Equal(t, entity, ctx.Entity)
	require.Equal(t, entityID, ctx.EntityID)
	require.NotNil(t, ctx.CurrentSequence)
	require.Equal(t, currentSeq, *ctx.CurrentSequence)
	require.NotNil(t, ctx.CurrentSnapshot)
	require.Equal(t, snapshotSeq, *ctx.CurrentSnapshot)
}

func Test_Context_NilPointers(t *testing.T) {
	// Test Context with nil pointer fields
	entityID := EntityID("test-entity")

	ctx := Context{
		Entity:          nil,
		EntityID:        entityID,
		CurrentSequence: nil,
		CurrentSnapshot: nil,
	}

	require.Nil(t, ctx.Entity)
	require.Equal(t, entityID, ctx.EntityID)
	require.Nil(t, ctx.CurrentSequence)
	require.Nil(t, ctx.CurrentSnapshot)
}

func Test_Context_PointerManipulation(t *testing.T) {
	// Test working with sequence pointers
	seq1 := EventSequence(5)
	seq2 := EventSequence(10)

	ctx := Context{
		EntityID:        "test",
		CurrentSequence: &seq1,
		CurrentSnapshot: &seq2,
	}

	// Verify initial values
	require.Equal(t, EventSequence(5), *ctx.CurrentSequence)
	require.Equal(t, EventSequence(10), *ctx.CurrentSnapshot)

	// Modify the values through the pointers
	*ctx.CurrentSequence = 15
	*ctx.CurrentSnapshot = 8

	require.Equal(t, EventSequence(15), *ctx.CurrentSequence)
	require.Equal(t, EventSequence(8), *ctx.CurrentSnapshot)

	// The original variables should also be changed since they share the same memory
	require.Equal(t, EventSequence(15), seq1)
	require.Equal(t, EventSequence(8), seq2)
}

// Mock implementations for testing Event interface
type MockEvent struct {
	EventType    EventType
	EventVersion EventVersion
	EntType      EntityType
	EntID        EntityID
}

func (e MockEvent) Type() EventType {
	return e.EventType
}

func (e MockEvent) Version() EventVersion {
	return e.EventVersion
}

func (e MockEvent) EntityType() EntityType {
	return e.EntType
}

func (e MockEvent) EntityID() EntityID {
	return e.EntID
}

func Test_Event_Interface(t *testing.T) {
	// Test that our mock event properly implements the Event interface
	event := MockEvent{
		EventType:    "UserCreated",
		EventVersion: 1,
		EntType:      "User",
		EntID:        "user-123",
	}

	// Test interface methods
	require.Equal(t, EventType("UserCreated"), event.Type())
	require.Equal(t, EventVersion(1), event.Version())
	require.Equal(t, EntityType("User"), event.EntityType())
	require.Equal(t, EntityID("user-123"), event.EntityID())

	// Test that it can be used as Event interface
	var e Event = event
	require.Equal(t, EventType("UserCreated"), e.Type())
	require.Equal(t, EventVersion(1), e.Version())
	require.Equal(t, EntityType("User"), e.EntityType())
	require.Equal(t, EntityID("user-123"), e.EntityID())
}

func Test_Event_DifferentTypes(t *testing.T) {
	testCases := []struct {
		name  string
		event MockEvent
	}{
		{
			name: "User event",
			event: MockEvent{
				EventType:    "UserCreated",
				EventVersion: 1,
				EntType:      "User",
				EntID:        "user-123",
			},
		},
		{
			name: "Order event",
			event: MockEvent{
				EventType:    "OrderPlaced",
				EventVersion: 2,
				EntType:      "Order",
				EntID:        "order-456",
			},
		},
		{
			name: "Event with special characters",
			event: MockEvent{
				EventType:    "Payment.Processed",
				EventVersion: 3,
				EntType:      "Payment",
				EntID:        "payment_789",
			},
		},
		{
			name: "Event with version 0",
			event: MockEvent{
				EventType:    "SystemInitialized",
				EventVersion: 0,
				EntType:      "System",
				EntID:        "system-root",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var e Event = tc.event

			require.Equal(t, tc.event.EventType, e.Type())
			require.Equal(t, tc.event.EventVersion, e.Version())
			require.Equal(t, tc.event.EntType, e.EntityType())
			require.Equal(t, tc.event.EntID, e.EntityID())
		})
	}
}

func Test_EventSequence_Arithmetic(t *testing.T) {
	// Test basic arithmetic operations with EventSequence
	seq1 := EventSequence(10)
	seq2 := EventSequence(5)

	// Addition
	sum := seq1 + seq2
	require.Equal(t, EventSequence(15), sum)

	// Subtraction
	diff := seq1 - seq2
	require.Equal(t, EventSequence(5), diff)

	// Increment
	seq1++
	require.Equal(t, EventSequence(11), seq1)

	// Decrement
	seq2--
	require.Equal(t, EventSequence(4), seq2)
}

func Test_EventSequence_Comparison(t *testing.T) {
	// Test comparison operations with EventSequence
	seq1 := EventSequence(10)
	seq2 := EventSequence(5)
	seq3 := EventSequence(10)

	// Equality
	require.True(t, seq1 == seq3)
	require.False(t, seq1 == seq2)

	// Inequality
	require.False(t, seq1 != seq3)
	require.True(t, seq1 != seq2)

	// Ordering
	require.True(t, seq1 > seq2)
	require.False(t, seq2 > seq1)
	require.True(t, seq2 < seq1)
	require.False(t, seq1 < seq2)

	// Greater than or equal
	require.True(t, seq1 >= seq3)
	require.True(t, seq1 >= seq2)
	require.False(t, seq2 >= seq1)

	// Less than or equal
	require.True(t, seq1 <= seq3)
	require.False(t, seq1 <= seq2)
	require.True(t, seq2 <= seq1)
}

func Test_EventVersion_Comparison(t *testing.T) {
	// Test version comparison for schema evolution
	v1 := EventVersion(1)
	v2 := EventVersion(2)
	v1Copy := EventVersion(1)

	require.True(t, v1 == v1Copy)
	require.False(t, v1 == v2)
	require.True(t, v1 < v2)
	require.False(t, v2 < v1)
	require.True(t, v2 > v1)
	require.True(t, v1 <= v1Copy)
	require.True(t, v2 >= v1)
}

func Test_EventID_Parsing(t *testing.T) {
	// Test that we can work with EventID strings
	eventID := EventID("user-123:42")

	// Convert to string and verify format
	idStr := string(eventID)
	require.Equal(t, "user-123:42", idStr)

	// Test that we can create EventID from GetEventID and it's consistent
	reconstructed := GetEventID("user-123", 42)
	require.Equal(t, eventID, reconstructed)
}

func Test_Types_ZeroValues(t *testing.T) {
	// Test zero values of our types
	var eventType EventType
	var eventID EventID
	var eventSequence EventSequence
	var eventVersion EventVersion

	require.Equal(t, "", string(eventType))
	require.Equal(t, "", string(eventID))
	require.Equal(t, 0, int(eventSequence))
	require.Equal(t, 0, int(eventVersion))
}

func Test_Context_ZeroValue(t *testing.T) {
	// Test zero value of Context
	var ctx Context

	require.Nil(t, ctx.Entity)
	require.Equal(t, EntityID(""), ctx.EntityID)
	require.Nil(t, ctx.CurrentSequence)
	require.Nil(t, ctx.CurrentSnapshot)
	require.Nil(t, ctx.SeenCommandIDs)
}

func Test_Context_HasCommandID_NilMap(t *testing.T) {
	ctx := Context{}
	require.False(t, ctx.HasCommandID("cmd-1"))
}

func Test_Context_RecordAndHasCommandID(t *testing.T) {
	ctx := Context{}
	ctx.RecordCommandID("cmd-1")

	require.True(t, ctx.HasCommandID("cmd-1"))
	require.False(t, ctx.HasCommandID("cmd-2"))
}

func Test_Context_RecordCommandID_MultipleIDs(t *testing.T) {
	ctx := Context{}
	ctx.RecordCommandID("a")
	ctx.RecordCommandID("b")
	ctx.RecordCommandID("c")

	require.True(t, ctx.HasCommandID("a"))
	require.True(t, ctx.HasCommandID("b"))
	require.True(t, ctx.HasCommandID("c"))
	require.False(t, ctx.HasCommandID("d"))
}
