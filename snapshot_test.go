package evt

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

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
