package evt

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests document the SerializedView structure and its serialization behavior.

func Test_SerializedView_Creation(t *testing.T) {
	view := SerializedView{
		PK:         "user:user-123",
		EntityID:   EntityID("user-123"),
		EntityType: EntityType("User"),
		Payload:    []byte(`{"id":"user-123","name":"John","email":"john@example.com"}`),
	}

	require.Equal(t, "user:user-123", view.PK)
	require.Equal(t, EntityID("user-123"), view.EntityID)
	require.Equal(t, EntityType("User"), view.EntityType)
	require.Contains(t, string(view.Payload), "email")
}

func Test_SerializedView_ZeroValue(t *testing.T) {
	var view SerializedView

	require.Equal(t, "", view.PK)
	require.Equal(t, EntityID(""), view.EntityID)
	require.Equal(t, EntityType(""), view.EntityType)
	require.Nil(t, view.Payload)
}

func Test_SerializedView_JSONSerialization(t *testing.T) {
	view := SerializedView{
		PK:         "order:order-456",
		EntityID:   "order-456",
		EntityType: "Order",
		Payload:    []byte(`{"status":"pending"}`),
	}

	// Test JSON marshaling
	data, err := json.Marshal(view)
	require.NoError(t, err)

	// Verify JSON field names
	var fields map[string]interface{}
	err = json.Unmarshal(data, &fields)
	require.NoError(t, err)

	require.Contains(t, fields, "pk")
	require.Contains(t, fields, "entityID")
	require.Contains(t, fields, "entityType")
	require.Contains(t, fields, "payload")
}

func Test_SerializedView_JSONDeserialization(t *testing.T) {
	jsonData := `{
		"pk": "product:prod-789",
		"entityID": "prod-789",
		"entityType": "Product",
		"payload": "eyJza3UiOiJQUk9ELTEyMyIsInByaWNlIjo5OS45OX0="
	}`

	var view SerializedView
	err := json.Unmarshal([]byte(jsonData), &view)
	require.NoError(t, err)

	require.Equal(t, "product:prod-789", view.PK)
	require.Equal(t, EntityID("prod-789"), view.EntityID)
	require.Equal(t, EntityType("Product"), view.EntityType)
}

func Test_SerializedView_Roundtrip(t *testing.T) {
	original := SerializedView{
		PK:         "customer:cust-abc",
		EntityID:   "cust-abc",
		EntityType: "Customer",
		Payload:    []byte(`{"name":"Jane","tier":"gold"}`),
	}

	// Marshal
	data, err := json.Marshal(original)
	require.NoError(t, err)

	// Unmarshal
	var roundtripped SerializedView
	err = json.Unmarshal(data, &roundtripped)
	require.NoError(t, err)

	require.Equal(t, original.PK, roundtripped.PK)
	require.Equal(t, original.EntityID, roundtripped.EntityID)
	require.Equal(t, original.EntityType, roundtripped.EntityType)
	require.NotNil(t, roundtripped.Payload)
}

func Test_SerializedView_PKFormats(t *testing.T) {
	// Document common PK formats used for views
	testCases := []struct {
		name       string
		pk         string
		entityType EntityType
		entityID   EntityID
	}{
		{
			"Entity ID only",
			"user-123",
			"User",
			"user-123",
		},
		{
			"Type prefixed",
			"User:user-123",
			"User",
			"user-123",
		},
		{
			"Composite key",
			"org:org-1:user:user-123",
			"User",
			"user-123",
		},
		{
			"UUID format",
			"550e8400-e29b-41d4-a716-446655440000",
			"Entity",
			"550e8400-e29b-41d4-a716-446655440000",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			view := SerializedView{
				PK:         tc.pk,
				EntityID:   tc.entityID,
				EntityType: tc.entityType,
			}

			require.NotEmpty(t, view.PK)
			require.NotEmpty(t, view.EntityID)
			require.NotEmpty(t, view.EntityType)
		})
	}
}

func Test_SerializedView_PayloadTypes(t *testing.T) {
	// Test various payload structures
	testCases := []struct {
		name    string
		payload string
	}{
		{"Empty object", `{}`},
		{"Simple fields", `{"name":"test","count":42}`},
		{"Nested object", `{"user":{"name":"test","address":{"city":"NYC"}}}`},
		{"Array field", `{"items":["a","b","c"]}`},
		{"Mixed types", `{"active":true,"price":19.99,"tags":["sale"]}`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			view := SerializedView{
				PK:      "test",
				Payload: []byte(tc.payload),
			}

			// Ensure payload is valid JSON
			var parsed interface{}
			err := json.Unmarshal(view.Payload, &parsed)
			require.NoError(t, err)
		})
	}
}
