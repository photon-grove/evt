package dynamo

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/stretchr/testify/require"
)

// These tests document the expected DynamoDB item structure for Events and Snapshots.
// They serve as behavioral invariants for the evt framework.

func Test_Event_Struct_Serialization(t *testing.T) {
	event := Event{
		PK:         evt.EntityID("test-entity-123"),
		SK:         evt.EventSequence(42),
		Type:       evt.EventType("UserCreated"),
		Version:    evt.EventVersion(1),
		EntityType: evt.EntityType("User"),
		Payload:    `{"name":"John","email":"john@example.com"}`,
		Metadata:   `{"region":"us-east-1","timestamp":"2024-01-01T00:00:00Z"}`,
	}

	// Test JSON serialization
	jsonData, err := json.Marshal(event)
	require.NoError(t, err)

	// Verify field names match expected DynamoDB schema
	var unmarshaled map[string]interface{}
	err = json.Unmarshal(jsonData, &unmarshaled)
	require.NoError(t, err)

	// Assert expected JSON field names (these match DynamoDB attribute names)
	require.Equal(t, "test-entity-123", unmarshaled["pk"])
	require.Equal(t, float64(42), unmarshaled["sk"])
	require.Equal(t, "UserCreated", unmarshaled["type"])
	require.Equal(t, float64(1), unmarshaled["version"])
	require.Equal(t, "User", unmarshaled["entityType"])
	require.Contains(t, unmarshaled["payload"], "name")
	require.Contains(t, unmarshaled["metadata"], "region")
}

func Test_Event_Struct_Deserialization(t *testing.T) {
	// This is the expected DynamoDB item format
	jsonData := `{
		"pk": "entity-456",
		"sk": 10,
		"type": "OrderPlaced",
		"version": 2,
		"entityType": "Order",
		"payload": "{\"orderId\":\"ord-123\",\"total\":99.99}",
		"metadata": "{\"region\":\"eu-west-1\"}"
	}`

	var event Event
	err := json.Unmarshal([]byte(jsonData), &event)
	require.NoError(t, err)

	require.Equal(t, evt.EntityID("entity-456"), event.PK)
	require.Equal(t, evt.EventSequence(10), event.SK)
	require.Equal(t, evt.EventType("OrderPlaced"), event.Type)
	require.Equal(t, evt.EventVersion(2), event.Version)
	require.Equal(t, evt.EntityType("Order"), event.EntityType)
	require.Contains(t, event.Payload, "orderId")
	require.Contains(t, event.Metadata, "region")
}

func Test_Event_ZeroValue(t *testing.T) {
	var event Event

	require.Equal(t, evt.EntityID(""), event.PK)
	require.Equal(t, evt.EventSequence(0), event.SK)
	require.Equal(t, evt.EventType(""), event.Type)
	require.Equal(t, evt.EventVersion(0), event.Version)
	require.Equal(t, evt.EntityType(""), event.EntityType)
	require.Equal(t, "", event.Payload)
	require.Equal(t, "", event.Metadata)
}

func Test_Event_PKSKNaming_Invariant(t *testing.T) {
	// IMPORTANT: These field names are critical for DynamoDB pk/sk naming.
	// The partition key is "pk" (entity ID) and the sort key is "sk" (event sequence).
	// Changing these would break backward compatibility with existing data.

	event := Event{PK: "test", SK: 1}
	jsonData, err := json.Marshal(event)
	require.NoError(t, err)

	// Verify the exact JSON field names
	var raw map[string]json.RawMessage
	err = json.Unmarshal(jsonData, &raw)
	require.NoError(t, err)

	_, hasPK := raw["pk"]
	_, hasSK := raw["sk"]
	require.True(t, hasPK, "Event must serialize pk as 'pk'")
	require.True(t, hasSK, "Event must serialize sk as 'sk'")
}

func Test_Snapshot_Struct_Serialization(t *testing.T) {
	snapshot := Snapshot{
		PK:            evt.EntityID("test-entity-789"),
		Sequence:      evt.EventSequence(3),
		EventSequence: evt.EventSequence(15),
		EntityType:    evt.EntityType("User"),
		Payload:       `{"id":"test-entity-789","name":"Jane","isActive":true}`,
	}

	// Test JSON serialization
	jsonData, err := json.Marshal(snapshot)
	require.NoError(t, err)

	// Verify field names match expected DynamoDB schema
	var unmarshaled map[string]interface{}
	err = json.Unmarshal(jsonData, &unmarshaled)
	require.NoError(t, err)

	// Assert expected JSON field names (these match DynamoDB attribute names)
	require.Equal(t, "test-entity-789", unmarshaled["pk"])
	require.Equal(t, float64(3), unmarshaled["seq"])
	require.Equal(t, float64(15), unmarshaled["eventSeq"])
	require.Equal(t, "User", unmarshaled["entityType"])
	require.Contains(t, unmarshaled["payload"], "name")
}

func Test_Snapshot_Struct_Deserialization(t *testing.T) {
	// This is the expected DynamoDB item format for snapshots
	jsonData := `{
		"pk": "entity-123",
		"seq": 5,
		"eventSeq": 25,
		"entityType": "Order",
		"payload": "{\"id\":\"entity-123\",\"status\":\"completed\"}"
	}`

	var snapshot Snapshot
	err := json.Unmarshal([]byte(jsonData), &snapshot)
	require.NoError(t, err)

	require.Equal(t, evt.EntityID("entity-123"), snapshot.PK)
	require.Equal(t, evt.EventSequence(5), snapshot.Sequence)
	require.Equal(t, evt.EventSequence(25), snapshot.EventSequence)
	require.Equal(t, evt.EntityType("Order"), snapshot.EntityType)
	require.Contains(t, snapshot.Payload, "status")
}

func Test_Snapshot_ZeroValue(t *testing.T) {
	var snapshot Snapshot

	require.Equal(t, evt.EntityID(""), snapshot.PK)
	require.Equal(t, evt.EventSequence(0), snapshot.Sequence)
	require.Equal(t, evt.EventSequence(0), snapshot.EventSequence)
	require.Equal(t, evt.EntityType(""), snapshot.EntityType)
	require.Equal(t, "", snapshot.Payload)
}

func Test_Snapshot_PKNaming_Invariant(t *testing.T) {
	// IMPORTANT: The partition key is "pk" (entity ID).
	// The snapshot sequence is "seq" and the event sequence is "eventSeq".
	// These field names are critical for DynamoDB key structure.

	snapshot := Snapshot{PK: "test", Sequence: 1, EventSequence: 5}
	jsonData, err := json.Marshal(snapshot)
	require.NoError(t, err)

	// Verify the exact JSON field names
	var raw map[string]json.RawMessage
	err = json.Unmarshal(jsonData, &raw)
	require.NoError(t, err)

	_, hasPK := raw["pk"]
	_, hasSeq := raw["seq"]
	_, hasEventSeq := raw["eventSeq"]
	require.True(t, hasPK, "Snapshot must serialize pk as 'pk'")
	require.True(t, hasSeq, "Snapshot must serialize sequence as 'seq'")
	require.True(t, hasEventSeq, "Snapshot must serialize event sequence as 'eventSeq'")
}

func Test_Event_Roundtrip(t *testing.T) {
	original := Event{
		PK:         evt.EntityID("user-abc-123"),
		SK:         evt.EventSequence(100),
		Type:       evt.EventType("UserUpdated"),
		Version:    evt.EventVersion(2),
		EntityType: evt.EntityType("User"),
		Payload:    `{"email":"updated@example.com"}`,
		Metadata:   `{"commandId":"cmd-456","region":"us-west-2"}`,
	}

	// Serialize
	jsonData, err := json.Marshal(original)
	require.NoError(t, err)

	// Deserialize
	var roundtripped Event
	err = json.Unmarshal(jsonData, &roundtripped)
	require.NoError(t, err)

	// Verify all fields match
	require.Equal(t, original.PK, roundtripped.PK)
	require.Equal(t, original.SK, roundtripped.SK)
	require.Equal(t, original.Type, roundtripped.Type)
	require.Equal(t, original.Version, roundtripped.Version)
	require.Equal(t, original.EntityType, roundtripped.EntityType)
	require.Equal(t, original.Payload, roundtripped.Payload)
	require.Equal(t, original.Metadata, roundtripped.Metadata)
}

func Test_Snapshot_Roundtrip(t *testing.T) {
	original := Snapshot{
		PK:            evt.EntityID("order-xyz-789"),
		Sequence:      evt.EventSequence(10),
		EventSequence: evt.EventSequence(50),
		EntityType:    evt.EntityType("Order"),
		Payload:       `{"items":[{"sku":"123","qty":2}],"total":199.99}`,
	}

	// Serialize
	jsonData, err := json.Marshal(original)
	require.NoError(t, err)

	// Deserialize
	var roundtripped Snapshot
	err = json.Unmarshal(jsonData, &roundtripped)
	require.NoError(t, err)

	// Verify all fields match
	require.Equal(t, original.PK, roundtripped.PK)
	require.Equal(t, original.Sequence, roundtripped.Sequence)
	require.Equal(t, original.EventSequence, roundtripped.EventSequence)
	require.Equal(t, original.EntityType, roundtripped.EntityType)
	require.Equal(t, original.Payload, roundtripped.Payload)
}

func Test_Event_PayloadTypes(t *testing.T) {
	testCases := []struct {
		name    string
		payload string
	}{
		{"Empty object", "{}"},
		{"Simple string field", `{"name":"test"}`},
		{"Nested object", `{"user":{"id":"1","name":"test"}}`},
		{"Array field", `{"items":["a","b","c"]}`},
		{"Numeric fields", `{"count":42,"price":19.99}`},
		{"Boolean fields", `{"isActive":true,"deleted":false}`},
		{"Null field", `{"optional":null}`},
		{"Complex nested", `{"data":{"nested":{"deep":{"value":123}}}}`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event := Event{
				PK:      "test",
				SK:      1,
				Payload: tc.payload,
			}

			jsonData, err := json.Marshal(event)
			require.NoError(t, err)

			var roundtripped Event
			err = json.Unmarshal(jsonData, &roundtripped)
			require.NoError(t, err)

			require.Equal(t, tc.payload, roundtripped.Payload)
		})
	}
}

func Test_Snapshot_LargePayload(t *testing.T) {
	// Test that large payloads serialize correctly
	largePayload := `{"data":"` + string(make([]byte, 10000)) + `"}`

	snapshot := Snapshot{
		PK:            "test",
		Sequence:      1,
		EventSequence: 100,
		EntityType:    "Test",
		Payload:       largePayload,
	}

	jsonData, err := json.Marshal(snapshot)
	require.NoError(t, err)

	var roundtripped Snapshot
	err = json.Unmarshal(jsonData, &roundtripped)
	require.NoError(t, err)

	require.Equal(t, snapshot.Payload, roundtripped.Payload)
}

// Test toDynamoItems function for Transaction conversion

// mockTransactionGroup implements both evt.TransactionGroup and dynamo.TransactionGroup for testing
type mockDynamoTransactionGroup struct {
	items       []types.TransactWriteItem
	storageType evt.StorageType
}

func (g *mockDynamoTransactionGroup) ToWriteItems() []types.TransactWriteItem {
	return g.items
}

func (g *mockDynamoTransactionGroup) MergeDynamo(_ TransactionGroup) (TransactionGroup, error) {
	return g, nil
}

func (g *mockDynamoTransactionGroup) TransactionType() evt.TransactionType {
	return "MockDynamo"
}

func (g *mockDynamoTransactionGroup) StorageType() evt.StorageType {
	return g.storageType
}

func (g *mockDynamoTransactionGroup) Len() int {
	return len(g.items)
}

func (g *mockDynamoTransactionGroup) HandleError(err error, _ int) error {
	return err
}

func (g *mockDynamoTransactionGroup) Merge(_ evt.TransactionGroup) (evt.TransactionGroup, error) {
	return g, nil
}

// nonDynamoTransactionGroup simulates a TransactionGroup for a different storage type
type nonDynamoTransactionGroup struct{}

func (g *nonDynamoTransactionGroup) TransactionType() evt.TransactionType { return "Other" }
func (g *nonDynamoTransactionGroup) StorageType() evt.StorageType         { return "Postgres" }
func (g *nonDynamoTransactionGroup) Len() int                             { return 1 }
func (g *nonDynamoTransactionGroup) HandleError(err error, _ int) error   { return err }
func (g *nonDynamoTransactionGroup) Merge(_ evt.TransactionGroup) (evt.TransactionGroup, error) {
	return g, nil
}

func Test_toDynamoItems_EmptyTransaction(t *testing.T) {
	result, err := toDynamoItems(nil)
	require.NoError(t, err)
	require.Nil(t, result)

	result, err = toDynamoItems(evt.Transaction{})
	require.NoError(t, err)
	require.Nil(t, result)
}

func Test_toDynamoItems_NilGroup(t *testing.T) {
	// Test with nil group in transaction
	transaction := evt.Transaction{nil}
	result, err := toDynamoItems(transaction)
	require.NoError(t, err)
	require.Nil(t, result)
}

func Test_toDynamoItems_Success(t *testing.T) {
	writeItems := []types.TransactWriteItem{
		{Put: &types.Put{TableName: strPtr("test-table")}},
		{Put: &types.Put{TableName: strPtr("test-table-2")}},
	}

	group := &mockDynamoTransactionGroup{
		items:       writeItems,
		storageType: StorageType,
	}

	transaction := evt.Transaction{group}
	result, err := toDynamoItems(transaction)
	require.NoError(t, err)
	require.Len(t, result, 2)
}

func Test_toDynamoItems_NonDynamoStorageType(t *testing.T) {
	group := &nonDynamoTransactionGroup{}
	transaction := evt.Transaction{group}

	_, err := toDynamoItems(transaction)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not based on DynamoDB")
}

func Test_toDynamoItems_MultipleGroups(t *testing.T) {
	group1 := &mockDynamoTransactionGroup{
		items: []types.TransactWriteItem{
			{Put: &types.Put{TableName: strPtr("table1")}},
		},
		storageType: StorageType,
	}
	group2 := &mockDynamoTransactionGroup{
		items: []types.TransactWriteItem{
			{Put: &types.Put{TableName: strPtr("table2")}},
			{Put: &types.Put{TableName: strPtr("table3")}},
		},
		storageType: StorageType,
	}

	transaction := evt.Transaction{group1, group2}
	result, err := toDynamoItems(transaction)
	require.NoError(t, err)
	require.Len(t, result, 3)
}

func strPtr(s string) *string {
	return &s
}
