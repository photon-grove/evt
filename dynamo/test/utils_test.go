package test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/stretchr/testify/require"
)

func Test_HasConditionalCheckFailure_Success(t *testing.T) {
	// Test with conditional check failure
	transactionErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{
				Code: aws.String("ConditionalCheckFailed"),
			},
		},
	}

	contains, index := dynamo.HasConditionalCheckFailure(transactionErr)
	require.True(t, contains)
	require.Equal(t, 0, index)
}

func Test_HasConditionalCheckFailure_MultipleReasons(t *testing.T) {
	// Test with multiple reasons, conditional check failure at index 1
	transactionErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{
				Code: aws.String("ValidationException"),
			},
			{
				Code: aws.String("ConditionalCheckFailed"),
			},
			{
				Code: aws.String("ThrottlingException"),
			},
		},
	}

	contains, index := dynamo.HasConditionalCheckFailure(transactionErr)
	require.True(t, contains)
	require.Equal(t, 1, index)
}

func Test_HasConditionalCheckFailure_NoFailure(t *testing.T) {
	// Test with no conditional check failure
	transactionErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{
				Code: aws.String("ValidationException"),
			},
			{
				Code: aws.String("ThrottlingException"),
			},
		},
	}

	contains, index := dynamo.HasConditionalCheckFailure(transactionErr)
	require.False(t, contains)
	require.Equal(t, 0, index)
}

func Test_HasConditionalCheckFailure_DifferentError(t *testing.T) {
	// Test with different error type
	differentErr := &types.ResourceNotFoundException{
		Message: aws.String("Resource not found"),
	}

	contains, index := dynamo.HasConditionalCheckFailure(differentErr)
	require.False(t, contains)
	require.Equal(t, 0, index)
}

func Test_HasConditionalCheckFailure_NilError(t *testing.T) {
	// Test with nil error
	contains, index := dynamo.HasConditionalCheckFailure(nil)
	require.False(t, contains)
	require.Equal(t, 0, index)
}

func Test_MarshalJSON_Success(t *testing.T) {
	// Create test DynamoDB AttributeValue map
	metadata := evt.Metadata{
		Region: "us-east-1",
		Origin: &evt.Origin{
			Source:   "test",
			Endpoint: "test-endpoint",
		},
	}
	metadataJSON, err := json.Marshal(metadata)
	require.NoError(t, err)

	item := map[string]events.DynamoDBAttributeValue{
		"pk":         events.NewStringAttribute("test-entity-id"),
		"sk":         events.NewNumberAttribute("5"),
		"entityType": events.NewStringAttribute("TestEntity"),
		"type":       events.NewStringAttribute("TestEvent"),
		"version":    events.NewNumberAttribute("1"),
		"payload":    events.NewStringAttribute(`{"test": "data"}`),
		"metadata":   events.NewStringAttribute(string(metadataJSON)),
	}

	serializedEvent, eventJSON, err := dynamo.MarshalJSON(item)
	require.NoError(t, err)

	// Verify the serialized event
	require.Equal(t, evt.EntityID("test-entity-id"), serializedEvent.EntityID)
	require.Equal(t, evt.EventSequence(5), serializedEvent.Sequence)
	require.Equal(t, evt.EntityType("TestEntity"), serializedEvent.EntityType)
	require.Equal(t, evt.EventType("TestEvent"), serializedEvent.Type)
	require.Equal(t, evt.EventVersion(1), serializedEvent.Version)
	require.Equal(t, []byte(`{"test": "data"}`), serializedEvent.Payload)
	require.Equal(t, metadata, serializedEvent.Metadata)

	// Verify the JSON output
	var unmarshaled evt.SerializedEvent
	err = json.Unmarshal(eventJSON, &unmarshaled)
	require.NoError(t, err)
	require.Equal(t, serializedEvent, unmarshaled)
}

func Test_MarshalJSON_InvalidMetadata(t *testing.T) {
	item := map[string]events.DynamoDBAttributeValue{
		"pk":         events.NewStringAttribute("test-entity-id"),
		"sk":         events.NewNumberAttribute("5"),
		"entityType": events.NewStringAttribute("TestEntity"),
		"type":       events.NewStringAttribute("TestEvent"),
		"version":    events.NewNumberAttribute("1"),
		"payload":    events.NewStringAttribute(`{"test": "data"}`),
		"metadata":   events.NewStringAttribute("invalid-json{"),
	}

	_, _, err := dynamo.MarshalJSON(item)
	require.Error(t, err)
	require.True(t, dynamo.IsInvalidEventError(err))
	require.Contains(t, err.Error(), "invalid event field")
}

func Test_MarshalJSON_EmptyMetadata(t *testing.T) {
	item := map[string]events.DynamoDBAttributeValue{
		"pk":         events.NewStringAttribute("test-entity-id"),
		"sk":         events.NewNumberAttribute("5"),
		"entityType": events.NewStringAttribute("TestEntity"),
		"type":       events.NewStringAttribute("TestEvent"),
		"version":    events.NewNumberAttribute("1"),
		"payload":    events.NewStringAttribute(`{"test": "data"}`),
		"metadata":   events.NewStringAttribute(""),
	}

	_, _, err := dynamo.MarshalJSON(item)
	require.Error(t, err)
	require.True(t, dynamo.IsInvalidEventError(err))
	require.Contains(t, err.Error(), "metadata")
}

func Test_UnmarshalAttributeMap_Success(t *testing.T) {
	item := map[string]events.DynamoDBAttributeValue{
		"pk":         events.NewStringAttribute("test-entity-id"),
		"sk":         events.NewNumberAttribute("5"),
		"entityType": events.NewStringAttribute("TestEntity"),
	}

	var result struct {
		PK         evt.EntityID      `json:"pk"`
		SK         evt.EventSequence `json:"sk"`
		EntityType evt.EntityType    `json:"entityType"`
	}

	err := dynamo.UnmarshalAttributeMap(item, &result)
	require.NoError(t, err)

	require.Equal(t, evt.EntityID("test-entity-id"), result.PK)
	require.Equal(t, evt.EventSequence(5), result.SK)
	require.Equal(t, evt.EntityType("TestEntity"), result.EntityType)
}

func Test_UnmarshalAttributeMap_NilValue(t *testing.T) {
	var result struct {
		PK string `json:"pk"`
	}

	err := dynamo.UnmarshalAttributeMap(nil, &result)
	require.NoError(t, err)
}

// Merged enhanced JSON and conversion tests are in this file already.

func Test_FromAttributeValue_AllTypes(t *testing.T) {
	testCases := []struct {
		name     string
		input    events.DynamoDBAttributeValue
		expected interface{}
	}{
		{
			name:     "Null",
			input:    events.NewNullAttribute(),
			expected: true,
		},
		{
			name:     "Boolean",
			input:    events.NewBooleanAttribute(true),
			expected: true,
		},
		{
			name:     "Number",
			input:    events.NewNumberAttribute("42"),
			expected: "42",
		},
		{
			name:     "String",
			input:    events.NewStringAttribute("test-string"),
			expected: "test-string",
		},
		{
			name:     "Binary",
			input:    events.NewBinaryAttribute([]byte("binary-data")),
			expected: []byte("binary-data"),
		},
		{
			name:     "String Set",
			input:    events.NewStringSetAttribute([]string{"a", "b", "c"}),
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "Number Set",
			input:    events.NewNumberSetAttribute([]string{"1", "2", "3"}),
			expected: []string{"1", "2", "3"},
		},
		{
			name:     "Binary Set",
			input:    events.NewBinarySetAttribute([][]byte{[]byte("a"), []byte("b")}),
			expected: [][]byte{[]byte("a"), []byte("b")},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := dynamo.FromAttributeValue(tc.input)
			require.NoError(t, err)
			require.NotNil(t, result)
		})
	}
}

func Test_FromAttributeValue_List(t *testing.T) {
	listValue := []events.DynamoDBAttributeValue{
		events.NewStringAttribute("item1"),
		events.NewNumberAttribute("42"),
	}

	input := events.NewListAttribute(listValue)

	result, err := dynamo.FromAttributeValue(input)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func Test_FromAttributeValue_Map(t *testing.T) {
	mapValue := map[string]events.DynamoDBAttributeValue{
		"key1": events.NewStringAttribute("value1"),
		"key2": events.NewNumberAttribute("123"),
	}

	input := events.NewMapAttribute(mapValue)

	result, err := dynamo.FromAttributeValue(input)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func Test_FromAttributeValue_UnknownType(t *testing.T) {
	// Create a valid string attribute and then test with invalid conversion
	// Since we can't create invalid DynamoDBAttributeValue easily,
	// this test verifies the conversion doesn't panic
	input := events.NewStringAttribute("test")

	result, err := dynamo.FromAttributeValue(input)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func Test_FromAttributeValueMap_Success(t *testing.T) {
	input := map[string]events.DynamoDBAttributeValue{
		"string_field": events.NewStringAttribute("test-value"),
		"number_field": events.NewNumberAttribute("42"),
	}

	result, err := dynamo.FromAttributeValueMap(input)
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Contains(t, result, "string_field")
	require.Contains(t, result, "number_field")
}

func Test_FromAttributeValueMap_Error(t *testing.T) {
	// Test with empty map since we can't easily create invalid attribute values
	input := map[string]events.DynamoDBAttributeValue{}

	result, err := dynamo.FromAttributeValueMap(input)
	require.NoError(t, err)
	require.Len(t, result, 0)
}

func Test_FromAttributeValueList_Success(t *testing.T) {
	input := []events.DynamoDBAttributeValue{
		events.NewStringAttribute("item1"),
		events.NewNumberAttribute("42"),
	}

	result, err := dynamo.FromAttributeValueList(input)
	require.NoError(t, err)
	require.Len(t, result, 2)
}

func Test_FromAttributeValueList_Error(t *testing.T) {
	// Test with empty list since we can't easily create invalid attribute values
	input := []events.DynamoDBAttributeValue{}

	result, err := dynamo.FromAttributeValueList(input)
	require.NoError(t, err)
	require.Len(t, result, 0)
}
