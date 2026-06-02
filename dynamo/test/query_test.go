package test

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Test retrieving Events for an Entity
func (s *RepositorySuite) Test_GetEvents_Success() {
	ctx := context.Background()

	input := dynamodb.QueryInput{
		TableName:              &s.repo.EventsTable,
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("pk = :pk AND sk > :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "test-id"},
			":sk": &types.AttributeValueMemberN{Value: "0"},
		},
	}

	expected := []evt.SerializedEvent{
		{
			ID:         evt.GetEventID("test-id", 2),
			EntityType: "test",
			EntityID:   "test-id",
			Sequence:   2,
			Type:       "test-event",
			Version:    1,
			Payload:    []byte{},
			Metadata:   evt.Metadata{Region: "us-east-1", Origin: &evt.Origin{Endpoint: "Testing"}},
		},
	}

	metadata, err := json.Marshal(evt.Metadata{Region: "us-east-1", Origin: &evt.Origin{Endpoint: "Testing"}})
	require.NoError(s.T(), err)

	output := new(dynamodb.QueryOutput)
	output.Items = []map[string]types.AttributeValue{
		{
			"pk":         &types.AttributeValueMemberS{Value: "test-id"},
			"sk":         &types.AttributeValueMemberN{Value: "2"},
			"type":       &types.AttributeValueMemberS{Value: "test-event"},
			"entityType": &types.AttributeValueMemberS{Value: "test"},
			"version":    &types.AttributeValueMemberN{Value: "1"},
			"payload":    &types.AttributeValueMemberS{Value: ""},
			"metadata":   &types.AttributeValueMemberS{Value: string(metadata)},
		},
	}

	s.client.On("Query", mock.Anything, &input, mock.Anything).Return(output, nil)

	eventsList, err := s.repo.GetEvents(ctx, "test-id")
	require.NoError(s.T(), err)

	s.client.AssertExpectations(s.T())

	require.Equal(s.T(), expected, eventsList)
}

// Test retrieving Events after a particular sequence for an Entity with a Snapshot
func (s *RepositorySuite) Test_GetLatestEvents_Success() {
	ctx := context.Background()

	input := dynamodb.QueryInput{
		TableName:              &s.repo.EventsTable,
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("pk = :pk AND sk > :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "test-id"},
			":sk": &types.AttributeValueMemberN{Value: "4"},
		},
	}

	expected := []evt.SerializedEvent{
		{
			ID:         evt.GetEventID("test-id", 2),
			EntityType: "test",
			EntityID:   "test-id",
			Sequence:   2,
			Type:       "test-event",
			Version:    1,
			Payload:    []byte{},
			Metadata:   evt.Metadata{Region: "us-east-1", Origin: &evt.Origin{Endpoint: "Testing"}},
		},
	}

	metadata, err := json.Marshal(evt.Metadata{Region: "us-east-1", Origin: &evt.Origin{Endpoint: "Testing"}})
	require.NoError(s.T(), err)

	output := new(dynamodb.QueryOutput)
	output.Items = []map[string]types.AttributeValue{
		{
			"pk":         &types.AttributeValueMemberS{Value: "test-id"},
			"sk":         &types.AttributeValueMemberN{Value: "2"},
			"type":       &types.AttributeValueMemberS{Value: "test-event"},
			"entityType": &types.AttributeValueMemberS{Value: "test"},
			"version":    &types.AttributeValueMemberN{Value: "1"},
			"payload":    &types.AttributeValueMemberS{Value: ""},
			"metadata":   &types.AttributeValueMemberS{Value: string(metadata)},
		},
	}

	s.client.On("Query", mock.Anything, &input, mock.Anything).Return(output, nil)

	eventsList, err := s.repo.GetLatestEvents(ctx, "test-id", 4)
	require.NoError(s.T(), err)

	s.client.AssertExpectations(s.T())

	require.Equal(s.T(), expected, eventsList)
}

// Test queryEvents pagination via repository GetEvents
func (s *RepositorySuite) Test_GetEvents_Pagination() {
	ctx := context.Background()

	output1 := &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{{
		"pk":         &types.AttributeValueMemberS{Value: "test-id"},
		"sk":         &types.AttributeValueMemberN{Value: "1"},
		"type":       &types.AttributeValueMemberS{Value: "test-event"},
		"entityType": &types.AttributeValueMemberS{Value: "TestEntity"},
		"version":    &types.AttributeValueMemberN{Value: "1"},
		"payload":    &types.AttributeValueMemberS{Value: `{"test": "data1"}`},
		"metadata":   &types.AttributeValueMemberS{Value: `{"region": "us-east-1"}`},
	}}, LastEvaluatedKey: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "test-id"}, "sk": &types.AttributeValueMemberN{Value: "1"}}}
	output2 := &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{{
		"pk":         &types.AttributeValueMemberS{Value: "test-id"},
		"sk":         &types.AttributeValueMemberN{Value: "2"},
		"type":       &types.AttributeValueMemberS{Value: "test-event"},
		"entityType": &types.AttributeValueMemberS{Value: "TestEntity"},
		"version":    &types.AttributeValueMemberN{Value: "1"},
		"payload":    &types.AttributeValueMemberS{Value: `{"test": "data2"}`},
		"metadata":   &types.AttributeValueMemberS{Value: `{"region": "us-east-1"}`},
	}}, LastEvaluatedKey: nil}

	s.client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(output1, nil).Once()
	s.client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(output2, nil).Once()

	eventsOut, err := s.repo.GetEvents(ctx, "test-id")
	require.NoError(s.T(), err)
	require.Len(s.T(), eventsOut, 2)
	s.client.AssertExpectations(s.T())
}

// Test queryEvents with unmarshal error in results
func (s *RepositorySuite) Test_GetEvents_UnmarshalError() {
	ctx := context.Background()
	output := &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{{
		"pk":         &types.AttributeValueMemberS{Value: "test-id"},
		"sk":         &types.AttributeValueMemberN{Value: "1"},
		"type":       &types.AttributeValueMemberS{Value: "test-event"},
		"entityType": &types.AttributeValueMemberS{Value: "TestEntity"},
		"version":    &types.AttributeValueMemberN{Value: "1"},
		"payload":    &types.AttributeValueMemberS{Value: `{"test": "data"}`},
		"metadata":   &types.AttributeValueMemberS{Value: "invalid-json{"},
	}}, LastEvaluatedKey: nil}
	s.client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(output, nil)
	eventsOut, err := s.repo.GetEvents(ctx, "test-id")
	require.Error(s.T(), err)
	require.Nil(s.T(), eventsOut)
	require.Contains(s.T(), err.Error(), "invalid character")
	s.client.AssertExpectations(s.T())
}

// Test GetEvents with empty result
func (s *RepositorySuite) Test_GetEvents_EmptyResult() {
	ctx := context.Background()
	entityID := evt.EntityID("test-id")

	input := dynamodb.QueryInput{
		TableName:              &s.repo.EventsTable,
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("pk = :pk AND sk > :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "test-id"},
			":sk": &types.AttributeValueMemberN{Value: "0"},
		},
	}

	output := &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{}}

	s.client.On("Query", mock.Anything, &input, mock.Anything).Return(output, nil)

	eventsList, err := s.repo.GetEvents(ctx, entityID)
	require.NoError(s.T(), err)
	require.Empty(s.T(), eventsList)
	s.client.AssertExpectations(s.T())
}

// Test GetEvents with DynamoDB error
func (s *RepositorySuite) Test_GetEvents_DynamoError() {
	ctx := context.Background()
	entityID := evt.EntityID("test-id")

	expectedError := "DynamoDB query failed"
	s.client.On("Query", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.QueryOutput)(nil), errors.New(expectedError))

	eventsList, err := s.repo.GetEvents(ctx, entityID)
	require.Error(s.T(), err)
	require.Nil(s.T(), eventsList)
	require.Contains(s.T(), err.Error(), expectedError)
	s.client.AssertExpectations(s.T())
}

// Test GetLatestEvents with error
func (s *RepositorySuite) Test_GetLatestEvents_Error() {
	ctx := context.Background()
	entityID := evt.EntityID("test-id")
	lastSequence := evt.EventSequence(5)

	expectedError := "query failed"
	s.client.On("Query", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.QueryOutput)(nil), errors.New(expectedError))

	eventsList, err := s.repo.GetLatestEvents(ctx, entityID, lastSequence)
	require.Error(s.T(), err)
	require.Nil(s.T(), eventsList)
	require.Contains(s.T(), err.Error(), expectedError)
	s.client.AssertExpectations(s.T())
}
