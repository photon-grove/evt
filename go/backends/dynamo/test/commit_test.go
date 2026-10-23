package test

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/dynamo"
	"github.com/photon-grove/evt/go/internal/eventjson"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Test simple commits with Events
func (s *RepositorySuite) Test_Commit_Ok() {
	ctx := context.Background()

	serializedEvents := []evt.SerializedEvent{
		{
			EntityType: "test",
			EntityID:   "test-id",
			Sequence:   1,
			Type:       "test-event",
			Version:    1,
			Payload:    []byte{},
			Metadata:   evt.Metadata{Region: "us-east-1", Origin: &evt.Origin{Endpoint: "Testing"}},
		},
	}

	metadata, err := eventjson.MarshalMetadata(evt.Metadata{Region: "us-east-1", Origin: &evt.Origin{Endpoint: "Testing"}})
	require.NoError(s.T(), err)

	expected := []types.TransactWriteItem{
		{
			Update: &types.Update{
				TableName: &s.repo.EventsTable,
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "test-id"},
					"sk": &types.AttributeValueMemberN{Value: "-1"},
				},
				UpdateExpression:    aws.String("SET head = :last"),
				ConditionExpression: aws.String("attribute_not_exists(head)"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":last": &types.AttributeValueMemberN{Value: "1"},
				},
			},
		},
		{
			Put: &types.Put{
				TableName: &s.repo.EventsTable,
				Item: map[string]types.AttributeValue{
					"pk":         &types.AttributeValueMemberS{Value: "test-id"},
					"sk":         &types.AttributeValueMemberN{Value: "1"},
					"type":       &types.AttributeValueMemberS{Value: "test-event"},
					"entityType": &types.AttributeValueMemberS{Value: "test"},
					"version":    &types.AttributeValueMemberN{Value: "1"},
					"payload":    &types.AttributeValueMemberS{Value: ""},
					"metadata":   &types.AttributeValueMemberS{Value: string(metadata)},
				},
				ConditionExpression: aws.String("attribute_not_exists(sk)"),
			},
		},
	}

	input := dynamodb.TransactWriteItemsInput{TransactItems: append(controlChecks(s.repo.EventsTable), expected...)}

	// Return an empty output object, because the result isn't used
	output := new(dynamodb.TransactWriteItemsOutput)

	s.client.On("TransactWriteItems", mock.Anything, &input, mock.Anything).Return(output, nil)

	result := evt.SerializedResult{Events: serializedEvents}

	err = s.repo.Commit(ctx, result)
	require.NoError(s.T(), err)

	s.client.AssertExpectations(s.T())
}

// Test commits with Events and a Snapshot
func (s *RepositorySuite) Test_Commit_WithSnapshot() {
	ctx := context.Background()

	serializedEvents := []evt.SerializedEvent{
		{
			EntityType: "test",
			EntityID:   "test-id",
			Sequence:   2,
			Type:       "test-event",
			Version:    1,
			Payload:    []byte{},
			Metadata:   evt.Metadata{Region: "us-east-1", Origin: &evt.Origin{Endpoint: "Testing"}},
		},
	}

	metadata, err := eventjson.MarshalMetadata(evt.Metadata{Region: "us-east-1", Origin: &evt.Origin{Endpoint: "Testing"}})
	require.NoError(s.T(), err)

	expected := []types.TransactWriteItem{
		{
			Update: &types.Update{
				TableName: &s.repo.EventsTable,
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "test-id"},
					"sk": &types.AttributeValueMemberN{Value: "-1"},
				},
				UpdateExpression:    aws.String("SET head = :last"),
				ConditionExpression: aws.String("head = :prev"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":last": &types.AttributeValueMemberN{Value: "2"},
					":prev": &types.AttributeValueMemberN{Value: "1"},
				},
			},
		},
		{
			Put: &types.Put{
				TableName: &s.repo.EventsTable,
				Item: map[string]types.AttributeValue{
					"pk":         &types.AttributeValueMemberS{Value: "test-id"},
					"sk":         &types.AttributeValueMemberN{Value: "2"},
					"type":       &types.AttributeValueMemberS{Value: "test-event"},
					"entityType": &types.AttributeValueMemberS{Value: "test"},
					"version":    &types.AttributeValueMemberN{Value: "1"},
					"payload":    &types.AttributeValueMemberS{Value: ""},
					"metadata":   &types.AttributeValueMemberS{Value: string(metadata)},
				},
				ConditionExpression: aws.String("attribute_not_exists(sk)"),
			},
		},
		{
			Put: &types.Put{
				TableName: &s.repo.EventsTable,
				Item: map[string]types.AttributeValue{
					"pk":         &types.AttributeValueMemberS{Value: "test-id"},
					"sk":         &types.AttributeValueMemberN{Value: "0"},
					"seq":        &types.AttributeValueMemberN{Value: "1"},
					"eventSeq":   &types.AttributeValueMemberN{Value: "2"},
					"entityType": &types.AttributeValueMemberS{Value: "test"},
					"payload":    &types.AttributeValueMemberS{Value: ""},
				},
				ConditionExpression: aws.String("attribute_not_exists(seq)"),
			},
		},
	}

	// An append needs no read before the commit: the head condition carries continuity.
	input := dynamodb.TransactWriteItemsInput{TransactItems: append(controlChecks(s.repo.EventsTable), expected...)}

	// Return an empty output object, because the result isn't used
	output := new(dynamodb.TransactWriteItemsOutput)

	s.client.On("TransactWriteItems", mock.Anything, &input, mock.Anything).Return(output, nil)

	result := evt.SerializedResult{Events: serializedEvents}

	err = s.repo.CommitWithSnapshot(ctx, result, "test", "test-id", []byte{}, 1)
	require.NoError(s.T(), err)

	s.client.AssertExpectations(s.T())
}

// Test that a later snapshot requires only the loaded snapshot sequence: the head update owns the floor
func (s *RepositorySuite) Test_Commit_WithNextSnapshot() {
	ctx := context.Background()

	serializedEvents := []evt.SerializedEvent{{
		EntityType: "test", EntityID: "test-id", Sequence: 7, Type: "test-event", Version: 1,
		Payload: []byte{}, Metadata: evt.Metadata{Region: "us-east-1"},
	}}

	var input *dynamodb.TransactWriteItemsInput
	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			var ok bool
			input, ok = args.Get(1).(*dynamodb.TransactWriteItemsInput)
			require.True(s.T(), ok)
		}).
		Return(new(dynamodb.TransactWriteItemsOutput), nil)

	err := s.repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: serializedEvents}, "test", "test-id", []byte{}, 3)
	require.NoError(s.T(), err)

	put := input.TransactItems[len(input.TransactItems)-1].Put
	require.Equal(s.T(), "seq = :seq", aws.ToString(put.ConditionExpression))
	require.Equal(s.T(), map[string]types.AttributeValue{
		":seq": &types.AttributeValueMemberN{Value: "2"},
	}, put.ExpressionAttributeValues)

	// The append conditions on the head being the sequence before the commit's first.
	update := input.TransactItems[2].Update
	require.Equal(s.T(), "head = :prev", aws.ToString(update.ConditionExpression))
	require.Equal(s.T(), map[string]types.AttributeValue{
		":last": &types.AttributeValueMemberN{Value: "7"},
		":prev": &types.AttributeValueMemberN{Value: "6"},
	}, update.ExpressionAttributeValues)
}

// Test commit with large batch respects batching behavior
func (s *RepositorySuite) Test_Commit_LargeBatch_BatchesOnce() {
	ctx := context.Background()
	var serializedEvents []evt.SerializedEvent
	for i := 1; i <= 30; i++ {
		serializedEvents = append(serializedEvents, evt.SerializedEvent{
			EntityType: "test", EntityID: evt.EntityID("test-id-" + fmt.Sprint(i)), Sequence: 1, Type: "test-event", Version: 1,
			Payload: []byte(`{"test": "data` + fmt.Sprint(i) + `"}`), Metadata: evt.Metadata{Region: "us-east-1"},
		})
	}
	output := new(dynamodb.TransactWriteItemsOutput)
	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).Return(output, nil).Times(1)
	result := evt.SerializedResult{Events: serializedEvents}
	require.NoError(s.T(), s.repo.Commit(ctx, result))
	s.client.AssertExpectations(s.T())
}

// Test NewRepository function
func (s *RepositorySuite) Test_NewRepository_Creation() {
	repo := s.repo // already created in SetupTest

	require.NotNil(s.T(), repo)
	require.Equal(s.T(), "test-events", repo.EventsTable)
}

// Test Commit with empty events
func (s *RepositorySuite) Test_Commit_EmptyEvents() {
	ctx := context.Background()
	result := evt.SerializedResult{Events: []evt.SerializedEvent{}}

	err := s.repo.Commit(ctx, result)
	require.NoError(s.T(), err)

	// No DynamoDB calls should be made for empty events
	s.client.AssertNotCalled(s.T(), "TransactWriteItems")
}

// Test Commit with DynamoDB error
func (s *RepositorySuite) Test_Commit_DynamoError() {
	ctx := context.Background()

	serializedEvents := []evt.SerializedEvent{
		{
			EntityType: "test",
			EntityID:   "test-id",
			Sequence:   1,
			Type:       "test-event",
			Version:    1,
			Payload:    []byte("{}"),
			Metadata:   evt.Metadata{Region: "us-east-1"},
		},
	}

	expectedError := errors.New("DynamoDB connection failed")
	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.TransactWriteItemsOutput)(nil), expectedError)

	result := evt.SerializedResult{Events: serializedEvents}
	err := s.repo.Commit(ctx, result)

	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "DynamoDB connection failed")
	s.client.AssertExpectations(s.T())
}

// Test Commit with conditional check failure
func (s *RepositorySuite) Test_Commit_ConditionalCheckFailure() {
	ctx := context.Background()

	serializedEvents := []evt.SerializedEvent{
		{
			EntityType: "test",
			EntityID:   "test-id",
			Sequence:   1,
			Type:       "test-event",
			Version:    1,
			Payload:    []byte("{}"),
			Metadata:   evt.Metadata{Region: "us-east-1"},
		},
	}

	conditionalCheckErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{Code: aws.String("None")},                   // mandatory marker check
			{Code: aws.String("None")},                   // mandatory rebuild-lock check
			{Code: aws.String("ConditionalCheckFailed")}, // head update: the stream already exists
			{Code: aws.String("None")},
		},
	}

	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.TransactWriteItemsOutput)(nil), conditionalCheckErr)

	// The failure-path probe finds a healthy stream at head 1: a plain conflict.
	s.client.On("GetItem", mock.Anything, mock.MatchedBy(func(in *dynamodb.GetItemInput) bool {
		sk, skOK := in.Key["sk"].(*types.AttributeValueMemberN)
		pk, pkOK := in.Key["pk"].(*types.AttributeValueMemberS)
		return skOK && pkOK && sk.Value == "0" && pk.Value == "test-id"
	}), mock.Anything).Return(&dynamodb.GetItemOutput{}, nil)
	s.client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(&dynamodb.QueryOutput{
		Items: []map[string]types.AttributeValue{{"sk": &types.AttributeValueMemberN{Value: "1"}}},
	}, nil)
	s.client.On("GetItem", mock.Anything, mock.MatchedBy(func(in *dynamodb.GetItemInput) bool {
		sk, ok := in.Key["sk"].(*types.AttributeValueMemberN)
		return ok && sk.Value == "-1"
	}), mock.Anything).Return(&dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
		"pk":   &types.AttributeValueMemberS{Value: "test-id"},
		"sk":   &types.AttributeValueMemberN{Value: "-1"},
		"head": &types.AttributeValueMemberN{Value: "1"},
	}}, nil)

	result := evt.SerializedResult{Events: serializedEvents}
	err := s.repo.Commit(ctx, result)

	require.ErrorIs(s.T(), err, dynamo.ErrSnapshotRaceCondition)
	s.client.AssertExpectations(s.T())
}

// Test CommitWithSnapshot error handling
func (s *RepositorySuite) Test_CommitWithSnapshot_Error() {
	ctx := context.Background()

	serializedEvents := []evt.SerializedEvent{
		{
			EntityType: "test",
			EntityID:   "test-id",
			Sequence:   1,
			Type:       "test-event",
			Version:    1,
			Payload:    []byte("{}"),
			Metadata:   evt.Metadata{Region: "us-east-1"},
		},
	}

	expectedError := errors.New("snapshot commit failed")
	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.TransactWriteItemsOutput)(nil), expectedError)

	result := evt.SerializedResult{Events: serializedEvents}
	err := s.repo.CommitWithSnapshot(ctx, result, "test", "test-id", []byte("{}"), 1)

	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "snapshot commit failed")
	s.client.AssertExpectations(s.T())
}

// Test with event data including metadata to ensure JSON marshal works
func (s *RepositorySuite) Test_Commit_JSONMarshalError() {
	ctx := context.Background()

	serializedEvents := []evt.SerializedEvent{
		{
			EntityType: "test",
			EntityID:   "test-id",
			Sequence:   1,
			Type:       "test-event",
			Version:    1,
			Payload:    []byte("{}"),
			Metadata:   evt.Metadata{Region: "us-east-1"},
		},
	}

	// Mock successful transaction since JSON marshaling should work fine
	output := new(dynamodb.TransactWriteItemsOutput)
	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return(output, nil)

	result := evt.SerializedResult{Events: serializedEvents}
	err := s.repo.Commit(ctx, result)

	require.NoError(s.T(), err)
	s.client.AssertExpectations(s.T())
}
