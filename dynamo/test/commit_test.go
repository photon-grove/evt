package test

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/internal/eventjson"
	"github.com/photon-grove/evt/result" //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
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
		{
			ConditionCheck: &types.ConditionCheck{
				TableName: &s.repo.EventsTable,
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "test-id"},
					"sk": &types.AttributeValueMemberN{Value: "0"},
				},
				ConditionExpression: aws.String("attribute_not_exists(eventSeq) OR eventSeq < :first"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":first": &types.AttributeValueMemberN{Value: "1"},
				},
			},
		},
	}

	input := dynamodb.TransactWriteItemsInput{TransactItems: expected}

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

	input := dynamodb.TransactWriteItemsInput{TransactItems: expected}

	// Return an empty output object, because the result isn't used
	output := new(dynamodb.TransactWriteItemsOutput)

	s.client.On("TransactWriteItems", mock.Anything, &input, mock.Anything).Return(output, nil)

	result := evt.SerializedResult{Events: serializedEvents}

	err = s.repo.CommitWithSnapshot(ctx, result, "test", "test-id", []byte{}, 1)
	require.NoError(s.T(), err)

	s.client.AssertExpectations(s.T())
}

// Test that a later snapshot requires the loaded snapshot sequence and the floor
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
	require.Equal(s.T(), "seq = :seq AND eventSeq < :first", aws.ToString(put.ConditionExpression))
	require.Equal(s.T(), map[string]types.AttributeValue{
		":seq":   &types.AttributeValueMemberN{Value: "2"},
		":first": &types.AttributeValueMemberN{Value: "7"},
	}, put.ExpressionAttributeValues)
}

// Test CommitStream method with successful stream processing
func (s *RepositorySuite) Test_CommitStream_Ok() {
	ctx := context.Background()

	resultChannel := make(chan result.Result[evt.SerializedResult], 2)

	serializedEvents1 := []evt.SerializedEvent{{
		EntityType: "test", EntityID: "entity-1", Sequence: 1, Type: "test-event", Version: 1,
		Payload: []byte(`{"test": "data1"}`), Metadata: evt.Metadata{Region: "us-east-1"},
	}}
	serializedEvents2 := []evt.SerializedEvent{{
		EntityType: "test", EntityID: "entity-2", Sequence: 1, Type: "test-event", Version: 1,
		Payload: []byte(`{"test": "data2"}`), Metadata: evt.Metadata{Region: "us-east-1"},
	}}

	resultChannel <- result.Ok(evt.SerializedResult{Events: serializedEvents1})
	resultChannel <- result.Ok(evt.SerializedResult{Events: serializedEvents2})
	close(resultChannel)

	output := new(dynamodb.TransactWriteItemsOutput)
	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).Return(output, nil).Times(1)

	errors := s.repo.CommitStream(ctx, resultChannel) //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	require.Empty(s.T(), errors)
	s.client.AssertExpectations(s.T())
}

// Test CommitStream adds one floor check per stream per batch, using the batch's lowest sequence
// whatever order the results arrive in
func (s *RepositorySuite) Test_CommitStream_OneFloorCheckPerStream() {
	ctx := context.Background()

	event := func(id evt.EntityID, sequence evt.EventSequence) evt.SerializedEvent {
		return evt.SerializedEvent{
			EntityType: "test", EntityID: id, Sequence: sequence, Type: "test-event", Version: 1,
			Payload: []byte(`{}`), Metadata: evt.Metadata{Region: "us-east-1"},
		}
	}

	resultChannel := make(chan result.Result[evt.SerializedResult], 4)
	resultChannel <- result.Ok(evt.SerializedResult{Events: []evt.SerializedEvent{event("entity-1", 5)}})
	resultChannel <- result.Ok(evt.SerializedResult{Events: []evt.SerializedEvent{event("entity-2", 1)}})
	resultChannel <- result.Ok(evt.SerializedResult{Events: []evt.SerializedEvent{event("entity-1", 4)}})
	resultChannel <- result.Ok(evt.SerializedResult{Events: []evt.SerializedEvent{event("entity-2", 2)}})
	close(resultChannel)

	var checks []*types.ConditionCheck

	capture := mock.MatchedBy(func(in *dynamodb.TransactWriteItemsInput) bool {
		for _, item := range in.TransactItems {
			if item.ConditionCheck != nil {
				checks = append(checks, item.ConditionCheck)
			}
		}

		return true
	})

	output := new(dynamodb.TransactWriteItemsOutput)
	s.client.On("TransactWriteItems", mock.Anything, capture, mock.Anything).Return(output, nil).Once()

	require.Empty(s.T(), s.repo.CommitStream(ctx, resultChannel)) //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion

	floors := make(map[string]string)
	for _, check := range checks {
		pk, ok := check.Key["pk"].(*types.AttributeValueMemberS)
		require.True(s.T(), ok)

		first, ok := check.ExpressionAttributeValues[":first"].(*types.AttributeValueMemberN)
		require.True(s.T(), ok)

		floors[pk.Value] = first.Value
	}

	require.Len(s.T(), checks, 2)
	require.Equal(s.T(), map[string]string{"entity-1": "4", "entity-2": "1"}, floors)
	s.client.AssertExpectations(s.T())
}

// Test CommitStream with channel containing errors
func (s *RepositorySuite) Test_CommitStream_WithErrors() {
	ctx := context.Background()

	resultChannel := make(chan result.Result[evt.SerializedResult], 2)

	serializedEvents := []evt.SerializedEvent{{
		EntityType: "test", EntityID: "entity-1", Sequence: 1, Type: "test-event", Version: 1,
		Payload: []byte(`{"test": "data"}`), Metadata: evt.Metadata{Region: "us-east-1"},
	}}

	resultChannel <- result.Ok(evt.SerializedResult{Events: serializedEvents})
	resultChannel <- result.Err[evt.SerializedResult](errors.New("processing error"))
	close(resultChannel)

	output := new(dynamodb.TransactWriteItemsOutput)
	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).Return(output, nil).Once()

	errors := s.repo.CommitStream(ctx, resultChannel) //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	require.Len(s.T(), errors, 1)
	require.Contains(s.T(), errors[0].Error(), "processing error")
	s.client.AssertExpectations(s.T())
}

// Test CommitStream with DynamoDB commit errors
func (s *RepositorySuite) Test_CommitStream_CommitErrors() {
	ctx := context.Background()

	resultChannel := make(chan result.Result[evt.SerializedResult], 1)

	serializedEvents := []evt.SerializedEvent{{
		EntityType: "test", EntityID: "entity-1", Sequence: 1, Type: "test-event", Version: 1,
		Payload: []byte(`{"test": "data"}`), Metadata: evt.Metadata{Region: "us-east-1"},
	}}

	resultChannel <- result.Ok(evt.SerializedResult{Events: serializedEvents})
	close(resultChannel)

	commitError := errors.New("DynamoDB commit failed")
	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).Return((*dynamodb.TransactWriteItemsOutput)(nil), commitError)

	errors := s.repo.CommitStream(ctx, resultChannel) //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	require.Len(s.T(), errors, 1)
	require.Contains(s.T(), errors[0].Error(), "DynamoDB commit failed")
	s.client.AssertExpectations(s.T())
}

// Test CommitStream rejects a single result that exceeds the 100-item transaction limit
func (s *RepositorySuite) Test_CommitStream_OversizedSingleResult() {
	ctx := context.Background()

	// Create a result with 101 events — exceeds the 100-item DynamoDB transaction limit
	var oversizedEvents []evt.SerializedEvent
	for i := 1; i <= 101; i++ {
		oversizedEvents = append(oversizedEvents, evt.SerializedEvent{
			EntityType: "test", EntityID: evt.EntityID(fmt.Sprintf("entity-%d", i)),
			Sequence: 1, Type: "test-event", Version: 1,
			Payload: []byte(`{"test": "data"}`), Metadata: evt.Metadata{Region: "us-east-1"},
		})
	}

	resultChannel := make(chan result.Result[evt.SerializedResult], 1)
	resultChannel <- result.Ok(evt.SerializedResult{Events: oversizedEvents})
	close(resultChannel)

	// No DynamoDB calls should be made — the guard rejects before commit
	errors := s.repo.CommitStream(ctx, resultChannel) //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	require.Len(s.T(), errors, 1)
	require.Contains(s.T(), errors[0].Error(), "exceeding DynamoDB 100-item transaction limit")
	s.client.AssertNotCalled(s.T(), "TransactWriteItems")
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
			{Code: aws.String("ConditionalCheckFailed")},
		},
	}

	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.TransactWriteItemsOutput)(nil), conditionalCheckErr)

	result := evt.SerializedResult{Events: serializedEvents}
	err := s.repo.Commit(ctx, result)

	require.Error(s.T(), err)
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
	err := s.repo.CommitWithSnapshot(ctx, result, "test", "test-id", []byte("{}"), 0)

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
