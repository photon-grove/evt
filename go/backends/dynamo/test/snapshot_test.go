package test

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	evt "github.com/photon-grove/evt/go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Test retrieving a Snapshot for an Aggregate
func (s *RepositorySuite) Test_GetSnapshot_Success() {
	ctx := context.Background()

	input := dynamodb.GetItemInput{
		TableName:      &s.repo.EventsTable,
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "test-id"},
			"sk": &types.AttributeValueMemberN{Value: "0"},
		},
	}

	expected := evt.SerializedSnapshot{
		Sequence:      1,
		EventSequence: 2,
		EntityType:    "test",
		EntityID:      "test-id",
		Payload:       []byte{},
	}

	output := new(dynamodb.GetItemOutput)
	output.Item = map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "test-id"},
		"seq":        &types.AttributeValueMemberN{Value: "1"},
		"eventSeq":   &types.AttributeValueMemberN{Value: "2"},
		"entityType": &types.AttributeValueMemberS{Value: "test"},
		"payload":    &types.AttributeValueMemberS{Value: ""},
	}

	s.client.On("GetItem", mock.Anything, &input, mock.Anything).Return(output, nil)

	snapshot, err := s.repo.GetSnapshot(ctx, "test-id")
	require.NoError(s.T(), err)

	s.client.AssertExpectations(s.T())

	require.Equal(s.T(), &expected, snapshot)
}

// Test GetSnapshot with no snapshot found
func (s *RepositorySuite) Test_GetSnapshot_NotFound() {
	ctx := context.Background()
	entityID := evt.EntityID("test-id")

	input := dynamodb.GetItemInput{
		TableName:      &s.repo.EventsTable,
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "test-id"},
			"sk": &types.AttributeValueMemberN{Value: "0"},
		},
	}

	output := &dynamodb.GetItemOutput{Item: nil}

	s.client.On("GetItem", mock.Anything, &input, mock.Anything).Return(output, nil)

	snapshot, err := s.repo.GetSnapshot(ctx, entityID)
	require.NoError(s.T(), err)
	require.Nil(s.T(), snapshot)
	s.client.AssertExpectations(s.T())
}

// Test GetSnapshot with DynamoDB error
func (s *RepositorySuite) Test_GetSnapshot_Error() {
	ctx := context.Background()
	entityID := evt.EntityID("test-id")

	expectedError := "DynamoDB get failed"
	s.client.On("GetItem", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.GetItemOutput)(nil), errors.New(expectedError))

	snapshot, err := s.repo.GetSnapshot(ctx, entityID)
	require.Error(s.T(), err)
	require.Nil(s.T(), snapshot)
	require.Contains(s.T(), err.Error(), expectedError)
	s.client.AssertExpectations(s.T())
}

// Test PutSnapshot writes with a monotonic-floor condition expression.
func (s *RepositorySuite) Test_PutSnapshot_UsesMonotonicCondition() {
	ctx := context.Background()

	s.client.On("TransactWriteItems", mock.Anything, mock.MatchedBy(func(in *dynamodb.TransactWriteItemsInput) bool {
		require.Len(s.T(), in.TransactItems, 3)
		require.Equal(s.T(), markerCheck(s.repo.EventsTable), in.TransactItems[0])

		// The head guard sits between the marker check and the snapshot put (invariant H3).
		guard := in.TransactItems[1].ConditionCheck
		require.NotNil(s.T(), guard)
		require.Equal(s.T(), &types.AttributeValueMemberN{Value: "-1"}, guard.Key["sk"])
		require.Equal(s.T(), "head >= :new", aws.ToString(guard.ConditionExpression))
		require.Equal(s.T(), &types.AttributeValueMemberN{Value: "6"}, guard.ExpressionAttributeValues[":new"])

		put := in.TransactItems[2].Put
		require.NotNil(s.T(), put)
		require.Equal(s.T(), "attribute_not_exists(eventSeq) OR eventSeq <= :new", aws.ToString(put.ConditionExpression))
		require.Equal(s.T(), &types.AttributeValueMemberN{Value: "6"}, put.ExpressionAttributeValues[":new"])
		require.Equal(s.T(), &types.AttributeValueMemberN{Value: "0"}, put.Item["sk"])
		return true
	}), mock.Anything).Return(&dynamodb.TransactWriteItemsOutput{}, nil)

	err := s.repo.PutSnapshot(ctx, "test", "id-1", []byte("{}"), 1, 6)
	require.NoError(s.T(), err)
	s.client.AssertExpectations(s.T())
}

// Test PutSnapshot treats a regressing write (rejected by the condition) as a silent no-op.
func (s *RepositorySuite) Test_PutSnapshot_NoOpOnRegression() {
	ctx := context.Background()

	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.TransactWriteItemsOutput)(nil), &types.TransactionCanceledException{
			CancellationReasons: []types.CancellationReason{
				{Code: aws.String("None")},
				{Code: aws.String("None")},
				{Code: aws.String("ConditionalCheckFailed")},
			},
		})

	// A regression attempt must not surface as an error — the existing newer snapshot already covers it.
	err := s.repo.PutSnapshot(ctx, "test", "id-1", []byte("{}"), 2, 3)
	require.NoError(s.T(), err)
	s.client.AssertExpectations(s.T())
}

// Test PutSnapshot propagates non-conditional errors.
func (s *RepositorySuite) Test_PutSnapshot_PropagatesError() {
	ctx := context.Background()

	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.TransactWriteItemsOutput)(nil), errors.New("put failed"))

	err := s.repo.PutSnapshot(ctx, "test", "id-1", []byte("{}"), 1, 6)
	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "put failed")
	s.client.AssertExpectations(s.T())
}
