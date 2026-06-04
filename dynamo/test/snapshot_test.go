package test

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Test retrieving a Snapshot for an Entity
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

	s.client.On("PutItem", mock.Anything, mock.MatchedBy(func(in *dynamodb.PutItemInput) bool {
		return in.ConditionExpression != nil && *in.ConditionExpression == "attribute_not_exists(eventSeq) OR eventSeq <= :new"
	}), mock.Anything).Return(&dynamodb.PutItemOutput{}, nil)

	err := s.repo.PutSnapshot(ctx, "test", "id-1", []byte("{}"), 1, 6)
	require.NoError(s.T(), err)
	s.client.AssertExpectations(s.T())
}

// Test PutSnapshot treats a regressing write (rejected by the condition) as a silent no-op.
func (s *RepositorySuite) Test_PutSnapshot_NoOpOnRegression() {
	ctx := context.Background()

	s.client.On("PutItem", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.PutItemOutput)(nil), &types.ConditionalCheckFailedException{})

	// A regression attempt must not surface as an error — the existing newer snapshot already covers it.
	err := s.repo.PutSnapshot(ctx, "test", "id-1", []byte("{}"), 2, 3)
	require.NoError(s.T(), err)
	s.client.AssertExpectations(s.T())
}

// Test PutSnapshot propagates non-conditional errors.
func (s *RepositorySuite) Test_PutSnapshot_PropagatesError() {
	ctx := context.Background()

	s.client.On("PutItem", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.PutItemOutput)(nil), errors.New("put failed"))

	err := s.repo.PutSnapshot(ctx, "test", "id-1", []byte("{}"), 1, 6)
	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "put failed")
	s.client.AssertExpectations(s.T())
}
