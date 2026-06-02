package test

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Test Delete operation
func (s *RepositorySuite) Test_Delete_Success() {
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

	expected := []types.TransactWriteItem{
		{
			Delete: &types.Delete{
				TableName: &s.repo.EventsTable,
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "test-id"},
					"sk": &types.AttributeValueMemberN{Value: "1"},
				},
			},
		},
	}

	input := dynamodb.TransactWriteItemsInput{TransactItems: expected}

	output := new(dynamodb.TransactWriteItemsOutput)
	s.client.On("TransactWriteItems", mock.Anything, &input, mock.Anything).Return(output, nil)

	err := s.repo.Delete(ctx, serializedEvents)
	require.NoError(s.T(), err)
	s.client.AssertExpectations(s.T())
}

// Test Delete with error
func (s *RepositorySuite) Test_Delete_Error() {
	ctx := context.Background()

	serializedEvents := []evt.SerializedEvent{
		{
			EntityType: "test",
			EntityID:   "test-id",
			Sequence:   1,
		},
	}

	expectedError := errors.New("delete failed")
	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.TransactWriteItemsOutput)(nil), expectedError)

	err := s.repo.Delete(ctx, serializedEvents)
	require.Error(s.T(), err)
	require.Contains(s.T(), err.Error(), "delete failed")
	s.client.AssertExpectations(s.T())
}
