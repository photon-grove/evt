package test

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// snapshotItem builds a GetItemOutput resembling an inline snapshot row at sk=0.
func snapshotItem(entityID string, eventSeq int) *dynamodb.GetItemOutput {
	return &dynamodb.GetItemOutput{
		Item: map[string]types.AttributeValue{
			"pk":         &types.AttributeValueMemberS{Value: entityID},
			"sk":         &types.AttributeValueMemberN{Value: "0"},
			"seq":        &types.AttributeValueMemberN{Value: "1"},
			"eventSeq":   &types.AttributeValueMemberN{Value: strconv.Itoa(eventSeq)},
			"entityType": &types.AttributeValueMemberS{Value: "test"},
			"payload":    &types.AttributeValueMemberS{Value: "{}"},
		},
	}
}

// eventKeyPage builds a QueryOutput of (pk, sk) keys for the given sequences.
func eventKeyPage(entityID string, sequences ...int) *dynamodb.QueryOutput {
	items := make([]map[string]types.AttributeValue, 0, len(sequences))
	for _, seq := range sequences {
		items = append(items, map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: entityID},
			"sk": &types.AttributeValueMemberN{Value: strconv.Itoa(seq)},
		})
	}

	return &dynamodb.QueryOutput{Items: items}
}

func (s *RepositorySuite) Test_CompactBelow_Success() {
	ctx := context.Background()
	const entityID = "acct-1"

	s.client.On("GetItem", mock.Anything, mock.Anything, mock.Anything).
		Return(snapshotItem(entityID, 5), nil)

	s.client.On("Query", mock.Anything, mock.Anything, mock.Anything).
		Return(eventKeyPage(entityID, 1, 2, 3, 4, 5), nil)

	// Capture the batch and assert it deletes exactly the five keys.
	s.client.On("BatchWriteItem", mock.Anything, mock.MatchedBy(func(in *dynamodb.BatchWriteItemInput) bool {
		reqs := in.RequestItems[s.repo.EventsTable]
		if len(reqs) != 5 {
			return false
		}
		for _, r := range reqs {
			if r.DeleteRequest == nil {
				return false
			}
		}
		return true
	}), mock.Anything).Return(&dynamodb.BatchWriteItemOutput{}, nil)

	// *dynamo.Repository satisfies the evt.Compactor capability interface.
	var _ evt.Compactor = s.repo

	deleted, err := s.repo.CompactBelow(ctx, entityID, 5)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 5, deleted)
	s.client.AssertExpectations(s.T())
}

func (s *RepositorySuite) Test_CompactBelow_RefusesWithoutSnapshot() {
	ctx := context.Background()

	s.client.On("GetItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.GetItemOutput{Item: nil}, nil)

	deleted, err := s.repo.CompactBelow(ctx, "acct-nosnap", 5)
	require.ErrorIs(s.T(), err, evt.ErrCompactionUncovered)
	require.Equal(s.T(), 0, deleted)

	// No deletes should have been attempted.
	s.client.AssertNotCalled(s.T(), "Query", mock.Anything, mock.Anything, mock.Anything)
	s.client.AssertNotCalled(s.T(), "BatchWriteItem", mock.Anything, mock.Anything, mock.Anything)
}

func (s *RepositorySuite) Test_CompactBelow_RefusesWhenUncovered() {
	ctx := context.Background()
	const entityID = "acct-uncovered"

	// Snapshot only covers through event 3, but compaction requested through 5.
	s.client.On("GetItem", mock.Anything, mock.Anything, mock.Anything).
		Return(snapshotItem(entityID, 3), nil)

	deleted, err := s.repo.CompactBelow(ctx, entityID, 5)
	require.ErrorIs(s.T(), err, evt.ErrCompactionUncovered)
	require.Equal(s.T(), 0, deleted)
	s.client.AssertNotCalled(s.T(), "BatchWriteItem", mock.Anything, mock.Anything, mock.Anything)
}

func (s *RepositorySuite) Test_CompactBelow_ZeroIsNoOp() {
	ctx := context.Background()

	deleted, err := s.repo.CompactBelow(ctx, "acct-zero", 0)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, deleted)

	// A no-op must not even read the snapshot.
	s.client.AssertNotCalled(s.T(), "GetItem", mock.Anything, mock.Anything, mock.Anything)
}

func (s *RepositorySuite) Test_CompactBelow_NothingToDelete() {
	ctx := context.Background()
	const entityID = "acct-empty"

	s.client.On("GetItem", mock.Anything, mock.Anything, mock.Anything).
		Return(snapshotItem(entityID, 5), nil)
	s.client.On("Query", mock.Anything, mock.Anything, mock.Anything).
		Return(eventKeyPage(entityID), nil)

	deleted, err := s.repo.CompactBelow(ctx, entityID, 5)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, deleted)
	s.client.AssertNotCalled(s.T(), "BatchWriteItem", mock.Anything, mock.Anything, mock.Anything)
}

func (s *RepositorySuite) Test_CompactBelow_RetriesUnprocessedItems() {
	ctx := context.Background()
	const entityID = "acct-retry"

	s.client.On("GetItem", mock.Anything, mock.Anything, mock.Anything).
		Return(snapshotItem(entityID, 2), nil)
	s.client.On("Query", mock.Anything, mock.Anything, mock.Anything).
		Return(eventKeyPage(entityID, 1, 2), nil)

	unprocessed := &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{
			s.repo.EventsTable: {
				{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: entityID},
					"sk": &types.AttributeValueMemberN{Value: "2"},
				}}},
			},
		},
	}

	// First call leaves one item unprocessed; the retry clears it.
	s.client.On("BatchWriteItem", mock.Anything, mock.Anything, mock.Anything).
		Return(unprocessed, nil).Once()
	s.client.On("BatchWriteItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()

	deleted, err := s.repo.CompactBelow(ctx, entityID, 2)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, deleted)
	s.client.AssertExpectations(s.T())
}
