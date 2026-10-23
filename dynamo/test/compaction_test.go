package test

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	dynamo_mock "github.com/photon-grove/evt/dynamo/internal/mock"
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

// expectSnapshot scopes a GetItem expectation to the entity's sk=0 snapshot read.
func (s *RepositorySuite) expectSnapshot(entityID string, out *dynamodb.GetItemOutput) {
	s.client.On("GetItem", mock.Anything, mock.MatchedBy(func(in *dynamodb.GetItemInput) bool {
		pk, ok := in.Key["pk"].(*types.AttributeValueMemberS)
		return ok && pk.Value == entityID
	}), mock.Anything).Return(out, nil)
}

// expectDeclaredMarker scopes a GetItem expectation to the feature-marker key and returns a marker
// that declares stream-heads at version 1: the state of every store compaction may touch.
func (s *RepositorySuite) expectDeclaredMarker() {
	s.client.On("GetItem", mock.Anything, mock.MatchedBy(func(in *dynamodb.GetItemInput) bool {
		pk, ok := in.Key["pk"].(*types.AttributeValueMemberS)
		return ok && pk.Value == "SYSTEM#FEATURES"
	}), mock.Anything).Return(&dynamodb.GetItemOutput{Item: declaredMarker()}, nil)
}

// compactionMarkerCheck is the delete batch's marker check, pinning the declared marker's version.
func compactionMarkerCheck(table string) types.TransactWriteItem {
	return types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
		TableName: aws.String(table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "SYSTEM#FEATURES"},
			"sk": &types.AttributeValueMemberN{Value: "0"},
		},
		ConditionExpression: aws.String("version = :observed"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":observed": &types.AttributeValueMemberN{Value: "1"},
		},
	}}
}

func (s *RepositorySuite) Test_CompactBelow_Success() {
	ctx := context.Background()
	const entityID = "acct-1"

	s.expectSnapshot(entityID, snapshotItem(entityID, 5))
	s.expectDeclaredMarker()

	s.client.On("Query", mock.Anything, mock.Anything, mock.Anything).
		Return(eventKeyPage(entityID, 1, 2, 3, 4, 5), nil)

	// Pin marker absence and delete exactly the event keys, never sk=0.
	s.client.On("TransactWriteItems", mock.Anything, mock.MatchedBy(func(in *dynamodb.TransactWriteItemsInput) bool {
		require.Len(s.T(), in.TransactItems, 6)
		require.Equal(s.T(), compactionMarkerCheck(s.repo.EventsTable), in.TransactItems[0])
		for i, item := range in.TransactItems[1:] {
			require.NotNil(s.T(), item.Delete)
			require.Equal(s.T(), s.repo.EventsTable, *item.Delete.TableName)
			require.Equal(s.T(), eventKeyPage(entityID, i+1).Items[0], item.Delete.Key)
		}
		return true
	}), mock.Anything).Return(&dynamodb.TransactWriteItemsOutput{}, nil)

	// *dynamo.Repository satisfies the evt.Compactor capability interface.
	var _ evt.Compactor = s.repo

	deleted, err := s.repo.CompactBelow(ctx, entityID, 5)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 5, deleted)
	s.client.AssertExpectations(s.T())
}

func (s *RepositorySuite) Test_CompactBelow_Batches99DeletesPlusMarker() {
	const entityID = "acct-batches"
	s.expectSnapshot(entityID, snapshotItem(entityID, 100))
	sequences := make([]int, 100)
	for i := range sequences {
		sequences[i] = i + 1
	}
	s.client.On("Query", mock.Anything, mock.Anything, mock.Anything).
		Return(eventKeyPage(entityID, sequences...), nil).Once()

	deletedKeys := make([]int, 0, 100)
	batchSizes := []int{}
	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.TransactWriteItemsOutput{}, nil).Times(2).
		Run(func(args mock.Arguments) {
			in, ok := args.Get(1).(*dynamodb.TransactWriteItemsInput)
			require.True(s.T(), ok)
			require.LessOrEqual(s.T(), len(in.TransactItems), 100)
			require.Equal(s.T(), compactionMarkerCheck(s.repo.EventsTable), in.TransactItems[0])
			batchSizes = append(batchSizes, len(in.TransactItems)-1)
			for _, item := range in.TransactItems[1:] {
				require.NotNil(s.T(), item.Delete)
				require.Equal(s.T(), s.repo.EventsTable, *item.Delete.TableName)
				pk, ok := item.Delete.Key["pk"].(*types.AttributeValueMemberS)
				require.True(s.T(), ok)
				sk, ok := item.Delete.Key["sk"].(*types.AttributeValueMemberN)
				require.True(s.T(), ok)
				require.Equal(s.T(), entityID, pk.Value)
				sequence, err := strconv.Atoi(sk.Value)
				require.NoError(s.T(), err)
				deletedKeys = append(deletedKeys, sequence)
			}
		})

	deleted, err := s.repo.CompactBelow(context.Background(), entityID, 100)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 100, deleted)
	require.Equal(s.T(), []int{99, 1}, batchSizes)
	require.Equal(s.T(), sequences, deletedKeys, "every event is deleted exactly once and sk=0 is retained")
	s.client.AssertExpectations(s.T())
}

func (s *RepositorySuite) Test_CompactBelow_RefusesWithoutSnapshot() {
	ctx := context.Background()

	s.expectSnapshot("acct-nosnap", &dynamodb.GetItemOutput{Item: nil})
	s.expectDeclaredMarker()

	deleted, err := s.repo.CompactBelow(ctx, "acct-nosnap", 5)
	require.ErrorIs(s.T(), err, evt.ErrCompactionUncovered)
	require.Equal(s.T(), 0, deleted)

	// No deletes should have been attempted.
	s.client.AssertNotCalled(s.T(), "Query", mock.Anything, mock.Anything, mock.Anything)
	s.client.AssertNotCalled(s.T(), "TransactWriteItems", mock.Anything, mock.Anything, mock.Anything)
}

func (s *RepositorySuite) Test_CompactBelow_RefusesWhenUncovered() {
	ctx := context.Background()
	const entityID = "acct-uncovered"

	// Snapshot only covers through event 3, but compaction requested through 5.
	s.expectSnapshot(entityID, snapshotItem(entityID, 3))
	s.expectDeclaredMarker()

	deleted, err := s.repo.CompactBelow(ctx, entityID, 5)
	require.ErrorIs(s.T(), err, evt.ErrCompactionUncovered)
	require.Equal(s.T(), 0, deleted)
	s.client.AssertNotCalled(s.T(), "TransactWriteItems", mock.Anything, mock.Anything, mock.Anything)
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

	s.expectSnapshot(entityID, snapshotItem(entityID, 5))
	s.expectDeclaredMarker()
	s.client.On("Query", mock.Anything, mock.Anything, mock.Anything).
		Return(eventKeyPage(entityID), nil)

	deleted, err := s.repo.CompactBelow(ctx, entityID, 5)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, deleted)
	s.client.AssertNotCalled(s.T(), "TransactWriteItems", mock.Anything, mock.Anything, mock.Anything)
}

func (s *RepositorySuite) Test_CompactBelow_RetriesMarkerChange() {
	ctx := context.Background()
	const entityID = "acct-retry"

	// Script each marker read on a fresh client rather than using the suite default.
	client := dynamo_mock.NewClient()
	repo := dynamo.NewRepository(client, "test-events")
	client.On("GetItem", mock.Anything, mock.MatchedBy(func(in *dynamodb.GetItemInput) bool {
		pk, ok := in.Key["pk"].(*types.AttributeValueMemberS)
		return ok && pk.Value == entityID
	}), mock.Anything).Return(snapshotItem(entityID, 2), nil).Once()
	expectManifest(client, declaredMarker())
	client.On("Query", mock.Anything, mock.Anything, mock.Anything).
		Return(eventKeyPage(entityID, 1, 2), nil).Once()

	client.On("TransactWriteItems", mock.Anything, mock.MatchedBy(func(in *dynamodb.TransactWriteItemsInput) bool {
		if len(in.TransactItems) != 3 {
			return false
		}
		observed, isNumber := in.TransactItems[0].ConditionCheck.ExpressionAttributeValues[":observed"].(*types.AttributeValueMemberN)
		if !isNumber || observed.Value != "1" {
			return false
		}
		require.Equal(s.T(), compactionMarkerCheck(repo.EventsTable), in.TransactItems[0])
		for i, item := range in.TransactItems[1:] {
			require.Equal(s.T(), eventKeyPage(entityID, i+1).Items[0], item.Delete.Key)
		}
		return true
	}), mock.Anything).Return((*dynamodb.TransactWriteItemsOutput)(nil), &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{{Code: aws.String("ConditionalCheckFailed")}},
	}).Once()
	// An unrelated declaration wins the race: pin its new version and retry the whole
	// unchanged delete batch, counting the events only once.
	expectManifest(client, markerItem(2, `{"stream-heads":1}`, `["audit-feed"]`, ""))
	client.On("TransactWriteItems", mock.Anything, mock.MatchedBy(func(in *dynamodb.TransactWriteItemsInput) bool {
		if len(in.TransactItems) != 3 || aws.ToString(in.TransactItems[0].ConditionCheck.ConditionExpression) != "version = :observed" {
			return false
		}
		check := in.TransactItems[0].ConditionCheck
		require.Equal(s.T(), "version = :observed", aws.ToString(check.ConditionExpression))
		require.Equal(s.T(), &types.AttributeValueMemberN{Value: "2"}, check.ExpressionAttributeValues[":observed"])
		for i, item := range in.TransactItems[1:] {
			require.Equal(s.T(), eventKeyPage(entityID, i+1).Items[0], item.Delete.Key)
		}
		return true
	}), mock.Anything).Return(&dynamodb.TransactWriteItemsOutput{}, nil).Once()

	deleted, err := repo.CompactBelow(ctx, entityID, 2)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, deleted)
	client.AssertExpectations(s.T())
}

// Compaction reads strongly even when the repository uses eventually consistent reads, because an
// eventually consistent key query could miss recent events and leave them behind.
func (s *RepositorySuite) Test_CompactBelow_ReadsStronglyConsistent() {
	ctx := context.Background()
	const entityID = "acct-eventual"
	repo := s.repo.WithConsistentRead(false)

	consistent := func(read *bool) bool { return read != nil && *read }

	s.client.On("GetItem", mock.Anything, mock.MatchedBy(func(in *dynamodb.GetItemInput) bool {
		pk, ok := in.Key["pk"].(*types.AttributeValueMemberS)
		return ok && consistent(in.ConsistentRead) && pk.Value == entityID
	}), mock.Anything).Return(snapshotItem(entityID, 2), nil)
	s.expectDeclaredMarker()
	s.client.On("Query", mock.Anything, mock.MatchedBy(func(in *dynamodb.QueryInput) bool {
		return consistent(in.ConsistentRead)
	}), mock.Anything).Return(eventKeyPage(entityID, 1, 2), nil)
	s.client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.TransactWriteItemsOutput{}, nil)

	deleted, err := repo.CompactBelow(ctx, entityID, 2)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, deleted)
	s.client.AssertExpectations(s.T())
}
