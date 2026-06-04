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

// seedMockEntity reconstructs a MockEntity from a snapshot payload.
func seedMockEntity(_ context.Context, snapshot evt.SerializedSnapshot) (evt.Entity, error) {
	return &MockEntity{BaseEntity: evt.NewEntity(snapshot.EntityID)}, nil
}

// enumScanOutput builds a key-only Scan page (the entity-ID enumeration) for the given pks. It
// reuses pkItem from stream_test.go.
func enumScanOutput(pks ...string) *dynamodb.ScanOutput {
	items := make([]map[string]types.AttributeValue, 0, len(pks))
	for _, pk := range pks {
		items = append(items, pkItem(pk))
	}

	return &dynamodb.ScanOutput{Items: items}
}

// snapshotGetItem builds a GetItem response for an inline snapshot at sk=0.
func snapshotGetItem(entityID string, eventSeq int) *dynamodb.GetItemOutput {
	return &dynamodb.GetItemOutput{
		Item: map[string]types.AttributeValue{
			"pk":         &types.AttributeValueMemberS{Value: entityID},
			"sk":         &types.AttributeValueMemberN{Value: "0"},
			"seq":        &types.AttributeValueMemberN{Value: "1"},
			"eventSeq":   &types.AttributeValueMemberN{Value: strconv.Itoa(eventSeq)},
			"entityType": &types.AttributeValueMemberS{Value: "MockEntity"},
			"payload":    &types.AttributeValueMemberS{Value: `{"id":"` + entityID + `"}`},
		},
	}
}

// eventsQueryOutput builds a Query response of full event rows for the given sequences. It reuses
// eventItem from stream_test.go.
func eventsQueryOutput(entityID string, seqs ...int) *dynamodb.QueryOutput {
	items := make([]map[string]types.AttributeValue, 0, len(seqs))
	for _, seq := range seqs {
		items = append(items, eventItem(entityID, seq))
	}

	return &dynamodb.QueryOutput{Items: items}
}

// getItemForPK matches a GetItemInput whose pk key equals the given id.
func getItemForPK(id string) interface{} {
	return mock.MatchedBy(func(in *dynamodb.GetItemInput) bool {
		pk, ok := in.Key["pk"].(*types.AttributeValueMemberS)
		return ok && pk.Value == id
	})
}

// queryForPKSince matches a QueryInput for the given pk whose :sk lower bound equals the sequence
// (i.e. GetLatestEvents queried events strictly after the snapshot's eventSeq).
func queryForPKSince(id string, sinceSeq int) interface{} {
	return mock.MatchedBy(func(in *dynamodb.QueryInput) bool {
		pk, ok := in.ExpressionAttributeValues[":pk"].(*types.AttributeValueMemberS)
		if !ok || pk.Value != id {
			return false
		}
		sk, ok := in.ExpressionAttributeValues[":sk"].(*types.AttributeValueMemberN)
		return ok && sk.Value == strconv.Itoa(sinceSeq)
	})
}

// Test_StreamEntitiesFromSnapshots_SeedsAndQueriesAfterSnapshot verifies that an entity with a
// snapshot is seeded from sk=0 and that ONLY events after the snapshot's eventSeq are queried and
// applied (the query lower bound proves the covered events are never read).
func (s *RepositorySuite) Test_StreamEntitiesFromSnapshots_SeedsAndQueriesAfterSnapshot() {
	ctx := context.Background()

	s.client.On("Scan", mock.Anything, mock.Anything, mock.Anything).Return(enumScanOutput("acct-1"), nil)
	s.client.On("GetItem", mock.Anything, getItemForPK("acct-1"), mock.Anything).Return(snapshotGetItem("acct-1", 5), nil)
	// GetLatestEvents must query sk > 5 (events 1..5 are covered by the snapshot and never read).
	s.client.On("Query", mock.Anything, queryForPKSince("acct-1", 5), mock.Anything).Return(eventsQueryOutput("acct-1", 6, 7), nil)

	applied := make([]evt.EventSequence, 0)
	applyFunc := func(_ context.Context, event evt.SerializedEvent, entity evt.Entity) (evt.Entity, error) {
		applied = append(applied, event.Sequence)
		return entity, nil
	}

	var entities []evt.Entity
	for res := range s.repo.StreamEntitiesFromSnapshots(ctx, "", seedMockEntity, applyFunc) {
		entity, err := res.Unwrap()
		require.NoError(s.T(), err)
		entities = append(entities, entity)
	}

	require.Len(s.T(), entities, 1)
	require.Equal(s.T(), evt.EntityID("acct-1"), entities[0].GetID())
	require.Equal(s.T(), []evt.EventSequence{6, 7}, applied)
	s.client.AssertExpectations(s.T())
}

// Test_StreamEntitiesFromSnapshots_NoSnapshotFullReplay verifies that an entity without a snapshot
// is replayed from sequence 1 (GetEvents) and seedEntity is never called.
func (s *RepositorySuite) Test_StreamEntitiesFromSnapshots_NoSnapshotFullReplay() {
	ctx := context.Background()

	s.client.On("Scan", mock.Anything, mock.Anything, mock.Anything).Return(enumScanOutput("acct-2"), nil)
	s.client.On("GetItem", mock.Anything, getItemForPK("acct-2"), mock.Anything).Return(&dynamodb.GetItemOutput{Item: nil}, nil)
	s.client.On("Query", mock.Anything, mock.MatchedBy(queryForPK("acct-2")), mock.Anything).Return(eventsQueryOutput("acct-2", 1, 2), nil)

	applied := make([]evt.EventSequence, 0)
	seeded := false
	seedFunc := func(ctx context.Context, snapshot evt.SerializedSnapshot) (evt.Entity, error) {
		seeded = true
		return seedMockEntity(ctx, snapshot)
	}
	applyFunc := func(_ context.Context, event evt.SerializedEvent, _ evt.Entity) (evt.Entity, error) {
		applied = append(applied, event.Sequence)
		return &MockEntity{BaseEntity: evt.NewEntity(event.EntityID)}, nil
	}

	var entities []evt.Entity
	for res := range s.repo.StreamEntitiesFromSnapshots(ctx, "", seedFunc, applyFunc) {
		entity, err := res.Unwrap()
		require.NoError(s.T(), err)
		entities = append(entities, entity)
	}

	require.False(s.T(), seeded, "no snapshot row means seedEntity is never called")
	require.Equal(s.T(), []evt.EventSequence{1, 2}, applied)
	require.Len(s.T(), entities, 1)
	s.client.AssertExpectations(s.T())
}

// Test_StreamEntitiesFromSnapshots_MultipleEntities verifies that enumeration drives one
// reconstruction per distinct entity ID.
func (s *RepositorySuite) Test_StreamEntitiesFromSnapshots_MultipleEntities() {
	ctx := context.Background()

	s.client.On("Scan", mock.Anything, mock.Anything, mock.Anything).Return(enumScanOutput("acct-a", "acct-b"), nil)
	s.client.On("GetItem", mock.Anything, getItemForPK("acct-a"), mock.Anything).Return(snapshotGetItem("acct-a", 5), nil)
	s.client.On("GetItem", mock.Anything, getItemForPK("acct-b"), mock.Anything).Return(snapshotGetItem("acct-b", 5), nil)
	s.client.On("Query", mock.Anything, mock.MatchedBy(queryForPK("acct-a")), mock.Anything).Return(eventsQueryOutput("acct-a", 6), nil)
	s.client.On("Query", mock.Anything, mock.MatchedBy(queryForPK("acct-b")), mock.Anything).Return(eventsQueryOutput("acct-b", 6), nil)

	applyFunc := func(_ context.Context, _ evt.SerializedEvent, entity evt.Entity) (evt.Entity, error) {
		return entity, nil
	}

	ids := make([]evt.EntityID, 0, 2)
	for res := range s.repo.StreamEntitiesFromSnapshots(ctx, "", seedMockEntity, applyFunc) {
		entity, err := res.Unwrap()
		require.NoError(s.T(), err)
		ids = append(ids, entity.GetID())
	}

	require.ElementsMatch(s.T(), []evt.EntityID{"acct-a", "acct-b"}, ids)
	s.client.AssertExpectations(s.T())
}
