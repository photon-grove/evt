package test

import (
	"context"
	"errors"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// eventItem builds a raw Dynamo event item for the given entity id and sequence.
func eventItem(pk string, sk int) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: pk},
		"sk":         &types.AttributeValueMemberN{Value: strconv.Itoa(sk)},
		"type":       &types.AttributeValueMemberS{Value: "test-event"},
		"entityType": &types.AttributeValueMemberS{Value: "TestEntity"},
		"version":    &types.AttributeValueMemberN{Value: "1"},
		"payload":    &types.AttributeValueMemberS{Value: `{}`},
		"metadata":   &types.AttributeValueMemberS{Value: `{}`},
	}
}

// Mock Entity for StreamEntities tests
type MockEntity struct{ evt.BaseEntity }

func (m *MockEntity) Type() evt.EntityType { return "MockEntity" }
func (m *MockEntity) GetID() evt.EntityID  { return m.ID }
func (m *MockEntity) Handle(_ context.Context, _ evt.Command) (evt.CommandResult, error) {
	return evt.CommandResult{}, nil
}
func (m *MockEntity) Apply(_ evt.Event) error { return nil }
func (m *MockEntity) DeserializeEvent(_ evt.SerializedEvent) (evt.Event, error) {
	return nil, errors.New("not implemented")
}
func (m *MockEntity) EventUpcasters() []evt.EventUpcaster { return nil }
func (m *MockEntity) Projectors() []evt.EventProjector    { return nil }
func (m *MockEntity) Base() evt.BaseEntity                { return m.BaseEntity }

// Test StreamEntities Success
func (s *RepositorySuite) Test_StreamEntities_Success() {
	ctx := context.Background()

	output := &dynamodb.ScanOutput{Items: []map[string]types.AttributeValue{{
		"pk":         &types.AttributeValueMemberS{Value: "test-id-1"},
		"sk":         &types.AttributeValueMemberN{Value: "1"},
		"type":       &types.AttributeValueMemberS{Value: "test-event"},
		"entityType": &types.AttributeValueMemberS{Value: "TestEntity"},
		"version":    &types.AttributeValueMemberN{Value: "1"},
		"payload":    &types.AttributeValueMemberS{Value: `{"test": "data1"}`},
		"metadata":   &types.AttributeValueMemberS{Value: `{"region": "us-east-1"}`},
	}}, LastEvaluatedKey: nil}

	s.client.On("Scan", mock.Anything, mock.Anything, mock.Anything).Return(output, nil)

	applyFunc := func(_ context.Context, event evt.SerializedEvent, _ evt.Entity) (evt.Entity, error) {
		return &MockEntity{BaseEntity: evt.NewEntity(event.EntityID)}, nil
	}

	entityStream := s.repo.StreamEntities(ctx, nil, applyFunc)
	allEntities := make([]evt.Entity, 0, 1)
	for entityResult := range entityStream {
		require.True(s.T(), entityResult.IsOk())
		entity, err := entityResult.Unwrap()
		require.NoError(s.T(), err)
		allEntities = append(allEntities, entity)
	}
	require.Len(s.T(), allEntities, 1)
	mockEntity, ok := allEntities[0].(*MockEntity)
	require.True(s.T(), ok)
	require.Equal(s.T(), evt.EntityID("test-id-1"), mockEntity.ID)
	s.client.AssertExpectations(s.T())
}

// Test StreamEntities with apply function error
func (s *RepositorySuite) Test_StreamEntities_ApplyError() {
	ctx := context.Background()

	output := &dynamodb.ScanOutput{Items: []map[string]types.AttributeValue{{
		"pk":         &types.AttributeValueMemberS{Value: "test-id-1"},
		"sk":         &types.AttributeValueMemberN{Value: "1"},
		"type":       &types.AttributeValueMemberS{Value: "test-event"},
		"entityType": &types.AttributeValueMemberS{Value: "TestEntity"},
		"version":    &types.AttributeValueMemberN{Value: "1"},
		"payload":    &types.AttributeValueMemberS{Value: `{"test": "data1"}`},
		"metadata":   &types.AttributeValueMemberS{Value: `{"region": "us-east-1"}`},
	}}, LastEvaluatedKey: nil}

	s.client.On("Scan", mock.Anything, mock.Anything, mock.Anything).Return(output, nil)

	applyFunc := func(_ context.Context, _ evt.SerializedEvent, _ evt.Entity) (evt.Entity, error) {
		return nil, errors.New("apply function failed")
	}

	entityStream := s.repo.StreamEntities(ctx, nil, applyFunc)
	entityResult := <-entityStream
	require.True(s.T(), entityResult.IsErr())
	_, err := entityResult.Unwrap()
	require.Contains(s.T(), err.Error(), "apply function failed")
	_, ok := <-entityStream
	require.False(s.T(), ok)
	s.client.AssertExpectations(s.T())
}

// Test_StreamEntities_GroupsAndOrdersInterleavedScan verifies that StreamEntities reconstitutes
// entities correctly even when a Scan returns their events interleaved and out of sort-key order —
// the order DynamoDB is allowed to return for a Scan. Events for each entity must be applied in
// ascending sequence order.
func (s *RepositorySuite) Test_StreamEntities_GroupsAndOrdersInterleavedScan() {
	ctx := context.Background()

	// One page, two entities, events interleaved and out of order.
	output := &dynamodb.ScanOutput{Items: []map[string]types.AttributeValue{
		eventItem("a", 2),
		eventItem("b", 1),
		eventItem("a", 1),
		eventItem("b", 3),
		eventItem("b", 2),
		eventItem("a", 3),
	}}

	s.client.On("Scan", mock.Anything, mock.Anything, mock.Anything).Return(output, nil)

	applied := map[evt.EntityID][]int{}
	applyFunc := func(_ context.Context, event evt.SerializedEvent, entity evt.Entity) (evt.Entity, error) {
		applied[event.EntityID] = append(applied[event.EntityID], int(event.Sequence))
		if entity == nil {
			return &MockEntity{BaseEntity: evt.NewEntity(event.EntityID)}, nil
		}

		return entity, nil
	}

	ids := make([]evt.EntityID, 0, 2)
	for entityResult := range s.repo.StreamEntities(ctx, nil, applyFunc) {
		entity, err := entityResult.Unwrap()
		require.NoError(s.T(), err)
		ids = append(ids, entity.GetID())
	}

	require.ElementsMatch(s.T(), []evt.EntityID{"a", "b"}, ids)
	// Each entity's events were applied in ascending sequence order despite the scan order.
	require.Equal(s.T(), []int{1, 2, 3}, applied["a"])
	require.Equal(s.T(), []int{1, 2, 3}, applied["b"])
	s.client.AssertExpectations(s.T())
}

// Test_StreamEntities_ParallelSegments verifies that a segmented (parallel) scan sets the
// Segment/TotalSegments parameters and that events from different segments are grouped into the
// correct entities.
func (s *RepositorySuite) Test_StreamEntities_ParallelSegments() {
	ctx := context.Background()

	seg0 := &dynamodb.ScanOutput{Items: []map[string]types.AttributeValue{
		eventItem("a", 1), eventItem("a", 2),
	}}
	seg1 := &dynamodb.ScanOutput{Items: []map[string]types.AttributeValue{
		eventItem("b", 1), eventItem("b", 2),
	}}

	s.client.On("Scan", mock.Anything, mock.MatchedBy(func(in *dynamodb.ScanInput) bool {
		return in.TotalSegments != nil && *in.TotalSegments == 2 && in.Segment != nil && *in.Segment == 0
	}), mock.Anything).Return(seg0, nil)
	s.client.On("Scan", mock.Anything, mock.MatchedBy(func(in *dynamodb.ScanInput) bool {
		return in.TotalSegments != nil && *in.TotalSegments == 2 && in.Segment != nil && *in.Segment == 1
	}), mock.Anything).Return(seg1, nil)

	applyFunc := func(_ context.Context, event evt.SerializedEvent, entity evt.Entity) (evt.Entity, error) {
		if entity == nil {
			return &MockEntity{BaseEntity: evt.NewEntity(event.EntityID)}, nil
		}

		return entity, nil
	}

	repo := s.repo.WithScanSegments(2)

	ids := make([]evt.EntityID, 0, 2)
	for entityResult := range repo.StreamEntities(ctx, nil, applyFunc) {
		entity, err := entityResult.Unwrap()
		require.NoError(s.T(), err)
		ids = append(ids, entity.GetID())
	}

	require.ElementsMatch(s.T(), []evt.EntityID{"a", "b"}, ids)
	s.client.AssertExpectations(s.T())
}

// pkItem builds a key-only scan item (what enumeration projects).
func pkItem(pk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: pk}}
}

// queryForPK matches a GetEvents query for a specific partition key.
func queryForPK(pk string) func(*dynamodb.QueryInput) bool {
	return func(in *dynamodb.QueryInput) bool {
		v, ok := in.ExpressionAttributeValues[":pk"].(*types.AttributeValueMemberS)
		return ok && v.Value == pk
	}
}

// Test_StreamEntitiesByQuery_EnumeratesAndQueries verifies the enumerate-then-query path:
// duplicate partition keys are de-duplicated during enumeration, and each entity is rebuilt from
// its own ordered partition query.
func (s *RepositorySuite) Test_StreamEntitiesByQuery_EnumeratesAndQueries() {
	ctx := context.Background()

	// Enumeration scan: key-only, with a duplicate pk to exercise de-duplication.
	s.client.On("Scan", mock.Anything, mock.MatchedBy(func(in *dynamodb.ScanInput) bool {
		return in.ProjectionExpression != nil && *in.ProjectionExpression == "pk"
	}), mock.Anything).Return(&dynamodb.ScanOutput{
		Items: []map[string]types.AttributeValue{pkItem("a"), pkItem("b"), pkItem("a")},
	}, nil).Once()

	// Per-entity partition queries.
	s.client.On("Query", mock.Anything, mock.MatchedBy(queryForPK("a")), mock.Anything).
		Return(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{eventItem("a", 1), eventItem("a", 2)}}, nil).Once()
	s.client.On("Query", mock.Anything, mock.MatchedBy(queryForPK("b")), mock.Anything).
		Return(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{eventItem("b", 1)}}, nil).Once()

	applied := map[evt.EntityID][]int{}
	applyFunc := func(_ context.Context, event evt.SerializedEvent, entity evt.Entity) (evt.Entity, error) {
		applied[event.EntityID] = append(applied[event.EntityID], int(event.Sequence))
		if entity == nil {
			return &MockEntity{BaseEntity: evt.NewEntity(event.EntityID)}, nil
		}

		return entity, nil
	}

	ids := make([]evt.EntityID, 0, 2)
	for entityResult := range s.repo.StreamEntitiesByQuery(ctx, dynamo.StreamByQueryOptions{Workers: 1}, applyFunc) {
		entity, err := entityResult.Unwrap()
		require.NoError(s.T(), err)
		ids = append(ids, entity.GetID())
	}

	require.ElementsMatch(s.T(), []evt.EntityID{"a", "b"}, ids)
	require.Equal(s.T(), []int{1, 2}, applied["a"])
	require.Equal(s.T(), []int{1}, applied["b"])
	s.client.AssertExpectations(s.T())
}

// Test_StreamEntitiesByQuery_Skip verifies that skipped entity IDs are never queried.
func (s *RepositorySuite) Test_StreamEntitiesByQuery_Skip() {
	ctx := context.Background()

	s.client.On("Scan", mock.Anything, mock.Anything, mock.Anything).Return(&dynamodb.ScanOutput{
		Items: []map[string]types.AttributeValue{pkItem("a"), pkItem("b")},
	}, nil).Once()

	// Only "a" should be queried; a query for "b" would be an unexpected call.
	s.client.On("Query", mock.Anything, mock.MatchedBy(queryForPK("a")), mock.Anything).
		Return(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{eventItem("a", 1)}}, nil).Once()

	applyFunc := func(_ context.Context, event evt.SerializedEvent, entity evt.Entity) (evt.Entity, error) {
		if entity == nil {
			return &MockEntity{BaseEntity: evt.NewEntity(event.EntityID)}, nil
		}

		return entity, nil
	}

	opts := dynamo.StreamByQueryOptions{
		Workers: 1,
		Skip:    func(id evt.EntityID) bool { return id == "b" },
	}

	ids := make([]evt.EntityID, 0, 1)
	for entityResult := range s.repo.StreamEntitiesByQuery(ctx, opts, applyFunc) {
		entity, err := entityResult.Unwrap()
		require.NoError(s.T(), err)
		ids = append(ids, entity.GetID())
	}

	require.Equal(s.T(), []evt.EntityID{"a"}, ids)
	s.client.AssertExpectations(s.T())
}

// Test_StreamEntitiesByQuery_EntityTypeFilter verifies enumeration filters the scan by entity type.
func (s *RepositorySuite) Test_StreamEntitiesByQuery_EntityTypeFilter() {
	ctx := context.Background()

	s.client.On("Scan", mock.Anything, mock.MatchedBy(func(in *dynamodb.ScanInput) bool {
		v, ok := in.ExpressionAttributeValues[":et"].(*types.AttributeValueMemberS)
		return in.FilterExpression != nil && ok && v.Value == "TestEntity"
	}), mock.Anything).Return(&dynamodb.ScanOutput{
		Items: []map[string]types.AttributeValue{pkItem("a")},
	}, nil).Once()

	s.client.On("Query", mock.Anything, mock.MatchedBy(queryForPK("a")), mock.Anything).
		Return(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{eventItem("a", 1)}}, nil).Once()

	applyFunc := func(_ context.Context, event evt.SerializedEvent, entity evt.Entity) (evt.Entity, error) {
		if entity == nil {
			return &MockEntity{BaseEntity: evt.NewEntity(event.EntityID)}, nil
		}

		return entity, nil
	}

	opts := dynamo.StreamByQueryOptions{Workers: 1, EntityType: evt.EntityType("TestEntity")}

	count := 0
	for entityResult := range s.repo.StreamEntitiesByQuery(ctx, opts, applyFunc) {
		_, err := entityResult.Unwrap()
		require.NoError(s.T(), err)
		count++
	}

	require.Equal(s.T(), 1, count)
	s.client.AssertExpectations(s.T())
}

// Test_StreamEntitiesByQuery_ParallelWorkers exercises the worker pool over several entities.
func (s *RepositorySuite) Test_StreamEntitiesByQuery_ParallelWorkers() {
	ctx := context.Background()

	s.client.On("Scan", mock.Anything, mock.Anything, mock.Anything).Return(&dynamodb.ScanOutput{
		Items: []map[string]types.AttributeValue{pkItem("a"), pkItem("b"), pkItem("c"), pkItem("d")},
	}, nil).Once()

	for _, id := range []string{"a", "b", "c", "d"} {
		s.client.On("Query", mock.Anything, mock.MatchedBy(queryForPK(id)), mock.Anything).
			Return(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{eventItem(id, 1)}}, nil).Once()
	}

	applyFunc := func(_ context.Context, event evt.SerializedEvent, entity evt.Entity) (evt.Entity, error) {
		if entity == nil {
			return &MockEntity{BaseEntity: evt.NewEntity(event.EntityID)}, nil
		}

		return entity, nil
	}

	ids := make([]evt.EntityID, 0, 4)
	for entityResult := range s.repo.StreamEntitiesByQuery(ctx, dynamo.StreamByQueryOptions{Workers: 4}, applyFunc) {
		entity, err := entityResult.Unwrap()
		require.NoError(s.T(), err)
		ids = append(ids, entity.GetID())
	}

	require.ElementsMatch(s.T(), []evt.EntityID{"a", "b", "c", "d"}, ids)
	s.client.AssertExpectations(s.T())
}

// Test_StreamEntitiesByQuery_EnumerationError surfaces a scan failure as an error result.
func (s *RepositorySuite) Test_StreamEntitiesByQuery_EnumerationError() {
	ctx := context.Background()

	s.client.On("Scan", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.ScanOutput)(nil), errors.New("scan boom")).Once()

	applyFunc := func(_ context.Context, event evt.SerializedEvent, _ evt.Entity) (evt.Entity, error) {
		return &MockEntity{BaseEntity: evt.NewEntity(event.EntityID)}, nil
	}

	var gotErr error
	for entityResult := range s.repo.StreamEntitiesByQuery(ctx, dynamo.StreamByQueryOptions{Workers: 2}, applyFunc) {
		if _, err := entityResult.Unwrap(); err != nil {
			gotErr = err
		}
	}

	require.Error(s.T(), gotErr)
	require.Contains(s.T(), gotErr.Error(), "scan boom")
	s.client.AssertExpectations(s.T())
}
