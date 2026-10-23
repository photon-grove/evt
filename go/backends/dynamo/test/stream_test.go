package test

import (
	"context"
	"errors"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	evt "github.com/photon-grove/evt/go"
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

// Mock Aggregate for StreamEntities tests
type MockAggregate struct {
	ID evt.EntityID `json:"id"`
}

func (m *MockAggregate) Type() evt.EntityType { return "MockEntity" }
func (m *MockAggregate) GetID() evt.EntityID  { return m.ID }
func (m *MockAggregate) Handle(_ context.Context, _ evt.Command) (evt.CommandResult, error) {
	return evt.CommandResult{}, nil
}
func (m *MockAggregate) Apply(_ evt.Event) error { return nil }
func (m *MockAggregate) DeserializeEvent(_ evt.SerializedEvent) (evt.Event, error) {
	return nil, errors.New("not implemented")
}
func (m *MockAggregate) EventUpcasters() []evt.EventUpcaster { return nil }
func (m *MockAggregate) Projectors() []evt.EventProjector    { return nil }

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

	applyFunc := func(_ context.Context, event evt.SerializedEvent, _ evt.Aggregate) (evt.Aggregate, error) {
		return &MockAggregate{ID: event.EntityID}, nil
	}

	aggregateStream := s.repo.StreamEntities(ctx, evt.StreamFilter{}, applyFunc)
	allAggregates := make([]evt.Aggregate, 0, 1)
	for aggregateResult := range aggregateStream {
		require.True(s.T(), aggregateResult.IsOk())
		aggregate, err := aggregateResult.Unwrap()
		require.NoError(s.T(), err)
		allAggregates = append(allAggregates, aggregate)
	}
	require.Len(s.T(), allAggregates, 1)
	mockAggregate, ok := allAggregates[0].(*MockAggregate)
	require.True(s.T(), ok)
	require.Equal(s.T(), evt.EntityID("test-id-1"), mockAggregate.ID)
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

	applyFunc := func(_ context.Context, _ evt.SerializedEvent, _ evt.Aggregate) (evt.Aggregate, error) {
		return nil, errors.New("apply function failed")
	}

	aggregateStream := s.repo.StreamEntities(ctx, evt.StreamFilter{}, applyFunc)
	aggregateResult := <-aggregateStream
	require.True(s.T(), aggregateResult.IsErr())
	_, err := aggregateResult.Unwrap()
	require.Contains(s.T(), err.Error(), "apply function failed")
	_, ok := <-aggregateStream
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
	applyFunc := func(_ context.Context, event evt.SerializedEvent, aggregate evt.Aggregate) (evt.Aggregate, error) {
		applied[event.EntityID] = append(applied[event.EntityID], int(event.Sequence))
		if aggregate == nil {
			return &MockAggregate{ID: event.EntityID}, nil
		}

		return aggregate, nil
	}

	ids := make([]evt.EntityID, 0, 2)
	for aggregateResult := range s.repo.StreamEntities(ctx, evt.StreamFilter{}, applyFunc) {
		aggregate, err := aggregateResult.Unwrap()
		require.NoError(s.T(), err)
		ids = append(ids, aggregate.GetID())
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

	applyFunc := func(_ context.Context, event evt.SerializedEvent, aggregate evt.Aggregate) (evt.Aggregate, error) {
		if aggregate == nil {
			return &MockAggregate{ID: event.EntityID}, nil
		}

		return aggregate, nil
	}

	repo := s.repo.WithScanSegments(2)

	ids := make([]evt.EntityID, 0, 2)
	for aggregateResult := range repo.StreamEntities(ctx, evt.StreamFilter{}, applyFunc) {
		aggregate, err := aggregateResult.Unwrap()
		require.NoError(s.T(), err)
		ids = append(ids, aggregate.GetID())
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

// Test_StreamEntities_ScanErrorAborts verifies that a scan error aborts the stream without emitting
// any entities, since a failed/partial scan means some events are missing.
func (s *RepositorySuite) Test_StreamEntities_ScanErrorAborts() {
	ctx := context.Background()

	s.client.On("Scan", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.ScanOutput)(nil), errors.New("scan failed")).Once()

	applyFunc := func(_ context.Context, event evt.SerializedEvent, aggregate evt.Aggregate) (evt.Aggregate, error) {
		if aggregate == nil {
			return &MockAggregate{ID: event.EntityID}, nil
		}

		return aggregate, nil
	}

	var errCount, okCount int
	var lastErr error
	for r := range s.repo.StreamEntities(ctx, evt.StreamFilter{}, applyFunc) {
		if _, err := r.Unwrap(); err != nil {
			errCount++
			lastErr = err
		} else {
			okCount++
		}
	}

	require.Equal(s.T(), 0, okCount, "no entities should be emitted after a scan error")
	require.Equal(s.T(), 1, errCount)
	require.ErrorContains(s.T(), lastErr, "scan failed")
	s.client.AssertExpectations(s.T())
}
