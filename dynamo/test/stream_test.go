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
