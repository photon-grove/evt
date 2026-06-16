package snapshots_test

import (
	"context"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
)

// MockRepository for testing error scenarios
type MockRepository struct {
	snapshot                *evt.SerializedSnapshot
	events                  []evt.SerializedEvent
	getSnapshotError        error
	getEventsError          error
	getLatestEventsError    error
	commitError             error
	commitWithSnapshotError error
	putSnapshotError        error
	putSnapshotCalls        int
}

func (m *MockRepository) GetSnapshot(_ context.Context, _ evt.EntityID) (*evt.SerializedSnapshot, error) {
	if m.getSnapshotError != nil {
		return nil, m.getSnapshotError
	}
	return m.snapshot, nil
}

func (m *MockRepository) GetEvents(_ context.Context, _ evt.EntityID) ([]evt.SerializedEvent, error) {
	if m.getEventsError != nil {
		return nil, m.getEventsError
	}
	return m.events, nil
}

func (m *MockRepository) GetLatestEvents(_ context.Context, _ evt.EntityID, _ evt.EventSequence) ([]evt.SerializedEvent, error) {
	if m.getLatestEventsError != nil {
		return nil, m.getLatestEventsError
	}
	return m.events, nil
}

func (m *MockRepository) Commit(_ context.Context, _ evt.SerializedResult) error {
	if m.commitError != nil {
		return m.commitError
	}
	return nil
}

func (m *MockRepository) CommitWithSnapshot(_ context.Context, _ evt.SerializedResult, _ evt.EntityType, _ evt.EntityID, _ []byte, _ evt.EventSequence) error {
	if m.commitWithSnapshotError != nil {
		return m.commitWithSnapshotError
	}
	return nil
}

func (m *MockRepository) Delete(_ context.Context, _ []evt.SerializedEvent) error {
	return nil
}

func (m *MockRepository) PutSnapshot(
	_ context.Context,
	_ evt.EntityType,
	_ evt.EntityID,
	_ []byte,
	_ evt.EventSequence,
	_ evt.EventSequence,
) error {
	m.putSnapshotCalls++
	if m.putSnapshotError != nil {
		return m.putSnapshotError
	}
	return nil
}

func (m *MockRepository) CommitStream(_ context.Context, _ <-chan result.Result[evt.SerializedResult]) []error {
	return nil
}

func (m *MockRepository) StreamAllEvents(_ context.Context, _ evt.StreamFilter) <-chan result.Result[[]evt.SerializedEvent] {
	ch := make(chan result.Result[[]evt.SerializedEvent])
	close(ch)
	return ch
}

func (m *MockRepository) StreamEntities(_ context.Context, _ evt.StreamFilter, _ func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error)) <-chan result.Result[evt.Entity] {
	ch := make(chan result.Result[evt.Entity])
	close(ch)
	return ch
}
