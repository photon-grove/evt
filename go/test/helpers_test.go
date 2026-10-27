package test

import (
	"context"
	"errors"
	"testing"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/result"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type stubRepo struct{}

func (stubRepo) Commit(context.Context, evt.SerializedResult) error { return nil }

func (stubRepo) CommitWithSnapshot(context.Context, evt.SerializedResult, evt.EntityType, evt.EntityID, []byte, evt.EventSequence) error {
	return nil
}
func (stubRepo) GetEvents(context.Context, evt.EntityID) ([]evt.SerializedEvent, error) {
	return nil, nil
}
func (stubRepo) GetLatestEvents(context.Context, evt.EntityID, evt.EventSequence) ([]evt.SerializedEvent, error) {
	return nil, nil
}
func (stubRepo) GetRecentEvents(context.Context, evt.EntityID, int) ([]evt.SerializedEvent, error) {
	return nil, nil
}
func (stubRepo) GetSnapshot(context.Context, evt.EntityID) (*evt.SerializedSnapshot, error) {
	return nil, nil
}
func (stubRepo) StreamAllEvents(context.Context, evt.StreamFilter) <-chan result.Result[[]evt.SerializedEvent] {
	ch := make(chan result.Result[[]evt.SerializedEvent])
	close(ch)
	return ch
}
func (stubRepo) StreamEntities(context.Context, evt.StreamFilter, func(context.Context, evt.SerializedEvent, evt.Aggregate) (evt.Aggregate, error)) <-chan result.Result[evt.Aggregate] {
	ch := make(chan result.Result[evt.Aggregate])
	close(ch)
	return ch
}

type stubStore struct {
	load   func(ctx context.Context, aggregate evt.Aggregate, id evt.EntityID) (evt.CommandContext, error)
	exec   func(ctx context.Context, aggregate evt.Aggregate, id evt.EntityID, cmd evt.Command, meta evt.Metadata) error
	commit func(ctx context.Context, result evt.CommandResult, cc evt.CommandContext, meta evt.Metadata) ([]evt.SerializedEvent, error)
}

func (s stubStore) LoadAggregate(ctx context.Context, aggregate evt.Aggregate, id evt.EntityID) (evt.CommandContext, error) {
	return s.load(ctx, aggregate, id)
}
func (s stubStore) Execute(ctx context.Context, aggregate evt.Aggregate, id evt.EntityID, cmd evt.Command, meta evt.Metadata) error {
	return s.exec(ctx, aggregate, id, cmd, meta)
}
func (s stubStore) Commit(ctx context.Context, result evt.CommandResult, cc evt.CommandContext, meta evt.Metadata) ([]evt.SerializedEvent, error) {
	return s.commit(ctx, result, cc, meta)
}
func Test_Commands_And_Events_Accessors(t *testing.T) {
	other := "x"

	create := CreateAggregate{Value: "v", Other: &other}
	require.Equal(t, EntityType, create.EntityType())
	require.Equal(t, CreateCommand, create.Type())

	replace := ReplaceAggregate{Value: "v2", Other: nil}
	require.Equal(t, EntityType, replace.EntityType())
	require.Equal(t, ReplaceCommand, replace.Type())

	fc := &FakeCommand{}
	require.Equal(t, EntityType, fc.EntityType())
	require.Equal(t, CreateCommand, fc.Type())

	created := AggregateCreated{ID: "e1", Value: "v", Other: &other}
	require.Equal(t, EntityType, created.EntityType())
	require.Equal(t, evt.EntityID("e1"), created.EntityID())
	require.Equal(t, CreatedEvent, created.Type())
	require.Equal(t, evt.EventVersion(1), created.Version())

	updated := AggregateUpdated{ID: "e1", Value: "v2", Other: nil}
	require.Equal(t, EntityType, updated.EntityType())
	require.Equal(t, evt.EntityID("e1"), updated.EntityID())
	require.Equal(t, UpdatedEvent, updated.Type())
	require.Equal(t, evt.EventVersion(1), updated.Version())

	fe := &FakeEvent{}
	require.Equal(t, EntityType, fe.EntityType())
	require.Equal(t, evt.EventType("test:fake"), fe.Type())
	require.Equal(t, evt.EventVersion(1), fe.Version())
}

func Test_Framework_WithEvents_Execute_Load(t *testing.T) {
	ctx := context.Background()

	store := stubStore{
		load: func(_ context.Context, aggregate evt.Aggregate, _ evt.EntityID) (evt.CommandContext, error) {
			seq := evt.EventSequence(0)
			snap := evt.EventSequence(0)
			return evt.CommandContext{Aggregate: aggregate, EntityID: "e1", CurrentSequence: &seq, CurrentSnapshot: &snap}, nil
		},
		exec: func(context.Context, evt.Aggregate, evt.EntityID, evt.Command, evt.Metadata) error { return nil },
		commit: func(context.Context, evt.CommandResult, evt.CommandContext, evt.Metadata) ([]evt.SerializedEvent, error) {
			return nil, nil
		},
	}

	f := NewFramework(stubRepo{}, store, func() evt.Aggregate {
		return NewAggregate("e1")
	}, "e1")

	loaded, err := f.WithEvents(ctx, []evt.Event{AggregateCreated{ID: "e1", Value: "v"}})
	require.NoError(t, err)
	require.NotNil(t, loaded)

	loaded, err = f.Execute(ctx, ReplaceAggregate{Value: "v2"}, evt.Metadata{})
	require.NoError(t, err)
	require.NotNil(t, loaded)

	loaded, err = f.Load(ctx)
	require.NoError(t, err)
	require.NotNil(t, loaded)
}

func Test_FrameworkWithFactory_UsesFactoryForEachFreshLoad(t *testing.T) {
	ctx := context.Background()

	factoryCalls := 0
	store := stubStore{
		load: func(_ context.Context, aggregate evt.Aggregate, _ evt.EntityID) (evt.CommandContext, error) {
			seq := evt.EventSequence(0)
			snap := evt.EventSequence(0)
			return evt.CommandContext{Aggregate: aggregate, EntityID: "e1", CurrentSequence: &seq, CurrentSnapshot: &snap}, nil
		},
		exec: func(context.Context, evt.Aggregate, evt.EntityID, evt.Command, evt.Metadata) error { return nil },
		commit: func(context.Context, evt.CommandResult, evt.CommandContext, evt.Metadata) ([]evt.SerializedEvent, error) {
			return nil, nil
		},
	}

	f := NewFrameworkWithFactory(stubRepo{}, store, func() evt.Aggregate {
		factoryCalls++
		return NewAggregate("e1")
	}, "e1")

	_, err := f.Load(ctx)
	require.NoError(t, err)

	_, err = f.Execute(ctx, ReplaceAggregate{Value: "v2"}, evt.Metadata{})
	require.NoError(t, err)

	require.Equal(t, 3, factoryCalls)
}

func Test_MockStore_Methods(t *testing.T) {
	ms := NewMockStore()

	// LoadAggregate: nil return
	ms.On("LoadAggregate", mock.Anything, mock.Anything, evt.EntityID("e1")).Return(nil, errors.New("nope")).Once()
	_, err := ms.LoadAggregate(context.Background(), NewAggregate("e1"), "e1")
	require.Error(t, err)

	// LoadAggregate: wrong type return
	ms.On("LoadAggregate", mock.Anything, mock.Anything, evt.EntityID("e1")).Return("bad", nil).Once()
	_, err = ms.LoadAggregate(context.Background(), NewAggregate("e1"), "e1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected evt.CommandContext")

	// LoadAggregate: success with pointer (backward compatible)
	ms.On("LoadAggregate", mock.Anything, mock.Anything, evt.EntityID("e1")).Return(&evt.CommandContext{EntityID: "e1"}, nil).Once()
	_, err = ms.LoadAggregate(context.Background(), NewAggregate("e1"), "e1")
	require.NoError(t, err)

	// LoadAggregate: success with value
	ms.On("LoadAggregate", mock.Anything, mock.Anything, evt.EntityID("e1")).Return(evt.CommandContext{EntityID: "e1"}, nil).Once()
	_, err = ms.LoadAggregate(context.Background(), NewAggregate("e1"), "e1")
	require.NoError(t, err)

	// Commit: nil return
	ms.On("Commit", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("nope")).Once()
	_, err = ms.Commit(context.Background(), evt.CommandResult{}, evt.CommandContext{}, evt.Metadata{})
	require.Error(t, err)

	// Commit: wrong type return
	ms.On("Commit", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("bad", nil).Once()
	_, err = ms.Commit(context.Background(), evt.CommandResult{}, evt.CommandContext{}, evt.Metadata{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected []evt.SerializedEvent")

	// Commit: success
	ms.On("Commit", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]evt.SerializedEvent{}, nil).Once()
	_, err = ms.Commit(context.Background(), evt.CommandResult{}, evt.CommandContext{}, evt.Metadata{})
	require.NoError(t, err)

	// Execute: error propagation
	ms.On("Execute", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("nope")).Once()
	require.Error(t, ms.Execute(context.Background(), NewAggregate("e1"), "e1", ReplaceAggregate{}, evt.Metadata{}))

	ms.On("Execute", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, ms.Execute(context.Background(), NewAggregate("e1"), "e1", ReplaceAggregate{}, evt.Metadata{}))

}
