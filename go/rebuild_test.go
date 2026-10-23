package evt_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/result"
)

// --- Test doubles ---

type stubAggregate struct {
	ID    evt.EntityID `json:"id"`
	Value string       `json:"value"`
}

func (e *stubAggregate) Type() evt.EntityType                { return "stub" }
func (e *stubAggregate) GetID() evt.EntityID                 { return e.ID }
func (e *stubAggregate) Reset() evt.Aggregate                { return &stubAggregate{} }
func (e *stubAggregate) EventUpcasters() []evt.EventUpcaster { return nil }
func (e *stubAggregate) Projectors() []evt.EventProjector    { return nil }
func (e *stubAggregate) Handle(_ context.Context, _ evt.Command) (evt.CommandResult, error) {
	return evt.CommandResult{}, nil
}
func (e *stubAggregate) Apply(_ evt.Event) error { return nil }
func (e *stubAggregate) DeserializeEvent(_ evt.SerializedEvent) (evt.Event, error) {
	return nil, fmt.Errorf("not implemented")
}

// stubProjector records calls and returns a configurable TransactionGroup.
type stubProjector struct {
	mu    sync.Mutex
	calls []evt.Aggregate
	group evt.TransactionGroup
	err   error
}

func (p *stubProjector) Project(_ context.Context, aggregate evt.Aggregate, _ []evt.Event) (evt.TransactionGroup, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, aggregate)
	return p.group, p.err
}

func (p *stubProjector) getCalls() []evt.Aggregate {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]evt.Aggregate, len(p.calls))
	copy(cp, p.calls)
	return cp
}

// stubTransactionGroup is a minimal TransactionGroup for testing.
type stubTransactionGroup struct {
	size int
}

func (g *stubTransactionGroup) Len() int { return g.size }
func (g *stubTransactionGroup) Merge(_ evt.TransactionGroup) (evt.TransactionGroup, error) {
	return g, nil
}
func (g *stubTransactionGroup) StorageType() evt.StorageType         { return "test" }
func (g *stubTransactionGroup) TransactionType() evt.TransactionType { return "test-put" }
func (g *stubTransactionGroup) HandleError(err error, _ int) error   { return err }

// fakeRepo yields a fixed set of entities via StreamEntities.
type fakeRepo struct {
	aggregates []evt.Aggregate
	streamErr  error
}

func (r *fakeRepo) Commit(_ context.Context, _ evt.SerializedResult) error { return nil }
func (r *fakeRepo) CommitWithSnapshot(_ context.Context, _ evt.SerializedResult, _ evt.EntityType, _ evt.EntityID, _ []byte, _ evt.EventSequence) error {
	return nil
}
func (r *fakeRepo) GetEvents(_ context.Context, _ evt.EntityID) ([]evt.SerializedEvent, error) {
	return nil, nil
}
func (r *fakeRepo) GetRecentEvents(_ context.Context, _ evt.EntityID, _ int) ([]evt.SerializedEvent, error) {
	return nil, nil
}
func (r *fakeRepo) GetLatestEvents(_ context.Context, _ evt.EntityID, _ evt.EventSequence) ([]evt.SerializedEvent, error) {
	return nil, nil
}
func (r *fakeRepo) GetSnapshot(_ context.Context, _ evt.EntityID) (*evt.SerializedSnapshot, error) {
	return nil, nil
}
func (r *fakeRepo) StreamAllEvents(_ context.Context, _ evt.StreamFilter) <-chan result.Result[[]evt.SerializedEvent] {
	ch := make(chan result.Result[[]evt.SerializedEvent])
	close(ch)
	return ch
}
func (r *fakeRepo) StreamEntities(
	ctx context.Context,
	_ evt.StreamFilter,
	_ func(context.Context, evt.SerializedEvent, evt.Aggregate) (evt.Aggregate, error),
) <-chan result.Result[evt.Aggregate] {
	ch := make(chan result.Result[evt.Aggregate])
	go func() {
		defer close(ch)
		if r.streamErr != nil {
			select {
			case ch <- result.Err[evt.Aggregate](r.streamErr):
			case <-ctx.Done():
			}
			return
		}
		for _, e := range r.aggregates {
			// Honor cancellation so an early-returning consumer does not strand this producer.
			select {
			case ch <- result.Ok(e):
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

// noopApplyEvent is a stub for tests where the fakeRepo yields pre-built entities.
func noopApplyEvent(_ context.Context, _ evt.SerializedEvent, e evt.Aggregate) (evt.Aggregate, error) {
	return e, nil
}

// --- Tests ---

func TestRebuildProjections_NilApplyEvent(t *testing.T) {
	repo := &fakeRepo{}
	_, err := evt.RebuildProjections(context.Background(), repo, nil, evt.RebuildConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "applyEvent callback is required")
}

func TestRebuildProjections_NoProjectors(t *testing.T) {
	repo := &fakeRepo{}
	_, err := evt.RebuildProjections(context.Background(), repo, noopApplyEvent, evt.RebuildConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one projector is required")
}

func TestRebuildProjections_NoCommitGroupWhenNotDryRun(t *testing.T) {
	repo := &fakeRepo{}
	proj := &stubProjector{}
	_, err := evt.RebuildProjections(context.Background(), repo, noopApplyEvent, evt.RebuildConfig{
		Projectors: []evt.EventProjector{proj},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CommitGroup is required")
}

func TestRebuildProjections_EmptyRepo(t *testing.T) {
	repo := &fakeRepo{}
	proj := &stubProjector{}
	res, err := evt.RebuildProjections(context.Background(), repo, noopApplyEvent, evt.RebuildConfig{
		Projectors: []evt.EventProjector{proj},
		DryRun:     true,
	})
	require.NoError(t, err)
	assert.Equal(t, 0, res.Processed)
	assert.Equal(t, 0, res.Skipped)
	assert.Empty(t, res.Errors)
}

func TestRebuildProjections_ProcessesEntities(t *testing.T) {
	e1 := &stubAggregate{ID: "e1", Value: "hello"}
	e2 := &stubAggregate{ID: "e2", Value: "world"}
	repo := &fakeRepo{aggregates: []evt.Aggregate{e1, e2}}

	txnGroup := &stubTransactionGroup{size: 1}
	proj := &stubProjector{group: txnGroup}

	var committed int
	commitGroup := func(_ context.Context, _ evt.TransactionGroup) error {
		committed++
		return nil
	}

	res, err := evt.RebuildProjections(context.Background(), repo, noopApplyEvent, evt.RebuildConfig{
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: commitGroup,
	})

	require.NoError(t, err)
	assert.Equal(t, 2, res.Processed)
	assert.Equal(t, 0, res.Skipped)
	assert.Empty(t, res.Errors)
	assert.Equal(t, 2, committed)

	calls := proj.getCalls()
	assert.Len(t, calls, 2)
	assert.Equal(t, evt.EntityID("e1"), calls[0].GetID())
	assert.Equal(t, evt.EntityID("e2"), calls[1].GetID())
}

func TestRebuildProjections_DryRunSkipsCommit(t *testing.T) {
	e1 := &stubAggregate{ID: "e1", Value: "hello"}
	repo := &fakeRepo{aggregates: []evt.Aggregate{e1}}

	txnGroup := &stubTransactionGroup{size: 1}
	proj := &stubProjector{group: txnGroup}

	res, err := evt.RebuildProjections(context.Background(), repo, noopApplyEvent, evt.RebuildConfig{
		Projectors: []evt.EventProjector{proj},
		DryRun:     true,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, res.Processed)
	assert.Empty(t, res.Errors)
}

func TestRebuildProjections_FiltersEntityType(t *testing.T) {
	e1 := &stubAggregate{ID: "e1", Value: "match"}

	// otherEntity has a different type
	other := &otherTypeAggregate{ID: "e2", Value: "skip"}

	repo := &fakeRepo{aggregates: []evt.Aggregate{e1, other}}
	proj := &stubProjector{group: &stubTransactionGroup{size: 1}}

	var committed int
	commitGroup := func(_ context.Context, _ evt.TransactionGroup) error {
		committed++
		return nil
	}

	res, err := evt.RebuildProjections(context.Background(), repo, noopApplyEvent, evt.RebuildConfig{
		EntityType:  "stub",
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: commitGroup,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, res.Processed)
	assert.Equal(t, 1, res.Skipped)
}

func TestRebuildProjections_ProjectorError(t *testing.T) {
	e1 := &stubAggregate{ID: "e1", Value: "hello"}
	repo := &fakeRepo{aggregates: []evt.Aggregate{e1}}

	proj := &stubProjector{err: errors.New("projection failed")}

	res, err := evt.RebuildProjections(context.Background(), repo, noopApplyEvent, evt.RebuildConfig{
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: func(_ context.Context, _ evt.TransactionGroup) error { return nil },
	})

	require.NoError(t, err) // rebuild itself succeeds
	assert.Equal(t, 0, res.Processed)
	assert.Len(t, res.Errors, 1)
	assert.Contains(t, res.Errors[0].Error(), "projection failed")
}

func TestRebuildProjections_CommitError(t *testing.T) {
	e1 := &stubAggregate{ID: "e1", Value: "hello"}
	repo := &fakeRepo{aggregates: []evt.Aggregate{e1}}

	proj := &stubProjector{group: &stubTransactionGroup{size: 1}}

	commitGroup := func(_ context.Context, _ evt.TransactionGroup) error {
		return errors.New("commit failed")
	}

	res, err := evt.RebuildProjections(context.Background(), repo, noopApplyEvent, evt.RebuildConfig{
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: commitGroup,
	})

	require.NoError(t, err)
	assert.Equal(t, 0, res.Processed)
	assert.Len(t, res.Errors, 1)
	assert.Contains(t, res.Errors[0].Error(), "commit failed")
}

func TestRebuildProjections_StreamError(t *testing.T) {
	repo := &fakeRepo{streamErr: errors.New("stream broke")}
	proj := &stubProjector{group: &stubTransactionGroup{size: 1}}

	res, err := evt.RebuildProjections(context.Background(), repo, noopApplyEvent, evt.RebuildConfig{
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: func(_ context.Context, _ evt.TransactionGroup) error { return nil },
	})

	require.NoError(t, err)
	assert.Len(t, res.Errors, 1)
	assert.Contains(t, res.Errors[0].Error(), "stream broke")
}

func TestRebuildProjections_NilProjectorResult(t *testing.T) {
	e1 := &stubAggregate{ID: "e1", Value: "hello"}
	repo := &fakeRepo{aggregates: []evt.Aggregate{e1}}

	// Projector returns nil group (no-op)
	proj := &stubProjector{group: nil}

	res, err := evt.RebuildProjections(context.Background(), repo, noopApplyEvent, evt.RebuildConfig{
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: func(_ context.Context, _ evt.TransactionGroup) error { return nil },
	})

	require.NoError(t, err)
	assert.Equal(t, 1, res.Processed)
	assert.Empty(t, res.Errors)
}

func TestRebuildProjections_OnProgress(t *testing.T) {
	e1 := &stubAggregate{ID: "e1", Value: "a"}
	e2 := &stubAggregate{ID: "e2", Value: "b"}
	repo := &fakeRepo{aggregates: []evt.Aggregate{e1, e2}}

	proj := &stubProjector{group: &stubTransactionGroup{size: 1}}

	var progressCalls []struct{ processed, errors int }
	onProgress := func(processed, errs int) {
		progressCalls = append(progressCalls, struct{ processed, errors int }{processed, errs})
	}

	res, err := evt.RebuildProjections(context.Background(), repo, noopApplyEvent, evt.RebuildConfig{
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: func(_ context.Context, _ evt.TransactionGroup) error { return nil },
		OnProgress:  onProgress,
	})

	require.NoError(t, err)
	assert.Equal(t, 2, res.Processed)
	assert.Len(t, progressCalls, 2)
	assert.Equal(t, 1, progressCalls[0].processed)
	assert.Equal(t, 2, progressCalls[1].processed)
}

func TestRebuildProjections_MultipleProjectors(t *testing.T) {
	e1 := &stubAggregate{ID: "e1", Value: "hello"}
	repo := &fakeRepo{aggregates: []evt.Aggregate{e1}}

	proj1 := &stubProjector{group: &stubTransactionGroup{size: 1}}
	proj2 := &stubProjector{group: &stubTransactionGroup{size: 2}}

	var committed int
	commitGroup := func(_ context.Context, _ evt.TransactionGroup) error {
		committed++
		return nil
	}

	res, err := evt.RebuildProjections(context.Background(), repo, noopApplyEvent, evt.RebuildConfig{
		Projectors:  []evt.EventProjector{proj1, proj2},
		CommitGroup: commitGroup,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, res.Processed)
	assert.Equal(t, 2, committed) // one commit per projector per entity
}

func TestRebuildProjections_NilAggregate(t *testing.T) {
	repo := &fakeRepo{aggregates: []evt.Aggregate{nil}}
	proj := &stubProjector{group: &stubTransactionGroup{size: 1}}

	res, err := evt.RebuildProjections(context.Background(), repo, noopApplyEvent, evt.RebuildConfig{
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: func(_ context.Context, _ evt.TransactionGroup) error { return nil },
	})

	require.NoError(t, err)
	assert.Equal(t, 0, res.Processed)
	assert.Equal(t, 1, res.Skipped)
}

func TestRebuildProjections_ContextCancellation(t *testing.T) {
	// Create a slow repo that yields many entities
	aggregates := make([]evt.Aggregate, 100)
	for i := range aggregates {
		aggregates[i] = &stubAggregate{ID: evt.EntityID(fmt.Sprintf("e%d", i))}
	}
	repo := &fakeRepo{aggregates: aggregates}

	proj := &stubProjector{group: &stubTransactionGroup{size: 1}}

	ctx, cancel := context.WithCancel(context.Background())

	var committed int
	commitGroup := func(_ context.Context, _ evt.TransactionGroup) error {
		committed++
		if committed == 3 {
			cancel()
		}
		return nil
	}

	res, err := evt.RebuildProjections(ctx, repo, noopApplyEvent, evt.RebuildConfig{
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: commitGroup,
	})

	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)
	// Should have processed some but not all entities
	assert.True(t, res.Processed < 100)
	assert.True(t, res.Processed >= 3)
}

func TestRebuildProjections_MaxErrors(t *testing.T) {
	e1 := &stubAggregate{ID: "e1", Value: "a"}
	e2 := &stubAggregate{ID: "e2", Value: "b"}
	e3 := &stubAggregate{ID: "e3", Value: "c"}
	repo := &fakeRepo{aggregates: []evt.Aggregate{e1, e2, e3}}

	proj := &stubProjector{err: errors.New("always fails")}

	res, err := evt.RebuildProjections(context.Background(), repo, noopApplyEvent, evt.RebuildConfig{
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: func(_ context.Context, _ evt.TransactionGroup) error { return nil },
		MaxErrors:   2,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "MaxErrors limit")
	assert.Len(t, res.Errors, 2)
	assert.Equal(t, 0, res.Processed)
}

// --- Helper types ---

// otherTypeAggregate returns a different entity type for filtering tests.
type otherTypeAggregate struct {
	ID    evt.EntityID `json:"id"`
	Value string       `json:"value"`
}

func (e *otherTypeAggregate) Type() evt.EntityType                { return "other" }
func (e *otherTypeAggregate) GetID() evt.EntityID                 { return e.ID }
func (e *otherTypeAggregate) Reset() evt.Aggregate                { return &otherTypeAggregate{} }
func (e *otherTypeAggregate) EventUpcasters() []evt.EventUpcaster { return nil }
func (e *otherTypeAggregate) Projectors() []evt.EventProjector    { return nil }
func (e *otherTypeAggregate) Handle(_ context.Context, _ evt.Command) (evt.CommandResult, error) {
	return evt.CommandResult{}, nil
}
func (e *otherTypeAggregate) Apply(_ evt.Event) error { return nil }
func (e *otherTypeAggregate) DeserializeEvent(_ evt.SerializedEvent) (evt.Event, error) {
	return nil, fmt.Errorf("not implemented")
}

// streamOf returns a closed channel yielding the given entities as Ok results.
func streamOf(aggregates ...evt.Aggregate) <-chan result.Result[evt.Aggregate] {
	ch := make(chan result.Result[evt.Aggregate])
	go func() {
		defer close(ch)
		for _, e := range aggregates {
			ch <- result.Ok(e)
		}
	}()
	return ch
}

func TestRebuildProjectionsFromStream_ProcessesEntities(t *testing.T) {
	e1 := &stubAggregate{ID: "e1", Value: "hello"}
	e2 := &stubAggregate{ID: "e2", Value: "world"}

	proj := &stubProjector{group: &stubTransactionGroup{size: 1}}
	var committed int

	res, err := evt.RebuildProjectionsFromStream(context.Background(), streamOf(e1, e2), evt.RebuildConfig{
		Projectors:  []evt.EventProjector{proj},
		CommitGroup: func(_ context.Context, _ evt.TransactionGroup) error { committed++; return nil },
	})

	require.NoError(t, err)
	assert.Equal(t, 2, res.Processed)
	assert.Equal(t, 2, committed)
	assert.Len(t, proj.getCalls(), 2)
}

func TestRebuildProjectionsFromStream_RequiresProjectors(t *testing.T) {
	_, err := evt.RebuildProjectionsFromStream(context.Background(), streamOf(), evt.RebuildConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one projector is required")
}

func TestRebuildProjectionsFromStream_PropagatesStreamError(t *testing.T) {
	ch := make(chan result.Result[evt.Aggregate])
	go func() {
		defer close(ch)
		ch <- result.Err[evt.Aggregate](errors.New("stream broke"))
	}()

	res, err := evt.RebuildProjectionsFromStream(context.Background(), ch, evt.RebuildConfig{
		Projectors:  []evt.EventProjector{&stubProjector{group: &stubTransactionGroup{size: 1}}},
		CommitGroup: func(_ context.Context, _ evt.TransactionGroup) error { return nil },
	})

	require.NoError(t, err)
	require.Len(t, res.Errors, 1)
	assert.Contains(t, res.Errors[0].Error(), "stream broke")
}

func TestRebuildProjectionsFromStream_CancelledStreamReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// An already-closed, empty stream models a producer that stopped on cancellation without
	// delivering a final item. The rebuild must report the cancellation, not success.
	res, err := evt.RebuildProjectionsFromStream(ctx, streamOf(), evt.RebuildConfig{
		Projectors:  []evt.EventProjector{&stubProjector{group: &stubTransactionGroup{size: 1}}},
		CommitGroup: func(_ context.Context, _ evt.TransactionGroup) error { return nil },
	})

	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, res)
}
