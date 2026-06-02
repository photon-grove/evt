package evt_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
)

// --- Test doubles ---

type stubEntity struct {
	evt.BaseEntity
	Value string `json:"value"`
}

func (e *stubEntity) Type() evt.EntityType                { return "stub" }
func (e *stubEntity) GetID() evt.EntityID                 { return e.ID }
func (e *stubEntity) Base() evt.BaseEntity                { return e.BaseEntity }
func (e *stubEntity) Reset() evt.Entity                   { return &stubEntity{} }
func (e *stubEntity) EventUpcasters() []evt.EventUpcaster { return nil }
func (e *stubEntity) Projectors() []evt.EventProjector    { return nil }
func (e *stubEntity) Handle(_ context.Context, _ evt.Command) (evt.CommandResult, error) {
	return evt.CommandResult{}, nil
}
func (e *stubEntity) Apply(_ evt.Event) error { return nil }
func (e *stubEntity) DeserializeEvent(_ evt.SerializedEvent) (evt.Event, error) {
	return nil, fmt.Errorf("not implemented")
}

// stubProjector records calls and returns a configurable TransactionGroup.
type stubProjector struct {
	mu    sync.Mutex
	calls []evt.Entity
	group evt.TransactionGroup
	err   error
}

func (p *stubProjector) Project(_ context.Context, entity evt.Entity, _ []evt.Event) (evt.TransactionGroup, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, entity)
	return p.group, p.err
}

func (p *stubProjector) getCalls() []evt.Entity {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]evt.Entity, len(p.calls))
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
	entities  []evt.Entity
	streamErr error
}

func (r *fakeRepo) Commit(_ context.Context, _ evt.SerializedResult) error { return nil }
func (r *fakeRepo) CommitStream(_ context.Context, _ <-chan result.Result[evt.SerializedResult]) []error {
	return nil
}
func (r *fakeRepo) CommitWithSnapshot(_ context.Context, _ evt.SerializedResult, _ evt.EntityType, _ evt.EntityID, _ []byte, _ evt.EventSequence) error {
	return nil
}
func (r *fakeRepo) GetEvents(_ context.Context, _ evt.EntityID) ([]evt.SerializedEvent, error) {
	return nil, nil
}
func (r *fakeRepo) GetLatestEvents(_ context.Context, _ evt.EntityID, _ evt.EventSequence) ([]evt.SerializedEvent, error) {
	return nil, nil
}
func (r *fakeRepo) GetSnapshot(_ context.Context, _ evt.EntityID) (*evt.SerializedSnapshot, error) {
	return nil, nil
}
func (r *fakeRepo) StreamAllEvents(_ context.Context, _ *expression.Expression) <-chan result.Result[[]evt.SerializedEvent] {
	ch := make(chan result.Result[[]evt.SerializedEvent])
	close(ch)
	return ch
}
func (r *fakeRepo) StreamEntities(
	_ context.Context,
	_ *expression.Expression,
	_ func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
) <-chan result.Result[evt.Entity] {
	ch := make(chan result.Result[evt.Entity])
	go func() {
		defer close(ch)
		if r.streamErr != nil {
			ch <- result.Err[evt.Entity](r.streamErr)
			return
		}
		for _, e := range r.entities {
			ch <- result.Ok(e)
		}
	}()
	return ch
}

// noopApplyEvent is a stub for tests where the fakeRepo yields pre-built entities.
func noopApplyEvent(_ context.Context, _ evt.SerializedEvent, e evt.Entity) (evt.Entity, error) {
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
	e1 := &stubEntity{BaseEntity: evt.BaseEntity{ID: "e1"}, Value: "hello"}
	e2 := &stubEntity{BaseEntity: evt.BaseEntity{ID: "e2"}, Value: "world"}
	repo := &fakeRepo{entities: []evt.Entity{e1, e2}}

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
	e1 := &stubEntity{BaseEntity: evt.BaseEntity{ID: "e1"}, Value: "hello"}
	repo := &fakeRepo{entities: []evt.Entity{e1}}

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
	e1 := &stubEntity{BaseEntity: evt.BaseEntity{ID: "e1"}, Value: "match"}

	// otherEntity has a different type
	other := &otherTypeEntity{BaseEntity: evt.BaseEntity{ID: "e2"}, Value: "skip"}

	repo := &fakeRepo{entities: []evt.Entity{e1, other}}
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
	e1 := &stubEntity{BaseEntity: evt.BaseEntity{ID: "e1"}, Value: "hello"}
	repo := &fakeRepo{entities: []evt.Entity{e1}}

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
	e1 := &stubEntity{BaseEntity: evt.BaseEntity{ID: "e1"}, Value: "hello"}
	repo := &fakeRepo{entities: []evt.Entity{e1}}

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
	e1 := &stubEntity{BaseEntity: evt.BaseEntity{ID: "e1"}, Value: "hello"}
	repo := &fakeRepo{entities: []evt.Entity{e1}}

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
	e1 := &stubEntity{BaseEntity: evt.BaseEntity{ID: "e1"}, Value: "a"}
	e2 := &stubEntity{BaseEntity: evt.BaseEntity{ID: "e2"}, Value: "b"}
	repo := &fakeRepo{entities: []evt.Entity{e1, e2}}

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
	e1 := &stubEntity{BaseEntity: evt.BaseEntity{ID: "e1"}, Value: "hello"}
	repo := &fakeRepo{entities: []evt.Entity{e1}}

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

func TestRebuildProjections_NilEntity(t *testing.T) {
	repo := &fakeRepo{entities: []evt.Entity{nil}}
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
	entities := make([]evt.Entity, 100)
	for i := range entities {
		entities[i] = &stubEntity{BaseEntity: evt.BaseEntity{ID: evt.EntityID(fmt.Sprintf("e%d", i))}}
	}
	repo := &fakeRepo{entities: entities}

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
	e1 := &stubEntity{BaseEntity: evt.BaseEntity{ID: "e1"}, Value: "a"}
	e2 := &stubEntity{BaseEntity: evt.BaseEntity{ID: "e2"}, Value: "b"}
	e3 := &stubEntity{BaseEntity: evt.BaseEntity{ID: "e3"}, Value: "c"}
	repo := &fakeRepo{entities: []evt.Entity{e1, e2, e3}}

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

// otherTypeEntity returns a different entity type for filtering tests.
type otherTypeEntity struct {
	evt.BaseEntity
	Value string `json:"value"`
}

func (e *otherTypeEntity) Type() evt.EntityType                { return "other" }
func (e *otherTypeEntity) GetID() evt.EntityID                 { return e.ID }
func (e *otherTypeEntity) Base() evt.BaseEntity                { return e.BaseEntity }
func (e *otherTypeEntity) Reset() evt.Entity                   { return &otherTypeEntity{} }
func (e *otherTypeEntity) EventUpcasters() []evt.EventUpcaster { return nil }
func (e *otherTypeEntity) Projectors() []evt.EventProjector    { return nil }
func (e *otherTypeEntity) Handle(_ context.Context, _ evt.Command) (evt.CommandResult, error) {
	return evt.CommandResult{}, nil
}
func (e *otherTypeEntity) Apply(_ evt.Event) error { return nil }
func (e *otherTypeEntity) DeserializeEvent(_ evt.SerializedEvent) (evt.Event, error) {
	return nil, fmt.Errorf("not implemented")
}
