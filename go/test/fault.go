package test

// WARNING: Not intended for use in Production

import (
	"context"
	"fmt"

	evt "github.com/photon-grove/evt/go"
)

// FaultRepository wraps an evt.OutcomeRepository and lets a test intercept Commit, so a commit can
// really apply while the caller observes an indeterminate error — the situation ambiguous-write
// recovery exists for. Everything else, including GetOutcome, is the wrapped repository's.
//
// Commit is intercepted, as the engine's outcome path commits through it, and so is GetSnapshot,
// the first read of a load, so a test can run another writer just before a load.
type FaultRepository struct {
	evt.OutcomeRepository

	// Intercept, when non-nil, replaces Commit. It receives the result and a function that performs
	// the real commit on the wrapped repository.
	Intercept func(ctx context.Context, result evt.SerializedResult, commit func() error) error

	// BeforeLoad, when non-nil, runs before GetSnapshot reads the wrapped repository: before the
	// load of an aggregate reads anything of its stream.
	BeforeLoad func(ctx context.Context, entityID evt.EntityID)
}

// NewFaultRepository wraps repo, which must implement evt.OutcomeRepository.
func NewFaultRepository(repo evt.Repository) (*FaultRepository, error) {
	outcomes, err := evt.RequireOutcomes(repo)
	if err != nil {
		return nil, err
	}

	return &FaultRepository{OutcomeRepository: outcomes}, nil
}

// WritesViews reports the wrapped repository's capability (evt.ViewWritingRepository): a decorator
// hides it unless it declares it too, and it must not claim one the wrapped repository lacks.
func (f *FaultRepository) WritesViews() bool { return evt.WritesViews(f.OutcomeRepository) }

// DeclareHistoryDependent forwards to the wrapped repository (evt.HistoryDeclarer), or refuses when
// it cannot declare.
func (f *FaultRepository) DeclareHistoryDependent(ctx context.Context, types ...evt.EntityType) error {
	declarer, ok := f.OutcomeRepository.(evt.HistoryDeclarer)
	if !ok {
		return &evt.HistoryDeclarationUnsupportedError{Repository: fmt.Sprintf("%T", f.OutcomeRepository)}
	}

	return declarer.DeclareHistoryDependent(ctx, types...)
}

// Commit runs the interceptor, or the real commit when none is set.
func (f *FaultRepository) Commit(ctx context.Context, result evt.SerializedResult) error {
	commit := func() error { return f.OutcomeRepository.Commit(ctx, result) }

	if f.Intercept == nil {
		return commit()
	}

	return f.Intercept(ctx, result, commit)
}

// GetSnapshot runs BeforeLoad, then reads the wrapped repository's snapshot.
func (f *FaultRepository) GetSnapshot(ctx context.Context, entityID evt.EntityID) (*evt.SerializedSnapshot, error) {
	if f.BeforeLoad != nil {
		f.BeforeLoad(ctx, entityID)
	}

	return f.OutcomeRepository.GetSnapshot(ctx, entityID)
}

// ApplyThenFail arms the repository so the next commit really applies and the caller then observes
// an indeterminate *evt.AmbiguousWriteError with the given cause. The interceptor disarms itself.
func (f *FaultRepository) ApplyThenFail(cause error) {
	f.Intercept = func(_ context.Context, result evt.SerializedResult, commit func() error) error {
		f.Intercept = nil

		if err := commit(); err != nil {
			return err
		}

		return ambiguous(result, cause)
	}
}

// FailWithoutApplying arms the repository so the next commit is dropped and the caller observes an
// indeterminate *evt.AmbiguousWriteError: the commit did not apply. The interceptor disarms itself.
func (f *FaultRepository) FailWithoutApplying(cause error) {
	f.Intercept = func(_ context.Context, result evt.SerializedResult, _ func() error) error {
		f.Intercept = nil

		return ambiguous(result, cause)
	}
}

func ambiguous(result evt.SerializedResult, cause error) error {
	var id evt.CommandID
	if result.Outcome != nil {
		id = result.Outcome.CommandID
	}

	return &evt.AmbiguousWriteError{CommandID: id, Cause: cause}
}
