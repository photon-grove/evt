package conformance

import (
	"context"

	"github.com/photon-grove/evt"
)

// FaultRepository wraps an evt.OutcomeRepository and lets a test intercept Commit, so a commit can
// really apply while the caller observes an indeterminate error — the situation ambiguous-write
// recovery exists for. Everything else, including GetOutcome, is the wrapped repository's.
//
// Only Commit is intercepted; the engine's outcome path commits through it.
type FaultRepository struct {
	evt.OutcomeRepository

	// Intercept, when non-nil, replaces Commit. It receives the result and a function that performs
	// the real commit on the wrapped repository.
	Intercept func(ctx context.Context, result evt.SerializedResult, commit func() error) error
}

// NewFaultRepository wraps repo, which must implement evt.OutcomeRepository.
func NewFaultRepository(repo evt.Repository) (*FaultRepository, error) {
	outcomes, err := evt.RequireOutcomes(repo)
	if err != nil {
		return nil, err
	}

	return &FaultRepository{OutcomeRepository: outcomes}, nil
}

// Commit runs the interceptor, or the real commit when none is set.
func (f *FaultRepository) Commit(ctx context.Context, result evt.SerializedResult) error {
	commit := func() error { return f.OutcomeRepository.Commit(ctx, result) }

	if f.Intercept == nil {
		return commit()
	}

	return f.Intercept(ctx, result, commit)
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
