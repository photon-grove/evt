package policy_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/policy"
)

// The rebuild fence's refusals classify transient whoever returns them, wrapped or not, because a
// later attempt can succeed once the rebuild ends; the rest of the lifecycle's errors are final.
func TestClassify_RebuildFenceRefusalsAreTransient(t *testing.T) {
	for name, err := range map[string]error{
		"a refused commit":      &evt.RebuildInProgressError{},
		"a lost Begin race":     &evt.RebuildConflictError{Attempts: 8},
		"an unavailable reader": &evt.RebuildUnavailableError{Generation: evt.RebuildGeneration{Value: 3}},
		"a changed epoch":       &evt.RebuildEpochChangedError{Observed: 2, Current: 4},
		"a wrapped refusal":     fmt.Errorf("committing: %w", &evt.RebuildInProgressError{}),
	} {
		require.Equal(t, policy.ClassTransient, policy.Classify(err), name)
		require.True(t, policy.IsTransient(err), name)
	}

	for name, err := range map[string]error{
		"a held lock":  &evt.RebuildLockHeldError{Owner: "job"},
		"a lost lease": &evt.RebuildLeaseLostError{Owner: "job"},
	} {
		require.Equal(t, policy.ClassUnknown, policy.Classify(err), name)
	}

	// An explicit classification wins over the fence's default.
	permanent := &policy.ClassifiedError{Class: policy.ClassPermanent, Err: &evt.RebuildInProgressError{}}
	require.Equal(t, policy.ClassPermanent, policy.Classify(permanent))
}

// A Begin whose result is unknown may hold the lock, so it is never transient, even when its cause
// is explicitly classified as such.
func TestClassify_AnIndeterminateBeginIsUnknown(t *testing.T) {
	for name, cause := range map[string]error{
		"a timeout":          context.DeadlineExceeded,
		"a classified cause": &policy.ClassifiedError{Class: policy.ClassTransient, Err: context.DeadlineExceeded},
	} {
		err := fmt.Errorf("rebuilding: %w", &evt.RebuildBeginIndeterminateError{Cause: cause})
		require.Equal(t, policy.ClassUnknown, policy.Classify(err), name)
	}

	// A classification the caller wraps around it is its own, and wins.
	dropped := &policy.ClassifiedError{Class: policy.ClassPermanent, Err: &evt.RebuildBeginIndeterminateError{
		Cause: &policy.ClassifiedError{Class: policy.ClassTransient, Err: context.DeadlineExceeded},
	}}
	require.Equal(t, policy.ClassPermanent, policy.Classify(dropped))
}
