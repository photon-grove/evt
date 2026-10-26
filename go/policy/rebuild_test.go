package policy_test

import (
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
