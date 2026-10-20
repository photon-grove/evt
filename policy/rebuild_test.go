package policy_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
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

func TestExecuteWithRetry_WaitsOutARebuild(t *testing.T) {
	attempts := 0
	cfg := policy.Config{MaxAttempts: 3, Sleep: func(_ context.Context, _ time.Duration) error { return nil }}

	err := policy.Execute(context.Background(), cfg, func() error {
		attempts++
		if attempts < 3 {
			return &evt.RebuildInProgressError{}
		}

		return nil
	}, policy.Hooks{})
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}
