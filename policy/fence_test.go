package policy_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
)

func TestFenceConflict_EngineFilledIsTransientAndMatchesTheTypedError(t *testing.T) {
	fence := evt.ReadFence{EntityID: "stream-a", EntityType: "test", Head: 4}

	err := policy.FenceConflict(fence)

	require.Equal(t, policy.ClassTransient, policy.Classify(err))
	require.True(t, policy.IsTransient(err))

	var conflict *evt.FenceConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, fence, conflict.Fence)
	require.True(t, evt.IsConflictErr(err), "a fence conflict is still an optimistic-concurrency conflict")
	require.NotContains(t, err.Error(), "item", "the transaction item index is internal")
}

func TestFenceConflict_CallerStampedIsNotRetryable(t *testing.T) {
	err := policy.FenceConflict(evt.ReadFence{EntityID: "stream-a", Head: 4, CallerStamped: true})

	require.Equal(t, policy.ClassUnknown, policy.Classify(err))
	require.False(t, policy.IsTransient(err))

	var conflict *evt.FenceConflictError
	require.ErrorAs(t, err, &conflict)
	require.True(t, conflict.Fence.CallerStamped)
}

func TestExecute_RetriesEngineFilledFenceConflictsLikeEventPutConflicts(t *testing.T) {
	config := policy.Config{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    time.Millisecond,
		Jitter:      func(time.Duration) time.Duration { return 0 },
		Sleep:       func(context.Context, time.Duration) error { return nil },
	}

	t.Run("engine-filled fence", func(t *testing.T) {
		calls := 0
		err := policy.Execute(context.Background(), config, func() error {
			calls++
			if calls < 3 {
				return policy.FenceConflict(evt.ReadFence{EntityID: "stream-a", Head: 1})
			}

			return nil
		}, policy.Hooks{})

		require.NoError(t, err)
		require.Equal(t, 3, calls)
	})

	t.Run("caller-stamped fence", func(t *testing.T) {
		calls := 0
		err := policy.Execute(context.Background(), config, func() error {
			calls++

			return policy.FenceConflict(evt.ReadFence{EntityID: "stream-a", Head: 1, CallerStamped: true})
		}, policy.Hooks{})

		var conflict *evt.FenceConflictError
		require.True(t, errors.As(err, &conflict))
		require.Equal(t, 1, calls, "a stale caller-stamped fence is surfaced, not retried")
	})
}
