package policy_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/policy"
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
