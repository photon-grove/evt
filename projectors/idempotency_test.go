package projectors_test

import (
	"context"
	"testing"

	"github.com/photon-grove/evt/projectors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryIdempotencyGuard_NotProcessedByDefault(t *testing.T) {
	guard := projectors.NewInMemoryIdempotencyGuard()
	processed, err := guard.IsProcessed(context.Background(), "proj-a", "evt-1")
	require.NoError(t, err)
	assert.False(t, processed)
}

func TestInMemoryIdempotencyGuard_MarkAndCheck(t *testing.T) {
	guard := projectors.NewInMemoryIdempotencyGuard()
	ctx := context.Background()

	require.NoError(t, guard.MarkProcessed(ctx, "proj-a", "evt-1"))

	processed, err := guard.IsProcessed(ctx, "proj-a", "evt-1")
	require.NoError(t, err)
	assert.True(t, processed)
}

func TestInMemoryIdempotencyGuard_IsolatesByProjector(t *testing.T) {
	guard := projectors.NewInMemoryIdempotencyGuard()
	ctx := context.Background()

	require.NoError(t, guard.MarkProcessed(ctx, "proj-a", "evt-1"))

	// Same event ID, different projector — should not be processed.
	processed, err := guard.IsProcessed(ctx, "proj-b", "evt-1")
	require.NoError(t, err)
	assert.False(t, processed)
}

func TestInMemoryIdempotencyGuard_Reset(t *testing.T) {
	guard := projectors.NewInMemoryIdempotencyGuard()
	ctx := context.Background()

	require.NoError(t, guard.MarkProcessed(ctx, "proj-a", "evt-1"))
	guard.Reset()

	processed, err := guard.IsProcessed(ctx, "proj-a", "evt-1")
	require.NoError(t, err)
	assert.False(t, processed, "reset should clear all state")
}
