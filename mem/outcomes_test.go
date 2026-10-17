package mem_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/mem"
)

// The in-memory outcome row is a deep copy: neither the value a caller committed nor the one it
// reads back shares nested maps or slices with the stored row.
func TestOutcomeRowsDoNotAliasNestedRejectionDetails(t *testing.T) {
	ctx := context.Background()

	repo, ok := mem.NewRepository().(evt.OutcomeRepository)
	require.True(t, ok)

	inner := map[string]any{"k": "v"}
	list := []any{"a", "b"}
	nested := map[string]any{"list": list, "inner": inner}
	rejection := &evt.Rejection{Code: "c", Message: "m", Details: map[string]any{"nested": nested}}

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Outcome: &evt.Outcome{CommandID: "cmd", Status: evt.OutcomeRejected, Digest: "d", Rejection: rejection},
	}))

	// Mutating what the caller committed does not reach the stored row.
	nested["inner"] = map[string]any{"k": "tampered"}
	nested["list"] = []any{"tampered", "b"}
	inner["k"] = "tampered"
	list[0] = "tampered"

	first, err := repo.GetOutcome(ctx, "cmd")
	require.NoError(t, err)

	encoded, err := evt.EncodeRejection(*first.Rejection)
	require.NoError(t, err)
	require.JSONEq(t, `{"code":"c","message":"m","details":{"nested":{"inner":{"k":"v"},"list":["a","b"]}}}`, string(encoded))

	// Mutating what a read returned does not reach it either.
	details, ok := first.Rejection.Details["nested"].(map[string]any)
	require.True(t, ok)

	readInner, ok := details["inner"].(map[string]any)
	require.True(t, ok)
	readList, ok := details["list"].([]any)
	require.True(t, ok)

	readInner["k"] = "tampered-again"
	readList[1] = "tampered-again"

	second, err := repo.GetOutcome(ctx, "cmd")
	require.NoError(t, err)

	encoded, err = evt.EncodeRejection(*second.Rejection)
	require.NoError(t, err)
	require.JSONEq(t, `{"code":"c","message":"m","details":{"nested":{"inner":{"k":"v"},"list":["a","b"]}}}`, string(encoded))
}
