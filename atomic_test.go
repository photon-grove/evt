package evt_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
)

func sizedResult() evt.SerializedResult {
	return evt.SerializedResult{
		Events: []evt.SerializedEvent{
			{ID: "a:1", Sequence: 1, Type: "t:created", Version: 1, EntityID: "a", EntityType: "retained", Payload: []byte(`{}`)},
			{ID: "b:1", Sequence: 1, Type: "t:created", Version: 1, EntityID: "b", EntityType: "plain", Payload: []byte(`{}`)},
		},
		Snapshots: []evt.SerializedSnapshot{
			{EntityType: "retained", EntityID: "a", Sequence: 1, EventSequence: 1, Payload: []byte(`{}`)},
		},
		Fences: []evt.ReadFence{{EntityID: "c", EntityType: "plain", Head: 3}},
	}
}

func TestTransactionItemSizesWithTTL_CountsTheTTLAttributeOnRetainedTypesOnly(t *testing.T) {
	result := sizedResult()

	plain, err := evt.TransactionItemSizes(result)
	require.NoError(t, err)

	const ttl = int64(1_800_000_000)
	retained, err := evt.TransactionItemSizesWithTTL(result, func(entityType evt.EntityType) int64 {
		if entityType == "retained" {
			return ttl
		}

		return 0
	})
	require.NoError(t, err)

	// Two control rows, then the event puts, the snapshot put, the floor check for the stream
	// without a snapshot, and the two checks of the fence.
	require.Len(t, plain, 2+2+1+1+2)
	require.Len(t, retained, len(plain))

	attribute := len("ttl") + len("1800000000")
	want := append([]int(nil), plain...)
	want[2] += attribute // a's event put
	want[4] += attribute // a's snapshot put
	require.Equal(t, want, retained, "only retained types' event and snapshot items carry a ttl")
}

func TestTransactionItemSizes_NilTTLEqualsNoRetention(t *testing.T) {
	result := sizedResult()

	plain, err := evt.TransactionItemSizes(result)
	require.NoError(t, err)

	withNil, err := evt.TransactionItemSizesWithTTL(result, nil)
	require.NoError(t, err)
	require.Equal(t, plain, withNil)
}

func TestSequenceGapError_IsAConflictAndMatchesTheSentinel(t *testing.T) {
	err := error(&evt.SequenceGapError{EntityID: "stream-a", After: 2, Found: 5})

	require.ErrorIs(t, err, evt.ErrSequenceGap)
	require.True(t, evt.IsConflictErr(err), "reloading resolves a gap, so it is a conflict")
	require.False(t, errors.Is(evt.NewConflictError("other"), evt.ErrSequenceGap))
	require.Equal(t,
		"stream stream-a skips from sequence 2 to 5: it was compacted during the load, or events expired",
		err.Error())
}
