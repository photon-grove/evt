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

	// The feature-marker check, then the event puts, the snapshot put, one head update per written
	// stream, and the one head check of the fence.
	require.Len(t, plain, 1+2+1+2+1)
	require.Len(t, retained, len(plain))

	attribute := len("ttl") + len("1800000000")
	want := append([]int(nil), plain...)
	want[1] += attribute // a's event put
	want[3] += attribute // a's snapshot put

	// a's head update also raises the head item's ttl: the longer update expression, the name
	// placeholder and name, and the :t value.
	want[4] += len("SET head = :last, #ttl = :t") - len("SET head = :last") +
		len("#ttl") + len("ttl") + len(":t") + len("1800000000")
	require.Equal(t, want, retained, "only retained types' items carry a ttl")

	// The head updates (create, so no :prev) and the fence check (one check at head 3).
	require.Equal(t, 2+2+len("a")+len("-1")+len(":last")+len("1")+len("SET head = :last")+len("attribute_not_exists(head)"), plain[4])
	require.Equal(t, 2+2+len("c")+len("-1")+len("head = :head")+len(":head")+len("3"), plain[6])
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

func TestCommitMetadata_TreatsAnEmptyCommandIDAsAbsent(t *testing.T) {
	empty := evt.CommandID("")

	generated := evt.CommitMetadata(evt.Metadata{CommandID: &empty})
	require.Len(t, generated.CommitID, 26, "a ULID is generated, not the empty command ID")

	pinned := evt.CommitMetadata(evt.Metadata{CommandID: &empty, CommitID: "caller-commit"})
	require.Equal(t, "caller-commit", pinned.CommitID)

	command := evt.CommandID("cmd-1")
	derived := evt.CommitMetadata(evt.Metadata{CommandID: &command, CommitID: "caller-commit"})
	require.Equal(t, "cmd-1", derived.CommitID, "a non-empty command ID wins")
}

func TestPreflightCommit_BudgetsOnlyEmittedActions(t *testing.T) {
	events := func(count int) []evt.SerializedEvent {
		out := make([]evt.SerializedEvent, 0, count)
		for sequence := 1; sequence <= count; sequence++ {
			out = append(out, evt.SerializedEvent{
				ID: evt.GetEventID("a", evt.EventSequence(sequence)), Sequence: evt.EventSequence(sequence),
				Type: "t:created", Version: 1, EntityID: "a", EntityType: "t", Payload: []byte(`{}`),
			})
		}

		return out
	}

	// The feature-marker check, 98 event puts, and the head update fill the 100 actions exactly.
	fits, err := evt.TransactionItemSizes(evt.SerializedResult{Events: events(98)})
	require.NoError(t, err)
	require.Len(t, fits, 100)
	require.NoError(t, evt.PreflightCommit(evt.SerializedResult{Events: events(98)}))

	var tooLarge *evt.CommitTooLargeError
	require.ErrorAs(t, evt.PreflightCommit(evt.SerializedResult{Events: events(99)}), &tooLarge)
	require.Equal(t, 101, tooLarge.Items)
}

type sizedGroup struct{ sizes []int }

func (g sizedGroup) Len() int                           { return len(g.sizes) }
func (sizedGroup) StorageType() evt.StorageType         { return "test" }
func (sizedGroup) TransactionType() evt.TransactionType { return "test" }
func (sizedGroup) HandleError(err error, _ int) error   { return err }
func (g sizedGroup) Merge(evt.TransactionGroup) (evt.TransactionGroup, error) {
	return g, nil
}
func (g sizedGroup) OperationSizes() []int { return g.sizes }

type opaqueGroup struct{ size int }

func (g opaqueGroup) Len() int                           { return g.size }
func (opaqueGroup) StorageType() evt.StorageType         { return "test" }
func (opaqueGroup) TransactionType() evt.TransactionType { return "test" }
func (opaqueGroup) HandleError(err error, _ int) error   { return err }
func (g opaqueGroup) Merge(evt.TransactionGroup) (evt.TransactionGroup, error) {
	return g, nil
}

func TestProjectorOperationSizes_OneEntryPerOperation(t *testing.T) {
	result := evt.SerializedResult{Transaction: evt.Transaction{
		sizedGroup{sizes: []int{10, 20}},
		nil,
		opaqueGroup{size: 3},
	}}

	require.Equal(t, []int{10, 20, 0, 0, 0}, evt.ProjectorOperationSizes(result))
	require.Equal(t, 5, evt.ProjectorOperationCount(result))
}

func TestPreflightCommit_CountsProjectorOperations(t *testing.T) {
	event := evt.SerializedEvent{
		ID: "a:1", Sequence: 1, Type: "t:created", Version: 1, EntityID: "a", EntityType: "t", Payload: []byte(`{}`),
	}

	// Marker check, one event, and the head update leave 97 actions for projector operations.
	require.NoError(t, evt.PreflightCommit(evt.SerializedResult{
		Events: []evt.SerializedEvent{event}, Transaction: evt.Transaction{opaqueGroup{size: 97}},
	}))

	var tooLarge *evt.CommitTooLargeError
	require.ErrorAs(t, evt.PreflightCommit(evt.SerializedResult{
		Events: []evt.SerializedEvent{event}, Transaction: evt.Transaction{opaqueGroup{size: 98}},
	}), &tooLarge)
	require.Equal(t, 101, tooLarge.Items)

	// A sized group's bytes count too: one operation over the per-item limit is refused.
	require.ErrorAs(t, evt.PreflightCommit(evt.SerializedResult{
		Events: []evt.SerializedEvent{event}, Transaction: evt.Transaction{sizedGroup{sizes: []int{400_000}}},
	}), &tooLarge)
}
