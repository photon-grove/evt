package dynamo_test

// These tests send commit-wide view operations (ADR 0004, C7) through the real DynamoDB repository
// against the in-memory fake table: the layout of the transaction, the refusals before any write, and
// where a failed projector condition ranks among the other ways a commit can fail. The live suite
// proves the same on an emulator.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/dynamo"
	"github.com/photon-grove/evt/go/internal/compat"
)

func viewEvent(id evt.EntityID, sequence evt.EventSequence) evt.SerializedEvent {
	event := outcomeEvent(id, sequence)
	event.Metadata.CommitID = "commit-1"

	return event
}

func projected(name string, index, operation int, op evt.ViewOperation) evt.ProjectedView {
	return evt.ProjectedView{Projector: name, ProjectorIndex: index, Operation: operation, Op: op}
}

func newViewsRepo() (*compat.FakeDynamo, *dynamo.Repository) {
	fake := compat.NewFakeDynamo()

	return fake, dynamo.NewRepository(fake, "events").WithLazyInitialization(true).WithViewsTable("views")
}

func TestViewOperationsSitBetweenTheControlChecksAndTheWrittenStreams(t *testing.T) {
	fake, repo := newViewsRepo()

	require.NoError(t, repo.Commit(context.Background(), evt.SerializedResult{
		Events: []evt.SerializedEvent{viewEvent("b", 1), viewEvent("a", 1)},
		Views: []evt.ProjectedView{
			projected("directory", 0, 0, evt.PutView("directory", "a", "a", "t", []byte(`{}`))),
			projected("directory", 0, 1, evt.DeleteView("directory", "old", "o")),
			projected("audit", 1, 0, evt.PutView("audit", "commit-1", "o", "", []byte(`{}`)).IfAbsent()),
		},
	}))

	calls := fake.Transactions()
	require.Len(t, calls, 1)

	layout := make([]string, 0, len(calls[0]))
	for _, action := range calls[0] {
		table := "events"
		key := fmt.Sprintf("%d", action.SK)

		if action.Table == "views" {
			table, key = "views", action.SKText
		}

		layout = append(layout, fmt.Sprintf("%s %s %s/%s", action.Operation, table, action.PK, key))
	}

	require.Equal(t, []string{
		"ConditionCheck events SYSTEM#FEATURES/0",
		"ConditionCheck events SYSTEM#REBUILD/0",
		"Put views directory/a",
		"Delete views directory/old",
		"Put views audit/commit-1",
		"Update events a/-1",
		"Put events a/1",
		"Update events b/-1",
		"Put events b/1",
	}, layout)

	require.Equal(t, "attribute_not_exists(pk)", calls[0][4].Condition)
	require.Len(t, fake.Views("directory"), 1)
	require.Len(t, fake.Views("audit"), 1)
}

func TestViewOperationsWithoutAViewsTableAreRefusedBeforeAnyRequest(t *testing.T) {
	fake := compat.NewFakeDynamo()
	repo := dynamo.NewRepository(fake, "events").WithLazyInitialization(true)

	err := repo.Commit(context.Background(), evt.SerializedResult{
		Events: []evt.SerializedEvent{viewEvent("a", 1)},
		Views:  []evt.ProjectedView{projected("p", 0, 0, evt.PutView("pk", "sk", "o", "", nil))},
	})

	var refused *evt.ViewsNotConfiguredError

	require.ErrorAs(t, err, &refused)
	require.Equal(t, 1, refused.Operations)
	require.Empty(t, fake.Transactions(), "nothing was sent")

	// A commit with no view operations needs no views table.
	require.NoError(t, repo.Commit(context.Background(), evt.SerializedResult{Events: []evt.SerializedEvent{viewEvent("a", 1)}}))
}

func TestAFailedViewConditionRollsTheWholeCommitBackAndNamesTheOperation(t *testing.T) {
	fake, repo := newViewsRepo()
	ctx := context.Background()

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: []evt.SerializedEvent{viewEvent("a", 1)},
		Views:  []evt.ProjectedView{projected("claims", 0, 0, evt.PutView("claims", "ada", "a", "t", []byte(`{"owner":"a"}`)))},
	}))

	taken := errors.New("handle taken")
	err := repo.Commit(ctx, evt.SerializedResult{
		Events: []evt.SerializedEvent{viewEvent("b", 1)},
		Views: []evt.ProjectedView{
			projected("profile", 0, 0, evt.PutView("profiles", "b", "b", "t", []byte(`{}`))),
			projected("claims", 1, 0, evt.PutView("claims", "ada", "b", "t", []byte(`{"owner":"b"}`)).IfAbsent().OnConflict(taken)),
		},
	})

	var failed *evt.ProjectorOperationError

	require.ErrorAs(t, err, &failed)
	require.ErrorIs(t, err, taken)
	require.Equal(t, "claims", failed.Projector)
	require.Equal(t, 1, failed.ProjectorIndex)
	require.Equal(t, 0, failed.Operation)
	require.Equal(t, "ConditionalCheckFailed", failed.Reason)
	require.False(t, evt.IsOptimisticConflict(err))

	events, err := repo.GetEvents(ctx, "b")
	require.NoError(t, err)
	require.Empty(t, events)
	require.Empty(t, fake.Views("profiles"), "the first projector's row was rolled back with the rest")
	require.Len(t, fake.Views("claims"), 1)
}

func TestAStreamConflictOutranksAFailedViewCondition(t *testing.T) {
	_, repo := newViewsRepo()
	ctx := context.Background()

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: []evt.SerializedEvent{viewEvent("a", 1)},
		Views:  []evt.ProjectedView{projected("claims", 0, 0, evt.PutView("claims", "ada", "a", "t", nil))},
	}))

	// The commit repeats sequence 1 of stream a, so its head update fails, and its claim fails too.
	// The view item is first in the transaction, but the engine ranks the stream's conflict above
	// any projector condition, so the caller sees a conflict it can re-prepare.
	err := repo.Commit(ctx, evt.SerializedResult{
		Events: []evt.SerializedEvent{viewEvent("a", 1)},
		Views:  []evt.ProjectedView{projected("claims", 0, 0, evt.PutView("claims", "ada", "a", "t", nil).IfAbsent())},
	})

	require.Error(t, err)
	require.True(t, evt.IsOptimisticConflict(err), "got %v", err)
	require.NotErrorIs(t, err, evt.ErrProjectorOperation)
}

func TestAStreamHeadGapOutranksAFailedViewCondition(t *testing.T) {
	_, repo := newViewsRepo()
	ctx := context.Background()

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: []evt.SerializedEvent{viewEvent("b", 1)},
		Views:  []evt.ProjectedView{projected("claims", 0, 0, evt.PutView("claims", "ada", "b", "t", nil))},
	}))

	// Stream a starts above its head: a sequence gap, which ranks above every conflict and projector.
	err := repo.Commit(ctx, evt.SerializedResult{
		Events: []evt.SerializedEvent{viewEvent("a", 3)},
		Views:  []evt.ProjectedView{projected("claims", 0, 0, evt.PutView("claims", "ada", "a", "t", nil).IfAbsent())},
	})

	require.ErrorIs(t, err, evt.ErrSequenceGap)
	require.NotErrorIs(t, err, evt.ErrProjectorOperation)
}

func TestALostOutcomeRowOutranksAFailedViewCondition(t *testing.T) {
	_, repo := newViewsRepo()
	ctx := context.Background()

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events:  []evt.SerializedEvent{viewEvent("a", 1)},
		Outcome: accepted("cmd-1"),
		Views:   []evt.ProjectedView{projected("claims", 0, 0, evt.PutView("claims", "ada", "a", "t", nil))},
	}))

	// Another command ID already has its row, and this commit's claim fails too: the row is where
	// the answer lives, so the engine reports the outcome conflict, never the projector failure.
	err := repo.Commit(ctx, evt.SerializedResult{
		Events:  []evt.SerializedEvent{viewEvent("b", 1)},
		Outcome: accepted("cmd-1"),
		Views:   []evt.ProjectedView{projected("claims", 0, 0, evt.PutView("claims", "ada", "b", "t", nil).IfAbsent())},
	})

	var conflict *evt.OutcomeConflictError

	require.ErrorAs(t, err, &conflict)
	require.NotErrorIs(t, err, evt.ErrProjectorOperation)
}

func TestViewOperationsCountInTheBudget(t *testing.T) {
	fake, repo := newViewsRepo()
	ctx := context.Background()

	views := func(count int) []evt.ProjectedView {
		out := make([]evt.ProjectedView, 0, count)
		for i := range count {
			out = append(out, projected("bulk", 0, i, evt.PutView("bulk", fmt.Sprintf("row-%03d", i), "o", "", nil)))
		}

		return out
	}

	// The two control checks, one event put, and one head update leave 96 actions for view operations.
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: []evt.SerializedEvent{viewEvent("a", 1)}, Views: views(96)}))
	require.Len(t, fake.Transactions()[0], 100)

	before := len(fake.Transactions())
	err := repo.Commit(ctx, evt.SerializedResult{Events: []evt.SerializedEvent{viewEvent("b", 1)}, Views: views(97)})

	var tooLarge *evt.CommitTooLargeError

	require.ErrorAs(t, err, &tooLarge)
	require.Equal(t, 101, tooLarge.Items)
	require.Len(t, fake.Transactions(), before, "an oversized commit sends no request")

	// The byte budget covers the views' payloads: 11 items of 350,000 bytes pass the 380,000-byte
	// item limit but not the 3,800,000-byte request limit.
	big := make([]evt.ProjectedView, 0, 11)
	for i := range 11 {
		big = append(big, projected("big", 0, i, evt.PutView("big", fmt.Sprintf("part-%02d", i), "o", "", bytes.Repeat([]byte("x"), 350_000))))
	}

	err = repo.Commit(ctx, evt.SerializedResult{Events: []evt.SerializedEvent{viewEvent("c", 1)}, Views: big})
	require.ErrorAs(t, err, &tooLarge)
	require.Len(t, fake.Transactions(), before)
}

func TestViewKeysAreCheckedBeforeAnyRequest(t *testing.T) {
	fake, repo := newViewsRepo()

	long := bytes.Repeat([]byte("p"), evt.MaxViewPKBytes+1)

	for name, views := range map[string][]evt.ProjectedView{
		"a key over the limit": {projected("p", 0, 0, evt.PutView(string(long), "sk", "o", "", nil))},
		"a repeated key": {
			projected("p", 0, 0, evt.PutView("pk", "sk", "o", "", nil)),
			projected("q", 1, 0, evt.DeleteView("pk", "sk", "o")),
		},
		"an empty key": {projected("p", 0, 0, evt.PutView("pk", "", "o", "", nil))},
	} {
		err := repo.Commit(context.Background(), evt.SerializedResult{Events: []evt.SerializedEvent{viewEvent("a", 1)}, Views: views})
		require.ErrorIs(t, err, evt.ErrInvalidArgument, name)
		require.Empty(t, fake.Transactions(), name)
	}
}
