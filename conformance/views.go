package conformance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/test"
)

// ViewReader reads back one view row from the repository under test: the row at pk and sk, or nil
// when there is none. The suite uses it to prove what a commit's view operations wrote.
type ViewReader func(ctx context.Context, repo evt.Repository, pk, sk string) (*evt.SerializedView, error)

// errClaimTaken is the mapped error the cases attach to a claim's failed condition.
var errClaimTaken = errors.New("claim already taken")

// runViewCases runs the commit-wide view cases (ADR 0004, C7) against a backend that writes view
// operations in the commit transaction: arbitrary pk/sk rows, conditions, the typed per-operation
// error and where it ranks, the refusals before any write, and the budget.
func runViewCases(t *testing.T, newRepo func() evt.Repository, opts SuiteOptions) {
	t.Helper()
	require.NotNil(t, opts.ReadView, "a backend claiming views must supply SuiteOptions.ReadView")

	testBudget := func(t *testing.T, repo evt.Repository, read ViewReader) {
		t.Helper()
		testViewsBudget(t, repo, read, !opts.ViewsSizedBySQL)
	}

	cases := []struct {
		name string
		run  func(*testing.T, evt.Repository, ViewReader)
	}{
		{"WritesViewsAtAnyKeyWithTheEvents", testViewsWriteAtAnyKey},
		{"OverwritesAndDeletes", testViewsOverwriteAndDelete},
		{"AFailedConditionIsATypedErrorAndWritesNothing", testViewsFailedAbsentCondition},
		{"AFailedPresentDeleteIsATypedError", testViewsFailedPresentDelete},
		{"ARowIsOnlyWrittenByItsOwner", testViewsOwnerIsEnforced},
		{"RefusesProjectorOnlyViews", testViewsProjectorOnly},
		{"RefusesInvalidViewsBeforeAnyWrite", testViewsRefusedBeforeAnyWrite},
		{"CountsViewsInTheBudget", testBudget},
		{"AConflictOutranksAFailedView", testViewsConflictOutranks},
		{"ASequenceGapOutranksAFailedView", testViewsGapOutranks},
		{"AMovedFenceOutranksAFailedView", testViewsFenceOutranks},
	}

	for _, c := range cases {
		t.Run("Views/"+c.name, func(t *testing.T) {
			c.run(t, newRepo(), opts.ReadView)
		})
	}

	if opts.SupportsOutcomes {
		t.Run("Views/ALostOutcomeRowOutranksAFailedView", func(t *testing.T) {
			testViewsOutcomeOutranks(t, newRepo(), opts.ReadView)
		})

		runProjectorCases(t, newRepo, opts)
	}
}

// projected builds the provenance of one operation of a projector.
func projected(name string, index, operation int, op evt.ViewOperation) evt.ProjectedView {
	return evt.ProjectedView{Projector: name, ProjectorIndex: index, Operation: operation, Op: op}
}

func put(pk, sk, payload string) evt.ViewOperation {
	return evt.PutView(pk, sk, "owner", "test", []byte(payload))
}

func requireRow(t *testing.T, read ViewReader, repo evt.Repository, pk, sk, payload string) {
	t.Helper()

	view, err := read(context.Background(), repo, pk, sk)
	require.NoError(t, err)
	require.NotNil(t, view, "the row %s/%s should exist", pk, sk)
	require.Equal(t, payload, string(view.Payload))
	require.Equal(t, evt.EntityID("owner"), view.EntityID)
	require.Equal(t, evt.EntityType("test"), view.EntityType)
}

func requireNoRow(t *testing.T, read ViewReader, repo evt.Repository, pk, sk string) {
	t.Helper()

	view, err := read(context.Background(), repo, pk, sk)
	require.NoError(t, err)
	require.Nil(t, view, "the row %s/%s should not exist", pk, sk)
}

func requireNoEvents(t *testing.T, repo evt.Repository, id evt.EntityID) {
	t.Helper()

	stored, err := repo.GetEvents(context.Background(), id)
	require.NoError(t, err)
	require.Empty(t, stored, "a refused commit writes no events")
}

func testViewsWriteAtAnyKey(t *testing.T, repo evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	idA, idB := evt.EntityID(ns+"-a"), evt.EntityID(ns+"-b")

	// One commit writes two streams and rows in partitions that name no entity, from two projectors.
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: append(
			serialize(t, idA, "test", 0, &test.EntityCreated{ID: idA, Value: "a"}),
			serialize(t, idB, "test", 0, &test.EntityCreated{ID: idB, Value: "b"})...,
		),
		Views: []evt.ProjectedView{
			projected("directory", 0, 0, put(ns+"#directory", "a", `{"stream":"a"}`)),
			projected("directory", 0, 1, put(ns+"#directory", "b", `{"stream":"b"}`)),
			projected("audit", 1, 0, put(ns+"#audit", "commit", `{"streams":2}`)),
		},
	}))

	requireRow(t, read, repo, ns+"#directory", "a", `{"stream":"a"}`)
	requireRow(t, read, repo, ns+"#directory", "b", `{"stream":"b"}`)
	requireRow(t, read, repo, ns+"#audit", "commit", `{"streams":2}`)

	for _, id := range []evt.EntityID{idA, idB} {
		stored, err := repo.GetEvents(ctx, id)
		require.NoError(t, err)
		require.Len(t, stored, 1)
	}
}

func testViewsOverwriteAndDelete(t *testing.T, repo evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	id := evt.EntityID(ns + "-a")
	pk := ns + "#rows"

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, id, "test", 0, &test.EntityCreated{ID: id, Value: "one"}),
		Views: []evt.ProjectedView{
			projected("rows", 0, 0, put(pk, "keep", `{"v":1}`)),
			projected("rows", 0, 1, put(pk, "drop", `{"v":1}`)),
		},
	}))

	// A put overwrites, a conditioned delete of a present row removes it, and an unconditional
	// delete of an absent row succeeds.
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, id, "test", 1, &test.EntityUpdated{ID: id, Value: "two"}),
		Views: []evt.ProjectedView{
			projected("rows", 0, 0, put(pk, "keep", `{"v":2}`)),
			projected("rows", 0, 1, evt.DeleteView(pk, "drop", "owner").IfPresent()),
			projected("rows", 0, 2, evt.DeleteView(pk, "never-existed", "owner")),
		},
	}))

	requireRow(t, read, repo, pk, "keep", `{"v":2}`)
	requireNoRow(t, read, repo, pk, "drop")
	requireNoRow(t, read, repo, pk, "never-existed")
}

func testViewsFailedAbsentCondition(t *testing.T, repo evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	owner, intruder := evt.EntityID(ns+"-owner"), evt.EntityID(ns+"-intruder")
	claims := ns + "#claims"

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, owner, "test", 0, &test.EntityCreated{ID: owner, Value: "owner"}),
		Views:  []evt.ProjectedView{projected("claims", 0, 0, put(claims, "ada", `{"owner":"first"}`).IfAbsent())},
	}))

	// The second commit claims the same name from another stream. Its second projector's second
	// operation fails, so the typed error names that projector and that operation, carries the
	// operation's mapped error, and nothing from the commit is written: not its events, and not
	// the row its first projector put before the failed operation.
	err := repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, intruder, "test", 0, &test.EntityCreated{ID: intruder, Value: "intruder"}),
		Views: []evt.ProjectedView{
			projected("profile", 0, 0, put(ns+"#profiles", "intruder", `{"name":"ada"}`)),
			projected("claims", 1, 0, put(claims, "ada-note", `{"note":true}`)),
			projected("claims", 1, 1, put(claims, "ada", `{"owner":"second"}`).IfAbsent().OnConflict(errClaimTaken)),
		},
	})
	require.ErrorIs(t, err, evt.ErrProjectorOperation)
	require.ErrorIs(t, err, errClaimTaken)

	var failed *evt.ProjectorOperationError
	require.ErrorAs(t, err, &failed)
	require.Equal(t, "claims", failed.Projector)
	require.Equal(t, 1, failed.ProjectorIndex)
	require.Equal(t, 1, failed.Operation)
	require.Equal(t, evt.ViewPut, failed.Kind)
	require.Equal(t, claims, failed.PK)
	require.Equal(t, "ada", failed.SK)
	require.Equal(t, evt.ReasonConditionFailed, failed.Reason)
	require.False(t, evt.IsOptimisticConflict(err), "a failed projector condition is not an optimistic-concurrency conflict")

	requireNoEvents(t, repo, intruder)
	requireNoRow(t, read, repo, ns+"#profiles", "intruder")
	requireNoRow(t, read, repo, claims, "ada-note")
	requireRow(t, read, repo, claims, "ada", `{"owner":"first"}`)
}

func testViewsFailedPresentDelete(t *testing.T, repo evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	id := evt.EntityID(ns + "-a")

	err := repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, id, "test", 0, &test.EntityCreated{ID: id, Value: "one"}),
		Views: []evt.ProjectedView{
			projected("release", 0, 0, put(ns+"#rows", "kept", `{}`)),
			projected("release", 0, 1, evt.DeleteView(ns+"#rows", "absent", "owner").IfPresent()),
		},
	})

	var failed *evt.ProjectorOperationError
	require.ErrorAs(t, err, &failed)
	require.Equal(t, "release", failed.Projector)
	require.Equal(t, 1, failed.Operation)
	require.Equal(t, evt.ViewDelete, failed.Kind)
	require.Equal(t, evt.ReasonConditionFailed, failed.Reason)
	require.NoError(t, failed.Err, "an operation with no mapped error maps none")

	requireNoEvents(t, repo, id)
	requireNoRow(t, read, repo, ns+"#rows", "kept")
}

// testViewsOwnerIsEnforced covers the rule that a row's owner never changes: a write by any other
// stream fails its condition, a put or a delete, with or without a presence condition, and writes
// nothing; the owner itself still overwrites and deletes.
func testViewsOwnerIsEnforced(t *testing.T, repo evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	pk := ns + "#rows"
	first, second := evt.EntityID(ns+"-first"), evt.EntityID(ns+"-second")

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, first, "test", 0, &test.EntityCreated{ID: first, Value: "one"}),
		Views:  []evt.ProjectedView{projected("rows", 0, 0, put(pk, "row", `{"v":1}`))},
	}))

	foreign := map[string]evt.ViewOperation{
		"put":                  evt.PutView(pk, "row", "intruder", "test", []byte(`{"v":2}`)),
		"delete":               evt.DeleteView(pk, "row", "intruder"),
		"delete with presence": evt.DeleteView(pk, "row", "intruder").IfPresent(),
	}

	for name, op := range foreign {
		err := repo.Commit(ctx, evt.SerializedResult{
			Events: serialize(t, second, "test", 0, &test.EntityCreated{ID: second, Value: "two"}),
			Views:  []evt.ProjectedView{projected("rows", 0, 0, op.OnConflict(errClaimTaken))},
		})

		var failed *evt.ProjectorOperationError

		require.ErrorAs(t, err, &failed, name)
		require.ErrorIs(t, err, errClaimTaken, name)
		require.Equal(t, evt.ReasonConditionFailed, failed.Reason, name)

		requireNoEvents(t, repo, second)
		requireRow(t, read, repo, pk, "row", `{"v":1}`)
	}

	// The owner overwrites its row and deletes it; the delete of an absent row still succeeds.
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, first, "test", 1, &test.EntityUpdated{ID: first, Value: "two"}),
		Views:  []evt.ProjectedView{projected("rows", 0, 0, put(pk, "row", `{"v":3}`))},
	}))
	requireRow(t, read, repo, pk, "row", `{"v":3}`)

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, first, "test", 2, &test.EntityUpdated{ID: first, Value: "three"}),
		Views: []evt.ProjectedView{
			projected("rows", 0, 0, evt.DeleteView(pk, "row", "owner").IfPresent()),
			projected("rows", 0, 1, evt.DeleteView(pk, "gone", "intruder")),
		},
	}))
	requireNoRow(t, read, repo, pk, "row")
}

func testViewsProjectorOnly(t *testing.T, repo evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	views := []evt.ProjectedView{
		projected("rows", 0, 0, put(ns+"#rows", "a", `{}`)),
		projected("rows", 0, 1, put(ns+"#rows", "b", `{}`)),
	}

	// Views are projections of committed events: operations with no event behind them are refused,
	// with or without a fence.
	for name, result := range map[string]evt.SerializedResult{
		"views only": {Views: views},
		"views with a fence": {
			Views:  views,
			Fences: []evt.ReadFence{{EntityID: evt.EntityID(ns + "-fenced"), EntityType: "test"}},
		},
	} {
		err := repo.Commit(ctx, result)

		var refused *evt.ProjectorOnlyCommitError
		require.ErrorAs(t, err, &refused, name)
		require.Equal(t, 2, refused.Operations, name)
	}

	requireNoRow(t, read, repo, ns+"#rows", "a")
}

func testViewsRefusedBeforeAnyWrite(t *testing.T, repo evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	id := evt.EntityID(ns + "-a")
	events := func() []evt.SerializedEvent {
		return serialize(t, id, "test", 0, &test.EntityCreated{ID: id, Value: "one"})
	}

	// The exact limits pass: a 2,048-byte pk and a 1,024-byte sk.
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: events(),
		Views:  []evt.ProjectedView{projected("wide", 0, 0, put(strings.Repeat("p", evt.MaxViewPKBytes), strings.Repeat("s", evt.MaxViewSKBytes), `{}`))},
	}))
	requireRow(t, read, repo, strings.Repeat("p", evt.MaxViewPKBytes), strings.Repeat("s", evt.MaxViewSKBytes), `{}`)

	other := evt.EntityID(ns + "-other")
	otherEvents := func() []evt.SerializedEvent {
		return serialize(t, other, "test", 0, &test.EntityCreated{ID: other, Value: "one"})
	}

	var tooLong *evt.ViewKeyTooLongError

	err := repo.Commit(ctx, evt.SerializedResult{
		Events: otherEvents(),
		Views:  []evt.ProjectedView{projected("wide", 0, 0, put(strings.Repeat("p", evt.MaxViewPKBytes+1), "sk", `{}`))},
	})
	require.ErrorAs(t, err, &tooLong)
	require.Equal(t, "pk", tooLong.Field)
	require.Equal(t, evt.MaxViewPKBytes+1, tooLong.Length)

	err = repo.Commit(ctx, evt.SerializedResult{
		Events: otherEvents(),
		Views:  []evt.ProjectedView{projected("wide", 0, 0, put(ns+"#rows", strings.Repeat("s", evt.MaxViewSKBytes+1), `{}`))},
	})
	require.ErrorAs(t, err, &tooLong)
	require.Equal(t, "sk", tooLong.Field)

	var duplicate *evt.DuplicateViewKeyError

	err = repo.Commit(ctx, evt.SerializedResult{
		Events: otherEvents(),
		Views: []evt.ProjectedView{
			projected("first", 0, 0, put(ns+"#rows", "twice", `{}`)),
			projected("second", 1, 0, evt.DeleteView(ns+"#rows", "twice", "owner")),
		},
	})
	require.ErrorAs(t, err, &duplicate)
	require.Equal(t, "first", duplicate.First.Projector)
	require.Equal(t, "second", duplicate.Second.Projector)

	var invalid *evt.InvalidViewOperationError

	err = repo.Commit(ctx, evt.SerializedResult{
		Events: otherEvents(),
		Views:  []evt.ProjectedView{projected("empty", 0, 0, put("", "sk", `{}`))},
	})
	require.ErrorAs(t, err, &invalid)

	err = repo.Commit(ctx, evt.SerializedResult{
		Events: otherEvents(),
		Views:  []evt.ProjectedView{projected("nul", 0, 0, put(ns+"#rows", "sk", "{\"v\":\"\x00\"}"))},
	})
	require.ErrorAs(t, err, &invalid)

	requireNoEvents(t, repo, other)
}

func testViewsBudget(t *testing.T, repo evt.Repository, read ViewReader, bytes bool) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	views := func(count int) []evt.ProjectedView {
		out := make([]evt.ProjectedView, 0, count)
		for i := range count {
			out = append(out, projected("bulk", 0, i, put(ns+"#bulk", fmt.Sprintf("row-%03d", i), `{}`)))
		}

		return out
	}

	// The two control checks, one event put, and the stream's head update leave 96 actions for
	// view operations: 96 fit exactly and 97 do not, on every backend.
	fits, over := evt.EntityID(ns+"-fits"), evt.EntityID(ns+"-over")

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, fits, "test", 0, &test.EntityCreated{ID: fits, Value: "fits"}),
		Views:  views(96),
	}))
	requireRow(t, read, repo, ns+"#bulk", "row-095", `{}`)

	err := repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, over, "test", 0, &test.EntityCreated{ID: over, Value: "over"}),
		Views:  views(97),
	})

	var tooLarge *evt.CommitTooLargeError
	require.ErrorAs(t, err, &tooLarge)
	require.Equal(t, 101, tooLarge.Items)
	requireNoEvents(t, repo, over)

	// The byte budgets apply too: one item over 380,000 bytes, and an aggregate over 3,800,000
	// bytes of individually valid items. A backend that sizes an operation by its SQL text cannot
	// see the bound arguments, so its byte estimate is a lower bound and these cases do not apply.
	if !bytes {
		return
	}

	big := evt.EntityID(ns + "-big")
	huge := func(size int) string { return strings.Repeat("x", size) }

	err = repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, big, "test", 0, &test.EntityCreated{ID: big, Value: "big"}),
		Views:  []evt.ProjectedView{projected("big", 0, 0, put(ns+"#big", "one", huge(381_000)))},
	})
	require.ErrorAs(t, err, &tooLarge)

	many := make([]evt.ProjectedView, 0, 12)
	for i := range 12 {
		many = append(many, projected("big", 0, i, put(ns+"#big", fmt.Sprintf("part-%02d", i), huge(340_000))))
	}

	err = repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, big, "test", 0, &test.EntityCreated{ID: big, Value: "big"}),
		Views:  many,
	})
	require.ErrorAs(t, err, &tooLarge)
	requireNoEvents(t, repo, big)
	requireNoRow(t, read, repo, ns+"#big", "part-00")
}

func testViewsConflictOutranks(t *testing.T, repo evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	id := evt.EntityID(ns + "-a")
	pk := ns + "#rows"

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, id, "test", 0, &test.EntityCreated{ID: id, Value: "one"}),
		Views:  []evt.ProjectedView{projected("rows", 0, 0, put(pk, "taken", `{}`))},
	}))

	// A writer that loaded before the first commit repeats sequence 1 and also trips its own
	// projector's condition. The stream's conflict outranks the projector operation, so the
	// caller sees a conflict it can re-prepare, never a projector failure.
	err := repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, id, "test", 0, &test.EntityCreated{ID: id, Value: "stale"}),
		Views:  []evt.ProjectedView{projected("rows", 0, 0, put(pk, "taken", `{}`).IfAbsent())},
	})
	require.Error(t, err)
	require.True(t, evt.IsOptimisticConflict(err), "got %v", err)
	require.NotErrorIs(t, err, evt.ErrProjectorOperation)
	requireRow(t, read, repo, pk, "taken", `{}`)
}

func testViewsGapOutranks(t *testing.T, repo evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	idA, idB := evt.EntityID(ns+"-a"), evt.EntityID(ns+"-b")
	pk := ns + "#rows"

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, idB, "test", 0, &test.EntityCreated{ID: idB, Value: "b"}),
		Views:  []evt.ProjectedView{projected("rows", 0, 0, put(pk, "taken", `{}`))},
	}))

	// Stream a starts above its head: a gap, which ranks above any projector operation.
	err := repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, idA, "test", 2, &test.EntityCreated{ID: idA, Value: "a"}),
		Views:  []evt.ProjectedView{projected("rows", 0, 0, put(pk, "taken", `{}`).IfAbsent())},
	})
	require.ErrorIs(t, err, evt.ErrSequenceGap)
	require.NotErrorIs(t, err, evt.ErrProjectorOperation)
	requireRow(t, read, repo, pk, "taken", `{}`)
}

func testViewsFenceOutranks(t *testing.T, repo evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	watched, writer := evt.EntityID(ns+"-watched"), evt.EntityID(ns+"-writer")
	pk := ns + "#rows"

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, watched, "test", 0, &test.EntityCreated{ID: watched, Value: "w"}),
		Views:  []evt.ProjectedView{projected("rows", 0, 0, put(pk, "taken", `{}`))},
	}))

	// The fence was taken before the first commit (head 0), so it has moved; the commit's claim
	// fails too. The fence is reported, so the caller sees a conflict it can re-prepare.
	err := repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, writer, "test", 0, &test.EntityCreated{ID: writer, Value: "x"}),
		Fences: []evt.ReadFence{{EntityID: watched, EntityType: "test", Head: 0}},
		Views:  []evt.ProjectedView{projected("rows", 0, 0, put(pk, "taken", `{}`).IfAbsent())},
	})

	var fence *evt.FenceConflictError

	require.ErrorAs(t, err, &fence)
	require.Equal(t, watched, fence.Fence.EntityID)
	require.NotErrorIs(t, err, evt.ErrProjectorOperation)

	// The same fence at the current head passes, and then the claim is what fails.
	err = repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, writer, "test", 0, &test.EntityCreated{ID: writer, Value: "x"}),
		Fences: []evt.ReadFence{{EntityID: watched, EntityType: "test", Head: 1}},
		Views:  []evt.ProjectedView{projected("rows", 0, 0, put(pk, "taken", `{}`).IfAbsent())},
	})
	require.ErrorIs(t, err, evt.ErrProjectorOperation)
	requireNoEvents(t, repo, writer)
	requireRow(t, read, repo, pk, "taken", `{}`)
}

func testViewsOutcomeOutranks(t *testing.T, repo evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	idA, idB := evt.EntityID(ns+"-a"), evt.EntityID(ns+"-b")
	pk := ns + "#rows"
	outcome := func() *evt.Outcome {
		return &evt.Outcome{CommandID: evt.CommandID(ns + "-cmd"), Status: evt.OutcomeAccepted, Digest: "d"}
	}

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events:  serialize(t, idA, "test", 0, &test.EntityCreated{ID: idA, Value: "a"}),
		Outcome: outcome(),
		Views:   []evt.ProjectedView{projected("rows", 0, 0, put(pk, "taken", `{}`))},
	}))

	// Another command ID already has its row, and this commit's claim fails as well: the row is
	// where the answer lives, so the outcome conflict is what the caller sees.
	err := repo.Commit(ctx, evt.SerializedResult{
		Events:  serialize(t, idB, "test", 0, &test.EntityCreated{ID: idB, Value: "b"}),
		Outcome: outcome(),
		Views:   []evt.ProjectedView{projected("rows", 0, 0, put(pk, "taken", `{}`).IfAbsent())},
	})
	require.ErrorIs(t, err, evt.ErrOutcomeConflict)
	require.NotErrorIs(t, err, evt.ErrProjectorOperation)
	requireNoEvents(t, repo, idB)
	requireRow(t, read, repo, pk, "taken", `{}`)
}
