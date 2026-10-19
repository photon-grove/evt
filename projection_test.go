package evt_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
)

func view(projector string, index, operation int, op evt.ViewOperation) evt.ProjectedView {
	return evt.ProjectedView{Projector: projector, ProjectorIndex: index, Operation: operation, Op: op}
}

func put(pk, sk string) evt.ViewOperation {
	return evt.PutView(pk, sk, "owner", "account", []byte(`{}`))
}

func Test_CheckViews_AcceptsValidOperations(t *testing.T) {
	require.NoError(t, evt.CheckViews(nil))
	require.NoError(t, evt.CheckViews([]evt.ProjectedView{
		view("a", 0, 0, put("pk", "one").IfAbsent()),
		view("a", 0, 1, evt.DeleteView("pk", "two").IfPresent()),
		view("b", 1, 0, evt.DeleteView("pk", "three")),
		view("b", 1, 1, evt.PutView("other", "one", "", "", nil)),
	}), "entityID, entityType, and the payload may be empty")
}

func Test_CheckViews_KeyLimitsAreInUTF8Bytes(t *testing.T) {
	require.NoError(t, evt.CheckViews([]evt.ProjectedView{
		view("wide", 0, 0, put(strings.Repeat("p", evt.MaxViewPKBytes), strings.Repeat("s", evt.MaxViewSKBytes))),
	}), "the limits are inclusive")

	var tooLong *evt.ViewKeyTooLongError

	err := evt.CheckViews([]evt.ProjectedView{view("wide", 2, 3, put(strings.Repeat("p", evt.MaxViewPKBytes+1), "sk"))})
	require.ErrorAs(t, err, &tooLong)
	require.Equal(t, "pk", tooLong.Field)
	require.Equal(t, evt.MaxViewPKBytes+1, tooLong.Length)
	require.Equal(t, evt.MaxViewPKBytes, tooLong.Max)
	require.Equal(t, evt.ProjectorOperationRef{Projector: "wide", ProjectorIndex: 2, Operation: 3}, tooLong.ProjectorOperationRef)
	require.ErrorIs(t, err, evt.ErrInvalidArgument)

	err = evt.CheckViews([]evt.ProjectedView{view("wide", 0, 0, put("pk", strings.Repeat("s", evt.MaxViewSKBytes+1)))})
	require.ErrorAs(t, err, &tooLong)
	require.Equal(t, "sk", tooLong.Field)

	// A multi-byte rune counts its bytes: 683 three-byte runes are 2,049 bytes.
	err = evt.CheckViews([]evt.ProjectedView{view("wide", 0, 0, put(strings.Repeat("€", 683), "sk"))})
	require.ErrorAs(t, err, &tooLong)
	require.Equal(t, 2049, tooLong.Length)
}

func Test_CheckViews_RejectsInvalidOperations(t *testing.T) {
	for name, op := range map[string]evt.ViewOperation{
		"empty pk":                put("", "sk"),
		"empty sk":                put("pk", ""),
		"NUL in the pk":           put("p\x00k", "sk"),
		"NUL in the sk":           put("pk", "s\x00k"),
		"invalid UTF-8 in the sk": put("pk", "\xff"),
		"NUL in the payload":      evt.PutView("pk", "sk", "owner", "account", []byte("{\"v\":\"\x00\"}")),
		"NUL in the entity ID":    evt.PutView("pk", "sk", "own\x00er", "account", nil),
		"NUL in the entity type":  evt.PutView("pk", "sk", "owner", "acc\x00ount", nil),
		"unknown kind":            {Kind: "upsert", PK: "pk", SK: "sk"},
		"absent on a delete":      evt.DeleteView("pk", "sk").IfAbsent(),
		"present on a put":        put("pk", "sk").IfPresent(),
		"unknown condition":       {Kind: evt.ViewPut, PK: "pk", SK: "sk", Condition: "maybe"},
	} {
		var invalid *evt.InvalidViewOperationError

		err := evt.CheckViews([]evt.ProjectedView{view("p", 4, 2, op)})
		require.ErrorAs(t, err, &invalid, name)
		require.Equal(t, evt.ProjectorOperationRef{Projector: "p", ProjectorIndex: 4, Operation: 2}, invalid.ProjectorOperationRef, name)
		require.ErrorIs(t, err, evt.ErrInvalidArgument, name)
	}
}

func Test_CheckViews_TextRulesApplyToTheFieldsAnOperationWrites(t *testing.T) {
	// A delete writes only its keys, so the fields it does not write are not held to the text rules.
	op := evt.DeleteView("pk", "sk")
	op.Payload = []byte("\x00")
	op.EntityID = "\xff"

	require.NoError(t, evt.CheckViews([]evt.ProjectedView{view("p", 0, 0, op)}))
}

func Test_CheckViews_ReportsTheFirstFailureInOrder(t *testing.T) {
	// Per operation: invalid, then key length, then duplicate; across operations: the first wins.
	var (
		invalid *evt.InvalidViewOperationError
		tooLong *evt.ViewKeyTooLongError
	)

	err := evt.CheckViews([]evt.ProjectedView{
		view("p", 0, 0, put("pk", "sk")),
		view("p", 0, 1, put(strings.Repeat("p", evt.MaxViewPKBytes+1), "sk")),
		view("p", 0, 2, put("", "sk")),
	})
	require.ErrorAs(t, err, &tooLong, "the oversized key comes first")
	require.Equal(t, 1, tooLong.Operation)

	err = evt.CheckViews([]evt.ProjectedView{
		view("p", 0, 0, put("", "sk")),
		view("p", 0, 1, put(strings.Repeat("p", evt.MaxViewPKBytes+1), "sk")),
	})
	require.ErrorAs(t, err, &invalid, "the invalid operation comes first")
	require.Equal(t, 0, invalid.Operation)
}

func Test_CheckViews_RefusesARepeatedKey(t *testing.T) {
	var duplicate *evt.DuplicateViewKeyError

	err := evt.CheckViews([]evt.ProjectedView{
		view("first", 0, 0, put("pk", "one")),
		view("first", 0, 1, put("pk", "two")),
		view("second", 1, 0, evt.DeleteView("pk", "one")),
	})
	require.ErrorAs(t, err, &duplicate)
	require.Equal(t, "pk", duplicate.PK)
	require.Equal(t, "one", duplicate.SK)
	require.Equal(t, evt.ProjectorOperationRef{Projector: "first", ProjectorIndex: 0, Operation: 0}, duplicate.First)
	require.Equal(t, evt.ProjectorOperationRef{Projector: "second", ProjectorIndex: 1, Operation: 0}, duplicate.Second)

	require.NoError(t, evt.CheckViews([]evt.ProjectedView{
		view("p", 0, 0, put("pk", "one")),
		view("p", 0, 1, put("pk2", "one")),
		view("p", 0, 2, put("pk", "One")),
	}), "keys are compared exactly")
}

func Test_ViewOperationSize_CountsTheAttributesTheBackendStores(t *testing.T) {
	op := evt.PutView("pk", "sk", "id", "type", []byte("abcd"))
	require.Equal(t,
		len("pk")+2+len("sk")+2+len("entityID")+2+len("entityType")+4+len("payload")+4,
		evt.ViewOperationSize(op))
	require.Equal(t, evt.ViewOperationSize(op), evt.ViewOperationSize(op.IfAbsent()), "a put counts its item only")

	deletion := evt.DeleteView("pk", "sk")
	require.Equal(t, len("pk")+2+len("sk")+2, evt.ViewOperationSize(deletion))
	require.Equal(t, evt.ViewOperationSize(deletion)+len("attribute_exists(pk)"), evt.ViewOperationSize(deletion.IfPresent()))
}

func Test_ViewOperation_BuildersDoNotShareState(t *testing.T) {
	base := put("pk", "sk")
	conflict := errors.New("taken")
	mapped := base.IfAbsent().OnConflict(conflict)

	require.Equal(t, evt.ViewUnconditional, base.Condition)
	require.Nil(t, base.Conflict)
	require.Equal(t, evt.ViewAbsent, mapped.Condition)
	require.Same(t, conflict, mapped.Conflict)
}

func event(entity string, sequence evt.EventSequence, commit string) evt.SerializedEvent {
	id := evt.EntityID(entity)

	return evt.SerializedEvent{
		ID: evt.GetEventID(id, sequence), Sequence: sequence, Type: "created", EntityID: id, EntityType: "account",
		Metadata: evt.Metadata{CommitID: commit},
	}
}

func Test_NewCommitView_OrdersStreamsByIDBytesAndEventsBySequence(t *testing.T) {
	commit := evt.NewCommitView([]evt.SerializedEvent{
		event("b", 2, "c1"), event("B", 1, "c1"), event("b", 1, "c1"), event("a", 1, "c1"),
	}, map[evt.EntityID][]byte{"a": []byte(`{"a":1}`), "b": []byte(`{"b":1}`)})

	require.Equal(t, "c1", commit.CommitID)
	require.Equal(t, 4, commit.EventCount())
	require.Len(t, commit.Streams, 3)

	// Bytewise: "B" (0x42) sorts before "a" (0x61) before "b" (0x62).
	require.Equal(t, evt.EntityID("B"), commit.Streams[0].EntityID)
	require.Equal(t, evt.EntityID("a"), commit.Streams[1].EntityID)
	require.Equal(t, evt.EntityID("b"), commit.Streams[2].EntityID)
	require.Equal(t, []byte(`{"a":1}`), commit.Streams[1].State)
	require.Nil(t, commit.Streams[0].State)
	require.Equal(t, evt.EventSequence(1), commit.Streams[2].Events[0].Sequence)
	require.Equal(t, evt.EventSequence(2), commit.Streams[2].Events[1].Sequence)
	require.Equal(t, evt.EntityType("account"), commit.Streams[2].EntityType)
}

func Test_GroupCommits_RegroupsStoredEventsByCommitID(t *testing.T) {
	stored := []evt.SerializedEvent{
		// Listed out of order on purpose: grouping must not depend on how a scan returned them.
		event("b", 2, "c3"),
		event("a", 1, "c1"),
		event("b", 1, "c1"),
		event("a", 2, "c2"),
		event("a", 3, ""), // written before v0.9: no commit ID
		event("a", 4, ""),
	}

	groups := evt.GroupCommits(stored)
	require.Len(t, groups, 5, "two pre-v0.9 events are two commits")

	byID := map[string]evt.CommitView{}
	for _, group := range groups {
		if group.CommitID != "" {
			byID[group.CommitID] = group
		}
	}

	c1 := byID["c1"]
	require.Len(t, c1.Streams, 2)
	require.Equal(t, evt.EntityID("a"), c1.Streams[0].EntityID)
	require.Equal(t, evt.EntityID("b"), c1.Streams[1].EntityID)
	require.Equal(t, 1, byID["c2"].EventCount())
	require.Equal(t, 1, byID["c3"].EventCount())

	// The result does not depend on the order the events arrive in.
	reversed := make([]evt.SerializedEvent, len(stored))
	for i, e := range stored {
		reversed[len(stored)-1-i] = e
	}

	require.Equal(t, groups, evt.GroupCommits(reversed))
	require.Empty(t, evt.GroupCommits(nil))
}

// directory is a commit-wide projector that records how it was called.
type directory struct {
	name  string
	calls int
	err   error
	ops   func(evt.CommitView) []evt.ViewOperation
}

func (d *directory) Name() string { return d.name }

func (d *directory) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	d.calls++

	if d.err != nil {
		return nil, d.err
	}

	return d.ops(commit), nil
}

func Test_ProjectCommit_TagsOperationsWithTheirProvenanceInDeclarationOrder(t *testing.T) {
	first := &directory{name: "first", ops: func(evt.CommitView) []evt.ViewOperation {
		return []evt.ViewOperation{put("pk", "a"), put("pk", "b")}
	}}
	second := &directory{name: "second", ops: func(evt.CommitView) []evt.ViewOperation {
		return []evt.ViewOperation{evt.DeleteView("pk", "c")}
	}}

	projected, err := evt.ProjectCommit(context.Background(),
		[]evt.CommitProjector{first, nil, second}, evt.NewCommitView([]evt.SerializedEvent{event("a", 1, "c")}, nil))
	require.NoError(t, err)
	require.Len(t, projected, 3)

	require.Equal(t, evt.ProjectorOperationRef{Projector: "first", ProjectorIndex: 0, Operation: 0},
		evt.ProjectorOperationRef{Projector: projected[0].Projector, ProjectorIndex: projected[0].ProjectorIndex, Operation: projected[0].Operation})
	require.Equal(t, 1, projected[1].Operation)
	require.Equal(t, "second", projected[2].Projector)
	require.Equal(t, 2, projected[2].ProjectorIndex, "a nil projector keeps its position")
	require.Equal(t, 0, projected[2].Operation)
}

func Test_ProjectCommit_RunsNothingForACommitThatWritesNoEvent(t *testing.T) {
	p := &directory{name: "p", ops: func(evt.CommitView) []evt.ViewOperation { return []evt.ViewOperation{put("pk", "a")} }}

	projected, err := evt.ProjectCommit(context.Background(), []evt.CommitProjector{p}, evt.CommitView{})
	require.NoError(t, err)
	require.Empty(t, projected)
	require.Zero(t, p.calls)

	projected, err = evt.ProjectCommit(context.Background(), nil, evt.NewCommitView([]evt.SerializedEvent{event("a", 1, "c")}, nil))
	require.NoError(t, err)
	require.Empty(t, projected)
}

func Test_ProjectCommit_WrapsAProjectorFailureAndValidatesTheOperations(t *testing.T) {
	cause := errors.New("cannot project")
	commit := evt.NewCommitView([]evt.SerializedEvent{event("a", 1, "c")}, nil)

	var failed *evt.ProjectorError

	_, err := evt.ProjectCommit(context.Background(), []evt.CommitProjector{&directory{name: "broken", err: cause}}, commit)
	require.ErrorAs(t, err, &failed)
	require.Equal(t, "broken", failed.Projector)
	require.ErrorIs(t, err, cause)

	var duplicate *evt.DuplicateViewKeyError

	_, err = evt.ProjectCommit(context.Background(), []evt.CommitProjector{
		&directory{name: "one", ops: func(evt.CommitView) []evt.ViewOperation { return []evt.ViewOperation{put("pk", "a")} }},
		&directory{name: "two", ops: func(evt.CommitView) []evt.ViewOperation { return []evt.ViewOperation{put("pk", "a")} }},
	}, commit)
	require.ErrorAs(t, err, &duplicate)
}

func Test_ProjectorOperationError_WrapsTheMappedErrorOnlyForAFailedCondition(t *testing.T) {
	taken := errors.New("taken")
	cause := errors.New("database says no")
	op := view("claims", 1, 2, put("pk", "sk").IfAbsent().OnConflict(taken))

	failed := evt.NewProjectorOperationError(op, evt.ReasonConditionFailed, cause)
	require.ErrorIs(t, failed, evt.ErrProjectorOperation)
	require.ErrorIs(t, failed, taken)
	require.ErrorIs(t, failed, cause, "the backend's cause stays matchable")
	require.Same(t, taken, failed.Err)
	require.Equal(t, "claims", failed.Projector)
	require.Equal(t, 1, failed.ProjectorIndex)
	require.Equal(t, 2, failed.Operation)
	require.Equal(t, evt.ViewPut, failed.Kind)
	require.Contains(t, failed.Error(), `projector "claims"`)
	require.Contains(t, failed.Error(), "taken")

	// Any other reason is not a failed condition, so the mapped error is not attached.
	other := evt.NewProjectorOperationError(op, "ValidationError", nil)
	require.NotErrorIs(t, other, taken)
	require.Nil(t, other.Err)
	require.Nil(t, other.Cause)
}

func Test_ProjectorOperationCount_CountsViewsAndTheCommitIsRefusedWithoutEvents(t *testing.T) {
	result := evt.SerializedResult{Views: []evt.ProjectedView{view("p", 0, 0, put("pk", "a")), view("p", 0, 1, put("pk", "b"))}}
	require.Equal(t, 2, evt.ProjectorOperationCount(result))

	var refused *evt.ProjectorOnlyCommitError

	require.ErrorAs(t, evt.CheckProjectorOperations(result), &refused)
	require.Equal(t, 2, refused.Operations)

	_, err := evt.NormalizeCommit(result)
	require.ErrorAs(t, err, &refused)

	result.Events = []evt.SerializedEvent{event("a", 1, "c")}
	require.NoError(t, evt.CheckProjectorOperations(result))
	require.NoError(t, evt.CheckProjectorOperationsSupported("backend", result), "views are not transaction groups")

	normalized, err := evt.NormalizeCommit(result)
	require.NoError(t, err)
	require.Len(t, normalized.Views, 2)

	// NormalizeCommit validates the views before any write.
	result.Views = append(result.Views, view("p", 0, 2, put("pk", "a")))
	_, err = evt.NormalizeCommit(result)

	var duplicate *evt.DuplicateViewKeyError

	require.ErrorAs(t, err, &duplicate)
}

func Test_PreflightCommit_BudgetsViewOperations(t *testing.T) {
	result := evt.SerializedResult{Events: []evt.SerializedEvent{event("a", 1, "c")}}

	// The marker check, the event put, and the head update leave 97 of the 100 actions.
	for i := range 97 {
		result.Views = append(result.Views, view("p", 0, i, put("pk", string(rune('A'+i/26))+string(rune('a'+i%26)))))
	}

	require.NoError(t, evt.PreflightCommit(result))

	result.Views = append(result.Views, view("p", 0, 97, put("pk", "last")))

	var tooLarge *evt.CommitTooLargeError

	require.ErrorAs(t, evt.PreflightCommit(result), &tooLarge)
	require.Equal(t, 101, tooLarge.Items)
}

func Test_NewCommitView_GivesEventsTheMetadataAStoredEventHas(t *testing.T) {
	command := evt.CommandID("cmd-1")
	live := event("a", 1, "c1")
	live.Metadata.CommandID = &command

	require.Nil(t, live.Metadata.CausationID)
	require.Nil(t, live.Metadata.Extensions)

	commit := evt.NewCommitView([]evt.SerializedEvent{live}, nil)
	got := commit.Streams[0].Events[0].Metadata

	require.NotNil(t, got.CausationID)
	require.Equal(t, command, *got.CausationID, "the effective causation is the command ID")
	require.NotNil(t, got.Extensions, "extensions are an empty map, as stored")
	require.Nil(t, live.Metadata.CausationID, "the caller's event is not changed")
}
