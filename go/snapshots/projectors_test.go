package snapshots_test

// These tests exercise the commit-wide projectors of snapshots.Store (ADR 0004, C7) over the
// in-memory repository: what the projectors see, when they run, that every preparation of a command
// derives its operations again, and that replaying the stored events through a projector reproduces
// the operations the live commit wrote. The per-backend behavior of the operations (layout, budget,
// conditions, the typed error) is covered by the shared conformance suite and the repositories' own
// tests.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/mem"
	"github.com/photon-grove/evt/go/snapshots"
	"github.com/photon-grove/evt/go/test"
)

const counterType evt.EntityType = "counter"

// counter is a small aggregate with pure state: nothing in its snapshot depends on the clock, so the
// state a stream has after a commit can be rebuilt from its events.
type counter struct {
	id    evt.EntityID
	Total int      `json:"total"`
	Notes []string `json:"notes"`
}

func (c *counter) Type() evt.EntityType                { return counterType }
func (c *counter) GetID() evt.EntityID                 { return c.id }
func (c *counter) SetID(id evt.EntityID)               { c.id = id }
func (c *counter) EventUpcasters() []evt.EventUpcaster { return nil }
func (c *counter) Projectors() []evt.EventProjector    { return nil }

type bumped struct {
	By   int    `json:"by"`
	Note string `json:"note"`
}

func (*bumped) Type() evt.EventType        { return "counter:bumped" }
func (*bumped) Version() evt.EventVersion  { return 1 }
func (*bumped) EntityType() evt.EntityType { return counterType }
func (*bumped) EntityID() evt.EntityID     { return "" }

type bump struct {
	By   int
	Note string
}

func (*bump) Type() evt.CommandType      { return "counter:bump" }
func (*bump) EntityType() evt.EntityType { return counterType }

type overdraw struct{}

func (*overdraw) Type() evt.CommandType      { return "counter:overdraw" }
func (*overdraw) EntityType() evt.EntityType { return counterType }

type refusal struct{}

func (*refusal) Error() string { return "overdrawn" }
func (*refusal) Rejection() evt.Rejection {
	return evt.Rejection{Code: "overdrawn", Message: "overdrawn"}
}

func (c *counter) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
	switch cmd := command.(type) {
	case *bump:
		return evt.CommandResult{Events: []evt.Event{&bumped{By: cmd.By, Note: cmd.Note}}}, nil
	case *overdraw:
		// A refusal that still records what happened, in the same commit.
		return evt.CommandResult{Events: []evt.Event{&bumped{By: 0, Note: "overdraw-attempted"}}}, evt.RejectWithEvents(&refusal{})
	}

	return evt.CommandResult{}, evt.NewBadCommandError(command)
}

func (c *counter) Apply(event evt.Event) error {
	switch e := event.(type) {
	case *bumped:
		c.Total += e.By
		c.Notes = append(c.Notes, e.Note)

		return nil
	}

	return evt.NewBadEventError(event)
}

func (c *counter) DeserializeEvent(serialized evt.SerializedEvent) (evt.Event, error) {
	var event bumped
	if err := json.Unmarshal(serialized.Payload, &event); err != nil {
		return nil, err
	}

	return &event, nil
}

// summary is a commit-wide projector: for every stream of the commit it puts the stream's state after
// the commit into the "totals" partition, and it puts one row for the commit itself into "commits",
// a partition that names no entity. It remembers the views it was called with.
type summary struct {
	calls []evt.CommitView
}

func (s *summary) Name() string { return "summary" }

// History: summary reads each stream's events, so the counter type is history-dependent and every
// row it writes is owned by a stream the commit wrote.
func (s *summary) History() []evt.EntityType { return []evt.EntityType{counterType} }

func (s *summary) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	s.calls = append(s.calls, commit)

	var (
		operations []evt.ViewOperation
		events     int
	)

	for _, stream := range commit.Streams {
		events += len(stream.Events)
		operations = append(operations,
			evt.PutView("totals", string(stream.EntityID), stream.EntityID, stream.EntityType, stream.State))
	}

	row, err := json.Marshal(map[string]int{"streams": len(commit.Streams), "events": events})
	if err != nil {
		return nil, err
	}

	return append(operations, evt.PutView("commits", commit.CommitID, commit.Streams[0].EntityID, "", row)), nil
}

// auditor is a second projector, to pin declaration order. It writes a row per commit, keyed by the
// commit ID, so it is history-dependent: a current-state projector is given no commit ID.
type auditor struct{}

func (auditor) Name() string { return "auditor" }

func (auditor) History() []evt.EntityType { return []evt.EntityType{counterType} }

func (auditor) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	return []evt.ViewOperation{evt.PutView("audit", commit.CommitID, commit.Streams[0].EntityID, "", []byte(`{}`))}, nil
}

// parity is a current-state projector: from the state each written stream has after the commit it
// puts a "state" row (the state itself) and, while the stream's total is even, deletes its "odd"
// row (put while it is odd). Both rows belong to the stream. It reads no event and no commit ID.
type parity struct{}

func (parity) Name() string { return "parity" }

func (parity) History() []evt.EntityType { return nil }

func (parity) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	var operations []evt.ViewOperation

	for _, stream := range commit.Streams {
		var state counter
		if err := json.Unmarshal(stream.State, &state); err != nil {
			return nil, err
		}

		operations = append(operations, evt.PutView("state", string(stream.EntityID), stream.EntityID, stream.EntityType, stream.State))

		if state.Total%2 == 0 {
			operations = append(operations, evt.DeleteView("odd", string(stream.EntityID), stream.EntityID))
		} else {
			operations = append(operations, evt.PutView("odd", string(stream.EntityID), stream.EntityID, stream.EntityType, []byte(`{}`)))
		}
	}

	return operations, nil
}

// recording is an outcome-capable repository that keeps every result handed to Commit, and can run
// a hook just before the first commit: another writer slipping in.
type recording struct {
	evt.OutcomeRepository

	results  []evt.SerializedResult
	declared []evt.EntityType
	before   func()
}

func (r *recording) AtomicTransactions() {}

// WritesViews and DeclareHistoryDependent forward the capabilities a decorator would otherwise hide.
func (r *recording) WritesViews() bool { return evt.WritesViews(r.OutcomeRepository) }

func (r *recording) DeclareHistoryDependent(ctx context.Context, types ...evt.EntityType) error {
	r.declared = append(r.declared, types...)

	declarer, ok := r.OutcomeRepository.(evt.HistoryDeclarer)
	if !ok {
		return &evt.HistoryDeclarationUnsupportedError{Repository: "recording"}
	}

	return declarer.DeclareHistoryDependent(ctx, types...)
}

func (r *recording) Commit(ctx context.Context, result evt.SerializedResult) error {
	if r.before != nil {
		hook := r.before
		r.before = nil
		hook()
	}

	r.results = append(r.results, result)

	return r.OutcomeRepository.Commit(ctx, result)
}

// CommitWithSnapshot routes through Commit, as the repositories do, so the snapshot is recorded
// with the rest of the result.
func (r *recording) CommitWithSnapshot(
	ctx context.Context,
	result evt.SerializedResult,
	entityType evt.EntityType,
	entityID evt.EntityID,
	payload []byte,
	currentSnapshot evt.EventSequence,
) error {
	result.Snapshots = append(result.Snapshots, evt.SerializedSnapshot{
		EntityType: entityType, EntityID: entityID, Sequence: currentSnapshot,
		EventSequence: result.Events[len(result.Events)-1].Sequence, Payload: payload,
	})

	return r.Commit(ctx, result)
}

func newRecording(t *testing.T) (*recording, *mem.Repository) {
	t.Helper()

	repo, ok := mem.NewRepository().(*mem.Repository)
	require.True(t, ok)

	return &recording{OutcomeRepository: repo}, repo
}

func commandID(id string) evt.Metadata {
	cmd := evt.CommandID(id)

	return evt.Metadata{CommandID: &cmd}
}

func row(t *testing.T, repo *mem.Repository, pk, sk string) string {
	t.Helper()

	view, ok := repo.View(pk, sk)
	require.True(t, ok, "row %s/%s", pk, sk)

	return string(view.Payload)
}

func TestCommitProjectors_ASingleAggregateCommitProjectsItsEventsAndState(t *testing.T) {
	recorder, repo := newRecording(t)
	projector := &summary{}
	store := snapshots.NewStore(recorder, 0).WithCommitProjectors(projector)
	ctx := context.Background()

	require.NoError(t, store.Execute(ctx, &counter{}, "c-1", &bump{By: 2, Note: "first"}, commandID("cmd-1")))
	require.NoError(t, store.Execute(ctx, &counter{}, "c-1", &bump{By: 3, Note: "second"}, commandID("cmd-2")))

	require.Len(t, projector.calls, 2)

	second := projector.calls[1]
	require.Equal(t, "cmd-2", second.CommitID, "the commit ID is the command ID")
	require.Len(t, second.Streams, 1)
	require.Equal(t, evt.EntityID("c-1"), second.Streams[0].EntityID)
	require.Equal(t, counterType, second.Streams[0].EntityType)
	require.Len(t, second.Streams[0].Events, 1, "only this commit's events")
	require.Equal(t, evt.EventSequence(2), second.Streams[0].Events[0].Sequence)
	require.Equal(t, "cmd-2", second.Streams[0].Events[0].Metadata.CommitID)
	require.JSONEq(t, `{"total":5,"notes":["first","second"]}`, string(second.Streams[0].State), "the state after the commit")

	require.JSONEq(t, `{"total":5,"notes":["first","second"]}`, row(t, repo, "totals", "c-1"))
	require.JSONEq(t, `{"streams":1,"events":1}`, row(t, repo, "commits", "cmd-2"))

	require.Len(t, recorder.results, 2)
	require.Len(t, recorder.results[1].Views, 2)
	require.Equal(t, evt.ProjectedView{
		Projector: "summary", ProjectorIndex: 0, Operation: 1, Op: recorder.results[1].Views[1].Op,
	}, recorder.results[1].Views[1])
}

func TestCommitProjectors_TheStateIsTheSnapshotPayloadWhenTheCommitSnapshots(t *testing.T) {
	recorder, repo := newRecording(t)
	projector := &summary{}
	store := snapshots.NewStore(recorder, 2).WithCommitProjectors(projector)
	ctx := context.Background()

	require.NoError(t, store.Execute(ctx, &counter{}, "c-1", &bump{By: 1, Note: "a"}, commandID("cmd-1")))
	require.NoError(t, store.Execute(ctx, &counter{}, "c-1", &bump{By: 1, Note: "b"}, commandID("cmd-2")))

	last := recorder.results[len(recorder.results)-1]
	require.Len(t, last.Snapshots, 1, "the second commit reaches the interval")
	require.Equal(t, last.Snapshots[0].Payload, projector.calls[1].Streams[0].State)
	require.JSONEq(t, string(last.Snapshots[0].Payload), row(t, repo, "totals", "c-1"))
}

func TestCommitProjectors_ProjectorsRunInDeclarationOrder(t *testing.T) {
	recorder, repo := newRecording(t)
	store := snapshots.NewStore(recorder, 0).WithCommitProjectors(&summary{}).WithCommitProjectors(auditor{})

	require.NoError(t, store.Execute(context.Background(), &counter{}, "c-1", &bump{By: 1}, commandID("cmd-1")))

	order := make([]string, 0, len(recorder.results[0].Views))
	for _, view := range recorder.results[0].Views {
		order = append(order, fmt.Sprintf("%s/%d/%d", view.Projector, view.ProjectorIndex, view.Operation))
	}

	require.Equal(t, []string{"summary/0/0", "summary/0/1", "auditor/1/0"}, order)
	require.JSONEq(t, `{}`, row(t, repo, "audit", "cmd-1"))
}

func TestCommitProjectors_ATransactionProjectsEveryWrittenStreamAtOnce(t *testing.T) {
	recorder, repo := newRecording(t)
	projector := &summary{}
	store := snapshots.NewStore(recorder, 0).WithCommitProjectors(projector)
	ctx := context.Background()

	require.NoError(t, store.Execute(ctx, &counter{}, "watched", &bump{By: 7}, commandID("seed")))
	recorder.results, projector.calls = nil, nil

	tx := store.Transaction()
	require.NoError(t, tx.Read(ctx, &counter{}, "b"))
	require.NoError(t, tx.Read(ctx, &counter{}, "a"))
	require.NoError(t, tx.Read(ctx, &counter{}, "watched"))
	require.NoError(t, tx.Append("b", &bumped{By: 2, Note: "b1"}, &bumped{By: 3, Note: "b2"}))
	require.NoError(t, tx.Append("a", &bumped{By: 1, Note: "a1"}))

	_, err := tx.Commit(ctx, commandID("cmd-multi"))
	require.NoError(t, err)

	require.Len(t, projector.calls, 1, "one call for the whole commit, not one per stream")

	commit := projector.calls[0]
	require.Equal(t, "cmd-multi", commit.CommitID)
	require.Len(t, commit.Streams, 2, "a stream that was only read is fenced, not part of the commit")
	require.Equal(t, evt.EntityID("a"), commit.Streams[0].EntityID, "ascending by entity ID bytes, whatever the read order")
	require.Equal(t, evt.EntityID("b"), commit.Streams[1].EntityID)
	require.Len(t, commit.Streams[1].Events, 2)
	require.JSONEq(t, `{"total":5,"notes":["b1","b2"]}`, string(commit.Streams[1].State))

	require.JSONEq(t, `{"total":1,"notes":["a1"]}`, row(t, repo, "totals", "a"))
	require.JSONEq(t, `{"total":5,"notes":["b1","b2"]}`, row(t, repo, "totals", "b"))
	require.JSONEq(t, `{"streams":2,"events":3}`, row(t, repo, "commits", "cmd-multi"))
	require.Len(t, recorder.results[0].Fences, 1)
}

func TestCommitProjectors_ACommandThatWritesNoEventRunsNoProjector(t *testing.T) {
	recorder, repo := newRecording(t)
	projector := &summary{}
	store := snapshots.NewStore(recorder, 0).WithOutcomes().WithCommitProjectors(projector)
	ctx := context.Background()

	// A transaction that appends nothing records an accepted no-op; one that rejects records a
	// plain rejection. Neither has an event for a view to project.
	tx, _, err := store.BeginCommand(ctx, evt.CommandIdentity{Type: "noop"}, commandID("noop"))
	require.NoError(t, err)
	_, err = tx.Complete(ctx, nil)
	require.NoError(t, err)

	tx, _, err = store.BeginCommand(ctx, evt.CommandIdentity{Type: "refuse"}, commandID("refuse"))
	require.NoError(t, err)
	_, err = tx.Reject(ctx, evt.Rejection{Code: "no", Message: "no"})
	require.NoError(t, err)

	require.Empty(t, projector.calls)
	require.Empty(t, repo.Views())
}

func TestCommitProjectors_ARejectionThatCarriesEventsProjectsThem(t *testing.T) {
	recorder, repo := newRecording(t)
	projector := &summary{}
	store := snapshots.NewStore(recorder, 0).WithOutcomes().WithCommitProjectors(projector)
	ctx := context.Background()

	outcome, err := store.ExecuteWithOutcome(ctx, &counter{}, "c-1", &bump{By: 4, Note: "seed"}, commandID("seed"))
	require.NoError(t, err)
	require.False(t, outcome.Rejected())

	outcome, err = store.ExecuteWithOutcome(ctx, &counter{}, "c-1", &overdraw{}, commandID("overdraw"))
	require.NoError(t, err)
	require.True(t, outcome.Rejected())
	require.Len(t, outcome.EventIDs, 1, "the rejection committed its event")

	last := projector.calls[len(projector.calls)-1]
	require.Equal(t, "overdraw", last.CommitID)
	require.Equal(t, "counter:bumped", string(last.Streams[0].Events[0].Type))
	require.JSONEq(t, `{"total":4,"notes":["seed","overdraw-attempted"]}`, row(t, repo, "totals", "c-1"))

	// A replay of the command runs nothing: the stored outcome answers it.
	calls := len(projector.calls)
	outcome, err = store.ExecuteWithOutcome(ctx, &counter{}, "c-1", &overdraw{}, commandID("overdraw"))
	require.NoError(t, err)
	require.True(t, outcome.Replayed)
	require.Len(t, projector.calls, calls)
}

func TestCommitProjectors_APreparationDerivesItsOperationsAgain(t *testing.T) {
	recorder, repo := newRecording(t)
	projector := &summary{}
	store := snapshots.NewStore(recorder, 0).
		WithOutcomes().
		WithRetry(snapshots.ImmediateRetryPolicy(3)).
		WithCommitProjectors(projector)
	ctx := context.Background()

	_, err := store.ExecuteWithOutcome(ctx, &counter{}, "c-1", &bump{By: 1, Note: "seed"}, commandID("seed"))
	require.NoError(t, err)
	projector.calls, recorder.results = nil, nil

	// Another writer appends to the stream after this command loaded it and before it commits. The
	// first attempt conflicts, the command is decided again on the fresh state, and the projector
	// runs again over it: the failed attempt's operations are discarded with the rest of it.
	other := snapshots.NewStore(repo, 0)
	recorder.before = func() {
		require.NoError(t, other.Execute(ctx, &counter{}, "c-1", &bump{By: 10, Note: "other"}, evt.Metadata{}))
	}

	outcome, err := store.ExecuteWithOutcome(ctx, &counter{}, "c-1", &bump{By: 100, Note: "mine"}, commandID("mine"))
	require.NoError(t, err)
	require.False(t, outcome.Rejected())

	require.Len(t, projector.calls, 2, "once per preparation")
	require.JSONEq(t, `{"total":101,"notes":["seed","mine"]}`, string(projector.calls[0].Streams[0].State), "decided on the stale state")
	require.JSONEq(t, `{"total":111,"notes":["seed","other","mine"]}`, string(projector.calls[1].Streams[0].State), "decided on the fresh state")

	require.Len(t, recorder.results, 2)
	require.JSONEq(t, `{"total":111,"notes":["seed","other","mine"]}`, row(t, repo, "totals", "c-1"), "only the committing attempt wrote")
}

func TestCommitProjectors_AProjectorFailureIsAnInternalFailure(t *testing.T) {
	recorder, repo := newRecording(t)
	cause := errors.New("cannot project")
	store := snapshots.NewStore(recorder, 0).WithOutcomes().WithCommitProjectors(failing{cause})
	ctx := context.Background()

	_, err := store.ExecuteWithOutcome(ctx, &counter{}, "c-1", &bump{By: 1}, commandID("cmd-1"))

	var failed *evt.ProjectorError
	require.ErrorAs(t, err, &failed)
	require.Equal(t, "failing", failed.Projector)
	require.ErrorIs(t, err, cause)

	events, err := repo.GetEvents(ctx, "c-1")
	require.NoError(t, err)
	require.Empty(t, events, "nothing is written")

	outcome, err := repo.GetOutcome(ctx, "cmd-1")
	require.NoError(t, err)
	require.Nil(t, outcome, "an internal failure records no outcome")
}

type failing struct{ err error }

func (failing) Name() string              { return "failing" }
func (failing) History() []evt.EntityType { return nil }
func (f failing) ProjectCommit(context.Context, evt.CommitView) ([]evt.ViewOperation, error) {
	return nil, f.err
}

func TestCommitProjectors_AFailedConditionIsNotRetriedAndRecordsNoOutcome(t *testing.T) {
	recorder, repo := newRecording(t)
	taken := errors.New("handle taken")
	store := snapshots.NewStore(recorder, 0).
		WithOutcomes().
		WithRetry(snapshots.ImmediateRetryPolicy(5)).
		WithCommitProjectors(claimer{taken})
	ctx := context.Background()

	_, err := store.ExecuteWithOutcome(ctx, &counter{}, "c-1", &bump{By: 1}, commandID("first"))
	require.NoError(t, err)

	_, err = store.ExecuteWithOutcome(ctx, &counter{}, "c-2", &bump{By: 1}, commandID("second"))
	require.ErrorIs(t, err, evt.ErrProjectorOperation)
	require.ErrorIs(t, err, taken)
	require.Len(t, recorder.results, 2, "returned at once: no second preparation, no stale-fence rejection")

	events, err := repo.GetEvents(ctx, "c-2")
	require.NoError(t, err)
	require.Empty(t, events)

	outcome, err := repo.GetOutcome(ctx, "second")
	require.NoError(t, err)
	require.Nil(t, outcome)
}

// claimer claims the handle "ada" for whichever stream it projects: the second claim fails.
type claimer struct{ taken error }

func (claimer) Name() string              { return "claims" }
func (claimer) History() []evt.EntityType { return nil }
func (c claimer) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	return []evt.ViewOperation{
		evt.PutView("handles", "ada", commit.Streams[0].EntityID, counterType, []byte(`{}`)).IfAbsent().OnConflict(c.taken),
	}, nil
}

func TestCommitProjectors_ReplayingTheStoredEventsReproducesTheOperationsAndTheRows(t *testing.T) {
	recorder, repo := newRecording(t)
	store := snapshots.NewStore(recorder, 3).WithCommitProjectors(&summary{}, auditor{}, parity{})
	ctx := context.Background()

	// A history of single-stream and multi-stream commits, across snapshot boundaries.
	for i := range 5 {
		require.NoError(t, store.Execute(ctx, &counter{}, "a", &bump{By: i + 1, Note: fmt.Sprintf("a%d", i)}, commandID(fmt.Sprintf("single-%d", i))))
	}

	tx := store.Transaction()
	require.NoError(t, tx.Read(ctx, &counter{}, "a"))
	require.NoError(t, tx.Read(ctx, &counter{}, "b"))
	require.NoError(t, tx.Append("b", &bumped{By: 2, Note: "b0"}, &bumped{By: 4, Note: "b1"}))
	require.NoError(t, tx.Append("a", &bumped{By: 8, Note: "a-multi"}))
	_, err := tx.Commit(ctx, commandID("multi"))
	require.NoError(t, err)

	live := map[string][]evt.ProjectedView{}
	for _, result := range recorder.results {
		live[result.Events[0].Metadata.CommitID] = result.Views
	}

	// Wipe and replay: regroup every stored event by its commit ID, fold the state each stream had
	// after the commit, and run the projectors again. They must write the same operations.
	var stored []evt.SerializedEvent
	for _, id := range []evt.EntityID{"a", "b"} {
		events, gerr := repo.GetEvents(ctx, id)
		require.NoError(t, gerr)
		stored = append(stored, events...)
	}

	groups, err := evt.GroupCommits(stored)
	require.NoError(t, err)
	require.Len(t, groups, 6)

	projectors := []evt.CommitProjector{&summary{}, auditor{}, parity{}}

	for _, group := range groups {
		for i, stream := range group.Streams {
			group.Streams[i].State = foldState(t, stored, stream.EntityID, stream.Events[len(stream.Events)-1].Sequence)
		}

		views, projectErr := evt.ProjectCommit(ctx, projectors, group)
		require.NoError(t, projectErr)
		require.Equal(t, live[group.CommitID], views, "commit %s", group.CommitID)
	}

	// The rows themselves: a wipe and a replay of every commit rebuilds exactly the rows the live
	// commits left, deletes included.
	for i, group := range groups {
		for j, stream := range group.Streams {
			groups[i].Streams[j].State = foldState(t, stored, stream.EntityID, stream.Events[len(stream.Events)-1].Sequence)
		}
	}

	rebuilt, err := evt.ReplayViews(ctx, projectors, groups)
	require.NoError(t, err)
	require.NotEmpty(t, rebuilt)
	require.Equal(t, repo.Views(), rebuilt, "the rebuilt rows are the live rows, byte for byte")
}

// foldState rebuilds a stream's state after the given sequence from its stored events.
func foldState(t *testing.T, stored []evt.SerializedEvent, id evt.EntityID, through evt.EventSequence) []byte {
	t.Helper()

	var events []evt.SerializedEvent
	for _, event := range stored {
		if event.EntityID == id && event.Sequence <= through {
			events = append(events, event)
		}
	}

	slices.SortFunc(events, func(a, b evt.SerializedEvent) int { return int(a.Sequence - b.Sequence) })

	aggregate := &counter{id: id}
	for _, serialized := range events {
		event, err := aggregate.DeserializeEvent(serialized)
		require.NoError(t, err)
		require.NoError(t, aggregate.Apply(event))
	}

	state, err := json.Marshal(aggregate)
	require.NoError(t, err)

	return state
}

func TestCommitProjectors_AStoreWithoutProjectorsWritesNoViews(t *testing.T) {
	recorder, repo := newRecording(t)
	store := snapshots.NewStore(recorder, 0)

	require.NoError(t, store.Execute(context.Background(), &counter{}, "c-1", &bump{By: 1}, commandID("cmd-1")))

	require.Empty(t, recorder.results[0].Views)
	require.Empty(t, repo.Views())
	require.False(t, strings.Contains(fmt.Sprint(recorder.results[0]), "Projector"))
}

func TestCommitProjectors_AMappedErrorThatLooksLikeAConflictIsNotRetried(t *testing.T) {
	recorder, repo := newRecording(t)
	store := snapshots.NewStore(recorder, 0).
		WithOutcomes().
		WithRetry(snapshots.ImmediateRetryPolicy(5)).
		WithCommitProjectors(claimer{evt.NewConflictError("handle taken")})
	ctx := context.Background()

	_, err := store.ExecuteWithOutcome(ctx, &counter{}, "c-1", &bump{By: 1}, commandID("first"))
	require.NoError(t, err)

	attempts := len(recorder.results)

	// The application maps the failed claim to a conflict-shaped error of its own. It is still the
	// projector's failure: returned at once, never re-prepared, never recorded as a stale-fence.
	_, err = store.ExecuteWithOutcome(ctx, &counter{}, "c-2", &bump{By: 1}, commandID("second"))
	require.ErrorIs(t, err, evt.ErrProjectorOperation)
	require.Len(t, recorder.results, attempts+1, "one attempt, not five")

	outcome, err := repo.GetOutcome(ctx, "second")
	require.NoError(t, err)
	require.Nil(t, outcome)
}

func TestCommitProjectors_AHistoryDependentProjectorIsDeclaredDurablyBeforeItsFirstCommit(t *testing.T) {
	recorder, repo := newRecording(t)
	store := snapshots.NewStore(recorder, 1).WithCommitProjectors(&summary{}, auditor{})
	ctx := context.Background()

	require.NoError(t, store.Execute(ctx, &counter{}, "c-1", &bump{By: 1}, commandID("cmd-1")))
	require.NoError(t, store.Execute(ctx, &counter{}, "c-1", &bump{By: 1}, commandID("cmd-2")))

	require.Equal(t, []evt.EntityType{counterType}, recorder.declared, "declared once, not on every commit")

	manifest, err := repo.Manifest(ctx)
	require.NoError(t, err)
	require.Equal(t, []evt.EntityType{counterType}, manifest.HistoryDependent)

	// The declaration is durable and shared: a writer that never registered the projector is
	// refused compaction of the type, because the commits a rebuild replays must stay.
	_, err = repo.CompactBelow(ctx, "c-1", 1)
	require.True(t, evt.IsHistoryDependentProjectionErr(err), "got %v", err)
}

func TestCommitProjectors_ACurrentStateProjectorDeclaresNothingAndSeesNoEvents(t *testing.T) {
	recorder, repo := newRecording(t)
	store := snapshots.NewStore(recorder, 1).WithCommitProjectors(parity{})
	ctx := context.Background()

	require.NoError(t, store.Execute(ctx, &counter{}, "c-1", &bump{By: 1}, commandID("cmd-1")))
	require.NoError(t, store.Execute(ctx, &counter{}, "c-1", &bump{By: 1}, commandID("cmd-2")))

	require.Empty(t, recorder.declared)

	deleted, err := repo.CompactBelow(ctx, "c-1", 1)
	require.NoError(t, err, "no history is declared, so compaction is free")
	require.Equal(t, 1, deleted)
}

// bare is a repository written before v0.9: it declares neither view writing nor history.
type bare struct{ evt.OutcomeRepository }

func (bare) AtomicTransactions() {}

func TestCommitProjectors_ARepositoryThatDoesNotWriteViewsIsRefusedBeforeAnyWrite(t *testing.T) {
	inner, repo := newRecording(t)
	store := snapshots.NewStore(bare{inner}, 0).WithCommitProjectors(auditor{})

	err := store.Execute(context.Background(), &counter{}, "c-1", &bump{By: 1}, commandID("cmd-1"))

	var unsupported *evt.ViewsUnsupportedError

	require.ErrorAs(t, err, &unsupported)
	require.Empty(t, inner.results)

	events, gerr := repo.GetEvents(context.Background(), "c-1")
	require.NoError(t, gerr)
	require.Empty(t, events, "the events were not committed without their views")
}

// viewsOnly writes views but cannot declare history.
type viewsOnly struct{ bare }

func (viewsOnly) WritesViews() bool { return true }

func TestCommitProjectors_ARepositoryThatCannotDeclareHistoryRefusesAHistoryDependentProjector(t *testing.T) {
	inner, _ := newRecording(t)
	store := snapshots.NewStore(viewsOnly{bare{inner}}, 0).WithCommitProjectors(&summary{})

	err := store.Execute(context.Background(), &counter{}, "c-1", &bump{By: 1}, commandID("cmd-1"))

	var unsupported *evt.HistoryDeclarationUnsupportedError

	require.ErrorAs(t, err, &unsupported)
	require.Empty(t, inner.results)
}

// unmarshalable cannot be encoded as JSON, so serializing its state fails.
type unmarshalable struct {
	counter

	Broken func() `json:"broken"`
}

func (u *unmarshalable) Handle(ctx context.Context, command evt.Command) (evt.CommandResult, error) {
	if _, ok := command.(*ping); ok {
		return evt.CommandResult{}, nil // accepted, with no events
	}

	return u.counter.Handle(ctx, command)
}

type ping struct{}

func (*ping) Type() evt.CommandType      { return "counter:ping" }
func (*ping) EntityType() evt.EntityType { return counterType }

func TestCommitProjectors_ACommitThatWritesNoEventNeverEncodesState(t *testing.T) {
	recorder, repo := newRecording(t)
	store := snapshots.NewStore(recorder, 0).WithCommitProjectors(auditor{})
	ctx := context.Background()

	// Encoding this aggregate's state fails. A no-op writes no event, so no projector runs and no
	// state is encoded: the command succeeds, as it does without projectors.
	require.NoError(t, store.Execute(ctx, &unmarshalable{}, "c-1", &ping{}, commandID("noop")))

	for _, result := range recorder.results {
		require.Empty(t, result.Views)
		require.Empty(t, result.Events)
	}

	events, err := repo.GetEvents(ctx, "c-1")
	require.NoError(t, err)
	require.Empty(t, events)
}

// rowOf writes one fixed row for the first stream a commit wrote.
type rowOf struct{}

func (rowOf) Name() string { return "row-of" }

func (rowOf) History() []evt.EntityType { return []evt.EntityType{counterType} }

func (rowOf) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	first := commit.Streams[0].EntityID

	return []evt.ViewOperation{evt.PutView("shared", "row", first, counterType, []byte(`"`+string(first)+`"`))}, nil
}

func TestCommitProjectors_AProjectorCannotWriteARowAnotherStreamOwns(t *testing.T) {
	recorder, repo := newRecording(t)
	store := snapshots.NewStore(recorder, 0).WithCommitProjectors(rowOf{})
	ctx := context.Background()

	require.NoError(t, store.Execute(ctx, &counter{}, "a", &bump{By: 1}, commandID("cmd-a")))

	// Another stream writes the same row: the owner never changes, so the commit fails, live, on the
	// owner condition rather than later at a rebuild.
	err := store.Execute(ctx, &counter{}, "b", &bump{By: 1}, commandID("cmd-b"))
	require.ErrorIs(t, err, evt.ErrProjectorOperation)

	var failed *evt.ProjectorOperationError

	require.ErrorAs(t, err, &failed)
	require.Equal(t, evt.ReasonConditionFailed, failed.Reason)

	events, gerr := repo.GetEvents(ctx, "b")
	require.NoError(t, gerr)
	require.Empty(t, events)
	require.Equal(t, `"a"`, row(t, repo, "shared", "row"))

	// The owner still rewrites its row.
	require.NoError(t, store.Execute(ctx, &counter{}, "a", &bump{By: 1}, commandID("cmd-a2")))
}

// handles keeps one row per handle stream ("handle:<name>"): the holder named by the stream's latest
// claim or release event, with a release written as a tombstone, never a delete. The handle stream
// owns the row for good, whoever claims the name.
type handles struct{}

func (handles) Name() string { return "handles" }

func (handles) History() []evt.EntityType { return []evt.EntityType{counterType} }

func (handles) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	var operations []evt.ViewOperation

	for _, stream := range commit.Streams {
		name, ok := strings.CutPrefix(string(stream.EntityID), "handle:")
		if !ok {
			continue
		}

		var note struct {
			Note string `json:"note"`
		}

		if err := json.Unmarshal(stream.Events[len(stream.Events)-1].Payload, &note); err != nil {
			return nil, err
		}

		holder := ""
		if claimed, ok := strings.CutPrefix(note.Note, "claim:"); ok {
			holder = claimed
		}

		operations = append(operations, evt.PutView("handles", name, stream.EntityID, stream.EntityType,
			fmt.Appendf(nil, `{"holder":%q}`, holder)))
	}

	return operations, nil
}

func TestCommitProjectors_AHandleStreamOwnsItsRowAcrossReleaseAndReclaim(t *testing.T) {
	recorder, repo := newRecording(t)
	projectors := []evt.CommitProjector{handles{}}
	store := snapshots.NewStore(recorder, 0).WithCommitProjectors(projectors...)
	ctx := context.Background()

	claim := func(command, holder string) {
		tx := store.Transaction()
		require.NoError(t, tx.Read(ctx, &counter{}, "handle:ada"))
		require.NoError(t, tx.Read(ctx, &counter{}, evt.EntityID(holder)))
		require.NoError(t, tx.Append("handle:ada", &bumped{By: 1, Note: "claim:" + holder}))
		require.NoError(t, tx.Append(evt.EntityID(holder), &bumped{By: 1, Note: "claimed"}))

		_, err := tx.Commit(ctx, commandID(command))
		require.NoError(t, err)
	}

	claim("alice-claims", "alice")
	require.Equal(t, `{"holder":"alice"}`, row(t, repo, "handles", "ada"))

	require.NoError(t, store.Execute(ctx, &counter{}, "handle:ada", &bump{By: 1, Note: "released"}, commandID("alice-releases")))
	require.Equal(t, `{"holder":""}`, row(t, repo, "handles", "ada"), "a release is a tombstone put")

	claim("bob-claims", "bob")
	require.Equal(t, `{"holder":"bob"}`, row(t, repo, "handles", "ada"))

	// A rebuild reproduces it: every write to the row came from commits of the handle stream.
	var stored []evt.SerializedEvent
	for _, id := range []evt.EntityID{"handle:ada", "alice", "bob"} {
		events, err := repo.GetEvents(ctx, id)
		require.NoError(t, err)
		stored = append(stored, events...)
	}

	groups, err := evt.GroupCommits(stored)
	require.NoError(t, err)

	rebuilt, err := evt.ReplayViews(ctx, projectors, groups)
	require.NoError(t, err)
	require.Equal(t, repo.Views(), rebuilt)
}

func TestCommitProjectors_ADecoratorReportsTheCapabilityOfTheRepositoryItWraps(t *testing.T) {
	inner, _ := newRecording(t)

	over, err := test.NewFaultRepository(inner)
	require.NoError(t, err)
	require.True(t, evt.WritesViews(over))

	// A decorator over a repository that cannot write views must not claim it can.
	bareOver, err := test.NewFaultRepository(bare{inner})
	require.NoError(t, err)
	require.False(t, evt.WritesViews(bareOver))

	store := snapshots.NewStore(bareOver, 0).WithCommitProjectors(parity{})

	var unsupported *evt.ViewsUnsupportedError

	require.ErrorAs(t, store.Execute(context.Background(), &counter{}, "c-1", &bump{By: 1}, commandID("cmd-1")), &unsupported)
	require.Empty(t, inner.results)
}
