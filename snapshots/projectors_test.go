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

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/mem"
	"github.com/photon-grove/evt/snapshots"
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
func (c *counter) Base() evt.BaseEntity                { return evt.BaseEntity{ID: c.id, IsActive: true} } //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion

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

func (s *summary) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	s.calls = append(s.calls, commit)

	var (
		operations []evt.ViewOperation
		events     int
	)

	for _, stream := range commit.Streams {
		events += len(stream.Events)
		operations = append(operations, evt.PutView("totals", string(stream.EntityID), stream.EntityID, stream.EntityType, stream.State))
	}

	row, err := json.Marshal(map[string]int{"streams": len(commit.Streams), "events": events})
	if err != nil {
		return nil, err
	}

	return append(operations, evt.PutView("commits", commit.CommitID, "", "", row)), nil
}

// auditor is a second projector, to pin declaration order.
type auditor struct{}

func (auditor) Name() string { return "auditor" }

func (auditor) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	return []evt.ViewOperation{evt.PutView("audit", commit.CommitID, "", "", []byte(`{}`))}, nil
}

// recording is an outcome-capable repository that keeps every result handed to Commit, and can run
// a hook just before the first commit: another writer slipping in.
type recording struct {
	evt.OutcomeRepository

	results []evt.SerializedResult
	before  func()
}

func (r *recording) AtomicTransactions() {}

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

func (failing) Name() string { return "failing" }
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

func (claimer) Name() string { return "claims" }
func (c claimer) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	return []evt.ViewOperation{
		evt.PutView("handles", "ada", commit.Streams[0].EntityID, counterType, []byte(`{}`)).IfAbsent().OnConflict(c.taken),
	}, nil
}

func TestCommitProjectors_ReplayingTheStoredEventsReproducesTheOperations(t *testing.T) {
	recorder, repo := newRecording(t)
	store := snapshots.NewStore(recorder, 3).WithCommitProjectors(&summary{}, auditor{})
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
		events, err := repo.GetEvents(ctx, id)
		require.NoError(t, err)
		stored = append(stored, events...)
	}

	groups := evt.GroupCommits(stored)
	require.Len(t, groups, 6)

	replayed := &summary{}
	projectors := []evt.CommitProjector{replayed, auditor{}}

	for _, group := range groups {
		for i, stream := range group.Streams {
			group.Streams[i].State = foldState(t, stored, stream.EntityID, stream.Events[len(stream.Events)-1].Sequence)
		}

		views, err := evt.ProjectCommit(ctx, projectors, group)
		require.NoError(t, err)
		require.Equal(t, live[group.CommitID], views, "commit %s", group.CommitID)
	}
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

	entity := &counter{id: id}
	for _, serialized := range events {
		event, err := entity.DeserializeEvent(serialized)
		require.NoError(t, err)
		require.NoError(t, entity.Apply(event))
	}

	state, err := json.Marshal(entity)
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
