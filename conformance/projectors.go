package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/snapshots"
)

// These cases drive commit-wide projectors (ADR 0004, C7) through snapshots.Store over the backend
// under test: the engine builds the commit view, the repository writes the rows in the commit's
// transaction, and a failed projector operation comes back typed and is neither retried nor
// recorded as an outcome.

const tallyType evt.EntityType = "tally"

// tally is an aggregate with pure state, so the state a stream has after a commit is a function of
// its events.
type tally struct {
	id    evt.EntityID
	Total int `json:"total"`
}

func (t *tally) Type() evt.EntityType                { return tallyType }
func (t *tally) GetID() evt.EntityID                 { return t.id }
func (t *tally) SetID(id evt.EntityID)               { t.id = id }
func (t *tally) EventUpcasters() []evt.EventUpcaster { return nil }
func (t *tally) Projectors() []evt.EventProjector    { return nil }
func (t *tally) Base() evt.BaseEntity                { return evt.BaseEntity{ID: t.id, IsActive: true} } //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion

type tallied struct {
	By int `json:"by"`
}

func (*tallied) Type() evt.EventType        { return "tally:tallied" }
func (*tallied) Version() evt.EventVersion  { return 1 }
func (*tallied) EntityType() evt.EntityType { return tallyType }
func (*tallied) EntityID() evt.EntityID     { return "" }

type tallyUp struct{ By int }

func (*tallyUp) Type() evt.CommandType      { return "tally:up" }
func (*tallyUp) EntityType() evt.EntityType { return tallyType }

func (t *tally) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
	if up, ok := command.(*tallyUp); ok {
		return evt.CommandResult{Events: []evt.Event{&tallied{By: up.By}}}, nil
	}

	return evt.CommandResult{}, evt.NewBadCommandError(command)
}

func (t *tally) Apply(event evt.Event) error {
	if e, ok := event.(*tallied); ok {
		t.Total += e.By

		return nil
	}

	return evt.NewBadEventError(event)
}

func (t *tally) DeserializeEvent(serialized evt.SerializedEvent) (evt.Event, error) {
	var event tallied
	if err := json.Unmarshal(serialized.Payload, &event); err != nil {
		return nil, err
	}

	return &event, nil
}

// rollup is a commit-wide projector: one row per written stream, in a partition that names no
// entity, from the state after the commit, plus a row for the commit itself.
type rollup struct {
	namespace string
	calls     int
	claim     string
}

func (r *rollup) Name() string { return "rollup" }

// History: the rollup reads each stream's state after the commit, never its events.
func (r *rollup) History() []evt.EntityType { return nil }

func (r *rollup) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	r.calls++

	var operations []evt.ViewOperation

	for _, stream := range commit.Streams {
		operations = append(operations, evt.PutView(r.namespace+"#totals", string(stream.EntityID), stream.EntityID, stream.EntityType, stream.State))
	}

	operations = append(operations, evt.PutView(r.namespace+"#commits", commit.CommitID, "", "", fmt.Appendf(nil, `{"streams":%d}`, len(commit.Streams))))

	if r.claim != "" {
		// The claim is exclusive: a second stream claiming the same name fails the whole commit.
		operations = append(operations, evt.PutView(r.namespace+"#claims", r.claim, commit.Streams[0].EntityID, tallyType, []byte(`{}`)).
			IfAbsent().OnConflict(errClaimTaken))
	}

	return operations, nil
}

func tallyMetadata(id string) evt.Metadata {
	commandID := evt.CommandID(id)

	return evt.Metadata{CommandID: &commandID}
}

// runProjectorCases runs the engine-level cases of commit-wide projectors on a backend that writes
// view operations and records command outcomes.
func runProjectorCases(t *testing.T, newRepo func() evt.Repository, opts SuiteOptions) {
	t.Helper()
	require.NotNil(t, opts.ReadView)

	cases := []struct {
		name string
		run  func(*testing.T, func() evt.Repository, ViewReader)
	}{
		{"Projectors/ProjectEveryCommitOfAnAggregateAndATransaction", testProjectorsProjectCommits},
		{"Projectors/AFailedOperationIsTypedNotRetriedAndRecordsNoOutcome", testProjectorFailureIsTyped},
		{"Projectors/APreparationDerivesItsOperationsAgain", testProjectorsRunAgainOnReprepare},
		{"Projectors/AHistoryDependentProjectorIsDeclaredAndRefusesCompaction", testProjectorsDeclareHistory},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.run(t, newRepo, opts.ReadView)
		})
	}
}

func testProjectorsProjectCommits(t *testing.T, newRepo func() evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	repo := newRepo()
	projector := &rollup{namespace: ns}
	store := snapshots.NewStore(repo, 3).WithCommitProjectors(projector)

	idA, idB := evt.EntityID(ns+"-a"), evt.EntityID(ns+"-b")

	// Single-aggregate commands, across a snapshot boundary: each commit rewrites the stream's row
	// from its state after the commit.
	for i := range 4 {
		require.NoError(t, store.Execute(ctx, &tally{}, idA, &tallyUp{By: i + 1}, tallyMetadata(fmt.Sprintf("%s-single-%d", ns, i))))
	}

	view, err := read(ctx, repo, ns+"#totals", string(idA))
	require.NoError(t, err)
	require.NotNil(t, view)
	require.JSONEq(t, `{"total":10}`, string(view.Payload))
	require.Equal(t, idA, view.EntityID)
	require.Equal(t, tallyType, view.EntityType)

	// One multi-stream transaction projects both streams in one call and one commit row.
	before := projector.calls
	tx := store.Transaction()
	require.NoError(t, tx.Read(ctx, &tally{}, idA))
	require.NoError(t, tx.Read(ctx, &tally{}, idB))
	require.NoError(t, tx.Append(idB, &tallied{By: 5}))
	require.NoError(t, tx.Append(idA, &tallied{By: 100}))

	_, err = tx.Commit(ctx, tallyMetadata(ns+"-multi"))
	require.NoError(t, err)
	require.Equal(t, before+1, projector.calls, "one call for the whole commit")

	for id, want := range map[evt.EntityID]string{idA: `{"total":110}`, idB: `{"total":5}`} {
		view, err = read(ctx, repo, ns+"#totals", string(id))
		require.NoError(t, err)
		require.NotNil(t, view)
		require.JSONEq(t, want, string(view.Payload))
	}

	view, err = read(ctx, repo, ns+"#commits", ns+"-multi")
	require.NoError(t, err)
	require.NotNil(t, view)
	require.JSONEq(t, `{"streams":2}`, string(view.Payload))
}

func testProjectorFailureIsTyped(t *testing.T, newRepo func() evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	repo := newRepo()
	outcomes, err := evt.RequireOutcomes(repo)
	require.NoError(t, err)

	idA, idB := evt.EntityID(ns+"-a"), evt.EntityID(ns+"-b")
	store := snapshots.NewStore(repo, 0).
		WithOutcomes().
		WithRetry(snapshots.ImmediateRetryPolicy(5)).
		WithCommitProjectors(&rollup{namespace: ns, claim: "ada"})

	_, err = store.ExecuteWithOutcome(ctx, &tally{}, idA, &tallyUp{By: 1}, tallyMetadata(ns+"-first"))
	require.NoError(t, err)

	// The second stream claims the same name: the typed error comes back at once, from the first
	// preparation, and the command leaves no events, no rows, and no outcome behind.
	_, err = store.ExecuteWithOutcome(ctx, &tally{}, idB, &tallyUp{By: 2}, tallyMetadata(ns+"-second"))
	require.ErrorIs(t, err, evt.ErrProjectorOperation)
	require.ErrorIs(t, err, errClaimTaken)

	var failed *evt.ProjectorOperationError

	require.ErrorAs(t, err, &failed)
	require.Equal(t, "rollup", failed.Projector)
	require.Equal(t, 2, failed.Operation, "the claim is the rollup's third operation")
	require.Equal(t, evt.ReasonConditionFailed, failed.Reason)

	requireNoEvents(t, repo, idB)
	requireNoRow(t, read, repo, ns+"#totals", string(idB))

	outcome, err := outcomes.GetOutcome(ctx, evt.CommandID(ns+"-second"))
	require.NoError(t, err)
	require.Nil(t, outcome, "a projector failure is an internal failure: no outcome is recorded")
}

// hookRepository runs a hook just before its first commit: another writer slipping in between a
// command's load and its commit.
type hookRepository struct {
	evt.OutcomeRepository

	before func()
}

func (h *hookRepository) AtomicTransactions() {}

// WritesViews and DeclareHistoryDependent forward what the wrapped repository does, which a
// decorator would otherwise hide.
func (h *hookRepository) WritesViews() {}

func (h *hookRepository) DeclareHistoryDependent(ctx context.Context, types ...evt.EntityType) error {
	declarer, ok := h.OutcomeRepository.(evt.HistoryDeclarer)
	if !ok {
		return &evt.HistoryDeclarationUnsupportedError{Repository: fmt.Sprintf("%T", h.OutcomeRepository)}
	}

	return declarer.DeclareHistoryDependent(ctx, types...)
}

func (h *hookRepository) Commit(ctx context.Context, result evt.SerializedResult) error {
	if hook := h.before; hook != nil {
		h.before = nil
		hook()
	}

	return h.OutcomeRepository.Commit(ctx, result)
}

func (h *hookRepository) CommitWithSnapshot(
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

	return h.Commit(ctx, result)
}

func testProjectorsRunAgainOnReprepare(t *testing.T, newRepo func() evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	repo := newRepo()
	outcomes, err := evt.RequireOutcomes(repo)
	require.NoError(t, err)

	id := evt.EntityID(ns + "-a")
	projector := &rollup{namespace: ns}
	hooked := &hookRepository{OutcomeRepository: outcomes}

	store := snapshots.NewStore(hooked, 0).
		WithOutcomes().
		WithRetry(snapshots.ImmediateRetryPolicy(3)).
		WithCommitProjectors(projector)

	_, err = store.ExecuteWithOutcome(ctx, &tally{}, id, &tallyUp{By: 1}, tallyMetadata(ns+"-seed"))
	require.NoError(t, err)

	seeded := projector.calls

	// A competing writer appends after this command loaded the stream and before it commits. The
	// first attempt conflicts; the command is decided again on the fresh state and the projector
	// runs again over it. Only the committing attempt wrote its rows.
	other := snapshots.NewStore(repo, 0)
	hooked.before = func() {
		require.NoError(t, other.Execute(ctx, &tally{}, id, &tallyUp{By: 10}, evt.Metadata{}))
	}

	outcome, err := store.ExecuteWithOutcome(ctx, &tally{}, id, &tallyUp{By: 100}, tallyMetadata(ns+"-mine"))
	require.NoError(t, err)
	require.False(t, outcome.Rejected())
	require.Equal(t, seeded+2, projector.calls, "once per preparation")

	view, err := read(ctx, repo, ns+"#totals", string(id))
	require.NoError(t, err)
	require.NotNil(t, view)
	require.JSONEq(t, `{"total":111}`, string(view.Payload), "the row reflects the attempt that committed")

	view, err = read(ctx, repo, ns+"#commits", ns+"-mine")
	require.NoError(t, err)
	require.NotNil(t, view)
}

// eventCounter is a history-dependent projector: it counts the events a commit wrote, per stream,
// and owns each row by its stream.
type eventCounter struct{ namespace string }

func (eventCounter) Name() string { return "event-counter" }

func (eventCounter) History() []evt.EntityType { return []evt.EntityType{tallyType} }

func (e eventCounter) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
	operations := make([]evt.ViewOperation, 0, len(commit.Streams))

	for _, stream := range commit.Streams {
		operations = append(operations, evt.PutView(
			e.namespace+"#events", string(stream.EntityID), stream.EntityID, stream.EntityType,
			fmt.Appendf(nil, `{"events":%d,"last":%d}`, len(stream.Events), stream.Events[len(stream.Events)-1].Sequence),
		).OwnedBy(stream.EntityID))
	}

	return operations, nil
}

func testProjectorsDeclareHistory(t *testing.T, newRepo func() evt.Repository, read ViewReader) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	repo := newRepo()
	id := evt.EntityID(ns + "-a")
	store := snapshots.NewStore(repo, 1).WithCommitProjectors(eventCounter{namespace: ns})

	require.NoError(t, store.Execute(ctx, &tally{}, id, &tallyUp{By: 1}, tallyMetadata(ns+"-one")))
	require.NoError(t, store.Execute(ctx, &tally{}, id, &tallyUp{By: 1}, tallyMetadata(ns+"-two")))

	view, err := read(ctx, repo, ns+"#events", string(id))
	require.NoError(t, err)
	require.NotNil(t, view)
	require.JSONEq(t, `{"events":1,"last":2}`, string(view.Payload), "the projector saw its type's events")

	// The registration declared the type durably, before the first commit, so a compaction from any
	// process is refused: the commits a rebuild replays stay.
	compactor, ok := repo.(evt.Compactor)
	if !ok {
		t.Skip("repository does not compact")
	}

	_, err = compactor.CompactBelow(ctx, id, 1)
	require.True(t, evt.IsHistoryDependentProjectionErr(err), "got %v", err)

	stored, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Len(t, stored, 2, "nothing was deleted")
}
