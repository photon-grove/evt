package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
	"github.com/photon-grove/evt/snapshots"
	"github.com/photon-grove/evt/test"
)

// RunOutcomeSuite verifies command outcomes (ADR 0004, C3/C4) on a repository that implements
// evt.OutcomeRepository: duplicate replay, key reuse, rejection persistence, the accepted no-op,
// the stored-value bounds, ambiguous-write recovery, the outcome-put race, stale-fence
// persistence, and the commit budget with the outcome item. newRepo must return a handle on a
// fresh (or namespace-isolated) repository, as for RunRepositorySuite. enforcesConcurrency says
// whether the backend rejects colliding event sequences (the in-memory double does not).
func RunOutcomeSuite(t *testing.T, newRepo func() evt.Repository, enforcesConcurrency bool) {
	t.Helper()

	outcomes := func(t *testing.T) evt.OutcomeRepository {
		t.Helper()

		repo, err := evt.RequireOutcomes(newRepo())
		require.NoError(t, err, "a backend claiming outcomes must implement evt.OutcomeRepository")

		return repo
	}

	t.Run("DuplicateReplay", func(t *testing.T) { testDuplicateReplay(t, outcomes(t)) })
	t.Run("DedupeAcrossSnapshots", func(t *testing.T) { testDedupeAcrossSnapshots(t, outcomes(t)) })
	t.Run("KeyReuse", func(t *testing.T) { testKeyReuse(t, outcomes(t)) })
	t.Run("RejectionPersistence", func(t *testing.T) { testRejectionPersistence(t, outcomes(t)) })
	t.Run("InternalFailuresWriteNoOutcome", func(t *testing.T) { testInternalFailures(t, outcomes(t)) })
	t.Run("AcceptedNoOp", func(t *testing.T) { testAcceptedNoOp(t, outcomes(t)) })
	t.Run("OutcomeTooLarge", func(t *testing.T) { testOutcomeTooLarge(t, outcomes(t)) })
	t.Run("CommandIDRules", func(t *testing.T) { testCommandIDRules(t, outcomes(t)) })
	t.Run("AmbiguousWriteRecovery", func(t *testing.T) { testAmbiguousRecovery(t, outcomes(t)) })
	t.Run("OutcomePutRace", func(t *testing.T) { testOutcomePutRace(t, outcomes(t)) })
	t.Run("TransactionOutcomes", func(t *testing.T) { testTransactionOutcomes(t, outcomes(t)) })
	t.Run("StaleFencePersistence", func(t *testing.T) { testStaleFencePersistence(t, outcomes(t)) })
	t.Run("OutcomeBudget", func(t *testing.T) { testOutcomeBudget(t, outcomes(t)) })
	t.Run("RejectsReservedOutcomeStreams", func(t *testing.T) { testOutcomeStreamsAreReserved(t, outcomes(t)) })

	if enforcesConcurrency {
		t.Run("ConflictLeavesNoOutcome", func(t *testing.T) { testConflictLeavesNoOutcome(t, outcomes(t)) })
	}
}

// --- the suite's aggregate ---

const ledgerType evt.EntityType = "conformance-ledger"

// ledger is a tiny balance aggregate whose commands exercise every outcome path.
type ledger struct {
	id    evt.EntityID
	Total int `json:"total"`
}

func newLedger(id evt.EntityID) *ledger { return &ledger{id: id} }

func (l *ledger) Type() evt.EntityType { return ledgerType }
func (l *ledger) GetID() evt.EntityID  { return l.id }
func (l *ledger) SetID(id evt.EntityID) {
	l.id = id
}
func (l *ledger) Base() evt.BaseEntity { //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	return evt.BaseEntity{ID: l.id} //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
}
func (l *ledger) EventUpcasters() []evt.EventUpcaster { return nil }
func (l *ledger) Projectors() []evt.EventProjector    { return nil }

type ledgerEvent struct {
	Kind   string `json:"kind"`
	Amount int    `json:"amount"`
}

func (e *ledgerEvent) Type() evt.EventType        { return evt.EventType("ledger:" + e.Kind) }
func (e *ledgerEvent) Version() evt.EventVersion  { return 1 }
func (e *ledgerEvent) EntityType() evt.EntityType { return ledgerType }
func (e *ledgerEvent) EntityID() evt.EntityID     { return "" }
func (l *ledger) Apply(event evt.Event) error {
	changed, ok := event.(*ledgerEvent)
	if !ok {
		return evt.NewBadEventError(event)
	}

	if changed.Kind == "withdrawn" {
		l.Total -= changed.Amount
	} else {
		l.Total += changed.Amount
	}

	return nil
}

func (l *ledger) DeserializeEvent(serialized evt.SerializedEvent) (evt.Event, error) {
	var event ledgerEvent
	if err := json.Unmarshal(serialized.Payload, &event); err != nil {
		return nil, err
	}

	return &event, nil
}

// domainRejection is a domain error that maps onto a persisted rejection.
type domainRejection struct{ rejection evt.Rejection }

func (e *domainRejection) Error() string            { return e.rejection.Message }
func (e *domainRejection) Rejection() evt.Rejection { return e.rejection }

// ledgerCommand selects the behavior under test through Kind.
type ledgerCommand struct {
	Kind   string `json:"kind"`
	Amount int    `json:"amount"`

	// Size is the length of the response (kind "big") or rejection message (kind "bigreject").
	Size int `json:"size,omitempty"`

	// ran counts handler invocations; it is not part of the command's identity.
	ran *atomic.Int32
}

func (c *ledgerCommand) Type() evt.CommandType      { return evt.CommandType("ledger:" + c.Kind) }
func (c *ledgerCommand) EntityType() evt.EntityType { return ledgerType }

func (l *ledger) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
	c, ok := command.(*ledgerCommand)
	if !ok {
		return evt.CommandResult{}, evt.NewBadCommandError(command)
	}

	if c.ran != nil {
		c.ran.Add(1)
	}

	total := func(delta int) []byte { return fmt.Appendf(nil, `{"total":%d}`, l.Total+delta) }

	switch c.Kind {
	case "deposit":
		if c.Amount <= 0 {
			return evt.CommandResult{}, &domainRejection{evt.Rejection{Code: "invalid-amount", Message: "amount must be positive"}}
		}

		return evt.CommandResult{
			Events:   []evt.Event{&ledgerEvent{Kind: "deposited", Amount: c.Amount}},
			Response: total(c.Amount),
		}, nil

	case "withdraw":
		if c.Amount > l.Total {
			return evt.CommandResult{}, &domainRejection{evt.Rejection{
				Code: "insufficient-funds", Message: "balance is too low",
				Details: map[string]any{"balance": l.Total, "requested": c.Amount},
			}}
		}

		return evt.CommandResult{
			Events:   []evt.Event{&ledgerEvent{Kind: "withdrawn", Amount: c.Amount}},
			Response: total(-c.Amount),
		}, nil

	case "check":
		return evt.CommandResult{Response: total(0)}, nil

	case "noop":
		return evt.CommandResult{}, evt.NewNoOpError("nothing to do")

	case "missing":
		return evt.CommandResult{}, evt.NewNotFoundError("ledger not found")

	case "broken":
		return evt.CommandResult{}, errors.New("storage exploded")

	case "big":
		return evt.CommandResult{
			Events:   []evt.Event{&ledgerEvent{Kind: "deposited", Amount: 1}},
			Response: make([]byte, c.Size),
		}, nil

	case "bigreject":
		return evt.CommandResult{}, &domainRejection{evt.Rejection{Code: "too-big", Message: strings.Repeat("x", c.Size)}}
	}

	return evt.CommandResult{}, evt.NewBadCommandError(command)
}

// --- helpers ---

type outcomeEnv struct {
	repo  evt.OutcomeRepository
	store *snapshots.Store
	id    evt.EntityID
	ns    string
}

func newOutcomeEnv(t *testing.T, repo evt.OutcomeRepository, snapshotSize int) outcomeEnv {
	t.Helper()

	ns := newNamespace()

	return outcomeEnv{
		repo:  repo,
		store: snapshots.NewStore(repo, snapshotSize).WithOutcomes(),
		id:    evt.EntityID(ns + "-ledger"),
		ns:    ns,
	}
}

func (e outcomeEnv) commandID(name string) evt.CommandID { return evt.CommandID(e.ns + "-" + name) }

func withCommand(id evt.CommandID) evt.Metadata {
	return evt.Metadata{CommandID: &id, Region: "us-west-2", Timestamp: "2026-10-12T00:00:00Z"}
}

func (e outcomeEnv) run(
	t *testing.T,
	cmd *ledgerCommand,
	id evt.CommandID,
) (*evt.CommandOutcome, error) {
	t.Helper()

	return e.store.ExecuteWithOutcome(context.Background(), newLedger(e.id), e.id, cmd, withCommand(id))
}

func (e outcomeEnv) events(t *testing.T, id evt.EntityID) []evt.SerializedEvent {
	t.Helper()

	events, err := e.repo.GetEvents(context.Background(), id)
	require.NoError(t, err)

	return events
}

func (e outcomeEnv) requireNoOutcome(t *testing.T, id evt.CommandID) {
	t.Helper()

	stored, err := e.repo.GetOutcome(context.Background(), id)
	require.NoError(t, err)
	require.Nil(t, stored, "no outcome row may exist for %s", id)
}

func counter() *atomic.Int32 { return &atomic.Int32{} }

// --- cases ---

func testDuplicateReplay(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)
	id := env.commandID("deposit")
	ran := counter()

	first, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 5, ran: ran}, id)
	require.NoError(t, err)
	require.True(t, first.Accepted())
	require.False(t, first.Replayed)
	require.JSONEq(t, `{"total":5}`, string(first.Response))
	require.Equal(t, []evt.EventID{evt.GetEventID(env.id, 1)}, first.EventIDs)
	require.Len(t, first.Events, 1)
	require.NotEmpty(t, first.Digest)

	second, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 5, ran: ran}, id)
	require.NoError(t, err)
	require.True(t, second.Replayed, "the stored outcome is returned for the same digest")
	require.Equal(t, first.Outcome, second.Outcome, "the replay returns the stored response, events, and digest")
	require.Empty(t, second.Events)
	require.EqualValues(t, 1, ran.Load(), "a replay never runs the handler")
	require.Len(t, env.events(t, env.id), 1, "a replay writes nothing")

	stored, err := env.store.Outcome(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, first.Outcome, *stored)

	// The error-returning Execute treats an accepted replay as success.
	require.NoError(t, env.store.Execute(context.Background(), newLedger(env.id), env.id,
		&ledgerCommand{Kind: "deposit", Amount: 5, ran: ran}, withCommand(id)))
	require.Len(t, env.events(t, env.id), 1)
}

func testDedupeAcrossSnapshots(t *testing.T, repo evt.OutcomeRepository) {
	// A snapshot interval of 2 folds the first commands into snapshots; the old per-entity window
	// would stop seeing their IDs. The outcome row still deduplicates them.
	env := newOutcomeEnv(t, repo, 2)
	first := env.commandID("first")
	ran := counter()

	_, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1, ran: ran}, first)
	require.NoError(t, err)

	for i := range 6 {
		_, err = env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1}, env.commandID(fmt.Sprintf("later-%d", i)))
		require.NoError(t, err)
	}

	snapshot, err := env.repo.GetSnapshot(context.Background(), env.id)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.GreaterOrEqual(t, snapshot.EventSequence, evt.EventSequence(2), "the first command is folded into a snapshot")

	replayed, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1, ran: ran}, first)
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.EqualValues(t, 1, ran.Load())
	require.Len(t, env.events(t, env.id), 7, "the folded command ID is still deduplicated")
}

func testKeyReuse(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)
	id := env.commandID("shared")
	ran := counter()

	first, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 5, ran: ran}, id)
	require.NoError(t, err)

	// A different payload, a different command type, and a different principal each change the
	// identity digest.
	different := []struct {
		name string
		run  func() error
	}{
		{"payload", func() error {
			_, rerr := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 6, ran: ran}, id)
			return rerr
		}},
		{"type", func() error {
			_, rerr := env.run(t, &ledgerCommand{Kind: "withdraw", Amount: 1, ran: ran}, id)
			return rerr
		}},
		{"principal", func() error {
			principal := evt.PrincipalID("user:other")
			metadata := withCommand(id)
			metadata.PrincipalID = &principal
			_, rerr := env.store.ExecuteWithOutcome(context.Background(), newLedger(env.id), env.id,
				&ledgerCommand{Kind: "deposit", Amount: 5, ran: ran}, metadata)
			return rerr
		}},
	}

	for _, test := range different {
		err = test.run()
		require.ErrorIs(t, err, evt.ErrIdempotencyKeyReuse, test.name)

		var reuse *evt.IdempotencyKeyReuseError
		require.ErrorAs(t, err, &reuse)
		require.Equal(t, first.Outcome, reuse.Prior, "the error carries the prior outcome")
	}

	require.EqualValues(t, 1, ran.Load(), "a reused key never runs the handler")
	require.Len(t, env.events(t, env.id), 1)

	stored, err := env.repo.GetOutcome(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, first.Outcome, *stored, "the reuse error is not stored: the first command keeps the row")
}

func testRejectionPersistence(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)
	ctx := context.Background()
	ran := counter()

	_, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 3}, env.commandID("seed"))
	require.NoError(t, err)

	id := env.commandID("withdraw")
	first, err := env.run(t, &ledgerCommand{Kind: "withdraw", Amount: 10, ran: ran}, id)
	require.NoError(t, err, "a domain rejection is an outcome, not an error")
	require.True(t, first.Rejected())
	require.False(t, first.Replayed)
	require.Empty(t, first.EventIDs)
	require.Empty(t, first.Events)
	require.Equal(t, evt.RejectionCode("insufficient-funds"), first.Rejection.Code)
	require.EqualValues(t, 1, ran.Load())

	stored, err := env.repo.GetOutcome(ctx, id)
	require.NoError(t, err)
	require.True(t, stored.Rejected())

	encoded, err := evt.EncodeRejection(*stored.Rejection)
	require.NoError(t, err)
	require.JSONEq(t, `{"code":"insufficient-funds","message":"balance is too low","details":{"balance":3,"requested":10}}`, string(encoded))

	// The retried command replays the original rejection verbatim, without running the handler,
	// even though the aggregate has since changed.
	_, err = env.run(t, &ledgerCommand{Kind: "deposit", Amount: 100}, env.commandID("fund"))
	require.NoError(t, err)

	replayed, err := env.run(t, &ledgerCommand{Kind: "withdraw", Amount: 10, ran: ran}, id)
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.True(t, replayed.Rejected())
	require.Equal(t, stored.Rejection, replayed.Rejection)
	require.EqualValues(t, 1, ran.Load())
	require.Len(t, env.events(t, env.id), 2, "a rejection writes no events")

	// The error-returning Execute reports the rejection on first execution and on every replay.
	err = env.store.Execute(ctx, newLedger(env.id), env.id, &ledgerCommand{Kind: "withdraw", Amount: 10}, withCommand(id))
	var rejected *evt.RejectionError
	require.ErrorAs(t, err, &rejected)
	require.ErrorIs(t, err, evt.ErrRejected)
	require.Equal(t, evt.RejectionCode("insufficient-funds"), rejected.Rejection.Code)

	// Engine codes: not-found is a persisted rejection too.
	missing, err := env.run(t, &ledgerCommand{Kind: "missing"}, env.commandID("missing"))
	require.NoError(t, err)
	require.Equal(t, evt.RejectionNotFound, missing.Rejection.Code)
	require.Equal(t, "ledger not found", missing.Rejection.Message)
}

func testInternalFailures(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)

	// An internal failure is returned as an error and records nothing, so a retry after the
	// underlying bug is fixed starts clean instead of replaying a durable rejection forever.
	broken := env.commandID("broken")
	_, err := env.run(t, &ledgerCommand{Kind: "broken"}, broken)
	require.Error(t, err)
	env.requireNoOutcome(t, broken)

	// A bad event is an internal failure too, never a rejection.
	_, ok := evt.RejectionOf(evt.NewBadEventError(&ledgerEvent{}))
	require.False(t, ok)

	// An unrecognized command is the engine's bad-command rejection: durable.
	unknown := env.commandID("unknown")
	outcome, err := env.run(t, &ledgerCommand{Kind: "unknown"}, unknown)
	require.NoError(t, err)
	require.True(t, outcome.Rejected())
	require.Equal(t, evt.RejectionBadCommand, outcome.Rejection.Code)
}

func testAcceptedNoOp(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)
	ctx := context.Background()

	_, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 4}, env.commandID("seed"))
	require.NoError(t, err)

	for _, kind := range []string{"check", "noop"} {
		id := env.commandID(kind)

		first, err := env.run(t, &ledgerCommand{Kind: kind}, id)
		require.NoError(t, err, kind)
		require.True(t, first.Accepted(), kind)
		require.True(t, first.NoOp(), kind)
		require.Empty(t, first.EventIDs)

		if kind == "check" {
			require.JSONEq(t, `{"total":4}`, string(first.Response), "the response of a no-op is stored")
		}

		stored, err := env.repo.GetOutcome(ctx, id)
		require.NoError(t, err)
		require.True(t, stored.NoOp())
		require.Equal(t, first.Response, stored.Response)

		replayed, err := env.run(t, &ledgerCommand{Kind: kind}, id)
		require.NoError(t, err)
		require.True(t, replayed.Replayed)
		require.True(t, replayed.NoOp())
	}

	require.Len(t, env.events(t, env.id), 1, "an accepted no-op writes no events")
}

func testOutcomeTooLarge(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)

	id := env.commandID("big")
	_, err := env.run(t, &ledgerCommand{Kind: "big", Size: evt.MaxOutcomeBytes + 1}, id)

	var tooLarge *evt.OutcomeTooLargeError
	require.ErrorAs(t, err, &tooLarge)
	require.Equal(t, "response", tooLarge.What)
	require.Empty(t, env.events(t, env.id), "an oversized response fails before any write")
	env.requireNoOutcome(t, id)

	exact := env.commandID("exact")
	outcome, err := env.run(t, &ledgerCommand{Kind: "big", Size: evt.MaxOutcomeBytes}, exact)
	require.NoError(t, err)
	require.Len(t, outcome.Response, evt.MaxOutcomeBytes)

	stored, err := env.repo.GetOutcome(context.Background(), exact)
	require.NoError(t, err)
	require.Len(t, stored.Response, evt.MaxOutcomeBytes, "a response of arbitrary bytes round-trips exactly")

	reject := env.commandID("bigreject")
	_, err = env.run(t, &ledgerCommand{Kind: "bigreject", Size: evt.MaxOutcomeBytes}, reject)
	require.ErrorAs(t, err, &tooLarge, "the bound applies to the encoded rejection, message and details included")
	require.Equal(t, "rejection", tooLarge.What)
	env.requireNoOutcome(t, reject)
}

func testCommandIDRules(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)
	ran := counter()

	long := evt.CommandID(strings.Repeat("a", evt.MaxCommandIDBytes+1))
	_, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1, ran: ran}, long)

	var tooLong *evt.CommandIDTooLongError
	require.ErrorAs(t, err, &tooLong)
	require.EqualValues(t, 0, ran.Load(), "a bad command ID fails before the handler runs")
	require.Empty(t, env.events(t, env.id))

	_, err = env.store.ExecuteWithOutcome(context.Background(), newLedger(env.id), env.id,
		&ledgerCommand{Kind: "deposit", Amount: 1, ran: ran}, evt.Metadata{})
	require.ErrorIs(t, err, evt.ErrInvalidArgument, "a command ID is required when outcomes are enabled")
	require.Empty(t, env.events(t, env.id))

	exact := evt.CommandID(env.ns + strings.Repeat("b", evt.MaxCommandIDBytes-len(env.ns)))
	_, err = env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1}, exact)
	require.NoError(t, err, "an ID of exactly 512 bytes is accepted")
}

func testAmbiguousRecovery(t *testing.T, repo evt.OutcomeRepository) {
	faulty, err := test.NewFaultRepository(repo)
	require.NoError(t, err)

	env := newOutcomeEnv(t, faulty, 0)
	ran := counter()
	cause := errors.New("connection reset by peer")

	// The commit applied; the caller saw an indeterminate error. The re-read returns the stored
	// outcome, and the handler is not run again.
	applied := env.commandID("applied")
	faulty.ApplyThenFail(cause)

	outcome, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 7, ran: ran}, applied)
	require.NoError(t, err)
	require.True(t, outcome.Replayed, "recovery returns the stored outcome")
	require.True(t, outcome.Accepted())
	require.JSONEq(t, `{"total":7}`, string(outcome.Response))
	require.EqualValues(t, 1, ran.Load())
	require.Len(t, env.events(t, env.id), 1, "the applied commit wrote its events exactly once")

	// A recovered rejection is the stored rejection.
	rejected := env.commandID("rejected")
	faulty.ApplyThenFail(cause)

	outcome, err = env.run(t, &ledgerCommand{Kind: "withdraw", Amount: 100}, rejected)
	require.NoError(t, err)
	require.True(t, outcome.Replayed)
	require.True(t, outcome.Rejected())

	// The commit did not apply: the row is absent, so retrying the same command ID is safe.
	missing := env.commandID("missing-row")
	faulty.FailWithoutApplying(cause)

	_, err = env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1, ran: ran}, missing)
	require.ErrorIs(t, err, evt.ErrAmbiguousWrite)
	require.ErrorIs(t, err, cause)
	require.True(t, policy.IsTransient(err), "a commit proven not to have applied is retryable")

	var ambiguous *evt.AmbiguousWriteError
	require.ErrorAs(t, err, &ambiguous)
	require.True(t, ambiguous.NotApplied)
	env.requireNoOutcome(t, missing)

	retried, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1, ran: ran}, missing)
	require.NoError(t, err)
	require.False(t, retried.Replayed)
	require.Len(t, env.events(t, env.id), 2)

	// Another command claimed the ID between the pre-check and the commit: its row has a different
	// digest, so recovery reports IdempotencyKeyReuse.
	contested := env.commandID("contested")
	other := newOutcomeEnv(t, repo, 0)
	other.id, other.ns = env.id, env.ns

	faulty.Intercept = func(_ context.Context, _ evt.SerializedResult, _ func() error) error {
		faulty.Intercept = nil

		_, rerr := other.run(t, &ledgerCommand{Kind: "deposit", Amount: 50}, contested)
		require.NoError(t, rerr)

		return &evt.AmbiguousWriteError{CommandID: contested, Cause: cause}
	}

	_, err = env.run(t, &ledgerCommand{Kind: "deposit", Amount: 9}, contested)
	require.ErrorIs(t, err, evt.ErrIdempotencyKeyReuse)
}

func testOutcomePutRace(t *testing.T, repo evt.OutcomeRepository) {
	faulty, err := test.NewFaultRepository(repo)
	require.NoError(t, err)

	env := newOutcomeEnv(t, faulty, 0)
	rival := newOutcomeEnv(t, repo, 0)
	rival.id, rival.ns = env.id, env.ns

	// The rival commits the same command between this writer's pre-check and its commit. The lost
	// outcome put (or the collided events) is resolved by reading the winner's row, not by
	// re-preparing the command.
	same := env.commandID("same")
	raced := false
	faulty.Intercept = func(_ context.Context, _ evt.SerializedResult, commit func() error) error {
		faulty.Intercept = nil
		raced = true

		_, rerr := rival.run(t, &ledgerCommand{Kind: "deposit", Amount: 2}, same)
		require.NoError(t, rerr)

		return commit()
	}

	outcome, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 2}, same)
	require.True(t, raced)
	require.NoError(t, err)
	require.True(t, outcome.Replayed, "the loser returns the winner's stored outcome")
	require.False(t, errors.Is(err, evt.ErrOutcomeConflict))
	require.Len(t, env.events(t, env.id), 1, "the command applied exactly once")

	// A rival with a different digest wins: the loser gets IdempotencyKeyReuse.
	differing := env.commandID("differing")
	faulty.Intercept = func(_ context.Context, _ evt.SerializedResult, commit func() error) error {
		faulty.Intercept = nil

		_, rerr := rival.run(t, &ledgerCommand{Kind: "deposit", Amount: 3}, differing)
		require.NoError(t, rerr)

		return commit()
	}

	_, err = env.run(t, &ledgerCommand{Kind: "deposit", Amount: 4}, differing)
	require.ErrorIs(t, err, evt.ErrIdempotencyKeyReuse)
	require.Len(t, env.events(t, env.id), 2)
}

func testConflictLeavesNoOutcome(t *testing.T, repo evt.OutcomeRepository) {
	faulty, err := test.NewFaultRepository(repo)
	require.NoError(t, err)

	env := newOutcomeEnv(t, faulty, 0)
	rival := newOutcomeEnv(t, repo, 0)
	rival.id, rival.ns = env.id, env.ns

	// Another command appends between this writer's load and its commit: the event put fails, the
	// whole transaction rolls back, and no outcome row remains.
	id := env.commandID("loser")
	faulty.Intercept = func(_ context.Context, _ evt.SerializedResult, commit func() error) error {
		faulty.Intercept = nil

		_, rerr := rival.run(t, &ledgerCommand{Kind: "deposit", Amount: 1}, env.commandID("winner"))
		require.NoError(t, rerr)

		return commit()
	}

	_, err = env.run(t, &ledgerCommand{Kind: "deposit", Amount: 2}, id)
	require.Error(t, err)
	require.True(t, evt.IsConflictErr(err) || policy.IsTransient(err), "an event collision is an ordinary conflict: %v", err)
	require.False(t, errors.Is(err, evt.ErrOutcomeConflict), "it is not an outcome conflict")
	env.requireNoOutcome(t, id)
	require.Len(t, env.events(t, env.id), 1)

	// The retry runs the command against the fresh state.
	retried, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 2}, id)
	require.NoError(t, err)
	require.JSONEq(t, `{"total":3}`, string(retried.Response))
}

func testTransactionOutcomes(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)
	ctx := context.Background()
	other := evt.EntityID(env.ns + "-aaa-first")

	identity := evt.CommandIdentity{
		Type: "ledger:transfer", Streams: []evt.EntityID{env.id, other}, Payload: map[string]int{"amount": 5},
	}
	deposited := &ledgerEvent{Kind: "deposited", Amount: 5}

	// A multi-stream commit records the event IDs in canonical order: stream ID bytewise, then
	// sequence, whatever order the command read or appended in.
	id := env.commandID("transfer")
	tx, replayed, err := env.store.BeginCommand(ctx, identity, withCommand(id))
	require.NoError(t, err)
	require.Nil(t, replayed)
	require.NoError(t, tx.Read(ctx, newLedger(env.id), env.id))
	require.NoError(t, tx.Read(ctx, newLedger(other), other))
	require.NoError(t, tx.Append(env.id, deposited))
	require.NoError(t, tx.Append(other, deposited))

	outcome, err := tx.Complete(ctx, []byte("done"))
	require.NoError(t, err)
	require.Equal(t, []evt.EventID{evt.GetEventID(other, 1), evt.GetEventID(env.id, 1)}, outcome.EventIDs)
	require.Equal(t, []byte("done"), outcome.Response)

	// The same command replays from BeginCommand without loading anything.
	tx, replayed, err = env.store.BeginCommand(ctx, identity, withCommand(id))
	require.NoError(t, err)
	require.Nil(t, tx)
	require.True(t, replayed.Replayed)
	require.Equal(t, outcome.Outcome, replayed.Outcome)

	// A different identity under the same ID is a reuse.
	identity.Payload = map[string]int{"amount": 6}
	_, _, err = env.store.BeginCommand(ctx, identity, withCommand(id))
	require.ErrorIs(t, err, evt.ErrIdempotencyKeyReuse)
	identity.Payload = map[string]int{"amount": 5}

	// An accepted no-op still checks the fences of its reads: it succeeds while the read stream is
	// unchanged, and fails with an ordinary (retryable) fence conflict, recording nothing, once the
	// stream has moved.
	noop := env.commandID("noop")
	tx, _, err = env.store.BeginCommand(ctx, evt.CommandIdentity{Type: "ledger:peek", Streams: nil}, withCommand(noop))
	require.NoError(t, err)
	require.NoError(t, tx.Read(ctx, newLedger(env.id), env.id))

	_, err = env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1}, env.commandID("mover"))
	require.NoError(t, err)

	_, err = tx.Complete(ctx, nil)

	var fence *evt.FenceConflictError
	require.ErrorAs(t, err, &fence)
	require.False(t, fence.Fence.CallerStamped)
	require.True(t, policy.IsTransient(err), "an engine-filled fence conflict is retryable")
	env.requireNoOutcome(t, noop)

	tx, _, err = env.store.BeginCommand(ctx, evt.CommandIdentity{Type: "ledger:peek"}, withCommand(noop))
	require.NoError(t, err)
	require.NoError(t, tx.Read(ctx, newLedger(env.id), env.id))

	outcome, err = tx.Complete(ctx, []byte("peeked"))
	require.NoError(t, err)
	require.True(t, outcome.NoOp())

	// A rejection is an outcome-only commit; the engine rejects only against state that still holds.
	rejection := evt.Rejection{Code: "username-taken", Message: "handle ada is taken", Details: map[string]any{"handle": "ada"}}
	reject := env.commandID("reject")
	tx, _, err = env.store.BeginCommand(ctx, evt.CommandIdentity{Type: "claim-username", Streams: []evt.EntityID{env.id}}, withCommand(reject))
	require.NoError(t, err)
	require.NoError(t, tx.Read(ctx, newLedger(env.id), env.id))

	outcome, err = tx.Reject(ctx, rejection)
	require.NoError(t, err)
	require.True(t, outcome.Rejected())
	require.Equal(t, rejection, *outcome.Rejection)

	stored, err := env.repo.GetOutcome(ctx, reject)
	require.NoError(t, err)
	require.Equal(t, rejection, *stored.Rejection)

	// A transaction not begun with BeginCommand cannot silently skip the outcome.
	plain := env.store.Transaction()
	require.NoError(t, plain.Read(ctx, newLedger(env.id), env.id))
	_, err = plain.Commit(ctx, withCommand(env.commandID("plain")))
	require.ErrorIs(t, err, evt.ErrInvalidArgument)
}

func testStaleFencePersistence(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)
	ctx := context.Background()
	referral := evt.EntityID(env.ns + "-referral")

	_, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1}, env.commandID("seed"))
	require.NoError(t, err)

	rival := snapshots.NewStore(repo, 0).WithOutcomes()
	moveReferral := func(name string) {
		_, merr := rival.ExecuteWithOutcome(ctx, newLedger(referral), referral,
			&ledgerCommand{Kind: "deposit", Amount: 1}, withCommand(env.commandID(name)))
		require.NoError(t, merr)
	}
	moveReferral("referral-1")

	identity := evt.CommandIdentity{Type: "ledger:rename", Streams: []evt.EntityID{env.id}, Payload: map[string]string{"name": "Grace"}}
	id := env.commandID("stale")

	begin := func(cmdID evt.CommandID, head evt.EventSequence) *snapshots.Transaction {
		tx, replayed, berr := env.store.BeginCommand(ctx, identity, withCommand(cmdID))
		require.NoError(t, berr)
		require.Nil(t, replayed)
		require.NoError(t, tx.Read(ctx, newLedger(env.id), env.id))
		require.NoError(t, tx.Append(env.id, &ledgerEvent{Kind: "deposited", Amount: 1}))
		require.NoError(t, tx.Fence(evt.ReadFence{EntityID: referral, EntityType: ledgerType, Head: head}))

		return tx
	}

	// The caller decided on the referral at head 1; another writer moves it before the commit.
	tx := begin(id, 1)
	moveReferral("referral-2")

	outcome, err := tx.Complete(ctx, nil)
	require.NoError(t, err, "a stale caller-stamped fence is a persisted rejection, not an error")
	require.True(t, outcome.Rejected())
	require.Equal(t, evt.RejectionStaleFence, outcome.Rejection.Code)
	require.Empty(t, outcome.EventIDs)
	require.Len(t, env.events(t, env.id), 1, "the failed commit wrote none of its events")

	// The old command ID is terminal: it replays the stored stale-fence rejection.
	tx, replayed, err := env.store.BeginCommand(ctx, identity, withCommand(id))
	require.NoError(t, err)
	require.Nil(t, tx)
	require.True(t, replayed.Replayed)
	require.Equal(t, evt.RejectionStaleFence, replayed.Rejection.Code)

	// The remedy is a new command ID decided on the fresh head.
	tx = begin(env.commandID("stale-retry"), 2)
	outcome, err = tx.Complete(ctx, nil)
	require.NoError(t, err)
	require.True(t, outcome.Accepted())
	require.Len(t, env.events(t, env.id), 2)
}

func testOutcomeBudget(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	ns := newNamespace()

	outcome := func(name string) *evt.Outcome {
		return &evt.Outcome{CommandID: evt.CommandID(ns + name), Status: evt.OutcomeAccepted, Digest: "digest"}
	}

	events := func(id evt.EntityID, from, count int) []evt.SerializedEvent {
		out := make([]evt.Event, 0, count)
		for range count {
			out = append(out, &ledgerEvent{Kind: "deposited", Amount: 1})
		}

		serialized, err := evt.SerializeEvents(out, evt.EventSequence(from), id, evt.Metadata{})
		require.NoError(t, err)

		for i := range serialized {
			serialized[i].EntityType = ledgerType
		}

		return serialized
	}

	// The feature-marker check, 97 event puts, the floor check, and the outcome put are 100 actions.
	fits := evt.EntityID(ns + "-fits")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: events(fits, 0, 97), Outcome: outcome("-fits")}))

	stored, err := repo.GetOutcome(ctx, evt.CommandID(ns+"-fits"))
	require.NoError(t, err)
	require.Len(t, stored.EventIDs, 97)

	over := evt.EntityID(ns + "-over")
	err = repo.Commit(ctx, evt.SerializedResult{Events: events(over, 0, 98), Outcome: outcome("-over")})

	var tooLarge *evt.CommitTooLargeError
	require.ErrorAs(t, err, &tooLarge, "the outcome item counts against the 100-action budget")
	require.Equal(t, 101, tooLarge.Items)
	require.Empty(t, mustEvents(t, repo, over), "an oversized commit is refused before any write")

	missing, err := repo.GetOutcome(ctx, evt.CommandID(ns+"-over"))
	require.NoError(t, err)
	require.Nil(t, missing)

	// Appending to an existing stream adds a continuation check: 96 events fit with the outcome.
	grows := evt.EntityID(ns + "-grows")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: events(grows, 0, 1)}))
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: events(grows, 1, 96), Outcome: outcome("-grows")}))

	growsOver := evt.EntityID(ns + "-grows-over")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: events(growsOver, 0, 1)}))
	require.ErrorAs(t,
		repo.Commit(ctx, evt.SerializedResult{Events: events(growsOver, 1, 97), Outcome: outcome("-grows-over")}),
		&tooLarge)

	// An outcome-only commit is the marker check plus the outcome put.
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Outcome: outcome("-only")}))

	// A response at the 64 KiB bound still fits the byte budgets.
	large := outcome("-large")
	large.Response = make([]byte, evt.MaxOutcomeBytes)
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Outcome: large}))
}

func mustEvents(t *testing.T, repo evt.Repository, id evt.EntityID) []evt.SerializedEvent {
	t.Helper()

	events, err := repo.GetEvents(context.Background(), id)
	require.NoError(t, err)

	return events
}

func testOutcomeStreamsAreReserved(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	ns := newNamespace()

	// Engine-written outcome rows are the one exemption from the reserved namespace; an entity
	// stream under the prefix is still refused.
	id := evt.EntityID(evt.CommandOutcomeNamespacePrefix + ns)
	events, err := evt.SerializeEvents([]evt.Event{&ledgerEvent{Kind: "deposited", Amount: 1}}, 0, id, evt.Metadata{})
	require.NoError(t, err)

	err = repo.Commit(ctx, evt.SerializedResult{Events: events})
	require.True(t, evt.IsReservedEntityIdErr(err), "got %v", err)

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Outcome: &evt.Outcome{CommandID: evt.CommandID(ns), Status: evt.OutcomeAccepted, Digest: "d"},
	}))
}
