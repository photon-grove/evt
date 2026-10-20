package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"

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
	t.Run("RejectionNumbersReplayVerbatim", func(t *testing.T) { testRejectionNumbers(t, outcomes(t)) })
	t.Run("InternalFailuresWriteNoOutcome", func(t *testing.T) { testInternalFailures(t, outcomes(t)) })
	t.Run("AcceptedNoOp", func(t *testing.T) { testAcceptedNoOp(t, outcomes(t)) })
	t.Run("OutcomeTooLarge", func(t *testing.T) { testOutcomeTooLarge(t, outcomes(t)) })
	t.Run("CommandIDRules", func(t *testing.T) { testCommandIDRules(t, outcomes(t)) })
	t.Run("AmbiguousWriteRecovery", func(t *testing.T) { testAmbiguousRecovery(t, outcomes(t)) })
	t.Run("OutcomePutRace", func(t *testing.T) { testOutcomePutRace(t, outcomes(t)) })
	t.Run("TransactionOutcomes", func(t *testing.T) { testTransactionOutcomes(t, outcomes(t)) })
	t.Run("StaleFencePersistence", func(t *testing.T) { testStaleFencePersistence(t, outcomes(t)) })
	t.Run("OutcomeBudget", func(t *testing.T) { testOutcomeBudget(t, outcomes(t)) })
	t.Run("WriteSetIsNotIdentity", func(t *testing.T) { testWriteSetIsNotIdentity(t, outcomes(t)) })
	t.Run("EngineFilledConflictRePrepares", func(t *testing.T) { testEngineFilledRePrepare(t, outcomes(t)) })
	t.Run("CallerFencedConflictFailsFast", func(t *testing.T) { testCallerFencedFailsFast(t, outcomes(t), enforcesConcurrency) })
	t.Run("ExhaustedRetriesPersistStaleRejection", func(t *testing.T) { testExhaustedRetries(t, outcomes(t)) })
	t.Run("ReservedWriteIsStillRefused", func(t *testing.T) { testReservedWriteRefused(t, outcomes(t)) })
	t.Run("WriteSetChangesBetweenAttempts", func(t *testing.T) { testWriteSetChanges(t, outcomes(t)) })
	t.Run("ResponseIsCopied", func(t *testing.T) { testResponseCopied(t, outcomes(t)) })
	t.Run("NULIsRefusedBeforeAnyWrite", func(t *testing.T) { testNULRefused(t, outcomes(t)) })
	t.Run("ReservedTargetStreams", func(t *testing.T) { testReservedTargets(t, outcomes(t)) })
	t.Run("OutcomeReadValidatesCommandIDs", func(t *testing.T) { testOutcomeReadValidation(t, outcomes(t)) })
	t.Run("ReplayRehydratesTheEntity", func(t *testing.T) { testReplayRehydrates(t, outcomes(t)) })
	t.Run("FenceRaceResolvesToTheWinner", func(t *testing.T) { testFenceRace(t, outcomes(t)) })
	t.Run("NonCanonicalEventIDsWriteNothing", func(t *testing.T) { testNonCanonicalEventIDs(t, outcomes(t)) })
	t.Run("RejectionSupersedesCallerFences", func(t *testing.T) { testRejectionFenceOrdering(t, outcomes(t)) })
	t.Run("LegacyCommandIDWindow", func(t *testing.T) { testLegacyCommandIDWindow(t, outcomes(t)) })
	t.Run("RejectsReservedOutcomeStreams", func(t *testing.T) { testOutcomeStreamsAreReserved(t, outcomes(t)) })
	t.Run("RejectionWithEvents", func(t *testing.T) { testRejectionWithEvents(t, outcomes(t)) })
	t.Run("RejectionWithEventsInATransaction", func(t *testing.T) { testRejectionWithEventsTransaction(t, outcomes(t)) })
	t.Run("RejectionWithEventsCommitRules", func(t *testing.T) { testRejectionWithEventsCommitRules(t, outcomes(t), enforcesConcurrency) })

	if enforcesConcurrency {
		t.Run("ConflictLeavesNoOutcome", func(t *testing.T) { testConflictLeavesNoOutcome(t, outcomes(t)) })
		t.Run("SingleAggregateRetryRePrepares", func(t *testing.T) { testSingleAggregateRetry(t, outcomes(t)) })
		t.Run("RejectionWithEventsRePreparesAndExhausts", func(t *testing.T) { testRejectionWithEventsRetry(t, outcomes(t)) })
		t.Run("RejectionWithEventsAndAStaleCallerFence", func(t *testing.T) { testRejectionWithEventsCallerFence(t, outcomes(t)) })
	}
}

// --- the suite's aggregate ---

const ledgerType evt.EntityType = "conformance-ledger"

// ledger is a tiny balance aggregate whose commands exercise every outcome path.
type ledger struct {
	id    evt.EntityID
	Total int `json:"total"`

	// Seen counts the events applied by kind. The constructor initializes the map, like an
	// aggregate that keeps labels or per-key counters, so a replay must not leave it dirty.
	Seen map[string]int `json:"seen"`
}

func newLedger(id evt.EntityID) *ledger { return &ledger{id: id, Seen: map[string]int{}} }

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

	if l.Seen == nil {
		l.Seen = map[string]int{}
	}

	l.Seen[changed.Kind]++

	switch changed.Kind {
	case "withdrawn":
		l.Total -= changed.Amount
	case "flagged":
		// A failure record: it changes no balance.
	default:
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

	// read, when set, is a read of derived state the handler makes before deciding (kind
	// "read-derived"): its error is the handler's own. It is not part of the command's identity.
	read func(ctx context.Context) error
}

func (c *ledgerCommand) Type() evt.CommandType      { return evt.CommandType("ledger:" + c.Kind) }
func (c *ledgerCommand) EntityType() evt.EntityType { return ledgerType }

func (l *ledger) Handle(ctx context.Context, command evt.Command) (evt.CommandResult, error) {
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

	case "withdraw-flagged":
		if c.Amount > l.Total {
			// The refusal is recorded beside the rejection: the failed attempt is part of the history.
			return evt.CommandResult{
					Events:   []evt.Event{&ledgerEvent{Kind: "flagged", Amount: c.Amount}},
					Response: []byte("ignored with a rejection"),
				}, evt.RejectWithEvents(&domainRejection{evt.Rejection{
					Code: "insufficient-funds", Message: "balance is too low",
					Details: map[string]any{"balance": l.Total, "requested": c.Amount},
				}})
		}

		return evt.CommandResult{
			Events:   []evt.Event{&ledgerEvent{Kind: "withdrawn", Amount: c.Amount}},
			Response: total(-c.Amount),
		}, nil

	case "check":
		return evt.CommandResult{Response: total(0)}, nil

	case "read-derived":
		if err := c.read(ctx); err != nil {
			return evt.CommandResult{}, err
		}

		return evt.CommandResult{
			Events:   []evt.Event{&ledgerEvent{Kind: "deposited", Amount: c.Amount}},
			Response: total(c.Amount),
		}, nil

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

	case "numbers":
		return evt.CommandResult{}, &domainRejection{evt.Rejection{
			Code: "numbers", Message: "numbers in every form",
			Details: map[string]any{
				"big":     1e21,
				"huge":    6.805598656397509e25,
				"tiny":    1.5e-7,
				"negzero": math.Copysign(0, -1),
				"scaled":  json.Number("1.50e1"),
				"nested":  []any{1e2, map[string]any{"zero": json.Number("-0.0")}},
			},
		}}

	case "bigreject":
		return evt.CommandResult{}, &domainRejection{evt.Rejection{Code: "too-big", Message: strings.Repeat("x", c.Size)}}
	}

	return evt.CommandResult{}, evt.NewBadCommandError(command)
}

// recoveryWait is the ambiguous-write polling window the suite's stores use: long enough to catch
// a commit that lands a moment after the caller gave up, short enough to keep the suite quick.
const recoveryWait = 300 * time.Millisecond

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
		store: snapshots.NewStore(repo, snapshotSize).WithOutcomes(snapshots.WithRecoveryWait(recoveryWait)),
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
	require.Equal(t, first.Rejection, replayed.Rejection,
		"a first execution returns the canonical stored form, so it equals a replay on every backend")
	require.Equal(t, json.Number("3"), first.Rejection.Details["balance"], "numbers read back as json.Number")
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

// A rejection's numbers read back the same on every backend, so a first execution equals a replay
// even where the backend rewrites the stored text (PostgreSQL's jsonb prints numbers without an
// exponent and drops the sign of zero).
func testRejectionNumbers(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)
	id := env.commandID("numbers")

	first, err := env.run(t, &ledgerCommand{Kind: "numbers"}, id)
	require.NoError(t, err)
	require.True(t, first.Rejected())
	require.False(t, first.Replayed)

	replayed, err := env.run(t, &ledgerCommand{Kind: "numbers"}, id)
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, first.Rejection, replayed.Rejection, "a first execution equals a replay")

	stored, err := env.repo.GetOutcome(context.Background(), id)
	require.NoError(t, err)

	encoded, err := evt.EncodeRejection(*stored.Rejection)
	require.NoError(t, err)
	require.Equal(t,
		`{"code":"numbers","message":"numbers in every form","details":{"big":1000000000000000000000,"huge":68055986563975090000000000,"negzero":0,"nested":[100,{"zero":0.0}],"scaled":15.0,"tiny":0.00000015}}`,
		string(encoded), "every backend stores and returns the same numeric text")
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

	// One attempt: this case pins what a single attempt returns when the row never appears. Fence-
	// aware retry would run the command again on its own (see the retry cases).
	env := newOutcomeEnv(t, faulty, 0)
	env.store.WithRetry(snapshots.RetryPolicy{MaxAttempts: 1})

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

	// The commit lands a moment after the caller gave up: recovery polls the row for its window
	// and finds it, instead of calling the first miss "not applied".
	delayed := env.commandID("delayed")
	faulty.Intercept = func(_ context.Context, _ evt.SerializedResult, commit func() error) error {
		faulty.Intercept = nil

		go func() {
			time.Sleep(recoveryWait / 3)

			assert.NoError(t, commit())
		}()

		return &evt.AmbiguousWriteError{CommandID: delayed, Cause: cause}
	}

	outcome, err = env.run(t, &ledgerCommand{Kind: "deposit", Amount: 4, ran: ran}, delayed)
	require.NoError(t, err)
	require.True(t, outcome.Replayed, "the late commit is found by polling")
	require.EqualValues(t, 2, ran.Load(), "the handler ran once for the delayed command")

	// The commit never lands: the row stays absent through the whole window. That proves nothing —
	// a transaction can land after any single read — so the error is a retryable ambiguous write,
	// never "not applied". Retrying the same command ID is safe: every attempt reads the row first.
	missing := env.commandID("missing-row")
	faulty.FailWithoutApplying(cause)

	_, err = env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1, ran: ran}, missing)
	require.ErrorIs(t, err, evt.ErrAmbiguousWrite)
	require.ErrorIs(t, err, cause)
	require.True(t, policy.IsTransient(err), "an ambiguous write is retryable")
	env.requireNoOutcome(t, missing)

	retried, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1, ran: ran}, missing)
	require.NoError(t, err)
	require.False(t, retried.Replayed)
	require.Len(t, env.events(t, env.id), 3)

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

	// The loser returns ONLY the winner's stored outcome. Its own decision — here a rejection with
	// the balance it saw — is neither returned nor reachable through the error chain.
	rejectedID := env.commandID("rejected-race")
	_, err = env.run(t, &ledgerCommand{Kind: "deposit", Amount: 3}, env.commandID("seed-race"))
	require.NoError(t, err)

	faulty.Intercept = func(_ context.Context, _ evt.SerializedResult, commit func() error) error {
		faulty.Intercept = nil

		// The state moves without changing the decision, so the rival records a rejection whose
		// details differ from the loser's.
		_, rerr := rival.run(t, &ledgerCommand{Kind: "deposit", Amount: 1}, env.commandID("mover-race"))
		require.NoError(t, rerr)

		_, rerr = rival.run(t, &ledgerCommand{Kind: "withdraw", Amount: 1000}, rejectedID)
		require.NoError(t, rerr)

		return commit()
	}

	loser, err := env.run(t, &ledgerCommand{Kind: "withdraw", Amount: 1000}, rejectedID)
	require.NoError(t, err)
	require.True(t, loser.Replayed)
	require.True(t, loser.Rejected())
	// Earlier in this case the ledger reached 2, then 5 with the seed: the loser decided at 5 and the
	// winner at 6.
	require.Equal(t, "6", fmt.Sprint(loser.Rejection.Details["balance"]), "the winner's balance, not the 5 the loser saw")

	// The error-returning Execute attaches no local handler error to a replayed rejection.
	faulty.Intercept = func(_ context.Context, _ evt.SerializedResult, commit func() error) error {
		faulty.Intercept = nil

		_, rerr := rival.run(t, &ledgerCommand{Kind: "withdraw", Amount: 1000}, env.commandID("rejected-race-2"))
		require.NoError(t, rerr)

		return commit()
	}

	err = env.store.Execute(context.Background(), newLedger(env.id), env.id,
		&ledgerCommand{Kind: "withdraw", Amount: 1000}, withCommand(env.commandID("rejected-race-2")))

	var rejected *evt.RejectionError
	require.ErrorAs(t, err, &rejected)
	require.NoError(t, rejected.Err, "the loser's own handler error must not leak")
	require.Equal(t, evt.RejectionCode("insufficient-funds"), rejected.Rejection.Code)

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
	require.Len(t, env.events(t, env.id), 4, "the first race winner, the seed, the mover, and the rival; rejections write none")
}

func testConflictLeavesNoOutcome(t *testing.T, repo evt.OutcomeRepository) {
	faulty, err := test.NewFaultRepository(repo)
	require.NoError(t, err)

	env := newOutcomeEnv(t, faulty, 0)
	rival := newOutcomeEnv(t, repo, 0)
	rival.id, rival.ns = env.id, env.ns
	ctx := context.Background()

	// Another command appends between this writer's load and its commit: the event put fails, the
	// whole transaction rolls back, and no outcome row remains. A single attempt (Complete) shows
	// the raw conflict; the engine's own retry is covered by the retry cases.
	id := env.commandID("loser")
	identity := evt.CommandIdentity{Type: "ledger:deposit", Targets: []evt.EntityID{env.id}}

	tx, replayed, err := env.store.BeginCommand(ctx, identity, withCommand(id))
	require.NoError(t, err)
	require.Nil(t, replayed)
	require.NoError(t, tx.Read(ctx, newLedger(env.id), env.id))
	require.NoError(t, tx.Append(env.id, &ledgerEvent{Kind: "deposited", Amount: 2}))

	faulty.Intercept = func(_ context.Context, _ evt.SerializedResult, commit func() error) error {
		faulty.Intercept = nil

		_, rerr := rival.run(t, &ledgerCommand{Kind: "deposit", Amount: 1}, env.commandID("winner"))
		require.NoError(t, rerr)

		return commit()
	}

	_, err = tx.Complete(ctx, nil)
	require.Error(t, err)
	require.True(t, evt.IsOptimisticConflict(err), "an event collision is an ordinary conflict: %v", err)
	require.False(t, errors.Is(err, evt.ErrOutcomeConflict), "it is not an outcome conflict")
	env.requireNoOutcome(t, id)
	require.Len(t, env.events(t, env.id), 1)

	// The command ID is still free: the same command runs against the fresh state.
	retried, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 2}, id)
	require.NoError(t, err)
	require.JSONEq(t, `{"total":3}`, string(retried.Response))
}

func testTransactionOutcomes(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)
	ctx := context.Background()
	other := evt.EntityID(env.ns + "-aaa-first")

	identity := evt.CommandIdentity{
		Type: "ledger:transfer", Targets: []evt.EntityID{env.id, other}, Payload: map[string]int{"amount": 5},
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
	tx, _, err = env.store.BeginCommand(ctx, evt.CommandIdentity{Type: "ledger:peek", Targets: nil}, withCommand(noop))
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
	tx, _, err = env.store.BeginCommand(ctx, evt.CommandIdentity{Type: "claim-username", Targets: []evt.EntityID{env.id}}, withCommand(reject))
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

	identity := evt.CommandIdentity{Type: "ledger:rename", Targets: []evt.EntityID{env.id}, Payload: map[string]string{"name": "Grace"}}
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

	// The two control checks, the head update, 96 event puts, and the outcome put are 100 actions.
	fits := evt.EntityID(ns + "-fits")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: events(fits, 0, 96), Outcome: outcome("-fits")}))

	stored, err := repo.GetOutcome(ctx, evt.CommandID(ns+"-fits"))
	require.NoError(t, err)
	require.Len(t, stored.EventIDs, 96)

	over := evt.EntityID(ns + "-over")
	err = repo.Commit(ctx, evt.SerializedResult{Events: events(over, 0, 97), Outcome: outcome("-over")})

	var tooLarge *evt.CommitTooLargeError
	require.ErrorAs(t, err, &tooLarge, "the outcome item counts against the 100-action budget")
	require.Equal(t, 101, tooLarge.Items)
	require.Empty(t, mustEvents(t, repo, over), "an oversized commit is refused before any write")

	missing, err := repo.GetOutcome(ctx, evt.CommandID(ns+"-over"))
	require.NoError(t, err)
	require.Nil(t, missing)

	// Appending to an existing stream costs the same one head update: 96 events fit with the
	// outcome, and 97 do not.
	grows := evt.EntityID(ns + "-grows")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: events(grows, 0, 1)}))
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: events(grows, 1, 96), Outcome: outcome("-grows")}))

	growsOver := evt.EntityID(ns + "-grows-over")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: events(growsOver, 0, 1)}))
	require.ErrorAs(t,
		repo.Commit(ctx, evt.SerializedResult{Events: events(growsOver, 1, 97), Outcome: outcome("-grows-over")}),
		&tooLarge)

	// An outcome-only commit is the two control checks plus the outcome put.
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

// The winner of a race for the same command ID wrote a stream the loser fenced at the head it
// read. A backend that asserts fences before the outcome insert reports the stale fence, not the
// lost outcome put; the loser must still resolve to the winner's outcome (or key reuse).
func testFenceRace(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()

	faulty, err := test.NewFaultRepository(repo)
	require.NoError(t, err)

	env := newOutcomeEnv(t, faulty, 0)
	rival := snapshots.NewStore(repo, 0).WithOutcomes()

	for _, sameIdentity := range []bool{true, false} {
		watched := evt.EntityID(newNamespace() + "-watched")
		target := evt.EntityID(newNamespace() + "-target")

		_, err = rival.ExecuteWithOutcome(ctx, newLedger(watched), watched,
			&ledgerCommand{Kind: "deposit", Amount: 1}, withCommand(env.commandID("seed-"+string(watched))))
		require.NoError(t, err)

		id := env.commandID(fmt.Sprintf("fence-race-%t-%s", sameIdentity, watched))
		// Both writers name both streams: the winner writes the watched one.
		identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{target, watched}}

		// The loser reads the watched stream (an engine fence at head 1) and writes the target.
		tx, replayed, berr := env.store.BeginCommand(ctx, identity, withCommand(id))
		require.NoError(t, berr)
		require.Nil(t, replayed)
		require.NoError(t, tx.Read(ctx, newLedger(watched), watched))
		require.NoError(t, tx.Read(ctx, newLedger(target), target))
		require.NoError(t, tx.Append(target, &ledgerEvent{Kind: "deposited", Amount: 1}))

		// The winner, between the loser's load and commit, moves the watched stream under the same
		// command ID.
		winnerIdentity := identity
		if !sameIdentity {
			winnerIdentity.Type = "ledger:other"
		}

		faulty.Intercept = func(_ context.Context, _ evt.SerializedResult, commit func() error) error {
			faulty.Intercept = nil

			winner, wtx, werr := rival.BeginCommand(ctx, winnerIdentity, withCommand(id))
			require.NoError(t, werr)
			require.Nil(t, wtx)
			require.NoError(t, winner.Read(ctx, newLedger(watched), watched))
			require.NoError(t, winner.Append(watched, &ledgerEvent{Kind: "deposited", Amount: 1}))

			_, werr = winner.Complete(ctx, nil)
			require.NoError(t, werr)

			return commit()
		}

		outcome, err := tx.Complete(ctx, nil)

		if sameIdentity {
			require.NoError(t, err)
			require.True(t, outcome.Replayed, "the loser returns the winner's stored outcome")
		} else {
			require.ErrorIs(t, err, evt.ErrIdempotencyKeyReuse)
		}

		require.Empty(t, env.events(t, target), "the loser wrote nothing")
	}
}

// The returned outcome owns its response bytes: a caller that reuses its buffer after the commit
// changes neither the stored row nor the outcome it was handed.
func testResponseCopied(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newOutcomeEnv(t, repo, 0)

	id := env.commandID("response-copy")
	identity := evt.CommandIdentity{Type: "ledger:respond", Targets: []evt.EntityID{env.id}}

	tx, _, err := env.store.BeginCommand(ctx, identity, withCommand(id))
	require.NoError(t, err)
	require.NoError(t, tx.Read(ctx, newLedger(env.id), env.id))
	require.NoError(t, tx.Append(env.id, &ledgerEvent{Kind: "deposited", Amount: 1}))

	buffer := []byte("original")

	outcome, err := tx.Complete(ctx, buffer)
	require.NoError(t, err)

	copy(buffer, "MUTATED!")

	require.Equal(t, []byte("original"), outcome.Response, "the returned outcome does not alias the caller's buffer")

	stored, err := repo.GetOutcome(ctx, id)
	require.NoError(t, err)
	require.Equal(t, []byte("original"), stored.Response)
}

// PostgreSQL text and jsonb cannot hold U+0000, so every backend refuses it alike, before any
// handler side effect or write: in a custom digest, a command ID, and a rejection's code, message,
// details strings, and details keys.
func testNULRefused(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newOutcomeEnv(t, repo, 0)

	// A digest with U+0000: refused before the pre-check and the handler.
	ran := counter()
	digested := snapshots.NewStore(repo, 0).WithOutcomes(snapshots.WithDigester(func(evt.DigestInput) (string, error) {
		return "digest\x00with-nul", nil
	}))

	_, err := digested.ExecuteWithOutcome(ctx, newLedger(env.id), env.id,
		&ledgerCommand{Kind: "deposit", Amount: 1, ran: ran}, withCommand(env.commandID("digest-nul")))
	require.ErrorIs(t, err, evt.ErrInvalidArgument)
	require.EqualValues(t, 0, ran.Load(), "the handler never ran")

	// A command ID with U+0000.
	_, err = env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1, ran: ran}, evt.CommandID("id\x00nul"))
	require.ErrorIs(t, err, evt.ErrInvalidArgument)
	require.EqualValues(t, 0, ran.Load())

	// Every other persisted metadata string is held to the same rule, before the handler runs: the
	// principal, the causation ID, the extension keys and values, and the rest.
	for name, mutate := range map[string]func(*evt.Metadata){
		"principal":     func(m *evt.Metadata) { p := evt.PrincipalID("who\x00"); m.PrincipalID = &p },
		"causation":     func(m *evt.Metadata) { c := evt.CommandID("cause\x00"); m.CausationID = &c },
		"extension key": func(m *evt.Metadata) { m.Extensions = map[string]string{"k\x00": "v"} },
		"extension val": func(m *evt.Metadata) { m.Extensions = map[string]string{"k": "v\x00"} },
		"region":        func(m *evt.Metadata) { m.Region = "us\x00" },
		"origin":        func(m *evt.Metadata) { m.Origin = &evt.Origin{Source: "s\x00", Endpoint: "e"} },
		"address":       func(m *evt.Metadata) { a := "addr\x00"; m.Address = &a },
		"trace":         func(m *evt.Metadata) { m.Trace = &propagation.MapCarrier{"traceparent": "x\x00"} },
		"invalid UTF-8": func(m *evt.Metadata) { p := evt.PrincipalID("who\xff"); m.PrincipalID = &p },
	} {
		metadata := withCommand(env.commandID("metadata-nul-" + strings.ReplaceAll(name, " ", "-")))
		mutate(&metadata)

		_, err = env.store.ExecuteWithOutcome(ctx, newLedger(env.id), env.id,
			&ledgerCommand{Kind: "deposit", Amount: 1, ran: ran}, metadata)
		require.ErrorIs(t, err, evt.ErrInvalidArgument, name)
		require.EqualValues(t, 0, ran.Load(), "%s: the handler never ran", name)
		env.requireNoOutcome(t, *metadata.CommandID)
	}

	// A rejection with U+0000 anywhere in its code, message, or details (strings and keys).
	for index, rejection := range []evt.Rejection{
		{Code: "c\x00", Message: "m"},
		{Code: "c", Message: "m\x00"},
		{Code: "c", Message: "m", Details: map[string]any{"key": "va\x00lue"}},
		{Code: "c", Message: "m", Details: map[string]any{"ke\x00y": "value"}},
		{Code: "c", Message: "m", Details: map[string]any{"nested": []any{map[string]any{"deep": "x\x00"}}}},
		// Typed values reach the encoder too: the check inspects the encoding, not the Go types.
		{Code: "c", Message: "m", Details: map[string]any{"tags": []string{"bad\x00value"}}},
		{Code: "c", Message: "m", Details: map[string]any{"m": map[string][]string{"ke\x00y": {"v"}}}},
		{Code: "c", Message: "m", Details: map[string]any{"s": struct {
			Name string `json:"name"`
		}{Name: "n\x00"}}},
	} {
		id := env.commandID(fmt.Sprintf("reject-nul-%d", index))

		tx, replayed, berr := env.store.BeginCommand(ctx, evt.CommandIdentity{Type: "t", Targets: []evt.EntityID{env.id}}, withCommand(id))
		require.NoError(t, berr)
		require.Nil(t, replayed)

		_, err = tx.Reject(ctx, rejection)
		require.ErrorIs(t, err, evt.ErrInvalidArgument, "%+v", rejection)
		env.requireNoOutcome(t, id)
	}
}

func testReservedTargets(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)
	ctx := context.Background()

	for _, reserved := range []evt.EntityID{"SYSTEM#target", "command-outcome#target"} {
		// Multi-stream: the identity names a reserved stream, so no outcome is read or recorded.
		id := env.commandID("reserved-" + string(reserved))
		identity := evt.CommandIdentity{Type: "ledger:any", Targets: []evt.EntityID{env.id, reserved}}

		_, _, err := env.store.BeginCommand(ctx, identity, withCommand(id))
		require.True(t, evt.IsReservedEntityIdErr(err), "BeginCommand: %v", err)

		_, err = env.store.ResolveCommand(ctx, identity, withCommand(id))
		require.True(t, evt.IsReservedEntityIdErr(err), "ResolveCommand: %v", err)

		// Single aggregate: a no-op or a rejection aimed at a reserved stream records nothing.
		for _, kind := range []string{"check", "withdraw"} {
			sid := env.commandID(kind + "-" + string(reserved))
			_, err = env.store.ExecuteWithOutcome(ctx, newLedger(reserved), reserved,
				&ledgerCommand{Kind: kind, Amount: 1000}, withCommand(sid))
			require.True(t, evt.IsReservedEntityIdErr(err), "%s: %v", kind, err)
			env.requireNoOutcome(t, sid)
		}

		env.requireNoOutcome(t, id)
	}
}

func testOutcomeReadValidation(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)
	ctx := context.Background()

	// Every backend refuses an empty, non-UTF-8, or oversized ID with a typed error, never
	// "absent": a lookup with a bad ID is a programmer error.
	for name, id := range map[string]evt.CommandID{
		"empty":     "",
		"not UTF-8": "bad\xff",
		"oversized": evt.CommandID(strings.Repeat("a", evt.MaxCommandIDBytes+1)),
	} {
		stored, err := repo.GetOutcome(ctx, id)
		require.Error(t, err, name)
		require.Nil(t, stored, name)

		_, err = env.store.Outcome(ctx, id)
		require.Error(t, err, name)
	}
}

func testReplayRehydrates(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()

	faulty, err := test.NewFaultRepository(repo)
	require.NoError(t, err)

	env := newOutcomeEnv(t, faulty, 0)
	rival := snapshots.NewStore(repo, 0).WithOutcomes()

	_, err = env.run(t, &ledgerCommand{Kind: "deposit", Amount: 5}, env.commandID("seed"))
	require.NoError(t, err)

	// A duplicate command returns early, before any load: the entity the caller supplied (zero
	// state) must still reflect durable state when Execute returns.
	id := env.commandID("early")
	require.NoError(t, env.store.Execute(ctx, newLedger(env.id), env.id, &ledgerCommand{Kind: "deposit", Amount: 2}, withCommand(id)))

	duplicate := newLedger(env.id)
	require.NoError(t, env.store.Execute(ctx, duplicate, env.id, &ledgerCommand{Kind: "deposit", Amount: 2}, withCommand(id)))
	require.Equal(t, 7, duplicate.Total, "a replayed Execute loads the entity")

	// A lost race settled by the winner's row: the loser decided on balance 7 and applied it
	// locally, but the winner ran later and the durable state is different. The supplied entity
	// must show the durable state, not the loser's local mutation.
	raced := env.commandID("raced")
	faulty.Intercept = func(_ context.Context, _ evt.SerializedResult, commit func() error) error {
		faulty.Intercept = nil

		_, rerr := rival.ExecuteWithOutcome(ctx, newLedger(env.id), env.id,
			&ledgerCommand{Kind: "deposit", Amount: 10}, withCommand(env.commandID("mover")))
		require.NoError(t, rerr)

		_, rerr = rival.ExecuteWithOutcome(ctx, newLedger(env.id), env.id,
			&ledgerCommand{Kind: "withdraw", Amount: 1}, withCommand(raced))
		require.NoError(t, rerr)

		return commit()
	}

	loser := newLedger(env.id)
	require.NoError(t, env.store.Execute(ctx, loser, env.id, &ledgerCommand{Kind: "withdraw", Amount: 1}, withCommand(raced)))
	require.Equal(t, 16, loser.Total, "7 + 10 - 1: the durable state, not the loser's local 6")
	require.Equal(t, map[string]int{"deposited": 3, "withdrawn": 1}, loser.Seen,
		"a map the constructor initialized must not keep the loser's own applied events")
}

func testNonCanonicalEventIDs(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	ns := newNamespace()
	id := evt.EntityID(ns + "-ledger")

	events, err := evt.SerializeEvents([]evt.Event{&ledgerEvent{Kind: "deposited", Amount: 1}}, 0, id, evt.Metadata{})
	require.NoError(t, err)

	events[0].ID = "someone-else:7"

	err = repo.Commit(ctx, evt.SerializedResult{
		Events:  events,
		Outcome: &evt.Outcome{CommandID: evt.CommandID(ns + "-cmd"), Status: evt.OutcomeAccepted, Digest: "d"},
	})

	var invalid *evt.InvalidCommitError
	require.ErrorAs(t, err, &invalid)

	stored, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Empty(t, stored, "nothing is written")

	receipt, err := repo.GetOutcome(ctx, evt.CommandID(ns+"-cmd"))
	require.NoError(t, err)
	require.Nil(t, receipt, "and no receipt names an event that was not written")
}

func testRejectionFenceOrdering(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)
	ctx := context.Background()

	// A caller fence declared before the stream is read is superseded by the engine's own fence,
	// whichever order the calls came in, so the rejection is recorded (no duplicate fence).
	_, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1}, env.commandID("seed"))
	require.NoError(t, err)

	id := env.commandID("fence-then-read")
	tx, replayed, err := env.store.BeginCommand(ctx, evt.CommandIdentity{Type: "ledger:withdraw", Targets: []evt.EntityID{env.id}}, withCommand(id))
	require.NoError(t, err)
	require.Nil(t, replayed)
	require.NoError(t, tx.Fence(evt.ReadFence{EntityID: env.id, EntityType: ledgerType, Head: 1}))
	require.NoError(t, tx.Read(ctx, newLedger(env.id), env.id))

	outcome, err := tx.Reject(ctx, evt.Rejection{Code: "insufficient-funds", Message: "balance is too low"})
	require.NoError(t, err)
	require.True(t, outcome.Rejected())
}

func testLegacyCommandIDWindow(t *testing.T, repo evt.OutcomeRepository) {
	env := newOutcomeEnv(t, repo, 0)
	ctx := context.Background()

	// An upgraded log: a command recorded before outcomes existed left its ID on an event and no
	// outcome row. A store without outcomes writes exactly that.
	id := env.commandID("legacy")
	legacy := snapshots.NewStore(repo, 0)
	require.NoError(t, legacy.Execute(ctx, newLedger(env.id), env.id, &ledgerCommand{Kind: "deposit", Amount: 5}, withCommand(id)))
	env.requireNoOutcome(t, id)

	// The command ran, so no outcome is recorded for it: not an accepted one, not a rejection that
	// would then replay forever, and not a stale-fence rejection.
	_, err := env.run(t, &ledgerCommand{Kind: "withdraw", Amount: 1000}, id)
	require.True(t, evt.IsDuplicateCommandErr(err), "the single-aggregate path: %v", err)
	env.requireNoOutcome(t, id)

	identity := evt.CommandIdentity{Type: "ledger:withdraw", Targets: []evt.EntityID{env.id}}

	tx, replayed, err := env.store.BeginCommand(ctx, identity, withCommand(id))
	require.NoError(t, err)
	require.Nil(t, replayed)
	require.NoError(t, tx.Read(ctx, newLedger(env.id), env.id))

	_, err = tx.Reject(ctx, evt.Rejection{Code: "insufficient-funds", Message: "balance is too low"})
	require.True(t, evt.IsDuplicateCommandErr(err), "a rejection: %v", err)
	env.requireNoOutcome(t, id)

	referral := evt.EntityID(env.ns + "-referral")
	tx, _, err = env.store.BeginCommand(ctx, identity, withCommand(id))
	require.NoError(t, err)
	require.NoError(t, tx.Read(ctx, newLedger(env.id), env.id))
	require.NoError(t, tx.Fence(evt.ReadFence{EntityID: referral, EntityType: ledgerType, Head: 0}))

	rival := snapshots.NewStore(repo, 0).WithOutcomes()
	_, err = rival.ExecuteWithOutcome(ctx, newLedger(referral), referral,
		&ledgerCommand{Kind: "deposit", Amount: 1}, withCommand(env.commandID("referral")))
	require.NoError(t, err)

	_, err = tx.Complete(ctx, nil)
	require.True(t, evt.IsDuplicateCommandErr(err), "a stale caller-stamped fence: %v", err)
	env.requireNoOutcome(t, id)
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

// --- rejections that carry events ---

// kinds lists the event kinds of a stream's stored events, in order.
func kinds(t *testing.T, events []evt.SerializedEvent) []string {
	t.Helper()

	out := make([]string, 0, len(events))

	for _, event := range events {
		var payload ledgerEvent
		require.NoError(t, json.Unmarshal(event.Payload, &payload))

		out = append(out, payload.Kind)
	}

	return out
}

// A rejection marked with RejectWithEvents commits its events and the rejected outcome row in one
// atomic commit; the outcome lists the events, and a replay returns the stored rejection without
// running the handler or writing the events again.
func testRejectionWithEvents(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newOutcomeEnv(t, repo, 0)
	ran := counter()

	first, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 2}, env.commandID("seed"))
	require.NoError(t, err)
	require.True(t, first.Accepted())

	id := env.commandID("flagged")
	command := &ledgerCommand{Kind: "withdraw-flagged", Amount: 5, ran: ran}
	entity := newLedger(env.id)

	outcome, err := env.store.ExecuteWithOutcome(ctx, entity, env.id, command, withCommand(id))
	require.NoError(t, err)
	require.True(t, outcome.Rejected())
	require.False(t, outcome.Replayed)
	require.Equal(t, evt.RejectionCode("insufficient-funds"), outcome.Rejection.Code)
	require.Nil(t, outcome.Response, "a rejection stores no response")
	require.Equal(t, []evt.EventID{evt.GetEventID(env.id, 2)}, outcome.EventIDs)
	require.Len(t, outcome.Events, 1)
	require.Equal(t, 1, int(ran.Load()))

	stored := env.events(t, env.id)
	require.Equal(t, []string{"deposited", "flagged"}, kinds(t, stored))
	require.Equal(t, 2, entity.Total, "the flagged event changed no balance")
	require.Equal(t, 1, entity.Seen["flagged"], "the entity reflects the committed events")

	row, err := env.repo.GetOutcome(ctx, id)
	require.NoError(t, err)
	require.Equal(t, outcome.Outcome, *row, "the rejected row lists the events it wrote")

	// Store.Execute surfaces the rejection and still commits the events once.
	errID := env.commandID("flagged-execute")
	err = env.store.Execute(ctx, newLedger(env.id), env.id, &ledgerCommand{Kind: "withdraw-flagged", Amount: 9}, withCommand(errID))

	var rejected *evt.RejectionError
	require.ErrorAs(t, err, &rejected)
	require.ErrorIs(t, err, evt.ErrRejected)
	require.Len(t, env.events(t, env.id), 3)

	// A replay returns the stored rejection and applies nothing.
	replayed, err := env.store.ExecuteWithOutcome(ctx, newLedger(env.id), env.id, command, withCommand(id))
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, outcome.Outcome, replayed.Outcome)
	require.Empty(t, replayed.Events)
	require.Equal(t, 1, int(ran.Load()), "the handler did not run again")
	require.Len(t, env.events(t, env.id), 3, "the events were not duplicated")

	// The command ID is terminal for its digest, and another command under it is a reuse.
	_, err = env.run(t, &ledgerCommand{Kind: "withdraw-flagged", Amount: 6}, id)
	require.ErrorIs(t, err, evt.ErrIdempotencyKeyReuse)
	require.Len(t, env.events(t, env.id), 3)

	// The same marked error without a balance problem is an ordinary accepted command.
	accepted, err := env.run(t, &ledgerCommand{Kind: "withdraw-flagged", Amount: 1}, env.commandID("fine"))
	require.NoError(t, err)
	require.True(t, accepted.Accepted())
	require.Equal(t, []string{"deposited", "flagged", "flagged", "withdrawn"}, kinds(t, env.events(t, env.id)))
}

// A Prepare function that stages events and returns a marked rejection commits them with the
// rejection, on any stream it read; a plain rejection still discards what it staged.
func testRejectionWithEventsTransaction(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	target := evt.EntityID(env.ns + "-target")
	env.deposit(t, target, 2)

	identity := evt.CommandIdentity{Type: "ledger:apply", Targets: []evt.EntityID{target}}
	rejection := &domainRejection{evt.Rejection{Code: "apply-conflict", Message: "the base is stale"}}

	stage := func(wrap func(error) error) snapshots.Prepare {
		return func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
			if err := tx.Read(ctx, newLedger(target), target); err != nil {
				return nil, err
			}

			if err := tx.Append(target, &ledgerEvent{Kind: "flagged"}); err != nil {
				return nil, err
			}

			return nil, wrap(rejection)
		}
	}

	// Plain: the staged event is discarded, as before.
	plain, err := env.store.ExecuteTransaction(ctx, identity, withCommand(env.commandID("plain")), stage(func(err error) error { return err }))
	require.NoError(t, err)
	require.True(t, plain.Rejected())
	require.Empty(t, plain.EventIDs)
	require.Len(t, env.events(t, target), 1)

	// Marked: the event and the rejected row commit together.
	id := env.commandID("marked")
	marked, err := env.store.ExecuteTransaction(ctx, identity, withCommand(id), stage(evt.RejectWithEvents))
	require.NoError(t, err)
	require.True(t, marked.Rejected())
	require.Equal(t, evt.RejectionCode("apply-conflict"), marked.Rejection.Code)
	require.Equal(t, []evt.EventID{evt.GetEventID(target, 2)}, marked.EventIDs)
	require.Len(t, marked.Events, 1)
	require.Equal(t, []string{"deposited", "flagged"}, kinds(t, env.events(t, target)))

	row, err := env.repo.GetOutcome(ctx, id)
	require.NoError(t, err)
	require.Equal(t, marked.Outcome, *row)

	// A replay prepares nothing and writes nothing.
	calls := 0
	replayed, err := env.store.ExecuteTransaction(ctx, identity, withCommand(id),
		func(context.Context, *snapshots.Transaction) ([]byte, error) { calls++; return nil, rejection })
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, marked.Outcome, replayed.Outcome)
	require.Zero(t, calls)
	require.Len(t, env.events(t, target), 2)

	// A marked error that is not a rejection is an internal failure that writes nothing.
	internalID := env.commandID("internal")
	_, err = env.store.ExecuteTransaction(ctx, identity, withCommand(internalID),
		stage(func(error) error { return evt.RejectWithEvents(errors.New("storage exploded")) }))
	require.ErrorContains(t, err, "storage exploded")
	env.requireNoOutcome(t, internalID)
	require.Len(t, env.events(t, target), 2)

	// The Transaction API: Reject discards, RejectWithEvents commits.
	manual, _, err := env.store.BeginCommand(ctx, identity, withCommand(env.commandID("manual")))
	require.NoError(t, err)
	require.NoError(t, manual.Read(ctx, newLedger(target), target))
	require.NoError(t, manual.Append(target, &ledgerEvent{Kind: "flagged"}))

	committed, err := manual.RejectWithEvents(ctx, rejection.Rejection())
	require.NoError(t, err)
	require.True(t, committed.Rejected())
	require.Len(t, committed.Events, 1)
	require.Len(t, env.events(t, target), 3)
}

// The commit rules of an accepted commit apply to a rejection with events: it is atomic, a
// reserved stream and a projector-only commit are refused, and the budget counts the outcome row.
func testRejectionWithEventsCommitRules(t *testing.T, repo evt.OutcomeRepository, enforcesConcurrency bool) {
	ctx := context.Background()
	ns := newNamespace()

	rejected := func(name string) *evt.Outcome {
		return &evt.Outcome{
			CommandID: evt.CommandID(ns + name), Status: evt.OutcomeRejected, Digest: "digest",
			Rejection: &evt.Rejection{Code: "refused", Message: "refused"},
		}
	}

	events := func(id evt.EntityID, from, count int) []evt.SerializedEvent {
		out := make([]evt.Event, 0, count)
		for range count {
			out = append(out, &ledgerEvent{Kind: "flagged"})
		}

		serialized, err := evt.SerializeEvents(out, evt.EventSequence(from), id, evt.Metadata{})
		require.NoError(t, err)

		for i := range serialized {
			serialized[i].EntityType = ledgerType
		}

		return serialized
	}

	stream := evt.EntityID(ns + "-stream")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: events(stream, 0, 1)}))

	// A conflicting commit writes neither its events nor its rejected row.
	if enforcesConcurrency {
		err := repo.Commit(ctx, evt.SerializedResult{Events: events(stream, 0, 1), Outcome: rejected("-lost")})
		require.True(t, evt.IsOptimisticConflict(err), "got %v", err)

		missing, gerr := repo.GetOutcome(ctx, evt.CommandID(ns+"-lost"))
		require.NoError(t, gerr)
		require.Nil(t, missing)
		require.Len(t, mustEvents(t, repo, stream), 1)
	}

	// The winning commit writes both, and the row lists the events.
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: events(stream, 1, 2), Outcome: rejected("-won")}))

	row, err := repo.GetOutcome(ctx, evt.CommandID(ns+"-won"))
	require.NoError(t, err)
	require.True(t, row.Rejected())
	require.Equal(t, []evt.EventID{evt.GetEventID(stream, 2), evt.GetEventID(stream, 3)}, row.EventIDs)
	require.Len(t, mustEvents(t, repo, stream), 3)

	// A reserved stream is refused before any write.
	reserved := evt.EntityID("SYSTEM#" + ns)
	err = repo.Commit(ctx, evt.SerializedResult{Events: events(reserved, 0, 1), Outcome: rejected("-reserved")})
	require.True(t, evt.IsReservedEntityIdErr(err), "got %v", err)

	// Projector operations without an event are refused.
	err = repo.Commit(ctx, evt.SerializedResult{Transaction: evt.Transaction{rejectedGroup{}}, Outcome: rejected("-projector")})

	var projectorOnly *evt.ProjectorOnlyCommitError
	require.ErrorAs(t, err, &projectorOnly)

	// The outcome row counts against the budget: the two control checks, the head update, 96 event
	// puts, and the outcome fit; one more event does not.
	fits := evt.EntityID(ns + "-fits")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: events(fits, 0, 96), Outcome: rejected("-fits")}))

	over := evt.EntityID(ns + "-over")
	err = repo.Commit(ctx, evt.SerializedResult{Events: events(over, 0, 97), Outcome: rejected("-over")})

	var tooLarge *evt.CommitTooLargeError
	require.ErrorAs(t, err, &tooLarge)
	require.Empty(t, mustEvents(t, repo, over))

	missing, err := repo.GetOutcome(ctx, evt.CommandID(ns+"-over"))
	require.NoError(t, err)
	require.Nil(t, missing)
}

// rejectedGroup is a one-operation projector group with no events behind it.
type rejectedGroup struct{}

func (rejectedGroup) Len() int { return 1 }
func (rejectedGroup) Merge(evt.TransactionGroup) (evt.TransactionGroup, error) {
	return rejectedGroup{}, nil
}
func (rejectedGroup) StorageType() evt.StorageType         { return "conformance" }
func (rejectedGroup) TransactionType() evt.TransactionType { return "conformance" }
func (rejectedGroup) HandleError(err error, _ int) error   { return err }

// A conflict while committing a rejection with events re-prepares the command like any other, and
// the final answer is decided on the fresh state. When every attempt loses, the stale-fence
// rejection is recorded without the caller's events.
func testRejectionWithEventsRetry(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)
	ran := counter()

	env.deposit(t, env.id, 1)

	// Another writer moves the stream between the load and the first commit. Still short of funds:
	// the second attempt rejects again, on the fresh head, and its event follows the rival's.
	env.interfere(true, hasEvents, func() { env.deposit(t, env.id, 1) })

	id := env.commandID("reprepare")
	entity := newLedger(env.id)

	outcome, err := env.store.ExecuteWithOutcome(ctx, entity, env.id,
		&ledgerCommand{Kind: "withdraw-flagged", Amount: 50, ran: ran}, withCommand(id))
	require.NoError(t, err)
	require.True(t, outcome.Rejected())
	require.EqualValues(t, 2, ran.Load(), "the failed commit re-prepared the command")
	require.Equal(t, []string{"deposited", "deposited", "flagged"}, kinds(t, env.events(t, env.id)),
		"only the committing attempt's event was written")
	require.Equal(t, []evt.EventID{evt.GetEventID(env.id, 3)}, outcome.EventIDs)
	require.Equal(t, 2, entity.Total)
	require.Equal(t, 1, entity.Seen["flagged"], "the failed attempt's event was not counted")

	// The rival's move can change the decision: now the command is accepted and records no failure.
	env.interfere(true, hasEvents, func() { env.deposit(t, env.id, 100) })
	ran.Store(0)

	flipped, err := env.run(t, &ledgerCommand{Kind: "withdraw-flagged", Amount: 50, ran: ran}, env.commandID("flipped"))
	require.NoError(t, err)
	require.True(t, flipped.Accepted(), "decided on the fresh state: %+v", flipped.Rejection)
	require.EqualValues(t, 2, ran.Load())
	require.Equal(t, []string{"deposited", "deposited", "flagged", "deposited", "withdrawn"}, kinds(t, env.events(t, env.id)))

	// Every attempt loses: the stale-fence rejection is recorded with none of the caller's events.
	before := len(env.events(t, env.id))
	env.interfere(false, hasEvents, func() { env.deposit(t, env.id, 1) })
	ran.Store(0)

	exhausted := env.commandID("exhausted")
	entity = newLedger(env.id)

	lost, err := env.store.ExecuteWithOutcome(ctx, entity, env.id,
		&ledgerCommand{Kind: "withdraw-flagged", Amount: 5000, ran: ran}, withCommand(exhausted))
	require.NoError(t, err)
	require.True(t, lost.Rejected())
	require.Equal(t, evt.RejectionStaleFence, lost.Rejection.Code)
	require.Equal(t, encodedRejection(t, evt.RetriesExhaustedRejection(3)), encodedRejection(t, *lost.Rejection))
	require.Empty(t, lost.EventIDs)
	require.Empty(t, lost.Events)
	require.EqualValues(t, 3, ran.Load())

	after := env.events(t, env.id)
	require.Len(t, after, before+3, "three rival events and no flagged event from the command")
	require.NotContains(t, kinds(t, after[before:]), "flagged")
	require.Equal(t, 1, entity.Seen["flagged"], "the caller's entity reflects durable state: only the earlier refusal")

	// The exhausted ID replays the stale-fence rejection.
	env.faulty.Intercept = nil

	replayed, err := env.store.ExecuteWithOutcome(ctx, newLedger(env.id), env.id,
		&ledgerCommand{Kind: "withdraw-flagged", Amount: 5000, ran: ran}, withCommand(exhausted))
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, lost.Outcome, replayed.Outcome)
}

// A caller-stamped fence that went stale beside a rejection's events records the stale-fence
// rejection alone: the engine cannot know the caller's decision, failure events included, still
// holds, so none of it is written, and the command is not prepared again.
func testRejectionWithEventsCallerFence(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	referral, target := evt.EntityID(env.ns+"-referral"), evt.EntityID(env.ns+"-target")
	env.deposit(t, referral, 1)

	identity := evt.CommandIdentity{Type: "ledger:apply", Targets: []evt.EntityID{target}}
	rejection := &domainRejection{evt.Rejection{Code: "apply-conflict", Message: "the base is stale"}}

	var calls int

	prepare := func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
		calls++

		if err := tx.Read(ctx, newLedger(target), target); err != nil {
			return nil, err
		}

		if err := tx.Append(target, &ledgerEvent{Kind: "flagged"}); err != nil {
			return nil, err
		}

		// The caller decided on the referral at head 1.
		if err := tx.Fence(evt.ReadFence{EntityID: referral, EntityType: ledgerType, Head: 1}); err != nil {
			return nil, err
		}

		return nil, evt.RejectWithEvents(rejection)
	}

	env.interfere(true, hasEvents, func() { env.deposit(t, referral, 1) })

	id := env.commandID("stamped")

	outcome, err := env.store.ExecuteTransaction(ctx, identity, withCommand(id), prepare)
	require.NoError(t, err, "a stale caller-stamped fence is a persisted rejection, not an error")
	require.True(t, outcome.Rejected())
	require.Equal(t, evt.RejectionStaleFence, outcome.Rejection.Code)
	require.Equal(t, encodedRejection(t, evt.CallerFenceStaleRejection()), encodedRejection(t, *outcome.Rejection))
	require.Empty(t, outcome.EventIDs)
	require.Empty(t, outcome.Events)
	require.Equal(t, 1, calls, "the command was not prepared again")
	require.Empty(t, env.events(t, target), "nothing was written on the target")

	stored, err := env.repo.GetOutcome(ctx, id)
	require.NoError(t, err)
	require.Equal(t, outcome.Outcome, *stored)
}
