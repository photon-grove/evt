package conformance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/snapshots"
)

// testExpectedVersions covers client-expected heads (Transaction.Expect): a matching head commits,
// a stale one records the stale-version rejection instead of the acceptance, a domain rejection
// wins over a stale head, an expectation on a stream the command never read is refused before any
// write, and the engine's stale codes can be replaced.
func testExpectedVersions(t *testing.T, repo evt.OutcomeRepository) {
	t.Run("MatchingHeadCommits", func(t *testing.T) { testExpectedHeadMatches(t, repo) })
	t.Run("StaleHeadRecordsStaleVersion", func(t *testing.T) { testExpectedHeadStale(t, repo) })
	t.Run("DomainRejectionWins", func(t *testing.T) { testExpectedHeadDomainRejectionWins(t, repo) })
	t.Run("UnreadStreamIsRefused", func(t *testing.T) { testExpectedHeadUnread(t, repo) })
	t.Run("StaleCodesAreConfigurable", func(t *testing.T) { testStaleCodesConfigurable(t, repo) })
	t.Run("EveryStaleStreamIsListedSorted", func(t *testing.T) { testExpectedHeadsListedSorted(t, repo) })
	t.Run("RejectionWithEventsIgnoresExpectations", func(t *testing.T) { testExpectedHeadRejectWithEvents(t, repo) })
	t.Run("ManualCommandFlow", func(t *testing.T) { testExpectedHeadManualFlow(t, repo) })
	t.Run("ExpectValidation", func(t *testing.T) { testExpectValidation(t, repo) })
}

// expectingCopy reads the watched stream, expects it at head, and appends a deposit to the target.
// A non-nil reject makes the step refuse the command instead.
func expectingCopy(watched, target evt.EntityID, head evt.EventSequence, reject *domainRejection, calls *int) snapshots.Prepare {
	return func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
		*calls++

		if err := tx.Expect(watched, head); err != nil {
			return nil, err
		}

		if err := tx.Read(ctx, newLedger(watched), watched); err != nil {
			return nil, err
		}

		if err := tx.Read(ctx, newLedger(target), target); err != nil {
			return nil, err
		}

		if reject != nil {
			return nil, reject
		}

		if err := tx.Append(target, &ledgerEvent{Kind: "deposited", Amount: 1}); err != nil {
			return nil, err
		}

		return []byte(`{"ok":true}`), nil
	}
}

func testExpectedHeadMatches(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	watched, target := evt.EntityID(env.ns+"-watched"), evt.EntityID(env.ns+"-target")
	env.deposit(t, watched, 5)

	var calls int

	identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{watched, target}}
	outcome, err := env.store.ExecuteTransaction(ctx, identity, withCommand(env.commandID("match")),
		expectingCopy(watched, target, 1, nil, &calls))
	require.NoError(t, err)
	require.True(t, outcome.Accepted(), "%+v", outcome)
	require.Len(t, env.events(t, target), 1)
	require.Equal(t, 1, calls)
}

func testExpectedHeadStale(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	watched, target := evt.EntityID(env.ns+"-watched"), evt.EntityID(env.ns+"-target")
	env.deposit(t, watched, 5)
	env.deposit(t, watched, 5) // the client saw head 1; the stream is at 2

	var calls int

	identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{watched, target}}
	id := env.commandID("stale")

	outcome, err := env.store.ExecuteTransaction(ctx, identity, withCommand(id), expectingCopy(watched, target, 1, nil, &calls))
	require.NoError(t, err, "a stale client version is a persisted rejection, not an error")
	require.True(t, outcome.Rejected())

	want := evt.StaleVersionRejection([]evt.StaleVersion{{EntityID: watched, Expected: 1, Actual: 2}})
	require.Equal(t, evt.RejectionStaleVersion, outcome.Rejection.Code)
	require.Equal(t, encodedRejection(t, want), encodedRejection(t, *outcome.Rejection))
	require.Empty(t, outcome.Events)
	require.Empty(t, env.events(t, target), "the acceptance was not committed")
	require.Equal(t, 1, calls)

	// The rejection is the command's durable answer.
	replayed, err := env.store.ExecuteTransaction(ctx, identity, withCommand(id), expectingCopy(watched, target, 1, nil, &calls))
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, encodedRejection(t, want), encodedRejection(t, *replayed.Rejection))
	require.Equal(t, 1, calls, "a replay never prepares the command")
}

func testExpectedHeadDomainRejectionWins(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	watched, target := evt.EntityID(env.ns+"-watched"), evt.EntityID(env.ns+"-target")
	env.deposit(t, watched, 5)
	env.deposit(t, watched, 5)

	var calls int

	taken := &domainRejection{evt.Rejection{Code: "taken", Message: "the handle is taken"}}
	identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{watched, target}}

	outcome, err := env.store.ExecuteTransaction(ctx, identity, withCommand(env.commandID("taken")),
		expectingCopy(watched, target, 1, taken, &calls))
	require.NoError(t, err)
	require.True(t, outcome.Rejected())
	require.Equal(t, encodedRejection(t, taken.rejection), encodedRejection(t, *outcome.Rejection),
		"a domain rejection is recorded whatever the client expected")
}

func testExpectedHeadUnread(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	watched, target := evt.EntityID(env.ns+"-watched"), evt.EntityID(env.ns+"-target")
	id := env.commandID("unread")

	prepare := func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
		if err := tx.Expect(watched, 0); err != nil {
			return nil, err
		}

		if err := tx.Read(ctx, newLedger(target), target); err != nil {
			return nil, err
		}

		return nil, tx.Append(target, &ledgerEvent{Kind: "deposited", Amount: 1})
	}

	identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{watched, target}}
	_, err := env.store.ExecuteTransaction(ctx, identity, withCommand(id), prepare)
	require.ErrorIs(t, err, evt.ErrInvalidArgument)

	stored, err := env.repo.GetOutcome(ctx, id)
	require.NoError(t, err)
	require.Nil(t, stored, "nothing is recorded for a programmer error")
	require.Empty(t, env.events(t, target))
}

func testStaleCodesConfigurable(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	store := snapshots.NewStore(env.faulty, 0).WithOutcomes(
		snapshots.WithRecoveryWait(recoveryWait),
		snapshots.WithStaleVersionCode("legacy-stale"),
		snapshots.WithStaleFenceCode("legacy-stale"),
	)

	watched, target := evt.EntityID(env.ns+"-watched"), evt.EntityID(env.ns+"-target")
	env.deposit(t, watched, 5)

	var calls int

	identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{watched, target}}
	outcome, err := store.ExecuteTransaction(ctx, identity, withCommand(env.commandID("version")),
		expectingCopy(watched, target, 3, nil, &calls))
	require.NoError(t, err)
	require.Equal(t, evt.RejectionCode("legacy-stale"), outcome.Rejection.Code)

	want := evt.StaleVersionRejection([]evt.StaleVersion{{EntityID: watched, Expected: 3, Actual: 1}})
	require.Equal(t, want.Message, outcome.Rejection.Message, "only the code changes")

	// A caller-stamped fence that is already stale records the stale-fence rejection under the
	// replaced code.
	fenced := func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
		if err := tx.Read(ctx, newLedger(target), target); err != nil {
			return nil, err
		}

		if err := tx.Fence(evt.ReadFence{EntityID: watched, EntityType: ledgerType, Head: 7}); err != nil {
			return nil, err
		}

		return nil, tx.Append(target, &ledgerEvent{Kind: "deposited", Amount: 1})
	}

	outcome, err = store.ExecuteTransaction(ctx, identity, withCommand(env.commandID("fence")), fenced)
	require.NoError(t, err)
	require.True(t, outcome.Rejected())
	require.Equal(t, evt.RejectionCode("legacy-stale"), outcome.Rejection.Code)
	require.Equal(t, evt.CallerFenceStaleRejection().Message, outcome.Rejection.Message)

	// Exhausted retries record the stale-fence rejection under the replaced code too: the watched
	// stream moves before every commit.
	store.WithRetry(snapshots.ImmediateRetryPolicy(2))
	env.interfere(false, hasEvents, func() { env.deposit(t, watched, 1) })
	defer func() { env.faulty.Intercept = nil }()

	var copies int

	outcome, err = store.ExecuteTransaction(ctx, identity, withCommand(env.commandID("exhausted")),
		readThenCopy(watched, target, &copies, new([]int)))
	require.NoError(t, err)
	require.Equal(t, evt.RejectionCode("legacy-stale"), outcome.Rejection.Code)
	require.Equal(t, evt.RetriesExhaustedRejection(2).Message, outcome.Rejection.Message)
	require.Equal(t, 2, copies)
}

// testExpectedHeadMovesDuringCommit moves the expected stream after the first attempt loaded it:
// its fence fails, and the next attempt, deciding on the new head, records the stale-version
// rejection instead of committing the acceptance.
func testExpectedHeadMovesDuringCommit(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	watched, target := evt.EntityID(env.ns+"-watched"), evt.EntityID(env.ns+"-target")
	env.deposit(t, watched, 5)

	env.interfere(true, hasEvents, func() { env.deposit(t, watched, 5) })

	var calls int

	identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{watched, target}}
	outcome, err := env.store.ExecuteTransaction(ctx, identity, withCommand(env.commandID("moved")),
		expectingCopy(watched, target, 1, nil, &calls))
	require.NoError(t, err)
	require.True(t, outcome.Rejected())

	want := evt.StaleVersionRejection([]evt.StaleVersion{{EntityID: watched, Expected: 1, Actual: 2}})
	require.Equal(t, encodedRejection(t, want), encodedRejection(t, *outcome.Rejection))
	require.Equal(t, 2, calls, "the fence conflict re-prepared once, and the second attempt decided stale")
	require.Empty(t, env.events(t, target), "the acceptance never committed")
}

func testExpectedHeadsListedSorted(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	// b sorts after a; the command expects them in the other order.
	first, second, target := evt.EntityID(env.ns+"-b"), evt.EntityID(env.ns+"-a"), evt.EntityID(env.ns+"-target")
	env.deposit(t, first, 5)
	env.deposit(t, second, 5)
	env.deposit(t, second, 5)

	prepare := func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
		for _, expected := range []struct {
			id   evt.EntityID
			head evt.EventSequence
		}{{first, 0}, {second, 1}} {
			if err := tx.Expect(expected.id, expected.head); err != nil {
				return nil, err
			}

			if err := tx.Read(ctx, newLedger(expected.id), expected.id); err != nil {
				return nil, err
			}
		}

		if err := tx.Read(ctx, newLedger(target), target); err != nil {
			return nil, err
		}

		return nil, tx.Append(target, &ledgerEvent{Kind: "deposited", Amount: 1})
	}

	identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{first, second, target}}
	outcome, err := env.store.ExecuteTransaction(ctx, identity, withCommand(env.commandID("both")), prepare)
	require.NoError(t, err)

	want := evt.StaleVersionRejection([]evt.StaleVersion{
		{EntityID: second, Expected: 1, Actual: 2},
		{EntityID: first, Expected: 0, Actual: 1},
	})
	require.Equal(t, encodedRejection(t, want), encodedRejection(t, *outcome.Rejection), "every stale stream, sorted by ID")
}

func testExpectedHeadRejectWithEvents(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	watched, target := evt.EntityID(env.ns+"-watched"), evt.EntityID(env.ns+"-target")
	env.deposit(t, watched, 5)
	env.deposit(t, watched, 5)

	refused := &domainRejection{evt.Rejection{Code: "apply-failed", Message: "the base is stale"}}

	prepare := func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
		if err := tx.Expect(watched, 1); err != nil {
			return nil, err
		}

		if err := tx.Read(ctx, newLedger(watched), watched); err != nil {
			return nil, err
		}

		if err := tx.Read(ctx, newLedger(target), target); err != nil {
			return nil, err
		}

		if err := tx.Append(target, &ledgerEvent{Kind: "flagged"}); err != nil {
			return nil, err
		}

		return nil, evt.RejectWithEvents(refused)
	}

	identity := evt.CommandIdentity{Type: "ledger:apply", Targets: []evt.EntityID{watched, target}}
	outcome, err := env.store.ExecuteTransaction(ctx, identity, withCommand(env.commandID("flagged")), prepare)
	require.NoError(t, err)
	require.Equal(t, encodedRejection(t, refused.rejection), encodedRejection(t, *outcome.Rejection))
	require.Len(t, env.events(t, target), 1, "the rejection's events committed, whatever the client expected")
}

func testExpectedHeadManualFlow(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	watched, target := evt.EntityID(env.ns+"-watched"), evt.EntityID(env.ns+"-target")
	env.deposit(t, watched, 5)
	env.deposit(t, watched, 5)

	identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{watched, target}}
	tx, replayed, err := env.store.BeginCommand(ctx, identity, withCommand(env.commandID("manual")))
	require.NoError(t, err)
	require.Nil(t, replayed)

	require.NoError(t, tx.Read(ctx, newLedger(watched), watched))
	require.NoError(t, tx.Expect(watched, 1), "Expect may follow the read")
	require.NoError(t, tx.Read(ctx, newLedger(target), target))
	require.NoError(t, tx.Append(target, &ledgerEvent{Kind: "deposited", Amount: 1}))

	outcome, err := tx.Complete(ctx, []byte(`{"ok":true}`))
	require.NoError(t, err)
	require.True(t, outcome.Rejected())
	require.Equal(t, evt.RejectionStaleVersion, outcome.Rejection.Code)
	require.Empty(t, outcome.Response, "the acceptance's response is not stored")
	require.Empty(t, env.events(t, target))
}

func testExpectValidation(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)
	watched := evt.EntityID(env.ns + "-watched")

	identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{watched}}
	tx, _, err := env.store.BeginCommand(ctx, identity, withCommand(env.commandID("validate")))
	require.NoError(t, err)

	require.ErrorIs(t, tx.Expect(watched, -1), evt.ErrInvalidArgument, "a negative head")
	require.NoError(t, tx.Expect(watched, 2))
	require.NoError(t, tx.Expect(watched, 2), "the same head again is fine")
	require.ErrorIs(t, tx.Expect(watched, 3), evt.ErrInvalidArgument, "one stream at two heads")

	plain := snapshots.NewStore(repo, 0).Transaction()
	require.ErrorIs(t, plain.Expect(watched, 0), evt.ErrInvalidArgument, "a store without outcomes")
}
