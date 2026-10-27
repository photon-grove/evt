package conformance

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/snapshots"
	"github.com/photon-grove/evt/go/test"
)

// retryEnv is an outcome environment whose store records the delays fence-aware retry would have
// slept, and whose repository can be interfered with between a command's load and its commit.
type retryEnv struct {
	outcomeEnv

	faulty *test.FaultRepository

	// rival is a store over the real repository: what "another writer" uses to move a stream.
	rival *snapshots.Store

	// delays are the waits the store asked for between attempts.
	delays []time.Duration
}

func newRetryEnv(t *testing.T, repo evt.OutcomeRepository, attempts int) *retryEnv {
	t.Helper()

	faulty, err := test.NewFaultRepository(repo)
	require.NoError(t, err)

	env := &retryEnv{
		outcomeEnv: newOutcomeEnv(t, faulty, 0),
		faulty:     faulty,
		rival:      snapshots.NewStore(repo, 0).WithOutcomes(),
	}

	env.store.WithRetry(snapshots.RetryPolicy{
		MaxAttempts: attempts,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    time.Second,
		Jitter:      func(time.Duration) time.Duration { return 0 },
		Sleep: func(_ context.Context, delay time.Duration) error {
			env.delays = append(env.delays, delay)

			return nil
		},
	})

	return env
}

// deposit moves a stream as another writer would: one deposited event under a fresh command ID.
func (e *retryEnv) deposit(t *testing.T, id evt.EntityID, amount int) {
	t.Helper()

	_, err := e.rival.ExecuteWithOutcome(context.Background(), newLedger(id), id,
		&ledgerCommand{Kind: "deposit", Amount: amount}, withCommand(e.commandID("rival-"+newNamespace())))
	require.NoError(t, err)
}

// interfere runs move before the commits that satisfy match, then lets them proceed. With once
// set, the first match disarms it.
func (e *retryEnv) interfere(once bool, match func(evt.SerializedResult) bool, move func()) {
	e.faulty.Intercept = func(_ context.Context, result evt.SerializedResult, commit func() error) error {
		if match(result) {
			if once {
				e.faulty.Intercept = nil
			}

			move()
		}

		return commit()
	}
}

func hasFences(result evt.SerializedResult) bool { return len(result.Fences) > 0 }
func hasEvents(result evt.SerializedResult) bool { return len(result.Events) > 0 }

// readThenCopy is a Prepare that reads a watched stream (an engine-filled fence) and copies its
// total into the target, so a decision is only valid on the watched stream's head it saw.
func readThenCopy(watched, target evt.EntityID, calls *int, saw *[]int) snapshots.Prepare {
	return func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
		*calls++

		source, destination := newLedger(watched), newLedger(target)
		if err := tx.Read(ctx, source, watched); err != nil {
			return nil, err
		}

		if err := tx.Read(ctx, destination, target); err != nil {
			return nil, err
		}

		*saw = append(*saw, source.Total)

		if err := tx.Append(target, &ledgerEvent{Kind: "deposited", Amount: source.Total}); err != nil {
			return nil, err
		}

		return []byte(strconv.Itoa(source.Total)), nil
	}
}

func encodedRejection(t *testing.T, rejection evt.Rejection) string {
	t.Helper()

	encoded, err := evt.EncodeRejection(rejection)
	require.NoError(t, err)

	return string(encoded)
}

// A fence the engine filled can be re-derived, so a conflict on it re-prepares: the command runs
// again against the fresh state and only the attempt that commits writes anything.
func testEngineFilledRePrepare(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	watched, target := evt.EntityID(env.ns+"-watched"), evt.EntityID(env.ns+"-target")
	env.deposit(t, watched, 1)

	identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{target}}
	id := env.commandID("re-prepare")

	var (
		calls int
		saw   []int
	)

	// Another writer moves the watched stream between the command's reads and its first commit.
	env.interfere(true, hasFences, func() { env.deposit(t, watched, 1) })

	outcome, err := env.store.ExecuteTransaction(ctx, identity, withCommand(id), readThenCopy(watched, target, &calls, &saw))
	require.NoError(t, err)
	require.True(t, outcome.Accepted())
	require.False(t, outcome.Replayed)
	require.Equal(t, 2, calls, "the failed fence re-prepared the command once")
	require.Equal(t, []int{1, 2}, saw, "the second attempt decided on the fresh state")
	require.Equal(t, []byte("2"), outcome.Response)
	require.Len(t, env.delays, 1, "one backoff between the two attempts")

	// The failed attempt wrote nothing: the target holds exactly the committing attempt's event.
	events := env.events(t, target)
	require.Len(t, events, 1)
	require.JSONEq(t, `{"kind":"deposited","amount":2}`, string(events[0].Payload))
	require.Equal(t, []evt.EventID{evt.GetEventID(target, 1)}, outcome.EventIDs)

	stored, err := env.repo.GetOutcome(ctx, id)
	require.NoError(t, err)
	require.Equal(t, outcome.Outcome, *stored, "exactly one outcome row, the committed one")
}

// A caller-stamped fence cannot be re-derived: when it is the item that failed the engine records
// the stale-fence rejection at once, without running the command again. A caller-stamped fence
// that still holds says nothing about an event conflict beside it, which re-prepares.
func testCallerFencedFailsFast(t *testing.T, repo evt.OutcomeRepository, enforcesConcurrency bool) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	referral, target := evt.EntityID(env.ns+"-referral"), evt.EntityID(env.ns+"-target")
	env.deposit(t, referral, 1)

	identity := evt.CommandIdentity{Type: "ledger:rename", Targets: []evt.EntityID{target}, Payload: map[string]string{"name": "Grace"}}

	var calls int

	// stamped prepares a transfer decided on the referral at the given head.
	stamped := func(head evt.EventSequence) snapshots.Prepare {
		return func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
			calls++

			if rerr := tx.Read(ctx, newLedger(target), target); rerr != nil {
				return nil, rerr
			}

			if aerr := tx.Append(target, &ledgerEvent{Kind: "deposited", Amount: 1}); aerr != nil {
				return nil, aerr
			}

			return nil, tx.Fence(evt.ReadFence{EntityID: referral, EntityType: ledgerType, Head: head})
		}
	}

	// The caller decided on the referral at head 1.
	prepare := stamped(1)

	env.interfere(true, hasEvents, func() { env.deposit(t, referral, 1) })

	id := env.commandID("fail-fast")

	outcome, err := env.store.ExecuteTransaction(ctx, identity, withCommand(id), prepare)
	require.NoError(t, err, "a stale caller-stamped fence is a persisted rejection, not an error")
	require.True(t, outcome.Rejected())
	require.Equal(t, evt.RejectionStaleFence, outcome.Rejection.Code)
	require.Equal(t, encodedRejection(t, evt.CallerFenceStaleRejection()), encodedRejection(t, *outcome.Rejection))
	require.Equal(t, 1, calls, "the command was not prepared again")
	require.Empty(t, env.delays, "and nothing waited")
	require.Empty(t, env.events(t, target), "the failed commit wrote none of its events")

	// The old command ID is terminal: it replays the stored rejection without preparing anything.
	replayed, err := env.store.ExecuteTransaction(ctx, identity, withCommand(id), prepare)
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, outcome.Outcome, replayed.Outcome)
	require.Equal(t, 1, calls)

	if !enforcesConcurrency {
		return
	}

	// A passing caller-stamped fence beside an event conflict: the failed item is the event put,
	// an ordinary conflict, so the command re-prepares and succeeds.
	calls = 0
	retryID := env.commandID("event-conflict")

	// The stale head 1 is out of date now; stamp the current one.
	prepare = stamped(2)

	env.interfere(true, hasEvents, func() { env.deposit(t, target, 5) })

	outcome, err = env.store.ExecuteTransaction(ctx, identity, withCommand(retryID), prepare)
	require.NoError(t, err)
	require.True(t, outcome.Accepted(), "a caller fence that held does not turn a conflict into a rejection: %+v", outcome.Rejection)
	require.Equal(t, 2, calls)
	require.Len(t, env.events(t, target), 2, "the rival's event and the committing attempt's")
}

// When every attempt conflicts the engine persists a stale-fence rejection as the command's
// outcome, so the command ID is terminal and a replay returns the same answer.
func testExhaustedRetries(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	watched, target := evt.EntityID(env.ns+"-watched"), evt.EntityID(env.ns+"-target")
	env.deposit(t, watched, 1)

	identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{target}}
	id := env.commandID("exhausted")

	var (
		calls int
		saw   []int
	)

	// The watched stream moves before every commit that fences it. The rejection's own outcome-only
	// commit carries no fences, so it is left alone.
	env.interfere(false, hasFences, func() { env.deposit(t, watched, 1) })

	outcome, err := env.store.ExecuteTransaction(ctx, identity, withCommand(id), readThenCopy(watched, target, &calls, &saw))
	require.NoError(t, err, "exhausted retries are a persisted rejection, not an error")
	require.True(t, outcome.Rejected())
	require.False(t, outcome.Replayed)
	require.Equal(t, evt.RejectionStaleFence, outcome.Rejection.Code)
	require.Equal(t, encodedRejection(t, evt.RetriesExhaustedRejection(3)), encodedRejection(t, *outcome.Rejection))
	require.Equal(t, 3, calls, "the command was prepared once per attempt and no more")
	require.Equal(t, []int{1, 2, 3}, saw)
	require.Len(t, env.delays, 2, "a backoff between attempts, none after the last")
	require.Less(t, env.delays[0], env.delays[1], "the delays grow")
	require.Empty(t, outcome.EventIDs)
	require.Empty(t, env.events(t, target), "no attempt committed")

	stored, err := env.repo.GetOutcome(ctx, id)
	require.NoError(t, err)
	require.Equal(t, outcome.Outcome, *stored)

	// A replay of the same command gets the same answer, and prepares nothing.
	env.faulty.Intercept = nil

	replayed, err := env.store.ExecuteTransaction(ctx, identity, withCommand(id), readThenCopy(watched, target, &calls, &saw))
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, outcome.Outcome, replayed.Outcome)
	require.Equal(t, 3, calls)

	// A different command under the same ID is still a reuse, never a replay of the rejection.
	other := evt.CommandIdentity{Type: "ledger:other", Targets: []evt.EntityID{target}}
	_, err = env.store.ExecuteTransaction(ctx, other, withCommand(id), readThenCopy(watched, target, &calls, &saw))
	require.ErrorIs(t, err, evt.ErrIdempotencyKeyReuse)

	// The remedy is a new command ID, decided on the fresh state.
	fresh, err := env.store.ExecuteTransaction(ctx, identity, withCommand(env.commandID("exhausted-retry")),
		readThenCopy(watched, target, &calls, &saw))
	require.NoError(t, err)
	require.True(t, fresh.Accepted())
	require.Len(t, env.events(t, target), 1)
}

// A command whose write set depends on the state it reads keeps one identity: the write set is
// derived per attempt, not part of the digest, so a re-prepare that derives other streams, and a
// client retry that arrives after the command landed, are the same command.
func testWriteSetChanges(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)

	index, profile := evt.EntityID(env.ns+"-index"), evt.EntityID(env.ns+"-profile")
	env.deposit(t, index, 1)

	slot := func(n int) evt.EntityID { return evt.EntityID(fmt.Sprintf("%s-slot-%d", env.ns, n)) }

	// A rename releases the slot the entity currently holds: which stream that is comes from state.
	identity := evt.CommandIdentity{
		Type: "ledger:rename", Targets: []evt.EntityID{profile}, Payload: map[string]string{"to": "grace"},
	}
	id := env.commandID("rename")

	var written []evt.EntityID

	prepare := func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
		holder := newLedger(index)
		if err := tx.Read(ctx, holder, index); err != nil {
			return nil, err
		}

		released := slot(holder.Total)
		written = append(written, released)

		for _, stream := range []evt.EntityID{profile, released} {
			if err := tx.Read(ctx, newLedger(stream), stream); err != nil {
				return nil, err
			}

			if err := tx.Append(stream, &ledgerEvent{Kind: "deposited", Amount: 1}); err != nil {
				return nil, err
			}
		}

		return nil, nil
	}

	// The holder changes under the first attempt, so the second derives another write set.
	env.interfere(true, hasFences, func() { env.deposit(t, index, 1) })

	outcome, err := env.store.ExecuteTransaction(ctx, identity, withCommand(id), prepare)
	require.NoError(t, err)
	require.True(t, outcome.Accepted(), "the re-prepared command committed: %+v", outcome.Rejection)
	require.Equal(t, []evt.EntityID{slot(1), slot(2)}, written, "the write set changed between attempts")
	require.Empty(t, env.events(t, slot(1)), "the first attempt's write set was never committed")
	require.Len(t, env.events(t, slot(2)), 1)
	require.Len(t, env.events(t, profile), 1)
	require.Equal(t, []evt.EventID{evt.GetEventID(profile, 1), evt.GetEventID(slot(2), 1)}, outcome.EventIDs)

	// The recorded digest is the identity's: the request's targets, never the streams written.
	input, err := identity.DigestInput(withCommand(id))
	require.NoError(t, err)

	digest, err := evt.DefaultDigest(input)
	require.NoError(t, err)
	require.Equal(t, digest, outcome.Digest)

	// A client retry that arrives after the rename landed is the same command, not a key reuse.
	before := len(written)

	retried, err := env.store.ExecuteTransaction(ctx, identity, withCommand(id), prepare)
	require.NoError(t, err)
	require.True(t, retried.Replayed)
	require.Equal(t, outcome.Outcome, retried.Outcome)
	require.Equal(t, before, len(written), "a replay prepares nothing")

	// So is a recovery read, which never runs the command.
	resolved, err := env.store.ResolveCommand(ctx, identity, withCommand(id))
	require.NoError(t, err)
	require.Equal(t, outcome.Outcome, resolved.Outcome)
}

// The write set is not part of a command's identity, so a transaction may write a stream its
// identity does not name.
func testWriteSetIsNotIdentity(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newOutcomeEnv(t, repo, 0)

	named := env.id
	derived := evt.EntityID(string(env.id) + "-derived")
	id := env.commandID("write-set")
	identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{named}}

	tx, replayed, err := env.store.BeginCommand(ctx, identity, withCommand(id))
	require.NoError(t, err)
	require.Nil(t, replayed)
	require.NoError(t, tx.Read(ctx, newLedger(named), named))
	require.NoError(t, tx.Read(ctx, newLedger(derived), derived))
	require.NoError(t, tx.Append(named, &ledgerEvent{Kind: "deposited", Amount: 1}))
	require.NoError(t, tx.Append(derived, &ledgerEvent{Kind: "deposited", Amount: 1}), "a stream derived from state needs no declaration")

	outcome, err := tx.Complete(ctx, nil)
	require.NoError(t, err)
	require.True(t, outcome.Accepted())
	require.Len(t, env.events(t, named), 1)
	require.Len(t, env.events(t, derived), 1)

	// The same identity replays the outcome, which lists every stream the commit wrote.
	tx, replayed, err = env.store.BeginCommand(ctx, identity, withCommand(id))
	require.NoError(t, err)
	require.Nil(t, tx)
	require.True(t, replayed.Replayed)
	require.ElementsMatch(t, []evt.EventID{evt.GetEventID(named, 1), evt.GetEventID(derived, 1)}, replayed.EventIDs)

	// A request that names a different stream is a different command.
	other := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{derived}}
	_, _, err = env.store.BeginCommand(ctx, other, withCommand(id))
	require.ErrorIs(t, err, evt.ErrIdempotencyKeyReuse)
}

// The single-aggregate shortcut fences nothing, so every conflict re-prepares: the entity is reset
// and loaded again, the handler runs again, and when attempts run out the stale-fence rejection
// is recorded and the caller's entity still reflects durable state.
func testSingleAggregateRetry(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)
	ran := counter()

	env.deposit(t, env.id, 1)

	// Another writer appends between the command's load and its commit, once.
	env.interfere(true, hasEvents, func() { env.deposit(t, env.id, 1) })

	aggregate := newLedger(env.id)

	outcome, err := env.store.ExecuteWithOutcome(ctx, aggregate, env.id,
		&ledgerCommand{Kind: "deposit", Amount: 2, ran: ran}, withCommand(env.commandID("shortcut")))
	require.NoError(t, err)
	require.True(t, outcome.Accepted())
	require.EqualValues(t, 2, ran.Load(), "the handler re-ran against the fresh state")
	require.JSONEq(t, `{"total":4}`, string(outcome.Response))
	require.Equal(t, 4, aggregate.Total, "events applied by the failed attempt were reset, not double-counted")
	require.Len(t, env.events(t, env.id), 3)

	// Every attempt loses: the rejection is recorded and the entity is what storage holds.
	exhausted := env.commandID("shortcut-exhausted")
	env.interfere(false, hasEvents, func() { env.deposit(t, env.id, 1) })
	ran.Store(0)

	aggregate = newLedger(env.id)

	outcome, err = env.store.ExecuteWithOutcome(ctx, aggregate, env.id,
		&ledgerCommand{Kind: "deposit", Amount: 2, ran: ran}, withCommand(exhausted))
	require.NoError(t, err)
	require.True(t, outcome.Rejected())
	require.Equal(t, encodedRejection(t, evt.RetriesExhaustedRejection(3)), encodedRejection(t, *outcome.Rejection))
	require.EqualValues(t, 3, ran.Load())

	durable := env.events(t, env.id)
	require.Len(t, durable, 6, "three rival events; the command committed none")
	require.Equal(t, 7, aggregate.Total, "the caller's entity reflects durable state")

	env.faulty.Intercept = nil

	replayed, err := env.store.ExecuteWithOutcome(ctx, newLedger(env.id), env.id,
		&ledgerCommand{Kind: "deposit", Amount: 2, ran: ran}, withCommand(exhausted))
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, outcome.Outcome, replayed.Outcome)
	require.EqualValues(t, 3, ran.Load())
}

// The identity no longer limits a command's write set, so the commit is what refuses a reserved
// stream: before any write, and without preparing the command again.
func testReservedWriteRefused(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newRetryEnv(t, repo, 3)
	identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{env.id}}

	for _, reserved := range []evt.EntityID{"SYSTEM#retry-case", "command-outcome#retry-case"} {
		var calls int

		id := env.commandID("reserved-" + newNamespace())

		_, err := env.store.ExecuteTransaction(ctx, identity, withCommand(id),
			func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
				calls++

				if rerr := tx.Read(ctx, newLedger(reserved), reserved); rerr != nil {
					return nil, rerr
				}

				return nil, tx.Append(reserved, &ledgerEvent{Kind: "deposited", Amount: 1})
			})
		require.True(t, evt.IsReservedEntityIdErr(err), "%s: %v", reserved, err)
		require.Equal(t, 1, calls, "%s: a refusal is not retried", reserved)
		env.requireNoOutcome(t, id)
	}
}
