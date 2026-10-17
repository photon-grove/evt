package snapshots_test

// These tests pin the policy side of fence-aware retry (ADR 0004, C5) over the in-memory
// repository with scripted commit failures: the bounded attempts, the backoff, what is retried and
// what is returned, and how the entity is reset. How retry interacts with real conflicts and
// fences on each backend is covered by the shared conformance suite.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
	"github.com/photon-grove/evt/snapshots"
	etest "github.com/photon-grove/evt/test"
)

// scriptedRepository fails the commits that write events with the scripted errors, one per
// attempt, then lets them through. Outcome-only commits (the stale-fence rejection) are untouched.
type scriptedRepository struct {
	evt.OutcomeRepository

	failures []error
	commits  atomic.Int32
}

func (r *scriptedRepository) Commit(ctx context.Context, result evt.SerializedResult) error {
	if len(result.Events) > 0 {
		attempt := int(r.commits.Add(1)) - 1
		if attempt < len(r.failures) {
			return r.failures[attempt]
		}
	}

	return r.OutcomeRepository.Commit(ctx, result)
}

func conflicts(n int) []error {
	failures := make([]error, n)
	for i := range failures {
		failures[i] = evt.NewConflictError("another writer committed this sequence")
	}

	return failures
}

// delays records the waits a store asks for instead of sleeping.
type delays struct{ waited []time.Duration }

func (d *delays) sleep(_ context.Context, delay time.Duration) error {
	d.waited = append(d.waited, delay)

	return nil
}

func retryStore(t *testing.T, failures []error, retry snapshots.RetryPolicy) (*snapshots.Store, *scriptedRepository) {
	t.Helper()

	repo := &scriptedRepository{OutcomeRepository: memOutcomes(t), failures: failures}

	return snapshots.NewStore(repo, 0).WithOutcomes().WithRetry(retry), repo
}

func TestRetry_ABackoffGrowsAndIsBounded(t *testing.T) {
	waits := &delays{}
	store, _ := retryStore(t, conflicts(10), snapshots.RetryPolicy{
		MaxAttempts: 4, BaseDelay: 10 * time.Millisecond, MaxDelay: 25 * time.Millisecond, Sleep: waits.sleep,
	})

	outcome, err := store.ExecuteWithOutcome(context.Background(), etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.True(t, outcome.Rejected())
	require.Equal(t, evt.RejectionStaleFence, outcome.Rejection.Code)

	// Without jitter each delay is the bottom of [delay/2, delay]: 10 ms doubling, capped at 25 ms.
	require.Equal(t, []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 12500 * time.Microsecond}, waits.waited)

	exhausted, err := evt.CanonicalRejection(evt.RetriesExhaustedRejection(4))
	require.NoError(t, err)
	require.Equal(t, exhausted, *outcome.Rejection, "the attempt count is the policy's")
}

func TestRetry_AttemptsBelowOneMeanOne(t *testing.T) {
	waits := &delays{}
	store, repo := retryStore(t, conflicts(10), snapshots.RetryPolicy{MaxAttempts: 0, BaseDelay: time.Second, MaxDelay: time.Second, Sleep: waits.sleep})

	outcome, err := store.ExecuteWithOutcome(context.Background(), etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.True(t, outcome.Rejected(), "the first conflict is recorded without a retry")
	require.EqualValues(t, 1, repo.commits.Load())
	require.Empty(t, waits.waited)
}

func TestRetry_ARetriedCommandCommitsOnce(t *testing.T) {
	waits := &delays{}
	store, repo := retryStore(t, conflicts(2), snapshots.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Second, Sleep: waits.sleep})
	entity := etest.NewEntity("e-1")

	outcome, err := store.ExecuteWithOutcome(context.Background(), entity, "e-1", createCommand(), commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.True(t, outcome.Accepted())
	require.EqualValues(t, 3, repo.commits.Load())
	require.Len(t, waits.waited, 2)

	events, err := repo.GetEvents(context.Background(), "e-1")
	require.NoError(t, err)
	require.Len(t, events, 1, "only the attempt that committed wrote anything")
	require.Equal(t, "created", entity.Value)
}

func TestRetry_ATransientFailureIsReturnedWhenAttemptsRunOut(t *testing.T) {
	throttled := &policy.ClassifiedError{Class: policy.ClassTransient, Err: errors.New("throttled")}
	waits := &delays{}
	store, repo := retryStore(t, []error{throttled, throttled, throttled, throttled}, snapshots.RetryPolicy{
		MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Second, Sleep: waits.sleep,
	})

	_, err := store.ExecuteWithOutcome(context.Background(), etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
	require.ErrorIs(t, err, throttled)
	require.True(t, policy.IsTransient(err))
	require.EqualValues(t, 3, repo.commits.Load())
	require.Len(t, waits.waited, 2)

	stored, err := repo.GetOutcome(context.Background(), "cmd-1")
	require.NoError(t, err)
	require.Nil(t, stored, "nothing says the state went stale, so no rejection is recorded")
}

func TestRetry_AnInternalFailureIsNeverRetried(t *testing.T) {
	waits := &delays{}
	store, repo := retryStore(t, nil, snapshots.RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: time.Second, Sleep: waits.sleep})

	// A handler that returns a domain ConflictError (a duplicate resource) is an internal failure,
	// not an optimistic-concurrency conflict.
	entity := &conflictingEntity{Entity: etest.NewEntity("e-1")}

	_, err := store.ExecuteWithOutcome(context.Background(), entity, "e-1", createCommand(), commandMetadata("cmd-1"))
	require.Error(t, err)
	require.True(t, evt.IsConflictErr(err))
	require.Empty(t, waits.waited, "the handler's own error is returned at once")
	require.EqualValues(t, 0, repo.commits.Load())

	stored, err := repo.GetOutcome(context.Background(), "cmd-1")
	require.NoError(t, err)
	require.Nil(t, stored)
}

// conflictingEntity is an entity whose handler fails with a domain ConflictError.
type conflictingEntity struct{ *etest.Entity }

func (*conflictingEntity) Handle(context.Context, evt.Command) (evt.CommandResult, error) {
	return evt.CommandResult{}, evt.NewConflictError("handle is taken")
}

func TestRetry_AnEntityThatCannotBeResetIsAttemptedOnce(t *testing.T) {
	waits := &delays{}
	store, repo := retryStore(t, conflicts(10), snapshots.RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: time.Second, Sleep: waits.sleep})

	_, err := store.ExecuteWithOutcome(context.Background(), valueEntity{Entity: etest.NewEntity("e-1")}, "e-1", createCommand(), commandMetadata("cmd-1"))
	require.Error(t, err)
	require.True(t, evt.IsConflictErr(err), "the conflict is returned as is")
	require.EqualValues(t, 1, repo.commits.Load(), "applying events twice would double-count them")
	require.Empty(t, waits.waited)

	stored, err := repo.GetOutcome(context.Background(), "cmd-1")
	require.NoError(t, err)
	require.Nil(t, stored)
}

// valueEntity is an entity passed by value (its state lives behind the embedded pointer), which the
// engine cannot reset between attempts.
type valueEntity struct{ *etest.Entity }

func TestRetry_TheEntityReflectsDurableStateAfterTheAttemptsRanOut(t *testing.T) {
	store, _ := retryStore(t, conflicts(10), snapshots.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Second, Sleep: (&delays{}).sleep})
	entity := etest.NewEntity("e-1")

	outcome, err := store.ExecuteWithOutcome(context.Background(), entity, "e-1", createCommand(), commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.True(t, outcome.Rejected())
	require.Empty(t, entity.Value, "events applied by the failed attempts were discarded")
}

func TestRetry_ACancelledContextEndsTheBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, repo := retryStore(t, conflicts(10), snapshots.RetryPolicy{
		MaxAttempts: 5, BaseDelay: time.Hour, MaxDelay: time.Hour,
	})

	cancel()

	_, err := store.ExecuteWithOutcome(ctx, etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, evt.IsConflictErr(err), "the conflict that was being retried is not lost: %v", err)
	require.EqualValues(t, 1, repo.commits.Load())
}

func TestRetry_ATransactionIsPreparedAgainOnlyForRetryableFailures(t *testing.T) {
	ctx := context.Background()
	waits := &delays{}
	store, _ := retryStore(t, conflicts(1), snapshots.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Second, Sleep: waits.sleep})
	identity := evt.CommandIdentity{Type: "test:create", Targets: []evt.EntityID{"e-1"}}

	var prepared int

	outcome, err := store.ExecuteTransaction(ctx, identity, commandMetadata("cmd-1"),
		func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
			prepared++

			entity := etest.NewEntity("e-1")
			if err := tx.Read(ctx, entity, "e-1"); err != nil {
				return nil, err
			}

			return nil, tx.Append("e-1", &etest.EntityCreated{ID: "e-1", Value: "v"})
		})
	require.NoError(t, err)
	require.True(t, outcome.Accepted())
	require.Equal(t, 2, prepared)
	require.Len(t, waits.waited, 1)

	// A prepare function's own error is an internal failure: returned at once, no outcome.
	prepared = 0
	boom := errors.New("storage exploded")

	_, err = store.ExecuteTransaction(ctx, identity, commandMetadata("cmd-2"),
		func(context.Context, *snapshots.Transaction) ([]byte, error) {
			prepared++

			return nil, evt.NewConflictError("handle is taken")
		})
	require.True(t, evt.IsConflictErr(err))
	require.Equal(t, 1, prepared, "a domain ConflictError from prepare is not a concurrency conflict")

	_, err = store.ExecuteTransaction(ctx, identity, commandMetadata("cmd-3"),
		func(context.Context, *snapshots.Transaction) ([]byte, error) { return nil, boom })
	require.ErrorIs(t, err, boom)

	// A rejection is recorded, fenced on what the prepare function read.
	rejected, err := store.ExecuteTransaction(ctx, identity, commandMetadata("cmd-4"),
		func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
			if rerr := tx.Read(ctx, etest.NewEntity("e-1"), "e-1"); rerr != nil {
				return nil, rerr
			}

			return nil, &denied{}
		})
	require.NoError(t, err)
	require.True(t, rejected.Rejected())
	require.Equal(t, evt.RejectionCode("denied"), rejected.Rejection.Code)
}

type denied struct{}

func (*denied) Error() string { return "denied" }
func (*denied) Rejection() evt.Rejection {
	return evt.Rejection{Code: "denied", Message: "not allowed"}
}

func TestRetry_ADelayOfZeroRetriesWithoutWaiting(t *testing.T) {
	for name, retry := range map[string]snapshots.RetryPolicy{
		"no base delay": {MaxAttempts: 3, MaxDelay: time.Second},
		"no max delay":  {MaxAttempts: 3, BaseDelay: time.Second},
	} {
		waits := &delays{}
		retry.Sleep = waits.sleep

		store, repo := retryStore(t, conflicts(2), retry)

		outcome, err := store.ExecuteWithOutcome(context.Background(), etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
		require.NoError(t, err, name)
		require.True(t, outcome.Accepted(), name)
		require.EqualValues(t, 3, repo.commits.Load(), name)
		require.Empty(t, waits.waited, name)
	}
}

func TestRetry_ATransactionNeedsAPrepareFunction(t *testing.T) {
	store, _ := retryStore(t, nil, snapshots.DefaultRetryPolicy())

	_, err := store.ExecuteTransaction(context.Background(), evt.CommandIdentity{Type: "t"}, commandMetadata("cmd-1"), nil)
	require.ErrorIs(t, err, evt.ErrInvalidArgument)
}

// gappedRepository returns an event stream that skips a sequence, as a load that raced a
// compaction (or one over expired events) sees it.
type gappedRepository struct{ evt.OutcomeRepository }

func (gappedRepository) GetEvents(_ context.Context, id evt.EntityID) ([]evt.SerializedEvent, error) {
	return []evt.SerializedEvent{{ID: evt.GetEventID(id, 2), EntityID: id, Sequence: 2}}, nil
}

func TestRetry_ALoadGapIsRetriedButNeverRecordedAsStale(t *testing.T) {
	ctx := context.Background()
	waits := &delays{}
	repo := gappedRepository{memOutcomes(t)}
	store := snapshots.NewStore(repo, 0).WithOutcomes().WithRetry(snapshots.RetryPolicy{
		MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Second, Sleep: waits.sleep,
	})

	// The single-aggregate shortcut.
	_, err := store.ExecuteWithOutcome(ctx, etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
	require.ErrorIs(t, err, evt.ErrSequenceGap, "the gap is returned, not turned into a rejection")
	require.Len(t, waits.waited, 2, "but it is retried like any transient failure")

	stored, err := repo.GetOutcome(ctx, "cmd-1")
	require.NoError(t, err)
	require.Nil(t, stored, "a read that never became consistent says nothing about the decision going stale")

	// A multi-stream command reading the same stream.
	waits.waited = nil
	identity := evt.CommandIdentity{Type: "test:create", Targets: []evt.EntityID{"e-1"}}

	_, err = store.ExecuteTransaction(ctx, identity, commandMetadata("cmd-2"),
		func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
			return nil, tx.Read(ctx, etest.NewEntity("e-1"), "e-1")
		})
	require.ErrorIs(t, err, evt.ErrSequenceGap)
	require.Len(t, waits.waited, 2)

	stored, err = repo.GetOutcome(ctx, "cmd-2")
	require.NoError(t, err)
	require.Nil(t, stored)
}
