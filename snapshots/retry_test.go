package snapshots_test

// These tests pin the policy side of fence-aware retry (ADR 0004, C5) over the in-memory
// repository with scripted commit failures: the bounded attempts, the backoff, what is retried and
// what is returned, and how the entity is reset. How retry interacts with real conflicts and
// fences on each backend is covered by the shared conformance suite.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		Jitter: func(time.Duration) time.Duration { return 0 },
	})

	outcome, err := store.ExecuteWithOutcome(context.Background(), etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.True(t, outcome.Rejected())
	require.Equal(t, evt.RejectionStaleFence, outcome.Rejection.Code)

	// With no jitter each delay is the bottom of [delay/2, delay]: 10 ms doubling, capped at 25 ms.
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

func TestRetry_UnsetDelaysTakeTheDefaultsAndImmediateOptsOut(t *testing.T) {
	// A policy that only sets the attempts still backs off, like Rust's RetryPolicy::new.
	waits := &delays{}
	store, repo := retryStore(t, conflicts(2), snapshots.RetryPolicy{MaxAttempts: 3, Sleep: waits.sleep})

	outcome, err := store.ExecuteWithOutcome(context.Background(), etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.True(t, outcome.Accepted())
	require.EqualValues(t, 3, repo.commits.Load())
	require.Len(t, waits.waited, 2)

	// Jittered into [delay/2, delay] of 25 ms and 50 ms.
	require.GreaterOrEqual(t, waits.waited[0], 12500*time.Microsecond)
	require.LessOrEqual(t, waits.waited[0], snapshots.DefaultRetryBaseDelay)
	require.GreaterOrEqual(t, waits.waited[1], snapshots.DefaultRetryBaseDelay)
	require.LessOrEqual(t, waits.waited[1], 2*snapshots.DefaultRetryBaseDelay)

	// Immediate never waits, whatever the delay fields say.
	waits = &delays{}
	immediate := snapshots.ImmediateRetryPolicy(3)
	immediate.BaseDelay, immediate.Sleep = time.Hour, waits.sleep

	store, repo = retryStore(t, conflicts(2), immediate)

	outcome, err = store.ExecuteWithOutcome(context.Background(), etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.True(t, outcome.Accepted())
	require.EqualValues(t, 3, repo.commits.Load())
	require.Empty(t, waits.waited)
}

func TestRetry_ADeadlineEndsTheRetries(t *testing.T) {
	store, repo := retryStore(t, conflicts(10), snapshots.RetryPolicy{
		MaxAttempts: 5, Immediate: true, Deadline: time.Nanosecond,
	})

	outcome, err := store.ExecuteWithOutcome(context.Background(), etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.True(t, outcome.Rejected(), "a conflict past the deadline is recorded like exhausted attempts")
	require.EqualValues(t, 1, repo.commits.Load())

	exhausted, err := evt.CanonicalRejection(evt.RetriesExhaustedRejection(1))
	require.NoError(t, err)
	require.Equal(t, exhausted, *outcome.Rejection, "the attempts made")

	// A transient failure past the deadline is returned, as when attempts run out.
	throttled := &policy.ClassifiedError{Class: policy.ClassTransient, Err: errors.New("throttled")}
	store, repo = retryStore(t, []error{throttled, throttled, throttled}, snapshots.RetryPolicy{
		MaxAttempts: 5, Immediate: true, Deadline: time.Nanosecond,
	})

	_, err = store.ExecuteWithOutcome(context.Background(), etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-2"))
	require.ErrorIs(t, err, throttled)
	require.EqualValues(t, 1, repo.commits.Load())

	// Without one, every attempt runs.
	store, repo = retryStore(t, conflicts(2), snapshots.ImmediateRetryPolicy(5))

	outcome, err = store.ExecuteWithOutcome(context.Background(), etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-3"))
	require.NoError(t, err)
	require.True(t, outcome.Accepted())
	require.EqualValues(t, 3, repo.commits.Load())
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

// tally is an entity whose events accumulate: replaying an event twice doubles its effect, so a
// retry that failed to reset it would be caught.
type tally struct {
	id     evt.EntityID
	Total  int            `json:"total"`
	Counts map[string]int `json:"counts"`
}

func newTally(id evt.EntityID) *tally { return &tally{id: id, Counts: map[string]int{}} }

const tallyType evt.EntityType = "retry-tally"

func (t *tally) Type() evt.EntityType  { return tallyType }
func (t *tally) GetID() evt.EntityID   { return t.id }
func (t *tally) SetID(id evt.EntityID) { t.id = id }
func (t *tally) Base() evt.BaseEntity { //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	return evt.BaseEntity{ID: t.id} //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
}
func (t *tally) EventUpcasters() []evt.EventUpcaster { return nil }
func (t *tally) Projectors() []evt.EventProjector    { return nil }

type tallied struct {
	N int `json:"n"`
}

func (*tallied) Type() evt.EventType        { return "retry-tally:tallied" }
func (*tallied) Version() evt.EventVersion  { return 1 }
func (*tallied) EntityType() evt.EntityType { return tallyType }
func (*tallied) EntityID() evt.EntityID     { return "" }

func (t *tally) Apply(event evt.Event) error {
	tallied, ok := event.(*tallied)
	if !ok {
		return evt.NewBadEventError(event)
	}

	if t.Counts == nil {
		t.Counts = map[string]int{}
	}

	t.Total += tallied.N
	t.Counts["tallied"]++

	return nil
}

func (t *tally) DeserializeEvent(serialized evt.SerializedEvent) (evt.Event, error) {
	var event tallied
	if err := json.Unmarshal(serialized.Payload, &event); err != nil {
		return nil, err
	}

	return &event, nil
}

// tally adds N. Attempts is set by the handler on every run, so it is part of the command's JSON
// but not of what the caller asked for.
type tallyAdd struct {
	N        int `json:"n"`
	Attempts int `json:"attempts"`
}

func (*tallyAdd) Type() evt.CommandType      { return "retry-tally:add" }
func (*tallyAdd) EntityType() evt.EntityType { return tallyType }

func (t *tally) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
	add, ok := command.(*tallyAdd)
	if !ok {
		return evt.CommandResult{}, evt.NewBadCommandError(command)
	}

	add.Attempts++

	return evt.CommandResult{Events: []evt.Event{&tallied{N: add.N}}}, nil
}

func TestRetry_ARetriedCommandNeverAppliesItsEventsTwice(t *testing.T) {
	ctx := context.Background()
	store, repo := retryStore(t, conflicts(2), snapshots.ImmediateRetryPolicy(3))
	entity := newTally("t-1")

	outcome, err := store.ExecuteWithOutcome(ctx, entity, "t-1", &tallyAdd{N: 5}, commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.True(t, outcome.Accepted())
	require.EqualValues(t, 3, repo.commits.Load())

	// Two failed attempts applied the event to the entity before their commits failed.
	require.Equal(t, 5, entity.Total, "the failed attempts' events were reset, not accumulated")
	require.Equal(t, map[string]int{"tallied": 1}, entity.Counts, "and so was the exported map")

	events, err := repo.GetEvents(ctx, "t-1")
	require.NoError(t, err)
	require.Len(t, events, 1)

	// When the attempts run out the caller's entity is what storage holds: nothing.
	repo.failures, repo.commits = conflicts(10), atomic.Int32{}
	entity = newTally("t-2")

	outcome, err = store.ExecuteWithOutcome(ctx, entity, "t-2", &tallyAdd{N: 5}, commandMetadata("cmd-2"))
	require.NoError(t, err)
	require.True(t, outcome.Rejected())
	require.Zero(t, entity.Total)
	require.Empty(t, entity.Counts)
}

func TestRetry_TheDigestIsTakenOnceBeforeTheFirstAttempt(t *testing.T) {
	ctx := context.Background()
	store, repo := retryStore(t, conflicts(2), snapshots.ImmediateRetryPolicy(3))

	// Every run of the handler changes the command's JSON (Attempts), but not what was asked for.
	command := &tallyAdd{N: 5}

	input, err := evt.CommandIdentity{Type: command.Type(), Targets: []evt.EntityID{"t-1"}, Payload: &tallyAdd{N: 5}}.DigestInput(commandMetadata("cmd-1"))
	require.NoError(t, err)

	want, err := evt.DefaultDigest(input)
	require.NoError(t, err)

	outcome, err := store.ExecuteWithOutcome(ctx, newTally("t-1"), "t-1", command, commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.EqualValues(t, 3, repo.commits.Load())
	require.Equal(t, 3, command.Attempts, "the handler ran three times")
	require.Equal(t, want, outcome.Digest, "the digest is the request's, not the command as the last attempt left it")

	// The same request replays: it does not look like another command under the same ID.
	replayed, err := store.ExecuteWithOutcome(ctx, newTally("t-1"), "t-1", &tallyAdd{N: 5}, commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
}

// flakyReadRepository fails the first reads of a stream with a scripted error.
type flakyReadRepository struct {
	evt.OutcomeRepository

	failures []error
	reads    atomic.Int32
}

func (r *flakyReadRepository) GetEvents(ctx context.Context, id evt.EntityID) ([]evt.SerializedEvent, error) {
	if read := int(r.reads.Add(1)) - 1; read < len(r.failures) {
		return nil, r.failures[read]
	}

	return r.OutcomeRepository.GetEvents(ctx, id)
}

func TestRetry_ATransientReadInsidePrepareIsRetriedButNeverRecorded(t *testing.T) {
	ctx := context.Background()
	identity := evt.CommandIdentity{Type: "test:create", Targets: []evt.EntityID{"e-1"}}
	throttled := &policy.ClassifiedError{Class: policy.ClassTransient, Err: errors.New("throttled")}

	// The function wraps the load error, as real code does.
	prepare := func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
		entity := etest.NewEntity("e-1")
		if err := tx.Read(ctx, entity, "e-1"); err != nil {
			return nil, fmt.Errorf("read the entity: %w", err)
		}

		return nil, tx.Append("e-1", &etest.EntityCreated{ID: "e-1", Value: "v"})
	}

	repo := &flakyReadRepository{OutcomeRepository: memOutcomes(t), failures: []error{throttled}}
	store := snapshots.NewStore(repo, 0).WithOutcomes().WithRetry(snapshots.ImmediateRetryPolicy(3))

	outcome, err := store.ExecuteTransaction(ctx, identity, commandMetadata("cmd-1"), prepare)
	require.NoError(t, err, "a transient read inside prepare is retried")
	require.True(t, outcome.Accepted())
	require.EqualValues(t, 2, repo.reads.Load())

	// A read that keeps failing is returned when the attempts run out, with nothing recorded.
	repo = &flakyReadRepository{OutcomeRepository: memOutcomes(t), failures: []error{throttled, throttled, throttled, throttled}}
	store = snapshots.NewStore(repo, 0).WithOutcomes().WithRetry(snapshots.ImmediateRetryPolicy(3))

	_, err = store.ExecuteTransaction(ctx, identity, commandMetadata("cmd-2"), prepare)
	require.ErrorIs(t, err, throttled)
	require.EqualValues(t, 3, repo.reads.Load())

	stored, err := repo.GetOutcome(ctx, "cmd-2")
	require.NoError(t, err)
	require.Nil(t, stored, "a failed read says nothing about the decision going stale")

	// A transient error the function makes up itself, not from a Read, is its own failure.
	repo = &flakyReadRepository{OutcomeRepository: memOutcomes(t)}
	store = snapshots.NewStore(repo, 0).WithOutcomes().WithRetry(snapshots.ImmediateRetryPolicy(3))

	var runs int

	_, err = store.ExecuteTransaction(ctx, identity, commandMetadata("cmd-3"),
		func(context.Context, *snapshots.Transaction) ([]byte, error) {
			runs++

			return nil, throttled
		})
	require.ErrorIs(t, err, throttled)
	require.Equal(t, 1, runs, "only a failed Read the function passes on is a load failure")
}

func TestRetry_AnAmbiguousWriteOnTheLastAttemptIsAnErrorNotARejection(t *testing.T) {
	ctx := context.Background()
	ambiguous := &evt.AmbiguousWriteError{CommandID: "cmd-1", Cause: errors.New("connection reset")}
	repo := &scriptedRepository{OutcomeRepository: memOutcomes(t), failures: []error{ambiguous, ambiguous}}
	store := snapshots.NewStore(repo, 0).
		WithOutcomes(snapshots.WithRecoveryWait(10 * time.Millisecond)).
		WithRetry(snapshots.ImmediateRetryPolicy(2))

	_, err := store.ExecuteWithOutcome(ctx, etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
	require.ErrorIs(t, err, evt.ErrAmbiguousWrite)
	require.True(t, policy.IsTransient(err))
	require.EqualValues(t, 2, repo.commits.Load(), "both attempts ran")

	stored, err := repo.GetOutcome(ctx, "cmd-1")
	require.NoError(t, err)
	require.Nil(t, stored, "an absent row proves nothing, so no rejection is recorded")
}

// corruptingRepository serves a malformed outcome row after cleanReads reads, and fails every
// commit that writes events as an ambiguous write.
type corruptingRepository struct {
	evt.OutcomeRepository

	cleanReads int
	reads      atomic.Int32
	commits    atomic.Int32
}

func (r *corruptingRepository) GetOutcome(ctx context.Context, id evt.CommandID) (*evt.Outcome, error) {
	if int(r.reads.Add(1)) > r.cleanReads {
		return nil, &evt.CorruptOutcomeError{CommandID: id, Reason: "the digest is missing"}
	}

	return r.OutcomeRepository.GetOutcome(ctx, id)
}

func (r *corruptingRepository) Commit(ctx context.Context, result evt.SerializedResult) error {
	if len(result.Events) > 0 {
		r.commits.Add(1)

		return &evt.AmbiguousWriteError{CommandID: result.Outcome.CommandID, Cause: errors.New("lost")}
	}

	return r.OutcomeRepository.Commit(ctx, result)
}

func TestRetry_ACorruptOutcomeIsNeverRePreparedOrRecorded(t *testing.T) {
	ctx := context.Background()

	// Found by the pre-check.
	repo := &corruptingRepository{OutcomeRepository: memOutcomes(t)}
	store := snapshots.NewStore(repo, 0).WithOutcomes().WithRetry(snapshots.ImmediateRetryPolicy(5))

	_, err := store.ExecuteWithOutcome(ctx, etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
	require.ErrorIs(t, err, evt.ErrCorruptOutcome)
	require.EqualValues(t, 1, repo.reads.Load(), "not read again")
	require.Zero(t, repo.commits.Load(), "and nothing ran")

	// Found by ambiguous-write recovery after the first attempt's commit.
	repo = &corruptingRepository{OutcomeRepository: memOutcomes(t), cleanReads: 1}
	store = snapshots.NewStore(repo, 0).
		WithOutcomes(snapshots.WithRecoveryWait(10 * time.Millisecond)).
		WithRetry(snapshots.ImmediateRetryPolicy(5))

	_, err = store.ExecuteWithOutcome(ctx, etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-2"))
	require.ErrorIs(t, err, evt.ErrCorruptOutcome)
	require.EqualValues(t, 1, repo.commits.Load(), "the command was not prepared again")

	stored, err := repo.OutcomeRepository.GetOutcome(ctx, "cmd-2")
	require.NoError(t, err)
	require.Nil(t, stored, "and no rejection was recorded over the corrupt row")
}

func TestRetry_AReservedStreamIsStillRefusedWithoutAnIdentityCheck(t *testing.T) {
	ctx := context.Background()
	store, repo := retryStore(t, nil, snapshots.ImmediateRetryPolicy(3))
	identity := evt.CommandIdentity{Type: "test:create", Targets: []evt.EntityID{"e-1"}}

	for _, reserved := range []evt.EntityID{"SYSTEM#e-1", "command-outcome#e-1"} {
		// The reserved stream is not named by the identity, which no longer limits the write set,
		// but the commit still refuses it, before any write.
		_, err := store.ExecuteTransaction(ctx, identity, commandMetadata("cmd-"+string(reserved)),
			func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
				if err := tx.Read(ctx, etest.NewEntity(reserved), reserved); err != nil {
					return nil, err
				}

				return nil, tx.Append(reserved, &etest.EntityCreated{ID: reserved, Value: "v"})
			})
		require.True(t, evt.IsReservedEntityIdErr(err), "%s: %v", reserved, err)

		events, err := repo.GetEvents(ctx, "e-1")
		require.NoError(t, err)
		require.Empty(t, events)
	}
}
