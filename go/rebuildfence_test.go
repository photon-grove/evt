package evt_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
)

func TestPlanRebuildBegin_DecidesFromTheObservedState(t *testing.T) {
	now := time.Unix(1_790_000_000, 0)
	lock := func(owner string, expiry int64) *evt.RebuildLock {
		return &evt.RebuildLock{Owner: owner, LeaseExpiresAt: time.Unix(expiry, 0)}
	}

	tests := []struct {
		name  string
		state evt.RebuildState
		want  evt.RebuildPlan
	}{
		{
			name: "a store that never ran a rebuild acquires, growing the generation by one",
			want: evt.RebuildPlan{Kind: evt.RebuildAcquire, Increment: 1, SeenEven: 0, SeenOdd: evt.RebuildNoMatch},
		},
		{
			name:  "a settled generation acquires from the value it saw",
			state: evt.RebuildState{Generation: evt.RebuildGeneration{Value: 4}},
			want:  evt.RebuildPlan{Kind: evt.RebuildAcquire, Increment: 1, SeenEven: 4, SeenOdd: evt.RebuildNoMatch},
		},
		{
			name:  "a force-released generation acquires, adding two so it stays odd",
			state: evt.RebuildState{Generation: evt.RebuildGeneration{Value: 5, ForceReleased: true}},
			want:  evt.RebuildPlan{Kind: evt.RebuildAcquireForceReleased, Increment: 2, SeenEven: evt.RebuildNoMatch, SeenOdd: 5},
		},
		{
			name: "an expired lease is taken over, observing its owner",
			state: evt.RebuildState{
				Generation: evt.RebuildGeneration{Value: 5},
				Lock:       lock("job-a", now.Unix()-1),
			},
			want: evt.RebuildPlan{
				Kind: evt.RebuildTakeover, Increment: 2, SeenEven: evt.RebuildNoMatch, SeenOdd: 5, ObservedOwner: "job-a",
			},
		},
		{
			name: "a takeover during a force-released rebuild observes the odd generation",
			state: evt.RebuildState{
				Generation: evt.RebuildGeneration{Value: 7, ForceReleased: true},
				Lock:       lock("job-b", now.Unix()-30),
			},
			want: evt.RebuildPlan{
				Kind: evt.RebuildTakeover, Increment: 2, SeenEven: evt.RebuildNoMatch, SeenOdd: 7, ObservedOwner: "job-b",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := evt.PlanRebuildBegin(test.state, now)
			require.NoError(t, err)
			require.Equal(t, test.want, plan)
		})
	}
}

func TestPlanRebuildBegin_RefusesWhatItCannotUse(t *testing.T) {
	now := time.Unix(1_790_000_000, 0)

	// A lease is still held at the second it expires.
	_, err := evt.PlanRebuildBegin(evt.RebuildState{
		Generation: evt.RebuildGeneration{Value: 3},
		Lock:       &evt.RebuildLock{Owner: "job-a", LeaseExpiresAt: now},
	}, now)

	var held *evt.RebuildLockHeldError
	require.ErrorAs(t, err, &held)
	require.Equal(t, "job-a", held.Owner)
	require.Equal(t, now.Unix(), held.LeaseExpiresAt.Unix())

	// Two states the protocol cannot produce are reported, not repaired.
	for name, state := range map[string]evt.RebuildState{
		"odd generation, no lock, no flag": {Generation: evt.RebuildGeneration{Value: 3}},
		"lock row, even generation": {
			Generation: evt.RebuildGeneration{Value: 4},
			Lock:       &evt.RebuildLock{Owner: "job-a", LeaseExpiresAt: now.Add(time.Hour)},
		},
	} {
		_, err = evt.PlanRebuildBegin(state, now)
		require.True(t, evt.IsRebuildStateInvalidErr(err), name)
	}
}

func TestRebuildGeneration_IsSettledWhenEven(t *testing.T) {
	require.True(t, evt.RebuildGeneration{}.Settled(), "an absent item reads as zero")
	require.True(t, evt.RebuildGeneration{Value: 6}.Settled())
	require.False(t, evt.RebuildGeneration{Value: 7}.Settled())
	require.False(t, evt.RebuildGeneration{Value: 7, ForceReleased: true}.Settled())
}

func TestRebuildLock_ExpiresAfterItsLastSecond(t *testing.T) {
	lock := evt.RebuildLock{Owner: "job", LeaseExpiresAt: time.Unix(100, 0)}

	require.False(t, lock.Expired(time.Unix(99, 0)))
	require.False(t, lock.Expired(time.Unix(100, 999_000_000)), "the lease is held through its expiry second")
	require.True(t, lock.Expired(time.Unix(101, 0)))
	require.True(t, evt.RebuildState{Lock: &lock}.InProgress(), "an expired lock row is still a rebuild in progress")
	require.False(t, evt.RebuildState{}.InProgress())
}

func TestCheckRebuildOwner(t *testing.T) {
	require.NoError(t, evt.CheckRebuildOwner("job-1"))
	require.NoError(t, evt.CheckRebuildOwner(strings.Repeat("o", evt.MaxRebuildOwnerBytes)))
	require.NoError(t, evt.CheckRebuildOwner("é"))

	for name, owner := range map[string]string{
		"empty":     "",
		"oversized": strings.Repeat("o", evt.MaxRebuildOwnerBytes+1),
		"not UTF-8": "\xff\xfe",
		"has NUL":   "job\x00",
	} {
		require.ErrorIs(t, evt.CheckRebuildOwner(owner), evt.ErrInvalidArgument, name)
	}
}

func TestRebuildLeaseSeconds_RoundsUpToWholeSeconds(t *testing.T) {
	for lease, want := range map[time.Duration]int64{
		time.Second:                          1,
		1001 * time.Millisecond:              2,
		1500 * time.Millisecond:              2,
		time.Minute:                          60,
		time.Minute + time.Nanosecond:        61,
		24 * time.Hour:                       86_400,
		2*time.Second - time.Nanosecond:      2,
		2*time.Second + 999*time.Millisecond: 3,
	} {
		seconds, err := evt.RebuildLeaseSeconds(lease)
		require.NoError(t, err, lease)
		require.Equal(t, want, seconds, lease)
	}

	for _, lease := range []time.Duration{0, -time.Second, 999 * time.Millisecond} {
		_, err := evt.RebuildLeaseSeconds(lease)
		require.ErrorIs(t, err, evt.ErrInvalidArgument, lease)
	}
}

func TestCheckRebuildViewBatch(t *testing.T) {
	put := func(i int) evt.ProjectedView {
		return evt.ProjectedView{Projector: "p", Op: evt.PutView("pk", fmt.Sprintf("sk-%03d", i), "owner", "t", []byte(`{}`))}
	}

	batch := func(count int) []evt.ProjectedView {
		views := make([]evt.ProjectedView, 0, count)
		for i := range count {
			views = append(views, put(i))
		}

		return views
	}

	require.NoError(t, evt.CheckRebuildViewBatch(batch(1)))
	require.NoError(t, evt.CheckRebuildViewBatch(batch(evt.MaxRebuildViewBatch)))

	var tooLarge *evt.CommitTooLargeError
	require.ErrorAs(t, evt.CheckRebuildViewBatch(batch(evt.MaxRebuildViewBatch+1)), &tooLarge)
	require.Equal(t, evt.MaxRebuildViewBatch+2, tooLarge.Items, "the guard is the last action")

	require.ErrorIs(t, evt.CheckRebuildViewBatch(nil), evt.ErrInvalidArgument)

	// An invalid or repeated operation is refused like a commit's.
	var duplicate *evt.DuplicateViewKeyError
	require.ErrorAs(t, evt.CheckRebuildViewBatch([]evt.ProjectedView{put(1), put(1)}), &duplicate)

	var invalid *evt.InvalidViewOperationError
	require.ErrorAs(t, evt.CheckRebuildViewBatch([]evt.ProjectedView{{Projector: "p", Op: evt.PutView("", "sk", "o", "t", nil)}}), &invalid)

	// The byte budget applies too: one operation over the per-item limit.
	huge := evt.ProjectedView{Projector: "p", Op: evt.PutView("pk", "sk", "owner", "t", []byte(strings.Repeat("x", 400_000)))}
	require.ErrorAs(t, evt.CheckRebuildViewBatch([]evt.ProjectedView{huge}), &tooLarge)
}

// readerAt is a RebuildReader that answers from a script.
type readerAt struct {
	generations []evt.RebuildGeneration
	calls       int
	err         error
}

func (r *readerAt) Generation(context.Context) (evt.RebuildGeneration, error) {
	if r.err != nil {
		return evt.RebuildGeneration{}, r.err
	}

	generation := r.generations[min(r.calls, len(r.generations)-1)]
	r.calls++

	return generation, nil
}

func TestRebuildEpoch_FencesAReadAgainstTheGeneration(t *testing.T) {
	ctx := context.Background()
	settled := func(value int64) evt.RebuildGeneration { return evt.RebuildGeneration{Value: value} }

	t.Run("a settled generation that holds verifies", func(t *testing.T) {
		reader := &readerAt{generations: []evt.RebuildGeneration{settled(4), settled(4)}}

		var ran int
		require.NoError(t, evt.ReadAtRebuildEpoch(ctx, reader, func(context.Context) error { ran++; return nil }))
		require.Equal(t, 1, ran)
		require.Equal(t, 2, reader.calls, "one read before the read and one after")
	})

	t.Run("an odd generation before the read skips it", func(t *testing.T) {
		reader := &readerAt{generations: []evt.RebuildGeneration{{Value: 5}}}

		var ran int
		err := evt.ReadAtRebuildEpoch(ctx, reader, func(context.Context) error { ran++; return nil })

		var unavailable *evt.RebuildUnavailableError
		require.ErrorAs(t, err, &unavailable)
		require.False(t, unavailable.Generation.ForceReleased)
		require.Zero(t, ran)
		require.Contains(t, err.Error(), "rebuild is in progress")
	})

	t.Run("a force-released generation is unavailable too, and says why", func(t *testing.T) {
		reader := &readerAt{generations: []evt.RebuildGeneration{{Value: 7, ForceReleased: true}}}

		_, err := evt.ObserveRebuildEpoch(ctx, reader)

		var unavailable *evt.RebuildUnavailableError
		require.ErrorAs(t, err, &unavailable)
		require.True(t, unavailable.Generation.ForceReleased)
		require.Contains(t, err.Error(), "force-released")
	})

	t.Run("a rebuild that started during the read fails the verify", func(t *testing.T) {
		reader := &readerAt{generations: []evt.RebuildGeneration{settled(4), {Value: 5}}}

		err := evt.ReadAtRebuildEpoch(ctx, reader, func(context.Context) error { return nil })
		require.True(t, evt.IsRebuildUnavailableErr(err))
	})

	t.Run("a rebuild that started and finished during the read changed the epoch", func(t *testing.T) {
		reader := &readerAt{generations: []evt.RebuildGeneration{settled(4), settled(6)}}

		err := evt.ReadAtRebuildEpoch(ctx, reader, func(context.Context) error { return nil })

		var changed *evt.RebuildEpochChangedError
		require.ErrorAs(t, err, &changed)
		require.Equal(t, int64(4), changed.Observed)
		require.Equal(t, int64(6), changed.Current)
	})

	t.Run("a failed read is returned without verifying", func(t *testing.T) {
		reader := &readerAt{generations: []evt.RebuildGeneration{settled(4)}}
		boom := errors.New("boom")

		require.ErrorIs(t, evt.ReadAtRebuildEpoch(ctx, reader, func(context.Context) error { return boom }), boom)
		require.Equal(t, 1, reader.calls)
	})

	t.Run("a failed generation read is returned as it is", func(t *testing.T) {
		boom := errors.New("boom")
		reader := &readerAt{err: boom}

		_, err := evt.ObserveRebuildEpoch(ctx, reader)
		require.ErrorIs(t, err, boom)

		require.ErrorIs(t, evt.RebuildEpoch{Generation: 4}.Verify(ctx, reader), boom)
	})
}

func TestIsRebuildTransient_NamesTheRefusalsARetryCanOutwait(t *testing.T) {
	for name, err := range map[string]error{
		"a refused commit":          &evt.RebuildInProgressError{},
		"a lost Begin race":         &evt.RebuildConflictError{Attempts: 8},
		"an unavailable reader":     &evt.RebuildUnavailableError{},
		"a changed epoch":           &evt.RebuildEpochChangedError{Observed: 2, Current: 4},
		"a refusal that is wrapped": fmt.Errorf("committing: %w", &evt.RebuildInProgressError{}),
	} {
		require.True(t, evt.IsRebuildTransient(err), name)
	}

	for name, err := range map[string]error{
		"a held lock":      &evt.RebuildLockHeldError{Owner: "job"},
		"a lost lease":     &evt.RebuildLeaseLostError{Owner: "job"},
		"an invalid state": &evt.RebuildStateInvalidError{Detail: "x"},
		"nil":              nil,
	} {
		require.False(t, evt.IsRebuildTransient(err), name)
	}

	require.ErrorIs(t, &evt.RebuildInProgressError{}, evt.ErrRebuildInProgress)
	require.False(t, evt.IsOptimisticConflict(&evt.RebuildInProgressError{}), "a refusal is never a conflict")
}
