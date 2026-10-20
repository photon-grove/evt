package mem_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/mem"
	"github.com/photon-grove/evt/test"
)

func fenceOf(t *testing.T, repo evt.Repository) evt.RebuildFence {
	t.Helper()

	provider, ok := repo.(evt.RebuildFenceProvider)
	require.True(t, ok)

	return provider.RebuildFence()
}

func commitOne(t *testing.T, repo evt.Repository, id evt.EntityID) error {
	t.Helper()

	events, err := evt.SerializeEvents([]evt.Event{&test.EntityCreated{ID: id, Value: "v"}}, 0, id, evt.Metadata{})
	require.NoError(t, err)

	for i := range events {
		events[i].EntityType = "test"
	}

	return repo.Commit(context.Background(), evt.SerializedResult{Events: events})
}

// Copies of the repository, and the handles a clock is injected into, are one store: the fence is
// shared state like the events.
func TestRebuildFence_IsSharedByCopiesOfTheRepository(t *testing.T) {
	ctx := context.Background()
	repo, ok := mem.NewRepository().(*mem.Repository)
	require.True(t, ok)

	clock := time.Unix(time.Now().Add(time.Hour).Unix(), 0)
	timed := repo.WithClock(func() time.Time { return clock })

	_, err := timed.RebuildFence().Begin(ctx, "job-a", 10*time.Second)
	require.NoError(t, err)

	state, err := repo.RebuildFence().State(ctx)
	require.NoError(t, err)
	require.True(t, state.InProgress(), "the original handle sees the lock")
	require.Equal(t, "job-a", state.Lock.Owner)
	require.Equal(t, clock.Unix()+10, state.Lock.LeaseExpiresAt.Unix())

	require.True(t, evt.IsRebuildInProgressErr(commitOne(t, repo, "shared-1")))

	// The default clock is the wall clock: the injected clock's lease lies an hour ahead of it.
	_, err = repo.RebuildFence().Begin(ctx, "job-b", time.Minute)
	require.True(t, evt.IsRebuildLockHeldErr(err))
}

func TestRebuildFence_ExactlyOneConcurrentBeginWins(t *testing.T) {
	ctx := context.Background()
	fence := fenceOf(t, mem.NewRepository())

	var (
		wins    atomic.Int32
		blocked atomic.Int32
		wg      sync.WaitGroup
	)

	for i := range 16 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, err := fence.Begin(ctx, fmt.Sprintf("worker-%d", i), time.Minute)

			switch {
			case err == nil:
				wins.Add(1)
			case evt.IsRebuildLockHeldErr(err):
				blocked.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	wg.Wait()

	require.EqualValues(t, 1, wins.Load())
	require.EqualValues(t, 15, blocked.Load())

	generation, err := fence.Generation(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, generation.Value, "one acquisition, one step")
}

// A commit and a lifecycle call serialize on one lock, so no commit that starts after a Begin has
// returned can land: the lock check and the writes are one critical section.
func TestRebuildFence_NoCommitStartedAfterBeginLands(t *testing.T) {
	ctx := context.Background()
	repo := mem.NewRepository()
	fence := fenceOf(t, repo)

	var (
		wg         sync.WaitGroup
		started    = make(chan struct{})
		begun      atomic.Bool
		landed     atomic.Int32
		violations atomic.Int32
		refused    atomic.Int32
	)

	for i := range 8 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-started

			// Commit until the rebuild has begun, then a little longer, so the commits the lock
			// refuses are certain rather than lucky.
			for n, extra := 0, 0; extra < 20; n++ {
				id := evt.EntityID(fmt.Sprintf("stream-%d-%d", i, n))

				afterBegin := begun.Load()
				if afterBegin {
					extra++
				}

				err := commitOne(t, repo, id)

				switch {
				case err == nil && afterBegin:
					violations.Add(1)
				case err == nil:
					landed.Add(1)
				case evt.IsRebuildInProgressErr(err):
					refused.Add(1)
				default:
					t.Errorf("unexpected error: %v", err)
				}
			}
		}()
	}

	close(started)
	time.Sleep(time.Millisecond)

	_, err := fence.Begin(ctx, "job", time.Hour)
	require.NoError(t, err)
	begun.Store(true)

	wg.Wait()

	require.Zero(t, violations.Load(), "a commit that started after Begin returned landed")
	require.Positive(t, refused.Load(), "the lock refused the commits that raced it")
}

// A guarded batch is checked as a whole: a condition that fails on any of its operations writes
// none of them, exactly like a commit's view operations.
func TestRebuildFence_GuardedBatchWritesAllOrNothing(t *testing.T) {
	ctx := context.Background()
	repo, ok := mem.NewRepository().(*mem.Repository)
	require.True(t, ok)
	fence := repo.RebuildFence()

	lease, err := fence.Begin(ctx, "job", time.Hour)
	require.NoError(t, err)

	// Another stream owns the row at ("pk", "taken").
	require.NoError(t, fence.WriteViews(ctx, lease, []evt.ProjectedView{
		{Projector: "p", Op: evt.PutView("pk", "taken", "stream-a", "t", []byte(`{}`))},
	}))

	err = fence.WriteViews(ctx, lease, []evt.ProjectedView{
		{Projector: "p", Op: evt.PutView("pk", "fresh", "stream-b", "t", []byte(`{}`))},
		{Projector: "p", Operation: 1, Op: evt.PutView("pk", "taken", "stream-b", "t", []byte(`{}`))},
	})

	var failed *evt.ProjectorOperationError
	require.ErrorAs(t, err, &failed)
	require.Equal(t, 1, failed.Operation)

	_, found := repo.View("pk", "fresh")
	require.False(t, found, "the batch's first operation was not applied")

	row, found := repo.View("pk", "taken")
	require.True(t, found)
	require.Equal(t, evt.EntityID("stream-a"), row.EntityID)
}
