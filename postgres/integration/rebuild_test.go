//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/postgres"
	"github.com/photon-grove/evt/test"
)

// privateLog creates an event log of its own, so a rebuild lock held by a test refuses nobody
// else's commits, and drops its relations afterwards.
func privateLog(ctx context.Context, t *testing.T, pool *pgxpool.Pool) (*postgres.Repository, string) {
	t.Helper()

	events := fmt.Sprintf("evt_it_%d", time.Now().UnixNano())
	repo := logHandle(pool, events)
	require.NoError(t, repo.EnsureSchema(ctx))

	t.Cleanup(func() {
		for _, suffix := range []string{
			"", "__evt_command_outcomes", "__evt_features", "__evt_rebuild_lock", "__evt_rebuild_generation", "__evt_stream_heads",
		} {
			_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+events+suffix) //nolint:errcheck // best-effort teardown of a test-only table
		}

		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+events+"_snaps") //nolint:errcheck // best-effort teardown of a test-only table
	})

	return repo, events
}

// logHandle is a repository handle over the private log named events, as another process would
// open it.
func logHandle(pool *pgxpool.Pool, events string) *postgres.Repository {
	return postgres.NewRepository(pool, postgres.WithEventsTable(events), postgres.WithSnapshotsTable(events+"_snaps"))
}

func commitCreated(ctx context.Context, t *testing.T, repo evt.Repository, id evt.EntityID) error {
	t.Helper()

	events, err := evt.SerializeEvents([]evt.Event{&test.EntityCreated{ID: id, Value: "v"}}, 0, id, evt.Metadata{})
	require.NoError(t, err)

	for i := range events {
		events[i].EntityType = "test"
	}

	return repo.Commit(ctx, evt.SerializedResult{Events: events})
}

func Test_Rebuild_ExactlyOneConcurrentBeginWins(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)
	log, name := privateLog(ctx, t, pool)

	var (
		wg   sync.WaitGroup
		wins atomic.Int32
		held atomic.Int32
	)

	for i := range 12 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			// Each worker has a handle of its own, as separate processes would.
			_, err := logHandle(pool, name).RebuildFence().Begin(ctx, fmt.Sprintf("worker-%d", i), time.Minute)

			switch {
			case err == nil:
				wins.Add(1)
			case evt.IsRebuildLockHeldErr(err):
				held.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	wg.Wait()

	require.EqualValues(t, 1, wins.Load())
	require.EqualValues(t, 11, held.Load())

	generation, err := log.RebuildFence().Generation(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, generation.Value)
}

// A store whose schema predates the fence has no lock relation: nobody can hold the lock, so
// commits pass, and the lifecycle fails until EnsureSchema creates the relations.
func Test_Rebuild_AStoreWithoutTheFenceRelationsStillCommits(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)
	log, name := privateLog(ctx, t, pool)

	_, err := pool.Exec(ctx, fmt.Sprintf("DROP TABLE %[1]s__evt_rebuild_lock, %[1]s__evt_rebuild_generation", name))
	require.NoError(t, err)

	// The relations were seen by no commit of this handle yet: the assertion probes and passes.
	require.NoError(t, commitCreated(ctx, t, log, evt.EntityID(name+"-a")))

	_, err = log.RebuildFence().Begin(ctx, "job", time.Minute)
	require.Error(t, err)

	require.NoError(t, log.EnsureSchema(ctx))

	_, err = log.RebuildFence().Begin(ctx, "job", time.Minute)
	require.NoError(t, err)
	require.True(t, evt.IsRebuildInProgressErr(commitCreated(ctx, t, log, evt.EntityID(name+"-b"))))
}

// A commit holds the fence's shared advisory lock until COMMIT, so no Begin can slip between its
// lock-row assertion and its COMMIT: the Begin waits, and when it returns the commit has landed.
func Test_Rebuild_BeginWaitsForACommitInFlight(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)

	// A view writer whose statement sleeps keeps the commit's transaction open after its assertion.
	slow := func(_ evt.ViewOperation) (postgres.ViewStatement, error) {
		return postgres.ViewStatement{SQL: "SELECT pg_sleep(0.8)", Args: nil}, nil
	}

	base, name := privateLog(ctx, t, pool)
	log := base.WithViewWriter(slow)

	id := evt.EntityID(name + "-slow")
	events, err := evt.SerializeEvents([]evt.Event{&test.EntityCreated{ID: id, Value: "v"}}, 0, id, evt.Metadata{})
	require.NoError(t, err)

	for i := range events {
		events[i].EntityType = "test"
	}

	done := make(chan error, 1)

	go func() {
		done <- log.Commit(ctx, evt.SerializedResult{
			Events: events,
			Views:  []evt.ProjectedView{{Projector: "slow", Op: evt.PutView("pk", "sk", id, "test", []byte(`{}`))}},
		})
	}()

	// Let the commit pass its assertion and start sleeping.
	time.Sleep(300 * time.Millisecond)

	started := time.Now()
	_, err = log.RebuildFence().Begin(ctx, "job", time.Minute)
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(started), 300*time.Millisecond, "Begin waited for the commit")

	require.NoError(t, <-done, "the commit passed its assertion before the lock existed")

	stored, err := log.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Len(t, stored, 1, "and had landed by the time Begin returned")

	require.True(t, evt.IsRebuildInProgressErr(commitCreated(ctx, t, log, evt.EntityID(name+"-later"))))
}

// A guarded write holds the lock row FOR UPDATE until its COMMIT, so a takeover blocks on it: the
// old worker either commits first or, once the takeover has won, fails its assertion.
func Test_Rebuild_ATakeoverWaitsForAGuardedWriteInFlight(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)

	slow := func(_ evt.ViewOperation) (postgres.ViewStatement, error) {
		return postgres.ViewStatement{SQL: "SELECT pg_sleep(0.8)"}, nil
	}

	base, _ := privateLog(ctx, t, pool)
	log := base.WithViewWriter(slow)

	clock := time.Now()
	worker := log.WithClock(func() time.Time { return clock })
	_, err := worker.RebuildFence().Begin(ctx, "old", 10*time.Second)
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() {
		done <- worker.RebuildFence().WriteViews(ctx, "old", []evt.ProjectedView{
			{Projector: "slow", Op: evt.PutView("pk", "sk", "owner", "test", []byte(`{}`))},
		})
	}()

	time.Sleep(300 * time.Millisecond)

	// The taker-over's clock says the old lease has lapsed.
	later := log.WithClock(func() time.Time { return clock.Add(time.Minute) })
	started := time.Now()

	taken, err := later.RebuildFence().Begin(ctx, "new", time.Minute)
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(started), 300*time.Millisecond, "the takeover waited for the batch")
	require.EqualValues(t, 3, taken.Generation)

	require.NoError(t, <-done, "the old worker's batch had asserted its lease and committed first")

	// Now it has lost the lock: the next batch aborts without writing.
	err = worker.RebuildFence().WriteViews(ctx, "old", []evt.ProjectedView{
		{Projector: "slow", Op: evt.PutView("pk", "again", "owner", "test", []byte(`{}`))},
	})
	require.True(t, evt.IsRebuildLeaseLostErr(err))
}
