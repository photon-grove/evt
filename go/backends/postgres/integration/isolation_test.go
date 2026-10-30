//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/postgres"
	"github.com/photon-grove/evt/go/test"
)

// repeatableReadPool is a pool whose sessions default to REPEATABLE READ, as a server or role
// configured with `default_transaction_isolation` would give them.
func repeatableReadPool(ctx context.Context, t *testing.T) *pgxpool.Pool {
	t.Helper()

	config, err := pgxpool.ParseConfig(resolveDSN())
	require.NoError(t, err)

	config.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"

	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	return pool
}

// waitForEntityLockWaiter polls until a session waits on `id`'s per-entity commit lock.
func waitForEntityLockWaiter(ctx context.Context, t *testing.T, pool *pgxpool.Pool, id evt.EntityID) {
	t.Helper()

	const waiting = `SELECT count(*) FROM pg_locks
		WHERE locktype = 'advisory' AND NOT granted
		  AND classid = 1702261760::oid
		  AND objid = (hashtext($1)::bigint & 4294967295)::oid`

	require.Eventually(t, func() bool {
		var n int
		if err := pool.QueryRow(ctx, waiting, string(id)).Scan(&n); err != nil {
			return false
		}

		return n > 0
	}, 5*time.Second, 10*time.Millisecond, "the commit waits on its entity lock")
}

// A commit reads the rebuild fence after it waits for its locks, so it must read it as of that
// moment, whatever the server's default isolation. Under REPEATABLE READ the snapshot would be the
// one taken before the wait: a rebuild that began meanwhile would be invisible to it, and the commit
// would land under the fence.
func Test_Isolation_ACommitSeesARebuildThatBeganWhileItWaited(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)
	_, name := privateLog(ctx, t, pool)
	log := logHandle(repeatableReadPool(ctx, t), name)
	id := evt.EntityID(name + "-waits")

	lock, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = lock.Rollback(ctx) }() //nolint:errcheck // no-op after the explicit Rollback below; releases the lock if the test fails early

	_, err = lock.Exec(ctx, "SELECT pg_advisory_xact_lock(1702261760, hashtext($1))", string(id))
	require.NoError(t, err)

	events, err := evt.SerializeEvents([]evt.Event{&test.AggregateCreated{ID: id, Value: "v"}}, 0, id, evt.Metadata{})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- log.Commit(ctx, evt.SerializedResult{Events: events}) }()

	waitForEntityLockWaiter(ctx, t, pool, id)

	lease, err := log.RebuildFence().Begin(ctx, "job", time.Minute)
	require.NoError(t, err)

	require.NoError(t, lock.Rollback(ctx))

	err = <-done
	require.True(t, evt.IsRebuildInProgressErr(err), "the commit saw the rebuild that began while it waited: %v", err)

	stored, err := log.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Empty(t, stored, "nothing landed under the fence")

	_, err = log.RebuildFence().End(ctx, lease)
	require.NoError(t, err)

	require.NoError(t, log.Commit(ctx, evt.SerializedResult{Events: events}), "the same commit lands once the rebuild ends")
}

// A caller may pass an ambient transaction as the Repository's DB. One at READ COMMITTED (the
// server default) works unchanged. One at a stricter level is refused at begin, because PostgreSQL
// cannot lower the level of a transaction already under way, and its snapshot would hide fences.
func Test_Isolation_AnAmbientTransactionMustBeReadCommitted(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)
	_, name := privateLog(ctx, t, pool)

	commit := func(tx pgx.Tx, suffix string) error {
		id := evt.EntityID(name + suffix)
		events, err := evt.SerializeEvents([]evt.Event{&test.AggregateCreated{ID: id, Value: "v"}}, 0, id, evt.Metadata{})
		require.NoError(t, err)

		repo := postgres.NewRepository(tx, postgres.WithEventsTable(name), postgres.WithSnapshotsTable(name+"_snaps"))

		return repo.Commit(ctx, evt.SerializedResult{Events: events})
	}

	committed, err := pool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, commit(committed, "-read-committed"))
	require.NoError(t, committed.Commit(ctx))

	repeatable, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	require.NoError(t, err)
	defer func() { _ = repeatable.Rollback(ctx) }() //nolint:errcheck // the transaction is aborted by the refused SET

	refused := commit(repeatable, "-repeatable-read")
	require.ErrorContains(t, refused, "an ambient transaction must already run at it")
}
