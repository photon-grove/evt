//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/postgres"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

// Test_CommitWaitsForEntityLock verifies that a commit takes the per-entity advisory lock that the
// Rust crate's commits and snapshot writes also take, so a snapshot cannot raise the floor under a
// commit that already passed its head statement.
func Test_CommitWaitsForEntityLock(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)
	repo := postgres.NewRepository(pool)
	id := evt.EntityID(fmt.Sprintf("lock-%d", time.Now().UnixNano()))

	lock, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = lock.Rollback(ctx) }() //nolint:errcheck // no-op after the explicit Rollback below; releases the lock if the test fails early

	_, err = lock.Exec(ctx, "SELECT pg_advisory_xact_lock(1702261760, hashtext($1))", string(id))
	require.NoError(t, err)

	events, err := evt.SerializeEvents([]evt.Event{&test.EntityCreated{ID: id, Value: "one"}}, 0, id, evt.Metadata{})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- repo.Commit(ctx, evt.SerializedResult{Events: events}) }()

	select {
	case err := <-done:
		t.Fatalf("the commit finished while another transaction held the entity lock: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	require.NoError(t, lock.Rollback(ctx))
	require.NoError(t, <-done)
}
