//go:build integration

// Package integration runs the backend-neutral conformance suite and capability checks against a
// real PostgreSQL server. The tests are gated behind the `integration` build tag so the default
// `go test ./...` run never needs a database; CI stands one up (see infra/local-postgres and the
// Postgres integration job) before invoking `moon run evt:integration-postgres`.
package integration

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt/postgres"
)

// defaultDSN points at the local infra/local-postgres database with its local-only credentials. The
// integration tests honor DATABASE_URL when set so CI and other environments can override it.
const defaultDSN = "postgres://evt:evt@localhost:5432/evt_local?sslmode=disable"

var (
	schemaOnce sync.Once
	schemaErr  error
)

// resolveDSN returns the PostgreSQL connection string, preferring DATABASE_URL.
func resolveDSN() string {
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		return dsn
	}

	return defaultDSN
}

// newPool connects to PostgreSQL and ensures the event-log schema exists exactly once per process.
// The connection is verified with a ping; an unreachable database fails the test loudly, matching
// the DynamoDB suite (CI gates the run on a readiness check).
func newPool(ctx context.Context, t *testing.T) *pgxpool.Pool {
	t.Helper()

	pool, err := pgxpool.New(ctx, resolveDSN())
	require.NoError(t, err, "connect to PostgreSQL (set DATABASE_URL to override %q)", defaultDSN)

	require.NoError(t, pool.Ping(ctx), "ping PostgreSQL")

	t.Cleanup(pool.Close)

	schemaOnce.Do(func() {
		schemaErr = postgres.NewRepository(pool).EnsureSchema(ctx)
	})
	require.NoError(t, schemaErr, "ensure schema")

	return pool
}
