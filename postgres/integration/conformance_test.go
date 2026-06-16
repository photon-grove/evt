//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/conformance"
	"github.com/photon-grove/evt/postgres"
)

// Test_RepositoryConformance runs the backend-neutral storage contract suite against the PostgreSQL
// repository. The suite namespaces its data per run, so it is safe to share the event-log tables
// across subtests. PostgreSQL enforces optimistic concurrency through the (entity_id, sequence)
// primary key and supports durable snapshots, so both options are on.
func Test_RepositoryConformance(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)

	newRepo := func() evt.Repository {
		return postgres.NewRepository(pool)
	}

	conformance.RunRepositorySuite(t, newRepo, conformance.SuiteOptions{
		SupportsSnapshots:             true,
		EnforcesOptimisticConcurrency: true,
	})
}
