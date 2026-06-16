package mem_test

import (
	"testing"

	"github.com/photon-grove/evt/conformance"
	"github.com/photon-grove/evt/mem"
)

// Test_RepositoryConformance runs the backend-neutral storage contract suite against the in-memory
// repository. The in-memory repository is a permissive test double, so optimistic-concurrency
// enforcement is left off; it does support snapshots.
func Test_RepositoryConformance(t *testing.T) {
	conformance.RunRepositorySuite(t, mem.NewRepository, conformance.SuiteOptions{
		SupportsSnapshots:             true,
		EnforcesOptimisticConcurrency: false,
	})
}
