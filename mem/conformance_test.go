package mem_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/photon-grove/evt"
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
		SupportsTransactions:          true,
		DeclareRetention: func(ctx context.Context, repo evt.Repository, entityType evt.EntityType) error {
			memRepo, ok := repo.(*mem.Repository)
			if !ok {
				return fmt.Errorf("unexpected repository type %T", repo)
			}

			_, err := memRepo.Declare(ctx, func(manifest *evt.FeatureManifest) {
				manifest.Retention = append(manifest.Retention, entityType)
			})

			return err
		},
	})
}
