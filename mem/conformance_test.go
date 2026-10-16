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
		ProjectorGroup:                func(operations int) evt.TransactionGroup { return projectorGroup{size: operations} },
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

// projectorGroup is a backend-neutral projector group of a given number of operations.
type projectorGroup struct{ size int }

func (g projectorGroup) Len() int                           { return g.size }
func (projectorGroup) StorageType() evt.StorageType         { return "conformance" }
func (projectorGroup) TransactionType() evt.TransactionType { return "conformance" }
func (projectorGroup) HandleError(err error, _ int) error   { return err }
func (g projectorGroup) Merge(evt.TransactionGroup) (evt.TransactionGroup, error) {
	return g, nil
}
