package mem_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/mem"
	"github.com/photon-grove/evt/go/conformance"
	"github.com/stretchr/testify/require"
)

// Test_RepositoryConformance runs the backend-neutral storage contract suite against the in-memory
// repository. The in-memory repository is a permissive test double, so optimistic-concurrency
// enforcement is left off; it does support snapshots.
func Test_RepositoryConformance(t *testing.T) {
	conformance.RunRepositorySuite(t, mem.NewRepository, conformance.SuiteOptions{
		SupportsSnapshots:             true,
		EnforcesOptimisticConcurrency: false,
		SupportsTransactions:          true,
		SupportsOutcomes:              true,
		ProjectorGroup:                func(operations int) evt.TransactionGroup { return projectorGroup{size: operations} },
		SupportsViews:                 true,
		ReadView: func(_ context.Context, repo evt.Repository, pk, sk string) (*evt.SerializedView, error) {
			memRepo, ok := repo.(*mem.Repository)
			if !ok {
				return nil, fmt.Errorf("unexpected repository type %T", repo)
			}

			view, found := memRepo.View(pk, sk)
			if !found {
				return nil, nil
			}

			return &view, nil
		},
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
		WithRebuildClock: func(repo evt.Repository, now func() time.Time) evt.Repository {
			memRepo, ok := repo.(*mem.Repository)
			if !ok {
				panic(fmt.Sprintf("unexpected repository type %T", repo))
			}

			return memRepo.WithClock(now)
		},
		BypassHead: func(_ context.Context, repo evt.Repository, event evt.SerializedEvent) error {
			memRepo, ok := repo.(*mem.Repository)
			if !ok {
				return fmt.Errorf("unexpected repository type %T", repo)
			}

			mem.AppendWithoutHead(memRepo, event)

			return nil
		},
		Unmigrate: func(ctx context.Context, repo evt.Repository) (func(), error) {
			memRepo, ok := repo.(*mem.Repository)
			if !ok {
				return nil, fmt.Errorf("unexpected repository type %T", repo)
			}

			_, err := memRepo.Declare(ctx, func(manifest *evt.FeatureManifest) {
				delete(manifest.Features, evt.FeatureStreamHeads)
			})

			return func() {
				_, _ = memRepo.Declare(context.Background(), func(manifest *evt.FeatureManifest) { //nolint:errcheck // the in-memory declaration cannot fail
					manifest.Features[evt.FeatureStreamHeads] = evt.StreamHeadsFormat
				})
			}, err
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

// Test_StoreInitializationConformance runs the store-initialization cases. The in-memory writer
// initializes a fresh store on its first write, so it does not refuse an uninitialized one.
func Test_StoreInitializationConformance(t *testing.T) {
	repo, ok := mem.NewRepository().(*mem.Repository)
	require.True(t, ok)

	conformance.RunInitializationSuite(t, repo, repo, conformance.InitializationOptions{
		Manifest: repo.Manifest,
		Unmigrate: func(ctx context.Context, _ evt.Repository) (func(), error) {
			_, err := repo.Declare(ctx, func(manifest *evt.FeatureManifest) {
				delete(manifest.Features, evt.FeatureStreamHeads)
			})

			return func() {
				_, _ = repo.Declare(context.Background(), func(manifest *evt.FeatureManifest) { //nolint:errcheck // the in-memory declaration cannot fail
					manifest.Features[evt.FeatureStreamHeads] = evt.StreamHeadsFormat
				})
			}, err
		},
		BypassHead: func(_ context.Context, _ evt.Repository, event evt.SerializedEvent) error {
			mem.AppendWithoutHead(repo, event)

			return nil
		},
	})
}

// Test_FeatureMarkerConformance runs the feature-marker cases. The in-memory repository keeps no
// cached validation (it checks the manifest on every write), so one store serves as both writers.
func Test_FeatureMarkerConformance(t *testing.T) {
	repo, ok := mem.NewRepository().(*mem.Repository)
	require.True(t, ok)

	conformance.RunFeatureMarkerSuite(t, repo, repo, conformance.FeatureMarkerOptions{
		Declare: func(ctx context.Context, _ evt.Repository, change func(*evt.FeatureManifest)) (evt.FeatureManifest, error) {
			return repo.Declare(ctx, change)
		},
		Manifest: func(ctx context.Context, _ evt.Repository) (*evt.FeatureManifest, error) {
			return repo.Manifest(ctx)
		},
	})
}
