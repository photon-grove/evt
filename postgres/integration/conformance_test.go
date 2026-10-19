//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/conformance"
	"github.com/photon-grove/evt/internal/eventjson"
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

	marker := postgres.NewRepository(pool).FeatureMarker(nil)

	conformance.RunRepositorySuite(t, newRepo, conformance.SuiteOptions{
		SupportsSnapshots:             true,
		EnforcesOptimisticConcurrency: true,
		SupportsTransactions:          true,
		SupportsOutcomes:              true,
		DeclareRetention: func(ctx context.Context, _ evt.Repository, entityType evt.EntityType) error {
			_, err := marker.Declare(ctx, func(manifest *evt.FeatureManifest) {
				manifest.Retention = append(manifest.Retention, entityType)
			})

			return err
		},
		BypassHead: func(ctx context.Context, _ evt.Repository, event evt.SerializedEvent) error {
			metadata, err := eventjson.MarshalMetadata(event.Metadata)
			if err != nil {
				return err
			}

			_, err = pool.Exec(ctx, `INSERT INTO evt_events
				(entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				string(event.EntityID), int64(event.Sequence), string(event.ID), string(event.Type),
				int64(event.Version), string(event.EntityType), event.Payload, metadata)

			return err
		},
		Unmigrate: func(ctx context.Context, _ evt.Repository) (func(), error) {
			_, err := marker.Declare(ctx, func(manifest *evt.FeatureManifest) {
				delete(manifest.Features, evt.FeatureStreamHeads)
			})

			return func() {
				// Declared again, the store is migrated: the cases that follow write to it.
				_, _ = marker.DeclareStreamHeads(context.Background()) //nolint:errcheck // best-effort restore of the shared test database
			}, err
		},
	})
}
