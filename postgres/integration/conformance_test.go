//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

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

	// Commit-wide view operations go to a table the application owns; evt defines no view schema.
	// This one is private to the run and dropped afterwards.
	views := newViewTable(ctx, t, pool)

	newRepo := func() evt.Repository {
		return postgres.NewRepository(pool).WithViewWriter(views.writer)
	}

	marker := postgres.NewRepository(pool).FeatureMarker(nil)

	conformance.RunRepositorySuite(t, newRepo, conformance.SuiteOptions{
		SupportsSnapshots:             true,
		EnforcesOptimisticConcurrency: true,
		SupportsTransactions:          true,
		SupportsOutcomes:              true,
		SupportsViews:                 true,
		ViewsSizedBySQL:               true,
		ReadView: func(ctx context.Context, _ evt.Repository, pk, sk string) (*evt.SerializedView, error) {
			return views.read(ctx, pk, sk)
		},
		DeclareRetention: func(ctx context.Context, _ evt.Repository, entityType evt.EntityType) error {
			_, err := marker.Declare(ctx, func(manifest *evt.FeatureManifest) {
				manifest.Retention = append(manifest.Retention, entityType)
			})

			return err
		},
		WithRebuildClock: func(repo evt.Repository, now func() time.Time) evt.Repository {
			return repo.(*postgres.Repository).WithClock(now) //nolint:errcheck,forcetypeassert // newRepo returns a *postgres.Repository
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

// Test_StoreInitializationConformance runs the store-initialization cases on an event log of its
// own. A PostgreSQL writer initializes a fresh store on its first write, so it does not refuse an
// uninitialized one.
func Test_StoreInitializationConformance(t *testing.T) {
	ctx := context.Background()
	repo, marker, name := newIsolatedEventLog(ctx, t)
	pool := newPool(ctx, t)
	other := postgres.NewRepository(pool,
		postgres.WithEventsTable(name), postgres.WithSnapshotsTable(name+"_snapshots"))

	conformance.RunInitializationSuite(t, repo, other, conformance.InitializationOptions{
		Manifest: marker.Manifest,
		Unmigrate: func(ctx context.Context, _ evt.Repository) (func(), error) {
			_, err := marker.Declare(ctx, func(manifest *evt.FeatureManifest) {
				delete(manifest.Features, evt.FeatureStreamHeads)
			})

			return func() {
				_, _ = marker.DeclareStreamHeads(context.Background()) //nolint:errcheck // best-effort restore of a test-only log
			}, err
		},
		BypassHead: func(ctx context.Context, _ evt.Repository, event evt.SerializedEvent) error {
			metadata, err := eventjson.MarshalMetadata(event.Metadata)
			if err != nil {
				return err
			}

			_, err = pool.Exec(ctx, `INSERT INTO `+name+`
				(entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				string(event.EntityID), int64(event.Sequence), string(event.ID), string(event.Type),
				int64(event.Version), string(event.EntityType), event.Payload, metadata)

			return err
		},
	})
}
