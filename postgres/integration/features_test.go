//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/postgres"
	"github.com/photon-grove/evt/test"
)

// The C9 feature-marker and C8 reserved-namespace integration tests run against the local
// PostgreSQL database: the derived features table, declarations under the advisory locks, the
// commit-time check, the writer refusals, and the enumeration skips. The feature manifest is
// table-global state, so each top-level test runs against its own event log: a unique events table
// name derives a unique features relation, and the tables are dropped afterward.

// newIsolatedEventLog creates a fresh event log (events, snapshots, and the derived features
// relation) for one test and returns its repository and feature marker.
func newIsolatedEventLog(ctx context.Context, t *testing.T) (*postgres.Repository, *postgres.FeatureMarker, string) {
	t.Helper()

	name := fmt.Sprintf("evt_it_%d", time.Now().UnixNano())
	repo := postgres.NewRepository(newPool(ctx, t),
		postgres.WithEventsTable(name),
		postgres.WithSnapshotsTable(name+"_snapshots"),
	)
	require.NoError(t, repo.EnsureSchema(ctx))

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pool := newPool(cleanupCtx, t)
		for _, table := range []string{name, name + "_snapshots", name + "__evt_features"} {
			// Best-effort teardown of a test-only log.
			_, _ = pool.Exec(cleanupCtx, "DROP TABLE IF EXISTS "+table) //nolint:errcheck // best-effort teardown
		}
	})

	return repo, repo.FeatureMarker(nil), name
}

// Test_FeatureMarker exercises the feature-marker protocol end to end.
func Test_FeatureMarker(t *testing.T) {
	ctx := context.Background()

	t.Run("DeclareCreatesAndBumpsVersion", func(t *testing.T) {
		repo, marker, _ := newIsolatedEventLog(ctx, t)
		_ = repo

		manifest, err := marker.Manifest(ctx)
		require.NoError(t, err)
		require.Nil(t, manifest, "a fresh log declares nothing")

		declared, err := marker.Declare(ctx, func(m *evt.FeatureManifest) {
			m.Features["journal"] = 1
		})
		require.NoError(t, err)
		require.Equal(t, int64(1), declared.Version)

		declared, err = marker.Declare(ctx, func(m *evt.FeatureManifest) {
			m.Features["journal-compaction"] = 1
			m.HistoryDependent = []evt.EntityType{"audit-feed"}
			m.Retention = []evt.EntityType{"session"}
		})
		require.NoError(t, err)
		require.Equal(t, int64(2), declared.Version, "one declaration adds exactly one to the version")

		stored, err := marker.Manifest(ctx)
		require.NoError(t, err)
		require.NotNil(t, stored)
		require.Equal(t, int64(2), stored.Version)
		require.Equal(t, map[string]evt.FeatureFormat{"journal": 1, "journal-compaction": 1}, stored.Features)
		require.Equal(t, []evt.EntityType{"audit-feed"}, stored.HistoryDependent)
		require.Equal(t, []evt.EntityType{"session"}, stored.Retention)

		// A writer that models the declared pairs validates; a set removal is a declaration in the
		// same protocol and bumps the version the same way.
		declared, err = marker.Declare(ctx, func(m *evt.FeatureManifest) {
			m.HistoryDependent = nil
		})
		require.NoError(t, err)
		require.Equal(t, int64(3), declared.Version)

		stored, err = marker.Manifest(ctx)
		require.NoError(t, err)
		require.Empty(t, stored.HistoryDependent)
	})

	t.Run("CommitRefusesUnsupportedFeature", func(t *testing.T) {
		repo, marker, _ := newIsolatedEventLog(ctx, t)

		ns := fmt.Sprintf("feat-%d", time.Now().UnixNano())
		entityType := evt.EntityType(ns)
		id := evt.EntityID(ns + "-a")

		enforcing := repo.WithFeatureMarker(repo.FeatureMarker(nil))

		// The first commit validates the empty table and succeeds.
		require.NoError(t, enforcing.Commit(ctx, evt.SerializedResult{
			Events: serializeAs(t, id, entityType, 0, &test.EntityCreated{ID: id, Value: "one"}),
		}))

		// Another writer declares an extension this writer does not model.
		_, err := marker.Declare(ctx, func(m *evt.FeatureManifest) {
			m.Features["journal"] = 1
		})
		require.NoError(t, err)

		id2 := evt.EntityID(ns + "-b")
		err = enforcing.Commit(ctx, evt.SerializedResult{
			Events: serializeAs(t, id2, entityType, 0, &test.EntityCreated{ID: id2, Value: "b"}),
		})
		require.Error(t, err)
		require.True(t, evt.IsUnsupportedFeatureErr(err), "expected UnsupportedFeatureError, got %v", err)

		stored, gerr := repo.GetEvents(ctx, id2)
		require.NoError(t, gerr)
		require.Empty(t, stored, "the refused commit writes nothing")
	})

	t.Run("ChangedManifestRetries", func(t *testing.T) {
		repo, marker, _ := newIsolatedEventLog(ctx, t)

		ns := fmt.Sprintf("bump-%d", time.Now().UnixNano())
		entityType := evt.EntityType(ns)
		id := evt.EntityID(ns + "-a")

		enforcing := repo.WithFeatureMarker(repo.FeatureMarker(evt.SupportedFeatures{"journal": 1}))

		require.NoError(t, enforcing.Commit(ctx, evt.SerializedResult{
			Events: serializeAs(t, id, entityType, 0, &test.EntityCreated{ID: id, Value: "one"}),
		}))

		// A set change bumps the version under the writer.
		_, err := marker.Declare(ctx, func(m *evt.FeatureManifest) {
			m.Retention = []evt.EntityType{"session"}
		})
		require.NoError(t, err)

		err = enforcing.Commit(ctx, evt.SerializedResult{
			Events: serializeAs(t, id, entityType, 1, &test.EntityUpdated{ID: id, Value: "two"}),
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, evt.ErrFeatureManifestChanged), "expected ErrFeatureManifestChanged, got %v", err)

		// The retry commits against the freshly cached version.
		require.NoError(t, enforcing.Commit(ctx, evt.SerializedResult{
			Events: serializeAs(t, id, entityType, 2, &test.EntityUpdated{ID: id, Value: "three"}),
		}))

		stored, gerr := repo.GetEvents(ctx, id)
		require.NoError(t, gerr)
		require.Len(t, stored, 2)
	})

	t.Run("CompactBelowRefusesHistoryDependent", func(t *testing.T) {
		repo, marker, eventsName := newIsolatedEventLog(ctx, t)

		ns := fmt.Sprintf("hist-%d", time.Now().UnixNano())
		entityType := evt.EntityType(ns)
		id := evt.EntityID(ns + "-a")

		serialized := serializeAs(t, id, entityType, 0,
			&test.EntityCreated{ID: id, Value: "one"},
			&test.EntityUpdated{ID: id, Value: "two"},
		)
		payload := []byte(fmt.Sprintf(`{"id":%q,"value":"two"}`, id))
		require.NoError(t, repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: serialized}, entityType, id, payload, 1))

		_, err := marker.Declare(ctx, func(m *evt.FeatureManifest) {
			m.HistoryDependent = []evt.EntityType{entityType}
		})
		require.NoError(t, err)

		_, err = repo.CompactBelow(ctx, id, 2)
		require.Error(t, err)
		require.True(t, evt.IsHistoryDependentProjectionErr(err), "expected HistoryDependentProjectionError, got %v", err)

		// A repository that does not enforce the manifest refuses all the same: the refusal reads
		// the row, not local configuration.
		plain := postgres.NewRepository(newPool(ctx, t),
			postgres.WithEventsTable(eventsName),
			postgres.WithSnapshotsTable(eventsName+"_snapshots"))
		_, err = plain.CompactBelow(ctx, id, 2)
		require.Error(t, err)
		require.True(t, evt.IsHistoryDependentProjectionErr(err), "expected HistoryDependentProjectionError, got %v", err)
	})
}

// Test_Namespaces exercises the C8 reserved namespaces: write-path refusals and the enumeration
// and heads skips.
func Test_Namespaces(t *testing.T) {
	ctx := context.Background()
	repo, _, eventsName := newIsolatedEventLog(ctx, t)

	ns := fmt.Sprintf("ns-%d", time.Now().UnixNano())
	entityType := evt.EntityType(ns)
	id := evt.EntityID(ns + "-a")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: serializeAs(t, id, entityType, 0, &test.EntityCreated{ID: id, Value: "one"}),
	}))

	t.Run("WritePathsRefuseReservedEntityIds", func(t *testing.T) {
		reserved := []evt.EntityID{
			evt.EntityID(evt.SystemNamespacePrefix + ns),
			evt.EntityID(evt.CommandOutcomeNamespacePrefix + ns),
		}

		for _, rid := range reserved {
			err := repo.Commit(ctx, evt.SerializedResult{
				Events: serializeAs(t, rid, entityType, 0, &test.EntityCreated{ID: rid, Value: "r"}),
			})
			require.Error(t, err)
			require.True(t, evt.IsReservedEntityIdErr(err), "expected ReservedEntityIdError, got %v", err)

			err = repo.CommitWithSnapshot(ctx, evt.SerializedResult{
				Events: serializeAs(t, rid, entityType, 0, &test.EntityCreated{ID: rid, Value: "r"}),
			}, entityType, rid, []byte("{}"), 1)
			require.Error(t, err)
			require.True(t, evt.IsReservedEntityIdErr(err))

			_, err = repo.CompactBelow(ctx, rid, 1)
			require.Error(t, err)
			require.True(t, evt.IsReservedEntityIdErr(err))
		}

		// The plain stream next door was never written by a refused commit.
		stored, err := repo.GetEvents(ctx, id)
		require.NoError(t, err)
		require.Len(t, stored, 1)
	})

	t.Run("EnumerationSkipsReservedNamespaces", func(t *testing.T) {
		// The features row lives in its own derived relation, so seed the reserved namespaces
		// directly the way an engine row (or a pre-v0.9 stream) would appear.
		_, err := newPool(ctx, t).Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata)
			 VALUES ($1, 1, $1 || ':1', 'outcome', 1, $2, '{}'::bytea, '{}'::jsonb)
			 ON CONFLICT (entity_id, sequence) DO NOTHING`,
			eventsName), evt.CommandOutcomeNamespacePrefix+"cmd-"+ns, string(entityType))
		require.NoError(t, err)

		_, err = newPool(ctx, t).Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata)
			 VALUES ($1, 1, $1 || ':1', 'legacy', 1, $2, '{}'::bytea, '{}'::jsonb)
			 ON CONFLICT (entity_id, sequence) DO NOTHING`,
			eventsName), evt.SystemNamespacePrefix+"legacy-"+ns, string(entityType))
		require.NoError(t, err)

		events, err := repo.GetEvents(ctx, id)
		require.NoError(t, err)
		require.Len(t, events, 1)

		heads, err := repo.StreamEntityHeads(ctx, "")
		require.NoError(t, err)
		require.NotContains(t, heads, evt.EntityID(evt.SystemNamespacePrefix+"legacy-"+ns))
		require.NotContains(t, heads, evt.EntityID(evt.CommandOutcomeNamespacePrefix+"cmd-"+ns))
		require.Contains(t, heads, id)

		for _, filter := range []evt.StreamFilter{{}, {EntityType: entityType}} {
			count := 0
			for r := range repo.StreamAllEvents(ctx, filter) {
				batch, err := r.Unwrap()
				require.NoError(t, err)
				require.Len(t, batch, 1)
				require.Equal(t, id, batch[0].EntityID)
				count++
			}
			require.Equal(t, 1, count, "reserved rows never surface in enumeration")

			count = 0
			for r := range repo.StreamEntities(ctx, filter, applyTestEvent) {
				entity, err := r.Unwrap()
				require.NoError(t, err)
				require.Equal(t, id, entity.GetID())
				count++
			}
			require.Equal(t, 1, count)
		}
	})

	t.Run("GetRecentEventsNewestFirst", func(t *testing.T) {
		for i := 2; i <= 5; i++ {
			require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
				Events: serializeAs(t, id, entityType, evt.EventSequence(i-1),
					&test.EntityUpdated{ID: id, Value: fmt.Sprintf("v%d", i)}),
			}))
		}

		recent, err := repo.GetRecentEvents(ctx, id, 2)
		require.NoError(t, err)
		require.Len(t, recent, 2)
		require.Equal(t, evt.EventSequence(5), recent[0].Sequence)
		require.Equal(t, evt.EventSequence(4), recent[1].Sequence)

		_, err = repo.GetRecentEvents(ctx, id, 0)
		require.Error(t, err)
	})
}
