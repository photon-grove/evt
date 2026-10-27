//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/postgres"
	"github.com/photon-grove/evt/go/test"
	"github.com/stretchr/testify/require"
)

// pausedManifestDB also wraps nontransactional reads, so the regression fails if a maintenance
// operation ever moves its validation outside the shared-lock transaction again.
type pausedManifestDB struct {
	postgres.DB
	once   sync.Once
	paused chan struct{}
	resume chan struct{}
}

func (db *pausedManifestDB) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := db.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}

	return &pausedManifestTx{Tx: tx, db: db}, nil
}

func (db *pausedManifestDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return db.wrap(ctx, sql, db.DB.QueryRow(ctx, sql, args...))
}

func (db *pausedManifestDB) wrap(ctx context.Context, sql string, row pgx.Row) pgx.Row {
	if strings.Contains(sql, "SELECT version, features") {
		return &pausedManifestRow{Row: row, db: db, ctx: ctx}
	}

	return row
}

type pausedManifestTx struct {
	pgx.Tx
	db *pausedManifestDB
}

func (tx *pausedManifestTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return tx.db.wrap(ctx, sql, tx.Tx.QueryRow(ctx, sql, args...))
}

type pausedManifestRow struct {
	pgx.Row
	db  *pausedManifestDB
	ctx context.Context
}

func (r *pausedManifestRow) Scan(dest ...any) error {
	err := r.Row.Scan(dest...)
	r.db.once.Do(func() {
		close(r.db.paused)
		select {
		case <-r.db.resume:
		case <-r.ctx.Done():
		}
	})

	return err
}

func Test_MaintenanceHoldsFeatureLock(t *testing.T) {
	for _, operation := range []string{"compaction", "snapshot"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			repo, marker, name := newIsolatedEventLog(ctx, t)
			id := evt.EntityID("account-1")
			events := serializeAs(t, id, "account", 0, &test.AggregateCreated{ID: id, Value: "one"})
			require.NoError(t, repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: events}, "account", id, []byte(`{}`), 1))
			db := &pausedManifestDB{DB: newPool(ctx, t), paused: make(chan struct{}), resume: make(chan struct{})}
			// Mixed-case handles address the same unquoted table and must share the lowercase lock.
			writer := postgres.NewRepository(db, postgres.WithEventsTable(strings.ToUpper(name)), postgres.WithSnapshotsTable(strings.ToUpper(name+"_snapshots")))
			done := make(chan error, 1)
			go func() {
				if operation == "compaction" {
					_, err := writer.CompactBelow(ctx, id, 1)
					done <- err
				} else {
					done <- writer.PutSnapshot(ctx, "account", id, []byte(`{"new":true}`), 2, 1)
				}
			}()

			select {
			case <-db.paused:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// Try rather than time a waiting declaration: false proves exclusion deterministically.
			tx, err := newPool(ctx, t).Begin(ctx)
			if err != nil {
				close(db.resume)
				t.Fatal(err)
			}
			var acquired bool
			err = tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock(1702261761, hashtext($1))", name+":features").Scan(&acquired)
			rollbackErr := tx.Rollback(ctx)
			close(db.resume)
			require.NoError(t, err)
			require.NoError(t, rollbackErr)
			require.False(t, acquired, "a declaration must wait until the paused maintenance write finishes")
			require.NoError(t, <-done)

			_, err = marker.Declare(ctx, func(m *evt.FeatureManifest) {
				m.Features[evt.FeatureJournal] = 1
				m.HistoryDependent = []evt.EntityType{"account"}
			})
			require.NoError(t, err)
			_, err = writer.CompactBelow(ctx, id, 1)
			require.True(t, evt.IsUnsupportedFeatureErr(err))
			require.True(t, evt.IsUnsupportedFeatureErr(writer.PutSnapshot(ctx, "account", id, []byte(`{}`), 3, 1)))
		})
	}
}

func Test_DefaultAndDeclaringWritersRefuseUnsupported(t *testing.T) {
	ctx := context.Background()
	repo, marker, _ := newIsolatedEventLog(ctx, t)
	declaring := repo.WithFeatureMarker(marker)
	id := evt.EntityID("account-1")
	events := serializeAs(t, id, "account", 0, &test.AggregateCreated{ID: id, Value: "one"})
	_, err := marker.Declare(ctx, func(m *evt.FeatureManifest) { m.Features[evt.FeatureJournal] = 1 })
	require.NoError(t, err)

	for _, writer := range []*postgres.Repository{repo, declaring} {
		res := evt.SerializedResult{Events: events}
		require.True(t, evt.IsUnsupportedFeatureErr(writer.Commit(ctx, res)))
		require.True(t, evt.IsUnsupportedFeatureErr(writer.CommitWithSnapshot(ctx, res, "account", id, []byte(`{}`), 1)))
		require.True(t, evt.IsUnsupportedFeatureErr(writer.PutSnapshot(ctx, "account", id, []byte(`{}`), 1, 1)))
	}
	_, err = marker.Declare(ctx, func(m *evt.FeatureManifest) { m.Retention = []evt.EntityType{"session"} })
	require.NoError(t, err)
	require.True(t, evt.IsUnsupportedFeatureErr(declaring.PutSnapshot(ctx, "account", id, []byte(`{}`), 1, 1)))
	stored, err := repo.GetSnapshot(ctx, id)
	require.NoError(t, err)
	require.Nil(t, stored)
}

func Test_CompactionRefusesUnsupportedFeature(t *testing.T) {
	ctx := context.Background()
	repo, marker, _ := newIsolatedEventLog(ctx, t)
	id := evt.EntityID("account-1")
	events := serializeAs(t, id, "account", 0, &test.AggregateCreated{ID: id, Value: "one"})
	require.NoError(t, repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: events}, "account", id, []byte(`{}`), 1))
	_, err := marker.Declare(ctx, func(m *evt.FeatureManifest) { m.Features[evt.FeatureJournal] = 1 })
	require.NoError(t, err)

	deleted, err := repo.CompactBelow(ctx, id, 1)
	require.True(t, evt.IsUnsupportedFeatureErr(err), "unsupported extensions refuse compaction without a history-dependent declaration: %v", err)
	require.Zero(t, deleted)
	stored, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Len(t, stored, 1)
}

// A log created before the feature marker has no features table and no head table, so it cannot
// declare stream-heads, and a write fails until its schema is upgraded. After EnsureSchema, an
// empty log declares the feature itself and every write path works.
func Test_PreMarkerSchemaNeedsAnUpgrade(t *testing.T) {
	ctx := context.Background()
	repo, marker, name := newIsolatedEventLog(ctx, t)
	_, err := newPool(ctx, t).Exec(ctx, "DROP TABLE "+name+"__evt_features, "+name+"__evt_stream_heads")
	require.NoError(t, err)
	manifest, err := marker.Manifest(ctx)
	require.NoError(t, err)
	require.Nil(t, manifest)

	id := evt.EntityID("account-1")
	events := serializeAs(t, id, "account", 0, &test.AggregateCreated{ID: id, Value: "one"})
	require.Error(t, repo.Commit(ctx, evt.SerializedResult{Events: events}), "the marker table is missing")

	require.NoError(t, repo.EnsureSchema(ctx))
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: events}))
	events = serializeAs(t, id, "account", 1, &test.AggregateUpdated{ID: id, Value: "two"})
	require.NoError(t, repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: events}, "account", id, []byte(`{}`), 1))
	require.NoError(t, repo.PutSnapshot(ctx, "account", id, []byte(`{"new":true}`), 2, 2))
	deleted, err := repo.CompactBelow(ctx, id, 1)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	stored, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.Equal(t, evt.EventSequence(2), stored[0].Sequence)

	// The writer declared stream-heads; schema setup and declarations still work afterwards.
	declared, err := marker.Manifest(ctx)
	require.NoError(t, err)
	require.True(t, declared.DeclaresStreamHeads())
	require.NoError(t, repo.EnsureSchema(ctx))
	_, err = marker.Declare(ctx, func(m *evt.FeatureManifest) { m.Features[evt.FeatureJournal] = 1 })
	require.NoError(t, err)
	require.True(t, evt.IsUnsupportedFeatureErr(repo.PutSnapshot(ctx, "account", id, []byte(`{}`), 3, 2)))
}

// A log that holds data but does not declare stream-heads is refused by every write path, and
// entity heads keep reading the events and snapshots until the feature is declared, because
// EnsureSchema creates the head table empty on a populated log.
func Test_PopulatedLogWithoutHeadsIsRefused(t *testing.T) {
	ctx := context.Background()
	repo, marker, name := newIsolatedEventLog(ctx, t)
	pool := newPool(ctx, t)

	// An event a v0.8 writer left behind: no head row, no marker.
	_, err := pool.Exec(ctx, "INSERT INTO "+name+` (entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata)
		VALUES ('account-1', 1, 'account-1:1', 'test:created', 1, 'account', '{}', '{}')`)
	require.NoError(t, err)

	id := evt.EntityID("account-1")
	events := serializeAs(t, id, "account", 1, &test.AggregateUpdated{ID: id, Value: "two"})
	err = repo.Commit(ctx, evt.SerializedResult{Events: events})
	require.ErrorIs(t, err, evt.ErrStreamHeadsNotMigrated)
	require.ErrorIs(t, repo.PutSnapshot(ctx, "account", id, []byte(`{}`), 1, 1), evt.ErrStreamHeadsNotMigrated)

	heads, err := repo.StreamEntityHeads(ctx, "")
	require.NoError(t, err)
	require.Equal(t, evt.EventSequence(1), heads[id], "the aggregate over events serves heads until stream-heads is declared")

	// Declaring is the migration's step, after the head rows exist.
	_, err = pool.Exec(ctx, "INSERT INTO "+name+"__evt_stream_heads (entity_id, entity_type, head) VALUES ('account-1', 'account', 1)")
	require.NoError(t, err)
	_, err = marker.DeclareStreamHeads(ctx)
	require.NoError(t, err)

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: events}))

	heads, err = repo.StreamEntityHeads(ctx, "")
	require.NoError(t, err)
	require.Equal(t, evt.EventSequence(2), heads[id], "the head table serves heads once stream-heads is declared")

	var head int64
	require.NoError(t, pool.QueryRow(ctx, "SELECT head FROM "+name+"__evt_stream_heads WHERE entity_id = 'account-1'").Scan(&head))
	require.Equal(t, int64(2), head)
}

func Test_ConcurrentEnsureSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	repo, _, name := newIsolatedEventLog(ctx, t)
	_, err := newPool(ctx, t).Exec(ctx, fmt.Sprintf("DROP TABLE %s, %s_snapshots, %s__evt_features, %s__evt_stream_heads", name, name, name, name))
	require.NoError(t, err)

	const writers = 8
	start := make(chan struct{})
	errs := make(chan error, writers)
	for range writers {
		go func() {
			<-start
			errs <- repo.EnsureSchema(ctx)
		}()
	}
	close(start)
	for range writers {
		require.NoError(t, <-errs)
	}
	require.NoError(t, repo.PutSnapshot(ctx, "account", "account-1", []byte(`{}`), 1, 0))
}

func Test_SchemaIndexUpgradeAndLogIsolation(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			base := fmt.Sprintf("p%x", time.Now().UnixNano())
			names := []string{base, base + "_entity_type_idx"}
			if reverse {
				names[0], names[1] = names[1], names[0]
			}
			for _, name := range names {
				repo := postgres.NewRepository(pool, postgres.WithEventsTable(name), postgres.WithSnapshotsTable(name+"s"))
				t.Cleanup(func() {
					_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s, %ss, %s__evt_features, %s__evt_stream_heads", name, name, name, name)) //nolint:errcheck // test-only teardown
				})
				require.NoError(t, repo.EnsureSchema(ctx))
				require.NoError(t, repo.EnsureSchema(ctx))
				require.NoError(t, repo.PutSnapshot(ctx, "account", "a", []byte(`{}`), 1, 0))
			}
		})
	}

	t.Run("upgrade", func(t *testing.T) {
		base := fmt.Sprintf("p%x", time.Now().UnixNano())
		repo := postgres.NewRepository(pool, postgres.WithEventsTable(base), postgres.WithSnapshotsTable(base+"s"))
		require.NoError(t, repo.EnsureSchema(ctx))
		t.Cleanup(func() {
			_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s, %ss, %s_entity_type_idx, %s__evt_features, %s__evt_stream_heads", base, base, base, base, base)) //nolint:errcheck // test-only teardown
		})
		_, err := pool.Exec(ctx, fmt.Sprintf("DROP INDEX %s__evt_entity_type_idx; CREATE INDEX %s_entity_type_idx ON %s (entity_type)", base, base, base))
		require.NoError(t, err)
		// Reclaim the old index name for a valid snapshots table during the same schema upgrade.
		upgraded := postgres.NewRepository(pool, postgres.WithEventsTable(base), postgres.WithSnapshotsTable(base+"_entity_type_idx"))
		require.NoError(t, upgraded.EnsureSchema(ctx))
		require.NoError(t, upgraded.EnsureSchema(ctx))
		require.NoError(t, upgraded.PutSnapshot(ctx, "account", "a", []byte(`{}`), 1, 0))
		var kind string
		require.NoError(t, pool.QueryRow(ctx, "SELECT relkind::text FROM pg_class WHERE oid = $1::regclass", base+"__evt_entity_type_idx").Scan(&kind))
		require.Equal(t, "i", kind)
	})
}
