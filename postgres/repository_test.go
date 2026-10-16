package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
)

func TestClassifyWriteError(t *testing.T) {
	t.Run("nil passes through", func(t *testing.T) {
		require.NoError(t, classifyWriteError(nil))
	})

	t.Run("unique violation becomes a ConflictError", func(t *testing.T) {
		pgErr := &pgconn.PgError{Code: uniqueViolation, Message: "duplicate key value"}
		err := classifyWriteError(pgErr)

		require.Error(t, err)
		require.True(t, evt.IsConflictErr(err), "a 23505 violation should map to evt.ConflictError")
		require.True(t, policy.IsTransient(err), "the conflict is transient so ExecuteWithRetry retries it")
	})

	t.Run("other errors pass through unchanged", func(t *testing.T) {
		sentinel := errors.New("connection refused")
		err := classifyWriteError(sentinel)

		require.ErrorIs(t, err, sentinel)
		require.False(t, evt.IsConflictErr(err))
	})

	t.Run("wrapped unique violation is still detected", func(t *testing.T) {
		pgErr := &pgconn.PgError{Code: uniqueViolation, Message: "dup"}
		wrapped := errors.Join(errors.New("postgres: insert event"), pgErr)

		require.True(t, evt.IsConflictErr(classifyWriteError(wrapped)))
	})
}

func TestNewRepositoryTableNames(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		repo := NewRepository(nil)
		require.Equal(t, defaultEventsTable, repo.eventsTable)
		require.Equal(t, defaultSnapshotsTable, repo.snapshotsTable)
	})

	t.Run("overrides flow into the DDL", func(t *testing.T) {
		repo := NewRepository(nil, WithEventsTable("ledger_events"), WithSnapshotsTable("ledger_snaps"))

		ddl := repo.schemaDDL()
		require.Contains(t, ddl, "ledger_events")
		require.Contains(t, ddl, "ledger_snaps")
		require.Contains(t, ddl, "PRIMARY KEY (entity_id, sequence)")
	})

	t.Run("invalid identifier panics", func(t *testing.T) {
		require.Panics(t, func() {
			NewRepository(nil, WithEventsTable("events; DROP TABLE x"))
		})
	})

	t.Run("derived relations fit at table name boundaries", func(t *testing.T) {
		require.NotPanics(t, func() {
			NewRepository(nil, WithEventsTable(strings.Repeat("e", 34)), WithSnapshotsTable(strings.Repeat("s", 58)))
		})
		require.Panics(t, func() {
			NewRepository(nil, WithEventsTable(strings.Repeat("e", 35)))
		})
		require.Panics(t, func() {
			NewRepository(nil, WithSnapshotsTable(strings.Repeat("s", 59)))
		})
	})

	t.Run("legacy entity type index suffix is available to snapshots", func(t *testing.T) {
		repo := NewRepository(nil, WithEventsTable("events"), WithSnapshotsTable("events_entity_type_idx"))
		require.Contains(t, repo.schemaDDL(), "events__evt_entity_type_idx")
		require.Equal(t, "events_entity_type_idx", repo.snapshotsTable)
	})

	t.Run("names that collide with each other or a derived index panic", func(t *testing.T) {
		for _, names := range [][2]string{
			{"evt_events", "Evt_Events"},
			{"events", "events__evt_entity_type_idx"},
			{"events_pkey", "events"},
		} {
			require.Panics(t, func() {
				NewRepository(nil, WithEventsTable(names[0]), WithSnapshotsTable(names[1]))
			}, "%v", names)
		}
	})

	t.Run("reserved keyword panics", func(t *testing.T) {
		require.Panics(t, func() {
			NewRepository(nil, WithEventsTable("Select"))
		})
	})
}

func TestFoldEntityAppliesOnlyEventsAboveThrough(t *testing.T) {
	ctx := context.Background()

	events := []evt.SerializedEvent{
		{Sequence: 1},
		{Sequence: 2},
		{Sequence: 3},
	}

	var applied []evt.EventSequence
	apply := func(_ context.Context, e evt.SerializedEvent, current evt.Entity) (evt.Entity, error) {
		applied = append(applied, e.Sequence)
		return current, nil
	}

	_, err := foldEntity(ctx, events, nil, 1, apply)
	require.NoError(t, err)
	require.Equal(t, []evt.EventSequence{2, 3}, applied, "events at or below the snapshot floor are skipped")
}

func TestFoldEntityPropagatesApplyError(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("apply failed")

	apply := func(_ context.Context, _ evt.SerializedEvent, _ evt.Entity) (evt.Entity, error) {
		return nil, boom
	}

	_, err := foldEntity(ctx, []evt.SerializedEvent{{Sequence: 1}}, nil, 0, apply)
	require.ErrorIs(t, err, boom)
}

func TestSchemaDDLIsIdempotent(t *testing.T) {
	ddl := NewRepository(nil).schemaDDL()

	require.Equal(t, 3, strings.Count(ddl, "CREATE TABLE IF NOT EXISTS"))
	require.Contains(t, ddl, "CREATE INDEX IF NOT EXISTS")
	require.Contains(t, ddl, defaultEventsTable+"__evt_features")
}

func TestNewRepositoryRejectsReservedInfix(t *testing.T) {
	require.Panics(t, func() {
		NewRepository(nil, WithEventsTable("orders__evt_features"))
	}, "a configured name containing the reserved infix could collide with a derived relation")

	require.Panics(t, func() {
		NewRepository(nil, WithEventsTable("orders"), WithSnapshotsTable("ORDERS__EVT_features"))
	}, "the infix check is case-insensitive")
}

// stubProjectorGroup is a backend-neutral projector group with operations.
type stubProjectorGroup struct{ size int }

func (g stubProjectorGroup) Len() int                           { return g.size }
func (stubProjectorGroup) StorageType() evt.StorageType         { return "stub" }
func (stubProjectorGroup) TransactionType() evt.TransactionType { return "stub" }
func (stubProjectorGroup) HandleError(err error, _ int) error   { return err }
func (g stubProjectorGroup) Merge(evt.TransactionGroup) (evt.TransactionGroup, error) {
	return g, nil
}

func TestCommitRefusesProjectorOperationsBeforeAnyWrite(t *testing.T) {
	// The repository has no database: the refusal must come before the first use of the pool.
	repo := NewRepository(nil)
	event := evt.SerializedEvent{
		ID: "pg-projector:1", Sequence: 1, Type: "t:created", Version: 1,
		EntityID: "pg-projector", EntityType: "t", Payload: []byte(`{}`),
	}

	err := repo.Commit(context.Background(), evt.SerializedResult{
		Events:      []evt.SerializedEvent{event},
		Transaction: evt.Transaction{stubProjectorGroup{size: 2}},
	})

	var unsupported *evt.ProjectorOperationsUnsupportedError
	require.ErrorAs(t, err, &unsupported)
	require.Equal(t, "postgres", unsupported.Backend)
	require.Equal(t, 2, unsupported.Operations)

	// An empty group carries no operation, so it is not refused here (the commit then needs the
	// database, which this repository lacks).
	require.NotPanics(t, func() {
		err := repo.Commit(context.Background(), evt.SerializedResult{
			Transaction: evt.Transaction{stubProjectorGroup{size: 0}},
		})
		require.NoError(t, err, "an eventless commit with no operations is a no-op")
	})

	// The capability is still declared for event, fence, and snapshot-only commits.
	require.NoError(t, evt.RequireTransactions(repo))
}
