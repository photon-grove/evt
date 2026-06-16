package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
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

	require.Equal(t, 2, strings.Count(ddl, "CREATE TABLE IF NOT EXISTS"))
	require.Contains(t, ddl, "CREATE INDEX IF NOT EXISTS")
}
