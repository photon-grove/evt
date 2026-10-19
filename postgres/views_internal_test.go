package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
)

func projectedView(op evt.ViewOperation) evt.ProjectedView {
	return evt.ProjectedView{Projector: "claims", ProjectorIndex: 1, Operation: 2, Op: op}
}

func Test_mapViewError_AUniqueViolationOnAnAbsentPutIsItsFailedCondition(t *testing.T) {
	taken := errors.New("taken")
	cause := &pgconn.PgError{Code: "23505", ConstraintName: "views_pkey"}
	view := projectedView(evt.PutView("pk", "sk", "o", "", nil).IfAbsent().OnConflict(taken))

	err := mapViewError(view, "views_pkey", cause)

	var failed *evt.ProjectorOperationError

	require.ErrorAs(t, err, &failed)
	require.Equal(t, evt.ReasonConditionFailed, failed.Reason)
	require.Equal(t, "claims", failed.Projector)
	require.Equal(t, 1, failed.ProjectorIndex)
	require.Equal(t, 2, failed.Operation)
	require.ErrorIs(t, err, taken, "the mapped error rides a failed condition")
	require.ErrorIs(t, err, cause, "and the database's error stays matchable")
}

func Test_mapViewError_AnyOtherDatabaseErrorIsTheStatementsFailureWithItsSQLState(t *testing.T) {
	cause := &pgconn.PgError{Code: "23514"}

	// An unconditioned put that hits a constraint is not a failed condition: its mapped error, if
	// any, does not apply, and the reason is the SQLSTATE.
	view := projectedView(evt.PutView("pk", "sk", "o", "", nil).OnConflict(errors.New("taken")))

	var failed *evt.ProjectorOperationError

	require.ErrorAs(t, mapViewError(view, "views_pkey", cause), &failed)
	require.Equal(t, "23514", failed.Reason)
	require.Nil(t, failed.Err)
	require.ErrorIs(t, failed, cause)

	// A unique violation on an unconditioned put is the same: it is a constraint the table has.
	require.ErrorAs(t, mapViewError(view, "views_pkey", &pgconn.PgError{Code: "23505", ConstraintName: "views_pkey"}), &failed)
	require.Equal(t, "23505", failed.Reason)
}

func Test_mapViewError_ATransactionRollbackOrAConnectionErrorIsNotTheProjectors(t *testing.T) {
	view := projectedView(evt.PutView("pk", "sk", "o", "", nil))

	for name, cause := range map[string]error{
		"serialization failure": &pgconn.PgError{Code: "40001"},
		"deadlock":              &pgconn.PgError{Code: "40P01"},
		"connection":            errors.New("connection reset"),
	} {
		err := mapViewError(view, "views_pkey", cause)
		require.NotErrorIs(t, err, evt.ErrProjectorOperation, name)
		require.ErrorIs(t, err, cause, name)
	}
}

func Test_viewCommitError_AnIntegrityViolationAtCommitIsTheProjectorsAndNamesNoOperation(t *testing.T) {
	failure := viewCommitError(&pgconn.PgError{Code: "23505"})

	require.NotNil(t, failure)
	require.Equal(t, "(at commit)", failure.Projector)

	require.Nil(t, viewCommitError(&pgconn.PgError{Code: "40001"}))
	require.Nil(t, viewCommitError(errors.New("connection reset")))
}

func Test_buildViews_RefusesWithoutAWriterAndWrapsAWriterFailure(t *testing.T) {
	repo := NewRepository(nil)
	views := []evt.ProjectedView{projectedView(evt.PutView("pk", "sk", "o", "", nil))}

	_, err := repo.buildViews(views)

	var refused *evt.ViewsNotConfiguredError

	require.ErrorAs(t, err, &refused)
	require.Equal(t, 1, refused.Operations)
	require.Equal(t, "postgres", refused.Backend)

	built, err := repo.buildViews(nil)
	require.NoError(t, err)
	require.Empty(t, built, "no view operations need no writer")

	cause := errors.New("no table for this row")
	failing := repo.WithViewWriter(func(evt.ViewOperation) (ViewStatement, error) { return ViewStatement{}, cause })

	_, err = failing.buildViews(views)
	require.ErrorIs(t, err, cause)
	require.NotErrorIs(t, err, evt.ErrProjectorOperation)

	empty := repo.WithViewWriter(func(evt.ViewOperation) (ViewStatement, error) { return ViewStatement{SQL: " "}, nil })

	var invalid *evt.InvalidViewOperationError

	_, err = empty.buildViews(views)
	require.ErrorAs(t, err, &invalid)

	ok := repo.WithViewWriter(func(op evt.ViewOperation) (ViewStatement, error) {
		return ViewStatement{SQL: "DELETE FROM v WHERE pk = $1", Args: []any{op.PK}}, nil
	})

	built, err = ok.buildViews(views)
	require.NoError(t, err)
	require.Len(t, built, 1)
	require.Nil(t, repo.viewWriter, "WithViewWriter returns a copy")
}

func Test_mapViewError_AUniqueViolationOfAnotherConstraintIsNotTheFailedCondition(t *testing.T) {
	taken := errors.New("taken")
	view := projectedView(evt.PutView("pk", "sk", "o", "", nil).IfAbsent().OnConflict(taken))
	cause := &pgconn.PgError{Code: "23505", ConstraintName: "views_payload_key"}

	var failed *evt.ProjectorOperationError

	err := mapViewError(view, "views_pkey", cause)
	require.ErrorAs(t, err, &failed)
	require.Equal(t, "23505", failed.Reason, "the SQLSTATE, not a failed condition")
	require.Nil(t, failed.Err, "the mapped error belongs to a failed condition only")
	require.NotErrorIs(t, err, taken)
	require.ErrorIs(t, err, cause)

	// The constraint name compares case-insensitively, as PostgreSQL folds unquoted names.
	require.ErrorIs(t, mapViewError(view, "Views_PKey", &pgconn.PgError{Code: "23505", ConstraintName: "views_pkey"}), taken)
}

func Test_buildViews_AnAbsentPutNeedsTheKeyConstraintNamed(t *testing.T) {
	views := []evt.ProjectedView{projectedView(evt.PutView("pk", "sk", "o", "", nil).IfAbsent())}

	unnamed := NewRepository(nil).WithViewWriter(func(evt.ViewOperation) (ViewStatement, error) {
		return ViewStatement{SQL: "INSERT INTO v VALUES ($1)"}, nil
	})

	var invalid *evt.InvalidViewOperationError

	_, err := unnamed.buildViews(views)
	require.ErrorAs(t, err, &invalid)

	named := NewRepository(nil).WithViewWriter(func(evt.ViewOperation) (ViewStatement, error) {
		return ViewStatement{SQL: "INSERT INTO v VALUES ($1)", KeyConstraint: "v_pkey"}, nil
	})

	_, err = named.buildViews(views)
	require.NoError(t, err)

	// An unconditioned put needs no constraint: it overwrites.
	_, err = unnamed.buildViews([]evt.ProjectedView{projectedView(evt.PutView("pk", "sk", "o", "", nil))})
	require.NoError(t, err)
}

func Test_viewConditionFailed_ReadsTheAffectedRowCountPerOperationShape(t *testing.T) {
	put := evt.PutView("pk", "sk", "o", "", nil)
	deletion := evt.DeleteView("pk", "sk", "o")

	// An upsert guarded by the owner affects no row when another owner holds the key.
	require.False(t, viewConditionFailed(put, 1))
	require.True(t, viewConditionFailed(put, 0))

	// A put that requires the row to be absent fails with a unique violation, never by count.
	require.False(t, viewConditionFailed(put.IfAbsent(), 0))
	require.False(t, viewConditionFailed(put.IfAbsent(), 1))

	// A delete that requires the owner's row fails when it deleted none.
	require.False(t, viewConditionFailed(deletion.IfPresent(), 1))
	require.True(t, viewConditionFailed(deletion.IfPresent(), 0))

	// A delete without a condition reports the rows of another owner it found, which fail it.
	require.False(t, viewConditionFailed(deletion, 0))
	require.True(t, viewConditionFailed(deletion, 1))
}

func Test_DeclareHistoryDependent_ChecksTheFeatureMarkerBeforeAnyRead(t *testing.T) {
	repo := NewRepository(nil)

	moved := *repo
	moved.eventsTable = "other_events"

	var mismatch *evt.FeatureMarkerTableMismatchError

	require.ErrorAs(t, moved.DeclareHistoryDependent(context.Background(), "account"), &mismatch)
}
