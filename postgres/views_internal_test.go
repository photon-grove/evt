package postgres

import (
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
	cause := &pgconn.PgError{Code: "23505"}
	view := projectedView(evt.PutView("pk", "sk", "", "", nil).IfAbsent().OnConflict(taken))

	err := mapViewError(view, cause)

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
	view := projectedView(evt.PutView("pk", "sk", "", "", nil).OnConflict(errors.New("taken")))

	var failed *evt.ProjectorOperationError

	require.ErrorAs(t, mapViewError(view, cause), &failed)
	require.Equal(t, "23514", failed.Reason)
	require.Nil(t, failed.Err)
	require.ErrorIs(t, failed, cause)

	// A unique violation on an unconditioned put is the same: it is a constraint the table has.
	require.ErrorAs(t, mapViewError(view, &pgconn.PgError{Code: "23505"}), &failed)
	require.Equal(t, "23505", failed.Reason)
}

func Test_mapViewError_ATransactionRollbackOrAConnectionErrorIsNotTheProjectors(t *testing.T) {
	view := projectedView(evt.PutView("pk", "sk", "", "", nil))

	for name, cause := range map[string]error{
		"serialization failure": &pgconn.PgError{Code: "40001"},
		"deadlock":              &pgconn.PgError{Code: "40P01"},
		"connection":            errors.New("connection reset"),
	} {
		err := mapViewError(view, cause)
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
	views := []evt.ProjectedView{projectedView(evt.PutView("pk", "sk", "", "", nil))}

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
