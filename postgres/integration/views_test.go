//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/postgres"
)

// viewTable is an application-owned view table, private to one test and dropped afterwards. evt
// defines no PostgreSQL view schema: the table, its DDL, and the SQL that writes it are the
// application's, which is the contract postgres.ViewWriter documents.
type viewTable struct {
	pool *pgxpool.Pool
	name string
}

func newViewTable(ctx context.Context, t *testing.T, pool *pgxpool.Pool) viewTable {
	t.Helper()

	name := fmt.Sprintf("evt_it_views_%d", time.Now().UnixNano())

	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
		pk text NOT NULL, sk text NOT NULL, entity_id text NOT NULL, entity_type text NOT NULL, payload text NOT NULL,
		PRIMARY KEY (pk, sk))`, name))
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+name) //nolint:errcheck // best-effort teardown of a test-only table
	})

	return viewTable{pool: pool, name: name}
}

// writer is the table's postgres.ViewWriter, with the ownership semantics it documents: a plain
// INSERT for a put that requires the row to be absent (its unique violation is the failed
// condition), an upsert guarded by the owner for any other put, a DELETE of the owner's row, and
// for a delete without a presence condition a statement that reports the rows of another owner.
func (v viewTable) writer(op evt.ViewOperation) (postgres.ViewStatement, error) {
	if op.Kind == evt.ViewDelete {
		args := []any{op.PK, op.SK, string(op.Owner)}
		if op.Condition == evt.ViewPresent {
			return postgres.ViewStatement{
				SQL:  fmt.Sprintf("DELETE FROM %s WHERE pk = $1 AND sk = $2 AND entity_id = $3", v.name),
				Args: args,
			}, nil
		}

		return postgres.ViewStatement{
			SQL: fmt.Sprintf("WITH gone AS (DELETE FROM %[1]s WHERE pk = $1 AND sk = $2 AND entity_id = $3) "+
				"SELECT 1 FROM %[1]s WHERE pk = $1 AND sk = $2 AND entity_id <> $3", v.name),
			Args: args,
		}, nil
	}

	insert := fmt.Sprintf("INSERT INTO %s (pk, sk, entity_id, entity_type, payload) VALUES ($1, $2, $3, $4, $5)", v.name)
	args := []any{op.PK, op.SK, string(op.Owner), string(op.EntityType), string(op.Payload)}

	if op.Condition == evt.ViewAbsent {
		return postgres.ViewStatement{SQL: insert, Args: args, KeyConstraint: v.name + "_pkey"}, nil
	}

	return postgres.ViewStatement{
		SQL: insert + fmt.Sprintf(" ON CONFLICT (pk, sk) DO UPDATE SET entity_type = EXCLUDED.entity_type, "+
			"payload = EXCLUDED.payload WHERE %s.entity_id = EXCLUDED.entity_id", v.name),
		Args: args,
	}, nil
}

func (v viewTable) read(ctx context.Context, pk, sk string) (*evt.SerializedView, error) {
	view := evt.SerializedView{PK: pk, SK: sk}

	var payload string

	err := v.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT entity_id, entity_type, payload FROM %s WHERE pk = $1 AND sk = $2", v.name), pk, sk,
	).Scan(&view.EntityID, &view.EntityType, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	view.Payload = []byte(payload)

	return &view, nil
}

func newConstrainedViewTable(ctx context.Context, t *testing.T, pool *pgxpool.Pool, constraint string) viewTable {
	t.Helper()

	name := fmt.Sprintf("evt_it_views_%d", time.Now().UnixNano())

	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
		pk text NOT NULL, sk text NOT NULL, entity_id text NOT NULL, entity_type text NOT NULL, payload text NOT NULL,
		PRIMARY KEY (pk, sk), %s)`, name, constraint))
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+name) //nolint:errcheck // best-effort teardown of a test-only table
	})

	return viewTable{pool: pool, name: name}
}

func viewEvents(id evt.EntityID) []evt.SerializedEvent {
	return []evt.SerializedEvent{{
		ID: evt.GetEventID(id, 1), Sequence: 1, Type: "t:e", Version: 1, EntityID: id, EntityType: "t", Payload: []byte(`{}`),
	}}
}

func Test_ViewStatementsRunInTheCommitTransaction(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)

	// A check constraint of the application's table rejects the second statement: the typed error
	// names that operation with the SQLSTATE, and neither the first row nor the events survive.
	views := newConstrainedViewTable(ctx, t, pool, "CONSTRAINT payload_not_bad CHECK (payload <> 'bad')")
	repo := postgres.NewRepository(pool).WithViewWriter(views.writer)
	id := evt.EntityID(fmt.Sprintf("view-tx-%d", time.Now().UnixNano()))

	err := repo.Commit(ctx, evt.SerializedResult{
		Events: viewEvents(id),
		Views: []evt.ProjectedView{
			{Projector: "good", Op: evt.PutView("pk", "first", "o", "", []byte("fine"))},
			{Projector: "strict", ProjectorIndex: 1, Op: evt.PutView("pk", "second", "o", "", []byte("bad"))},
		},
	})

	var failed *evt.ProjectorOperationError

	require.ErrorAs(t, err, &failed)
	require.Equal(t, "strict", failed.Projector)
	require.Equal(t, "23514", failed.Reason, "the SQLSTATE of the check violation")
	require.False(t, evt.IsConflictErr(err))

	events, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Empty(t, events)

	row, err := views.read(ctx, "pk", "first")
	require.NoError(t, err)
	require.Nil(t, row, "the first statement rolled back with the commit")
}

func Test_AViolationOfADeferredConstraintAtCommitIsTheProjectorsAndNamesNoOperation(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)

	views := newConstrainedViewTable(ctx, t, pool, "CONSTRAINT one_payload UNIQUE (payload) DEFERRABLE INITIALLY DEFERRED")
	repo := postgres.NewRepository(pool).WithViewWriter(views.writer)
	id := evt.EntityID(fmt.Sprintf("view-deferred-%d", time.Now().UnixNano()))

	// Both statements succeed; the database refuses the transaction only at COMMIT.
	err := repo.Commit(ctx, evt.SerializedResult{
		Events: viewEvents(id),
		Views: []evt.ProjectedView{
			{Projector: "p", Op: evt.PutView("pk", "a", "o", "", []byte("same"))},
			{Projector: "p", Operation: 1, Op: evt.PutView("pk", "b", "o", "", []byte("same"))},
		},
	})

	var failed *evt.ProjectorError

	require.ErrorAs(t, err, &failed)
	require.NotErrorIs(t, err, evt.ErrProjectorOperation, "a commit-time failure names no operation")
	require.False(t, evt.IsConflictErr(err), "a deferred unique violation is not the event log's conflict")

	events, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Empty(t, events)
}

func Test_ViewOperationsWithoutAWriterAreRefusedBeforeAnyWrite(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)

	repo := postgres.NewRepository(pool)
	id := evt.EntityID(fmt.Sprintf("view-unconfigured-%d", time.Now().UnixNano()))

	err := repo.Commit(ctx, evt.SerializedResult{
		Events: viewEvents(id),
		Views:  []evt.ProjectedView{{Projector: "p", Op: evt.PutView("pk", "sk", "o", "", nil)}},
	})

	var refused *evt.ViewsNotConfiguredError

	require.ErrorAs(t, err, &refused)

	events, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Empty(t, events)
}

func Test_AViewWriterThatRefusesAnOperationFailsTheWholeCommit(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)

	cause := errors.New("this projector writes no table here")
	repo := postgres.NewRepository(pool).WithViewWriter(func(evt.ViewOperation) (postgres.ViewStatement, error) {
		return postgres.ViewStatement{}, cause
	})
	id := evt.EntityID(fmt.Sprintf("view-writer-refuses-%d", time.Now().UnixNano()))

	err := repo.Commit(ctx, evt.SerializedResult{
		Events: viewEvents(id),
		Views:  []evt.ProjectedView{{Projector: "p", Op: evt.PutView("pk", "sk", "o", "", nil)}},
	})
	require.ErrorIs(t, err, cause)

	events, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Empty(t, events)
}

func Test_AUniqueViolationOfAnotherConstraintIsNotAFailedAbsentCondition(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)

	views := newConstrainedViewTable(ctx, t, pool, "CONSTRAINT one_payload UNIQUE (payload)")
	repo := postgres.NewRepository(pool).WithViewWriter(views.writer)
	suffix := time.Now().UnixNano()
	first, second := evt.EntityID(fmt.Sprintf("view-unique-a-%d", suffix)), evt.EntityID(fmt.Sprintf("view-unique-b-%d", suffix))

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: viewEvents(first),
		Views:  []evt.ProjectedView{{Projector: "p", Op: evt.PutView("pk", "a", "o", "", []byte("same"))}},
	}))

	// (pk, sk) is free, but the payload collides with the other row's: a violation of the payload's
	// constraint, not of the key's. The row the condition asked about does not exist.
	taken := errors.New("row taken")
	err := repo.Commit(ctx, evt.SerializedResult{
		Events: viewEvents(second),
		Views:  []evt.ProjectedView{{Projector: "p", Op: evt.PutView("pk", "b", "o", "", []byte("same")).IfAbsent().OnConflict(taken)}},
	})

	var failed *evt.ProjectorOperationError

	require.ErrorAs(t, err, &failed)
	require.Equal(t, "23505", failed.Reason)
	require.NotEqual(t, evt.ReasonConditionFailed, failed.Reason)
	require.NotErrorIs(t, err, taken)

	// The same collision on the key itself is the failed condition.
	err = repo.Commit(ctx, evt.SerializedResult{
		Events: viewEvents(evt.EntityID(fmt.Sprintf("view-unique-c-%d", suffix))),
		Views:  []evt.ProjectedView{{Projector: "p", Op: evt.PutView("pk", "a", "o", "", []byte("other")).IfAbsent().OnConflict(taken)}},
	})
	require.ErrorAs(t, err, &failed)
	require.Equal(t, evt.ReasonConditionFailed, failed.Reason)
	require.ErrorIs(t, err, taken)
}
