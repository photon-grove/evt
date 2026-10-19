package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/photon-grove/evt"
)

// ViewStatement is the SQL statement that writes one view operation to the application's view table:
// the text, with $1-style placeholders, and its arguments.
type ViewStatement struct {
	SQL  string
	Args []any

	// KeyConstraint is the name of the table's unique or primary-key constraint on (pk, sk). It is
	// required for a put with evt.ViewAbsent: only a violation of this constraint means the row
	// already exists. A unique violation of any other constraint of the table (a unique payload
	// field, say) says nothing about the requested row, so it is an ordinary failure of the
	// statement, with the SQLSTATE as its reason.
	KeyConstraint string
}

// ViewWriter turns one commit-wide view operation (ADR 0004, C7) into the statement that writes it.
// evt defines no PostgreSQL view schema — the application owns its view tables and their DDL — so a
// repository executes whatever statement its writer returns, inside the commit's transaction, after
// the events and before the snapshots.
//
// The repository decides whether the operation's condition held from the statement's outcome, so
// the statement must have these semantics:
//
//   - a put with evt.ViewAbsent must raise a unique violation of the (pk, sk) key constraint when
//     the row exists: write a plain INSERT, with no ON CONFLICT clause, and name the constraint in
//     ViewStatement.KeyConstraint;
//   - a put without a condition overwrites: an INSERT ... ON CONFLICT DO UPDATE;
//   - a delete with evt.ViewPresent must delete a row: a DELETE whose affected-row count is zero
//     fails the condition;
//   - a delete without a condition succeeds whether or not it deleted a row.
//
// A failed condition is an *evt.ProjectorOperationError with the reason evt.ReasonConditionFailed;
// any other error the database raises is one too, with its SQLSTATE as the reason.
type ViewWriter func(op evt.ViewOperation) (ViewStatement, error)

// WithViewWriter returns a shallow copy of the repository that writes commit-wide view operations
// with the given writer. Without one a commit that carries view operations fails closed with
// *evt.ViewsNotConfiguredError before any write. A nil writer removes it. The original repository
// is unchanged.
func (repo *Repository) WithViewWriter(writer ViewWriter) *Repository {
	r := *repo
	r.viewWriter = writer

	return &r
}

// builtView is a view operation and the statement the writer produced for it.
type builtView struct {
	view      evt.ProjectedView
	statement ViewStatement
}

// buildViews turns a commit's view operations into statements, in the order the commit executes
// them. It runs before the budget is checked and before any write, so a repository with no writer,
// or a writer that refuses an operation, fails the whole commit with nothing written.
func (repo *Repository) buildViews(views []evt.ProjectedView) ([]builtView, error) {
	if len(views) == 0 {
		return nil, nil
	}

	if repo.viewWriter == nil {
		return nil, &evt.ViewsNotConfiguredError{Backend: "postgres", Operations: len(views)}
	}

	built := make([]builtView, 0, len(views))

	for _, view := range views {
		statement, err := repo.viewWriter(view.Op)
		if err != nil {
			return nil, fmt.Errorf("postgres: writing %s: %w", evt.ProjectorOperationRef{
				Projector: view.Projector, ProjectorIndex: view.ProjectorIndex, Operation: view.Operation,
			}, err)
		}

		if view.Op.Kind == evt.ViewPut && view.Op.Condition == evt.ViewAbsent && statement.KeyConstraint == "" {
			return nil, &evt.InvalidViewOperationError{
				ProjectorOperationRef: evt.ProjectorOperationRef{
					Projector: view.Projector, ProjectorIndex: view.ProjectorIndex, Operation: view.Operation,
				},
				Reason: "the view writer must name the key constraint of a put that requires the row to be absent",
			}
		}

		if strings.TrimSpace(statement.SQL) == "" {
			return nil, &evt.InvalidViewOperationError{
				ProjectorOperationRef: evt.ProjectorOperationRef{
					Projector: view.Projector, ProjectorIndex: view.ProjectorIndex, Operation: view.Operation,
				},
				Reason: "the view writer returned an empty statement",
			}
		}

		built = append(built, builtView{view: view, statement: statement})
	}

	return built, nil
}

// execViews executes the view statements inside the commit's transaction, in order, and maps the
// first failure to the typed error naming its projector and operation.
func execViews(ctx context.Context, tx pgx.Tx, built []builtView) error {
	for _, item := range built {
		tag, err := tx.Exec(ctx, item.statement.SQL, item.statement.Args...)
		if err != nil {
			return mapViewError(item.view, item.statement.KeyConstraint, err)
		}

		if item.view.Op.Kind == evt.ViewDelete && item.view.Op.Condition == evt.ViewPresent && tag.RowsAffected() == 0 {
			return evt.NewProjectorOperationError(item.view, evt.ReasonConditionFailed, nil)
		}
	}

	return nil
}

// mapViewError maps an error a view statement raised. A violation of the (pk, sk) key constraint on
// a put that requires the row to be absent is its failed condition; a unique violation of any other
// constraint is an ordinary failure of the statement. Any other error the database raised belongs to the
// statement, so it is the operation's failure with the SQLSTATE as its reason, except a transaction
// rollback (serialization failure, deadlock), which is not the projector's doing and passes through
// like it does for every other statement of the commit. A connection error passes through too.
func mapViewError(view evt.ProjectedView, keyConstraint string, err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return fmt.Errorf("postgres: view statement: %w", err)
	}

	switch {
	case pgErr.Code == uniqueViolation && view.Op.Kind == evt.ViewPut && view.Op.Condition == evt.ViewAbsent &&
		keyConstraint != "" && strings.EqualFold(pgErr.ConstraintName, keyConstraint):
		return evt.NewProjectorOperationError(view, evt.ReasonConditionFailed, err)

	case strings.HasPrefix(pgErr.Code, "40"):
		return fmt.Errorf("postgres: view statement: %w", err)
	}

	return evt.NewProjectorOperationError(view, pgErr.Code, err)
}

// viewCommitError maps a failure at COMMIT of a transaction that carried view operations: an
// integrity violation can only come from a deferred constraint on a projector's table, and names no
// operation, so it is the projector's failure rather than the event log's conflict.
func viewCommitError(err error) *evt.ProjectorError {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "23") {
		return &evt.ProjectorError{Projector: "(at commit)", Err: err}
	}

	return nil
}

// commitBudget checks the commit's shared budget with each view operation sized by its SQL text
// instead of the DynamoDB-shaped estimate: the bound arguments are opaque, so the byte estimate is
// a lower bound while the action count is exact, which is what makes CommitTooLarge fire at the same
// boundary on every backend.
func commitBudget(normalized evt.SerializedResult, views []builtView) error {
	own := normalized
	own.Views = nil

	sizes, err := evt.TransactionItemSizes(own)
	if err != nil {
		return err
	}

	sizes = append(sizes, evt.ProjectorOperationSizes(own)...)

	for _, item := range views {
		sizes = append(sizes, len(item.statement.SQL))
	}

	return evt.CommitBudget(sizes)
}

// WritesViews declares that Commit writes commit-wide view operations, or refuses them with
// *evt.ViewsNotConfiguredError when no view writer is configured (evt.ViewWritingRepository).
func (repo *Repository) WritesViews() {}

// DeclareHistoryDependent durably adds the entity types to the feature manifest's historyDependent
// set, so CompactBelow refuses them in every process (evt.HistoryDeclarer). Types already declared
// are left alone and nothing is written.
func (repo *Repository) DeclareHistoryDependent(ctx context.Context, types ...evt.EntityType) error {
	manifest, err := repo.features.Manifest(ctx)
	if err != nil {
		return err
	}

	missing := evt.MissingHistoryDependent(manifest, types)
	if len(missing) == 0 {
		return nil
	}

	_, err = repo.features.Declare(ctx, func(m *evt.FeatureManifest) { m.AddHistoryDependent(missing...) })

	return err
}
