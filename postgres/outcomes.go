package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/photon-grove/evt"
)

// outcomesInfix derives the command-outcome table from the configured events table through the
// reserved infix, so two event logs in one database never share outcome rows.
const outcomesInfix = "__evt_command_outcomes"

// outcomesTable returns the derived command-outcome table name for the configured events table.
func (repo *Repository) outcomesTable() string {
	return repo.eventsTable + outcomesInfix
}

// insertOutcome writes the outcome row as a plain INSERT in the commit transaction. A unique
// violation means another writer recorded the command first; it surfaces as
// *evt.OutcomeConflictError rather than as an optimistic-concurrency conflict, and the engine
// resolves it by re-reading the row.
func (repo *Repository) insertOutcome(ctx context.Context, tx pgx.Tx, outcome *evt.Outcome) error {
	if outcome == nil {
		return nil
	}

	events, err := evt.EncodeEventIDs(outcome.EventIDs)
	if err != nil {
		return fmt.Errorf("postgres: encode outcome events: %w", err)
	}

	if len(outcome.EventIDs) == 0 {
		events = "[]"
	}

	var (
		rejection any
		code      any
	)

	if outcome.Rejection != nil {
		encoded, rerr := evt.EncodeRejection(*outcome.Rejection)
		if rerr != nil {
			return fmt.Errorf("postgres: encode outcome rejection: %w", rerr)
		}

		rejection, code = string(encoded), string(outcome.Rejection.Code)
	}

	var response any
	if len(outcome.Response) > 0 {
		response = outcome.Response
	}

	_, err = tx.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (command_id, status, digest, response, events, rejection, code)
		 VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7)`,
		repo.outcomesTable(),
	), string(outcome.CommandID), string(outcome.Status), outcome.Digest, response, events, rejection, code)
	if err == nil {
		return nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return &evt.OutcomeConflictError{CommandID: outcome.CommandID}
	}

	return fmt.Errorf("postgres: insert outcome %s: %w", outcome.CommandID, err)
}

// GetOutcome reads a command's outcome row, or nil when none exists. PostgreSQL reads are
// transactionally consistent, so no consistency flag is needed.
func (repo *Repository) GetOutcome(ctx context.Context, id evt.CommandID) (*evt.Outcome, error) {
	if err := evt.CheckCommandID(id); err != nil {
		return nil, err
	}

	var (
		status, digest string
		response       []byte
		events         string
		rejection      *string
		code           *string
	)

	err := repo.db.QueryRow(ctx, fmt.Sprintf(
		`SELECT status, digest, response, events::text, rejection::text, code FROM %s WHERE command_id = $1`,
		repo.outcomesTable(),
	), string(id)).Scan(&status, &digest, &response, &events, &rejection, &code)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("postgres: get outcome %s: %w", id, err)
	}

	outcome := evt.Outcome{CommandID: id, Status: evt.OutcomeStatus(status), Digest: digest, Response: response}

	corrupt := func(err error) error { return &evt.CorruptOutcomeError{CommandID: id, Reason: err.Error()} }

	// jsonb normalizes the stored text; the decoded values are what round-trip.
	if ids, derr := evt.DecodeEventIDs(events); derr != nil {
		return nil, corrupt(derr)
	} else if len(ids) > 0 {
		outcome.EventIDs = ids
	}

	if rejection != nil {
		decoded, derr := evt.DecodeRejection([]byte(*rejection))
		if derr != nil {
			return nil, corrupt(derr)
		}

		outcome.Rejection = &decoded
	}

	if err = evt.ValidateStoredOutcome(outcome); err != nil {
		return nil, err
	}

	// The code column repeats the rejection's code.
	if outcome.Rejection != nil && (code == nil || *code != string(outcome.Rejection.Code)) {
		return nil, corrupt(errors.New("the code column disagrees with the rejection"))
	}

	return &outcome, nil
}
