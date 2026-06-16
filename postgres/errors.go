package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/photon-grove/evt"
)

// uniqueViolation is the PostgreSQL SQLSTATE for a unique-constraint violation.
const uniqueViolation = "23505"

// classifyWriteError maps a unique-constraint violation — the symptom of two commits racing on the
// same (entity_id, sequence) pair — to an evt.ConflictError so callers can detect the
// optimistic-concurrency failure. Other errors pass through unchanged.
func classifyWriteError(err error) error {
	if err == nil {
		return nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return evt.NewConflictError(err.Error())
	}

	return err
}
