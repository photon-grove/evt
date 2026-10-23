package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
)

// uniqueViolation is the PostgreSQL SQLSTATE for a unique-constraint violation.
const uniqueViolation = "23505"

// classifyWriteError maps a unique-constraint violation — the symptom of two commits racing on the
// same (entity_id, sequence) pair — to an evt.ConflictError so callers can detect the
// optimistic-concurrency failure. Other errors pass through unchanged. The conflict is classified
// as transient, as DynamoDB's is, so a retry reloads and tries again.
func classifyWriteError(err error) error {
	if err == nil {
		return nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return transientConflict(err.Error())
	}

	return err
}

// transientConflict returns an evt.ConflictError classified as transient. evt.IsConflictErr still
// matches it.
func transientConflict(message string) error {
	return &policy.ClassifiedError{Class: policy.ClassTransient, Err: evt.NewConflictError(message)}
}
