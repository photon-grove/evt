package dynamo

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	evt "github.com/photon-grove/evt/go"
)

// conflictError is a DynamoDB failure that means another writer moved state the commit depended
// on. It matches evt.ErrOptimisticConflict, so fence-aware retry (ADR 0004, C5) re-prepares the
// command; ErrSnapshotRaceCondition keeps its own identity for errors.Is.
type conflictError struct{ message string }

func (e *conflictError) Error() string { return e.message }

// Is makes errors.Is(err, evt.ErrOptimisticConflict) match.
func (e *conflictError) Is(target error) bool { return target == evt.ErrOptimisticConflict }

// ErrSnapshotRaceCondition is returned when a snapshot's sequence number was updated by another
// transaction between read and write. Callers can retry with a fresh entity load. It matches
// evt.ErrOptimisticConflict.
var ErrSnapshotRaceCondition error = &conflictError{"race condition - sequence number updated since it was read, try again"}

// ErrTransactionConflict is returned when DynamoDB cancelled a commit with TransactionConflict:
// another transaction was writing one of the same items at the same moment, which can be an item
// the command never changes, such as the feature-marker or rebuild-lock check, or a fence another
// command shares. Nothing was written and nothing the commit was decided on is known to have gone
// stale, so it is transient (policy.IsTransient), like throttling, and deliberately does not match
// evt.ErrOptimisticConflict: fence-aware retry runs the command again after a backoff and returns
// the error when the attempts run out, never recording a stale-fence rejection, so the same command
// ID can be sent again.
var ErrTransactionConflict = errors.New("transaction conflict - another writer was committing at the same time, try again")

// handleConditionalCheckFailure translates conditional check failures into domain errors. The
// itemIndexOffset is the number of control items (the feature-marker check) prepended to the
// transaction ahead of the caller's groups; indices reported by DynamoDB include them.
func (repo *Repository) handleConditionalCheckFailure(
	_ context.Context,
	transaction evt.Transaction,
	transactItems []types.TransactWriteItem,
	itemIndexOffset int,
	err error,
) error {
	if ok, i := HasConditionalCheckFailure(err); ok {
		i -= itemIndexOffset
		if i < len(transactItems) {
			// Identify the failing group and delegate error handling
			offset := 0
			for _, group := range transaction {
				if i < offset+group.Len() {
					return group.HandleError(err, i-offset)
				}
				offset += group.Len()
			}
			return wrapClassifiedError(err)
		}
		return wrapClassifiedError(ErrSnapshotRaceCondition)
	}

	return wrapClassifiedError(err)
}
