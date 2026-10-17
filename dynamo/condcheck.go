package dynamo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
)

// conflictError is a DynamoDB failure that means another writer moved state the commit depended
// on. It matches evt.ErrOptimisticConflict, so fence-aware retry (ADR 0004, C5) re-prepares the
// command; each sentinel below keeps its own identity for errors.Is.
type conflictError struct{ message string }

func (e *conflictError) Error() string { return e.message }

// Is makes errors.Is(err, evt.ErrOptimisticConflict) match.
func (e *conflictError) Is(target error) bool { return target == evt.ErrOptimisticConflict }

// ErrSnapshotRaceCondition is returned when a snapshot's sequence number was updated by another
// transaction between read and write. Callers can retry with a fresh entity load. It matches
// evt.ErrOptimisticConflict.
var ErrSnapshotRaceCondition error = &conflictError{"race condition - sequence number updated since it was read, try again"}

// ErrTransactionConflict is returned when DynamoDB cancelled a commit because another transaction
// was writing the same items at the same time. Running the command again can succeed. It matches
// evt.ErrOptimisticConflict.
var ErrTransactionConflict error = &conflictError{"transaction conflict - another writer was committing at the same time, try again"}

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
