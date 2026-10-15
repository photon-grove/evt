package dynamo

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
)

// ErrSnapshotRaceCondition is returned when a snapshot's sequence number was updated by another
// transaction between read and write. Callers can retry with a fresh entity load.
var ErrSnapshotRaceCondition = errors.New("race condition - sequence number updated since it was read, try again")

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
