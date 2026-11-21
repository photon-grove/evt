package dynamo

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// HasConditionalCheckFailure checks if the error returned by DynamoDB includes a conditional
// check failure.
func HasConditionalCheckFailure(err error) (bool, int) {
	var transactionCanceled *types.TransactionCanceledException
	if errors.As(err, &transactionCanceled) {
		for i, reason := range transactionCanceled.CancellationReasons {
			// Code is a *string and is nil for items that were not the cause of the
			// cancellation, so guard before dereferencing.
			if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
				return true, i
			}
		}
	}

	return false, 0
}

// hasTransactionConflict reports whether a cancelled transaction was cancelled because another
// transaction was writing the same items, and no condition failed.
func hasTransactionConflict(err error) bool {
	var transactionCanceled *types.TransactionCanceledException
	if !errors.As(err, &transactionCanceled) {
		return false
	}

	for _, reason := range transactionCanceled.CancellationReasons {
		if reason.Code != nil && *reason.Code == "TransactionConflict" {
			return true
		}
	}

	return false
}

// transactionConflictError is a commit DynamoDB cancelled with TransactionConflict. It matches
// ErrTransactionConflict and the SDK's cancellation with errors.Is and errors.As.
type transactionConflictError struct {
	err          error
	onOutcomeRow bool
}

func (e *transactionConflictError) Error() string {
	return ErrTransactionConflict.Error() + ": " + e.err.Error()
}

func (e *transactionConflictError) Unwrap() []error {
	return []error{ErrTransactionConflict, e.err}
}

// CollidedOnOutcomeRow reports whether the commit collided on its command-outcome put. Only
// another attempt of the same command writes that item, so its winner is still committing: the
// store polls the outcome row briefly to replay it, rather than reading it once.
func (e *transactionConflictError) CollidedOnOutcomeRow() bool {
	return e.onOutcomeRow
}
