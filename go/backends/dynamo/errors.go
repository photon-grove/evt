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
