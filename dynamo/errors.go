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
			if *reason.Code == "ConditionalCheckFailed" {
				return true, i
			}
		}
	}

	return false, 0
}
