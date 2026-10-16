package dynamo

import "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

// TransactItemSize exposes the projector-item size estimate to the external size tests.
func TransactItemSize(item types.TransactWriteItem) int { return transactItemSize(item) }
