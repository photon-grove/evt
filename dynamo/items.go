package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
)

// Convert generic Transactions to DynamoDB TransactWriteItems if the TransactionGroup is based on
// DynamoDB.
func toDynamoItems(transactions evt.Transaction) ([]types.TransactWriteItem, error) {
	if len(transactions) == 0 {
		return nil, nil
	}

	var items []types.TransactWriteItem

	for _, group := range transactions {
		if group == nil {
			continue
		}

		if group.StorageType() != StorageType {
			return nil, fmt.Errorf("TransactionGroup is not based on DynamoDB: %v", group)
		}
		dynamoGroup, ok := group.(TransactionGroup)
		if !ok {
			return nil, fmt.Errorf("TransactionGroup was unable to be converted to DynamoDB Transact Write Items: %v", group)
		}

		items = append(items, dynamoGroup.ToWriteItems()...)
	}

	return items, nil
}
