//go:build !prod

package dynamo

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
)

// Delete removes serialized events from the events table by point-deleting each (pk, sk). It
// performs NO snapshot-safety checks and can leave a stream unreplayable, so it is intended only
// for local and staging fixtures (e.g. resetting test data).
//
// This method is compiled out of production builds: it is guarded by `//go:build !prod`, so a
// binary built with `-tags prod` does not contain it and cannot call it. For principled,
// snapshot-verified log truncation in any environment, use CompactBelow (evt.Compactor) instead.
//
// WARNING: Use only in local and staging environments.
func (repo *Repository) Delete(
	ctx context.Context,
	serializedEvents []evt.SerializedEvent,
) error {
	// Generate Put transactions
	transactions, err := repo.buildEventDeleteTransactions(serializedEvents)
	if err != nil {
		return err
	}

	// Commit the transactions to DynamoDB
	return repo.commitEventsWithTransaction(ctx, transactions, nil)
}

// Generate the TransactWriteItems needed to delete Events from the DynamoDB table
// WARNING: This should only be used locally and in Staging
func (repo *Repository) buildEventDeleteTransactions(serializedEvents []evt.SerializedEvent) ([]types.TransactWriteItem, error) {
	transactions := make([]types.TransactWriteItem, 0, len(serializedEvents))

	for _, event := range serializedEvents {
		// Include it within a TransactWriteItem statement that ensures the given sequence
		// number doesn't already exist (optimistic locking)
		deleteItem := types.Delete{
			TableName: &repo.EventsTable,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: string(event.EntityID)},
				"sk": &types.AttributeValueMemberN{Value: strconv.Itoa(int(event.Sequence))},
			},
		}

		transactions = append(transactions, types.TransactWriteItem{Delete: &deleteItem})
	}

	return transactions, nil
}
