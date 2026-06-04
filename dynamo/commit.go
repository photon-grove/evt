package dynamo

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
)

// Commit writes serialized events to the events table.
func (repo *Repository) Commit(
	ctx context.Context,
	result evt.SerializedResult,
) error {
	if len(result.Events) == 0 {
		return nil
	}

	// Generate Put transactions
	writeItems, _, err := repo.buildEventPutTransactions(result.Events)
	if err != nil {
		return err
	}

	// Commit the transactions to DynamoDB
	return repo.commitEventsWithTransaction(ctx, writeItems, result.Transaction)
}

// CommitStream commits a stream of events using batches under the 100-item limit.
func (repo *Repository) CommitStream(
	ctx context.Context,
	channel <-chan result.Result[evt.SerializedResult],
) []error {
	var errors []error

	var batch []types.TransactWriteItem

	for result := range channel {
		serializedResult, err := result.Unwrap()
		if err != nil {
			errors = append(errors, err)
			continue
		}

		items, _, err := repo.buildEventPutTransactions(serializedResult.Events)
		if err != nil {
			errors = append(errors, err)
			continue
		}

		// Keep the batches under 100 items total, with transactions included
		transactItems, err := toDynamoItems(serializedResult.Transaction)
		if err != nil {
			errors = append(errors, err)
			continue
		}

		// Combine the two, with included transaction items first
		combined := append(transactItems, items...)

		// Guard against a single result that already exceeds the 100-item transaction limit
		if len(combined) > 100 {
			errors = append(errors, fmt.Errorf(
				"single result produces %d items, exceeding DynamoDB 100-item transaction limit",
				len(combined),
			))
			continue
		}

		// Check to see if the batch would overrun the limit of 100 items
		if len(batch)+len(combined) > 99 {
			// Commit the batch
			if err = repo.commitEventsWithTransaction(ctx, batch, nil); err != nil {
				errors = append(errors, err)
			}

			// Reset the batch
			batch = combined
		} else {
			// Add the combined items to the batch
			batch = append(batch, combined...)
		}
	}

	// Flush the final items from the batch
	if len(batch) > 0 {
		if err := repo.commitEventsWithTransaction(ctx, batch, nil); err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

// CommitWithSnapshot commits events and a snapshot in a single transaction.
func (repo *Repository) CommitWithSnapshot(
	ctx context.Context,
	result evt.SerializedResult,
	entityType evt.EntityType,
	entityID evt.EntityID,
	payload []byte,
	currentSnapshot evt.EventSequence,
) error {
	// Generate both the regular Put transactions, and a Put for the Snapshot
	writeItems, err := repo.buildSnapshotPutTransactions(result.Events, entityType, entityID, payload, currentSnapshot)
	if err != nil {
		return err
	}

	// Commit the transactions to DynamoDB
	return repo.commitEventsWithTransaction(ctx, writeItems, result.Transaction)
}

// Generate the TransactWriteItems needed to Put new Events to the DynamoDB table
func (repo *Repository) buildEventPutTransactions(serializedEvents []evt.SerializedEvent) ([]types.TransactWriteItem, evt.EventSequence, error) {
	currentSequence := evt.EventSequence(0)
	transactions := make([]types.TransactWriteItem, 0, len(serializedEvents))

	for _, event := range serializedEvents {
		// Keep track of the latest Event sequence number
		currentSequence = event.Sequence

		// Do the same for the Metadata
		metadataBytes, err := json.Marshal(event.Metadata)
		if err != nil {
			return nil, currentSequence, err
		}

		// Create the serialized representation of a DynamoDB Event. TTL is non-zero only when the
		// entity type has a retention policy; otherwise omitempty drops the attribute entirely.
		evt := Event{
			PK:         event.EntityID,
			SK:         event.Sequence,
			EntityType: event.EntityType,
			Version:    event.Version,
			Type:       event.Type,
			Payload:    string(event.Payload),
			Metadata:   string(metadataBytes),
			TTL:        repo.ttlFor(event.EntityType),
		}

		// Convert to a map of AttributeValues
		item, err := repo.marshalMap(evt)
		if err != nil {
			return nil, currentSequence, fmt.Errorf("error marshalling item to map: %w", err)
		}

		// Include it within a TransactWriteItem statement that ensures the given sequence
		// number doesn't already exist (optimistic locking)
		putItem := types.Put{
			TableName:           &repo.EventsTable,
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(sk)"),
		}

		transactions = append(transactions, types.TransactWriteItem{Put: &putItem})
	}

	return transactions, currentSequence, nil
}

// Generate the TransactWriteItems needed to Put new Snapshots to the DynamoDB table
func (repo *Repository) buildSnapshotPutTransactions(
	serializedEvents []evt.SerializedEvent,
	entityType evt.EntityType,
	entityID evt.EntityID,
	payload []byte,
	currentSnapshot evt.EventSequence,
) ([]types.TransactWriteItem, error) {
	expectedSnapshot := evt.EventSequence(int(currentSnapshot) - 1)

	// Generate the initial Put transactions that are needed to commit the Events themselves
	transactions, currentSequence, err := repo.buildEventPutTransactions(serializedEvents)
	if err != nil {
		return nil, err
	}

	// Create the serialized representation of a DynamoDB Snapshot (inline at sk=0). TTL mirrors the
	// events' policy so a snapshot never outlives the stream it summarizes; omitempty drops it for
	// un-policed entity types.
	event := Snapshot{
		PK:            entityID,
		SK:            0,
		Sequence:      currentSnapshot,
		EventSequence: currentSequence,
		EntityType:    entityType,
		Payload:       string(payload),
		TTL:           repo.ttlFor(entityType),
	}

	// Convert to a map of AttributeValues
	item, err := repo.marshalMap(event)
	if err != nil {
		return nil, fmt.Errorf("error marshalling item to map: %w", err)
	}

	// Include it within a TransactWriteItem statement that ensures the existing Snapshot
	// matches the previous Snapshot number (more optimistic locking).
	// Snapshots are stored inline in the event-log table at sk=0.
	putItem := types.Put{
		TableName:           &repo.EventsTable,
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(seq) OR (seq = :seq)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":seq": &types.AttributeValueMemberN{Value: strconv.Itoa(int(expectedSnapshot))},
		},
	}

	transactions = append(transactions, types.TransactWriteItem{Put: &putItem})

	return transactions, nil
}

// Commit the given TransactWriteItems to the DynamoDB table
func (repo *Repository) commitEventsWithTransaction(
	ctx context.Context,
	writeItems []types.TransactWriteItem,
	transaction evt.Transaction,
) error {
	transactItems, err := toDynamoItems(transaction)
	if err != nil {
		return err
	}

	// Combine the two, with included transaction items first
	combined := append(transactItems, writeItems...)

	count := len(combined)
	if count > 100 {
		// Limit increased from 25 to 100:
		// https://aws.amazon.com/about-aws/whats-new/2022/09/amazon-dynamodb-supports-100-actions-per-transaction/
		return fmt.Errorf("too many operations: %d, DynamoDb supports only up to 100 operations per transactions", count)
	}

	input := dynamodb.TransactWriteItemsInput{TransactItems: combined}
	_, err = repo.client.TransactWriteItems(ctx, &input)
	if err != nil {
		return repo.handleConditionalCheckFailure(ctx, transaction, transactItems, err)
	}

	return nil
}
