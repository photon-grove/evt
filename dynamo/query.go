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
)

// GetEvents returns all events for an entity instance.
func (repo *Repository) GetEvents(
	ctx context.Context,
	entityID evt.EntityID,
) ([]evt.SerializedEvent, error) {
	input := dynamodb.QueryInput{
		TableName:              &repo.EventsTable,
		ConsistentRead:         aws.Bool(repo.consistentRead),
		KeyConditionExpression: aws.String("pk = :pk AND sk > :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: string(entityID)},
			":sk": &types.AttributeValueMemberN{Value: "0"}, // Exclude sk=0 snapshots
		},
	}

	// Query all events for the given entity type and id
	serializedEvents, err := repo.queryEvents(ctx, input)
	if err != nil {
		return nil, err
	}

	return serializedEvents, nil
}

// GetLatestEvents returns events after the given sequence for an entity instance.
func (repo *Repository) GetLatestEvents(
	ctx context.Context,
	entityID evt.EntityID,
	lastSequence evt.EventSequence,
) ([]evt.SerializedEvent, error) {
	input := dynamodb.QueryInput{
		TableName:              &repo.EventsTable,
		ConsistentRead:         aws.Bool(repo.consistentRead),
		KeyConditionExpression: aws.String("pk = :pk AND sk > :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: string(entityID)},
			":sk": &types.AttributeValueMemberN{Value: strconv.Itoa(int(lastSequence))},
		},
	}

	// Query all events past a certain sequence for the given entity type and id
	serializedEvents, err := repo.queryEvents(ctx, input)
	if err != nil {
		return nil, err
	}

	return serializedEvents, nil
}

// GetMaxSequence returns the highest event sequence for an entity without fetching all events.
// Uses a reverse-order query with limit 1 for efficiency. Returns 0 if no events exist.
func (repo *Repository) GetMaxSequence(
	ctx context.Context,
	entityID evt.EntityID,
) (evt.EventSequence, error) {
	input := dynamodb.QueryInput{
		TableName:              &repo.EventsTable,
		ConsistentRead:         aws.Bool(repo.consistentRead),
		KeyConditionExpression: aws.String("pk = :pk AND sk > :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: string(entityID)},
			":sk": &types.AttributeValueMemberN{Value: "0"}, // Exclude sk=0 snapshots
		},
		ProjectionExpression: aws.String("sk"), // Only fetch the sequence number
		ScanIndexForward:     aws.Bool(false),  // Descending order
		Limit:                aws.Int32(1),
	}

	result, err := repo.client.Query(ctx, &input)
	if err != nil {
		return 0, err
	}

	if len(result.Items) == 0 {
		return 0, nil
	}

	// Read sk directly from the attribute map to avoid unmarshalling the full event
	skAttr, ok := result.Items[0]["sk"]
	if !ok {
		return 0, nil
	}

	skNum, ok := skAttr.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("unexpected sk attribute type")
	}

	sk, err := strconv.Atoi(skNum.Value)
	if err != nil {
		return 0, fmt.Errorf("failed to parse sk value %q: %w", skNum.Value, err)
	}

	return evt.EventSequence(sk), nil
}

// Submit the given QueryInput to retrieve Event records from the DynamoDB table
func (repo *Repository) queryEvents(
	ctx context.Context,
	queryInput dynamodb.QueryInput,
) ([]evt.SerializedEvent, error) {
	p := dynamodb.NewQueryPaginator(repo.client, &queryInput)

	serializedEvents := make([]evt.SerializedEvent, 0)

	for p.HasMorePages() {
		result, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return serializedEvents, nil
		}

		for _, item := range result.Items {
			event := Event{}
			if err = repo.unmarshalMap(item, &event); err != nil {
				return nil, err
			}

			// Defensive guard: the primary sk>0 filter is in the KeyConditionExpression,
			// but skip here too in case a query omits the sk condition.
			if event.SK == 0 {
				continue
			}

			var metadata evt.Metadata
			if err = json.Unmarshal([]byte(event.Metadata), &metadata); err != nil {
				return nil, err
			}

			serializedEvents = append(serializedEvents, evt.SerializedEvent{
				ID:         evt.GetEventID(event.PK, event.SK),
				EntityType: event.EntityType,
				EntityID:   event.PK,
				Sequence:   event.SK,
				Type:       event.Type,
				Version:    event.Version,
				Payload:    []byte(event.Payload),
				Metadata:   metadata,
			})
		}
	}

	return serializedEvents, nil
}
