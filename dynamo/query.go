package dynamo

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/internal/eventjson"
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

// GetLatestEvents returns events after the given sequence for an entity instance. A negative
// sequence reads as 0: the query's lower bound never reaches the head item at sk = -1 (ADR 0007).
func (repo *Repository) GetLatestEvents(
	ctx context.Context,
	entityID evt.EntityID,
	lastSequence evt.EventSequence,
) ([]evt.SerializedEvent, error) {
	lastSequence = max(lastSequence, 0)

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

const recentEventsPageSize = 100

// GetRecentEvents returns at most limit events for an entity, newest-first (descending sequence).
// The query reads the highest event sequences with ScanIndexForward=false and bounded pages; the
// `sk > 0` key condition excludes the sk=0 snapshot item. When a response returns fewer than limit
// items and carries a pagination key (large items can close a page early), the reader keeps paging
// until it has collected limit items or the stream runs out. A limit below 1 is a programmer error
// returned before any request.
func (repo *Repository) GetRecentEvents(
	ctx context.Context,
	entityID evt.EntityID,
	limit int,
) ([]evt.SerializedEvent, error) {
	if limit < 1 {
		return nil, &evt.InvalidArgumentError{Argument: "limit", Message: fmt.Sprintf("must be at least 1, got %d", limit)}
	}

	input := dynamodb.QueryInput{
		TableName:              &repo.EventsTable,
		ConsistentRead:         aws.Bool(repo.consistentRead),
		KeyConditionExpression: aws.String("pk = :pk AND sk > :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: string(entityID)},
			":sk": &types.AttributeValueMemberN{Value: "0"}, // Exclude sk=0 snapshots
		},
		ScanIndexForward: aws.Bool(false),                                    // Descending order
		Limit:            aws.Int32(int32(min(limit, recentEventsPageSize))), //nolint:gosec // bounded by the page size
	}

	recent := make([]evt.SerializedEvent, 0, min(limit, recentEventsPageSize))

	p := dynamodb.NewQueryPaginator(repo.client, &input)

	for p.HasMorePages() && len(recent) < limit {
		result, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		if result == nil {
			break
		}

		page, err := repo.decodeQueryPage(result.Items)
		if err != nil {
			return nil, err
		}

		recent = append(recent, page...)
	}

	// The first request's Limit bounds each page; trim a page that still overshot the window.
	if len(recent) > limit {
		recent = recent[:limit]
	}

	return recent, nil
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

		page, err := repo.decodeQueryPage(result.Items)
		if err != nil {
			return nil, err
		}

		serializedEvents = append(serializedEvents, page...)
	}

	return serializedEvents, nil
}

// decodeQueryPage turns a page of raw Query items into SerializedEvents, skipping the sk=0
// snapshot. Explicit point reads retain legacy reserved-prefix streams for migration.
func (repo *Repository) decodeQueryPage(items []map[string]types.AttributeValue) ([]evt.SerializedEvent, error) {
	serializedEvents := make([]evt.SerializedEvent, 0, len(items))

	for _, item := range items {
		event := Event{}
		if err := repo.unmarshalMap(item, &event); err != nil {
			return nil, err
		}

		if event.SK < 1 {
			continue
		}

		// A retained stream's head item carries a ttl at least that of every other item, so the
		// next commit needs the largest one this reader saw.
		repo.ttlFloors.observe(event.PK, event.TTL)

		var metadata evt.Metadata
		if err := eventjson.UnmarshalMetadata([]byte(event.Metadata), &metadata); err != nil {
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

	return serializedEvents, nil
}
