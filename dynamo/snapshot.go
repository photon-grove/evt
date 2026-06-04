package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
)

// GetSnapshot gets the latest Snapshot for an Entity instance.
// Snapshots are stored inline in the event-log table at sk=0.
func (repo *Repository) GetSnapshot(
	ctx context.Context,
	entityID evt.EntityID,
) (*evt.SerializedSnapshot, error) {
	input := dynamodb.GetItemInput{
		TableName:      &repo.EventsTable,
		ConsistentRead: aws.Bool(repo.consistentRead),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: string(entityID)},
			"sk": &types.AttributeValueMemberN{Value: "0"},
		},
	}

	snapshot, err := repo.querySnapshot(ctx, input)
	if err != nil {
		return nil, err
	}

	return snapshot, nil
}

// PutSnapshot writes a snapshot directly using PutItem (no transaction).
// Snapshots are stored inline in the event-log table at sk=0.
// This is intended for use by projectors and background processes that need to
// write snapshots outside of the transactional command handler flow.
//
// The write is monotonic in eventSeq: it never overwrites an existing snapshot with one whose
// EventSequence is lower. The sk=0 snapshot is the durable floor for both snapshot-aware load and
// CompactBelow — if a stale/background writer could regress it below already-compacted events, a
// later load would seed from the older snapshot and query events that compaction has deleted,
// rebuilding with missing history. A regressing write is treated as a no-op (the existing, newer
// snapshot already covers it), so background catch-up writers do not see spurious errors.
func (repo *Repository) PutSnapshot(
	ctx context.Context,
	entityType evt.EntityType,
	entityID evt.EntityID,
	payload []byte,
	snapshotSequence evt.EventSequence,
	eventSequence evt.EventSequence,
) error {
	snapshot := Snapshot{
		PK:            entityID,
		SK:            0,
		Sequence:      snapshotSequence,
		EventSequence: eventSequence,
		EntityType:    entityType,
		Payload:       string(payload),
		// Mirror the events' retention so a background/catch-up snapshot of a policy'd type expires
		// with its stream instead of lingering as an orphan sk=0 row; omitempty drops it otherwise.
		TTL: repo.ttlFor(entityType),
	}

	item, err := repo.marshalMap(snapshot)
	if err != nil {
		return fmt.Errorf("error marshalling snapshot to map: %w", err)
	}

	input := &dynamodb.PutItemInput{
		TableName: &repo.EventsTable,
		Item:      item,
		// Monotonic floor: allow the write only when no snapshot exists yet or the stored snapshot's
		// EventSequence is not ahead of this one. This prevents the sk=0 floor from regressing below
		// compacted events.
		ConditionExpression: aws.String("attribute_not_exists(eventSeq) OR eventSeq <= :new"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":new": &types.AttributeValueMemberN{Value: strconv.Itoa(int(eventSequence))},
		},
	}

	_, err = repo.client.PutItem(ctx, input)
	if err != nil {
		// A newer-or-equal snapshot already exists; the write would regress the floor, so skip it.
		var condFailed *types.ConditionalCheckFailedException
		if errors.As(err, &condFailed) {
			return nil
		}

		return fmt.Errorf("failed to put snapshot: %w", err)
	}

	return nil
}

// Submit the given QueryInput to retrieve Snapshot records from the DynamoDB table
func (repo *Repository) querySnapshot(
	ctx context.Context,
	queryInput dynamodb.GetItemInput,
) (*evt.SerializedSnapshot, error) {
	result, err := repo.client.GetItem(ctx, &queryInput)
	if err != nil {
		return nil, err
	}
	if len(result.Item) == 0 {
		return nil, nil
	}

	snapshot := Snapshot{}

	err = repo.unmarshalMap(result.Item, &snapshot)
	if err != nil {
		return nil, err
	}

	return &evt.SerializedSnapshot{
		EntityType:    snapshot.EntityType,
		EntityID:      snapshot.PK,
		Sequence:      snapshot.Sequence,
		EventSequence: snapshot.EventSequence,
		Payload:       []byte(snapshot.Payload),
	}, nil
}
