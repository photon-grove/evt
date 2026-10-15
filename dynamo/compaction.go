package dynamo

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
)

// compactionCheck reads the durable history-dependent set strongly and pins that exact marker
// version (or its absence). Every delete batch carries this check, fencing declarations that
// complete after validation but before deletion.
func (repo *Repository) compactionCheck(ctx context.Context, entityType evt.EntityType) (types.TransactWriteItem, error) {
	manifest, err := repo.features.Manifest(ctx)
	if err != nil {
		return types.TransactWriteItem{}, err
	}

	check := &types.ConditionCheck{
		TableName: &repo.EventsTable,
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: featuresPK},
			"sk": &types.AttributeValueMemberN{Value: "0"},
		},
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	}
	if manifest != nil {
		for _, declared := range manifest.HistoryDependent {
			if declared == entityType {
				return types.TransactWriteItem{}, evt.NewHistoryDependentProjectionError(entityType)
			}
		}

		check.ConditionExpression = aws.String("version = :observed")
		check.ExpressionAttributeValues = map[string]types.AttributeValue{
			":observed": &types.AttributeValueMemberN{Value: strconv.FormatInt(manifest.Version, 10)},
		}
	}

	return types.TransactWriteItem{ConditionCheck: check}, nil
}

// CompactBelow truncates an entity's event log by deleting events whose sequence is in the
// range [1, throughSequence], but only after verifying that a durable snapshot exists whose
// recorded EventSequence covers (>=) throughSequence. This guarantees every deleted event is
// already captured by the snapshot, so load and snapshot-aware rebuild remain correct.
//
// The sk=0 snapshot row is never deleted. CompactBelow returns the number of events deleted.
// It returns evt.ErrCompactionUncovered (wrapped) when no snapshot exists or the snapshot does
// not cover throughSequence. A throughSequence < 1 is a no-op.
//
// Concurrency: compaction only removes low, immutable, already-snapshotted events. Concurrent
// command handlers only append higher sequences and advance the sk=0 snapshot forward, so they
// never touch the deleted range. Each transactional delete batch also pins the feature manifest,
// so no deletion can escape a completed history-dependent declaration.
func (repo *Repository) CompactBelow(
	ctx context.Context,
	entityID evt.EntityID,
	throughSequence evt.EventSequence,
) (int, error) {
	if err := evt.CheckReservedEntityID(entityID); err != nil {
		return 0, err
	}

	if throughSequence < 1 {
		return 0, nil
	}

	// Compaction reads strongly regardless of WithConsistentRead: an eventually consistent read
	// could miss recent events, and compaction would report success while leaving them behind.
	snapshot, err := repo.getSnapshot(ctx, entityID, true)
	if err != nil {
		return 0, fmt.Errorf("compaction: reading snapshot for %s: %w", entityID, err)
	}
	if snapshot == nil {
		return 0, fmt.Errorf("%w: entity %s has no durable snapshot", evt.ErrCompactionUncovered, entityID)
	}

	check, err := repo.compactionCheck(ctx, snapshot.EntityType)
	if err != nil {
		return 0, err
	}

	if snapshot.EventSequence < throughSequence {
		return 0, fmt.Errorf(
			"%w: entity %s snapshot covers through event %d but compaction was requested through %d",
			evt.ErrCompactionUncovered, entityID, snapshot.EventSequence, throughSequence,
		)
	}

	keys, err := repo.eventKeysInRange(ctx, entityID, 1, throughSequence)
	if err != nil {
		return 0, fmt.Errorf("compaction: listing events for %s: %w", entityID, err)
	}
	if len(keys) == 0 {
		return 0, nil
	}

	deleted, err := repo.deleteCompactionKeys(ctx, keys, snapshot.EntityType, check)
	if err != nil {
		return deleted, fmt.Errorf("compaction: deleting events for %s: %w", entityID, err)
	}

	return deleted, nil
}

// eventKeysInRange returns the (pk, sk) keys of an entity's events whose sequence is in the
// inclusive range [lo, hi]. It projects only the key attributes to avoid reading payloads.
func (repo *Repository) eventKeysInRange(
	ctx context.Context,
	entityID evt.EntityID,
	lo evt.EventSequence,
	hi evt.EventSequence,
) ([]map[string]types.AttributeValue, error) {
	input := dynamodb.QueryInput{
		TableName:              &repo.EventsTable,
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("pk = :pk AND sk BETWEEN :lo AND :hi"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: string(entityID)},
			":lo": &types.AttributeValueMemberN{Value: strconv.Itoa(int(lo))},
			":hi": &types.AttributeValueMemberN{Value: strconv.Itoa(int(hi))},
		},
		ProjectionExpression: aws.String("pk, sk"),
	}

	p := dynamodb.NewQueryPaginator(repo.client, &input)

	keys := make([]map[string]types.AttributeValue, 0)

	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		if page == nil {
			break
		}

		for _, item := range page.Items {
			pk, ok := item["pk"]
			if !ok {
				continue
			}
			sk, ok := item["sk"]
			if !ok {
				continue
			}

			keys = append(keys, map[string]types.AttributeValue{"pk": pk, "sk": sk})
		}
	}

	return keys, nil
}

// deleteCompactionKeys reserves one of DynamoDB's 100 transaction items for the marker check.
// A changed marker forces a fresh durable refusal check before retrying the unchanged batch.
func (repo *Repository) deleteCompactionKeys(
	ctx context.Context,
	keys []map[string]types.AttributeValue,
	entityType evt.EntityType,
	check types.TransactWriteItem,
) (int, error) {
	const batchSize = 99

	deleted := 0
	for start := 0; start < len(keys); start += batchSize {
		end := min(start+batchSize, len(keys))
		items := make([]types.TransactWriteItem, 1, end-start+1)
		for _, key := range keys[start:end] {
			items = append(items, types.TransactWriteItem{Delete: &types.Delete{
				TableName: &repo.EventsTable,
				Key:       key,
			}})
		}

		for attempt := 0; ; attempt++ {
			items[0] = check
			_, err := repo.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
			if err == nil {
				deleted += end - start
				break
			}

			if failed, index := HasConditionalCheckFailure(err); !failed || index != 0 {
				return deleted, err
			}

			check, err = repo.compactionCheck(ctx, entityType)
			if err != nil {
				return deleted, err
			}
			if attempt >= maxDeclareAttempts {
				return deleted, transientFeatureChanged("compaction marker kept changing")
			}
		}
	}

	return deleted, nil
}
