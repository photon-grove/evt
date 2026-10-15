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

// maxBatchDeleteUnprocessedRetries bounds the retry loop for UnprocessedItems returned by
// BatchWriteItem so a persistently throttled table cannot spin forever.
const maxBatchDeleteUnprocessedRetries = 8

// checkHistoryDependent refuses compaction for an entity type declared history-dependent in the
// store's durable feature manifest (storage spec, Feature Marker). The refusal reads the manifest
// itself — strongly consistent, regardless of WithConsistentRead, so it cannot miss a just-written
// set — and holds in every process that opens the log, whether or not it registered the projector
// or configured a feature marker. A table with no marker row declares nothing and compaction
// proceeds as it always has.
func (repo *Repository) checkHistoryDependent(ctx context.Context, entityType evt.EntityType) error {
	if entityType == "" {
		return nil
	}

	manifest, err := NewFeatureMarker(repo.client, repo.EventsTable, nil).Manifest(ctx)
	if err != nil {
		return err
	}
	if manifest == nil {
		return nil
	}

	for _, declared := range manifest.HistoryDependent {
		if declared == entityType {
			return evt.NewHistoryDependentProjectionError(entityType)
		}
	}

	return nil
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
// never touch the deleted range; the coverage check is therefore safe without a transaction.
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

	err = repo.checkHistoryDependent(ctx, snapshot.EntityType)
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

	deleted, err := repo.batchDeleteKeys(ctx, keys)
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

// batchDeleteKeys deletes the given keys from the events table using BatchWriteItem in chunks
// of 25 (the DynamoDB per-batch limit), retrying any UnprocessedItems a bounded number of
// times. It returns the number of keys successfully deleted.
func (repo *Repository) batchDeleteKeys(
	ctx context.Context,
	keys []map[string]types.AttributeValue,
) (int, error) {
	const batchSize = 25

	deleted := 0

	for start := 0; start < len(keys); start += batchSize {
		end := start + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		requests := make([]types.WriteRequest, 0, end-start)
		for _, key := range keys[start:end] {
			requests = append(requests, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{Key: key},
			})
		}

		written, err := repo.flushDeleteBatch(ctx, requests)
		deleted += written
		if err != nil {
			return deleted, err
		}
	}

	return deleted, nil
}

// flushDeleteBatch sends a single BatchWriteItem of delete requests and drains any
// UnprocessedItems with bounded retries. It returns the count of deletes that completed.
func (repo *Repository) flushDeleteBatch(
	ctx context.Context,
	requests []types.WriteRequest,
) (int, error) {
	total := len(requests)
	pending := requests

	for attempt := 0; ; attempt++ {
		out, err := repo.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{repo.EventsTable: pending},
		})
		if err != nil {
			return total - len(pending), err
		}

		pending = out.UnprocessedItems[repo.EventsTable]
		if len(pending) == 0 {
			return total, nil
		}

		if attempt >= maxBatchDeleteUnprocessedRetries {
			return total - len(pending), fmt.Errorf(
				"compaction: %d items still unprocessed after %d retries",
				len(pending), maxBatchDeleteUnprocessedRetries,
			)
		}
	}
}
