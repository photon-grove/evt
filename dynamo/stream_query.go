package dynamo

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
)

// StreamByQueryOptions configures StreamEntitiesByQuery.
type StreamByQueryOptions struct {
	// EntityType, if set, restricts the rebuild to entities of this type. Enumeration filters the
	// key-only scan by entityType so non-matching partitions are never queried.
	EntityType evt.EntityType

	// Workers is the number of entity partitions queried concurrently. Values < 1 are treated as 1
	// (sequential). Each in-flight worker holds one entity's events in memory.
	Workers int

	// Skip, if non-nil, is consulted for each enumerated entity ID before its partition is queried.
	// Returning true skips that entity. Use it to resume an interrupted run or to rebuild a subset
	// (rebuilds are idempotent, so re-running from scratch is always safe; Skip just avoids redoing
	// finished work).
	Skip func(evt.EntityID) bool
}

// StreamEntitiesByQuery reconstitutes entities with bounded memory by first enumerating the distinct
// entity IDs in the event log, then querying each entity's partition (which returns its events in
// sort-key order) and folding them with applyEvent.
//
// Unlike StreamEntities — which scans the whole table and buffers every matched event so it can
// regroup the unordered scan output — this path queries one partition at a time and emits each
// entity as soon as it is rebuilt. Memory is bounded to the set of distinct entity IDs collected
// during enumeration plus up to Workers in-flight aggregates, rather than the entire event log. It
// is the preferred source for RebuildProjectionsFromStream on large tables.
//
// Note: enumeration is a key-only Scan, so it still reads every row once (cheaply, keys only) and
// holds the distinct IDs in memory. For constant-memory enumeration, back it with a dedicated
// per-entity index or registry instead.
func (repo *Repository) StreamEntitiesByQuery(
	ctx context.Context,
	opts StreamByQueryOptions,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
) <-chan result.Result[evt.Entity] {
	logger := repo.loggerOrDefault()

	results := make(chan result.Result[evt.Entity])

	workers := opts.Workers
	if workers < 1 {
		workers = 1
	}

	go func() {
		defer close(results)

		ids := make(chan evt.EntityID)

		// Enumerator: feed unique, non-skipped entity IDs to the workers, then close ids.
		var enumErr error
		go func() {
			defer close(ids)

			enumErr = repo.enumerateEntityIDs(ctx, opts.EntityType, func(id evt.EntityID) bool {
				if opts.Skip != nil && opts.Skip(id) {
					return true
				}

				select {
				case ids <- id:
					return true
				case <-ctx.Done():
					return false
				}
			})
		}()

		// Worker pool: each worker queries an entity partition and reconstitutes the entity.
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for id := range ids {
					events, err := repo.GetEvents(ctx, id)
					if err != nil {
						repo.sendEntity(ctx, results, result.Err[evt.Entity](fmt.Errorf("querying entity %s: %w", id, err)))
						continue
					}

					entity, ok := repo.buildEntity(ctx, id, events, applyEvent, results, logger)
					if !ok {
						continue
					}

					repo.sendEntity(ctx, results, result.Ok(entity))
				}
			}()
		}

		wg.Wait()

		// Surface any enumeration error once workers have drained. The write to enumErr is
		// sequenced before close(ids), which the workers' range observes before wg.Done, so this
		// read happens-after the write.
		if enumErr != nil {
			repo.sendEntity(ctx, results, result.Err[evt.Entity](enumErr))
		}
	}()

	return results
}

// enumerateEntityIDs paginates a key-only Scan of the event log and invokes visit once per distinct
// entity ID (partition key). visit returns false to stop enumeration early (e.g. on context
// cancellation). Snapshot rows share their entity's partition key, so de-duplication folds them in.
func (repo *Repository) enumerateEntityIDs(
	ctx context.Context,
	entityType evt.EntityType,
	visit func(evt.EntityID) bool,
) error {
	input := dynamodb.ScanInput{
		TableName:            &repo.EventsTable,
		ConsistentRead:       aws.Bool(repo.consistentRead),
		ProjectionExpression: aws.String("pk"),
	}

	if entityType != "" {
		input.FilterExpression = aws.String("entityType = :et")
		input.ExpressionAttributeValues = map[string]types.AttributeValue{
			":et": &types.AttributeValueMemberS{Value: string(entityType)},
		}
	}

	seen := make(map[evt.EntityID]struct{})
	p := dynamodb.NewScanPaginator(repo.client, &input)

	for p.HasMorePages() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		page, err := p.NextPage(ctx)
		if err != nil {
			return err
		}
		if page == nil {
			continue
		}

		for _, item := range page.Items {
			pkAttr, ok := item["pk"].(*types.AttributeValueMemberS)
			if !ok {
				continue
			}

			id := evt.EntityID(pkAttr.Value)
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}

			if !visit(id) {
				return ctx.Err()
			}
		}
	}

	return nil
}

// sendEntity delivers an entity result unless the context is cancelled first, returning false when
// the caller should stop producing. It keeps concurrent workers from blocking on an abandoned
// consumer.
func (repo *Repository) sendEntity(
	ctx context.Context,
	channel chan<- result.Result[evt.Entity],
	value result.Result[evt.Entity],
) bool {
	select {
	case channel <- value:
		return true
	case <-ctx.Done():
		return false
	}
}
