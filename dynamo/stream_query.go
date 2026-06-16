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

	// HeadSource, if set, enumerates entity IDs from a heads registry (one row per entity) instead
	// of the default key-only event-log scan. Because the registry is already unique, enumeration
	// streams IDs straight to the workers with no dedup set — constant memory, regardless of entity
	// count — and is naturally resumable. This is opt-in and requires the heads table to be
	// populated (maintained by the heads projector and seeded via HeadStore.Backfill); leave it nil
	// to keep the no-schema-change scan-and-dedup default. The events themselves are still read from
	// the event log per entity; the registry only supplies the IDs to rebuild.
	//
	// Unlike the default path, which collects every ID up front and treats an enumeration failure as
	// fatal before emitting anything, this path emits entities as it enumerates. A mid-enumeration
	// failure therefore surfaces as a stream error after some entities were already emitted; because
	// rebuilds are idempotent, re-run from scratch or resume with Skip.
	HeadSource evt.EntityHeadVisitor
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
// Note on cost: enumeration is still a table Scan. The ProjectionExpression on pk reduces the data
// returned, but DynamoDB charges scan read capacity by the size of the items read, not the
// attributes projected — so enumeration consumes read capacity comparable to scanning the full log
// and holds the distinct IDs in memory. The win is bounded memory, streaming output, and parallel
// per-entity queries, not lower read cost. For genuinely cheaper, constant-memory enumeration, set
// opts.HeadSource to enumerate IDs from a heads registry (one row per entity) instead of this scan.
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

		// Producer: enumerate entity IDs and feed them to the workers, then close ids. enumErr holds
		// any enumeration failure; it is read after wg.Wait below, which is safe because the producer
		// assigns it before close(ids), and a worker observing the closed channel happens-after that
		// close.
		var enumErr error
		go func() {
			defer close(ids)
			enumErr = repo.produceEntityIDs(ctx, opts, ids)
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
						if !repo.sendEntity(ctx, results, result.Err[evt.Entity](fmt.Errorf("querying entity %s: %w", id, err))) {
							return
						}

						continue
					}

					entity, ok := repo.buildEntity(ctx, id, events, applyEvent, results, logger)
					if !ok {
						// buildEntity reported an apply error (or produced no entity); keep going.
						if ctx.Err() != nil {
							return
						}

						continue
					}

					if !repo.sendEntity(ctx, results, result.Ok(entity)) {
						return
					}
				}
			}()
		}

		wg.Wait()

		// Surface an enumeration failure as a stream error. The default scan path fails before any
		// worker runs (collectEntityIDs returns nothing on error), so this preserves its
		// emit-nothing-then-error behavior; the HeadSource path may have emitted entities first.
		if enumErr != nil {
			repo.sendEntity(ctx, results, result.Err[evt.Entity](enumErr))
		}
	}()

	return results
}

// produceEntityIDs enumerates the entity IDs to rebuild and feeds them to ids, applying the optional
// Skip predicate. With opts.HeadSource set it streams IDs from the heads registry one at a time
// (constant memory); otherwise it collects the distinct IDs from a key-only event-log scan first —
// preserving the default all-or-nothing semantics, where an enumeration failure emits no IDs at all.
func (repo *Repository) produceEntityIDs(
	ctx context.Context,
	opts StreamByQueryOptions,
	ids chan<- evt.EntityID,
) error {
	send := func(id evt.EntityID) error {
		if opts.Skip != nil && opts.Skip(id) {
			return nil
		}

		select {
		case ids <- id:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if opts.HeadSource != nil {
		return opts.HeadSource.StreamEntityHeadsFunc(ctx, opts.EntityType, func(id evt.EntityID, _ evt.EventSequence) error {
			return send(id)
		})
	}

	// Default: collect the full distinct ID set first. If enumeration fails (a partial scan), return
	// the error without feeding any IDs — otherwise workers could query and commit a subset while
	// the caller sees an "ok-ish" result, leaving a silently under-rebuilt projection. This holds
	// the same set the scan already deduplicates, so it does not change the memory profile.
	idList, err := repo.collectEntityIDs(ctx, opts.EntityType, opts.Skip)
	if err != nil {
		return err
	}

	for _, id := range idList {
		select {
		case ids <- id:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// collectEntityIDs enumerates the distinct entity IDs in the event log, applying the optional Skip
// predicate, and returns them as a slice. It returns an error if the underlying scan fails, so the
// caller can treat enumeration as all-or-nothing.
func (repo *Repository) collectEntityIDs(
	ctx context.Context,
	entityType evt.EntityType,
	skip func(evt.EntityID) bool,
) ([]evt.EntityID, error) {
	var ids []evt.EntityID

	err := repo.enumerateEntityIDs(ctx, entityType, func(id evt.EntityID) bool {
		if skip != nil && skip(id) {
			return true
		}

		ids = append(ids, id)

		return true
	})
	if err != nil {
		return nil, err
	}

	return ids, nil
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
