package dynamo

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/result"
)

// snapshotStreamWorkers is the default number of entity partitions reconstructed concurrently by
// the snapshot-aware rebuild path. Each in-flight worker holds one entity's post-snapshot events.
const snapshotStreamWorkers = 4

// StreamFromSnapshotsOptions configures StreamEntitiesFromSnapshotsWithOptions.
type StreamFromSnapshotsOptions struct {
	// EntityType, if set, restricts the rebuild to entities of this type.
	EntityType evt.EntityType

	// Workers is the number of entity partitions reconstructed concurrently. Values < 1 default to
	// snapshotStreamWorkers. Each in-flight worker holds one entity's post-snapshot events.
	Workers int

	// Skip, if non-nil, is consulted for each enumerated entity ID before it is reconstructed.
	// Returning true skips that entity. Use it to resume an interrupted run or to rebuild a subset
	// (rebuilds are idempotent, so re-running from scratch is always safe; Skip just avoids redoing
	// finished work).
	Skip func(evt.EntityID) bool
}

// StreamEntitiesFromSnapshots reconstitutes entities with bounded memory by enumerating the
// distinct entity IDs in the event log and, for each, loading it from its latest durable snapshot
// (sk=0) plus only the events recorded after that snapshot — the same snapshot-aware path as
// snapshots.Store.LoadAggregate, applied per partition during a rebuild. Aggregates without a snapshot
// fall back to full replay from sequence 1. This is the rebuild path that stays correct after
// CompactBelow has removed events below a snapshot.
//
// It implements evt.SnapshotStreamer using the default scan-and-dedup enumeration (no schema
// change).
func (repo *Repository) StreamEntitiesFromSnapshots(
	ctx context.Context,
	entityType evt.EntityType,
	seedAggregate evt.SnapshotSeeder,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Aggregate) (evt.Aggregate, error),
) <-chan result.Result[evt.Aggregate] {
	return repo.StreamEntitiesFromSnapshotsWithOptions(
		ctx,
		StreamFromSnapshotsOptions{EntityType: entityType},
		seedAggregate,
		applyEvent,
	)
}

// StreamEntitiesFromSnapshotsWithOptions is the configurable form of StreamEntitiesFromSnapshots. It
// reconstitutes each entity from its latest durable snapshot plus only the post-snapshot events,
// falling back to full replay for entities with no snapshot.
//
// Per-entity ordering comes from partition Queries (sk-ascending), not from scan order. Entity IDs
// are collected from a key-only event-log scan first, where a failed (partial) enumeration is fatal
// and emits no entities so a rebuild never silently covers a subset. Memory is otherwise bounded to
// a small worker pool of in-flight aggregates.
func (repo *Repository) StreamEntitiesFromSnapshotsWithOptions(
	ctx context.Context,
	opts StreamFromSnapshotsOptions,
	seedAggregate evt.SnapshotSeeder,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Aggregate) (evt.Aggregate, error),
) <-chan result.Result[evt.Aggregate] {
	logger := repo.loggerOrDefault()

	results := make(chan result.Result[evt.Aggregate])

	workers := opts.Workers
	if workers < 1 {
		workers = snapshotStreamWorkers
	}

	go func() {
		defer close(results)

		ids := make(chan evt.EntityID)

		// Producer: enumerate entity IDs and feed them to the workers, then close ids. enumErr holds
		// any enumeration failure, read only after producerDone closes — a worker that exits early on
		// cancellation may never observe the closed ids channel, so wg.Wait() alone does not
		// synchronize with the producer's write to enumErr.
		producerDone := make(chan struct{})
		var enumErr error
		go func() {
			defer close(producerDone)
			defer close(ids)
			enumErr = repo.produceEntityIDs(ctx, opts.EntityType, opts.Skip, ids)
		}()

		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for id := range ids {
					aggregate, ok := repo.loadAggregateFromSnapshot(ctx, id, seedAggregate, applyEvent, results, logger)
					if !ok {
						if ctx.Err() != nil {
							return
						}

						continue
					}

					if !repo.sendAggregate(ctx, results, result.Ok(aggregate)) {
						return
					}
				}
			}()
		}

		wg.Wait()

		// Wait for the producer before reading enumErr, establishing the happens-before edge the
		// early-return workers above may not. On cancellation the producer unblocks via its own
		// ctx.Done select, so this never deadlocks.
		<-producerDone

		// Surface an enumeration failure as a stream error. Enumeration collects all IDs before any
		// worker runs, so this preserves its emit-nothing-then-error behavior.
		if enumErr != nil {
			repo.sendAggregate(ctx, results, result.Err[evt.Aggregate](enumErr))
		}
	}()

	return results
}

// sendAggregate delivers an aggregate result unless the context is cancelled first, returning false when
// the caller should stop producing. It keeps concurrent workers from blocking on an abandoned
// consumer.
func (repo *Repository) sendAggregate(
	ctx context.Context,
	channel chan<- result.Result[evt.Aggregate],
	value result.Result[evt.Aggregate],
) bool {
	select {
	case channel <- value:
		return true
	case <-ctx.Done():
		return false
	}
}

// produceEntityIDs enumerates the entity IDs to rebuild and feeds them to ids, applying the optional
// skip predicate. It collects the distinct IDs from a key-only event-log scan before emitting any —
// all-or-nothing semantics, where an enumeration failure emits no IDs at all, so a rebuild never
// silently covers a subset.
func (repo *Repository) produceEntityIDs(
	ctx context.Context,
	entityType evt.EntityType,
	skip func(evt.EntityID) bool,
	ids chan<- evt.EntityID,
) error {
	idList, err := repo.collectEntityIDs(ctx, entityType, skip)
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
// Head items (sk = -1, ADR 0007) are skipped: an orphan head, a partition with neither events nor a
// snapshot, would otherwise list a ghost entity ID.
func (repo *Repository) enumerateEntityIDs(
	ctx context.Context,
	entityType evt.EntityType,
	visit func(evt.EntityID) bool,
) error {
	input := dynamodb.ScanInput{
		TableName:            &repo.EventsTable,
		ConsistentRead:       aws.Bool(repo.consistentRead),
		ProjectionExpression: aws.String("pk, sk"),
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

			if sk, ok := numAttr(item, "sk"); ok && sk < 0 {
				// A head item says nothing about its entity's existence.
				continue
			}

			id := evt.EntityID(pkAttr.Value)
			if _, dup := seen[id]; dup {
				continue
			}
			if evt.IsReservedEntityID(id) {
				// Reserved namespaces hold engine-managed rows, never ordinary aggregates.
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

// loadAggregateFromSnapshot reconstructs a single entity from its durable snapshot (when present) plus
// the events recorded after it, mirroring snapshots.Store.LoadAggregate for one partition. It returns
// ok=false (after forwarding any error to results) when seeding, querying, or applying fails, or
// when the partition produced no entity.
func (repo *Repository) loadAggregateFromSnapshot(
	ctx context.Context,
	id evt.EntityID,
	seedAggregate evt.SnapshotSeeder,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Aggregate) (evt.Aggregate, error),
	results chan<- result.Result[evt.Aggregate],
	logger *slog.Logger,
) (evt.Aggregate, bool) {
	snapshot, err := repo.GetSnapshot(ctx, id)
	if err != nil {
		repo.sendAggregate(ctx, results, result.Err[evt.Aggregate](fmt.Errorf("reading snapshot for %s: %w", id, err)))
		return nil, false
	}

	// No snapshot: full replay from sequence 1.
	if snapshot == nil {
		events, eerr := repo.GetEvents(ctx, id)
		if eerr != nil {
			repo.sendAggregate(ctx, results, result.Err[evt.Aggregate](fmt.Errorf("reading events for %s: %w", id, eerr)))
			return nil, false
		}

		return repo.buildAggregate(ctx, id, events, applyEvent, results, logger)
	}

	// Seed from the snapshot, then apply only the events recorded after it.
	aggregate, err := seedAggregate(ctx, *snapshot)
	if err != nil {
		repo.sendAggregate(ctx, results, result.Err[evt.Aggregate](fmt.Errorf("seeding %s from snapshot: %w", id, err)))
		return nil, false
	}

	events, err := repo.GetLatestEvents(ctx, id, snapshot.EventSequence)
	if err != nil {
		repo.sendAggregate(ctx, results, result.Err[evt.Aggregate](fmt.Errorf("reading events for %s: %w", id, err)))
		return nil, false
	}

	// GetLatestEvents returns events in sort-key order; sort defensively to match buildAggregate.
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Sequence < events[j].Sequence
	})

	for _, event := range events {
		applied, aerr := applyEvent(ctx, event, aggregate)
		if aerr != nil {
			logger.
				With("id", event.ID).
				With("sequence", event.Sequence).
				With("entity_type", event.EntityType).
				With("event_type", event.Type).
				Error("Error during applyEvent", "error", aerr.Error())

			repo.sendAggregate(ctx, results, result.Err[evt.Aggregate](aerr))

			return nil, false
		}

		aggregate = applied
	}

	if aggregate == nil {
		return nil, false
	}

	logger.
		With("entity_id", id).
		With("entity_event_count", len(events)).
		With("seeded_from_snapshot", true).
		Debug("Entity Processed")

	return aggregate, true
}
