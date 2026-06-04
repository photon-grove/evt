package dynamo

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
)

// snapshotStreamWorkers is the number of entity partitions reconstructed concurrently by
// StreamEntitiesFromSnapshots. Each in-flight worker holds one entity's post-snapshot events.
const snapshotStreamWorkers = 4

// StreamEntitiesFromSnapshots reconstitutes entities with bounded memory by enumerating the
// distinct entity IDs in the event log and, for each, loading it from its latest durable snapshot
// (sk=0) plus only the events recorded after that snapshot — the same snapshot-aware path as
// snapshots.Store.LoadEntity, applied per partition during a rebuild. Entities without a snapshot
// fall back to full replay from sequence 1. This is the rebuild path that stays correct after
// CompactBelow has removed events below a snapshot.
//
// Like StreamEntitiesByQuery, per-entity ordering comes from partition Queries (sk-ascending), not
// from scan order, so it is correct under sequential or parallel segmented scans. Memory is bounded
// to the set of distinct entity IDs plus a small worker pool of in-flight aggregates.
func (repo *Repository) StreamEntitiesFromSnapshots(
	ctx context.Context,
	entityType evt.EntityType,
	seedEntity evt.SnapshotSeeder,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
) <-chan result.Result[evt.Entity] {
	logger := repo.loggerOrDefault()

	results := make(chan result.Result[evt.Entity])

	go func() {
		defer close(results)

		// Enumerate the full set of entity IDs first. As in StreamEntitiesByQuery, a failed (partial)
		// enumeration is fatal and emits no entities, so a rebuild never silently covers a subset.
		idList, err := repo.collectEntityIDs(ctx, entityType, nil)
		if err != nil {
			repo.sendEntity(ctx, results, result.Err[evt.Entity](err))
			return
		}

		ids := make(chan evt.EntityID)

		go func() {
			defer close(ids)

			for _, id := range idList {
				select {
				case ids <- id:
				case <-ctx.Done():
					return
				}
			}
		}()

		var wg sync.WaitGroup
		for i := 0; i < snapshotStreamWorkers; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for id := range ids {
					entity, ok := repo.loadEntityFromSnapshot(ctx, id, seedEntity, applyEvent, results, logger)
					if !ok {
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
	}()

	return results
}

// loadEntityFromSnapshot reconstructs a single entity from its durable snapshot (when present) plus
// the events recorded after it, mirroring snapshots.Store.LoadEntity for one partition. It returns
// ok=false (after forwarding any error to results) when seeding, querying, or applying fails, or
// when the partition produced no entity.
func (repo *Repository) loadEntityFromSnapshot(
	ctx context.Context,
	id evt.EntityID,
	seedEntity evt.SnapshotSeeder,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
	results chan<- result.Result[evt.Entity],
	logger *slog.Logger,
) (evt.Entity, bool) {
	snapshot, err := repo.GetSnapshot(ctx, id)
	if err != nil {
		repo.sendEntity(ctx, results, result.Err[evt.Entity](fmt.Errorf("reading snapshot for %s: %w", id, err)))
		return nil, false
	}

	// No snapshot: full replay from sequence 1 (identical to StreamEntitiesByQuery).
	if snapshot == nil {
		events, eerr := repo.GetEvents(ctx, id)
		if eerr != nil {
			repo.sendEntity(ctx, results, result.Err[evt.Entity](fmt.Errorf("reading events for %s: %w", id, eerr)))
			return nil, false
		}

		return repo.buildEntity(ctx, id, events, applyEvent, results, logger)
	}

	// Seed from the snapshot, then apply only the events recorded after it.
	entity, err := seedEntity(ctx, *snapshot)
	if err != nil {
		repo.sendEntity(ctx, results, result.Err[evt.Entity](fmt.Errorf("seeding %s from snapshot: %w", id, err)))
		return nil, false
	}

	events, err := repo.GetLatestEvents(ctx, id, snapshot.EventSequence)
	if err != nil {
		repo.sendEntity(ctx, results, result.Err[evt.Entity](fmt.Errorf("reading events for %s: %w", id, err)))
		return nil, false
	}

	// GetLatestEvents returns events in sort-key order; sort defensively to match buildEntity.
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Sequence < events[j].Sequence
	})

	for _, event := range events {
		applied, aerr := applyEvent(ctx, event, entity)
		if aerr != nil {
			logger.
				With("id", event.ID).
				With("sequence", event.Sequence).
				With("entity_type", event.EntityType).
				With("event_type", event.Type).
				Error("Error during applyEvent", "error", aerr.Error())

			repo.sendEntity(ctx, results, result.Err[evt.Entity](aerr))

			return nil, false
		}

		entity = applied
	}

	if entity == nil {
		return nil, false
	}

	logger.
		With("entity_id", id).
		With("entity_event_count", len(events)).
		With("seeded_from_snapshot", true).
		Debug("Entity Processed")

	return entity, true
}
