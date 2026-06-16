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

// snapshotStreamWorkers is the default number of entity partitions reconstructed concurrently by
// the snapshot-aware rebuild path. Each in-flight worker holds one entity's post-snapshot events.
const snapshotStreamWorkers = 4

// StreamFromSnapshotsOptions configures StreamEntitiesFromSnapshotsWithOptions. It mirrors
// StreamByQueryOptions for the snapshot-aware rebuild path.
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

	// HeadSource, if set, enumerates entity IDs from a heads registry (one row per entity) instead
	// of the default key-only event-log scan. Because the registry is already unique, enumeration
	// streams IDs straight to the workers with no dedup set — constant memory, regardless of entity
	// count — and is naturally resumable. The heads registry already accounts for a compacted
	// stream's snapshot floor (head = MAX(highest event sk, snapshot EventSequence)), so it is a
	// correct ID source for snapshot-seeded rebuilds. This is opt-in and requires the heads table to
	// be populated (maintained by the heads projector and seeded via HeadStore.Backfill); leave it
	// nil to keep the no-schema-change scan-and-dedup default. The events themselves are still read
	// from the event log per entity; the registry only supplies the IDs to rebuild.
	//
	// Unlike the default path, which collects every ID up front and treats an enumeration failure as
	// fatal before emitting anything, this path emits entities as it enumerates. A mid-enumeration
	// failure therefore surfaces as a stream error after some entities were already emitted; because
	// rebuilds are idempotent, re-run from scratch or resume with Skip.
	HeadSource evt.EntityHeadVisitor
}

// StreamEntitiesFromSnapshots reconstitutes entities with bounded memory by enumerating the
// distinct entity IDs in the event log and, for each, loading it from its latest durable snapshot
// (sk=0) plus only the events recorded after that snapshot — the same snapshot-aware path as
// snapshots.Store.LoadEntity, applied per partition during a rebuild. Entities without a snapshot
// fall back to full replay from sequence 1. This is the rebuild path that stays correct after
// CompactBelow has removed events below a snapshot.
//
// It implements evt.SnapshotStreamer using the default scan-and-dedup enumeration (no schema
// change). For constant-memory enumeration from a heads registry, call
// StreamEntitiesFromSnapshotsWithOptions with StreamFromSnapshotsOptions.HeadSource set.
func (repo *Repository) StreamEntitiesFromSnapshots(
	ctx context.Context,
	entityType evt.EntityType,
	seedEntity evt.SnapshotSeeder,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
) <-chan result.Result[evt.Entity] {
	return repo.StreamEntitiesFromSnapshotsWithOptions(
		ctx,
		StreamFromSnapshotsOptions{EntityType: entityType},
		seedEntity,
		applyEvent,
	)
}

// StreamEntitiesFromSnapshotsWithOptions is the configurable form of StreamEntitiesFromSnapshots. It
// reconstitutes each entity from its latest durable snapshot plus only the post-snapshot events,
// falling back to full replay for entities with no snapshot.
//
// Like StreamEntitiesByQuery, per-entity ordering comes from partition Queries (sk-ascending), not
// from scan order, and the two paths share enumeration (produceEntityIDs): with opts.HeadSource set,
// entity IDs stream from the heads registry with no dedup set (constant memory regardless of entity
// count); otherwise they are collected from a key-only event-log scan first, where a failed (partial)
// enumeration is fatal and emits no entities so a rebuild never silently covers a subset. Memory is
// otherwise bounded to a small worker pool of in-flight aggregates.
func (repo *Repository) StreamEntitiesFromSnapshotsWithOptions(
	ctx context.Context,
	opts StreamFromSnapshotsOptions,
	seedEntity evt.SnapshotSeeder,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
) <-chan result.Result[evt.Entity] {
	logger := repo.loggerOrDefault()

	results := make(chan result.Result[evt.Entity])

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
			enumErr = repo.produceEntityIDs(ctx, opts.EntityType, opts.Skip, opts.HeadSource, ids)
		}()

		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
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

		// Wait for the producer before reading enumErr, establishing the happens-before edge the
		// early-return workers above may not. On cancellation the producer unblocks via its own
		// ctx.Done select, so this never deadlocks.
		<-producerDone

		// Surface an enumeration failure as a stream error. The default scan path collects all IDs
		// before any worker runs, so this preserves its emit-nothing-then-error behavior; the
		// HeadSource path may have emitted entities first.
		if enumErr != nil {
			repo.sendEntity(ctx, results, result.Err[evt.Entity](enumErr))
		}
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
