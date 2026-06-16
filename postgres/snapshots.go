package postgres

import (
	"context"
	"fmt"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
)

// StreamEntitiesFromSnapshots streams completed entities, seeding each from its durable snapshot and
// applying only the events recorded after that snapshot. Streams with no snapshot fall back to full
// replay from sequence 1. entityType, when non-empty, restricts the rebuild to that type. It
// implements evt.SnapshotStreamer.
//
// Each entity is reconstructed from its own partition (per-entity queries), so the result stays
// correct after compaction has removed events below a snapshot — the snapshot, not event 1, is the
// authoritative floor.
func (repo *Repository) StreamEntitiesFromSnapshots(
	ctx context.Context,
	entityType evt.EntityType,
	seedEntity evt.SnapshotSeeder,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
) <-chan result.Result[evt.Entity] {
	channel := make(chan result.Result[evt.Entity])

	go func() {
		defer close(channel)

		ids, err := repo.distinctEntityIDs(ctx, entityType)
		if err != nil {
			channel <- result.Err[evt.Entity](err)
			return
		}

		for _, id := range ids {
			if ctx.Err() != nil {
				return
			}

			entity, err := repo.rebuildFromSnapshot(ctx, id, seedEntity, applyEvent)
			if err != nil {
				channel <- result.Err[evt.Entity](err)
				continue
			}

			if entity != nil {
				channel <- result.Ok(entity)
			}
		}
	}()

	return channel
}

// rebuildFromSnapshot reconstructs a single entity: seed from its snapshot when present and apply
// only post-snapshot events, otherwise replay every event from sequence 1.
func (repo *Repository) rebuildFromSnapshot(
	ctx context.Context,
	id evt.EntityID,
	seedEntity evt.SnapshotSeeder,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
) (evt.Entity, error) {
	snapshot, err := repo.GetSnapshot(ctx, id)
	if err != nil {
		return nil, err
	}

	var (
		entity  evt.Entity
		through evt.EventSequence
	)

	if snapshot != nil {
		entity, err = seedEntity(ctx, *snapshot)
		if err != nil {
			return nil, err
		}

		through = snapshot.EventSequence
	}

	events, err := repo.GetLatestEvents(ctx, id, through)
	if err != nil {
		return nil, err
	}

	return foldEntity(ctx, events, entity, through, applyEvent)
}

// distinctEntityIDs returns the entity IDs present in the event log or as a snapshot-only entity
// (events compacted away), narrowed by entityType. Only the IDs are materialized; each entity is
// rebuilt from its own partition afterward.
func (repo *Repository) distinctEntityIDs(ctx context.Context, entityType evt.EntityType) ([]evt.EntityID, error) {
	query := fmt.Sprintf(`
SELECT DISTINCT entity_id FROM (
    SELECT entity_id, entity_type FROM %s
    UNION
    SELECT entity_id, entity_type FROM %s
) sources
WHERE $1 = '' OR entity_type = $1
ORDER BY entity_id ASC`, repo.eventsTable, repo.snapshotsTable)

	rows, err := repo.db.Query(ctx, query, string(entityType))
	if err != nil {
		return nil, fmt.Errorf("postgres: list entities: %w", err)
	}
	defer rows.Close()

	var ids []evt.EntityID
	for rows.Next() {
		var id evt.EntityID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("postgres: scan entity id: %w", err)
		}

		ids = append(ids, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list entities: %w", err)
	}

	return ids, nil
}
