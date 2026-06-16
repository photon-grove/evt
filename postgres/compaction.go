package postgres

import (
	"context"
	"fmt"

	"github.com/photon-grove/evt"
)

// CompactBelow deletes events for the entity whose sequence is in [1, throughSequence], but only
// after confirming a durable snapshot covers (>=) throughSequence. It implements evt.Compactor.
//
// The snapshot row lives in a separate table and is never touched, so the "never delete the sk=0
// snapshot" invariant holds structurally. A throughSequence < 1 is a no-op. When no covering
// snapshot exists nothing is deleted and ErrCompactionUncovered is returned.
func (repo *Repository) CompactBelow(
	ctx context.Context,
	entityID evt.EntityID,
	throughSequence evt.EventSequence,
) (int, error) {
	if throughSequence < 1 {
		return 0, nil
	}

	snapshot, err := repo.GetSnapshot(ctx, entityID)
	if err != nil {
		return 0, err
	}

	if snapshot == nil {
		return 0, fmt.Errorf("%w: entity %s has no durable snapshot", evt.ErrCompactionUncovered, entityID)
	}

	if snapshot.EventSequence < throughSequence {
		return 0, fmt.Errorf(
			"%w: entity %s snapshot covers through event %d but compaction was requested through %d",
			evt.ErrCompactionUncovered, entityID, snapshot.EventSequence, throughSequence,
		)
	}

	query := fmt.Sprintf(
		`DELETE FROM %s WHERE entity_id = $1 AND sequence >= 1 AND sequence <= $2`,
		repo.eventsTable,
	)

	tag, err := repo.db.Exec(ctx, query, string(entityID), int64(throughSequence))
	if err != nil {
		return 0, fmt.Errorf("postgres: compact below: %w", err)
	}

	return int(tag.RowsAffected()), nil
}
