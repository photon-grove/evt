package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/photon-grove/evt"
)

// checkHistoryDependent refuses compaction for an entity type declared history-dependent in the
// store's durable feature manifest. The refusal holds in every process that opens the log, whether
// or not it registered the projector or configured a feature marker. A log with no marker row
// declares nothing and compaction proceeds as it always has.
func (repo *Repository) checkHistoryDependent(ctx context.Context, tx pgx.Tx, entityType evt.EntityType) error {
	if err := repo.features.lockShared(ctx, tx); err != nil {
		return err
	}

	manifest, err := repo.features.readManifestTx(ctx, tx)
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
	if err := evt.CheckReservedEntityID(entityID); err != nil {
		return 0, err
	}

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

	deleted := 0
	err = repo.inTx(ctx, func(tx pgx.Tx) error {
		// Hold the shared feature lock across validation and every delete: a completed
		// history-dependent declaration must fence any later deletion.
		if checkErr := repo.checkHistoryDependent(ctx, tx, snapshot.EntityType); checkErr != nil {
			return checkErr
		}

		tag, deleteErr := tx.Exec(ctx, query, string(entityID), int64(throughSequence))
		if deleteErr != nil {
			return fmt.Errorf("postgres: compact below: %w", deleteErr)
		}

		deleted = int(tag.RowsAffected())

		return nil
	})
	if err != nil {
		return 0, err
	}

	return deleted, nil
}
