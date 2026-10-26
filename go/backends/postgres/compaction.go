package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	evt "github.com/photon-grove/evt/go"
)

// checkCompactionManifest validates the store's durable feature manifest under the shared feature
// lock: the declared features must be supported, and the store must declare stream-heads. A store
// that has not been migrated is refused (*evt.StreamHeadsNotMigratedError): compaction never touches
// the head row, but it is a write path of a store whose streams have none yet.
func (repo *Repository) checkCompactionManifest(ctx context.Context, tx pgx.Tx) (*evt.FeatureManifest, error) {
	if err := repo.features.lockShared(ctx, tx); err != nil {
		return nil, err
	}

	manifest, err := repo.features.readManifestTx(ctx, tx)
	if err != nil {
		return nil, err
	}

	if manifest != nil {
		if err := manifest.Validate(repo.features.supported); err != nil {
			return nil, err
		}
	}

	if err := evt.RequireStreamHeads(manifest); err != nil {
		return nil, err
	}

	return manifest, nil
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
	if err := repo.checkFeatureMarkerTable(); err != nil {
		return 0, err
	}

	if err := evt.CheckReservedEntityID(entityID); err != nil {
		return 0, err
	}

	if throughSequence < 1 {
		return 0, nil
	}

	query := fmt.Sprintf(
		`DELETE FROM %s WHERE entity_id = $1 AND sequence >= 1 AND sequence <= $2`,
		repo.eventsTable,
	)

	deleted := 0
	err := repo.inTx(ctx, func(tx pgx.Tx) error {
		// Hold the shared feature lock across validation and every delete: a completed
		// history-dependent declaration must fence any later deletion. A store that has not been
		// migrated to stream heads is refused before anything else is read.
		manifest, err := repo.checkCompactionManifest(ctx, tx)
		if err != nil {
			return err
		}

		snapshot, err := repo.getSnapshot(ctx, tx, entityID)
		if err != nil {
			return err
		}

		if snapshot == nil {
			return fmt.Errorf("%w: entity %s has no durable snapshot", evt.ErrCompactionUncovered, entityID)
		}

		if snapshot.EventSequence < throughSequence {
			return fmt.Errorf(
				"%w: entity %s snapshot covers through event %d but compaction was requested through %d",
				evt.ErrCompactionUncovered, entityID, snapshot.EventSequence, throughSequence,
			)
		}

		// A history-dependent type's rebuild replays the commits compaction would delete.
		for _, declared := range manifest.HistoryDependent {
			if declared == snapshot.EntityType {
				return evt.NewHistoryDependentProjectionError(snapshot.EntityType)
			}
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
