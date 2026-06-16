package postgres

import (
	"context"
	"fmt"

	"github.com/photon-grove/evt"
)

// headsQuery folds the event log and the snapshot table down to one head per entity: the larger of
// the highest stored event sequence and the snapshot's recorded event_sequence. The UNION ALL lets
// a single GROUP BY/MAX cover both sources, so an entity whose early events were compacted away (and
// now exists only as a snapshot, or as a snapshot plus tail events) still reports the sequence its
// snapshot covers. entity_type travels with each row so the filter applies uniformly.
func (repo *Repository) headsQuery() string {
	return fmt.Sprintf(`
SELECT entity_id, MAX(head) AS head FROM (
    SELECT entity_id, sequence AS head, entity_type FROM %s
    UNION ALL
    SELECT entity_id, event_sequence AS head, entity_type FROM %s
) sources
WHERE $1 = '' OR entity_type = $1
GROUP BY entity_id`, repo.eventsTable, repo.snapshotsTable)
}

// StreamEntityHeads enumerates the distinct entities in the log and returns each entity's current
// head sequence. It implements evt.EntityHeadStreamer by materializing the streamed visitor into a
// map.
func (repo *Repository) StreamEntityHeads(
	ctx context.Context,
	entityType evt.EntityType,
) (map[evt.EntityID]evt.EventSequence, error) {
	heads := make(map[evt.EntityID]evt.EventSequence)

	err := repo.StreamEntityHeadsFunc(ctx, entityType, func(id evt.EntityID, head evt.EventSequence) error {
		heads[id] = head
		return nil
	})
	if err != nil {
		return nil, err
	}

	return heads, nil
}

// StreamEntityHeadsFunc enumerates entity heads and invokes visit once per entity, paging the result
// cursor without accumulating the full set — the constant-memory change-detection primitive a SQL
// backend can offer (a streamed cursor over SELECT entity_id, MAX(sequence) …). It implements
// evt.EntityHeadVisitor. If visit returns an error, enumeration stops and returns it.
func (repo *Repository) StreamEntityHeadsFunc(
	ctx context.Context,
	entityType evt.EntityType,
	visit func(evt.EntityID, evt.EventSequence) error,
) error {
	rows, err := repo.db.Query(ctx, repo.headsQuery(), string(entityType))
	if err != nil {
		return fmt.Errorf("postgres: stream entity heads: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id   evt.EntityID
			head int64
		)

		if err := rows.Scan(&id, &head); err != nil {
			return fmt.Errorf("postgres: scan entity head: %w", err)
		}

		if err := visit(id, evt.EventSequence(head)); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: stream entity heads: %w", err)
	}

	return nil
}
