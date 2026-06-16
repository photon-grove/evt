package postgres

import (
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/photon-grove/evt"
)

// eventColumns is the SELECT list for reading a SerializedEvent back out of the event-log table.
// The order must match scanEvent.
const eventColumns = `event_id, sequence, event_type, version, entity_id, entity_type, payload, metadata`

// scanEvents reads every row from a query over eventColumns into SerializedEvents. It always closes
// the rows.
func scanEvents(rows pgx.Rows) ([]evt.SerializedEvent, error) {
	defer rows.Close()

	events := make([]evt.SerializedEvent, 0)

	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: scan events: %w", err)
	}

	return events, nil
}

// scanEvent decodes a single row selected with eventColumns.
func scanEvent(rows pgx.Rows) (evt.SerializedEvent, error) {
	var (
		event    evt.SerializedEvent
		sequence int64
		version  int64
		metadata []byte
	)

	err := rows.Scan(
		&event.ID,
		&sequence,
		&event.Type,
		&version,
		&event.EntityID,
		&event.EntityType,
		&event.Payload,
		&metadata,
	)
	if err != nil {
		return evt.SerializedEvent{}, fmt.Errorf("postgres: scan event: %w", err)
	}

	event.Sequence = evt.EventSequence(sequence)
	event.Version = evt.EventVersion(version)

	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &event.Metadata); err != nil {
			return evt.SerializedEvent{}, fmt.Errorf("postgres: unmarshal metadata for %s: %w", event.ID, err)
		}
	}

	return event, nil
}
