package postgres

import (
	"context"
	"fmt"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/result"
)

// streamEventsByEntity selects every event matching the filter, ordered so that a single ascending
// pass yields each entity's events contiguously and in sequence order. Streaming the rows (rather
// than materializing the whole log) keeps memory bounded by the largest single entity.
func (repo *Repository) streamEventsByEntity(
	ctx context.Context,
	filter evt.StreamFilter,
	onBatch func([]evt.SerializedEvent) error,
) error {
	query := fmt.Sprintf(`SELECT %s FROM %s WHERE `+reservedExclusion, eventColumns, repo.eventsTable)

	var args []any
	if filter.EntityType != "" {
		query += ` AND entity_type = $1`
		args = append(args, string(filter.EntityType))
	}

	query += ` ORDER BY entity_id ASC, sequence ASC`

	rows, err := repo.db.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("postgres: stream events: %w", err)
	}
	defer rows.Close()

	var (
		current  evt.EntityID
		batch    []evt.SerializedEvent
		haveRows bool
	)

	for rows.Next() {
		event, scanErr := scanEvent(rows)
		if scanErr != nil {
			return scanErr
		}

		if haveRows && event.EntityID != current {
			if err := onBatch(batch); err != nil {
				return err
			}

			batch = nil
		}

		current = event.EntityID
		haveRows = true
		batch = append(batch, event)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: stream events: %w", err)
	}

	if len(batch) > 0 {
		return onBatch(batch)
	}

	return nil
}

// StreamAllEvents streams the event log as one batch per entity, narrowed by the StreamFilter's
// entity-type predicate (pushed into the WHERE clause). A zero filter streams every event.
func (repo *Repository) StreamAllEvents(
	ctx context.Context,
	filter evt.StreamFilter,
) <-chan result.Result[[]evt.SerializedEvent] {
	channel := make(chan result.Result[[]evt.SerializedEvent])

	go func() {
		defer close(channel)

		err := repo.streamEventsByEntity(ctx, filter, func(batch []evt.SerializedEvent) error {
			channel <- result.Ok(batch)
			return ctx.Err()
		})
		if err != nil {
			channel <- result.Err[[]evt.SerializedEvent](err)
		}
	}()

	return channel
}

// StreamEntities streams each entity folded from its events via applyEvent, narrowed by the
// StreamFilter's entity-type predicate. A zero filter streams every entity. Each entity is
// reconstructed from its own contiguous run of events, so the result is correct regardless of how
// many entities the log holds.
func (repo *Repository) StreamEntities(
	ctx context.Context,
	filter evt.StreamFilter,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Aggregate) (evt.Aggregate, error),
) <-chan result.Result[evt.Aggregate] {
	channel := make(chan result.Result[evt.Aggregate])

	go func() {
		defer close(channel)

		err := repo.streamEventsByEntity(ctx, filter, func(batch []evt.SerializedEvent) error {
			aggregate, foldErr := foldAggregate(ctx, batch, nil, 0, applyEvent)
			if foldErr != nil {
				channel <- result.Err[evt.Aggregate](foldErr)
				return nil
			}

			if aggregate != nil {
				channel <- result.Ok(aggregate)
			}

			return ctx.Err()
		})
		if err != nil {
			channel <- result.Err[evt.Aggregate](err)
		}
	}()

	return channel
}

// foldAggregate applies the events whose sequence is strictly greater than through to the (possibly
// nil) entity, using applyEvent. It is shared by the full-replay and snapshot-seeded stream paths.
func foldAggregate(
	ctx context.Context,
	events []evt.SerializedEvent,
	aggregate evt.Aggregate,
	through evt.EventSequence,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Aggregate) (evt.Aggregate, error),
) (evt.Aggregate, error) {
	for _, event := range events {
		if event.Sequence <= through {
			continue
		}

		next, err := applyEvent(ctx, event, aggregate)
		if err != nil {
			return nil, err
		}

		aggregate = next
	}

	return aggregate, nil
}
