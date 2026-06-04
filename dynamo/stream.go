package dynamo

import (
	"context"
	"log/slog"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
)

// StreamAllEvents scans all Events in the table, returning a channel for results and errors.
//
// Events are emitted page by page as they are read; the channel is a true stream and does not buffer
// the whole table. When the repository is configured with WithScanSegments, the scan runs in
// parallel across segments and pages from different segments interleave on the channel in no
// particular order.
func (repo *Repository) StreamAllEvents(
	ctx context.Context,
	expr *expression.Expression,
) <-chan result.Result[[]evt.SerializedEvent] {
	input := dynamodb.ScanInput{
		TableName:      &repo.EventsTable,
		ConsistentRead: aws.Bool(repo.consistentRead),
	}

	if expr != nil {
		input.ExpressionAttributeNames = expr.Names()
		input.ExpressionAttributeValues = expr.Values()
		input.FilterExpression = expr.Filter()
		input.ProjectionExpression = expr.Projection()
	}

	return repo.scanTable(ctx, input)
}

// StreamEntities streams Events from the table, optionally filtered by a DynamoDB expression, and
// yields each completed Entity after all of its Events have been applied in sequence order.
//
// A DynamoDB Scan does not guarantee that the Events for a given entity arrive contiguously or in
// sort-key order — and with parallel segmented scans they interleave freely — so this cannot
// reconstitute entities in a single streaming pass. Instead it groups every matched Event by entity
// ID, then, once the scan completes, sorts each entity's Events by sequence and applies them in
// order before yielding the entity.
//
// As a result StreamEntities buffers all matched Events in memory for the duration of the scan. It
// is intended for rebuild/diagnostic flows (see evt.RebuildProjections), not hot read paths. For
// large tables, pair it with WithScanSegments to parallelize the read.
func (repo *Repository) StreamEntities(
	ctx context.Context,
	expr *expression.Expression,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
) <-chan result.Result[evt.Entity] {
	logger := repo.loggerOrDefault()

	results := make(chan result.Result[evt.Entity])

	go func() {
		defer close(results)

		// Group serialized events by entity ID. order preserves first-seen entity order so the
		// output is deterministic for a given scan.
		grouped := make(map[evt.EntityID][]evt.SerializedEvent)
		order := make([]evt.EntityID, 0)

		for eventResults := range repo.StreamAllEvents(ctx, expr) {
			serialized, err := eventResults.Unwrap()
			if err != nil {
				results <- result.Err[evt.Entity](err)
				continue
			}

			for _, event := range serialized {
				if event.Sequence == 0 {
					// Defensive: inline snapshots (sk=0) are already filtered by the scan.
					continue
				}

				if _, seen := grouped[event.EntityID]; !seen {
					order = append(order, event.EntityID)
				}

				grouped[event.EntityID] = append(grouped[event.EntityID], event)
			}
		}

		if ctx.Err() != nil {
			results <- result.Err[evt.Entity](ctx.Err())
			return
		}

		for _, id := range order {
			entity, ok := repo.buildEntity(ctx, id, grouped[id], applyEvent, results, logger)
			if !ok {
				continue
			}

			results <- result.Ok(entity)
		}
	}()

	return results
}

// buildEntity applies an entity's events in sequence order and returns the reconstituted entity.
// It returns ok=false (after forwarding the error) when an event fails to apply, or when no events
// produced an entity.
func (repo *Repository) buildEntity(
	ctx context.Context,
	id evt.EntityID,
	events []evt.SerializedEvent,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
	results chan<- result.Result[evt.Entity],
	logger *slog.Logger,
) (evt.Entity, bool) {
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Sequence < events[j].Sequence
	})

	var entity evt.Entity

	for _, event := range events {
		applied, err := applyEvent(ctx, event, entity)
		if err != nil {
			logger.
				With("id", event.ID).
				With("sequence", event.Sequence).
				With("entity_type", event.EntityType).
				With("event_type", event.Type).
				Error("Error during applyEvent", "error", err.Error())

			results <- result.Err[evt.Entity](err)

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
		Debug("Entity Processed")

	return entity, true
}
