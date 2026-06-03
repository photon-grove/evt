package dynamo

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
)

// StreamAllEvents scans all Events in the table, returning a channel for results and errors
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
// yields each completed Entity after all of its Events have been loaded.
func (repo *Repository) StreamEntities(
	ctx context.Context,
	expr *expression.Expression,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
) <-chan result.Result[evt.Entity] {
	logger := repo.loggerOrDefault()

	results := make(chan result.Result[evt.Entity])

	go func() {
		defer close(results)

		var entity evt.Entity
		entityEvents := 0

		for eventResults := range repo.StreamAllEvents(ctx, expr) {
			serialized, err := eventResults.Unwrap()
			if err != nil {
				results <- result.Err[evt.Entity](err)
				continue
			}

			// DynamoDB streams Events from a partition key in order based on the sort key, so we
			// should get everything from one id before we move on to the next id. Once the current
			// Entity ID changes, process the previous ID before moving on to the next.

			// First, we need to turn the serialized Events into full Domain Events.
			for _, event := range serialized {
				if event.Sequence == 0 {
					// This is a snapshot, so skip it
					continue
				}

				if entity != nil && event.EntityID != entity.GetID() {
					// We've moved on to a new Entity. Process this one and reset the Entity
					// pointer back to nil.
					aggLogger := slog.
						With("entity_id", entity.GetID()).
						With("entity_type", entity.Type()).
						With("entity_event_count", entityEvents)

					aggLogger.Debug("Entity Processed")

					// Yield this finished Entity to the channel
					results <- result.Ok(entity)

					entity = nil
					entityEvents = 0
				}

				evtLogger := logger.
					With("id", event.ID).
					With("sequence", event.Sequence).
					With("entity_type", event.EntityType).
					With("event_type", event.Type)

				entity, err = applyEvent(ctx, event, entity)
				if err != nil {
					evtLogger.Error("Error during applyEvent", "error", err.Error())

					results <- result.Err[evt.Entity](err)

					continue
				}

				entityEvents++
			}
		}

		// Add the final Entity if it exists
		if entity != nil {
			aggLogger := logger.
				With("entity_id", entity.GetID()).
				With("entity_type", entity.Type()).
				With("entity_event_count", entityEvents)

			aggLogger.Debug("Adding Entity to batch")

			// Yield the final Entity to the channel
			results <- result.Ok(entity)
		}
	}()

	return results
}
