package publishers

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/photon-grove/evt/stream"
)

// StreamPublisher publishes DynamoDB stream records to a downstream stream.
type StreamPublisher interface {
	Publish(ctx context.Context, records []events.DynamoDBEventRecord) (*stream.PublishResult, error)
}

// HandleDynamoDBEvent publishes INSERT records from DynamoDB Streams to a
// downstream stream and returns partial batch failures for throttled or failed
// records.
func HandleDynamoDBEvent(
	ctx context.Context,
	event events.DynamoDBEvent,
	publisher StreamPublisher,
	budget BudgetController,
	loggers ...*slog.Logger,
) (events.DynamoDBEventResponse, error) {
	var logger *slog.Logger
	if len(loggers) > 0 {
		logger = loggers[0]
	}
	if logger == nil {
		logger = slog.Default()
	}
	if budget == nil {
		budget = NewBudgetController(0, 0, time.Now().UTC())
	}

	response := events.DynamoDBEventResponse{
		BatchItemFailures: []events.DynamoDBBatchItemFailure{},
	}

	records := make([]events.DynamoDBEventRecord, 0, len(event.Records))
	for _, record := range event.Records {
		if record.EventName == "INSERT" {
			records = append(records, record)
		}
	}

	if len(records) == 0 {
		logger.Debug("No INSERT records to publish")
		return response, nil
	}

	var publishedCount, skippedCount, malformedDroppedCount, throttledEventCount, throttledRetryCount int
	for i, record := range records {
		identifier := itemIdentifier(record)

		if len(record.Change.NewImage) == 0 {
			skippedCount++
			logger.Warn("Skipping INSERT record with empty NewImage", "index", i, "itemIdentifier", identifier)
			continue
		}

		if budget.AllowEvent(time.Now().UTC()) == DecisionDrop {
			throttledEventCount++
			response.BatchItemFailures = append(response.BatchItemFailures, events.DynamoDBBatchItemFailure{
				ItemIdentifier: identifier,
			})
			logger.Warn("Ingress budget dropped stream record",
				"index", i,
				"itemIdentifier", identifier,
			)
			continue
		}

		result, err := publisher.Publish(ctx, []events.DynamoDBEventRecord{record})
		if err != nil || (result != nil && len(result.FailedIndices) > 0) {
			if budget.AllowRetry(time.Now().UTC()) == DecisionDrop {
				throttledRetryCount++
				logger.Warn("Retry budget dropped failed publish record",
					"index", i,
					"itemIdentifier", identifier,
					"error", err,
				)
			}

			response.BatchItemFailures = append(response.BatchItemFailures, events.DynamoDBBatchItemFailure{
				ItemIdentifier: identifier,
			})
			continue
		}

		if result != nil && result.DroppedMalformedCount > 0 {
			malformedDroppedCount += result.DroppedMalformedCount
			logger.Warn("Dropping malformed stream record",
				"index", i,
				"itemIdentifier", identifier,
				"droppedMalformedCount", result.DroppedMalformedCount,
			)
			continue
		}

		publishedCount++
	}

	logger.Info("Publisher batch complete",
		"total", len(records),
		"published", publishedCount,
		"failures", len(response.BatchItemFailures),
		"skipped", skippedCount,
		"malformed_dropped", malformedDroppedCount,
		"throttled_event_budget", throttledEventCount,
		"throttled_retry_budget", throttledRetryCount,
	)

	return response, nil
}

func itemIdentifier(record events.DynamoDBEventRecord) string {
	if record.Change.SequenceNumber != "" {
		return record.Change.SequenceNumber
	}

	return record.EventID
}
