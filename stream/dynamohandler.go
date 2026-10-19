package stream

import (
	"context"
	"log/slog"

	"github.com/aws/aws-lambda-go/events"

	"github.com/photon-grove/evt/internal/streamheads"
)

// DynamoDBHandler filters DynamoDB stream events for INSERT records only.
type DynamoDBHandler struct {
	logger *slog.Logger
}

// NewDynamoDBHandler creates a new DynamoDB handler.
func NewDynamoDBHandler(loggers ...*slog.Logger) *DynamoDBHandler {
	var logger *slog.Logger
	if len(loggers) > 0 {
		logger = loggers[0]
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &DynamoDBHandler{logger: logger}
}

// Handle filters DynamoDB stream events for INSERT records only.
// Non-INSERT events (MODIFY, REMOVE) are ignored, and so are records of rows that are not events:
// the first commit of a stream inserts its head item (sk = -1, ADR 0007), which is concurrency
// metadata, not an event.
func (h *DynamoDBHandler) Handle(_ context.Context, event events.DynamoDBEvent) ([]events.DynamoDBEventRecord, error) {
	logger := h.logger
	if logger == nil {
		logger = slog.Default()
	}

	logger.Info("Handling DynamoDBEvent", "recordCount", len(event.Records))

	inserts := make([]events.DynamoDBEventRecord, 0, len(event.Records))
	for _, record := range event.Records {
		if record.EventName == "INSERT" && !streamheads.IsNonEventRecord(record) {
			inserts = append(inserts, record)
		}
	}

	logger.Debug("Filtered INSERT records", "count", len(inserts), "total", len(event.Records))

	return inserts, nil
}
