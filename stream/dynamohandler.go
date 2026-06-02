package stream

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/photon-grove/evt/logging"
)

// DynamoDBHandler filters DynamoDB stream events for INSERT records only.
type DynamoDBHandler struct{}

// NewDynamoDBHandler creates a new DynamoDB handler.
func NewDynamoDBHandler() *DynamoDBHandler {
	return &DynamoDBHandler{}
}

// Handle filters DynamoDB stream events for INSERT records only.
// Non-INSERT events (MODIFY, REMOVE) are ignored.
func (h *DynamoDBHandler) Handle(ctx context.Context, event events.DynamoDBEvent) ([]events.DynamoDBEventRecord, error) {
	logger := logging.GetLogger(ctx)
	logger.Info("Handling DynamoDBEvent", "recordCount", len(event.Records))

	inserts := make([]events.DynamoDBEventRecord, 0, len(event.Records))
	for _, record := range event.Records {
		if record.EventName == "INSERT" {
			inserts = append(inserts, record)
		}
	}

	logger.Debug("Filtered INSERT records", "count", len(inserts), "total", len(event.Records))

	return inserts, nil
}
