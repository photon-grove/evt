package projectors

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
	"github.com/photon-grove/evt/logging"
)

// DynamoDBStreamResponse is the Lambda response for DynamoDB Streams event
// sources with partial batch failure reporting enabled.
type DynamoDBStreamResponse struct {
	BatchItemFailures []BatchItemFailure `json:"batchItemFailures"`
}

// NewLambdaHandler returns a function suitable for lambda.Start() that
// processes DynamoDB Streams events through the provided Runtime.
func NewLambdaHandler(runtime *Runtime) func(ctx context.Context, event events.DynamoDBEvent) (DynamoDBStreamResponse, error) {
	return func(ctx context.Context, event events.DynamoDBEvent) (DynamoDBStreamResponse, error) {
		logger := logging.GetLogger(ctx)
		logger.Info("Received DynamoDB stream event", "recordCount", len(event.Records))

		records := make([]StreamRecord, 0, len(event.Records))
		for _, r := range event.Records {
			sr, ok := convertRecord(r)
			if !ok {
				logger.Warn("Skipping non-INSERT record", "eventName", r.EventName, "eventID", r.EventID)
				continue
			}
			records = append(records, sr)
		}

		if len(records) == 0 {
			return DynamoDBStreamResponse{}, nil
		}

		failures, err := runtime.Process(ctx, records)
		if err != nil {
			return DynamoDBStreamResponse{}, err
		}

		return DynamoDBStreamResponse{
			BatchItemFailures: failures,
		}, nil
	}
}

// convertRecord transforms a DynamoDB stream event record into a StreamRecord.
// Returns false if the record should be skipped (e.g., not an INSERT).
func convertRecord(r events.DynamoDBEventRecord) (StreamRecord, bool) {
	if r.EventName != "INSERT" {
		return StreamRecord{}, false
	}

	newImage := r.Change.NewImage
	if len(newImage) == 0 {
		return StreamRecord{}, false
	}

	sr := StreamRecord{
		EventID: r.EventID,
	}

	pkVal, hasPK := newImage["pk"]
	skVal, hasSK := newImage["sk"]
	typeVal, hasType := newImage["type"]

	if !hasPK || !hasSK || !hasType {
		return StreamRecord{}, false
	}

	sr.EntityID = pkVal.String()
	sr.EventType = typeVal.String()

	if seq, err := strconv.Atoi(skVal.Number()); err == nil {
		sr.Sequence = seq
	}

	if v, ok := newImage["version"]; ok {
		if version, err := strconv.Atoi(v.Number()); err == nil {
			sr.Version = version
		}
	}
	if v, ok := newImage["entityType"]; ok {
		sr.EntityType = v.String()
	}
	if v, ok := newImage["payload"]; ok {
		sr.Payload = []byte(v.String())
	}
	if v, ok := newImage["metadata"]; ok {
		sr.Metadata = json.RawMessage(v.String())
	}

	sr.ApproximateCreationDateTime = r.Change.ApproximateCreationDateTime.Time

	return sr, true
}
