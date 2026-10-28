package projectors

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"

	"github.com/aws/aws-lambda-go/events"

	"github.com/photon-grove/evt/go/internal/streamheads"
)

// DynamoDBStreamResponse is the Lambda response for DynamoDB Streams event
// sources with partial batch failure reporting enabled. Each ItemIdentifier is a
// stream record's SequenceNumber.
type DynamoDBStreamResponse struct {
	BatchItemFailures []BatchItemFailure `json:"batchItemFailures"`
}

// NewLambdaHandler returns a function suitable for lambda.Start() that processes
// DynamoDB Streams events directly through the provided Runtime.
//
// This wires a projector directly to the event-log stream, bypassing SNS fan-out.
// Use it when the projector must consume DynamoDB Streams directly, and set
// FunctionResponseTypes: ["ReportBatchItemFailures"] on the event source mapping.
//
// Records go through the runtime one at a time, in stream order (so each
// Projector.Process call gets a single record), and the first record that fails
// stops the batch. The response reports that record's
// SequenceNumber: Lambda treats the records before it as done and delivers it and
// every later record again. The later records are not attempted, so they are
// projected after the failed one, and per-item order holds. Projectors must still
// be idempotent, since the failed record and the ones after it may arrive again.
//
// A transient error from the projector is returned, so Lambda retries the whole
// batch; records that already succeeded are skipped by the idempotency guard.
func NewLambdaHandler(runtime *Runtime) func(ctx context.Context, event events.DynamoDBEvent) (DynamoDBStreamResponse, error) {
	return func(ctx context.Context, event events.DynamoDBEvent) (DynamoDBStreamResponse, error) {
		logger := runtime.Logger()
		logger.Info("Received DynamoDB stream event", "recordCount", len(event.Records))

		for _, r := range event.Records {
			sr, ok := convertRecord(r)
			if !ok {
				logger.Warn("Skipping non-INSERT record", "eventName", r.EventName, "eventID", r.EventID)
				continue
			}

			failures, err := runtime.Process(ctx, []StreamRecord{sr})
			if err != nil {
				return DynamoDBStreamResponse{}, err
			}

			if len(failures) > 0 {
				return failedFrom(logger, r), nil
			}
		}

		return DynamoDBStreamResponse{}, nil
	}
}

// failedFrom is the response that makes Lambda resume from record r. An empty
// SequenceNumber is reported as is, and Lambda then fails the whole batch.
func failedFrom(logger *slog.Logger, r events.DynamoDBEventRecord) DynamoDBStreamResponse {
	logger.Warn("Stopping batch at failed record",
		"eventID", r.EventID, "sequenceNumber", r.Change.SequenceNumber)

	return DynamoDBStreamResponse{
		BatchItemFailures: []BatchItemFailure{{ItemIdentifier: r.Change.SequenceNumber}},
	}
}

// convertRecord transforms a DynamoDB stream event record into a StreamRecord.
// Returns false if the record should be skipped (e.g., not an INSERT).
func convertRecord(r events.DynamoDBEventRecord) (StreamRecord, bool) {
	// A head item (sk = -1, ADR 0007) or a snapshot (sk = 0) is not an event.
	if r.EventName != "INSERT" || streamheads.IsNonEventRecord(r) {
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
