package projectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-lambda-go/events"

	"github.com/photon-grove/evt"
)

// FromSerializedEvent converts a decoded domain event into a StreamRecord. It is
// the consumer-side inverse of ToSerializedEvent and underpins the SNS/SQS
// handlers below.
func FromSerializedEvent(event evt.SerializedEvent) StreamRecord {
	record := StreamRecord{
		EventID:    string(event.ID),
		EntityID:   string(event.EntityID),
		EntityType: string(event.EntityType),
		EventType:  string(event.Type),
		Version:    int(event.Version),
		Sequence:   int(event.Sequence),
		Payload:    event.Payload,
	}

	if raw, err := json.Marshal(event.Metadata); err == nil {
		record.Metadata = raw
	}

	return record
}

// StreamRecordFromEnvelope decodes one CloudWatchEvent envelope — the message
// shape stream.SNSPublisher writes to the events topic — into a StreamRecord.
// The envelope's Detail field carries the serialized domain event.
//
// Pass the raw message body: the SQS message body when the SNS->SQS
// subscription uses raw message delivery, or record.SNS.Message for a direct
// SNS->Lambda subscription. Both handlers below call this for you.
func StreamRecordFromEnvelope(body []byte) (StreamRecord, error) {
	var envelope events.CloudWatchEvent
	if err := json.Unmarshal(body, &envelope); err != nil {
		return StreamRecord{}, fmt.Errorf("unmarshalling event envelope: %w", err)
	}

	var event evt.SerializedEvent
	if err := json.Unmarshal(envelope.Detail, &event); err != nil {
		return StreamRecord{}, fmt.Errorf("unmarshalling serialized event from envelope: %w", err)
	}

	if event.EntityID == "" {
		return StreamRecord{}, errors.New("event envelope has no entity id")
	}

	return FromSerializedEvent(event), nil
}

// NewSQSHandler returns a lambda.Start-compatible handler for the blessed
// fan-out path: the events topic delivered over SNS->SQS (with raw message
// delivery), driven through the Runtime. This is the recommended way to run a
// projector — the publisher is the single DynamoDB Streams consumer, and every
// projector subscribes to its SNS topic.
//
// It reports partial batch failures, so only messages whose events fail to
// project are redelivered (enable ReportBatchItemFailures on the event source
// mapping). A message whose body cannot be decoded is dropped and logged rather
// than retried — a malformed envelope will never succeed.
func NewSQSHandler(runtime *Runtime) func(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
	return func(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
		logger := runtime.Logger()
		logger.Info("Received SQS event", "recordCount", len(event.Records))

		records := make([]StreamRecord, 0, len(event.Records))
		// Track which SQS message carried each event so a failed event redelivers
		// the right message.
		messageIDByEvent := make(map[string]string, len(event.Records))
		for _, message := range event.Records {
			record, err := StreamRecordFromEnvelope([]byte(message.Body))
			if err != nil {
				logger.Warn("Skipping undecodable SQS message", "messageID", message.MessageId, "error", err)
				continue
			}
			records = append(records, record)
			messageIDByEvent[record.EventID] = message.MessageId
		}

		response := events.SQSEventResponse{BatchItemFailures: []events.SQSBatchItemFailure{}}
		if len(records) == 0 {
			return response, nil
		}

		failures, err := runtime.Process(ctx, records)
		if err != nil {
			return events.SQSEventResponse{}, err
		}

		for _, failure := range failures {
			if messageID, ok := messageIDByEvent[failure.ItemIdentifier]; ok {
				response.BatchItemFailures = append(response.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: messageID})
			}
		}

		return response, nil
	}
}

// NewSNSHandler returns a lambda.Start-compatible handler for a direct
// SNS->Lambda subscription to the events topic. Prefer NewSQSHandler for most
// projectors; reach for a direct subscription only for latency-sensitive
// consumers that can tolerate SNS's at-least-once, whole-invocation retries.
//
// SNS->Lambda has no partial-batch-failure protocol, so returning an error
// retries (and ultimately dead-letters) the whole invocation. Configure a
// redrive policy on the subscription for failures.
func NewSNSHandler(runtime *Runtime) func(ctx context.Context, event events.SNSEvent) error {
	return func(ctx context.Context, event events.SNSEvent) error {
		logger := runtime.Logger()
		logger.Info("Received SNS event", "recordCount", len(event.Records))

		records := make([]StreamRecord, 0, len(event.Records))
		for _, record := range event.Records {
			streamRecord, err := StreamRecordFromEnvelope([]byte(record.SNS.Message))
			if err != nil {
				logger.Warn("Skipping undecodable SNS message", "messageID", record.SNS.MessageID, "error", err)
				continue
			}
			records = append(records, streamRecord)
		}

		if len(records) == 0 {
			return nil
		}

		failures, err := runtime.Process(ctx, records)
		if err != nil {
			return err
		}

		if len(failures) > 0 {
			return errors.New("projector reported failed records on an SNS delivery; retrying the whole invocation")
		}

		return nil
	}
}
