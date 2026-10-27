package stream

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
)

// NormalizeSNSEvent accepts a raw Lambda invocation payload and returns an
// events.SNSEvent regardless of whether the Lambda was invoked by a direct
// SNS subscription (prod) or via an SQS ingress queue with
// raw_message_delivery=true (dev).
//
// In the SQS path each record's Body contains the raw CloudWatchEvent JSON
// (the same content that SNS would place in SNSEntity.Message), so we wrap
// it in a synthetic SNSEventRecord to keep downstream handlers unchanged.
func NormalizeSNSEvent(raw json.RawMessage) (events.SNSEvent, error) {
	if len(raw) == 0 {
		return events.SNSEvent{}, fmt.Errorf("empty event payload")
	}

	// Try SNS first (prod path).
	var snsEvent events.SNSEvent
	if err := json.Unmarshal(raw, &snsEvent); err == nil && len(snsEvent.Records) > 0 {
		if snsEvent.Records[0].EventSource == "aws:sns" {
			return snsEvent, nil
		}
	}

	// Try SQS (dev path with raw_message_delivery=true).
	// NOTE: This assumes raw_message_delivery is enabled on the SNS→SQS subscription,
	// so SQS Body contains the raw event JSON, not an SNS notification envelope.
	var sqsEvent events.SQSEvent
	if err := json.Unmarshal(raw, &sqsEvent); err == nil && len(sqsEvent.Records) > 0 {
		if sqsEvent.Records[0].EventSource == "aws:sqs" {
			records := make([]events.SNSEventRecord, 0, len(sqsEvent.Records))
			for _, r := range sqsEvent.Records {
				records = append(records, events.SNSEventRecord{
					EventSource: "aws:sns",
					SNS: events.SNSEntity{
						Message: r.Body,
					},
				})
			}
			return events.SNSEvent{Records: records}, nil
		}
	}

	return events.SNSEvent{}, fmt.Errorf("payload is neither SNS nor SQS event")
}
