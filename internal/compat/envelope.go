package compat

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
)

// SNSMessage builds the stream envelope for an event-log item: the CloudWatchEvent-style message
// body and the string message attributes that the SNS fan-out wrote. The Go library no longer
// ships that publisher, but the envelope is the cross-language wire format that the Rust crate
// encodes byte for byte, so the fixtures keep recording it. The item must be an event row, so the
// callers skip the snapshot (sk 0) and head (sk -1) items first.
func SNSMessage(item map[string]types.AttributeValue) (string, map[string]string, error) {
	image := make(map[string]events.DynamoDBAttributeValue, len(item))
	for name, value := range item {
		switch v := value.(type) {
		case *types.AttributeValueMemberS:
			image[name] = events.NewStringAttribute(v.Value)
		case *types.AttributeValueMemberN:
			image[name] = events.NewNumberAttribute(v.Value)
		default:
			return "", nil, fmt.Errorf("unsupported attribute %s (%T)", name, value)
		}
	}

	domainEvent, eventJSON, err := dynamo.MarshalJSON(image)
	if err != nil {
		return "", nil, err
	}

	envelope := events.CloudWatchEvent{
		ID:         string(domainEvent.ID),
		Source:     OriginSource,
		DetailType: string(domainEvent.Type),
		Detail:     eventJSON,
	}

	envelopeJSON, err := json.Marshal(envelope)
	if err != nil {
		return "", nil, fmt.Errorf("marshal envelope: %w", err)
	}

	attributes := map[string]string{
		"entityType": string(domainEvent.EntityType),
		"eventType":  string(domainEvent.Type),
	}
	correlationAttributes(domainEvent.Metadata, attributes)

	return string(envelopeJSON), attributes, nil
}

// correlationAttributes adds the command and trace correlation attributes. The command ID is the
// canonical cross-hop correlation key, and the traceparent is the fallback.
func correlationAttributes(metadata evt.Metadata, attributes map[string]string) {
	if metadata.CommandID != nil && *metadata.CommandID != "" {
		commandID := string(*metadata.CommandID)
		attributes["commandId"] = commandID
		attributes["correlationId"] = commandID
	}

	if metadata.Trace != nil {
		if traceParent, ok := (*metadata.Trace)["traceparent"]; ok && traceParent != "" {
			attributes["traceparent"] = traceParent
			if _, has := attributes["correlationId"]; !has {
				attributes["correlationId"] = traceParent
			}
		}
	}
}
