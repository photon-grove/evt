package dynamo

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	lambdaevents "github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
)

// InvalidEventError indicates an irrecoverable validation/parsing error in a stream event.
// Callers can treat this as a non-retryable drop path.
type InvalidEventError struct {
	Field  string
	Reason string
	Err    error
}

func (e *InvalidEventError) Error() string {
	return fmt.Sprintf("invalid event field %q (%s): %v", e.Field, e.Reason, e.Err)
}

func (e *InvalidEventError) Unwrap() error {
	return e.Err
}

// IsInvalidEventError returns true when err wraps an InvalidEventError.
func IsInvalidEventError(err error) bool {
	var invalidEventErr *InvalidEventError
	return errors.As(err, &invalidEventErr)
}

// MarshalJSON takes a SerializedEvent encoded as a DynamoDB AttributeValue map and returns the JSON
// representation for publishing to an event bus
func MarshalJSON(item map[string]lambdaevents.DynamoDBAttributeValue) (evt.SerializedEvent, []byte, error) {
	var event evt.SerializedEvent
	var data Event

	err := UnmarshalAttributeMap(item, &data)
	if err != nil {
		return event, nil, fmt.Errorf("error unmarshaling item from Attribute Value Map: %s", err.Error())
	}

	metadataJSON := strings.TrimSpace(data.Metadata)
	if metadataJSON == "" {
		return event, nil, &InvalidEventError{
			Field:  "metadata",
			Reason: "missing",
			Err:    errors.New("metadata is empty"),
		}
	}

	var metadata evt.Metadata
	if err = json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
		return event, nil, &InvalidEventError{
			Field:  "metadata",
			Reason: "invalid_json",
			Err:    err,
		}
	}

	event = evt.SerializedEvent{
		ID:         evt.GetEventID(data.PK, data.SK),
		EntityID:   data.PK,
		EntityType: data.EntityType,
		Sequence:   data.SK,
		Type:       data.Type,
		Version:    data.Version,
		Payload:    []byte(data.Payload),
		Metadata:   metadata,
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		return event, nil, fmt.Errorf("error marshaling event to JSON: %s", err.Error())
	}

	return event, eventJSON, nil
}

// UnmarshalAttributeMap unmarshals a map of `DynamoDBAttributeValues“ (from stream events)
// into a Go value type
func UnmarshalAttributeMap[T any](value map[string]lambdaevents.DynamoDBAttributeValue, out T) error {
	if value == nil {
		return nil
	}

	valueMap, err := FromAttributeValueMap(value)
	if err != nil {
		return err
	}

	return attributevalue.UnmarshalMapWithOptions(valueMap, out, func(opts *attributevalue.DecoderOptions) {
		opts.TagKey = "json"
	})
}

// FromAttributeValueMap converts a map of Lambda Event DynamoDB
// AttributeValues, including all nested members, to a dynamodbstreams map of AttributeValue.
func FromAttributeValueMap(from map[string]lambdaevents.DynamoDBAttributeValue) (to map[string]types.AttributeValue, err error) {
	to = make(map[string]types.AttributeValue, len(from))
	for field, value := range from {
		to[field], err = FromAttributeValue(value)
		if err != nil {
			return nil, err
		}
	}

	return to, nil
}

// FromAttributeValueList converts a slice of Lambda Event DynamoDB
// AttributeValues, including all nested members, to a slice ofdynamodbstreams AttributeValue.
func FromAttributeValueList(from []lambdaevents.DynamoDBAttributeValue) (to []types.AttributeValue, err error) {
	to = make([]types.AttributeValue, len(from))
	for i := 0; i < len(from); i++ {
		to[i], err = FromAttributeValue(from[i])
		if err != nil {
			return nil, err
		}
	}

	return to, nil
}

// FromAttributeValue converts a Lambda Event DynamoDB AttributeValue, including
// all nested members, to a dynamodbstreams AttributeValue.
func FromAttributeValue(from lambdaevents.DynamoDBAttributeValue) (types.AttributeValue, error) {
	switch from.DataType() {
	case lambdaevents.DataTypeNull:
		return &types.AttributeValueMemberNULL{Value: from.IsNull()}, nil

	case lambdaevents.DataTypeBoolean:
		return &types.AttributeValueMemberBOOL{Value: from.Boolean()}, nil

	case lambdaevents.DataTypeBinary:
		return &types.AttributeValueMemberB{Value: from.Binary()}, nil

	case lambdaevents.DataTypeBinarySet:
		bs := make([][]byte, len(from.BinarySet()))
		for i := 0; i < len(from.BinarySet()); i++ {
			bs[i] = append([]byte{}, from.BinarySet()[i]...)
		}
		return &types.AttributeValueMemberBS{Value: bs}, nil

	case lambdaevents.DataTypeNumber:
		return &types.AttributeValueMemberN{Value: from.Number()}, nil

	case lambdaevents.DataTypeNumberSet:
		return &types.AttributeValueMemberNS{Value: append([]string{}, from.NumberSet()...)}, nil

	case lambdaevents.DataTypeString:
		return &types.AttributeValueMemberS{Value: from.String()}, nil

	case lambdaevents.DataTypeStringSet:
		return &types.AttributeValueMemberSS{Value: append([]string{}, from.StringSet()...)}, nil

	case lambdaevents.DataTypeList:
		values, err := FromAttributeValueList(from.List())
		if err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberL{Value: values}, nil

	case lambdaevents.DataTypeMap:
		values, err := FromAttributeValueMap(from.Map())
		if err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberM{Value: values}, nil

	default:
		return nil, fmt.Errorf("unknown AttributeValue union member, %T", from)
	}
}
