package dynamo

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
)

// ViewProjector simply projects an Entity to a DynamoDB view by ID.
type ViewProjector struct {
	tableName string
	encoder   *attributevalue.Encoder
}

// NewViewProjector creates a new view projector for the given table name.
func NewViewProjector(tableName string) evt.EventProjector {
	encoder := attributevalue.NewEncoder(func(opts *attributevalue.EncoderOptions) {
		opts.TagKey = tagKey
	})

	return &ViewProjector{tableName, encoder}
}

// Project creates a new view for the given entity.
func (p *ViewProjector) Project(_ context.Context, entity evt.Entity, _ []evt.Event) (evt.TransactionGroup, error) {
	if p == nil || p.encoder == nil || entity == nil {
		return nil, nil
	}
	if p.tableName == "" {
		return nil, fmt.Errorf("table name is required")
	}
	entityID := entity.GetID()
	if entityID == "" {
		return nil, nil
	}

	payload, err := json.Marshal(entity)
	if err != nil {
		return nil, err
	}

	view := &evt.SerializedView{
		PK:         fmt.Sprintf("%s#%s", entity.Type(), entityID),
		SK:         evt.DefaultViewSK,
		EntityID:   entityID,
		EntityType: entity.Type(),
		Payload:    payload,
	}

	items := make([]types.TransactWriteItem, 0, 1)

	item, err := MarshalViewToItem(p.encoder, view)
	if err != nil {
		return nil, err
	}
	put := types.Put{
		TableName: aws.String(p.tableName),
		Item:      item,
	}
	items = append(items, types.TransactWriteItem{Put: &put})

	return &ViewPutGroup{
		tableName: p.tableName,
		items:     items,
	}, nil
}

// MarshalViewToItem converts a serialized view to a DynamoDB attribute map.
// If the SK field is empty, it defaults to DefaultViewSK ("VIEW") to prevent
// DynamoDB validation errors for empty key attributes.
func MarshalViewToItem(encoder *attributevalue.Encoder, view *evt.SerializedView) (map[string]types.AttributeValue, error) {
	if view == nil {
		return nil, fmt.Errorf("serialized view is nil")
	}

	sk := view.SK
	if sk == "" {
		sk = evt.DefaultViewSK
	}

	dynamoView := View{
		PK:         view.PK,
		SK:         sk,
		EntityID:   view.EntityID,
		EntityType: view.EntityType,
		Payload:    string(view.Payload),
	}

	av, err := encoder.Encode(dynamoView)
	if err != nil {
		return nil, err
	}
	if av == nil {
		return nil, fmt.Errorf("encoder returned nil attribute value")
	}

	member, ok := av.(*types.AttributeValueMemberM)
	if !ok {
		return nil, fmt.Errorf("encoder returned unexpected attribute value type: %T", av)
	}

	return member.Value, nil
}
