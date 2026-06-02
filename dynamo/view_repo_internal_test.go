package dynamo

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/stretchr/testify/require"
)

func Test_ViewRepository_marshalMap_Errors(t *testing.T) {
	repo := &ViewRepository{
		encoder: attributevalue.NewEncoder(func(opts *attributevalue.EncoderOptions) { opts.TagKey = tagKey }),
		decoder: attributevalue.NewDecoder(func(opts *attributevalue.DecoderOptions) { opts.TagKey = tagKey }),
	}

	// nil input => encoder returns a non-map AttributeValue (NULL)
	_, err := repo.marshalMap(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected AttributeValue type")

	// non-struct input => encoder returns a non-map AttributeValue
	_, err = repo.marshalMap("hello")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected AttributeValue type")
}

func Test_ViewRepository_unmarshalMap_Success(t *testing.T) {
	repo := &ViewRepository{
		encoder: attributevalue.NewEncoder(func(opts *attributevalue.EncoderOptions) { opts.TagKey = tagKey }),
		decoder: attributevalue.NewDecoder(func(opts *attributevalue.DecoderOptions) { opts.TagKey = tagKey }),
	}

	item := map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "pk1"},
		"entityID":   &types.AttributeValueMemberS{Value: "e1"},
		"entityType": &types.AttributeValueMemberS{Value: "t1"},
		"payload":    &types.AttributeValueMemberS{Value: "{}"},
	}

	var out View
	require.NoError(t, repo.unmarshalMap(item, &out))
	require.Equal(t, "pk1", out.PK)
	require.Equal(t, evt.EntityID("e1"), out.EntityID)
	require.Equal(t, evt.EntityType("t1"), out.EntityType)
	require.Equal(t, "{}", out.Payload)
}
