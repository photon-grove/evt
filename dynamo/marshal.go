package dynamo

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// marshalMap converts a struct into a DynamoDB attribute map.
func (repo *Repository) marshalMap(in any) (map[string]types.AttributeValue, error) {
	av, err := repo.encoder.Encode(in)
	if err != nil {
		return nil, err
	}
	asMap, ok := av.(*types.AttributeValueMemberM)
	if !ok || av == nil {
		return nil, err
	}
	return asMap.Value, nil
}

// unmarshalMap converts a DynamoDB attribute map into a struct.
func (repo *Repository) unmarshalMap(value map[string]types.AttributeValue, out any) error {
	return repo.decoder.Decode(&types.AttributeValueMemberM{Value: value}, out)
}
