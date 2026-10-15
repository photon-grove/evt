package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
)

// Convert generic Transactions to DynamoDB TransactWriteItems if the TransactionGroup is based on
// DynamoDB.
func toDynamoItems(transactions evt.Transaction) ([]types.TransactWriteItem, error) {
	if len(transactions) == 0 {
		return nil, nil
	}

	var items []types.TransactWriteItem

	for _, group := range transactions {
		if group == nil {
			continue
		}

		if group.StorageType() != StorageType {
			return nil, fmt.Errorf("TransactionGroup is not based on DynamoDB: %v", group)
		}
		dynamoGroup, ok := group.(TransactionGroup)
		if !ok {
			return nil, fmt.Errorf("TransactionGroup was unable to be converted to DynamoDB Transact Write Items: %v", group)
		}

		items = append(items, dynamoGroup.ToWriteItems()...)
	}

	return items, nil
}

// transactItemSize returns a projector operation's DynamoDB-encoded item size: the sum over its
// Put item's attributes of the attribute name's UTF-8 byte length plus the value's encoded byte
// length. Non-put operations carry no bytes.
func transactItemSize(item types.TransactWriteItem) int {
	if item.Put == nil {
		return 0
	}

	return itemSize(item.Put.Item)
}

// itemSize computes the DynamoDB item size of an attribute map, mirroring the service's rules:
// strings and numbers count their encoded text, binary counts raw bytes, booleans and nulls one
// byte, maps and lists their structural overhead plus their members.
func itemSize(item map[string]types.AttributeValue) int {
	total := 0
	for name, value := range item {
		total += len(name) + attributeValueSize(value)
	}

	return total
}

func attributeValueSize(value types.AttributeValue) int {
	switch value := value.(type) {
	case *types.AttributeValueMemberS:
		return len(value.Value)
	case *types.AttributeValueMemberN:
		return len(value.Value)
	case *types.AttributeValueMemberB:
		return len(value.Value)
	case *types.AttributeValueMemberBOOL, *types.AttributeValueMemberNULL:
		return 1
	case *types.AttributeValueMemberM:
		return 3 + itemSize(value.Value) + len(value.Value)
	case *types.AttributeValueMemberL:
		total := 3
		for _, element := range value.Value {
			total += 1 + attributeValueSize(element)
		}
		return total
	case *types.AttributeValueMemberSS:
		total := 0
		for _, element := range value.Value {
			total += len(element)
		}
		return total
	case *types.AttributeValueMemberNS:
		total := 0
		for _, element := range value.Value {
			total += len(element)
		}
		return total
	case *types.AttributeValueMemberBS:
		total := 0
		for _, element := range value.Value {
			total += len(element)
		}
		return total
	default:
		return 0
	}
}
