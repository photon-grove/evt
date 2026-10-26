package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	evt "github.com/photon-grove/evt/go"
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

// transactItemSize returns a projector operation's DynamoDB-encoded request size. A Put counts its
// item. An Update, Delete, or ConditionCheck counts its key attributes plus its expression
// strings, attribute-name placeholders, and attribute values, because DynamoDB applies the
// 4 MB aggregate request limit to every action type and a large expression value would otherwise
// bypass the CommitTooLarge preflight. Each size follows the item-size rules: attribute names
// count their UTF-8 bytes and values their encoded bytes.
func transactItemSize(item types.TransactWriteItem) int {
	switch {
	case item.Put != nil:
		return itemSize(item.Put.Item)
	case item.Update != nil:
		return itemSize(item.Update.Key) +
			expressionSize(item.Update.UpdateExpression, item.Update.ConditionExpression,
				item.Update.ExpressionAttributeNames, item.Update.ExpressionAttributeValues)
	case item.Delete != nil:
		return itemSize(item.Delete.Key) +
			expressionSize(nil, item.Delete.ConditionExpression,
				item.Delete.ExpressionAttributeNames, item.Delete.ExpressionAttributeValues)
	case item.ConditionCheck != nil:
		return itemSize(item.ConditionCheck.Key) +
			expressionSize(nil, item.ConditionCheck.ConditionExpression,
				item.ConditionCheck.ExpressionAttributeNames, item.ConditionCheck.ExpressionAttributeValues)
	}

	return 0
}

// expressionSize counts an action's expression text, name placeholders (both sides), and
// expression attribute values.
func expressionSize(
	update, condition *string,
	names map[string]string,
	values map[string]types.AttributeValue,
) int {
	total := 0
	if update != nil {
		total += len(*update)
	}
	if condition != nil {
		total += len(*condition)
	}
	for placeholder, name := range names {
		total += len(placeholder) + len(name)
	}

	return total + itemSize(values)
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
