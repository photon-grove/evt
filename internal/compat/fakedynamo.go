package compat

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/photon-grove/evt/dynamo"
)

// FakeDynamo is an in-memory DynamoDB table that understands exactly the key conditions and
// condition expressions the evt DynamoDB repository issues. It lets the golden fixtures come from
// the real dynamo.Repository write path without an emulator.
type FakeDynamo struct {
	mu    sync.Mutex
	items map[string]map[int64]map[string]types.AttributeValue
}

var _ dynamo.Client = (*FakeDynamo)(nil)

// NewFakeDynamo returns an empty table.
func NewFakeDynamo() *FakeDynamo {
	return &FakeDynamo{items: make(map[string]map[int64]map[string]types.AttributeValue)}
}

// Items returns every item in a partition, ascending by sort key (the snapshot at sk=0 first).
func (f *FakeDynamo) Items(pk string) []map[string]types.AttributeValue {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.sortedLocked(pk, true)
}

func (f *FakeDynamo) sortedLocked(pk string, ascending bool) []map[string]types.AttributeValue {
	partition := f.items[pk]
	keys := make([]int64, 0, len(partition))
	for sk := range partition {
		keys = append(keys, sk)
	}

	sort.Slice(keys, func(i, j int) bool {
		if ascending {
			return keys[i] < keys[j]
		}
		return keys[i] > keys[j]
	})

	out := make([]map[string]types.AttributeValue, 0, len(keys))
	for _, sk := range keys {
		out = append(out, partition[sk])
	}

	return out
}

func keyOf(item map[string]types.AttributeValue) (string, int64, error) {
	pk, ok := item["pk"].(*types.AttributeValueMemberS)
	if !ok {
		return "", 0, errors.New("fake dynamo: item has no string pk")
	}

	skAttr, ok := item["sk"].(*types.AttributeValueMemberN)
	if !ok {
		return "", 0, errors.New("fake dynamo: item has no numeric sk")
	}

	sk, err := strconv.ParseInt(skAttr.Value, 10, 64)
	if err != nil {
		return "", 0, err
	}

	return pk.Value, sk, nil
}

func number(values map[string]types.AttributeValue, name string) (int64, error) {
	attr, ok := values[name].(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("fake dynamo: %s is not a number", name)
	}

	return strconv.ParseInt(attr.Value, 10, 64)
}

// conditionHolds evaluates the condition expressions the repository uses.
func conditionHolds(
	existing map[string]types.AttributeValue,
	condition string,
	values map[string]types.AttributeValue,
) (bool, error) {
	switch condition {
	case "":
		return true, nil

	case "attribute_not_exists(sk)":
		return existing == nil, nil

	case dynamo.FloorCondition:
		return compareIfExists(existing, "eventSeq", values, ":first", func(have, first int64) bool { return have < first })

	case dynamo.FirstSnapshotCondition:
		_, exists := existing["seq"]
		return !exists, nil

	case dynamo.NextSnapshotCondition:
		if _, exists := existing["seq"]; !exists {
			return false, nil
		}
		sameSnapshot, err := compareIfExists(existing, "seq", values, ":seq", func(have, want int64) bool { return have == want })
		if err != nil || !sameSnapshot {
			return false, err
		}
		return compareIfExists(existing, "eventSeq", values, ":first", func(have, first int64) bool { return have < first })

	case "attribute_not_exists(eventSeq) OR eventSeq <= :new":
		return compareIfExists(existing, "eventSeq", values, ":new", func(have, limit int64) bool { return have <= limit })
	}

	return false, fmt.Errorf("fake dynamo: unsupported condition %q", condition)
}

// compareIfExists evaluates `attribute_not_exists(name) OR <compare(name, value)>` for a numeric
// attribute and expression value.
func compareIfExists(
	existing map[string]types.AttributeValue,
	name string,
	values map[string]types.AttributeValue,
	value string,
	compare func(have, want int64) bool,
) (bool, error) {
	if _, ok := existing[name]; !ok {
		return true, nil
	}

	have, err := number(existing, name)
	if err != nil {
		return false, err
	}
	want, err := number(values, value)
	if err != nil {
		return false, err
	}

	return compare(have, want), nil
}

// transactTarget is the item a transaction action touches and the condition it carries.
type transactTarget struct {
	pk        string
	sk        int64
	put       map[string]types.AttributeValue
	condition string
	values    map[string]types.AttributeValue
}

func targetOf(write types.TransactWriteItem) (transactTarget, error) {
	switch {
	case write.Put != nil:
		pk, sk, err := keyOf(write.Put.Item)
		return transactTarget{pk, sk, write.Put.Item, aws.ToString(write.Put.ConditionExpression), write.Put.ExpressionAttributeValues}, err
	case write.ConditionCheck != nil:
		pk, sk, err := keyOf(write.ConditionCheck.Key)
		return transactTarget{pk, sk, nil, aws.ToString(write.ConditionCheck.ConditionExpression), write.ConditionCheck.ExpressionAttributeValues}, err
	}

	return transactTarget{}, errors.New("fake dynamo: only Put and ConditionCheck are supported in transactions")
}

// TransactWriteItems applies puts atomically, cancelling the whole transaction on any failed
// condition with the same exception shape DynamoDB returns.
func (f *FakeDynamo) TransactWriteItems(
	_ context.Context,
	params *dynamodb.TransactWriteItemsInput,
	_ ...func(*dynamodb.Options),
) (*dynamodb.TransactWriteItemsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	reasons := make([]types.CancellationReason, len(params.TransactItems))
	targets := make([]transactTarget, len(params.TransactItems))
	touched := make(map[string]bool, len(params.TransactItems))
	failed := false

	for i, write := range params.TransactItems {
		target, err := targetOf(write)
		if err != nil {
			return nil, err
		}

		// DynamoDB rejects a transaction that touches one item twice.
		key := fmt.Sprintf("%s/%d", target.pk, target.sk)
		if touched[key] {
			return nil, fmt.Errorf("fake dynamo: transaction touches %s more than once", key)
		}
		touched[key] = true
		targets[i] = target

		reasons[i] = types.CancellationReason{Code: aws.String("None")}
		ok, err := conditionHolds(f.items[target.pk][target.sk], target.condition, target.values)
		if err != nil {
			return nil, err
		}
		if !ok {
			reasons[i] = types.CancellationReason{Code: aws.String("ConditionalCheckFailed")}
			failed = true
		}
	}

	if failed {
		return nil, &types.TransactionCanceledException{
			Message:             aws.String("Transaction cancelled, please refer cancellation reasons for specific reasons"),
			CancellationReasons: reasons,
		}
	}

	for _, target := range targets {
		if target.put != nil {
			f.putLocked(target.pk, target.sk, target.put)
		}
	}

	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func (f *FakeDynamo) putLocked(pk string, sk int64, item map[string]types.AttributeValue) {
	if f.items[pk] == nil {
		f.items[pk] = make(map[int64]map[string]types.AttributeValue)
	}

	copied := make(map[string]types.AttributeValue, len(item))
	for name, value := range item {
		copied[name] = value
	}

	f.items[pk][sk] = copied
}

// PutItem writes one item when its condition holds.
func (f *FakeDynamo) PutItem(
	_ context.Context,
	params *dynamodb.PutItemInput,
	_ ...func(*dynamodb.Options),
) (*dynamodb.PutItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	pk, sk, err := keyOf(params.Item)
	if err != nil {
		return nil, err
	}

	ok, err := conditionHolds(f.items[pk][sk], aws.ToString(params.ConditionExpression), params.ExpressionAttributeValues)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &types.ConditionalCheckFailedException{Message: aws.String("The conditional request failed")}
	}

	f.putLocked(pk, sk, params.Item)

	return &dynamodb.PutItemOutput{}, nil
}

// GetItem reads one item by key.
func (f *FakeDynamo) GetItem(
	_ context.Context,
	params *dynamodb.GetItemInput,
	_ ...func(*dynamodb.Options),
) (*dynamodb.GetItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	pk, sk, err := keyOf(params.Key)
	if err != nil {
		return nil, err
	}

	return &dynamodb.GetItemOutput{Item: f.items[pk][sk]}, nil
}

// Query supports `pk = :pk AND sk > :sk` and `pk = :pk AND sk BETWEEN :lo AND :hi`, with
// ScanIndexForward and Limit. It returns everything in one page.
func (f *FakeDynamo) Query(
	_ context.Context,
	params *dynamodb.QueryInput,
	_ ...func(*dynamodb.Options),
) (*dynamodb.QueryOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	values := params.ExpressionAttributeValues
	pk, ok := values[":pk"].(*types.AttributeValueMemberS)
	if !ok {
		return nil, errors.New("fake dynamo: query needs :pk")
	}

	var match func(sk int64) bool

	switch condition := aws.ToString(params.KeyConditionExpression); condition {
	case "pk = :pk AND sk > :sk":
		after, err := number(values, ":sk")
		if err != nil {
			return nil, err
		}
		match = func(sk int64) bool { return sk > after }

	case "pk = :pk AND sk BETWEEN :lo AND :hi":
		lo, err := number(values, ":lo")
		if err != nil {
			return nil, err
		}
		hi, err := number(values, ":hi")
		if err != nil {
			return nil, err
		}
		match = func(sk int64) bool { return sk >= lo && sk <= hi }

	default:
		return nil, fmt.Errorf("fake dynamo: unsupported key condition %q", condition)
	}

	ascending := params.ScanIndexForward == nil || *params.ScanIndexForward

	var items []map[string]types.AttributeValue
	for _, item := range f.sortedLocked(pk.Value, ascending) {
		_, sk, err := keyOf(item)
		if err != nil {
			return nil, err
		}

		if !match(sk) {
			continue
		}

		items = append(items, project(item, aws.ToString(params.ProjectionExpression)))
		if params.Limit != nil && len(items) == int(*params.Limit) {
			break
		}
	}

	return &dynamodb.QueryOutput{Items: items, Count: int32(len(items))}, nil //nolint:gosec // an in-memory fixture partition holds a handful of items
}

func project(item map[string]types.AttributeValue, projection string) map[string]types.AttributeValue {
	if projection == "" {
		return item
	}

	out := make(map[string]types.AttributeValue)
	for _, name := range strings.Split(projection, ",") {
		name = strings.TrimSpace(name)
		if value, ok := item[name]; ok {
			out[name] = value
		}
	}

	return out
}

// BatchWriteItem applies delete requests.
func (f *FakeDynamo) BatchWriteItem(
	_ context.Context,
	params *dynamodb.BatchWriteItemInput,
	_ ...func(*dynamodb.Options),
) (*dynamodb.BatchWriteItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, requests := range params.RequestItems {
		for _, request := range requests {
			if request.DeleteRequest == nil {
				return nil, errors.New("fake dynamo: only deletes are supported in batches")
			}

			pk, sk, err := keyOf(request.DeleteRequest.Key)
			if err != nil {
				return nil, err
			}

			delete(f.items[pk], sk)
		}
	}

	return &dynamodb.BatchWriteItemOutput{}, nil
}

var errUnsupported = errors.New("fake dynamo: operation not supported")

// Scan is not supported. The harness never scans.
func (f *FakeDynamo) Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return nil, errUnsupported
}

// BatchGetItem is not supported.
func (f *FakeDynamo) BatchGetItem(context.Context, *dynamodb.BatchGetItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	return nil, errUnsupported
}

// DeleteItem is not supported.
func (f *FakeDynamo) DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return nil, errUnsupported
}

// UpdateItem is not supported.
func (f *FakeDynamo) UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return nil, errUnsupported
}

// ExportTableToPointInTime is not supported.
func (f *FakeDynamo) ExportTableToPointInTime(
	context.Context,
	*dynamodb.ExportTableToPointInTimeInput,
	...func(*dynamodb.Options),
) (*dynamodb.ExportTableToPointInTimeOutput, error) {
	return nil, errUnsupported
}
