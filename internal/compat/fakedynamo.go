package compat

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/internal/streamheads"
)

// FakeDynamo is an in-memory DynamoDB table that understands exactly the key conditions and
// condition expressions the evt DynamoDB repository issues. It lets the golden fixtures come from
// the real dynamo.Repository write path without an emulator.
type FakeDynamo struct {
	mu    sync.Mutex
	items map[string]map[int64]map[string]types.AttributeValue

	// views holds the rows of a views table, whose sort key is a string (commit-wide view
	// operations, ADR 0004 C7): partition key, then sort key.
	views map[string]map[string]map[string]types.AttributeValue

	// transactions records the actions of every TransactWriteItems call in request order,
	// including calls the table then cancels.
	transactions [][]TransactionAction
}

// TransactionAction is one TransactWriteItems action as the repository sent it: the action
// kind, the item key it touches, and the condition it carries. Cross-language fixtures pin
// the sequence so an implementation that reorders actions fails even when the stored state is
// identical.
type TransactionAction struct {
	Operation string `json:"operation"`

	// Table is "views" for an action on the views table (commit-wide view operations) and absent
	// for the events table. A views row's sort key is a string, carried in SKText with SK zero.
	Table  string `json:"table,omitempty"`
	PK     string `json:"pk"`
	SK     int64  `json:"sk"`
	SKText string `json:"skText,omitempty"`

	// Update is an Update action's update expression (the head update of a written stream).
	Update    string            `json:"update,omitempty"`
	Condition string            `json:"condition,omitempty"`
	Names     map[string]string `json:"names,omitempty"`
	Values    map[string]any    `json:"values,omitempty"`
}

var _ dynamo.Client = (*FakeDynamo)(nil)

// NewFakeDynamo returns an empty table.
func NewFakeDynamo() *FakeDynamo {
	return &FakeDynamo{
		items: make(map[string]map[int64]map[string]types.AttributeValue),
		views: make(map[string]map[string]map[string]types.AttributeValue),
	}
}

// Transactions returns the actions of every TransactWriteItems call so far, in request order.
func (f *FakeDynamo) Transactions() [][]TransactionAction {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.transactions)
}

// Views returns every row of the views table in a partition, ascending by sort key bytes.
func (f *FakeDynamo) Views(pk string) []map[string]types.AttributeValue {
	f.mu.Lock()
	defer f.mu.Unlock()

	partition := f.views[pk]
	keys := make([]string, 0, len(partition))
	for sk := range partition {
		keys = append(keys, sk)
	}

	sort.Strings(keys)

	out := make([]map[string]types.AttributeValue, 0, len(keys))
	for _, sk := range keys {
		out = append(out, partition[sk])
	}

	return out
}

// SeedView stores a views row owned by owner directly, as a fixture's starting state.
func (f *FakeDynamo) SeedView(pk, sk, owner string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.putViewLocked(pk, sk, map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: pk},
		"sk":         &types.AttributeValueMemberS{Value: sk},
		"entityID":   &types.AttributeValueMemberS{Value: owner},
		"entityType": &types.AttributeValueMemberS{Value: ""},
		"payload":    &types.AttributeValueMemberS{Value: "{}"},
	})
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

// textKeyOf returns the keys of an item or key whose sort key is a string, as a views row's is, and
// false when the sort key is not a string.
func textKeyOf(item map[string]types.AttributeValue) (string, string, bool) {
	pk, ok := item["pk"].(*types.AttributeValueMemberS)
	if !ok {
		return "", "", false
	}

	sk, ok := item["sk"].(*types.AttributeValueMemberS)
	if !ok {
		return "", "", false
	}

	return pk.Value, sk.Value, true
}

func number(values map[string]types.AttributeValue, name string) (int64, error) {
	attr, ok := values[name].(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("fake dynamo: %s is not a number", name)
	}

	return strconv.ParseInt(attr.Value, 10, 64)
}

// ownedBy reports whether a stored views row's entityID is the expression's :owner.
func ownedBy(existing, values map[string]types.AttributeValue) bool {
	have, ok := existing["entityID"].(*types.AttributeValueMemberS)
	want, wantOK := values[dynamo.ViewOwnerValue].(*types.AttributeValueMemberS)

	return ok && wantOK && have.Value == want.Value
}

// conditionHolds evaluates the condition expressions the repository uses.
func conditionHolds(
	existing map[string]types.AttributeValue,
	condition string,
	values map[string]types.AttributeValue,
) (bool, error) {
	// is reports the stored numeric attribute compared with an expression value; it is false when
	// the attribute is absent.
	is := func(name, value string, compare func(have, want int64) bool) (bool, error) {
		if _, ok := existing[name]; !ok {
			return false, nil
		}

		return compareIfExists(existing, name, values, value, compare)
	}

	equal := func(have, want int64) bool { return have == want }
	atLeast := func(have, want int64) bool { return have >= want }

	// ttlNotLowered is `attribute_not_exists(#ttl) OR #ttl <= :t`.
	ttlNotLowered := func() (bool, error) {
		return compareIfExists(existing, "ttl", values, ":t", func(have, want int64) bool { return have <= want })
	}

	switch condition {
	case "":
		return true, nil

	case "attribute_not_exists(sk)", dynamo.ViewAbsentCondition:
		return existing == nil, nil

	case dynamo.ViewOwnedCondition:
		return existing == nil || ownedBy(existing, values), nil

	case dynamo.ViewPresentCondition:
		return existing != nil && ownedBy(existing, values), nil

	case "attribute_not_exists(pk) OR version = :v":
		return compareIfExists(existing, "version", values, ":v", equal)

	case "version = :observed":
		if existing == nil {
			return false, nil
		}
		return compareIfExists(existing, "version", values, ":observed", equal)

	case streamheads.CreateCondition:
		_, exists := existing["head"]
		return !exists, nil

	case streamheads.AppendCondition:
		return is("head", ":prev", equal)

	case streamheads.RetainedAppendCondition:
		held, err := is("head", ":prev", equal)
		if err != nil || !held {
			return false, err
		}
		return ttlNotLowered()

	case streamheads.FenceCondition:
		return is("head", ":head", equal)

	case streamheads.GuardCondition:
		return is("head", ":new", atLeast)

	case streamheads.GuardEmptyCondition:
		return compareIfExists(existing, "head", values, ":new", atLeast)

	case streamheads.GuardRetainedCondition:
		held, err := is("head", ":new", atLeast)
		if err != nil || !held {
			return false, err
		}
		return ttlNotLowered()

	case dynamo.FirstSnapshotCondition:
		_, exists := existing["seq"]
		return !exists, nil

	case dynamo.NextSnapshotCondition:
		return is("seq", ":seq", equal)

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
	pk string
	sk int64

	// skText is set, with views, for an action on the views table.
	skText    string
	views     bool
	put       map[string]types.AttributeValue
	condition string
	values    map[string]types.AttributeValue
	delete    bool

	// update, with names and values, is an Update action's expression.
	update string
	names  map[string]string
}

func targetOf(write types.TransactWriteItem) (transactTarget, error) {
	// An action whose sort key is a string touches the views table.
	if view, ok := viewTargetOf(write); ok {
		return view, nil
	}

	switch {
	case write.Put != nil:
		pk, sk, err := keyOf(write.Put.Item)
		return transactTarget{pk: pk, sk: sk, put: write.Put.Item, condition: aws.ToString(write.Put.ConditionExpression), values: write.Put.ExpressionAttributeValues}, err
	case write.ConditionCheck != nil:
		pk, sk, err := keyOf(write.ConditionCheck.Key)
		return transactTarget{pk: pk, sk: sk, condition: aws.ToString(write.ConditionCheck.ConditionExpression), values: write.ConditionCheck.ExpressionAttributeValues, names: write.ConditionCheck.ExpressionAttributeNames}, err
	case write.Delete != nil:
		pk, sk, err := keyOf(write.Delete.Key)
		return transactTarget{pk: pk, sk: sk, condition: aws.ToString(write.Delete.ConditionExpression), values: write.Delete.ExpressionAttributeValues, delete: true, names: write.Delete.ExpressionAttributeNames}, err
	case write.Update != nil:
		pk, sk, err := keyOf(write.Update.Key)
		return transactTarget{pk: pk, sk: sk, condition: aws.ToString(write.Update.ConditionExpression), values: write.Update.ExpressionAttributeValues, update: aws.ToString(write.Update.UpdateExpression), names: write.Update.ExpressionAttributeNames}, err
	}

	return transactTarget{}, errors.New("fake dynamo: unsupported transaction action")
}

// viewTargetOf recognizes a Put or Delete on the views table, whose sort key is a string.
func viewTargetOf(write types.TransactWriteItem) (transactTarget, bool) {
	switch {
	case write.Put != nil:
		if pk, sk, ok := textKeyOf(write.Put.Item); ok {
			return transactTarget{
				pk: pk, skText: sk, views: true, put: write.Put.Item,
				condition: aws.ToString(write.Put.ConditionExpression), values: write.Put.ExpressionAttributeValues,
			}, true
		}

	case write.Delete != nil:
		if pk, sk, ok := textKeyOf(write.Delete.Key); ok {
			return transactTarget{
				pk: pk, skText: sk, views: true, delete: true,
				condition: aws.ToString(write.Delete.ConditionExpression), values: write.Delete.ExpressionAttributeValues,
			}, true
		}
	}

	return transactTarget{}, false
}

// action summarizes the target for the recorded transaction order.
func (t transactTarget) action(write types.TransactWriteItem) TransactionAction {
	operation := "ConditionCheck"
	switch {
	case write.Put != nil:
		operation = "Put"
	case write.Delete != nil:
		operation = "Delete"
	case write.Update != nil:
		operation = "Update"
	}

	action := TransactionAction{Operation: operation, PK: t.pk, SK: t.sk, Update: t.update, Condition: t.condition}
	if t.views {
		action.Table, action.SKText = "views", t.skText
	}
	if len(t.names) > 0 {
		action.Names = t.names
	}
	if len(t.values) > 0 {
		action.Values = DynamoItemJSON(t.values)
	}

	return action
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
	recorded := make([]TransactionAction, 0, len(params.TransactItems))
	failed := false

	for i, write := range params.TransactItems {
		target, err := targetOf(write)
		if err != nil {
			return nil, err
		}

		// DynamoDB rejects a transaction that touches one item twice.
		key := fmt.Sprintf("%s/%d", target.pk, target.sk)
		if target.views {
			key = fmt.Sprintf("views/%s/%s", target.pk, target.skText)
		}
		if touched[key] {
			return nil, fmt.Errorf("fake dynamo: transaction touches %s more than once", key)
		}
		touched[key] = true
		targets[i] = target
		recorded = append(recorded, target.action(write))

		reasons[i] = types.CancellationReason{Code: aws.String("None")}
		ok, err := conditionHolds(f.existingLocked(target), target.condition, target.values)
		if err != nil {
			return nil, err
		}
		if !ok {
			reasons[i] = types.CancellationReason{Code: aws.String("ConditionalCheckFailed")}
			failed = true
		}
	}

	f.transactions = append(f.transactions, recorded)

	if failed {
		return nil, &types.TransactionCanceledException{
			Message:             aws.String("Transaction cancelled, please refer cancellation reasons for specific reasons"),
			CancellationReasons: reasons,
		}
	}

	for _, target := range targets {
		switch {
		case target.views && target.put != nil:
			f.putViewLocked(target.pk, target.skText, target.put)

		case target.views:
			delete(f.views[target.pk], target.skText)

		case target.put != nil:
			f.putLocked(target.pk, target.sk, target.put)

		case target.delete:
			delete(f.items[target.pk], target.sk)

		case target.update != "":
			if err := f.updateLocked(target); err != nil {
				return nil, err
			}
		}
	}

	return &dynamodb.TransactWriteItemsOutput{}, nil
}

// existingLocked returns the stored item a target touches, or nil.
func (f *FakeDynamo) existingLocked(target transactTarget) map[string]types.AttributeValue {
	if target.views {
		return f.views[target.pk][target.skText]
	}

	return f.items[target.pk][target.sk]
}

func (f *FakeDynamo) putViewLocked(pk, sk string, item map[string]types.AttributeValue) {
	if f.views[pk] == nil {
		f.views[pk] = make(map[string]map[string]types.AttributeValue)
	}

	copied := make(map[string]types.AttributeValue, len(item))
	for name, value := range item {
		copied[name] = value
	}

	f.views[pk][sk] = copied
}

// updateLocked applies an update expression of the form `SET lhs = :value[, lhs = :value]`, the
// only shape the repository issues, creating the item when it does not exist (as DynamoDB does).
func (f *FakeDynamo) updateLocked(target transactTarget) error {
	expression, ok := strings.CutPrefix(target.update, "SET ")
	if !ok {
		return fmt.Errorf("fake dynamo: unsupported update expression %q", target.update)
	}

	item := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: target.pk},
		"sk": &types.AttributeValueMemberN{Value: strconv.FormatInt(target.sk, 10)},
	}
	for name, value := range f.items[target.pk][target.sk] {
		item[name] = value
	}

	for _, assignment := range strings.Split(expression, ", ") {
		name, placeholder, found := strings.Cut(assignment, " = ")
		if !found {
			return fmt.Errorf("fake dynamo: unsupported update expression %q", target.update)
		}

		if resolved, named := target.names[name]; named {
			name = resolved
		}

		value, present := target.values[placeholder]
		if !present {
			return fmt.Errorf("fake dynamo: update value %s is missing", placeholder)
		}

		item[name] = value
	}

	f.putLocked(target.pk, target.sk, item)

	return nil
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

	item := f.items[pk][sk]
	if item != nil {
		item = project(item, aws.ToString(params.ProjectionExpression), params.ExpressionAttributeNames)
	}

	return &dynamodb.GetItemOutput{Item: item}, nil
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

		items = append(items, project(item, aws.ToString(params.ProjectionExpression), params.ExpressionAttributeNames))
		if params.Limit != nil && len(items) == int(*params.Limit) {
			break
		}
	}

	return &dynamodb.QueryOutput{Items: items, Count: int32(len(items))}, nil //nolint:gosec // an in-memory fixture partition holds a handful of items
}

func project(item map[string]types.AttributeValue, projection string, names map[string]string) map[string]types.AttributeValue {
	if projection == "" {
		return item
	}

	out := make(map[string]types.AttributeValue)
	for _, name := range strings.Split(projection, ",") {
		name = strings.TrimSpace(name)
		if resolved, named := names[name]; named {
			name = resolved
		}

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

// Scan returns every item in one page, projected. The harness scans only to prove a table holds no
// stream before a writer declares stream-heads on it; filters are not supported.
func (f *FakeDynamo) Scan(
	_ context.Context,
	params *dynamodb.ScanInput,
	_ ...func(*dynamodb.Options),
) (*dynamodb.ScanOutput, error) {
	if params.FilterExpression != nil {
		return nil, errUnsupported
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	pks := make([]string, 0, len(f.items))
	for pk := range f.items {
		pks = append(pks, pk)
	}

	sort.Strings(pks)

	var items []map[string]types.AttributeValue

	for _, pk := range pks {
		for _, item := range f.sortedLocked(pk, true) {
			items = append(items, project(item, aws.ToString(params.ProjectionExpression), params.ExpressionAttributeNames))
		}
	}

	return &dynamodb.ScanOutput{Items: items, Count: int32(len(items))}, nil //nolint:gosec // an in-memory fixture table holds a handful of items
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
