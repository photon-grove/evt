package dynamo

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo/mock"
	"github.com/photon-grove/evt/mem"
	"github.com/photon-grove/evt/projectors"
	"github.com/stretchr/testify/require"
)

// sval reads a DynamoDB string attribute, returning "" when absent or the wrong type.
func sval(av types.AttributeValue) string {
	if s, ok := av.(*types.AttributeValueMemberS); ok {
		return s.Value
	}

	return ""
}

// nval reads a DynamoDB number attribute as an int.
func nval(av types.AttributeValue) (int, error) {
	n, ok := av.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("attribute is not a number: %T", av)
	}

	return strconv.Atoi(n.Value)
}

// fakeHeadsDB is a behavioral fake of the heads table. It models UpdateItem's monotonic condition
// (attribute_not_exists(headSeq) OR headSeq < :seq) and Scan's single-page read with an optional
// entityType filter — enough to exercise HeadStore end to end without a live DynamoDB. The embedded
// mock.Client supplies the rest of the Client interface (never called here).
type fakeHeadsDB struct {
	*mock.Client
	mu    sync.Mutex
	items map[string]map[string]types.AttributeValue
}

func newFakeHeadsDB() *fakeHeadsDB {
	return &fakeHeadsDB{Client: mock.NewClient(), items: map[string]map[string]types.AttributeValue{}}
}

func (f *fakeHeadsDB) UpdateItem(
	_ context.Context,
	in *dynamodb.UpdateItemInput,
	_ ...func(*dynamodb.Options),
) (*dynamodb.UpdateItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	pk := sval(in.Key["pk"])

	newSeq, err := nval(in.ExpressionAttributeValues[":seq"])
	if err != nil {
		return nil, err
	}

	if existing, ok := f.items[pk]; ok {
		curSeq, err := nval(existing["headSeq"])
		if err != nil {
			return nil, err
		}
		if curSeq >= newSeq {
			// attribute_not_exists(headSeq) OR headSeq < :seq fails — the head already covers this.
			return nil, &types.ConditionalCheckFailedException{}
		}
	}

	f.items[pk] = map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: pk},
		"headSeq":    &types.AttributeValueMemberN{Value: strconv.Itoa(newSeq)},
		"entityType": &types.AttributeValueMemberS{Value: sval(in.ExpressionAttributeValues[":et"])},
	}

	return &dynamodb.UpdateItemOutput{}, nil
}

func (f *fakeHeadsDB) Scan(
	_ context.Context,
	in *dynamodb.ScanInput,
	_ ...func(*dynamodb.Options),
) (*dynamodb.ScanOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var filterType string
	if in.FilterExpression != nil {
		filterType = sval(in.ExpressionAttributeValues[":et"])
	}

	var out []map[string]types.AttributeValue
	for _, item := range f.items {
		if filterType != "" && sval(item["entityType"]) != filterType {
			continue
		}

		out = append(out, item)
	}

	return &dynamodb.ScanOutput{Items: out}, nil
}

func headRecord(entityID, entityType string, seq int) projectors.StreamRecord {
	return projectors.StreamRecord{
		EventID:    entityID + ":" + strconv.Itoa(seq),
		EntityID:   entityID,
		EntityType: entityType,
		Sequence:   seq,
	}
}

func TestHeadStore_Process_MonotonicUpsert(t *testing.T) {
	ctx := context.Background()
	store := NewHeadStore(newFakeHeadsDB(), "heads")

	// Advancing sequences set the head.
	failures, err := store.Process(ctx, []projectors.StreamRecord{
		headRecord("widget-1", "widget", 1),
		headRecord("widget-1", "widget", 2),
		headRecord("widget-1", "widget", 3),
	})
	require.NoError(t, err)
	require.Empty(t, failures)

	// A stale re-delivery and an out-of-order older event are no-ops, not failures.
	failures, err = store.Process(ctx, []projectors.StreamRecord{
		headRecord("widget-1", "widget", 2),
		headRecord("widget-1", "widget", 1),
	})
	require.NoError(t, err)
	require.Empty(t, failures)

	heads, err := store.StreamEntityHeads(ctx, "")
	require.NoError(t, err)
	require.Equal(t, map[evt.EntityID]evt.EventSequence{"widget-1": 3}, heads)
}

func TestHeadStore_Process_SkipsSnapshotAndMalformedRows(t *testing.T) {
	ctx := context.Background()
	store := NewHeadStore(newFakeHeadsDB(), "heads")

	failures, err := store.Process(ctx, []projectors.StreamRecord{
		headRecord("widget-1", "widget", 0), // snapshot row: no head to record
		{EventID: "no-entity", Sequence: 4}, // malformed: empty entity ID
	})
	require.NoError(t, err)
	require.Empty(t, failures)

	heads, err := store.StreamEntityHeads(ctx, "")
	require.NoError(t, err)
	require.Empty(t, heads)
}

func TestHeadStore_StreamEntityHeads_FiltersByType(t *testing.T) {
	ctx := context.Background()
	store := NewHeadStore(newFakeHeadsDB(), "heads")

	_, err := store.Process(ctx, []projectors.StreamRecord{
		headRecord("widget-1", "widget", 2),
		headRecord("gadget-1", "gadget", 9),
	})
	require.NoError(t, err)

	heads, err := store.StreamEntityHeads(ctx, "gadget")
	require.NoError(t, err)
	require.Equal(t, map[evt.EntityID]evt.EventSequence{"gadget-1": 9}, heads)
}

func TestHeadStore_Backfill_FromEventLogSource(t *testing.T) {
	ctx := context.Background()
	store := NewHeadStore(newFakeHeadsDB(), "heads")

	// A mem repo standing in for the event log: two widgets at different heads.
	source := mem.NewRepository()
	require.NoError(t, source.Commit(ctx, evt.SerializedResult{Events: []evt.SerializedEvent{
		{ID: "widget-1:1", Sequence: 1, Type: "Tested", EntityID: "widget-1", EntityType: "widget", Payload: []byte("{}")},
		{ID: "widget-1:2", Sequence: 2, Type: "Tested", EntityID: "widget-1", EntityType: "widget", Payload: []byte("{}")},
		{ID: "widget-2:1", Sequence: 1, Type: "Tested", EntityID: "widget-2", EntityType: "widget", Payload: []byte("{}")},
	}}))

	headSource, ok := source.(evt.EntityHeadStreamer)
	require.True(t, ok)

	written, err := store.Backfill(ctx, headSource, "widget")
	require.NoError(t, err)
	require.Equal(t, 2, written)

	heads, err := store.StreamEntityHeads(ctx, "widget")
	require.NoError(t, err)
	require.Equal(t, map[evt.EntityID]evt.EventSequence{"widget-1": 2, "widget-2": 1}, heads)
}
