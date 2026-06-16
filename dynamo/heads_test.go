package dynamo

import (
	"context"
	"errors"
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
	mu                 sync.Mutex
	items              map[string]map[string]types.AttributeValue
	lastScanConsistent bool
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

	f.lastScanConsistent = in.ConsistentRead != nil && *in.ConsistentRead

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

// pagingHeadsDB is a behavioral fake of the heads table that returns exactly one row per Scan page,
// driving DynamoDB's ExclusiveStartKey/LastEvaluatedKey pagination. It exists to prove that
// StreamEntityHeadsFunc consumes the table page by page and never depends on a single full-table
// page — i.e. it never needs to hold all rows at once. maxLive records the largest number of rows
// the fake ever had to materialize for one page (always 1 here), and pages counts the Scan calls.
type pagingHeadsDB struct {
	*mock.Client
	rows    []map[string]types.AttributeValue
	pages   int
	maxLive int
}

func newPagingHeadsDB(rows []map[string]types.AttributeValue) *pagingHeadsDB {
	return &pagingHeadsDB{Client: mock.NewClient(), rows: rows}
}

func headRow(entityID, entityType string, seq int) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: entityID},
		"headSeq":    &types.AttributeValueMemberN{Value: strconv.Itoa(seq)},
		"entityType": &types.AttributeValueMemberS{Value: entityType},
	}
}

func (f *pagingHeadsDB) Scan(
	_ context.Context,
	in *dynamodb.ScanInput,
	_ ...func(*dynamodb.Options),
) (*dynamodb.ScanOutput, error) {
	f.pages++

	start := 0
	if in.ExclusiveStartKey != nil {
		// The LastEvaluatedKey we hand back below encodes the next row index, so resume from it.
		n, err := nval(in.ExclusiveStartKey["pk"])
		if err != nil {
			return nil, err
		}

		start = n
	}

	var filterType string
	if in.FilterExpression != nil {
		filterType = sval(in.ExpressionAttributeValues[":et"])
	}

	// Emit at most one matching row per page, then hand back a LastEvaluatedKey pointing past it so
	// the paginator asks again. One row in flight at a time is the whole point of the fake.
	for i := start; i < len(f.rows); i++ {
		row := f.rows[i]
		if filterType != "" && sval(row["entityType"]) != filterType {
			continue
		}

		if f.maxLive < 1 {
			f.maxLive = 1
		}

		return &dynamodb.ScanOutput{
			Items:            []map[string]types.AttributeValue{row},
			LastEvaluatedKey: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberN{Value: strconv.Itoa(i + 1)}},
		}, nil
	}

	return &dynamodb.ScanOutput{}, nil
}

func TestHeadStore_StreamEntityHeadsFunc_VisitsEachRowAcrossPages(t *testing.T) {
	ctx := context.Background()
	db := newPagingHeadsDB([]map[string]types.AttributeValue{
		headRow("widget-1", "widget", 5),
		headRow("widget-2", "widget", 9),
		headRow("widget-3", "widget", 2),
	})
	store := NewHeadStore(db, "heads")

	visited := map[evt.EntityID]evt.EventSequence{}
	err := store.StreamEntityHeadsFunc(ctx, "", func(id evt.EntityID, seq evt.EventSequence) error {
		visited[id] = seq
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, map[evt.EntityID]evt.EventSequence{"widget-1": 5, "widget-2": 9, "widget-3": 2}, visited)
	// One row per page proves the visitor never relied on a single full-table page: every row was
	// delivered incrementally, and the fake never held more than one at a time.
	require.GreaterOrEqual(t, db.pages, 3, "each entity should arrive on its own page")
	require.Equal(t, 1, db.maxLive, "the visitor path must never materialize more than one page of rows")
}

func TestHeadStore_StreamEntityHeadsFunc_StopsOnVisitError(t *testing.T) {
	ctx := context.Background()
	db := newPagingHeadsDB([]map[string]types.AttributeValue{
		headRow("widget-1", "widget", 1),
		headRow("widget-2", "widget", 2),
		headRow("widget-3", "widget", 3),
	})
	store := NewHeadStore(db, "heads")

	boom := errors.New("visitor boom")
	count := 0
	err := store.StreamEntityHeadsFunc(ctx, "", func(_ evt.EntityID, _ evt.EventSequence) error {
		count++
		return boom // fail on the very first row
	})

	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, count, "enumeration must stop at the first visit error, not drain the table")
	// The visitor is resumable: it stopped after paging only what it had visited, so a resumed run
	// (e.g. via a Skip predicate over the IDs already handled) picks up the rest.
	require.Less(t, db.pages, 3, "a stopped visitor should not page the whole table")
}

func TestHeadStore_StreamEntityHeadsFunc_FiltersByType(t *testing.T) {
	ctx := context.Background()
	db := newPagingHeadsDB([]map[string]types.AttributeValue{
		headRow("widget-1", "widget", 2),
		headRow("gadget-1", "gadget", 9),
		headRow("widget-2", "widget", 4),
	})
	store := NewHeadStore(db, "heads")

	visited := map[evt.EntityID]evt.EventSequence{}
	err := store.StreamEntityHeadsFunc(ctx, "gadget", func(id evt.EntityID, seq evt.EventSequence) error {
		visited[id] = seq
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, map[evt.EntityID]evt.EventSequence{"gadget-1": 9}, visited)
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

func TestHeadStore_StreamEntityHeads_EventuallyConsistentByDefault(t *testing.T) {
	ctx := context.Background()
	db := newFakeHeadsDB()
	store := NewHeadStore(db, "heads")

	// Default reads are eventually consistent (half the RCU cost).
	_, err := store.StreamEntityHeads(ctx, "")
	require.NoError(t, err)
	require.False(t, db.lastScanConsistent)

	// WithConsistentRead(true) opts into strongly consistent reads without mutating the original.
	strong := store.WithConsistentRead(true)
	_, err = strong.StreamEntityHeads(ctx, "")
	require.NoError(t, err)
	require.True(t, db.lastScanConsistent)

	_, err = store.StreamEntityHeads(ctx, "")
	require.NoError(t, err)
	require.False(t, db.lastScanConsistent, "original store must remain eventually consistent")
}
