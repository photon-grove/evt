package dynamo

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	evt "github.com/photon-grove/evt/go"
	dynamock "github.com/photon-grove/evt/go/backends/dynamo/internal/mock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// ttlValue extracts the numeric ttl attribute from a built Put item, reporting whether it was present.
func ttlValue(t *testing.T, item types.TransactWriteItem) (int64, bool) {
	t.Helper()
	require.NotNil(t, item.Put)

	attr, ok := item.Put.Item["ttl"]
	if !ok {
		return 0, false
	}

	number, isNumber := attr.(*types.AttributeValueMemberN)
	require.True(t, isNumber, "ttl attribute should be a number")

	parsed, err := strconv.ParseInt(number.Value, 10, 64)
	require.NoError(t, err)

	return parsed, true
}

func Test_Event_TTL_OmittedWhenZero(t *testing.T) {
	data, err := json.Marshal(Event{PK: "e", SK: 1})
	require.NoError(t, err)

	var fields map[string]any
	require.NoError(t, json.Unmarshal(data, &fields))
	_, present := fields["ttl"]
	require.False(t, present, "ttl must be omitted when zero")

	data, err = json.Marshal(Event{PK: "e", SK: 1, TTL: 1717459200})
	require.NoError(t, err)
	require.Contains(t, string(data), `"ttl":1717459200`)
}

func Test_Snapshot_TTL_OmittedWhenZero(t *testing.T) {
	data, err := json.Marshal(Snapshot{PK: "e"})
	require.NoError(t, err)

	var fields map[string]any
	require.NoError(t, json.Unmarshal(data, &fields))
	_, present := fields["ttl"]
	require.False(t, present, "ttl must be omitted when zero")
}

func Test_buildEventPutTransactions_StampsTTLForPolicyTypeOnly(t *testing.T) {
	fixed := time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	repo := NewRepository(nil, "events").
		WithRetention(Retention{"transient": 30 * 24 * time.Hour}).
		WithClock(func() time.Time { return fixed })

	transactions, _, err := repo.buildEventPutTransactions([]evt.SerializedEvent{
		{EntityID: "a", EntityType: "transient", Sequence: 1},
		{EntityID: "b", EntityType: "durable", Sequence: 1},
	}, repo.ttlFor)
	require.NoError(t, err)
	require.Len(t, transactions, 2)

	got, present := ttlValue(t, transactions[0])
	require.True(t, present, "policy'd type must carry a ttl")
	require.Equal(t, fixed.Add(30*24*time.Hour).Unix(), got)

	_, present = ttlValue(t, transactions[1])
	require.False(t, present, "un-policed type must not carry a ttl")
}

func Test_buildEventPutTransactions_NoRetentionNeverStampsTTL(t *testing.T) {
	repo := NewRepository(nil, "events")

	transactions, _, err := repo.buildEventPutTransactions([]evt.SerializedEvent{
		{EntityID: "a", EntityType: "transient", Sequence: 1},
	}, repo.ttlFor)
	require.NoError(t, err)

	_, present := ttlValue(t, transactions[0])
	require.False(t, present, "no policy means no ttl on any row")
}

func Test_buildSnapshotPut_StampsTTLForPolicyType(t *testing.T) {
	fixed := time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	repo := NewRepository(nil, "events").
		WithRetention(Retention{"transient": 7 * 24 * time.Hour}).
		WithClock(func() time.Time { return fixed })

	snapshot, err := repo.buildSnapshotPut(
		evt.SerializedSnapshot{EntityType: "transient", EntityID: "a", Sequence: 1, EventSequence: 1, Payload: []byte(`{}`)},
		repo.ttlFor,
	)
	require.NoError(t, err)

	got, present := ttlValue(t, snapshot)
	require.True(t, present, "snapshot of a policy'd type must carry a ttl")
	require.Equal(t, fixed.Add(7*24*time.Hour).Unix(), got)
}

func Test_PutSnapshot_StampsTTLForPolicyType(t *testing.T) {
	fixed := time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	client := dynamock.NewClient()

	client.On("GetItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.GetItemOutput{Item: declaredMarkerItem()}, nil).Once()

	var captured *types.Put
	client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.TransactWriteItemsOutput{}, nil).
		Run(func(args mock.Arguments) {
			input, ok := args.Get(1).(*dynamodb.TransactWriteItemsInput)
			require.True(t, ok)
			require.Len(t, input.TransactItems, 3)
			require.NotNil(t, input.TransactItems[0].ConditionCheck)
			captured = input.TransactItems[2].Put
		})

	repo := NewRepository(client, "events").
		WithRetention(Retention{"transient": 30 * 24 * time.Hour}).
		WithClock(func() time.Time { return fixed })

	require.NoError(t, repo.PutSnapshot(context.Background(), "transient", "a", []byte(`{}`), 1, 3))
	require.NotNil(t, captured)

	attr, ok := captured.Item["ttl"].(*types.AttributeValueMemberN)
	require.True(t, ok, "background snapshot of a policy'd type must carry a ttl")
	require.Equal(t, strconv.FormatInt(fixed.Add(30*24*time.Hour).Unix(), 10), attr.Value)
}

func Test_PutSnapshot_NoTTLForUnpolicedType(t *testing.T) {
	client := dynamock.NewClient()

	client.On("GetItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.GetItemOutput{Item: declaredMarkerItem()}, nil).Once()

	var captured *types.Put
	client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.TransactWriteItemsOutput{}, nil).
		Run(func(args mock.Arguments) {
			input, ok := args.Get(1).(*dynamodb.TransactWriteItemsInput)
			require.True(t, ok)
			require.Len(t, input.TransactItems, 3)
			require.NotNil(t, input.TransactItems[0].ConditionCheck)
			captured = input.TransactItems[2].Put
		})

	repo := NewRepository(client, "events").
		WithRetention(Retention{"transient": 30 * 24 * time.Hour})

	require.NoError(t, repo.PutSnapshot(context.Background(), "durable", "a", []byte(`{}`), 1, 3))
	require.NotNil(t, captured)

	_, present := captured.Item["ttl"]
	require.False(t, present, "un-policed type must not carry a ttl on background snapshot writes")
}

// declaredMarkerItem is a feature marker that declares stream-heads, the state of every store a
// head-aware writer can write to.
func declaredMarkerItem() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk":       &types.AttributeValueMemberS{Value: "SYSTEM#FEATURES"},
		"sk":       &types.AttributeValueMemberN{Value: "0"},
		"version":  &types.AttributeValueMemberN{Value: "1"},
		"features": &types.AttributeValueMemberS{Value: `{"stream-heads":1}`},
	}
}
