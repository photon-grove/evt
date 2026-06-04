package dynamo

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
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
	})
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
	})
	require.NoError(t, err)

	_, present := ttlValue(t, transactions[0])
	require.False(t, present, "no policy means no ttl on any row")
}

func Test_buildSnapshotPutTransactions_StampsTTLForPolicyType(t *testing.T) {
	fixed := time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	repo := NewRepository(nil, "events").
		WithRetention(Retention{"transient": 7 * 24 * time.Hour}).
		WithClock(func() time.Time { return fixed })

	transactions, err := repo.buildSnapshotPutTransactions(
		[]evt.SerializedEvent{{EntityID: "a", EntityType: "transient", Sequence: 1}},
		"transient", "a", []byte(`{}`), 1,
	)
	require.NoError(t, err)
	require.NotEmpty(t, transactions)

	// The snapshot is the last item (sk=0), appended after the event puts.
	snapshot := transactions[len(transactions)-1]
	got, present := ttlValue(t, snapshot)
	require.True(t, present, "snapshot of a policy'd type must carry a ttl")
	require.Equal(t, fixed.Add(7*24*time.Hour).Unix(), got)
}
