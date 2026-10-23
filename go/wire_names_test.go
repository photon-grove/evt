package evt_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/test"
)

// The Entity to Aggregate and Context to CommandContext renames changed Go identifiers only. These
// checks pin the strings that identifiers could leak into: the event and command type names, the
// entity type, and the JSON field names of an event payload and a serialized event. The compat
// fixtures (go test ./internal/compat with no -update, and the Rust fixture tests) pin the rest.
func TestRenamedTypesKeepTheirWireStrings(t *testing.T) {
	var aggregate evt.Aggregate = test.NewAggregate("a-1")
	require.Equal(t, evt.EntityType("test"), aggregate.Type())
	require.Equal(t, evt.EventType("test:created"), test.AggregateCreated{}.Type())
	require.Equal(t, evt.EventType("test:updated"), test.AggregateUpdated{}.Type())
	require.Equal(t, evt.CommandType("test:create"), (&test.CreateAggregate{}).Type())
	require.Equal(t, evt.CommandType("test:update"), (&test.ReplaceAggregate{}).Type())

	payload, err := json.Marshal(test.AggregateCreated{ID: "a-1", Value: "v"})
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"a-1","value":"v","other":null}`, string(payload))

	encoded, err := evt.MarshalEvent(evt.SerializedEvent{
		ID: "a-1:1", Sequence: 1, Type: "test:created", Version: 1,
		EntityID: "a-1", EntityType: "test", Payload: []byte(`{}`),
	})
	require.NoError(t, err)

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	for _, name := range []string{"id", "sequence", "type", "version", "entityId", "entityType", "payload", "metadata"} {
		require.Contains(t, fields, name)
	}
	require.Len(t, fields, 8)
}
