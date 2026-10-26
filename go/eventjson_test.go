package evt

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSerializedEventDecodePreservesValues(t *testing.T) {
	for _, unmarshal := range []func([]byte, *SerializedEvent) error{
		func(data []byte, event *SerializedEvent) error { return json.Unmarshal(data, event) },
		UnmarshalEvent,
	} {
		event := SerializedEvent{ID: "account-1:1", Payload: []byte(`{}`)}
		require.NoError(t, unmarshal([]byte(`{"metadata":{"commandId":"cmd-1"}}`), &event))
		require.Equal(t, EventID("account-1:1"), event.ID)
		require.Equal(t, []byte(`{}`), event.Payload)
		require.Equal(t, CommandID("cmd-1"), *event.Metadata.CommandID)
		require.Nil(t, event.Metadata.CausationID)
		require.Nil(t, event.Metadata.Extensions)

		encoded, err := json.Marshal(event)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"causationId":"cmd-1","extensions":{}`)
		require.Nil(t, event.Metadata.CausationID)
		require.Nil(t, event.Metadata.Extensions)

		require.NoError(t, unmarshal([]byte(`null`), &event))
		require.Equal(t, EventID("account-1:1"), event.ID)
		require.Error(t, unmarshal([]byte(`{"payload":"not-base64!"}`), &event))
		require.Equal(t, []byte(`{}`), event.Payload)
		require.Error(t, unmarshal([]byte(`{}`), nil))
	}
}
