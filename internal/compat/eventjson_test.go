package compat

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
)

func TestSerializedEventPublicJSON(t *testing.T) {
	data, err := os.ReadFile(FixturePath(CodecFixtureFile))
	require.NoError(t, err)

	var fixtures CodecFixtures
	require.NoError(t, json.Unmarshal(data, &fixtures))

	expected := make(map[string]string, len(fixtures.Events))
	for _, vector := range fixtures.Events {
		expected[vector.Name] = vector.SerializedEventJSON
	}

	for _, c := range codecEvents() {
		t.Run(c.name, func(t *testing.T) {
			canonical, ok := expected[c.name]
			require.True(t, ok, "missing canonical fixture")
			metadataBefore, err := json.Marshal(c.event.Metadata)
			require.NoError(t, err)

			for _, marshal := range []func(evt.SerializedEvent) ([]byte, error){
				func(event evt.SerializedEvent) ([]byte, error) { return json.Marshal(event) },
				evt.MarshalEvent,
			} {
				encoded, marshalErr := marshal(c.event)
				require.NoError(t, marshalErr)
				require.Equal(t, canonical, string(encoded))
			}

			for _, unmarshal := range []func([]byte, *evt.SerializedEvent) error{
				func(data []byte, event *evt.SerializedEvent) error { return json.Unmarshal(data, event) },
				evt.UnmarshalEvent,
			} {
				var decoded evt.SerializedEvent
				require.NoError(t, unmarshal([]byte(canonical), &decoded))
				require.True(t, c.event.Metadata.Equal(decoded.Metadata))
				encoded, marshalErr := json.Marshal(decoded)
				require.NoError(t, marshalErr)
				require.Equal(t, canonical, string(encoded))
			}

			// The same encoding must apply when an event is nested in a public result.
			encoded, err := json.Marshal(evt.SerializedResult{Events: []evt.SerializedEvent{c.event}})
			require.NoError(t, err)
			var result struct {
				Events []json.RawMessage `json:"events"`
			}
			require.NoError(t, json.Unmarshal(encoded, &result))
			require.Len(t, result.Events, 1)
			require.Equal(t, canonical, string(result.Events[0]))

			metadataAfter, err := json.Marshal(c.event.Metadata)
			require.NoError(t, err)
			require.Equal(t, string(metadataBefore), string(metadataAfter))
		})
	}

	for _, vector := range fixtures.EventDecode {
		t.Run("decode/"+vector.Name, func(t *testing.T) {
			var event evt.SerializedEvent
			require.NoError(t, json.Unmarshal([]byte(vector.Input), &event))
			encoded, err := evt.MarshalEvent(event)
			require.NoError(t, err)
			require.Equal(t, vector.Canonical, string(encoded))
		})
	}
}
