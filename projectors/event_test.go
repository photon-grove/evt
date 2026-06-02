package projectors_test

import (
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/projectors"
	"github.com/stretchr/testify/require"
)

func TestToSerializedEvent(t *testing.T) {
	commandID := evt.CommandID("cmd-1")
	serialized := projectors.ToSerializedEvent(projectors.StreamRecord{
		EventID:    "evt-1",
		EntityID:   "entity-1",
		EntityType: "entity",
		EventType:  "entity:created",
		Version:    3,
		Sequence:   7,
		Payload:    []byte(`{"ok":true}`),
		Metadata:   []byte(`{"commandId":"cmd-1"}`),
	})

	require.Equal(t, evt.EventID("evt-1"), serialized.ID)
	require.Equal(t, evt.EntityID("entity-1"), serialized.EntityID)
	require.Equal(t, evt.EntityType("entity"), serialized.EntityType)
	require.Equal(t, evt.EventType("entity:created"), serialized.Type)
	require.Equal(t, evt.EventVersion(3), serialized.Version)
	require.Equal(t, evt.EventSequence(7), serialized.Sequence)
	require.Equal(t, commandID, *serialized.Metadata.CommandID)
	require.JSONEq(t, `{"ok":true}`, string(serialized.Payload))
}

func TestToSerializedEventDefaultsVersionAndIgnoresInvalidMetadata(t *testing.T) {
	serialized := projectors.ToSerializedEvent(projectors.StreamRecord{
		EventID:  "evt-1",
		Metadata: []byte(`{bad-json`),
	})

	require.Equal(t, evt.EventVersion(1), serialized.Version)
	require.Nil(t, serialized.Metadata.CommandID)
}

func TestToSerializedEventDefaultsNegativeVersion(t *testing.T) {
	serialized := projectors.ToSerializedEvent(projectors.StreamRecord{
		EventID: "evt-1",
		Version: -3,
	})

	require.Equal(t, evt.EventVersion(1), serialized.Version)
}
