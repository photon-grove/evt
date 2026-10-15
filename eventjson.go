package evt

import (
	"encoding/json"

	"github.com/photon-grove/evt/internal/jsondefaults"
)

// eventWire retains the envelope's fields without its JSON methods to avoid recursion.
type eventWire SerializedEvent

// MarshalEvent encodes a serialized event in the shared storage and stream format. It writes
// effective causation and an extensions object without mutating the caller's metadata.
func MarshalEvent(event SerializedEvent) ([]byte, error) {
	wire := eventWire(event)
	wire.Metadata.CausationID, wire.Metadata.Extensions = jsondefaults.Metadata(
		wire.Metadata.CommandID, wire.Metadata.CausationID, wire.Metadata.Extensions,
	)

	return json.Marshal(wire)
}

// UnmarshalEvent decodes a serialized event without applying encode-time metadata defaults.
// Missing fields retain the destination's values, matching encoding/json.
func UnmarshalEvent(data []byte, event *SerializedEvent) error {
	if event == nil {
		return json.Unmarshal(data, event)
	}

	wire := eventWire(*event)
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}

	*event = SerializedEvent(wire)

	return nil
}
