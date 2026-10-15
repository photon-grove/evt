// Package eventjson encodes framework events using the shared storage format without adding
// JSON marshaler methods to the public data model.
package eventjson

import (
	"encoding/json"

	"github.com/photon-grove/evt"
)

type metadataWire evt.Metadata

func metadataToWire(metadata evt.Metadata) metadataWire {
	wire := metadataWire(metadata)
	if wire.CausationID == nil {
		wire.CausationID = wire.CommandID
	}
	if wire.Extensions == nil {
		wire.Extensions = map[string]string{}
	}

	return wire
}

// MarshalMetadata writes effective causation and an extensions object, with the field and map-key
// order pinned by the storage format. It does not mutate the caller's metadata.
func MarshalMetadata(metadata evt.Metadata) ([]byte, error) {
	return json.Marshal(metadataToWire(metadata))
}

// UnmarshalMetadata preserves stored values; defaults are applied only when encoding.
func UnmarshalMetadata(data []byte, metadata *evt.Metadata) error {
	return json.Unmarshal(data, (*metadataWire)(metadata))
}

type eventWire struct {
	ID         evt.EventID       `json:"id"`
	Sequence   evt.EventSequence `json:"sequence"`
	Type       evt.EventType     `json:"type"`
	Version    evt.EventVersion  `json:"version"`
	EntityID   evt.EntityID      `json:"entityId"`
	EntityType evt.EntityType    `json:"entityType"`
	Payload    []byte            `json:"payload"`
	Metadata   metadataWire      `json:"metadata"`
}

// MarshalEvent writes a serialized event with storage-format metadata.
func MarshalEvent(event evt.SerializedEvent) ([]byte, error) {
	return json.Marshal(eventWire{
		ID:         event.ID,
		Sequence:   event.Sequence,
		Type:       event.Type,
		Version:    event.Version,
		EntityID:   event.EntityID,
		EntityType: event.EntityType,
		Payload:    event.Payload,
		Metadata:   metadataToWire(event.Metadata),
	})
}

// UnmarshalEvent decodes the wire envelope without applying encode-time metadata defaults.
func UnmarshalEvent(data []byte, event *evt.SerializedEvent) error {
	wire := eventWire{
		ID:         event.ID,
		Sequence:   event.Sequence,
		Type:       event.Type,
		Version:    event.Version,
		EntityID:   event.EntityID,
		EntityType: event.EntityType,
		Payload:    event.Payload,
		Metadata:   metadataWire(event.Metadata),
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}

	*event = evt.SerializedEvent{
		ID:         wire.ID,
		Sequence:   wire.Sequence,
		Type:       wire.Type,
		Version:    wire.Version,
		EntityID:   wire.EntityID,
		EntityType: wire.EntityType,
		Payload:    wire.Payload,
		Metadata:   evt.Metadata(wire.Metadata),
	}

	return nil
}
