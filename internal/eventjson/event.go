// Package eventjson encodes framework events and standalone metadata using the shared storage
// format. SerializedEvent owns its JSON encoding; Metadata has no JSON methods.
package eventjson

import (
	"encoding/json"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/internal/jsondefaults"
)

// MarshalMetadata writes effective causation and an extensions object, with the field and map-key
// order pinned by the storage format. It does not mutate the caller's metadata.
func MarshalMetadata(metadata evt.Metadata) ([]byte, error) {
	metadata.CausationID, metadata.Extensions = jsondefaults.Metadata(
		metadata.CommandID, metadata.CausationID, metadata.Extensions,
	)

	return json.Marshal(metadata)
}

// UnmarshalMetadata preserves stored values; defaults are applied only when encoding.
func UnmarshalMetadata(data []byte, metadata *evt.Metadata) error {
	return json.Unmarshal(data, metadata)
}

// MarshalEvent writes a serialized event with storage-format metadata.
func MarshalEvent(event evt.SerializedEvent) ([]byte, error) {
	return evt.MarshalEvent(event)
}

// UnmarshalEvent decodes the wire envelope without applying encode-time metadata defaults.
func UnmarshalEvent(data []byte, event *evt.SerializedEvent) error {
	return evt.UnmarshalEvent(data, event)
}
