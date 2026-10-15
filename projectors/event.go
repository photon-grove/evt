package projectors

import (
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/internal/eventjson"
)

// ToSerializedEvent converts a DynamoDB stream record shape into an evt
// SerializedEvent. Invalid metadata is treated as empty metadata so projection
// code can still process the event payload.
func ToSerializedEvent(rec StreamRecord) evt.SerializedEvent {
	var meta evt.Metadata
	if len(rec.Metadata) > 0 {
		if err := eventjson.UnmarshalMetadata(rec.Metadata, &meta); err != nil {
			meta = evt.Metadata{}
		}
	}

	version := evt.EventVersion(rec.Version)
	if version <= 0 {
		version = evt.EventVersion(1)
	}

	return evt.SerializedEvent{
		ID:         evt.EventID(rec.EventID),
		EntityID:   evt.EntityID(rec.EntityID),
		EntityType: evt.EntityType(rec.EntityType),
		Sequence:   evt.EventSequence(rec.Sequence),
		Type:       evt.EventType(rec.EventType),
		Version:    version,
		Payload:    rec.Payload,
		Metadata:   meta,
	}
}
