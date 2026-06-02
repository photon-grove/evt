package dynamo

import "github.com/photon-grove/evt"

// Snapshot is a struct to serialize a Snapshot for DynamoDB.
// Snapshots are stored inline in the event-log table at sk=0 for each entity.
type Snapshot struct {
	PK            evt.EntityID      `json:"pk"`         // the entity id is the partition key
	SK            evt.EventSequence `json:"sk"`         // always 0 for inline snapshots
	Sequence      evt.EventSequence `json:"seq"`        // the snapshot sequence number
	EventSequence evt.EventSequence `json:"eventSeq"`   // the last event sequence at the time of snapshotting
	EntityType    evt.EntityType    `json:"entityType"` // the entity type
	Payload       string            `json:"payload"`    // JSON-serialized entity state
}
