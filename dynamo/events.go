package dynamo

import "github.com/photon-grove/evt"

// Event is a struct to serialize an Event for DynamoDB
type Event struct {
	PK         evt.EntityID      `json:"pk"` // the partition key is the entity ID
	SK         evt.EventSequence `json:"sk"` // the sort key is based on the event sequence
	Type       evt.EventType     `json:"type"`
	Version    evt.EventVersion  `json:"version"`
	EntityType evt.EntityType    `json:"entityType"`
	Payload    string            `json:"payload"`
	Metadata   string            `json:"metadata"`
}
