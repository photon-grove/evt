package evt

import (
	"encoding/json"
	"fmt"
)

// SerializedEvent is a common event format that is ready to be committed to an Event Store and
// streamed to downstream event listeners.
type SerializedEvent struct {
	ID         EventID       `json:"id"` // Composed of the EntityID and EventSequence
	Sequence   EventSequence `json:"sequence"`
	Type       EventType     `json:"type"`
	Version    EventVersion  `json:"version"`
	EntityID   EntityID      `json:"entityId"`
	EntityType EntityType    `json:"entityType"`
	Payload    []byte        `json:"payload"`
	Metadata   Metadata      `json:"metadata"`
}

// SerializedResult is similar to CommandResult but uses SerializedEvents instead.
type SerializedResult struct {
	Events      []SerializedEvent `json:"events"`
	Transaction Transaction       `json:"transactions"`
}

// SerializedSnapshot represents a snapshot of current Entity state that is ready
// to be committed to a Repository.
type SerializedSnapshot struct {
	EntityType    EntityType    `json:"entityType"`
	EntityID      EntityID      `json:"entityID"`
	Sequence      EventSequence `json:"sequence"`
	EventSequence EventSequence `json:"eventSequence"`
	Payload       []byte        `json:"payload"`
}

// EventUpcaster migrates a serialized event from an older version to a newer
// version before aggregate replay.
type EventUpcaster interface {
	CanUpcast(EventType, EventVersion) bool
	Upcast(SerializedEvent) (SerializedEvent, error)
}

// UpcastError wraps a failure that occurred while upcasting a serialized event.
type UpcastError struct {
	Err             error           `json:"err"`
	SerializedEvent SerializedEvent `json:"serializedEvent"`
}

// NewUpcastError creates a new UpcastError instance.
func NewUpcastError(err error, serializedEvent SerializedEvent) *UpcastError {
	return &UpcastError{
		Err:             err,
		SerializedEvent: serializedEvent,
	}
}

// Error returns the wrapped error message.
func (e *UpcastError) Error() string {
	if e == nil || e.Err == nil {
		return "upcast error"
	}

	return e.Err.Error()
}

// SerializeEvents prepares an Event for serialization and storage.
func SerializeEvents(
	events []Event,
	sequence EventSequence,
	entityID EntityID,
	metadata Metadata,
) ([]SerializedEvent, error) {
	serializedEvents := make([]SerializedEvent, 0, len(events))

	for _, event := range events {
		eventType := event.Type()
		version := event.Version()
		sequence++

		payload, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}

		serializedEvents = append(serializedEvents, SerializedEvent{
			ID:         GetEventID(entityID, sequence),
			EntityType: event.EntityType(),
			EntityID:   entityID,
			Sequence:   sequence,
			Type:       eventType,
			Version:    version,
			Payload:    payload,
			Metadata:   metadata,
		})
	}

	return serializedEvents, nil
}

// SerializeEventsWithContext serializes Events with the Entity within the given Context.
func SerializeEventsWithContext(events []Event, eventContext *Context, metadata Metadata) ([]SerializedEvent, error) {
	if eventContext == nil {
		return nil, fmt.Errorf("context not found")
	}

	// Initialize the sequence with the last one from the context, but dereference
	// it so that the sequence in the context is not iterated, just this inner value.
	sequence := *eventContext.CurrentSequence

	return SerializeEvents(
		events,
		sequence,
		eventContext.EntityID,
		metadata,
	)
}

// SerializeResult creates a SerializedResult from events using the given context and entity.
// This is a convenience function for testing and integration scenarios.
func SerializeResult(
	eventContext Context,
	entity Entity,
	events []Event,
	transaction Transaction,
) (SerializedResult, error) {
	var entityID EntityID
	if entity != nil {
		entityID = entity.GetID()
	}
	if eventContext.EntityID != "" {
		entityID = eventContext.EntityID
	}

	sequence := EventSequence(0)
	if eventContext.CurrentSequence != nil {
		sequence = *eventContext.CurrentSequence
	}

	serializedEvents, err := SerializeEvents(events, sequence, entityID, Metadata{})
	if err != nil {
		return SerializedResult{}, err
	}

	return SerializedResult{
		Events:      serializedEvents,
		Transaction: transaction,
	}, nil
}

// DeserializeEvent decodes Events with the Entity within the given Context, applying upcasters.
func DeserializeEvent(serializedEvent SerializedEvent, entity Entity) (Event, error) {
	event := serializedEvent

	upcasters := entity.EventUpcasters()

	for _, upcaster := range upcasters {
		if upcaster.CanUpcast(event.Type, event.Version) {
			upcasted, err := upcaster.Upcast(event)
			if err != nil {
				return nil, &ReplayStrictnessError{
					EventType: event.Type,
					Version:   event.Version,
					Phase:     "upcast",
					Err:       err,
				}
			}

			event = upcasted
		}
	}

	deserializedEvent, err := entity.DeserializeEvent(event)
	if err != nil {
		return nil, &ReplayStrictnessError{
			EventType: event.Type,
			Version:   event.Version,
			Phase:     "deserialize",
			Err:       err,
		}
	}

	return deserializedEvent, nil
}

// CalculateAdditionalEvents reports how many events from a commit batch should be folded into a
// snapshot, given the entity's current sequence, the number of events in the batch, and the
// snapshot interval (maxSize).
//
// It returns 0 when the batch does not reach the next snapshot boundary, meaning no snapshot is
// taken for this commit. Otherwise it returns numEvents: a snapshot is written capturing the
// entity state through the entire batch.
//
// The full batch is captured (rather than only the events up to the boundary) because both the
// in-memory and DynamoDB repositories record the snapshot's EventSequence as the last event in the
// batch. Capturing a partial batch would leave the snapshot payload inconsistent with that recorded
// sequence, so a reload would restore stale state. A maxSize of 0 or less disables snapshots.
func CalculateAdditionalEvents(currentSequence EventSequence, numEvents int, maxSize int) int {
	if maxSize <= 0 {
		return 0
	}

	nextSnapshotAt := maxSize - (int(currentSequence) % maxSize)
	if numEvents < nextSnapshotAt {
		return 0
	}

	return numEvents
}

// ByCommandID allows you to sort SerializedEvents by the CommandID in the Metadata, falling back to
// sorting by event ID if the CommandID is empty.
type ByCommandID []SerializedEvent

// Len returns the length of the slice.
func (events ByCommandID) Len() int {
	return len(events)
}

// Swap swaps the elements with indexes i and j.
func (events ByCommandID) Swap(i, j int) {
	events[i], events[j] = events[j], events[i]
}

// Less reports whether the element with index i should sort before the element with index j.
func (events ByCommandID) Less(i, j int) bool {
	// Try to see if both have CommandIDs
	if cmd := events[i].Metadata.CommandID; cmd != nil {
		if otherCmd := events[j].Metadata.CommandID; otherCmd != nil {
			return *cmd < *otherCmd
		}
	}

	// Fall back to the event IDs.
	return events[i].ID < events[j].ID
}
