package evt

import (
	"encoding/json"
	"fmt"
)

// A SerializedEvent is a common event format that is ready to be committed to an Event Store and
// streamed to downstream event listeners
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

// SerializeEvents prepares an Event for serialization and storage
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

// SerializeEventsWithContext serializes Events with the Entity within the given Context
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

// DeserializeEvent decodes Events with the Entity within the given Context, applying upcasters
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

// CalculateAdditionalEvents calculates the number of additional events that can be applied
// after the next snapshot is taken
func CalculateAdditionalEvents(currentSequence EventSequence, numEvents int, maxSize int) int {
	nextSnapshotAt := maxSize - (int(currentSequence) % maxSize)
	if numEvents < nextSnapshotAt {
		return 0
	}

	eventsAfterNextSnapshot := numEvents - nextSnapshotAt
	eventsAfterNextSnapshotToApply := eventsAfterNextSnapshot - (eventsAfterNextSnapshot & maxSize)

	return nextSnapshotAt + eventsAfterNextSnapshotToApply
}
