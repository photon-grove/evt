package evt

import (
	"encoding/json"
	"fmt"
)

// SerializedEvent is a common event format that is ready to be committed to an Event Store and
// streamed to downstream event listeners. It owns its JSON encoding: encoding/json and
// MarshalEvent use the canonical storage format, including encode-time metadata defaults.
// Metadata itself has no JSON methods, so embedding it in application DTOs preserves sibling fields.
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

// MarshalJSON encodes the event using the shared storage and stream format.
func (e SerializedEvent) MarshalJSON() ([]byte, error) {
	return MarshalEvent(e)
}

// UnmarshalJSON decodes the event without applying encode-time metadata defaults.
func (e *SerializedEvent) UnmarshalJSON(data []byte) error {
	return UnmarshalEvent(data, e)
}

// SerializedResult is similar to CommandResult but uses SerializedEvents instead. A multi-stream
// commit (ADR 0004, C1) appends to any number of streams in one result: Events carries every
// written stream's events, Snapshots the per-stream snapshot puts, and Fences the read fences
// for read-only streams (C2). The zero value keeps the single-stream shape.
//
// Outcome, when set (ADR 0004, C3), is the command outcome row written once, in its final state,
// inside the same transaction. A commit may carry an Outcome and no Events: an accepted no-op or a
// persisted rejection that carries no events is an outcome-only commit. Only a Repository that implements
// OutcomeRepository honors it; Outcome.EventIDs is derived from Events by NormalizeCommit.
type SerializedResult struct {
	Events      []SerializedEvent    `json:"events"`
	Snapshots   []SerializedSnapshot `json:"snapshots,omitempty"`
	Fences      []ReadFence          `json:"fences,omitempty"`
	Outcome     *Outcome             `json:"outcome,omitempty"`
	Transaction Transaction          `json:"transactions"`

	// Views are the commit-wide projectors' view operations (ADR 0004, C7), in the order the commit
	// writes them: projector declaration order, each projector's in emission order. They follow the
	// operations of Transaction's groups. A repository that cannot write them fails closed with
	// *ViewsNotConfiguredError; a commit that carries them and no event is refused with
	// *ProjectorOnlyCommitError.
	Views []ProjectedView `json:"views,omitempty"`
}

// SerializedSnapshot represents a snapshot of current Aggregate state that is ready
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

// SerializeEventsWithCommandContext serializes Events with the Aggregate within the given CommandContext.
func SerializeEventsWithCommandContext(events []Event, cc *CommandContext, metadata Metadata) ([]SerializedEvent, error) {
	if cc == nil {
		return nil, fmt.Errorf("context not found")
	}

	// Initialize the sequence with the last one from the context, but dereference
	// it so that the sequence in the context is not iterated, just this inner value.
	sequence := *cc.CurrentSequence

	return SerializeEvents(
		events,
		sequence,
		cc.EntityID,
		metadata,
	)
}

// SerializeResult creates a SerializedResult from events using the given context and entity.
// This is a convenience function for testing and integration scenarios.
func SerializeResult(
	cc CommandContext,
	aggregate Aggregate,
	events []Event,
	transaction Transaction,
) (SerializedResult, error) {
	var entityID EntityID
	if aggregate != nil {
		entityID = aggregate.GetID()
	}
	if cc.EntityID != "" {
		entityID = cc.EntityID
	}

	sequence := EventSequence(0)
	if cc.CurrentSequence != nil {
		sequence = *cc.CurrentSequence
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

// DeserializeEvent decodes Events with the Aggregate within the given CommandContext, applying upcasters.
func DeserializeEvent(serializedEvent SerializedEvent, aggregate Aggregate) (Event, error) {
	event := serializedEvent

	upcasters := aggregate.EventUpcasters()

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

	deserializedEvent, err := aggregate.DeserializeEvent(event)
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
