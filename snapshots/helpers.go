package snapshots

import (
	"encoding/json"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
)

// createContext returns a new event context with the given entity and sequences.
func createContext(entity evt.Entity, entityID evt.EntityID, currentSequence, currentSnapshot *evt.EventSequence) evt.Context {
	return evt.Context{
		Entity:          entity,
		EntityID:        entityID,
		CurrentSequence: currentSequence,
		CurrentSnapshot: currentSnapshot,
	}
}

// createInitialContext returns an event context with zero sequences for new entities.
func createInitialContext(entity evt.Entity, entityID evt.EntityID) evt.Context {
	currentSequence := evt.EventSequence(0)
	currentSnapshot := evt.EventSequence(0)

	return createContext(entity, entityID, &currentSequence, &currentSnapshot)
}

// applyEventsToEntity applies events to an entity and updates the current sequence.
// It also records any CommandIDs found in event metadata for dedupliation.
//
// Sequences must continue from the context's current sequence without a gap. A gap means another
// writer snapshotted and compacted the stream between reading the snapshot and reading the events,
// or events expired. Applying events across it would build the wrong state, so it returns a
// conflict classified as transient: a caller that retries the command reloads a fresh entity, and
// evt.IsConflictErr still matches it.
func applyEventsToEntity(serializedEvents []evt.SerializedEvent, eventContext *evt.Context) error {
	for _, serializedEvent := range serializedEvents {
		var current evt.EventSequence
		if eventContext.CurrentSequence != nil {
			current = *eventContext.CurrentSequence
		}
		if serializedEvent.Sequence != current+1 {
			return &policy.ClassifiedError{Class: policy.ClassTransient, Err: &evt.SequenceGapError{
				EntityID: eventContext.EntityID,
				After:    current,
				Found:    serializedEvent.Sequence,
			}}
		}

		event, err := evt.DeserializeEvent(serializedEvent, eventContext.Entity)
		if err != nil {
			return err
		}

		seq := serializedEvent.Sequence
		eventContext.CurrentSequence = &seq

		// Track CommandIDs for the dedupe guard
		if serializedEvent.Metadata.CommandID != nil {
			eventContext.RecordCommandID(*serializedEvent.Metadata.CommandID)
		}

		// Apply each Event to the Entity, to build up the current state
		if err = eventContext.Entity.Apply(event); err != nil {
			return err
		}
	}

	return nil
}

// updateSnapshotSequence sets or increments the snapshot sequence in the context.
func updateSnapshotSequence(eventContext *evt.Context) {
	if eventContext.CurrentSnapshot == nil {
		one := evt.EventSequence(1)
		eventContext.CurrentSnapshot = &one
	} else {
		*eventContext.CurrentSnapshot++
	}
}

// generateSnapshotPayload marshals the current entity state to JSON.
func generateSnapshotPayload(entity evt.Entity) ([]byte, error) {
	snapshot, err := json.Marshal(entity)
	if err != nil {
		return nil, err
	}

	return snapshot, nil
}
