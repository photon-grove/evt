package snapshots

import (
	"encoding/json"

	"github.com/photon-grove/evt"
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
func applyEventsToEntity(serializedEvents []evt.SerializedEvent, eventContext *evt.Context) error {
	for _, serializedEvent := range serializedEvents {
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

// applyEventsForSnapshot applies events for snapshot creation up to a sequence limit.
func applyEventsForSnapshot(serializedEvents []evt.SerializedEvent, eventContext *evt.Context, commitSnapshotToEvent int) error {
	for i, serializedEvent := range serializedEvents {
		// Convert from the 0-indexed i value to the 1-indexed sequence value
		seq := i + 1

		event, err := evt.DeserializeEvent(serializedEvent, eventContext.Entity)
		if err != nil {
			return err
		}

		// If this is one of the Events that should be included in the Snapshot, apply the
		// event to the Entity
		if seq <= commitSnapshotToEvent {
			if err := eventContext.Entity.Apply(event); err != nil {
				return err
			}

			*eventContext.CurrentSequence++
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
