package snapshots

import (
	"encoding/json"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/policy"
)

// createCommandContext returns a new CommandContext with the given entity and sequences.
func createCommandContext(aggregate evt.Aggregate, entityID evt.EntityID, currentSequence, currentSnapshot *evt.EventSequence) evt.CommandContext {
	return evt.CommandContext{
		Aggregate:       aggregate,
		EntityID:        entityID,
		CurrentSequence: currentSequence,
		CurrentSnapshot: currentSnapshot,
	}
}

// createInitialCommandContext returns a CommandContext with zero sequences for new entities.
func createInitialCommandContext(aggregate evt.Aggregate, entityID evt.EntityID) evt.CommandContext {
	currentSequence := evt.EventSequence(0)
	currentSnapshot := evt.EventSequence(0)

	return createCommandContext(aggregate, entityID, &currentSequence, &currentSnapshot)
}

// applyEventsToAggregate applies events to an entity and updates the current sequence.
// It also records any CommandIDs found in event metadata for dedupliation.
//
// Sequences must continue from the context's current sequence without a gap. A gap means another
// writer snapshotted and compacted the stream between reading the snapshot and reading the events,
// or events expired. Applying events across it would build the wrong state, so it returns a
// conflict classified as transient: a caller that retries the command reloads a fresh entity, and
// evt.IsConflictErr still matches it.
func applyEventsToAggregate(serializedEvents []evt.SerializedEvent, cc *evt.CommandContext) error {
	for _, serializedEvent := range serializedEvents {
		var current evt.EventSequence
		if cc.CurrentSequence != nil {
			current = *cc.CurrentSequence
		}
		if serializedEvent.Sequence != current+1 {
			return &policy.ClassifiedError{Class: policy.ClassTransient, Err: &evt.SequenceGapError{
				EntityID: cc.EntityID,
				After:    current,
				Found:    serializedEvent.Sequence,
			}}
		}

		event, err := evt.DeserializeEvent(serializedEvent, cc.Aggregate)
		if err != nil {
			return err
		}

		seq := serializedEvent.Sequence
		cc.CurrentSequence = &seq

		// Track CommandIDs for the dedupe guard
		if serializedEvent.Metadata.CommandID != nil {
			cc.RecordCommandID(*serializedEvent.Metadata.CommandID)
		}

		// Apply each Event to the Aggregate, to build up the current state
		if err = cc.Aggregate.Apply(event); err != nil {
			return err
		}
	}

	return nil
}

// updateSnapshotSequence sets or increments the snapshot sequence in the context.
func updateSnapshotSequence(cc *evt.CommandContext) {
	if cc.CurrentSnapshot == nil {
		one := evt.EventSequence(1)
		cc.CurrentSnapshot = &one
	} else {
		*cc.CurrentSnapshot++
	}
}

// generateSnapshotPayload marshals the current entity state to JSON.
func generateSnapshotPayload(aggregate evt.Aggregate) ([]byte, error) {
	snapshot, err := json.Marshal(aggregate)
	if err != nil {
		return nil, err
	}

	return snapshot, nil
}
