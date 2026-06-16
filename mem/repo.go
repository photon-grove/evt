package mem

// WARNING: Not intended for use in Production

import (
	"context"
	"fmt"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
)

// This Repository lives in-memory for testing or local development
type Repository struct {
	events    map[string][]evt.SerializedEvent
	snapshots map[string]evt.SerializedSnapshot
}

// NewRepository creates a new instance of the Repository
func NewRepository() evt.Repository {
	return &Repository{
		make(map[string][]evt.SerializedEvent),
		make(map[string]evt.SerializedSnapshot),
	}
}

// The in-memory repository also satisfies the snapshot-aware and head-streaming capabilities.
var (
	_ evt.SnapshotStreamer   = (*Repository)(nil)
	_ evt.EntityHeadStreamer = (*Repository)(nil)
)

// Commit Events to memory
func (repo Repository) Commit(
	_ context.Context,
	result evt.SerializedResult,
) error {
	for _, event := range result.Events {
		id := string(event.EntityID)

		if repo.events[id] == nil {
			var empty []evt.SerializedEvent
			repo.events[id] = empty
		}

		repo.events[id] = append(repo.events[id], event)
	}

	return repo.applyTransactions(result.Transaction)
}

// CommitStream streams Events to memory
func (repo Repository) CommitStream(
	_ context.Context,
	channel <-chan result.Result[evt.SerializedResult],
) []error {
	var errors []error

	for result := range channel {
		serializedResult, err := result.Unwrap()
		if err != nil {
			errors = append(errors, err)
			continue
		}

		for _, event := range serializedResult.Events {
			id := string(event.EntityID)

			if repo.events[id] == nil {
				var empty []evt.SerializedEvent
				repo.events[id] = empty
			}

			repo.events[id] = append(repo.events[id], event)
		}

		if err := repo.applyTransactions(serializedResult.Transaction); err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

// CommitWithSnapshot commits Events with a Snapshot
func (repo Repository) CommitWithSnapshot(
	ctx context.Context,
	result evt.SerializedResult,
	entityType evt.EntityType,
	entityID evt.EntityID,
	payload []byte,
	currentSnapshot evt.EventSequence,
) error {
	if err := repo.Commit(ctx, result); err != nil {
		return err
	}

	currentSequence := result.Events[len(result.Events)-1].Sequence

	id := string(entityID)

	snapshot := evt.SerializedSnapshot{
		EntityType:    entityType,
		EntityID:      entityID,
		Sequence:      currentSnapshot,
		EventSequence: currentSequence,
		Payload:       payload,
	}

	repo.snapshots[id] = snapshot

	return nil
}

// GetEvents gets all Events
func (repo Repository) GetEvents(
	_ context.Context,
	entityID evt.EntityID,
) ([]evt.SerializedEvent, error) {
	id := string(entityID)

	if events, ok := repo.events[id]; ok {
		return events, nil
	}

	return []evt.SerializedEvent{}, nil
}

// GetLatestEvents gets all Events after a particular sequence
func (repo Repository) GetLatestEvents(
	_ context.Context,
	entityID evt.EntityID,
	lastSequence evt.EventSequence,
) ([]evt.SerializedEvent, error) {
	id := string(entityID)

	latestEvents := repo.events[id]

	latest := make([]evt.SerializedEvent, 0)
	for _, event := range latestEvents {
		if event.Sequence > lastSequence {
			latest = append(latest, event)
		}
	}

	return latest, nil
}

// GetSnapshot gets a Snapshot
func (repo Repository) GetSnapshot(
	_ context.Context,
	entityID evt.EntityID,
) (*evt.SerializedSnapshot, error) {
	id := string(entityID)

	if snapshot, ok := repo.snapshots[id]; ok {
		return &snapshot, nil
	}

	return nil, nil
}

// StreamAllEvents streams all Events, honoring the StreamFilter's entity-type narrowing
// client-side (the in-memory repository has no server-side filter to push down to).
func (repo Repository) StreamAllEvents(
	_ context.Context,
	filter evt.StreamFilter,
) <-chan result.Result[[]evt.SerializedEvent] {
	channel := make(chan result.Result[[]evt.SerializedEvent])

	go func() {
		defer close(channel)

		for id, events := range repo.events {
			if !filter.Matches(memPartitionType(events, repo.snapshots[id])) {
				continue
			}

			channel <- result.Ok(events)
		}
	}()

	return channel
}

// StreamEntityHeads implements evt.EntityHeadStreamer over the in-memory log. The head is the
// larger of the highest stored event sequence and the snapshot's recorded EventSequence, so it
// stays correct for streams whose early events were compacted away. entityType, when non-empty,
// restricts the result to that type.
func (repo Repository) StreamEntityHeads(
	_ context.Context,
	entityType evt.EntityType,
) (map[evt.EntityID]evt.EventSequence, error) {
	heads := make(map[evt.EntityID]evt.EventSequence)

	for id, events := range repo.events {
		snapshot := repo.snapshots[id]
		if entityType != "" && !memEventsMatchType(events, snapshot, entityType) {
			continue
		}

		var head evt.EventSequence
		for _, event := range events {
			if event.Sequence > head {
				head = event.Sequence
			}
		}
		if snapshot.EventSequence > head {
			head = snapshot.EventSequence
		}

		heads[evt.EntityID(id)] = head
	}

	// Entities present only as a snapshot (events compacted away with no event key) still have a head.
	for id, snapshot := range repo.snapshots {
		if _, seen := heads[evt.EntityID(id)]; seen {
			continue
		}
		if entityType != "" && snapshot.EntityType != entityType {
			continue
		}

		heads[evt.EntityID(id)] = snapshot.EventSequence
	}

	return heads, nil
}

// CompactBelow deletes events for an entity whose sequence is in [1, throughSequence], but only
// after verifying a durable snapshot covers (>=) throughSequence. It implements evt.Compactor.
func (repo Repository) CompactBelow(
	_ context.Context,
	entityID evt.EntityID,
	throughSequence evt.EventSequence,
) (int, error) {
	if throughSequence < 1 {
		return 0, nil
	}

	id := string(entityID)

	snapshot, ok := repo.snapshots[id]
	if !ok {
		return 0, fmt.Errorf("%w: entity %s has no durable snapshot", evt.ErrCompactionUncovered, entityID)
	}
	if snapshot.EventSequence < throughSequence {
		return 0, fmt.Errorf(
			"%w: entity %s snapshot covers through event %d but compaction was requested through %d",
			evt.ErrCompactionUncovered, entityID, snapshot.EventSequence, throughSequence,
		)
	}

	events := repo.events[id]
	kept := make([]evt.SerializedEvent, 0, len(events))
	deleted := 0

	for _, event := range events {
		if event.Sequence >= 1 && event.Sequence <= throughSequence {
			deleted++
			continue
		}

		kept = append(kept, event)
	}

	repo.events[id] = kept

	return deleted, nil
}

// StreamEntitiesFromSnapshots streams collected Entities, seeding each from its snapshot (when
// present) before applying post-snapshot events. entityType, when non-empty, restricts the stream
// to entities of that type. It implements evt.SnapshotStreamer.
func (repo Repository) StreamEntitiesFromSnapshots(
	ctx context.Context,
	entityType evt.EntityType,
	seedEntity evt.SnapshotSeeder,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
) <-chan result.Result[evt.Entity] {
	channel := make(chan result.Result[evt.Entity])

	go func() {
		defer close(channel)

		for id, serialized := range repo.events {
			if entityType != "" && !memEventsMatchType(serialized, repo.snapshots[id], entityType) {
				continue
			}

			entity, through, ok := repo.seedFromSnapshot(ctx, id, seedEntity, channel)
			if !ok {
				continue
			}

			entity, ok = applyMemEvents(ctx, serialized, entity, through, applyEvent, channel)
			if !ok {
				continue
			}

			if entity != nil {
				channel <- result.Ok(entity)
			}
		}
	}()

	return channel
}

// seedFromSnapshot reconstructs an entity from its snapshot when one exists. It returns the
// seeded entity (nil when there is no snapshot), the covered event sequence, and false only when
// seeding errored (the error is already emitted).
func (repo Repository) seedFromSnapshot(
	ctx context.Context,
	id string,
	seedEntity evt.SnapshotSeeder,
	channel chan<- result.Result[evt.Entity],
) (evt.Entity, evt.EventSequence, bool) {
	snapshot, ok := repo.snapshots[id]
	if !ok {
		return nil, 0, true
	}

	entity, err := seedEntity(ctx, snapshot)
	if err != nil {
		channel <- result.Err[evt.Entity](err)
		return nil, 0, false
	}

	return entity, snapshot.EventSequence, true
}

// memPartitionType reports a partition's entity type, reading the first non-snapshot event and
// falling back to the snapshot's recorded type when only a snapshot remains (events compacted away).
func memPartitionType(serialized []evt.SerializedEvent, snapshot evt.SerializedSnapshot) evt.EntityType {
	for _, event := range serialized {
		if event.Sequence == 0 {
			continue
		}

		return event.EntityType
	}

	return snapshot.EntityType
}

// memEventsMatchType reports whether a partition belongs to the given entity type.
func memEventsMatchType(serialized []evt.SerializedEvent, snapshot evt.SerializedSnapshot, entityType evt.EntityType) bool {
	return memPartitionType(serialized, snapshot) == entityType
}

// applyMemEvents applies events above the snapshot boundary to the (possibly seeded) entity.
func applyMemEvents(
	ctx context.Context,
	serialized []evt.SerializedEvent,
	entity evt.Entity,
	through evt.EventSequence,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
	channel chan<- result.Result[evt.Entity],
) (evt.Entity, bool) {
	for _, event := range serialized {
		if event.Sequence == 0 || event.Sequence <= through {
			continue
		}

		next, err := applyEvent(ctx, event, entity)
		if err != nil {
			channel <- result.Err[evt.Entity](err)
			return entity, false
		}

		entity = next
	}

	return entity, true
}

// StreamEntities streams all collected Entities, honoring the StreamFilter's entity-type narrowing
// client-side (the in-memory repository has no server-side filter to push down to).
func (repo Repository) StreamEntities(
	ctx context.Context,
	filter evt.StreamFilter,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
) <-chan result.Result[evt.Entity] {
	channel := make(chan result.Result[evt.Entity])

	go func() {
		defer close(channel)

		var entity evt.Entity
		var err error
		entityEvents := 0

		for id, serialized := range repo.events {
			if !filter.Matches(memPartitionType(serialized, repo.snapshots[id])) {
				continue
			}

			for _, event := range serialized {
				if event.Sequence == 0 {
					// This is a Snapshot, so skip it
					continue
				}

				if entity != nil && event.EntityID != entity.GetID() {
					// We've moved on to a new Entity. Process this one and reset the Entity
					// pointer back to nil.

					// Yield this finished Entity to the channel
					channel <- result.Ok(entity)

					entity = nil
					entityEvents = 0
				}

				entity, err = applyEvent(ctx, event, entity)
				if err != nil {
					channel <- result.Err[evt.Entity](err)

					continue
				}

				entityEvents++
			}
		}

		// Yield the final Entity to the channel. Guard against nil so a stream whose partitions were
		// all filtered out (or an empty repository) does not emit a spurious nil result.
		if entity != nil {
			channel <- result.Ok(entity)
		}
	}()

	return channel
}

// applyTransactions is a no-op for the in-memory repository, enabling projector tests to run without DynamoDB.
func (repo Repository) applyTransactions(_ evt.Transaction) error {
	return nil
}
