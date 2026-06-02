package mem

// WARNING: Not intended for use in Production

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
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

// StreamAllEvents streams all Events
func (repo Repository) StreamAllEvents(
	_ context.Context,
	_ *expression.Expression,
) <-chan result.Result[[]evt.SerializedEvent] {
	channel := make(chan result.Result[[]evt.SerializedEvent])

	go func() {
		defer close(channel)

		for _, events := range repo.events {
			channel <- result.Ok(events)
		}
	}()

	return channel
}

// StreamEntities streams all collected Entities
func (repo Repository) StreamEntities(
	ctx context.Context,
	_ *expression.Expression,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
) <-chan result.Result[evt.Entity] {
	channel := make(chan result.Result[evt.Entity])

	go func() {
		defer close(channel)

		var entity evt.Entity
		var err error
		entityEvents := 0

		for _, serialized := range repo.events {
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

		// Yield the final Entity to the channel
		channel <- result.Ok(entity)
	}()

	return channel
}

// applyTransactions is a no-op for the in-memory repository, enabling projector tests to run without DynamoDB.
func (repo Repository) applyTransactions(_ evt.Transaction) error {
	return nil
}
