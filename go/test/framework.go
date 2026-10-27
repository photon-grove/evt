package test

import (
	"context"

	evt "github.com/photon-grove/evt/go"
)

// Framework provides utilities for testing event-sourced entities
type Framework struct {
	repo     evt.Repository
	store    evt.Store
	factory  func() evt.Aggregate
	id       evt.EntityID
	sequence evt.EventSequence
}

// NewFramework creates a new testing framework for event-sourced entities.
func NewFramework(repo evt.Repository, store evt.Store, factory func() evt.Aggregate, id evt.EntityID) *Framework {
	return NewFrameworkWithFactory(repo, store, factory, id)
}

// NewFrameworkWithFactory creates a new testing framework from an aggregate constructor.
func NewFrameworkWithFactory(
	repo evt.Repository,
	store evt.Store,
	factory func() evt.Aggregate,
	id evt.EntityID,
) *Framework {
	return &Framework{repo: repo, store: store, factory: factory, id: id, sequence: 0}
}

// WithEvents applies the given events to the entity and persists them to the repository
func (f *Framework) WithEvents(ctx context.Context, events []evt.Event) (evt.Aggregate, error) {
	serializedEvents, err := evt.SerializeEvents(events, f.sequence, f.id, evt.Metadata{})
	if err != nil {
		return nil, err
	}

	err = f.repo.Commit(ctx, evt.SerializedResult{Events: serializedEvents})
	if err != nil {
		return nil, err
	}

	f.sequence += evt.EventSequence(len(events))

	return f.Load(ctx)
}

// Execute runs the given command against the entity and persists any resulting events to the repository
func (f *Framework) Execute(ctx context.Context, cmd evt.Command, metadata evt.Metadata) (evt.Aggregate, error) {
	if err := f.store.Execute(ctx, f.factory(), f.id, cmd, metadata); err != nil {
		return nil, err
	}

	return f.Load(ctx)
}

// Load retrieves the current state of the entity from the store
func (f *Framework) Load(ctx context.Context) (evt.Aggregate, error) {
	aggregate := f.factory()

	if _, err := f.store.LoadAggregate(ctx, aggregate, f.id); err != nil {
		return nil, err
	}

	return aggregate, nil
}
