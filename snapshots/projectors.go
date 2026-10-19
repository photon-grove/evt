package snapshots

import (
	"context"
	"fmt"
	"slices"

	"github.com/photon-grove/evt"
)

// WithCommitProjectors registers commit-wide projectors (ADR 0004, C7). Each runs once per commit
// that writes at least one event — a single-aggregate command, a multi-stream Transaction, and a
// rejection that carries events alike — over everything the commit wrote, and the view operations
// it returns are committed atomically with the events. They are appended to any already
// registered, so declaration order is call order; within a commit a projector's operations follow
// the earlier projectors' and keep their emission order.
//
// Projectors run after every stream of the commit is staged and its snapshot decided, and again
// each time fence-aware retry re-prepares the command: the operations of a failed attempt are
// discarded with the rest of it. A command that writes no event (a plain rejection, an accepted
// no-op) runs none. A projector must be a pure function of its evt.CommitView; see
// evt.CommitProjector.
//
// The store's repository must write view operations (evt.ViewWritingRepository) and have somewhere
// to put them — configure the DynamoDB repository with WithViewsTable, the PostgreSQL repository
// with WithViewWriter — or the commit fails closed, with *evt.ViewsUnsupportedError or
// *evt.ViewsNotConfiguredError, before any write.
//
// A projector whose History is not empty reads events, and a rebuild replays the commits it read:
// before the first commit that runs it, the store durably adds those entity types to the feature
// manifest's historyDependent set (the repository must implement evt.HistoryDeclarer, or the commit
// fails with *evt.HistoryDeclarationUnsupportedError), so CompactBelow refuses them in every
// process. A projector with no History is given no events at all.
//
// Like the other With options it must be called during Store initialization, before concurrent
// use.
func (store *Store) WithCommitProjectors(projectors ...evt.CommitProjector) *Store {
	store.commitProjectors = append(store.commitProjectors, projectors...)

	return store
}

// projectCommit runs the commit-wide projectors over the events of one commit, with each written
// stream's post-commit state. It returns nothing when no projector is registered.
func (store *Store) projectCommit(
	ctx context.Context,
	events []evt.SerializedEvent,
	states map[evt.EntityID][]byte,
) ([]evt.ProjectedView, error) {
	if len(store.commitProjectors) == 0 || len(events) == 0 {
		return nil, nil
	}

	if !evt.WritesViews(store.repo) {
		return nil, &evt.ViewsUnsupportedError{Repository: fmt.Sprintf("%T", store.repo)}
	}

	if err := store.declareHistory(ctx); err != nil {
		return nil, err
	}

	return evt.ProjectCommit(ctx, store.commitProjectors, evt.NewCommitView(events, states))
}

// declareHistory durably adds the entity types the projectors read events of to the manifest's
// historyDependent set, once per type per store, before the first commit that runs them.
func (store *Store) declareHistory(ctx context.Context) error {
	var types []evt.EntityType

	for _, projector := range store.commitProjectors {
		if projector != nil {
			types = append(types, projector.History()...)
		}
	}

	if len(types) == 0 {
		return nil
	}

	store.historyMu.Lock()
	defer store.historyMu.Unlock()

	var pending []evt.EntityType

	for _, entityType := range types {
		if _, done := store.historyDeclared[entityType]; !done && !slices.Contains(pending, entityType) {
			pending = append(pending, entityType)
		}
	}

	if len(pending) == 0 {
		return nil
	}

	declarer, ok := store.repo.(evt.HistoryDeclarer)
	if !ok {
		return &evt.HistoryDeclarationUnsupportedError{Repository: fmt.Sprintf("%T", store.repo)}
	}

	if err := declarer.DeclareHistoryDependent(ctx, pending...); err != nil {
		return fmt.Errorf("snapshots: declaring history-dependent entity types: %w", err)
	}

	if store.historyDeclared == nil {
		store.historyDeclared = map[evt.EntityType]struct{}{}
	}

	for _, entityType := range pending {
		store.historyDeclared[entityType] = struct{}{}
	}

	return nil
}

// commitState returns an entity's state as the commit-wide projectors see it, only when any are
// registered.
func (store *Store) commitState(entity evt.Entity) ([]byte, error) {
	if len(store.commitProjectors) == 0 {
		return nil, nil
	}

	return generateSnapshotPayload(entity)
}

// stateAfterCommit returns the state of an entity that already reflects a commit's events: the
// payload of the snapshot the commit writes for it, else its current JSON encoding. The snapshot
// is the same bytes, so a stream's state is encoded once. It returns nothing when no commit-wide
// projector is registered.
func (store *Store) stateAfterCommit(entity evt.Entity, snapshots []evt.SerializedSnapshot) ([]byte, error) {
	if len(store.commitProjectors) == 0 {
		return nil, nil
	}

	if len(snapshots) > 0 {
		return snapshots[0].Payload, nil
	}

	return generateSnapshotPayload(entity)
}
