package snapshots

import (
	"context"

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
// The store's repository must be able to write view operations — configure the DynamoDB
// repository with WithViewsTable, the PostgreSQL repository with WithViewWriter — or the commit
// fails closed with *evt.ViewsNotConfiguredError before any write. Like the other With options it
// must be called during Store initialization, before concurrent use.
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
	if len(store.commitProjectors) == 0 {
		return nil, nil
	}

	return evt.ProjectCommit(ctx, store.commitProjectors, evt.NewCommitView(events, states))
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
