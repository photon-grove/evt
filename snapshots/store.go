package snapshots

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/photon-grove/evt"
)

// Store handles executing Commands, committing Events that are yielded by those Commands,
// and loading Entity instances from a Repository. It captures snapshots of the Entity state
// every N events, where N is the snapshot size.
type Store struct {
	repo                     evt.Repository
	snapshotSize             int
	snapshotOverrides        map[evt.EntityType]int
	catchUpSnapshotThreshold int

	// outcomes is non-nil when command outcomes are enabled (ADR 0004, C3); see WithOutcomes.
	outcomes *outcomeConfig

	// retry bounds fence-aware retry on a store with outcomes (ADR 0004, C5); see WithRetry.
	retry RetryPolicy

	// commitProjectors are the commit-wide projectors (ADR 0004, C7), in declaration order; see
	// WithCommitProjectors.
	commitProjectors []evt.CommitProjector

	// historyDeclared are the entity types this store has already durably declared
	// history-dependent for its projectors, so it declares each once.
	historyMu       sync.Mutex
	historyDeclared map[evt.EntityType]struct{}
}

// NewStore creates a new Store with the given Repository and Snapshot size
// (the target number of events between each Snapshot).
func NewStore(repo evt.Repository, snapshotSize int) *Store {
	return &Store{
		repo:              repo,
		snapshotSize:      snapshotSize,
		snapshotOverrides: make(map[evt.EntityType]int),
		retry:             DefaultRetryPolicy(),
	}
}

// WithCatchUpSnapshotThreshold enables best-effort snapshot catch-up when replay debt exceeds threshold.
func (store *Store) WithCatchUpSnapshotThreshold(threshold int) *Store {
	store.catchUpSnapshotThreshold = threshold
	return store
}

// WithSnapshotSize sets a per-entity-type snapshot interval override. The default
// snapshotSize is used for any entity type without an explicit override.
// Non-positive sizes remove the override so the default is used.
// Must be called during Store initialization before concurrent use.
func (store *Store) WithSnapshotSize(entityType evt.EntityType, size int) *Store {
	if size <= 0 {
		delete(store.snapshotOverrides, entityType)
		return store
	}
	if store.snapshotOverrides == nil {
		store.snapshotOverrides = make(map[evt.EntityType]int)
	}
	store.snapshotOverrides[entityType] = size
	return store
}

// effectiveSnapshotSize returns the snapshot interval for the given entity type,
// falling back to the store default when no override is configured.
func (store *Store) effectiveSnapshotSize(entityType evt.EntityType) int {
	if size, ok := store.snapshotOverrides[entityType]; ok {
		return size
	}
	return store.snapshotSize
}

// LoadEntity loads the given Entity instance with the given id, retrieving Snapshots
// and Events from the repository.
func (store *Store) LoadEntity(
	ctx context.Context,
	entity evt.Entity,
	entityID evt.EntityID,
) (evt.Context, error) {
	var eventContext evt.Context

	// Look for a Snapshot first
	snapshot, err := store.repo.GetSnapshot(ctx, entityID)
	if err != nil {
		return eventContext, err
	}

	var serializedEvents []evt.SerializedEvent

	if snapshot != nil {
		// If found, rehydrate the current state at the point the Snapshot captured it
		if err = json.Unmarshal(snapshot.Payload, entity); err != nil {
			return eventContext, err
		}

		eventContext = createContext(entity, entityID, &snapshot.EventSequence, &snapshot.Sequence)

		// Then get any additional Events after the Snapshot, based on the last Event sequence
		events, ierr := store.repo.GetLatestEvents(ctx, entityID, snapshot.EventSequence)
		if ierr != nil {
			return eventContext, ierr
		}

		serializedEvents = events
	} else {
		// Otherwise, get /all/ Events for this Entity
		eventContext = createInitialContext(entity, entityID)

		events, ierr := store.repo.GetEvents(ctx, entityID)
		if ierr != nil {
			return eventContext, ierr
		}

		serializedEvents = events
	}

	// Apply all events to the entity to build up the current state
	if err = applyEventsToEntity(serializedEvents, &eventContext); err != nil {
		return eventContext, err
	}

	eventsSinceSnapshot := len(serializedEvents)
	if snapshot == nil && eventContext.CurrentSequence != nil {
		eventsSinceSnapshot = int(*eventContext.CurrentSequence)
	}
	if eventsSinceSnapshot < 0 {
		eventsSinceSnapshot = 0
	}

	store.maybeCatchUpSnapshot(ctx, entity, entityID, &eventContext, eventsSinceSnapshot)

	return eventContext, nil
}

// Commit persists result events and snapshots the entity when needed. A previously replayed
// CommandID returns DuplicateCommandError for idempotent success.
//
// eventContext.Entity must already reflect result.Events; otherwise a snapshot would be stale.
// Execute maintains this contract.
func (store *Store) Commit(
	ctx context.Context,
	result evt.CommandResult,
	eventContext evt.Context,
	metadata evt.Metadata,
) ([]evt.SerializedEvent, error) {
	// A store with outcomes records every command; a bare Commit has no command identity to digest.
	if store.outcomes != nil {
		return nil, errOutcomesNeedCommand
	}

	// Dedupe guard: reject duplicate CommandIDs (reads from event log, never projections)
	if metadata.CommandID != nil && eventContext.HasCommandID(*metadata.CommandID) {
		return nil, evt.NewDuplicateCommandError(*metadata.CommandID)
	}

	entityType := eventContext.Entity.Type()

	// The engine stamps the commit ID once per commit: every event of this commit — and of every
	// stream a multi-stream commit touches — carries the same value.
	metadata = evt.CommitMetadata(metadata)

	serializedEvents, err := evt.SerializeEventsWithContext(result.Events, &eventContext, metadata)
	if err != nil {
		return nil, err
	}

	// Determine if a Snapshot is needed, and what sequence to capture it at
	commitSnapshotToEvent := evt.CalculateAdditionalEvents(
		*eventContext.CurrentSequence,
		len(result.Events),
		store.effectiveSnapshotSize(entityType),
	)

	serializedResult := evt.SerializedResult{Events: serializedEvents, Transaction: result.Transaction} //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion

	if commitSnapshotToEvent == 0 {
		// If not needed, execute a simple commit, with the commit-wide projectors' operations.
		// A commit that writes no event runs no projector, so its state is never encoded.
		if len(serializedEvents) > 0 {
			state, serr := store.commitState(eventContext.Entity)
			if serr != nil {
				return nil, serr
			}

			if serializedResult.Views, err = store.projectCommit(ctx, serializedEvents, map[evt.EntityID][]byte{eventContext.EntityID: state}); err != nil {
				return nil, err
			}
		}

		if err = store.repo.Commit(ctx, serializedResult); err != nil {
			return nil, err
		}

		return serializedEvents, nil
	}

	// Otherwise, capture the Snapshot payload from the (already-applied) entity state
	payload, err := store.updateSnapshotWithEvents(
		serializedEvents,
		&eventContext,
	)
	if err != nil {
		return nil, err
	}

	// The snapshot payload is the stream's state after the commit, the same bytes the projectors see.
	if serializedResult.Views, err = store.projectCommit(ctx, serializedEvents, map[evt.EntityID][]byte{eventContext.EntityID: payload}); err != nil {
		return nil, err
	}

	// Commit the given Events along with the Snapshot payload that was generated
	if err = store.repo.CommitWithSnapshot(
		ctx,
		serializedResult,
		entityType,
		eventContext.EntityID,
		payload,
		*eventContext.CurrentSnapshot,
	); err != nil {
		return nil, err
	}

	return serializedEvents, nil
}

// Execute takes an empty Entity instance, loads it from the Repository, handles the given
// Command, commits the resulting Events using the given Metadata, and applies those Events to the
// current Entity instance.
//
// If the metadata carries a CommandID that was already processed, Execute returns a
// DuplicateCommandError. Callers should treat this as an idempotent success.
//
// On a store with outcomes enabled (WithOutcomes) Execute records the command as ExecuteWithOutcome
// does and reports only an error: nil for an accepted command, including a replay of one; a
// *evt.RejectionError for a rejected one, on first execution and on every replay.
func (store *Store) Execute(
	ctx context.Context,
	entity evt.Entity,
	entityID evt.EntityID,
	command evt.Command,
	metadata evt.Metadata,
) error {
	if store.outcomes != nil {
		return store.executeForError(ctx, entity, entityID, command, metadata)
	}

	context, err := store.LoadEntity(ctx, entity, entityID)
	if err != nil {
		return err
	}

	// Ensure the entity's internal ID matches the store key. An entity may carry
	// an abbreviated internal ID (e.g. "account:123") while the store key is a
	// fuller composite key (e.g. "account:tenant-a:123:2026-02-23"). Without this,
	// projectors write views to the wrong PK.
	if setter, ok := entity.(interface{ SetID(evt.EntityID) }); ok {
		setter.SetID(entityID)
	}

	// Early dedupe check before command handling (avoids side effects)
	if metadata.CommandID != nil && context.HasCommandID(*metadata.CommandID) {
		return evt.NewDuplicateCommandError(*metadata.CommandID)
	}

	result, err := entity.Handle(ctx, command)
	if err != nil {
		return err
	}

	// Update the entity with the resulting events
	for _, event := range result.Events {
		err = entity.Apply(event)
		if err != nil {
			return err
		}
	}

	// Run in-band projectors to generate a transaction for projected view updates that should block
	// events from being committed if they fail.
	projectorTransaction, err := buildProjectorTransaction(ctx, entity, result.Events)
	if err != nil {
		return err
	}

	result.Transaction = evt.MergeTransactions(result.Transaction, projectorTransaction) //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion

	if _, err = store.Commit(ctx, result, context, metadata); err != nil {
		return err
	}

	return nil
}

// LoadEntityWithFactory creates a fresh entity instance and loads it from the repository.
//
// Deprecated: Cut in ADR-0006 (K7): use Store.LoadEntity directly. It will be removed in the
// next minor release.
func (store *Store) LoadEntityWithFactory(
	ctx context.Context,
	factory evt.EntityFactory,
	entityID evt.EntityID,
) (evt.Entity, evt.Context, error) {
	return evt.LoadEntityWithFactory(ctx, store, factory, entityID) //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
}

// ExecuteWithFactory creates a fresh entity instance, executes the command, and returns it.
//
// Deprecated: Cut in ADR-0006 (K7): use Store.Execute directly. It will be removed in the next
// minor release.
func (store *Store) ExecuteWithFactory(
	ctx context.Context,
	factory evt.EntityFactory,
	entityID evt.EntityID,
	command evt.Command,
	metadata evt.Metadata,
) (evt.Entity, error) {
	return evt.ExecuteWithFactory(ctx, store, factory, entityID, command, metadata) //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
}

// updateSnapshotWithEvents advances sequence counters and captures current entity state.
// The entity must already include the events; they are not reapplied to avoid corrupting additive state.
func (store *Store) updateSnapshotWithEvents(
	serializedEvents []evt.SerializedEvent,
	eventContext *evt.Context,
) ([]byte, error) {
	// Advance the running event sequence to account for the committed batch.
	if eventContext.CurrentSequence != nil {
		*eventContext.CurrentSequence += evt.EventSequence(len(serializedEvents))
	}

	// Update snapshot sequence
	updateSnapshotSequence(eventContext)

	// Capture the entity's current (already-applied) state as the snapshot payload
	return generateSnapshotPayload(eventContext.Entity)
}

func buildProjectorTransaction(
	ctx context.Context,
	entity evt.Entity,
	events []evt.Event,
) (evt.Transaction, error) {
	if entity == nil {
		return nil, nil
	}

	projectors := entity.Projectors()
	if len(projectors) == 0 {
		return nil, nil
	}

	var transaction evt.Transaction

	for _, projector := range projectors {
		if projector == nil {
			continue
		}

		group, err := projector.Project(ctx, entity, events)
		if err != nil {
			return nil, err
		}
		if group == nil {
			continue
		}

		transaction = append(transaction, group)
	}

	return transaction, nil
}

type snapshotWriter interface {
	PutSnapshot(
		ctx context.Context,
		entityType evt.EntityType,
		entityID evt.EntityID,
		payload []byte,
		snapshotSequence evt.EventSequence,
		eventSequence evt.EventSequence,
	) error
}

func (store *Store) maybeCatchUpSnapshot(
	ctx context.Context,
	entity evt.Entity,
	entityID evt.EntityID,
	eventContext *evt.Context,
	eventsSinceSnapshot int,
) {
	if store.catchUpSnapshotThreshold <= 0 || eventsSinceSnapshot < store.catchUpSnapshotThreshold {
		return
	}
	if entity == nil || eventContext == nil || eventContext.CurrentSequence == nil || eventContext.CurrentSnapshot == nil {
		return
	}

	writer, ok := store.repo.(snapshotWriter)
	if !ok {
		return
	}

	payload, err := generateSnapshotPayload(entity)
	if err != nil {
		logCatchUpError(ctx, entity.Type(), entityID, "payload", err)
		return
	}

	nextSnapshot := *eventContext.CurrentSnapshot + 1
	err = writer.PutSnapshot(ctx, entity.Type(), entityID, payload, nextSnapshot, *eventContext.CurrentSequence)
	if err != nil {
		logCatchUpError(ctx, entity.Type(), entityID, "write", err)
		return
	}
}

// logCatchUpError logs a snapshot catch-up failure.
func logCatchUpError(
	ctx context.Context,
	entityType evt.EntityType,
	entityID evt.EntityID,
	phase string,
	err error,
) {
	slog.WarnContext(ctx, "snapshot catch-up failed",
		"entityType", string(entityType),
		"entityID", string(entityID),
		"phase", phase,
		"err", err,
	)
}
