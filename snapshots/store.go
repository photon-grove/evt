package snapshots

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"

	"github.com/photon-grove/evt"
)

// Store handles executing Commands, committing Events that are yielded by those Commands,
// and loading Entity instances from a Repository. It captures snapshots of the Entity state
// every N events, where N is the snapshot size.
type Store struct {
	repo                     evt.Repository
	snapshotSize             int
	snapshotOverrides        map[evt.EntityType]int
	replayEstimatePerEventMS float64
	replayBudgetEvents       int
	catchUpSnapshotThreshold int
	freshnessObserver        FreshnessObserver
}

// NewStore creates a new Store with the given Repository and Snapshot size
// (the target number of events between each Snapshot).
func NewStore(repo evt.Repository, snapshotSize int) *Store {
	return &Store{
		repo:                     repo,
		snapshotSize:             snapshotSize,
		snapshotOverrides:        make(map[evt.EntityType]int),
		replayEstimatePerEventMS: defaultReplayEstimatePerEventMS,
	}
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

	estimatedReplayMS := int(math.Round(float64(eventsSinceSnapshot) * store.replayEstimatePerEventMS))
	sample := FreshnessSample{
		EntityType:          string(entity.Type()),
		EntityID:            string(entityID),
		EventsSinceSnapshot: eventsSinceSnapshot,
		EstimatedReplayMS:   estimatedReplayMS,
	}
	if eventContext.CurrentSnapshot != nil {
		sample.SnapshotSequence = int64(*eventContext.CurrentSnapshot)
	}
	if eventContext.CurrentSequence != nil {
		sample.EventSequence = int64(*eventContext.CurrentSequence)
	}
	if store.replayBudgetEvents > 0 && eventsSinceSnapshot > store.replayBudgetEvents {
		sample.ReplayBudgetExceeded = true
	}

	store.maybeCatchUpSnapshot(ctx, entity, entityID, &eventContext, eventsSinceSnapshot, &sample)
	if store.freshnessObserver != nil {
		store.freshnessObserver(ctx, sample)
	}

	return eventContext, nil
}

// Commit new Events to the Entity within the given context, with Metadata.
// If the metadata carries a CommandID that was already seen during event replay,
// the commit is skipped and a DuplicateCommandError is returned so the caller
// can treat the retry as an idempotent success.
func (store *Store) Commit(
	ctx context.Context,
	result evt.CommandResult,
	eventContext evt.Context,
	metadata evt.Metadata,
) ([]evt.SerializedEvent, error) {
	// Dedupe guard: reject duplicate CommandIDs (reads from event log, never projections)
	if metadata.CommandID != nil && eventContext.HasCommandID(*metadata.CommandID) {
		return nil, evt.NewDuplicateCommandError(*metadata.CommandID)
	}

	entityType := eventContext.Entity.Type()

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

	serializedResult := evt.SerializedResult{Events: serializedEvents, Transaction: result.Transaction}

	if commitSnapshotToEvent == 0 {
		// If not needed, execute a simple commit
		if err = store.repo.Commit(ctx, serializedResult); err != nil {
			return nil, err
		}

		return serializedEvents, nil
	}

	// Otherwise, generate the payload for the Snapshot to be taken
	payload, err := store.updateSnapshotWithEvents(
		serializedEvents,
		&eventContext,
		commitSnapshotToEvent,
	)
	if err != nil {
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
func (store *Store) Execute(
	ctx context.Context,
	entity evt.Entity,
	entityID evt.EntityID,
	command evt.Command,
	metadata evt.Metadata,
) error {
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

	result.Transaction = evt.MergeTransactions(result.Transaction, projectorTransaction)

	if _, err = store.Commit(ctx, result, context, metadata); err != nil {
		return err
	}

	return nil
}

// LoadEntityWithFactory creates a fresh entity instance and loads it from the repository.
func (store *Store) LoadEntityWithFactory(
	ctx context.Context,
	factory evt.EntityFactory,
	entityID evt.EntityID,
) (evt.Entity, evt.Context, error) {
	return evt.LoadEntityWithFactory(ctx, store, factory, entityID)
}

// ExecuteWithFactory creates a fresh entity instance, executes the command, and returns it.
func (store *Store) ExecuteWithFactory(
	ctx context.Context,
	factory evt.EntityFactory,
	entityID evt.EntityID,
	command evt.Command,
	metadata evt.Metadata,
) (evt.Entity, error) {
	return evt.ExecuteWithFactory(ctx, store, factory, entityID, command, metadata)
}

// Generate the Snapshot payload when a new Snapshot needs to be taken
func (store *Store) updateSnapshotWithEvents(
	serializedEvents []evt.SerializedEvent,
	eventContext *evt.Context,
	commitSnapshotToEvent int,
) ([]byte, error) {
	// Apply events up to the snapshot point
	if err := applyEventsForSnapshot(serializedEvents, eventContext, commitSnapshotToEvent); err != nil {
		return nil, err
	}

	// Update snapshot sequence
	updateSnapshotSequence(eventContext)

	// Generate and return the snapshot payload
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
	sample *FreshnessSample,
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
		logCatchUpError(ctx, entity.Type(), entityID, "payload", err, sample)
		return
	}

	nextSnapshot := *eventContext.CurrentSnapshot + 1
	err = writer.PutSnapshot(ctx, entity.Type(), entityID, payload, nextSnapshot, *eventContext.CurrentSequence)
	if err != nil {
		logCatchUpError(ctx, entity.Type(), entityID, "write", err, sample)
		return
	}
	if sample != nil {
		sample.CatchUpApplied = true
		sample.SnapshotSequence = int64(nextSnapshot)
	}
}

// logCatchUpError logs a snapshot catch-up failure and records it in the freshness sample.
func logCatchUpError(
	ctx context.Context,
	entityType evt.EntityType,
	entityID evt.EntityID,
	phase string,
	err error,
	sample *FreshnessSample,
) {
	slog.WarnContext(ctx, "snapshot catch-up failed",
		"entityType", string(entityType),
		"entityID", string(entityID),
		"phase", phase,
		"err", err,
	)
	if sample != nil {
		sample.CatchUpError = err.Error()
	}
}
