package snapshots

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	evt "github.com/photon-grove/evt/go"
)

// Store handles executing Commands, committing Events that are yielded by those Commands,
// and loading Aggregate instances from a Repository. It captures snapshots of the Aggregate state
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

// LoadAggregate loads the given Aggregate instance with the given id, retrieving Snapshots
// and Events from the repository.
func (store *Store) LoadAggregate(
	ctx context.Context,
	aggregate evt.Aggregate,
	entityID evt.EntityID,
) (evt.CommandContext, error) {
	var cc evt.CommandContext

	// Look for a Snapshot first
	snapshot, err := store.repo.GetSnapshot(ctx, entityID)
	if err != nil {
		return cc, err
	}

	var serializedEvents []evt.SerializedEvent

	if snapshot != nil {
		// If found, rehydrate the current state at the point the Snapshot captured it
		if err = json.Unmarshal(snapshot.Payload, aggregate); err != nil {
			return cc, err
		}

		cc = createCommandContext(aggregate, entityID, &snapshot.EventSequence, &snapshot.Sequence)

		// Then get any additional Events after the Snapshot, based on the last Event sequence
		events, ierr := store.repo.GetLatestEvents(ctx, entityID, snapshot.EventSequence)
		if ierr != nil {
			return cc, ierr
		}

		serializedEvents = events
	} else {
		// Otherwise, get /all/ Events for this Aggregate
		cc = createInitialCommandContext(aggregate, entityID)

		events, ierr := store.repo.GetEvents(ctx, entityID)
		if ierr != nil {
			return cc, ierr
		}

		serializedEvents = events
	}

	// Apply all events to the entity to build up the current state
	if err = applyEventsToAggregate(serializedEvents, &cc); err != nil {
		return cc, err
	}

	eventsSinceSnapshot := len(serializedEvents)
	if snapshot == nil && cc.CurrentSequence != nil {
		eventsSinceSnapshot = int(*cc.CurrentSequence)
	}
	if eventsSinceSnapshot < 0 {
		eventsSinceSnapshot = 0
	}

	store.maybeCatchUpSnapshot(ctx, aggregate, entityID, &cc, eventsSinceSnapshot)

	return cc, nil
}

// Commit persists result events and snapshots the entity when needed. A previously replayed
// CommandID returns DuplicateCommandError for idempotent success.
//
// cc.Aggregate must already reflect result.Events; otherwise a snapshot would be stale.
// Execute maintains this contract.
func (store *Store) Commit(
	ctx context.Context,
	result evt.CommandResult,
	cc evt.CommandContext,
	metadata evt.Metadata,
) ([]evt.SerializedEvent, error) {
	return store.commitWithTransaction(ctx, result, nil, cc, metadata)
}

// commitWithTransaction is Commit with the in-band projectors' transaction attached to the commit.
func (store *Store) commitWithTransaction(
	ctx context.Context,
	result evt.CommandResult,
	transaction evt.Transaction,
	cc evt.CommandContext,
	metadata evt.Metadata,
) ([]evt.SerializedEvent, error) {
	// A store with outcomes records every command; a bare Commit has no command identity to digest.
	if store.outcomes != nil {
		return nil, errOutcomesNeedCommand
	}

	// Dedupe guard: reject duplicate CommandIDs (reads from event log, never projections)
	if metadata.CommandID != nil && cc.HasCommandID(*metadata.CommandID) {
		return nil, evt.NewDuplicateCommandError(*metadata.CommandID)
	}

	entityType := cc.Aggregate.Type()

	// The engine stamps the commit ID once per commit: every event of this commit — and of every
	// stream a multi-stream commit touches — carries the same value.
	metadata = evt.CommitMetadata(metadata)

	serializedEvents, err := evt.SerializeEventsWithCommandContext(result.Events, &cc, metadata)
	if err != nil {
		return nil, err
	}

	// Determine if a Snapshot is needed, and what sequence to capture it at
	commitSnapshotToEvent := evt.CalculateAdditionalEvents(
		*cc.CurrentSequence,
		len(result.Events),
		store.effectiveSnapshotSize(entityType),
	)

	serializedResult := evt.SerializedResult{Events: serializedEvents, Transaction: transaction}

	// Capture the Snapshot from the (already-applied) entity state, unless it is too large to write.
	var snapshot *evt.SerializedSnapshot
	if commitSnapshotToEvent > 0 {
		if snapshot, err = commitSnapshot(&cc, entityType, cc.EntityID, serializedEvents); err != nil {
			return nil, err
		}
	}

	if snapshot == nil {
		// If not needed, execute a simple commit, with the commit-wide projectors' operations.
		// A commit that writes no event runs no projector, so its state is never encoded.
		if len(serializedEvents) > 0 {
			state, serr := store.commitState(cc.Aggregate)
			if serr != nil {
				return nil, serr
			}

			if serializedResult.Views, err = store.projectCommit(ctx, serializedEvents, map[evt.EntityID][]byte{cc.EntityID: state}); err != nil {
				return nil, err
			}
		}

		if err = store.repo.Commit(ctx, serializedResult); err != nil {
			return nil, err
		}

		return serializedEvents, nil
	}

	// The snapshot payload is the stream's state after the commit, the same bytes the projectors see.
	if serializedResult.Views, err = store.projectCommit(ctx, serializedEvents, map[evt.EntityID][]byte{cc.EntityID: snapshot.Payload}); err != nil {
		return nil, err
	}

	// Commit the given Events along with the Snapshot payload that was generated
	if err = store.repo.CommitWithSnapshot(
		ctx,
		serializedResult,
		entityType,
		cc.EntityID,
		snapshot.Payload,
		snapshot.Sequence,
	); err != nil {
		return nil, err
	}

	return serializedEvents, nil
}

// Execute takes an empty Aggregate instance, loads it from the Repository, handles the given
// Command, commits the resulting Events using the given Metadata, and applies those Events to the
// current Aggregate instance.
//
// If the metadata carries a CommandID that was already processed, Execute returns a
// DuplicateCommandError. Callers should treat this as an idempotent success.
//
// On a store with outcomes enabled (WithOutcomes) Execute records the command as ExecuteWithOutcome
// does and reports only an error: nil for an accepted command, including a replay of one; a
// *evt.RejectionError for a rejected one, on first execution and on every replay.
func (store *Store) Execute(
	ctx context.Context,
	aggregate evt.Aggregate,
	entityID evt.EntityID,
	command evt.Command,
	metadata evt.Metadata,
) error {
	if store.outcomes != nil {
		return store.executeForError(ctx, aggregate, entityID, command, metadata)
	}

	cc, err := store.LoadAggregate(ctx, aggregate, entityID)
	if err != nil {
		return err
	}

	// Ensure the entity's internal ID matches the store key. An entity may carry
	// an abbreviated internal ID (e.g. "account:123") while the store key is a
	// fuller composite key (e.g. "account:tenant-a:123:2026-02-23"). Without this,
	// projectors write views to the wrong PK.
	if setter, ok := aggregate.(interface{ SetID(evt.EntityID) }); ok {
		setter.SetID(entityID)
	}

	// Early dedupe check before command handling (avoids side effects)
	if metadata.CommandID != nil && cc.HasCommandID(*metadata.CommandID) {
		return evt.NewDuplicateCommandError(*metadata.CommandID)
	}

	result, err := aggregate.Handle(ctx, command)
	if err != nil {
		return err
	}

	// Update the entity with the resulting events
	for _, event := range result.Events {
		err = aggregate.Apply(event)
		if err != nil {
			return err
		}
	}

	// Run in-band projectors to generate a transaction for projected view updates that should block
	// events from being committed if they fail.
	projectorTransaction, err := buildProjectorTransaction(ctx, aggregate, result.Events)
	if err != nil {
		return err
	}

	if _, err = store.commitWithTransaction(ctx, result, projectorTransaction, cc, metadata); err != nil {
		return err
	}

	return nil
}

// commitSnapshot captures the snapshot a commit that reaches the snapshot interval writes: the
// entity's state after the commit's events, which cc.Aggregate already includes, at the commit's
// last event. A snapshot whose item is over the per-item commit budget (evt.SnapshotCommitSize
// above evt.MaxCommitItemBytes) is left out: writing it would fail this commit, and every later
// one that reaches the interval, with CommitTooLargeError, so the commit proceeds without it, the
// skip is logged, and commitSnapshot returns nil and leaves cc unchanged. Otherwise it advances
// cc's event and snapshot sequences past the commit.
func commitSnapshot(
	cc *evt.CommandContext,
	entityType evt.EntityType,
	entityID evt.EntityID,
	serializedEvents []evt.SerializedEvent,
) (*evt.SerializedSnapshot, error) {
	if len(serializedEvents) == 0 {
		return nil, nil
	}

	payload, err := generateSnapshotPayload(cc.Aggregate)
	if err != nil {
		return nil, err
	}

	sequence := evt.EventSequence(1)
	if cc.CurrentSnapshot != nil {
		sequence = *cc.CurrentSnapshot + 1
	}

	snapshot := &evt.SerializedSnapshot{
		EntityType:    entityType,
		EntityID:      entityID,
		Sequence:      sequence,
		EventSequence: serializedEvents[len(serializedEvents)-1].Sequence,
		Payload:       payload,
	}
	if !snapshotFits(*snapshot) {
		return nil, nil
	}

	// Advance the running event sequence to account for the committed batch.
	if cc.CurrentSequence != nil {
		*cc.CurrentSequence += evt.EventSequence(len(serializedEvents))
	}

	updateSnapshotSequence(cc)

	return snapshot, nil
}

// snapshotFits reports whether a snapshot's item fits the per-item commit budget, and logs a
// snapshot that does not: the store skips it, and the stream's loads replay from the snapshot
// before it until the aggregate's state shrinks below the budget.
func snapshotFits(snapshot evt.SerializedSnapshot) bool {
	size := evt.SnapshotCommitSize(snapshot)
	if size <= evt.MaxCommitItemBytes {
		return true
	}

	slog.Warn("snapshot skipped: its item is over the per-item commit budget",
		"entityType", string(snapshot.EntityType),
		"entityID", string(snapshot.EntityID),
		"sequence", int64(snapshot.Sequence),
		"eventSequence", int64(snapshot.EventSequence),
		"bytes", size,
		"limit", evt.MaxCommitItemBytes,
	)

	return false
}

func buildProjectorTransaction(
	ctx context.Context,
	aggregate evt.Aggregate,
	events []evt.Event,
) (evt.Transaction, error) {
	if aggregate == nil {
		return nil, nil
	}

	projectors := aggregate.Projectors()
	if len(projectors) == 0 {
		return nil, nil
	}

	var transaction evt.Transaction

	for _, projector := range projectors {
		if projector == nil {
			continue
		}

		group, err := projector.Project(ctx, aggregate, events)
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
	aggregate evt.Aggregate,
	entityID evt.EntityID,
	cc *evt.CommandContext,
	eventsSinceSnapshot int,
) {
	if store.catchUpSnapshotThreshold <= 0 || eventsSinceSnapshot < store.catchUpSnapshotThreshold {
		return
	}
	if aggregate == nil || cc == nil || cc.CurrentSequence == nil || cc.CurrentSnapshot == nil {
		return
	}

	writer, ok := store.repo.(snapshotWriter)
	if !ok {
		return
	}

	payload, err := generateSnapshotPayload(aggregate)
	if err != nil {
		logCatchUpError(ctx, aggregate.Type(), entityID, "payload", err)
		return
	}

	nextSnapshot := *cc.CurrentSnapshot + 1
	if !snapshotFits(evt.SerializedSnapshot{
		EntityType:    aggregate.Type(),
		EntityID:      entityID,
		Sequence:      nextSnapshot,
		EventSequence: *cc.CurrentSequence,
		Payload:       payload,
	}) {
		return
	}

	err = writer.PutSnapshot(ctx, aggregate.Type(), entityID, payload, nextSnapshot, *cc.CurrentSequence)
	if err != nil {
		logCatchUpError(ctx, aggregate.Type(), entityID, "write", err)
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
