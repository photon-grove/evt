package mem

// WARNING: Not intended for use in Production

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
	"github.com/photon-grove/evt/result" //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
)

// This Repository lives in-memory for testing or local development
type Repository struct {
	events    map[string][]evt.SerializedEvent
	snapshots map[string]evt.SerializedSnapshot

	// features holds the durable feature manifest. Shared through a pointer, like the maps
	// above, so copies of the handle (and clones of it) see one manifest.
	features *memFeatures
}

// memFeatures is the in-memory form of the feature-marker row: the manifest plus the lock that
// keeps a declaration and a commit's retention check from interleaving.
type memFeatures struct {
	mu       sync.Mutex
	manifest *evt.FeatureManifest
}

// NewRepository creates a new instance of the Repository
func NewRepository() evt.Repository {
	return &Repository{
		make(map[string][]evt.SerializedEvent),
		make(map[string]evt.SerializedSnapshot),
		&memFeatures{},
	}
}

// The in-memory repository also satisfies the snapshot-aware and head-streaming capabilities.
var (
	_ evt.SnapshotStreamer   = (*Repository)(nil)
	_ evt.EntityHeadStreamer = (*Repository)(nil)
	_ evt.EntityHeadVisitor  = (*Repository)(nil)
)

// Commit Events to memory. It does not reject reused sequences, but like the durable backends it
// rejects a commit at or below the entity's snapshot, which compaction may have deleted below.
// A multi-stream commit validates every stream, snapshot, and read fence before writing
// anything, so a failure leaves every stream unchanged.
func (repo Repository) Commit(
	_ context.Context,
	result evt.SerializedResult,
) error {
	normalized, err := evt.NormalizeCommit(result)
	if err != nil {
		return err
	}

	if err := evt.PreflightCommit(normalized); err != nil {
		return err
	}

	// The manifest lock is held through the write, so a declaration cannot slip between the
	// marker checks and the events, mirroring the durable backends' transaction condition.
	repo.features.mu.Lock()
	defer repo.features.mu.Unlock()

	// A manifest that declares a feature this writer does not model refuses the commit, as the
	// durable backends' marker check does. The in-memory double models no extensions.
	if err := repo.features.validate(); err != nil {
		return err
	}

	// Fences are refused outright for entity types durably declared with retention, exactly as
	// the durable backends refuse them: TTL deletes reopen fenced slots without leaving a
	// successor snapshot, so the two checks cannot protect them.
	if err := evt.CheckFenceRetention(normalized.Fences, repo.features.manifest); err != nil {
		return err
	}

	if err := repo.checkFloor(normalized.Events); err != nil {
		return err
	}

	if err := repo.checkSnapshots(normalized.Snapshots); err != nil {
		return err
	}

	if err := repo.checkFences(normalized.Fences); err != nil {
		return err
	}

	for _, event := range normalized.Events {
		id := string(event.EntityID)

		if repo.events[id] == nil {
			var empty []evt.SerializedEvent
			repo.events[id] = empty
		}

		repo.events[id] = append(repo.events[id], event)
	}

	for _, snapshot := range normalized.Snapshots {
		repo.snapshots[string(snapshot.EntityID)] = snapshot
	}

	return repo.applyTransactions(normalized.Transaction)
}

// checkSnapshots rejects a snapshot whose sequence does not follow the stored one, or whose
// event sequence is not above the events it summarizes.
func (repo Repository) checkSnapshots(snapshots []evt.SerializedSnapshot) error {
	for _, snapshot := range snapshots {
		// A stream with no stored snapshot is at sequence 0, so its first snapshot must be 1,
		// as on DynamoDB and PostgreSQL.
		var storedSequence evt.EventSequence
		if stored, ok := repo.snapshots[string(snapshot.EntityID)]; ok {
			storedSequence = stored.Sequence
		}

		if storedSequence+1 != snapshot.Sequence {
			return evt.NewConflictError(fmt.Sprintf(
				"mem: snapshot sequence moved: stored %d, commit writes %d",
				storedSequence, snapshot.Sequence,
			))
		}
	}

	return nil
}

// checkFences rejects a fence whose stream has moved past the fenced head: any event above the
// head, or any snapshot covering it, means another writer advanced the stream. This is the
// in-memory form of the slot check plus the snapshot companion (ADR 0004, C2).
func (repo Repository) checkFences(fences []evt.ReadFence) error {
	for _, fence := range fences {
		for _, event := range repo.events[string(fence.EntityID)] {
			if event.Sequence > fence.Head {
				return policy.FenceConflict(fence)
			}
		}

		if snapshot, ok := repo.snapshots[string(fence.EntityID)]; ok && snapshot.EventSequence > fence.Head {
			return policy.FenceConflict(fence)
		}
	}

	return nil
}

// validate refuses a manifest that declares a feature this build does not model. The in-memory
// repository models no extensions, like the default Go writer. The caller holds mu.
func (f *memFeatures) validate() error {
	if f.manifest == nil {
		return nil
	}

	return f.manifest.Validate(nil)
}

// Manifest reads the stored feature manifest, or nil when nothing is declared. The returned
// manifest is a copy; declaring is the only way to change it.
func (repo Repository) Manifest(_ context.Context) (*evt.FeatureManifest, error) {
	repo.features.mu.Lock()
	defer repo.features.mu.Unlock()

	if repo.features.manifest == nil {
		return nil, nil
	}

	manifest := cloneManifest(*repo.features.manifest)

	return &manifest, nil
}

// Declare applies a change to the stored manifest in one read-modify-write and bumps its version
// by exactly one, like the durable backends' declarations, and returns the resulting manifest.
// Declaring does not grant support: the resulting manifest may declare features this build does
// not model, which then refuses later commits.
func (repo Repository) Declare(
	_ context.Context,
	change func(*evt.FeatureManifest),
) (evt.FeatureManifest, error) {
	repo.features.mu.Lock()
	defer repo.features.mu.Unlock()

	next := evt.FeatureManifest{Features: map[string]evt.FeatureFormat{}}
	if repo.features.manifest != nil {
		next = cloneManifest(*repo.features.manifest)
	}

	version := next.Version
	change(&next)
	next.Version = version + 1
	repo.features.manifest = &next

	return cloneManifest(next), nil
}

// cloneManifest copies a manifest so callers never alias the stored one.
func cloneManifest(manifest evt.FeatureManifest) evt.FeatureManifest {
	manifest.Features = maps.Clone(manifest.Features)
	manifest.HistoryDependent = slices.Clone(manifest.HistoryDependent)
	manifest.Retention = slices.Clone(manifest.Retention)

	return manifest
}

// CommitStream streams Events to memory
//
// Deprecated: Cut in ADR-0006 (K1). Commit batched commands through the normal commit path
// instead. It will be removed in the next minor release.
func (repo Repository) CommitStream(
	ctx context.Context,
	channel <-chan result.Result[evt.SerializedResult],
) []error {
	var errors []error

	for result := range channel {
		serializedResult, err := result.Unwrap()
		if err != nil {
			errors = append(errors, err)
			continue
		}

		if err := repo.Commit(ctx, serializedResult); err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

// checkFloor returns an evt.ConflictError when an entity's snapshot covers an event being committed.
func (repo Repository) checkFloor(events []evt.SerializedEvent) error {
	for _, event := range events {
		snapshot, ok := repo.snapshots[string(event.EntityID)]
		if ok && event.Sequence <= snapshot.EventSequence {
			return evt.NewConflictError(fmt.Sprintf(
				"mem: event %d for %s is at or below the snapshot floor %d",
				event.Sequence, event.EntityID, snapshot.EventSequence,
			))
		}
	}

	return nil
}

// CommitWithSnapshot commits Events with a Snapshot. It routes through the multi-stream commit
// path, writing the snapshot in the same logical commit as its events.
func (repo Repository) CommitWithSnapshot(
	ctx context.Context,
	result evt.SerializedResult,
	entityType evt.EntityType,
	entityID evt.EntityID,
	payload []byte,
	currentSnapshot evt.EventSequence,
) error {
	if len(result.Events) == 0 {
		return fmt.Errorf("mem: commit with snapshot requires at least one event")
	}

	if err := evt.CheckReservedEntityID(entityID); err != nil {
		return err
	}

	result.Snapshots = append(result.Snapshots, evt.SerializedSnapshot{
		EntityType:    entityType,
		EntityID:      entityID,
		Sequence:      currentSnapshot,
		EventSequence: result.Events[len(result.Events)-1].Sequence,
		Payload:       payload,
	})

	return repo.Commit(ctx, result)
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

// GetRecentEvents returns at most limit Events newest-first (descending sequence). A limit below
// 1 is a programmer error returned before any read.
func (repo Repository) GetRecentEvents(
	_ context.Context,
	entityID evt.EntityID,
	limit int,
) ([]evt.SerializedEvent, error) {
	if limit < 1 {
		return nil, &evt.InvalidArgumentError{Argument: "limit", Message: fmt.Sprintf("must be at least 1, got %d", limit)}
	}

	id := string(entityID)

	latest := make([]evt.SerializedEvent, 0, len(repo.events[id]))
	for _, event := range repo.events[id] {
		if event.Sequence == 0 {
			// Defensive: inline snapshots are not events.
			continue
		}

		latest = append(latest, event)
	}

	// Commits need not arrive in sequence order. Sort a copy so reads never reorder storage.
	slices.SortFunc(latest, func(a, b evt.SerializedEvent) int {
		return cmp.Compare(b.Sequence, a.Sequence)
	})

	return latest[:min(limit, len(latest))], nil
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
			if evt.IsReservedEntityID(evt.EntityID(id)) {
				// Reserved namespaces hold engine-managed rows, never ordinary aggregates.
				continue
			}

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
	ctx context.Context,
	entityType evt.EntityType,
) (map[evt.EntityID]evt.EventSequence, error) {
	heads := make(map[evt.EntityID]evt.EventSequence)

	err := repo.StreamEntityHeadsFunc(ctx, entityType, func(id evt.EntityID, head evt.EventSequence) error {
		heads[id] = head
		return nil
	})
	if err != nil {
		return nil, err
	}

	return heads, nil
}

// StreamEntityHeadsFunc implements evt.EntityHeadVisitor over the in-memory log, invoking visit once
// per entity without building the full map StreamEntityHeads returns. The in-memory backend holds
// the whole log anyway, so this is not constant-memory the way a paged heads table is; it exists so
// the in-memory repository can stand in for a streaming head source in tests and examples. The head
// has the same meaning as StreamEntityHeads (the larger of the highest stored event sequence and the
// snapshot's recorded EventSequence). If visit returns an error, enumeration stops and returns it.
func (repo Repository) StreamEntityHeadsFunc(
	_ context.Context,
	entityType evt.EntityType,
	visit func(evt.EntityID, evt.EventSequence) error,
) error {
	for id, events := range repo.events {
		if evt.IsReservedEntityID(evt.EntityID(id)) {
			// Reserved namespaces hold engine-managed rows, never ordinary aggregates.
			continue
		}

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

		if err := visit(evt.EntityID(id), head); err != nil {
			return err
		}
	}

	// Entities present only as a snapshot (events compacted away with no event key) still have a
	// head. Skip any id that already has an events entry — it was visited above.
	for id, snapshot := range repo.snapshots {
		if _, hasEvents := repo.events[id]; hasEvents {
			continue
		}
		if evt.IsReservedEntityID(evt.EntityID(id)) {
			continue
		}
		if entityType != "" && snapshot.EntityType != entityType {
			continue
		}

		if err := visit(evt.EntityID(id), snapshot.EventSequence); err != nil {
			return err
		}
	}

	return nil
}

// CompactBelow deletes events for an entity whose sequence is in [1, throughSequence], but only
// after verifying a durable snapshot covers (>=) throughSequence. It implements evt.Compactor.
func (repo Repository) CompactBelow(
	_ context.Context,
	entityID evt.EntityID,
	throughSequence evt.EventSequence,
) (int, error) {
	if err := evt.CheckReservedEntityID(entityID); err != nil {
		return 0, err
	}

	if throughSequence < 1 {
		return 0, nil
	}

	id := string(entityID)

	repo.features.mu.Lock()
	defer repo.features.mu.Unlock()

	if err := repo.features.validate(); err != nil {
		return 0, err
	}

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

	// A history-dependent type's rebuild replays the commits compaction would delete.
	if repo.features.manifest != nil && slices.Contains(repo.features.manifest.HistoryDependent, snapshot.EntityType) {
		return 0, evt.NewHistoryDependentProjectionError(snapshot.EntityType)
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
			if evt.IsReservedEntityID(evt.EntityID(id)) {
				// Reserved namespaces hold engine-managed rows, never ordinary aggregates.
				continue
			}

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
			if evt.IsReservedEntityID(evt.EntityID(id)) {
				// Reserved namespaces hold engine-managed rows, never ordinary aggregates.
				continue
			}

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
