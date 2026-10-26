package mem

// WARNING: Not intended for use in Production

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/policy"
	"github.com/photon-grove/evt/go/result"
)

// This Repository lives in-memory for testing or local development
type Repository struct {
	events    map[string][]evt.SerializedEvent
	snapshots map[string]evt.SerializedSnapshot

	// heads holds the per-stream head (ADR 0007): the highest event sequence ever committed to
	// the stream, maintained by every commit and never lowered. A stream with no entry has head 0.
	heads map[string]evt.EventSequence

	// features holds the durable feature manifest. Shared through a pointer, like the maps
	// above, so copies of the handle (and clones of it) see one manifest.
	features *memFeatures

	// now is the clock the rebuild fence reads for lease expiry; nil means time.Now.
	now func() time.Time
}

// memFeatures is the in-memory form of the feature-marker row: the manifest plus the lock that
// keeps a declaration and a commit's retention check from interleaving.
type memFeatures struct {
	mu       sync.Mutex
	manifest *evt.FeatureManifest

	// outcomes holds the command-outcome rows (ADR 0004, C3), keyed by command ID. They share the
	// lock with the manifest so an outcome is written in the same critical section as its events.
	outcomes map[evt.CommandID]evt.Outcome

	// views holds the rows commit-wide view operations write (ADR 0004, C7), keyed by pk and sk.
	// They share the lock with the events, so a commit's rows appear in the same critical section.
	views map[viewKey]evt.SerializedView

	// rebuild holds the rebuild fence (ADR 0004, C10): the lock row and the generation item. It
	// shares the lock with the events, so a commit's lock check and its writes are one critical
	// section.
	rebuild memRebuild
}

// NewRepository creates a new instance of the Repository
func NewRepository() evt.Repository {
	return &Repository{
		events:    make(map[string][]evt.SerializedEvent),
		snapshots: make(map[string]evt.SerializedSnapshot),
		heads:     make(map[string]evt.EventSequence),
		features:  &memFeatures{},
	}
}

// The in-memory repository also satisfies the snapshot-aware and head-streaming capabilities.
var (
	_ evt.SnapshotStreamer   = (*Repository)(nil)
	_ evt.EntityHeadStreamer = (*Repository)(nil)
	_ evt.EntityHeadVisitor  = (*Repository)(nil)

	_ evt.TransactionalRepository = (*Repository)(nil)
	_ evt.StoreInitializer        = (*Repository)(nil)
	_ evt.OutcomeRepository       = (*Repository)(nil)
	_ evt.RebuildFenceProvider    = (*Repository)(nil)
)

// AtomicTransactions declares that Commit writes every stream, snapshot, and fence of a result
// all or nothing (evt.TransactionalRepository).
func (Repository) AtomicTransactions() {}

// Commit Events to memory. Like the durable backends it conditions every written stream on its
// head (ADR 0007): a stream that starts at sequence 1 must have none, and one that appends must be
// at head first - 1, so a reused sequence, a skipped sequence, and a commit below a compacted
// snapshot are all refused. Read fences are one check of the same head. A multi-stream commit
// validates every stream, snapshot, and read fence before writing anything, so a failure leaves
// every stream unchanged.
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
	// durable backends' marker check does, and so does a store that holds data without declaring
	// stream-heads. An empty store declares it itself.
	if err := repo.validateLocked(); err != nil {
		return err
	}

	// Fences are refused outright for entity types durably declared with retention, exactly as
	// the durable backends refuse them. Like every refusal that needs only the manifest, it
	// precedes the rebuild-lock check.
	if err := evt.CheckFenceRetention(normalized.Fences, repo.features.manifest); err != nil {
		return err
	}

	// The rebuild-lock check (C10): an unsettled generation — any lock, including one whose lease
	// expired, or a force-release no rebuild has finished since — refuses the commit, ahead of the
	// outcome row and every stream check (storage spec, Writes).
	if repo.features.rebuild.unsettled() {
		return &evt.RebuildInProgressError{}
	}

	// A lost outcome row outranks every other failure but the marker (storage spec, Stream
	// Heads, Classification): the row is where the answer lives.
	if outcome := normalized.Outcome; outcome != nil {
		if _, exists := repo.features.outcomes[outcome.CommandID]; exists {
			return &evt.OutcomeConflictError{CommandID: outcome.CommandID}
		}
	}

	if err := repo.checkHeads(normalized); err != nil {
		return err
	}

	if err := repo.checkSnapshots(normalized.Snapshots); err != nil {
		return err
	}

	// A view operation's condition is the lowest-ranked failure: it is reported only when nothing
	// above it failed, and nothing is written when it does.
	if err := repo.checkViewConditions(normalized.Views); err != nil {
		return err
	}

	for _, event := range normalized.Events {
		id := string(event.EntityID)

		if repo.events[id] == nil {
			var empty []evt.SerializedEvent
			repo.events[id] = empty
		}

		repo.events[id] = append(repo.events[id], event)
		repo.heads[id] = max(repo.heads[id], event.Sequence)
	}

	for _, snapshot := range normalized.Snapshots {
		repo.snapshots[string(snapshot.EntityID)] = snapshot
	}

	repo.applyViews(normalized.Views)

	if outcome := normalized.Outcome; outcome != nil {
		if repo.features.outcomes == nil {
			repo.features.outcomes = make(map[evt.CommandID]evt.Outcome)
		}

		repo.features.outcomes[outcome.CommandID] = cloneOutcome(*outcome)
	}

	return repo.applyTransactions(normalized.Transaction)
}

// GetOutcome reads a command's outcome row, or nil when none exists. The in-memory repository is
// strongly consistent by construction. It implements evt.OutcomeRepository.
func (repo Repository) GetOutcome(_ context.Context, id evt.CommandID) (*evt.Outcome, error) {
	// The same typed validation as the durable backends: an empty, non-UTF-8, or oversized ID is a
	// programmer error, not an absent row.
	if err := evt.CheckCommandID(id); err != nil {
		return nil, err
	}

	repo.features.mu.Lock()
	defer repo.features.mu.Unlock()

	stored, ok := repo.features.outcomes[id]
	if !ok {
		return nil, nil
	}

	outcome := cloneOutcome(stored)

	return &outcome, nil
}

// cloneOutcome copies an outcome so neither the stored row nor the caller's value aliases the other.
func cloneOutcome(outcome evt.Outcome) evt.Outcome {
	outcome.Response = slices.Clone(outcome.Response)
	outcome.EventIDs = slices.Clone(outcome.EventIDs)

	if outcome.Rejection != nil {
		// A JSON round trip copies nested maps and slices too, and yields the form the durable
		// backends return, so mutating a value the caller holds cannot change the stored row.
		if canonical, err := evt.CanonicalRejection(*outcome.Rejection); err == nil {
			outcome.Rejection = &canonical
		} else {
			rejection := *outcome.Rejection
			rejection.Details = maps.Clone(rejection.Details)
			outcome.Rejection = &rejection
		}
	}

	return outcome
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

// validateLocked refuses a manifest that declares a feature this build does not model, and a store
// that holds data without declaring stream-heads (ADR 0007). The in-memory repository models no
// extensions beyond stream-heads, like the default Go writer. A store that holds no data yet
// declares stream-heads itself. The caller holds mu.
func (repo Repository) validateLocked() error {
	supported := evt.SupportedFeatures(nil).WithStreamHeads()

	if repo.features.manifest != nil {
		if err := repo.features.manifest.Validate(supported); err != nil {
			return err
		}

		if repo.features.manifest.DeclaresStreamHeads() {
			return nil
		}
	}

	if len(repo.events) > 0 || len(repo.snapshots) > 0 {
		return &evt.StreamHeadsNotMigratedError{}
	}

	next := evt.FeatureManifest{Features: map[string]evt.FeatureFormat{}}
	if repo.features.manifest != nil {
		next = cloneManifest(*repo.features.manifest)
	}

	if next.Features == nil {
		next.Features = map[string]evt.FeatureFormat{}
	}

	next.Features[evt.FeatureStreamHeads] = evt.StreamHeadsFormat
	next.Version++
	repo.features.manifest = &next

	return nil
}

// Initialize declares stream-heads on a repository that holds no events and no snapshots, and
// reports whether this call declared it (evt.StoreInitializer). The in-memory repository also
// initializes a fresh store on its first write, so this is only needed by code that treats every
// backend alike. A store that already declares the feature is left alone; one that holds data
// without it is refused with *evt.StreamHeadsNotMigratedError.
func (repo Repository) Initialize(context.Context) (bool, error) {
	repo.features.mu.Lock()
	defer repo.features.mu.Unlock()

	supported := evt.SupportedFeatures(nil).WithStreamHeads()
	if manifest := repo.features.manifest; manifest != nil {
		if err := manifest.Validate(supported); err != nil {
			return false, err
		}

		if manifest.DeclaresStreamHeads() {
			return false, nil
		}
	}

	if err := repo.validateLocked(); err != nil {
		return false, err
	}

	return true, nil
}

// requireStreamHeadsLocked refuses a store that does not declare stream-heads, without declaring
// it. The caller holds mu.
func (repo Repository) requireStreamHeadsLocked() error {
	if repo.features.manifest != nil {
		if err := repo.features.manifest.Validate(evt.SupportedFeatures(nil).WithStreamHeads()); err != nil {
			return err
		}
	}

	return evt.RequireStreamHeads(repo.features.manifest)
}

// derivedHead is a stream's head as its log derives it: the larger of its highest stored event
// sequence and its snapshot's event sequence (invariant H4).
func (repo Repository) derivedHead(id string) evt.EventSequence {
	var head evt.EventSequence
	for _, stored := range repo.events[id] {
		head = max(head, stored.Sequence)
	}

	if snapshot, ok := repo.snapshots[id]; ok {
		head = max(head, snapshot.EventSequence)
	}

	return head
}

// checkHeads evaluates every head condition of a commit — the head update of each written stream
// and the head check of each fenced one — and reports the highest-ranked failure: drift, then a
// gap, then the first conflict (storage spec, Stream Heads, Classification). It is the in-memory
// form of the head updates and fence checks; the log is read under the manifest lock, so no commit
// can land between the reads and the probe's verdict is certain.
func (repo Repository) checkHeads(commit evt.SerializedResult) error {
	var drift, gap, conflict error

	firsts := map[evt.EntityID]evt.EventSequence{}
	lasts := map[evt.EntityID]evt.EventSequence{}

	var order []evt.EntityID

	for _, event := range commit.Events {
		if _, seen := firsts[event.EntityID]; !seen {
			firsts[event.EntityID] = event.Sequence
			order = append(order, event.EntityID)
		}

		lasts[event.EntityID] = event.Sequence
	}

	for _, id := range order {
		first, last := firsts[id], lasts[id]
		stored, present := repo.heads[string(id)]

		if first == 1 && !present || first > 1 && stored == first-1 {
			// The head condition passes. An event already stored at or above the first sequence
			// would fail its put: it sits above the head, which is drift.
			for _, event := range repo.events[string(id)] {
				if event.Sequence >= first && event.Sequence <= last && drift == nil {
					drift = repo.drift(id)
				}
			}

			continue
		}

		derived := repo.derivedHead(string(id))

		switch {
		case stored != derived:
			if drift == nil {
				drift = repo.drift(id)
			}

		case first > 1 && stored < first-1:
			if gap == nil {
				gap = &policy.ClassifiedError{
					Class: policy.ClassTransient,
					Err:   &evt.SequenceGapError{EntityID: id, After: stored, Found: first},
				}
			}

		default:
			if conflict == nil {
				conflict = evt.NewConflictError(fmt.Sprintf(
					"mem: stream %s moved: head %d, commit continues from %d", id, stored, first-1,
				))
			}
		}
	}

	for _, fence := range commit.Fences {
		stored, present := repo.heads[string(fence.EntityID)]
		if stored == fence.Head && (present || fence.Head == 0) {
			continue
		}

		if stored != repo.derivedHead(string(fence.EntityID)) {
			if drift == nil {
				drift = repo.drift(fence.EntityID)
			}

			continue
		}

		if conflict == nil {
			conflict = policy.FenceConflict(fence)
		}
	}

	switch {
	case drift != nil:
		return drift
	case gap != nil:
		return gap
	}

	return conflict
}

// drift builds the typed drift error from the stored and derived heads of a stream.
func (repo Repository) drift(id evt.EntityID) error {
	stored, present := repo.heads[string(id)]

	return &evt.StreamHeadDriftError{
		EntityID:   id,
		Stored:     stored,
		HeadAbsent: !present,
		Derived:    repo.derivedHead(string(id)),
	}
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
	if err := evt.CheckProjectorOperations(result); err != nil {
		return err
	}

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

	if err := repo.requireStreamHeadsLocked(); err != nil {
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
