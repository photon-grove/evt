package snapshots

import (
	"context"

	"github.com/photon-grove/evt"
)

const defaultReplayEstimatePerEventMS = 8.0

// FreshnessSample captures replay debt metrics for a loaded entity.
type FreshnessSample struct {
	EntityType           string
	EntityID             string
	SnapshotSequence     int64
	EventSequence        int64
	EventsSinceSnapshot  int
	EstimatedReplayMS    int
	ReplayBudgetExceeded bool
	CatchUpApplied       bool
	CatchUpError         string
}

// FreshnessObserver receives snapshot freshness samples from LoadEntity.
type FreshnessObserver func(context.Context, FreshnessSample)

// WithFreshnessObserver registers an observer used to emit freshness metrics.
func (store *Store) WithFreshnessObserver(observer FreshnessObserver) *Store {
	store.freshnessObserver = observer
	return store
}

// WithReplayEstimatePerEventMS configures replay-duration estimation.
func (store *Store) WithReplayEstimatePerEventMS(ms float64) *Store {
	if ms > 0 {
		store.replayEstimatePerEventMS = ms
	}
	return store
}

// WithReplayBudgetEvents configures the events-since-snapshot budget. Values <= 0 disable budget checks.
func (store *Store) WithReplayBudgetEvents(maxEvents int) *Store {
	store.replayBudgetEvents = maxEvents
	return store
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
