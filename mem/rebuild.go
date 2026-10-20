package mem

import (
	"context"
	"time"

	"github.com/photon-grove/evt"
)

// memRebuild is the in-memory form of the rebuild-lock row and the generation item (storage spec,
// Rebuild Fence). The zero value is a store that never ran a rebuild: no lock, generation 0.
type memRebuild struct {
	lock          *evt.RebuildLock
	generation    int64
	forceReleased bool
}

// state returns the fence's state; the caller holds mu.
func (r *memRebuild) state() evt.RebuildState {
	state := evt.RebuildState{Generation: evt.RebuildGeneration{Value: r.generation, ForceReleased: r.forceReleased}}
	if r.lock != nil {
		lock := *r.lock
		state.Lock = &lock
	}

	return state
}

// unsettled reports whether commits are refused: a lock is held, or a force-release left the
// generation odd. The caller holds mu.
func (r *memRebuild) unsettled() bool {
	return r.lock != nil || r.generation%2 != 0
}

// WithClock returns a copy of the repository, sharing its streams, that reads the given clock for
// rebuild lease expiry. Intended for tests; production callers leave it unset to use time.Now.
func (repo Repository) WithClock(now func() time.Time) *Repository {
	repo.now = now

	return &repo
}

func (repo Repository) clock() time.Time {
	if repo.now != nil {
		return repo.now()
	}

	return time.Now()
}

// RebuildFence returns the fence over this repository (evt.RebuildFenceProvider). Handles from
// copies of the repository share one fence.
func (repo Repository) RebuildFence() evt.RebuildFence {
	return memFence{repo: repo}
}

// memFence is the in-memory evt.RebuildFence.
type memFence struct{ repo Repository }

var _ evt.RebuildFence = memFence{}

// Generation reads the generation item.
func (f memFence) Generation(_ context.Context) (evt.RebuildGeneration, error) {
	f.repo.features.mu.Lock()
	defer f.repo.features.mu.Unlock()

	return f.repo.features.rebuild.state().Generation, nil
}

// State reads the generation and the lock row.
func (f memFence) State(_ context.Context) (evt.RebuildState, error) {
	f.repo.features.mu.Lock()
	defer f.repo.features.mu.Unlock()

	return f.repo.features.rebuild.state(), nil
}

// Begin acquires the lock or takes over an expired lease.
func (f memFence) Begin(_ context.Context, owner string, lease time.Duration) (evt.RebuildLease, error) {
	seconds, err := f.validate(owner, lease)
	if err != nil {
		return evt.RebuildLease{}, err
	}

	f.repo.features.mu.Lock()
	defer f.repo.features.mu.Unlock()

	now := f.repo.clock()
	rebuild := &f.repo.features.rebuild

	plan, err := evt.PlanRebuildBegin(rebuild.state(), now)
	if err != nil {
		return evt.RebuildLease{}, err
	}

	expiry := time.Unix(now.Unix()+seconds, 0)
	rebuild.generation += plan.Increment
	rebuild.lock = &evt.RebuildLock{Owner: owner, Generation: rebuild.generation, LeaseExpiresAt: expiry}

	return evt.RebuildLease{Owner: owner, Generation: rebuild.generation, LeaseExpiresAt: expiry}, nil
}

// holds reports whether the lock row is the lease's: the same owner under the same generation. A
// takeover by the same owner token is a later generation, so the stale incarnation's lease is not
// the new holder's. The caller holds mu.
func (r *memRebuild) holds(lease evt.RebuildLease) bool {
	return r.lock != nil && r.lock.Owner == lease.Owner && r.lock.Generation == lease.Generation
}

// Renew extends the holder's lease.
func (f memFence) Renew(_ context.Context, lease evt.RebuildLease, extend time.Duration) (evt.RebuildLease, error) {
	seconds, err := f.validate(lease.Owner, extend)
	if err != nil {
		return evt.RebuildLease{}, err
	}

	f.repo.features.mu.Lock()
	defer f.repo.features.mu.Unlock()

	rebuild := &f.repo.features.rebuild
	if !rebuild.holds(lease) {
		return evt.RebuildLease{}, &evt.RebuildLeaseLostError{Owner: lease.Owner}
	}

	expiry := time.Unix(f.repo.clock().Unix()+seconds, 0)
	rebuild.lock.LeaseExpiresAt = expiry
	lease.LeaseExpiresAt = expiry

	return lease, nil
}

// End releases the lock and settles the generation.
func (f memFence) End(_ context.Context, lease evt.RebuildLease) (evt.RebuildGeneration, error) {
	if err := evt.CheckRebuildOwner(lease.Owner); err != nil {
		return evt.RebuildGeneration{}, err
	}

	f.repo.features.mu.Lock()
	defer f.repo.features.mu.Unlock()

	rebuild := &f.repo.features.rebuild
	if !rebuild.holds(lease) {
		return evt.RebuildGeneration{}, &evt.RebuildLeaseLostError{Owner: lease.Owner}
	}

	rebuild.lock = nil
	rebuild.generation++
	rebuild.forceReleased = false

	return rebuild.state().Generation, nil
}

// ForceRelease releases the named owner's lock and leaves the generation odd and flagged, so
// commits stay refused until a rebuild ends.
func (f memFence) ForceRelease(_ context.Context, owner string) (evt.RebuildGeneration, error) {
	if err := evt.CheckRebuildOwner(owner); err != nil {
		return evt.RebuildGeneration{}, err
	}

	f.repo.features.mu.Lock()
	defer f.repo.features.mu.Unlock()

	rebuild := &f.repo.features.rebuild
	if rebuild.lock == nil || rebuild.lock.Owner != owner {
		return evt.RebuildGeneration{}, &evt.RebuildLeaseLostError{Owner: owner}
	}

	rebuild.lock = nil
	rebuild.generation += 2
	rebuild.forceReleased = true

	return rebuild.state().Generation, nil
}

// WriteViews writes one batch of view operations under the lease guard: the owner must still hold
// an unexpired lease, and the batch is checked and applied in one critical section.
func (f memFence) WriteViews(_ context.Context, lease evt.RebuildLease, views []evt.ProjectedView) error {
	if err := evt.CheckRebuildOwner(lease.Owner); err != nil {
		return err
	}

	if err := evt.CheckRebuildViewBatch(views); err != nil {
		return err
	}

	f.repo.features.mu.Lock()
	defer f.repo.features.mu.Unlock()

	rebuild := &f.repo.features.rebuild
	if !rebuild.holds(lease) || rebuild.lock.LeaseExpiresAt.Unix() <= f.repo.clock().Unix() {
		return &evt.RebuildLeaseLostError{Owner: lease.Owner}
	}

	if err := f.repo.checkViewConditions(views); err != nil {
		return err
	}

	f.repo.applyViews(views)

	return nil
}

func (f memFence) validate(owner string, lease time.Duration) (int64, error) {
	if err := evt.CheckRebuildOwner(owner); err != nil {
		return 0, err
	}

	return evt.RebuildLeaseSeconds(lease)
}
