package conformance

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/policy"
	"github.com/photon-grove/evt/go/snapshots"
	"github.com/photon-grove/evt/go/test"
)

// rebuildClock is a settable clock for lease expiry, in whole Unix seconds.
type rebuildClock struct{ seconds atomic.Int64 }

func newRebuildClock() *rebuildClock {
	clock := &rebuildClock{}
	clock.seconds.Store(time.Now().Unix())

	return clock
}

func (c *rebuildClock) Now() time.Time { return time.Unix(c.seconds.Load(), 0) }

func (c *rebuildClock) Advance(seconds int64) { c.seconds.Add(seconds) }

// rebuildEnv is one repository handle with a controllable clock and its rebuild fence. The fence
// is store-wide state, so every case settles it again when it ends: a lock left behind would
// refuse the commits of the cases that follow.
type rebuildEnv struct {
	t     *testing.T
	clock *rebuildClock
	repo  evt.Repository
	fence evt.RebuildFence
	ns    string

	// leases are the leases the case's workers hold, by name.
	leases map[string]evt.RebuildLease
}

func newRebuildEnv(t *testing.T, newRepo func() evt.Repository, opts SuiteOptions) *rebuildEnv {
	t.Helper()

	require.NotNil(t, opts.WithRebuildClock, "a backend claiming the rebuild fence must supply SuiteOptions.WithRebuildClock")

	clock := newRebuildClock()
	repo := opts.WithRebuildClock(newRepo(), clock.Now)

	provider, ok := repo.(evt.RebuildFenceProvider)
	require.True(t, ok, "%T must implement evt.RebuildFenceProvider", repo)

	env := &rebuildEnv{t: t, clock: clock, repo: repo, fence: provider.RebuildFence(), ns: newNamespace(), leases: map[string]evt.RebuildLease{}}

	// Start from a settled store, whatever an earlier case left, and leave it settled.
	env.settle()
	t.Cleanup(env.settle)

	return env
}

// settle ends whatever rebuild is in progress and brings the generation back to an even value.
func (e *rebuildEnv) settle() {
	e.t.Helper()
	ctx := context.Background()

	state, err := e.fence.State(ctx)
	require.NoError(e.t, err)

	if state.Lock != nil {
		_, err = e.fence.ForceRelease(ctx, state.Lock.Owner)
		require.NoError(e.t, err)
	}

	generation, err := e.fence.Generation(ctx)
	require.NoError(e.t, err)

	if !generation.Settled() {
		settling, err := e.fence.Begin(ctx, e.ns+"-settle", time.Minute)
		require.NoError(e.t, err)

		_, err = e.fence.End(ctx, settling)
		require.NoError(e.t, err)
	}
}

func (e *rebuildEnv) generation() evt.RebuildGeneration {
	e.t.Helper()

	generation, err := e.fence.Generation(context.Background())
	require.NoError(e.t, err)

	return generation
}

func (e *rebuildEnv) owner(name string) string { return e.ns + "-" + name }

// lease is the lease the named worker holds. A worker that never began presents a lease with a
// generation no lock holds.
func (e *rebuildEnv) lease(name string) evt.RebuildLease {
	if lease, ok := e.leases[name]; ok {
		return lease
	}

	return evt.RebuildLease{Owner: e.owner(name), Generation: 999_999}
}

func (e *rebuildEnv) begin(name string, lease time.Duration) evt.RebuildLease {
	e.t.Helper()

	started, err := e.fence.Begin(context.Background(), e.owner(name), lease)
	require.NoError(e.t, err)

	e.leases[name] = started

	return started
}

// commit writes one event to a fresh stream of the case.
func (e *rebuildEnv) commit(name string) error {
	e.t.Helper()

	id := evt.EntityID(e.ns + "-" + name)

	return e.repo.Commit(context.Background(), evt.SerializedResult{
		Events: serialize(e.t, id, "test", 0, &test.AggregateCreated{ID: id, Value: name}),
	})
}

// requireRefused asserts err is a commit refused by the rebuild-lock check: transient, never a
// conflict, and matching the sentinel.
func requireRefused(t *testing.T, err error) {
	t.Helper()

	require.ErrorIs(t, err, evt.ErrRebuildInProgress)
	require.True(t, evt.IsRebuildInProgressErr(err))
	require.True(t, policy.IsTransient(err), "a refused commit is retried later")
	require.False(t, evt.IsOptimisticConflict(err), "and is never a conflict, so fence-aware retry never exhausts it into a rejection")
}

// RunRebuildFenceSuite runs the rebuild-fence cases alone, for a backend or an emulation of one that
// runs them without the rest of the repository suite. RunRepositorySuite runs them too for any
// backend that supports transactions. The options needed are WithRebuildClock, plus ReadView and
// SupportsViews for the guarded view writes, SupportsOutcomes for the outcome and engine cases,
// BypassHead for the drift case, and DeclareRetention for the manifest-only refusal.
func RunRebuildFenceSuite(t *testing.T, newRepo func() evt.Repository, opts SuiteOptions) {
	t.Helper()

	runRebuildFenceCases(t, newRepo, opts)
}

// runRebuildFenceCases verifies the durable rebuild fence (ADR 0004, C10) on a backend that
// provides evt.RebuildFenceProvider: the lock and generation lifecycle, the commit-time lock check
// and its precedence, the reader epoch check, and guarded view writes.
func runRebuildFenceCases(t *testing.T, newRepo func() evt.Repository, opts SuiteOptions) {
	t.Helper()

	t.Run("RebuildSettledStoreAcceptsCommits", func(t *testing.T) { testRebuildSettledStore(t, newRebuildEnv(t, newRepo, opts)) })
	t.Run("RebuildLifecycle", func(t *testing.T) { testRebuildLifecycle(t, newRebuildEnv(t, newRepo, opts)) })
	t.Run("RebuildLeaseExpiryAndTakeover", func(t *testing.T) { testRebuildTakeover(t, newRebuildEnv(t, newRepo, opts)) })
	t.Run("RebuildStaleIncarnation", func(t *testing.T) { testRebuildStaleIncarnation(t, newRebuildEnv(t, newRepo, opts), opts) })
	t.Run("RebuildForceRelease", func(t *testing.T) { testRebuildForceRelease(t, newRebuildEnv(t, newRepo, opts)) })
	t.Run("RebuildRejectsBadArguments", func(t *testing.T) { testRebuildArguments(t, newRebuildEnv(t, newRepo, opts)) })
	t.Run("RebuildRefusesCommits", func(t *testing.T) { testRebuildRefusesCommits(t, newRebuildEnv(t, newRepo, opts), opts) })
	t.Run("RebuildForceReleasedRefusesCommits", func(t *testing.T) {
		testRebuildForceReleasedRefusesCommits(t, newRebuildEnv(t, newRepo, opts), opts)
	})
	t.Run("RebuildLockCheckPrecedence", func(t *testing.T) { testRebuildPrecedence(t, newRebuildEnv(t, newRepo, opts), opts) })
	t.Run("RebuildEpochCheck", func(t *testing.T) { testRebuildEpochs(t, newRebuildEnv(t, newRepo, opts)) })

	if opts.SupportsViews {
		t.Run("RebuildGuardedViewWrites", func(t *testing.T) { testRebuildGuardedViews(t, newRebuildEnv(t, newRepo, opts), opts) })
	}

	if opts.SupportsOutcomes {
		t.Run("RebuildFenceAndFenceAwareRetry", func(t *testing.T) {
			testRebuildEngineRetry(t, newRebuildEnv(t, newRepo, opts))
		})
	}
}

func testRebuildLifecycle(t *testing.T, env *rebuildEnv) {
	ctx := context.Background()

	before := env.generation()
	require.True(t, before.Settled())
	require.False(t, before.ForceReleased)

	state, err := env.fence.State(ctx)
	require.NoError(t, err)
	require.False(t, state.InProgress())
	require.Equal(t, before, state.Generation)

	// Begin acquires: the generation grows by one and is odd, and the lock names the owner.
	started := env.begin("a", time.Minute)
	require.Equal(t, env.owner("a"), started.Owner)
	require.Equal(t, before.Value+1, started.Generation)
	require.Equal(t, env.clock.Now().Unix()+60, started.LeaseExpiresAt.Unix())

	state, err = env.fence.State(ctx)
	require.NoError(t, err)
	require.True(t, state.InProgress())
	require.Equal(t, evt.RebuildGeneration{Value: before.Value + 1}, state.Generation)
	require.Equal(t, env.owner("a"), state.Lock.Owner)
	require.Equal(t, started.LeaseExpiresAt.Unix(), state.Lock.LeaseExpiresAt.Unix())

	// Another owner is refused while the lease is held, naming the holder.
	_, err = env.fence.Begin(ctx, env.owner("b"), time.Minute)

	var held *evt.RebuildLockHeldError
	require.ErrorAs(t, err, &held)
	require.Equal(t, env.owner("a"), held.Owner)
	require.Equal(t, started.LeaseExpiresAt.Unix(), held.LeaseExpiresAt.Unix())
	require.Equal(t, before.Value+1, env.generation().Value, "a refused Begin changes nothing")

	// The same owner cannot begin twice either: it renews.
	_, err = env.fence.Begin(ctx, env.owner("a"), time.Minute)
	require.True(t, evt.IsRebuildLockHeldErr(err))

	// Renew extends the lease to the new duration from now, and only for the holder.
	env.clock.Advance(30)

	renewed, err := env.fence.Renew(ctx, env.lease("a"), 2*time.Minute)
	require.NoError(t, err)
	require.Equal(t, env.clock.Now().Unix()+120, renewed.LeaseExpiresAt.Unix())
	require.Equal(t, started.Generation, renewed.Generation, "renewing keeps the fencing generation")
	require.Equal(t, started.Owner, renewed.Owner)

	state, err = env.fence.State(ctx)
	require.NoError(t, err)
	require.Equal(t, renewed.LeaseExpiresAt.Unix(), state.Lock.LeaseExpiresAt.Unix())
	require.Equal(t, started.Generation, state.Lock.Generation, "the lock row holds the lease's fencing generation")
	require.Equal(t, before.Value+1, state.Generation.Value, "renewing does not touch the generation")

	_, err = env.fence.Renew(ctx, env.lease("b"), time.Minute)
	require.True(t, evt.IsRebuildLeaseLostErr(err))

	_, err = env.fence.End(ctx, env.lease("b"))
	require.True(t, evt.IsRebuildLeaseLostErr(err), "only the holder ends a rebuild")

	// End releases: the generation is even again, the lock is gone, and nothing is flagged.
	settled, err := env.fence.End(ctx, env.lease("a"))
	require.NoError(t, err)
	require.Equal(t, evt.RebuildGeneration{Value: before.Value + 2}, settled)
	require.Equal(t, settled, env.generation())

	state, err = env.fence.State(ctx)
	require.NoError(t, err)
	require.False(t, state.InProgress())

	_, err = env.fence.End(ctx, env.lease("a"))
	require.True(t, evt.IsRebuildLeaseLostErr(err), "a second End finds no lock")

	_, err = env.fence.Renew(ctx, env.lease("a"), time.Minute)
	require.True(t, evt.IsRebuildLeaseLostErr(err))

	// A new rebuild starts from the settled value, whoever ran the last one.
	again := env.begin("b", time.Minute)
	require.Equal(t, before.Value+3, again.Generation)

	_, err = env.fence.End(ctx, env.lease("b"))
	require.NoError(t, err)
}

func testRebuildTakeover(t *testing.T, env *rebuildEnv) {
	ctx := context.Background()
	before := env.generation()

	first := env.begin("a", 10*time.Second)

	// A lease is still held at the second it expires.
	env.clock.Advance(10)

	_, err := env.fence.Begin(ctx, env.owner("b"), time.Minute)
	require.True(t, evt.IsRebuildLockHeldErr(err), "the lease is held through its expiry second")

	// An expired lease still blocks commits until it is taken over.
	env.clock.Advance(1)
	requireRefused(t, env.commit("expired"))

	// Taking over replaces the owner and bumps the generation by two: it stays odd, and changed.
	taken := env.begin("b", time.Minute)
	require.Equal(t, first.Generation+2, taken.Generation)
	require.Equal(t, before.Value+3, taken.Generation)
	require.False(t, env.generation().Settled())

	state, err := env.fence.State(ctx)
	require.NoError(t, err)
	require.Equal(t, env.owner("b"), state.Lock.Owner)

	// The displaced worker can do nothing: its renew, its release, and its next batch fail.
	_, err = env.fence.Renew(ctx, env.lease("a"), time.Minute)
	require.True(t, evt.IsRebuildLeaseLostErr(err))

	_, err = env.fence.End(ctx, env.lease("a"))
	require.True(t, evt.IsRebuildLeaseLostErr(err))

	requireRefused(t, env.commit("taken"))

	// The new owner ends the rebuild: odd to even, one step.
	settled, err := env.fence.End(ctx, env.lease("b"))
	require.NoError(t, err)
	require.Equal(t, evt.RebuildGeneration{Value: before.Value + 4}, settled)
	require.NoError(t, env.commit("after-takeover"))

	// A renewal keeps a lease from being taken over.
	env.begin("c", 10*time.Second)
	env.clock.Advance(8)

	_, err = env.fence.Renew(ctx, env.lease("c"), 10*time.Second)
	require.NoError(t, err)

	env.clock.Advance(8)

	_, err = env.fence.Begin(ctx, env.owner("d"), time.Minute)
	require.True(t, evt.IsRebuildLockHeldErr(err))

	_, err = env.fence.End(ctx, env.lease("c"))
	require.NoError(t, err)
}

// A lease binds its owner token and its fencing generation. When one worker's lease lapses and a
// later incarnation of the worker takes over under the same owner token, the first incarnation
// can neither renew, write, nor end the lease: only the generation tells them apart.
func testRebuildStaleIncarnation(t *testing.T, env *rebuildEnv, opts SuiteOptions) {
	ctx := context.Background()
	pk := env.ns + "#incarnations"

	put := func(sk string) evt.ProjectedView {
		return evt.ProjectedView{Projector: "rebuild", Op: evt.PutView(pk, sk, "owner", "test", []byte(`{}`))}
	}

	const token = "worker"

	stale, err := env.fence.Begin(ctx, env.owner(token), 10*time.Second)
	require.NoError(t, err)

	env.clock.Advance(11)

	fresh, err := env.fence.Begin(ctx, env.owner(token), time.Minute)
	require.NoError(t, err, "an expired lease is taken over, even by the same owner token")
	require.Equal(t, stale.Owner, fresh.Owner)
	require.Equal(t, stale.Generation+2, fresh.Generation)

	before, err := env.fence.State(ctx)
	require.NoError(t, err)
	require.Equal(t, fresh.Generation, before.Lock.Generation)

	// The stale incarnation changes nothing.
	_, err = env.fence.Renew(ctx, stale, time.Hour)
	require.True(t, evt.IsRebuildLeaseLostErr(err), "renew: %v", err)

	if opts.SupportsViews {
		err = env.fence.WriteViews(ctx, stale, []evt.ProjectedView{put("stale")})
		require.True(t, evt.IsRebuildLeaseLostErr(err), "write views: %v", err)
		requireNoRow(t, opts.ReadView, env.repo, pk, "stale")
	}

	_, err = env.fence.End(ctx, stale)
	require.True(t, evt.IsRebuildLeaseLostErr(err), "end: %v", err)

	after, err := env.fence.State(ctx)
	require.NoError(t, err)
	require.Equal(t, before, after, "the new holder's lock, expiry, and generation are untouched")

	// The new incarnation can.
	renewed, err := env.fence.Renew(ctx, fresh, 2*time.Minute)
	require.NoError(t, err)
	require.Equal(t, env.clock.Now().Unix()+120, renewed.LeaseExpiresAt.Unix())

	if opts.SupportsViews {
		require.NoError(t, env.fence.WriteViews(ctx, renewed, []evt.ProjectedView{put("fresh")}))
		requireRow(t, opts.ReadView, env.repo, pk, "fresh", `{}`)
	}

	settled, err := env.fence.End(ctx, renewed)
	require.NoError(t, err)
	require.Equal(t, fresh.Generation+1, settled.Value)

	// A lease does not outlive its rebuild: after End, the same owner token begins again under a
	// new generation, and both earlier leases are dead.
	again, err := env.fence.Begin(ctx, env.owner(token), time.Minute)
	require.NoError(t, err)
	require.Equal(t, settled.Value+1, again.Generation)

	for name, old := range map[string]evt.RebuildLease{"stale": stale, "ended": fresh} {
		_, err = env.fence.Renew(ctx, old, time.Hour)
		require.True(t, evt.IsRebuildLeaseLostErr(err), name)

		_, err = env.fence.End(ctx, old)
		require.True(t, evt.IsRebuildLeaseLostErr(err), name)
	}

	_, err = env.fence.End(ctx, again)
	require.NoError(t, err)
}

func testRebuildForceRelease(t *testing.T, env *rebuildEnv) {
	ctx := context.Background()
	before := env.generation()

	env.begin("a", time.Hour)

	// Only the owner the operator names is released.
	_, err := env.fence.ForceRelease(ctx, env.owner("b"))
	require.True(t, evt.IsRebuildLeaseLostErr(err))

	requireRefused(t, env.commit("locked"))

	forced, err := env.fence.ForceRelease(ctx, env.owner("a"))
	require.NoError(t, err)
	require.Equal(t, evt.RebuildGeneration{Value: before.Value + 3, ForceReleased: true}, forced)
	require.Equal(t, forced, env.generation())

	state, err := env.fence.State(ctx)
	require.NoError(t, err)
	require.False(t, state.InProgress(), "no rebuild holds the lock")

	// Readers stay fenced and commits stay refused: the generation is odd and flagged, and the
	// derived state the released rebuild left may be half-written.
	requireRefused(t, env.commit("force-released"))

	_, err = evt.ObserveRebuildEpoch(ctx, env.fence)

	var unavailable *evt.RebuildUnavailableError
	require.ErrorAs(t, err, &unavailable)
	require.True(t, unavailable.Generation.ForceReleased)

	_, err = env.fence.ForceRelease(ctx, env.owner("a"))
	require.True(t, evt.IsRebuildLeaseLostErr(err), "there is no lock left to release")

	_, err = env.fence.End(ctx, env.lease("a"))
	require.True(t, evt.IsRebuildLeaseLostErr(err))

	// A new rebuild acquires from the force-released odd generation, adding two so it stays odd. The
	// flag stays until that rebuild ends.
	acquired := env.begin("c", time.Hour)
	require.Equal(t, before.Value+5, acquired.Generation)
	require.True(t, env.generation().ForceReleased)
	requireRefused(t, env.commit("rebuilding"))

	// Ending it settles the generation and clears the flag, so readers trust derived state again
	// and commits resume.
	settled, err := env.fence.End(ctx, env.lease("c"))
	require.NoError(t, err)
	require.Equal(t, evt.RebuildGeneration{Value: before.Value + 6}, settled)
	require.NoError(t, env.commit("force-released"))

	epoch, err := evt.ObserveRebuildEpoch(ctx, env.fence)
	require.NoError(t, err)
	require.Equal(t, before.Value+6, epoch.Generation)

	// A force-released rebuild can itself be force-released again from a later rebuild.
	env.begin("d", time.Hour)

	again, err := env.fence.ForceRelease(ctx, env.owner("d"))
	require.NoError(t, err)
	require.Equal(t, evt.RebuildGeneration{Value: before.Value + 9, ForceReleased: true}, again)
}

func testRebuildArguments(t *testing.T, env *rebuildEnv) {
	ctx := context.Background()
	before := env.generation()

	for name, call := range map[string]func() error{
		"empty owner": func() error {
			_, err := env.fence.Begin(ctx, "", time.Minute)

			return err
		},
		"oversized owner": func() error {
			_, err := env.fence.Begin(ctx, string(make([]byte, evt.MaxRebuildOwnerBytes+1)), time.Minute)

			return err
		},
		"owner with U+0000": func() error {
			_, err := env.fence.Begin(ctx, "job\x00", time.Minute)

			return err
		},
		"non-UTF-8 owner": func() error {
			_, err := env.fence.Begin(ctx, "\xff", time.Minute)

			return err
		},
		"zero lease": func() error {
			_, err := env.fence.Begin(ctx, env.owner("a"), 0)

			return err
		},
		"sub-second lease": func() error {
			_, err := env.fence.Begin(ctx, env.owner("a"), 999*time.Millisecond)

			return err
		},
		"renew with a zero lease": func() error {
			_, err := env.fence.Renew(ctx, env.lease("a"), 0)

			return err
		},
		"end with an empty owner": func() error {
			_, err := env.fence.End(ctx, evt.RebuildLease{Generation: 1})

			return err
		},
		"force-release with an empty owner": func() error {
			_, err := env.fence.ForceRelease(ctx, "")

			return err
		},
	} {
		require.ErrorIs(t, call(), evt.ErrInvalidArgument, name)
	}

	require.Equal(t, before, env.generation(), "refused calls change nothing")

	// A lease is rounded up to whole seconds.
	started := env.begin("a", 1500*time.Millisecond)
	require.Equal(t, env.clock.Now().Unix()+2, started.LeaseExpiresAt.Unix())
}

func testRebuildRefusesCommits(t *testing.T, env *rebuildEnv, opts SuiteOptions) {
	ctx := context.Background()

	// A stream that exists before the rebuild, a fence target.
	existing := evt.EntityID(env.ns + "-existing")
	commitEvents(t, env.repo, existing, "test", 0, &test.AggregateCreated{ID: existing, Value: "before"})

	env.begin("worker", time.Hour)

	// A plain commit, a multi-stream commit, a fenced commit, an outcome-only commit, and an
	// accepted outcome with events are all refused, and write nothing.
	plain := evt.EntityID(env.ns + "-plain")
	requireRefused(t, env.repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, plain, "test", 0, &test.AggregateCreated{ID: plain, Value: "plain"}),
	}))
	requireNoEvents(t, env.repo, plain)

	left, right := evt.EntityID(env.ns+"-left"), evt.EntityID(env.ns+"-right")
	requireRefused(t, env.repo.Commit(ctx, evt.SerializedResult{
		Events: append(
			serialize(t, left, "test", 0, &test.AggregateCreated{ID: left, Value: "l"}),
			serialize(t, right, "test", 0, &test.AggregateCreated{ID: right, Value: "r"})...,
		),
	}))
	requireNoEvents(t, env.repo, left)
	requireNoEvents(t, env.repo, right)

	// A fence is not a stale-fence verdict: the lock check outranks it, even when the fence is stale.
	fenced := evt.EntityID(env.ns + "-fenced")
	for _, head := range []evt.EventSequence{1, 7} {
		err := env.repo.Commit(ctx, evt.SerializedResult{
			Events: serialize(t, fenced, "test", 0, &test.AggregateCreated{ID: fenced, Value: "f"}),
			Fences: []evt.ReadFence{{EntityID: existing, EntityType: "test", Head: head}},
		})
		requireRefused(t, err)
		var fenceConflict *evt.FenceConflictError
		require.False(t, errors.As(err, &fenceConflict), "a rebuild is not a fence conflict")
	}

	requireNoEvents(t, env.repo, fenced)

	if opts.SupportsOutcomes {
		outcome := &evt.Outcome{CommandID: evt.CommandID(env.ns + "-only"), Status: evt.OutcomeAccepted, Digest: "digest"}
		requireRefused(t, env.repo.Commit(ctx, evt.SerializedResult{Outcome: outcome}))

		outcomes, ok := env.repo.(evt.OutcomeRepository)
		require.True(t, ok)

		stored, err := outcomes.GetOutcome(ctx, outcome.CommandID)
		require.NoError(t, err)
		require.Nil(t, stored, "a refused outcome-only commit records nothing")

		withEvents := &evt.Outcome{CommandID: evt.CommandID(env.ns + "-events"), Status: evt.OutcomeAccepted, Digest: "digest"}
		requireRefused(t, env.repo.Commit(ctx, evt.SerializedResult{
			Events:  serialize(t, plain, "test", 0, &test.AggregateCreated{ID: plain, Value: "plain"}),
			Outcome: withEvents,
		}))
		requireNoEvents(t, env.repo, plain)
	}

	// A commit-wide projector's view operations are part of the commit: refused with it, so a live
	// commit never interleaves with the rows a rebuild is rewriting.
	if opts.SupportsViews {
		viewed := evt.EntityID(env.ns + "-viewed")
		requireRefused(t, env.repo.Commit(ctx, evt.SerializedResult{
			Events: serialize(t, viewed, "test", 0, &test.AggregateCreated{ID: viewed, Value: "viewed"}),
			Views:  []evt.ProjectedView{{Projector: "live", Op: evt.PutView(env.ns+"#live", "row", viewed, "test", []byte(`{}`))}},
		}))
		requireNoEvents(t, env.repo, viewed)
		requireNoRow(t, opts.ReadView, env.repo, env.ns+"#live", "row")
	}

	// A refusal that needs no read of the lock keeps its own error during a rebuild: a commit over
	// budget, a reserved entity ID, and a projector-only commit.
	over := evt.EntityID(env.ns + "-over")
	events := make([]evt.Event, 0, 120)
	for range 120 {
		events = append(events, &test.AggregateUpdated{ID: over, Value: "x"})
	}

	var tooLarge *evt.CommitTooLargeError
	require.ErrorAs(t, env.repo.Commit(ctx, evt.SerializedResult{Events: serialize(t, over, "test", 0, events...)}), &tooLarge)

	reserved := evt.EntityID("SYSTEM#" + env.ns)
	require.True(t, evt.IsReservedEntityIdErr(env.repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, reserved, "test", 0, &test.AggregateCreated{ID: reserved, Value: "r"}),
	})))

	var projectorOnly *evt.ProjectorOnlyCommitError
	require.ErrorAs(t, env.repo.Commit(ctx, evt.SerializedResult{Transaction: evt.Transaction{projectorOnlyGroup{}}}), &projectorOnly)

	// The lock is released: commits resume, and the same streams commit as if nothing happened.
	_, err := env.fence.End(ctx, env.lease("worker"))
	require.NoError(t, err)
	commitEvents(t, env.repo, plain, "test", 0, &test.AggregateCreated{ID: plain, Value: "plain"})
	require.Len(t, mustEventsOf(t, env.repo, plain), 1)

	// An expired lock still refuses commits, and so does a force-release of it: only a rebuild
	// that ends settles the generation.
	env.begin("short", time.Second)
	env.clock.Advance(5)
	requireRefused(t, env.commit("expired-lock"))

	_, err = env.fence.ForceRelease(ctx, env.owner("short"))
	require.NoError(t, err)
	requireRefused(t, env.commit("after-force-release"))
}

// A settled generation accepts commits, a store that never ran a rebuild included: the commit's
// lock check passes against the absent fence rows, and the commit writes neither of them.
func testRebuildSettledStore(t *testing.T, env *rebuildEnv) {
	ctx := context.Background()

	before, err := env.fence.State(ctx)
	require.NoError(t, err)
	require.True(t, before.Generation.Settled())
	require.Nil(t, before.Lock)

	require.NoError(t, env.commit("settled"))

	after, err := env.fence.State(ctx)
	require.NoError(t, err)
	require.Equal(t, before, after, "a commit never writes the fence")
}

// A rebuild that fails after its first write leaves derived state half-written. The operator
// force-releases it, and every commit — a plain one, one with commit-wide views, an outcome-only
// one — is still refused and writes nothing, so nothing derived from the half-written state (a
// journal position allocated from a wiped counter, say) can be committed. A rebuild that then
// runs to its end settles the generation, and the same commits succeed.
func testRebuildForceReleasedRefusesCommits(t *testing.T, env *rebuildEnv, opts SuiteOptions) {
	ctx := context.Background()
	pk := env.ns + "#half-built"

	put := func(sk string) evt.ProjectedView {
		return evt.ProjectedView{Projector: "rebuild", Op: evt.PutView(pk, sk, "owner", "test", []byte(`{}`))}
	}

	// The rebuild writes its first batch, then fails and never ends.
	env.begin("failed", time.Hour)

	if opts.SupportsViews {
		require.NoError(t, env.fence.WriteViews(ctx, env.lease("failed"), []evt.ProjectedView{put("first")}))
	}

	forced, err := env.fence.ForceRelease(ctx, env.owner("failed"))
	require.NoError(t, err)
	require.True(t, forced.ForceReleased)
	require.False(t, forced.Settled())

	state, err := env.fence.State(ctx)
	require.NoError(t, err)
	require.Nil(t, state.Lock, "no rebuild holds the lock")

	// Every commit is refused with the error a held lock gives, and writes nothing.
	plain := evt.EntityID(env.ns + "-plain")
	requireRefused(t, env.repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, plain, "test", 0, &test.AggregateCreated{ID: plain, Value: "plain"}),
	}))
	requireNoEvents(t, env.repo, plain)

	viewed := evt.EntityID(env.ns + "-viewed")
	live := func() evt.SerializedResult {
		return evt.SerializedResult{
			Events: serialize(t, viewed, "test", 0, &test.AggregateCreated{ID: viewed, Value: "viewed"}),
			Views:  []evt.ProjectedView{{Projector: "live", Op: evt.PutView(env.ns+"#live", "row", viewed, "test", []byte(`{}`))}},
		}
	}

	if opts.SupportsViews {
		requireRefused(t, env.repo.Commit(ctx, live()))
		requireNoEvents(t, env.repo, viewed)
		requireNoRow(t, opts.ReadView, env.repo, env.ns+"#live", "row")
	}

	outcome := &evt.Outcome{CommandID: evt.CommandID(env.ns + "-only"), Status: evt.OutcomeAccepted, Digest: "digest"}

	if opts.SupportsOutcomes {
		requireRefused(t, env.repo.Commit(ctx, evt.SerializedResult{Outcome: outcome}))

		outcomes, ok := env.repo.(evt.OutcomeRepository)
		require.True(t, ok)

		stored, gerr := outcomes.GetOutcome(ctx, outcome.CommandID)
		require.NoError(t, gerr)
		require.Nil(t, stored, "a refused outcome-only commit records nothing")
	}

	// The failed worker writes nothing more: its lease is gone.
	if opts.SupportsViews {
		err = env.fence.WriteViews(ctx, env.lease("failed"), []evt.ProjectedView{put("second")})
		require.True(t, evt.IsRebuildLeaseLostErr(err))
		requireNoRow(t, opts.ReadView, env.repo, pk, "second")
	}

	// A new rebuild runs to its end: commits stay refused while it runs, and resume once it ends.
	env.begin("recovery", time.Hour)
	requireRefused(t, env.repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, plain, "test", 0, &test.AggregateCreated{ID: plain, Value: "plain"}),
	}))

	if opts.SupportsViews {
		require.NoError(t, env.fence.WriteViews(ctx, env.lease("recovery"), []evt.ProjectedView{put("first"), put("second")}))
	}

	settled, err := env.fence.End(ctx, env.lease("recovery"))
	require.NoError(t, err)
	require.True(t, settled.Settled())
	require.False(t, settled.ForceReleased)

	commitEvents(t, env.repo, plain, "test", 0, &test.AggregateCreated{ID: plain, Value: "plain"})
	require.Len(t, mustEventsOf(t, env.repo, plain), 1)

	if opts.SupportsViews {
		require.NoError(t, env.repo.Commit(ctx, live()))
		require.Len(t, mustEventsOf(t, env.repo, viewed), 1)
		row, rerr := opts.ReadView(ctx, env.repo, env.ns+"#live", "row")
		require.NoError(t, rerr)
		require.NotNil(t, row, "the commit's view row")
		require.Equal(t, viewed, row.EntityID)
		requireRow(t, opts.ReadView, env.repo, pk, "second", `{}`)
	}

	if opts.SupportsOutcomes {
		require.NoError(t, env.repo.Commit(ctx, evt.SerializedResult{Outcome: outcome}))
	}
}

func mustEventsOf(t *testing.T, repo evt.Repository, id evt.EntityID) []evt.SerializedEvent {
	t.Helper()

	stored, err := repo.GetEvents(context.Background(), id)
	require.NoError(t, err)

	return stored
}

// The lock check outranks every other failed item of a commit, and a refusal that the manifest
// alone decides precedes it.
func testRebuildPrecedence(t *testing.T, env *rebuildEnv, opts SuiteOptions) {
	ctx := context.Background()

	// A head conflict: the stream moved since the loser read it.
	moved := evt.EntityID(env.ns + "-moved")
	commitEvents(t, env.repo, moved, "test", 0, &test.AggregateCreated{ID: moved, Value: "one"})

	// An existing outcome row for the command.
	var duplicate *evt.Outcome

	if opts.SupportsOutcomes {
		duplicate = &evt.Outcome{CommandID: evt.CommandID(env.ns + "-duplicate"), Status: evt.OutcomeAccepted, Digest: "digest"}
		require.NoError(t, env.repo.Commit(ctx, evt.SerializedResult{Outcome: duplicate}))
	}

	// A gap: the commit starts above the head plus one.
	gap := evt.EntityID(env.ns + "-gap")
	commitEvents(t, env.repo, gap, "test", 0, &test.AggregateCreated{ID: gap, Value: "one"})

	// Drift: an event with no head update.
	drifted := evt.EntityID(env.ns + "-drifted")
	commitEvents(t, env.repo, drifted, "test", 0, &test.AggregateCreated{ID: drifted, Value: "one"})

	if opts.BypassHead != nil {
		bypass := serialize(t, drifted, "test", 1, &test.AggregateUpdated{ID: drifted, Value: "bypass"})
		require.NoError(t, opts.BypassHead(ctx, env.repo, bypass[0]))
	}

	env.begin("worker", time.Hour)

	// Without the lock each of these would fail with its own error; with it, all report the lock.
	stale := serialize(t, moved, "test", 0, &test.AggregateCreated{ID: moved, Value: "again"})
	requireRefused(t, env.repo.Commit(ctx, evt.SerializedResult{Events: stale}))

	skipped := serialize(t, gap, "test", 5, &test.AggregateUpdated{ID: gap, Value: "skipped"})
	requireRefused(t, env.repo.Commit(ctx, evt.SerializedResult{Events: skipped}))

	if duplicate != nil {
		requireRefused(t, env.repo.Commit(ctx, evt.SerializedResult{Outcome: duplicate}))
	}

	if opts.BypassHead != nil {
		next := serialize(t, drifted, "test", 2, &test.AggregateUpdated{ID: drifted, Value: "next"})
		requireRefused(t, env.repo.Commit(ctx, evt.SerializedResult{Events: next}))
	}

	// A stale fence beside an unrelated write: the lock, not a fence conflict.
	other := evt.EntityID(env.ns + "-other")
	requireRefused(t, env.repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, other, "test", 0, &test.AggregateCreated{ID: other, Value: "o"}),
		Fences: []evt.ReadFence{{EntityID: moved, EntityType: "test", Head: 9}},
	}))

	// A fence on a retained type is refused by the manifest alone, before the lock is read.
	if opts.DeclareRetention != nil {
		retained := evt.EntityType(env.ns + "-retained")
		require.NoError(t, opts.DeclareRetention(ctx, env.repo, retained))

		retainedFence := evt.SerializedResult{
			Fences: []evt.ReadFence{{EntityID: evt.EntityID(env.ns + "-r"), EntityType: retained, Head: 0}},
		}

		// The declaration bumped the manifest, so a writer that cached the old version meets its
		// marker check first: the marker outranks the lock, which holds throughout. The retry
		// validates the new manifest and is refused for the retained type.
		err := env.repo.Commit(ctx, retainedFence)
		if errors.Is(err, evt.ErrFeatureManifestChanged) {
			require.NotErrorIs(t, err, evt.ErrRebuildInProgress, "the marker check outranks the lock check")

			err = env.repo.Commit(ctx, retainedFence)
		}

		var refused *evt.RetentionConflictError
		require.ErrorAs(t, err, &refused)
	}

	// Once the lock is gone each commit reports the failure it would have had.
	_, err := env.fence.End(ctx, env.lease("worker"))
	require.NoError(t, err)

	err = env.repo.Commit(ctx, evt.SerializedResult{Events: skipped})
	require.Error(t, err)
	require.NotErrorIs(t, err, evt.ErrRebuildInProgress)

	if duplicate != nil {
		err = env.repo.Commit(ctx, evt.SerializedResult{Outcome: duplicate})

		var conflict *evt.OutcomeConflictError
		require.ErrorAs(t, err, &conflict)
	}
}

func testRebuildEpochs(t *testing.T, env *rebuildEnv) {
	ctx := context.Background()
	before := env.generation()

	// A settled generation is an epoch, and a read under it verifies.
	epoch, err := evt.ObserveRebuildEpoch(ctx, env.fence)
	require.NoError(t, err)
	require.Equal(t, before.Value, epoch.Generation)
	require.NoError(t, epoch.Verify(ctx, env.fence))

	ran := 0
	require.NoError(t, evt.ReadAtRebuildEpoch(ctx, env.fence, func(context.Context) error {
		ran++

		return nil
	}))
	require.Equal(t, 1, ran)

	// A rebuild that starts during the read fails the verify: the generation is odd.
	err = evt.ReadAtRebuildEpoch(ctx, env.fence, func(context.Context) error {
		env.begin("a", time.Hour)

		return nil
	})

	var unavailable *evt.RebuildUnavailableError
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, before.Value+1, unavailable.Generation.Value)
	require.False(t, unavailable.Generation.ForceReleased)
	require.True(t, policy.IsTransient(err))

	// While it runs, observing fails and the read is skipped.
	ran = 0
	err = evt.ReadAtRebuildEpoch(ctx, env.fence, func(context.Context) error {
		ran++

		return nil
	})
	require.True(t, evt.IsRebuildUnavailableErr(err))
	require.Zero(t, ran, "a failed observe skips the read")

	// A rebuild that started and finished during the read leaves a settled but changed generation.
	_, err = env.fence.End(ctx, env.lease("a"))
	require.NoError(t, err)

	err = evt.ReadAtRebuildEpoch(ctx, env.fence, func(context.Context) error {
		env.begin("b", time.Hour)

		_, endErr := env.fence.End(ctx, env.lease("b"))

		return endErr
	})

	var changed *evt.RebuildEpochChangedError
	require.ErrorAs(t, err, &changed)
	require.Equal(t, before.Value+2, changed.Observed)
	require.Equal(t, before.Value+4, changed.Current)
	require.True(t, policy.IsTransient(err))

	// A failed read is returned as it is, without verifying.
	boom := errors.New("boom")
	require.ErrorIs(t, evt.ReadAtRebuildEpoch(ctx, env.fence, func(context.Context) error { return boom }), boom)

	// A takeover during a read is a changed epoch too: it moves odd to a different odd.
	env.begin("c", time.Second)

	stalled, err := env.fence.Generation(ctx)
	require.NoError(t, err)
	require.False(t, stalled.Settled())

	env.clock.Advance(3)
	taken := env.begin("d", time.Hour)
	require.Equal(t, stalled.Value+2, taken.Generation)

	_, err = env.fence.End(ctx, env.lease("d"))
	require.NoError(t, err)
}

func testRebuildGuardedViews(t *testing.T, env *rebuildEnv, opts SuiteOptions) {
	ctx := context.Background()
	pk := env.ns + "#rebuilt"

	put := func(sk string) evt.ProjectedView {
		return evt.ProjectedView{
			Projector: "rebuild", Op: evt.PutView(pk, sk, "owner", "test", []byte(`{}`)),
		}
	}

	// Without a lock there is nothing to guard the write: it is refused and writes nothing.
	err := env.fence.WriteViews(ctx, env.lease("a"), []evt.ProjectedView{put("none")})
	require.True(t, evt.IsRebuildLeaseLostErr(err))
	requireNoRow(t, opts.ReadView, env.repo, pk, "none")

	env.begin("a", 10*time.Second)

	require.NoError(t, env.fence.WriteViews(ctx, env.lease("a"), []evt.ProjectedView{put("one"), put("two")}))
	requireRow(t, opts.ReadView, env.repo, pk, "one", `{}`)
	requireRow(t, opts.ReadView, env.repo, pk, "two", `{}`)

	// Another owner's batch is refused, whole.
	err = env.fence.WriteViews(ctx, env.lease("b"), []evt.ProjectedView{put("foreign")})
	require.True(t, evt.IsRebuildLeaseLostErr(err))
	requireNoRow(t, opts.ReadView, env.repo, pk, "foreign")

	// A batch beyond the limit and an empty one are refused before any write.
	var tooLarge *evt.CommitTooLargeError

	batch := make([]evt.ProjectedView, 0, evt.MaxRebuildViewBatch+1)
	for i := range evt.MaxRebuildViewBatch + 1 {
		batch = append(batch, put("bulk-"+string(rune('a'+i/26))+string(rune('a'+i%26))))
	}

	require.ErrorAs(t, env.fence.WriteViews(ctx, env.lease("a"), batch), &tooLarge)
	require.NoError(t, env.fence.WriteViews(ctx, env.lease("a"), batch[:evt.MaxRebuildViewBatch]))
	require.ErrorIs(t, env.fence.WriteViews(ctx, env.lease("a"), nil), evt.ErrInvalidArgument)

	// A lapsed lease is no guard, even before anyone takes over.
	env.clock.Advance(11)

	err = env.fence.WriteViews(ctx, env.lease("a"), []evt.ProjectedView{put("lapsed")})
	require.True(t, evt.IsRebuildLeaseLostErr(err))
	requireNoRow(t, opts.ReadView, env.repo, pk, "lapsed")

	// The worker that lost its lock to a takeover can write nothing; the new owner can.
	env.begin("b", time.Hour)

	err = env.fence.WriteViews(ctx, env.lease("a"), []evt.ProjectedView{put("displaced")})
	require.True(t, evt.IsRebuildLeaseLostErr(err))
	requireNoRow(t, opts.ReadView, env.repo, pk, "displaced")

	require.NoError(t, env.fence.WriteViews(ctx, env.lease("b"), []evt.ProjectedView{put("taken-over")}))
	requireRow(t, opts.ReadView, env.repo, pk, "taken-over", `{}`)
}

// A refused commit is transient, and the engine retries it: a command that meets a rebuild waits
// out the backoff and succeeds when the rebuild ends; one that never does gets the error back, not
// a stale-fence rejection and not an outcome, and a rejection decided during a rebuild is never
// recorded either.
func testRebuildEngineRetry(t *testing.T, env *rebuildEnv) {
	ctx := context.Background()

	repo, ok := env.repo.(evt.OutcomeRepository)
	require.True(t, ok)

	retry := newRetryEnv(t, repo, 4)
	id := evt.EntityID(retry.ns + "-ledger")

	var waits int

	waiting := func(onWait func(waits int)) snapshots.RetryPolicy {
		return snapshots.RetryPolicy{
			MaxAttempts: 4,
			BaseDelay:   10 * time.Millisecond,
			MaxDelay:    time.Second,
			Jitter:      func(time.Duration) time.Duration { return 0 },
			Sleep: func(context.Context, time.Duration) error {
				waits++
				onWait(waits)

				return nil
			},
		}
	}

	// The lock is released during the second backoff: the third attempt commits.
	env.begin("worker", time.Hour)
	retry.store.WithRetry(waiting(func(n int) {
		if n == 2 {
			_, err := env.fence.End(ctx, env.lease("worker"))
			require.NoError(t, err)
		}
	}))

	command := retry.commandID("meets-a-rebuild")
	outcome, err := retry.store.ExecuteWithOutcome(ctx, newLedger(id), id, &ledgerCommand{Kind: "deposit", Amount: 5}, withCommand(command))
	require.NoError(t, err)
	require.True(t, outcome.Accepted())
	require.Equal(t, 2, waits)
	require.Len(t, retry.events(t, id), 1)

	// A rebuild that never ends: the attempts run out and the error comes back as it is.
	waits = 0
	env.begin("stuck", time.Hour)
	retry.store.WithRetry(waiting(func(int) {}))

	stuck := retry.commandID("never-ends")
	_, err = retry.store.ExecuteWithOutcome(ctx, newLedger(id), id, &ledgerCommand{Kind: "deposit", Amount: 7}, withCommand(stuck))
	requireRefused(t, err)
	require.Equal(t, 3, waits, "four attempts, three backoffs")
	retry.requireNoOutcome(t, stuck)
	require.Len(t, retry.events(t, id), 1, "nothing was written")

	// A rejection decided while a rebuild runs cannot be recorded: the outcome-only transaction
	// carries the lock check too, so the answer would rest on state the rebuild may be rewriting.
	waits = 0
	rejected := retry.commandID("rejected-in-a-rebuild")
	_, err = retry.store.ExecuteWithOutcome(ctx, newLedger(id), id, &ledgerCommand{Kind: "withdraw", Amount: 1000}, withCommand(rejected))
	requireRefused(t, err)
	retry.requireNoOutcome(t, rejected)

	// So is a rejection that carries events: they commit atomically with the rejected row, and the
	// whole commit carries the lock check.
	flagged := retry.commandID("flagged-in-a-rebuild")
	_, err = retry.store.ExecuteWithOutcome(ctx, newLedger(id), id, &ledgerCommand{Kind: "withdraw-flagged", Amount: 1000}, withCommand(flagged))
	requireRefused(t, err)
	retry.requireNoOutcome(t, flagged)
	require.Len(t, retry.events(t, id), 1, "the flagged event was not written either")

	// A handler that read a derived view under the epoch check fails with the reader's error while
	// a rebuild runs: the engine returns it at once, runs the handler once, and records no outcome,
	// although policy.Classify calls the error transient.
	var handlerRuns atomic.Int32

	derived := retry.commandID("read-derived")
	reading := &ledgerCommand{Kind: "read-derived", Amount: 3, ran: &handlerRuns, read: func(ctx context.Context) error {
		return evt.ReadAtRebuildEpoch(ctx, env.fence, func(context.Context) error { return nil })
	}}

	_, err = retry.store.ExecuteWithOutcome(ctx, newLedger(id), id, reading, withCommand(derived))
	require.True(t, evt.IsRebuildUnavailableErr(err), "the reader's error comes back: %v", err)
	require.True(t, policy.IsTransient(err))
	require.EqualValues(t, 1, handlerRuns.Load(), "returned at once, not re-prepared")
	retry.requireNoOutcome(t, derived)
	require.Len(t, retry.events(t, id), 1)

	// A caller-stamped fence does not turn the refusal into a stale-fence rejection either.
	referral := evt.EntityID(retry.ns + "-referral")
	identity := evt.CommandIdentity{Type: "ledger:rename", Targets: []evt.EntityID{id}, Payload: map[string]string{"name": "Grace"}}
	stamped := retry.commandID("stamped")

	_, err = retry.store.ExecuteTransaction(ctx, identity, withCommand(stamped),
		func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
			if rerr := tx.Read(ctx, newLedger(id), id); rerr != nil {
				return nil, rerr
			}

			if aerr := tx.Append(id, &ledgerEvent{Kind: "deposited", Amount: 1}); aerr != nil {
				return nil, aerr
			}

			return nil, tx.Fence(evt.ReadFence{EntityID: referral, EntityType: ledgerType, Head: 3})
		})
	requireRefused(t, err)
	retry.requireNoOutcome(t, stamped)

	// Once the rebuild ends the same command IDs run: the stuck deposit commits and the rejection
	// is recorded.
	_, err = env.fence.End(ctx, env.lease("stuck"))
	require.NoError(t, err)

	retry.store.WithRetry(waiting(func(int) {}))

	again, err := retry.store.ExecuteWithOutcome(ctx, newLedger(id), id, &ledgerCommand{Kind: "deposit", Amount: 7}, withCommand(stuck))
	require.NoError(t, err)
	require.True(t, again.Accepted())

	late, err := retry.store.ExecuteWithOutcome(ctx, newLedger(id), id, &ledgerCommand{Kind: "withdraw", Amount: 1000}, withCommand(rejected))
	require.NoError(t, err)
	require.True(t, late.Rejected())

	lateFlagged, err := retry.store.ExecuteWithOutcome(ctx, newLedger(id), id,
		&ledgerCommand{Kind: "withdraw-flagged", Amount: 1000}, withCommand(flagged))
	require.NoError(t, err)
	require.True(t, lateFlagged.Rejected())
	require.Len(t, lateFlagged.EventIDs, 1, "the rejection commits its event once the rebuild is over")
}
