// Package conformance verifies evt.Repository implementations against backend-neutral guarantees:
// ordering, entity isolation, snapshots, filtering, and optional optimistic concurrency.
//
// Run the suite with a fresh repository factory and SuiteOptions for optional guarantees. Cases
// namespace IDs and types so durable backends may share a store across subtests.
package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
	"github.com/photon-grove/evt/result" //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

// SuiteOptions toggles assertions that only some backends provide. The zero value runs only the
// guarantees every Repository must satisfy.
type SuiteOptions struct {
	// EnforcesOptimisticConcurrency indicates the backend rejects a second commit that reuses an
	// already-committed (entityID, sequence) pair. Durable backends enforce this with a conditional
	// write (DynamoDB) or a unique constraint (SQL); the in-memory test double does not, so it
	// leaves this false and the concurrency assertion is skipped.
	EnforcesOptimisticConcurrency bool

	// SupportsSnapshots indicates the backend implements CommitWithSnapshot and GetSnapshot durably.
	// Both the in-memory and DynamoDB backends do; a minimal backend may leave it false to skip the
	// snapshot assertions.
	SupportsSnapshots bool

	// SupportsTransactions indicates the backend commits a multi-stream SerializedResult — events
	// for several streams, per-stream snapshots, and read fences — atomically. The in-memory
	// double and every durable backend support it; a minimal backend may leave it false.
	SupportsTransactions bool

	// SupportsOutcomes indicates the backend implements evt.OutcomeRepository: command outcomes
	// (ADR 0004, C3/C4) are written inside the commit transaction and read back strongly
	// consistently. The in-memory double and every durable backend support it.
	SupportsOutcomes bool

	// DeclareRetention durably adds the given entity type to the feature manifest's retention set
	// of the repository under test, so the fence-retention-refusal case can run. It is mandatory
	// for a backend that sets SupportsTransactions: the spec refuses read fences for retained
	// types on every backend, so a missing hook fails the suite instead of skipping the case.
	// The declaration refuses every later fence for that type.
	DeclareRetention func(ctx context.Context, repo evt.Repository, entityType evt.EntityType) error

	// SupportsViews indicates the backend writes commit-wide view operations (ADR 0004, C7) in the
	// commit transaction, so the view cases run. ReadView is then mandatory.
	SupportsViews bool

	// ReadView reads back a view row from the repository under test.
	ReadView ViewReader

	// ViewsSizedBySQL indicates the backend sizes a view operation by its SQL text (PostgreSQL),
	// so the bound arguments are not budgeted and the byte-budget cases for views are skipped; the
	// action count is exact either way.
	ViewsSizedBySQL bool

	// ProjectorGroup, when non-nil, builds a projector transaction group of exactly the given
	// number of operations that the backend under test accepts, enabling the projector-budget
	// case: every backend that executes projector operations counts each one in the shared
	// 100-action budget, so CommitTooLarge fires at the same boundary. A backend that executes no
	// transaction group (Go PostgreSQL: its view operations go through SupportsViews) leaves it nil.
	ProjectorGroup func(operations int) evt.TransactionGroup

	// BypassHead appends one event to the repository's log without maintaining the stream's head
	// item: the writer a head-aware build must detect (ADR 0007). It enables the drift and
	// precedence cases. Leave it nil for a backend that cannot be bypassed.
	BypassHead func(ctx context.Context, repo evt.Repository, event evt.SerializedEvent) error

	// Unmigrate puts the store behind the repository into the state of one written before stream
	// heads: the feature manifest no longer declares stream-heads while the store holds data. It
	// returns a function that declares the feature again, which the case also calls on cleanup, so a
	// shared durable store is left migrated for the cases that follow. It enables the
	// StreamHeadsNotMigrated case.
	Unmigrate func(ctx context.Context, repo evt.Repository) (restore func(), err error)

	// WithRebuildClock returns a handle over the same store as repo whose rebuild fence reads the
	// given clock for lease expiry (ADR 0004, C10). It is mandatory for a backend that sets
	// SupportsTransactions: every such backend keeps the rebuild fence, and the lease cases drive its
	// clock instead of sleeping.
	WithRebuildClock func(repo evt.Repository, now func() time.Time) evt.Repository

	// RefusesCommitStream indicates the deprecated batch path is refused outright because it
	// writes events without maintaining head items (the DynamoDB backend).
	RefusesCommitStream bool
}

// RunRepositorySuite runs the full backend-neutral contract suite against the repository produced by
// newRepo. newRepo MUST return a fresh repository on each call; for an in-memory backend that means
// an empty store, while a shared durable backend may return a handle to the same table (the suite
// namespaces its data so that is safe).
func RunRepositorySuite(t *testing.T, newRepo func() evt.Repository, opts SuiteOptions) {
	t.Helper()

	t.Run("CommitAndGetEvents", func(t *testing.T) {
		testCommitAndGetEvents(t, newRepo())
	})

	t.Run("MetadataRoundTrip", func(t *testing.T) {
		testMetadataRoundTrip(t, newRepo())
	})

	t.Run("GetEventsIsolatesEntities", func(t *testing.T) {
		testGetEventsIsolatesEntities(t, newRepo())
	})

	t.Run("GetLatestEvents", func(t *testing.T) {
		testGetLatestEvents(t, newRepo())
	})

	t.Run("GetRecentEvents", func(t *testing.T) {
		testGetRecentEvents(t, newRepo())
	})

	t.Run("RejectsReservedEntityIds", func(t *testing.T) {
		testRejectsReservedEntityIds(t, newRepo())
	})

	t.Run("StreamAllEventsHonorsFilter", func(t *testing.T) {
		testStreamAllEventsHonorsFilter(t, newRepo())
	})

	t.Run("StreamEntitiesHonorsFilter", func(t *testing.T) {
		testStreamEntitiesHonorsFilter(t, newRepo())
	})

	if !opts.RefusesCommitStream {
		t.Run("CommitStream", func(t *testing.T) {
			testCommitStream(t, newRepo())
		})
	}

	if opts.SupportsSnapshots {
		t.Run("SnapshotRoundTrip", func(t *testing.T) {
			testSnapshotRoundTrip(t, newRepo())
		})
	}

	if opts.EnforcesOptimisticConcurrency {
		t.Run("OptimisticConcurrency", func(t *testing.T) {
			testOptimisticConcurrency(t, newRepo())
		})
	}

	if opts.SupportsSnapshots {
		t.Run("RejectsCommitsBelowCompactedFloor", func(t *testing.T) {
			testCompactedFloor(t, newRepo())
		})

		t.Run("RejectsFirstSnapshotAboveOne", func(t *testing.T) {
			testRejectsFirstSnapshotAboveOne(t, newRepo())
		})
	}

	if opts.SupportsTransactions {
		t.Run("DeclaresTransactionalRepository", func(t *testing.T) {
			require.NoError(t, evt.RequireTransactions(newRepo()),
				"a backend claiming transactions must implement evt.TransactionalRepository")
		})

		t.Run("MultiStreamCommit", func(t *testing.T) {
			testMultiStreamCommit(t, newRepo())
		})

		if opts.EnforcesOptimisticConcurrency {
			t.Run("MultiStreamAtomicity", func(t *testing.T) {
				testMultiStreamAtomicity(t, newRepo())
			})
		}

		t.Run("ReadFence", func(t *testing.T) {
			testReadFence(t, newRepo())
		})

		t.Run("CommitTooLarge", func(t *testing.T) {
			testCommitTooLarge(t, newRepo())
		})

		if opts.SupportsSnapshots {
			t.Run("FenceCompactionABA", func(t *testing.T) {
				testFenceCompactionABA(t, newRepo())
			})
		}

		if opts.ProjectorGroup != nil {
			t.Run("CountsProjectorOperationsInTheBudget", func(t *testing.T) {
				testProjectorBudget(t, newRepo(), opts.ProjectorGroup)
			})
		}

		t.Run("RejectsSequenceGaps", func(t *testing.T) {
			testRejectsSequenceGaps(t, newRepo(), opts.SupportsSnapshots)
		})

		t.Run("RejectsProjectorOnlyCommits", func(t *testing.T) {
			testRejectsProjectorOnlyCommits(t, newRepo())
		})

		if opts.SupportsViews {
			runViewCases(t, newRepo, opts)
		}

		t.Run("RejectsFencesForRetention", func(t *testing.T) {
			require.NotNil(t, opts.DeclareRetention,
				"a backend claiming transactions must supply SuiteOptions.DeclareRetention")

			testRejectsFencesForRetention(t, newRepo(), opts.DeclareRetention)
		})

		runStreamHeadCases(t, newRepo, opts)

		runRebuildFenceCases(t, newRepo, opts)
	}

	if opts.EnforcesOptimisticConcurrency && opts.SupportsSnapshots {
		t.Run("RejectsOutOfOrderEvents", func(t *testing.T) {
			testOutOfOrderEvents(t, newRepo())
		})
	}

	if opts.SupportsOutcomes {
		t.Run("Outcomes", func(t *testing.T) {
			RunOutcomeSuite(t, newRepo, opts.EnforcesOptimisticConcurrency)
		})
	}
}

// --- contract cases ---

func testCommitAndGetEvents(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	id := evt.EntityID(ns + "-1")
	commitEvents(t, repo, id, "test", 0,
		&test.EntityCreated{ID: id, Value: "one"},
		&test.EntityUpdated{ID: id, Value: "two"},
	)

	events, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Len(t, events, 2)
	requireAscendingSequences(t, events)
	require.Equal(t, evt.EventSequence(1), events[0].Sequence)
	require.Equal(t, evt.EventSequence(2), events[1].Sequence)
}

func testMetadataRoundTrip(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	id := evt.EntityID(newNamespace() + "-1")

	serialized := serialize(t, id, "test", 0,
		&test.EntityCreated{ID: id, Value: "one"},
		&test.EntityUpdated{ID: id, Value: "two"},
	)
	for i := range serialized {
		commandID := evt.CommandID(fmt.Sprintf("command-%d", i+1))
		principalID := evt.PrincipalID(fmt.Sprintf("principal-%d", i+1))
		causationID := evt.CommandID(fmt.Sprintf("cause-%d", i+1))
		serialized[i].Metadata = evt.Metadata{
			CommandID:   &commandID,
			PrincipalID: &principalID,
			CausationID: &causationID,
			Timestamp:   "2026-01-02T03:04:05Z",
			Extensions:  map[string]string{"source": fmt.Sprintf("source-%d", i+1), "empty": ""},
		}
	}

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: serialized}))

	events, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Len(t, events, 2)
	for i, event := range events {
		want := serialized[i].Metadata
		require.Equal(t, want.CommandID, event.Metadata.CommandID)
		require.Equal(t, want.PrincipalID, event.Metadata.PrincipalID)
		require.Equal(t, want.CausationID, event.Metadata.CausationID)
		require.Equal(t, want.Extensions, event.Metadata.Extensions)
	}
}

func testGetEventsIsolatesEntities(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	idA := evt.EntityID(ns + "-a")
	idB := evt.EntityID(ns + "-b")
	commitEvents(t, repo, idA, "test", 0, &test.EntityCreated{ID: idA, Value: "a"})
	commitEvents(t, repo, idB, "test", 0, &test.EntityCreated{ID: idB, Value: "b"})

	events, err := repo.GetEvents(ctx, idA)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, idA, events[0].EntityID)

	missing, err := repo.GetEvents(ctx, evt.EntityID(ns+"-missing"))
	require.NoError(t, err)
	require.Empty(t, missing)
}

func testGetLatestEvents(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	id := evt.EntityID(ns + "-1")
	commitEvents(t, repo, id, "test", 0,
		&test.EntityCreated{ID: id, Value: "one"},
		&test.EntityUpdated{ID: id, Value: "two"},
		&test.EntityUpdated{ID: id, Value: "three"},
	)

	latest, err := repo.GetLatestEvents(ctx, id, 1)
	require.NoError(t, err)
	require.Len(t, latest, 2)
	for _, e := range latest {
		require.Greater(t, e.Sequence, evt.EventSequence(1))
	}
}

func testGetRecentEvents(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	id := evt.EntityID(ns + "-1")
	commitEvents(t, repo, id, "test", 0,
		&test.EntityCreated{ID: id, Value: "one"},
		&test.EntityUpdated{ID: id, Value: "two"},
		&test.EntityUpdated{ID: id, Value: "three"},
		&test.EntityUpdated{ID: id, Value: "four"},
	)

	// A bounded newest-first window: the last two events, descending.
	recent, err := repo.GetRecentEvents(ctx, id, 2)
	require.NoError(t, err)
	require.Len(t, recent, 2)
	require.Greater(t, recent[0].Sequence, recent[1].Sequence, "newest-first")
	require.Equal(t, evt.EventSequence(4), recent[0].Sequence)
	require.Equal(t, evt.EventSequence(3), recent[1].Sequence)

	// A limit above the stream's length returns every event, still newest-first.
	all, err := repo.GetRecentEvents(ctx, id, 10)
	require.NoError(t, err)
	require.Len(t, all, 4)
	require.Greater(t, all[0].Sequence, all[len(all)-1].Sequence)

	// Even an oversized window only allocates for actual events, not the requested count.
	oversized, err := repo.GetRecentEvents(ctx, id, math.MaxInt)
	require.NoError(t, err)
	require.Equal(t, all, oversized)

	// A limit below 1 is a programmer error returned before any request.
	_, err = repo.GetRecentEvents(ctx, id, 0)
	require.ErrorIs(t, err, evt.ErrInvalidArgument)
	var invalid *evt.InvalidArgumentError
	require.ErrorAs(t, err, &invalid)
	_, err = repo.GetRecentEvents(ctx, id, -1)
	require.ErrorIs(t, err, evt.ErrInvalidArgument)

	// An unknown stream returns no events.
	missing, err := repo.GetRecentEvents(ctx, evt.EntityID(ns+"-missing"), 3)
	require.NoError(t, err)
	require.Empty(t, missing)

	// The newest-first window never includes the snapshot item.
	serialized := serialize(t, id, "test", 4, &test.EntityUpdated{ID: id, Value: "five"})
	payload := []byte(fmt.Sprintf(`{"id":%q,"value":"five"}`, id))
	require.NoError(t, repo.CommitWithSnapshot(
		ctx, evt.SerializedResult{Events: serialized}, "test", id, payload, 1))

	afterSnapshot, err := repo.GetRecentEvents(ctx, id, 1)
	require.NoError(t, err)
	require.Len(t, afterSnapshot, 1)
	require.Equal(t, evt.EventSequence(5), afterSnapshot[0].Sequence)
}

// testRejectsReservedEntityIds asserts every entity-addressed write path refuses an entity ID in
// a reserved namespace, before any request: commits (multi-stream included), out-of-band snapshot
// writes, and compaction.
func testRejectsReservedEntityIds(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	reserved := []evt.EntityID{
		evt.EntityID(evt.SystemNamespacePrefix + ns + "-1"),
		evt.EntityID(evt.CommandOutcomeNamespacePrefix + ns + "-1"),
	}

	for _, id := range reserved {
		serialized := serialize(t, id, "test", 0, &test.EntityCreated{ID: id, Value: "one"})

		err := repo.Commit(ctx, evt.SerializedResult{Events: serialized})
		require.Error(t, err, "a commit into %q must be refused", id)
		require.True(t, evt.IsReservedEntityIdErr(err), "expected a ReservedEntityIdError, got %v", err)

		// One reserved stream fails a multi-stream commit whole.
		plain := evt.EntityID(ns + "-plain")
		mixed := serialize(t, plain, "test", 0, &test.EntityCreated{ID: plain, Value: "ok"})
		mixed = append(mixed, serialize(t, id, "test", 0, &test.EntityCreated{ID: id, Value: "reserved"})...)

		err = repo.Commit(ctx, evt.SerializedResult{Events: mixed})
		require.Error(t, err, "a multi-stream commit with one reserved stream must be refused")
		require.True(t, evt.IsReservedEntityIdErr(err))

		// The refusal happens before any write: the plain stream stays empty.
		stored, err := repo.GetEvents(ctx, plain)
		require.NoError(t, err)
		require.Empty(t, stored, "the refused commit writes nothing")

		// Snapshot writes refuse all the same.
		err = repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: serialize(t, id, "test", 0,
			&test.EntityCreated{ID: id, Value: "one"})}, "test", id, []byte(`{"id":"x"}`), 1)
		require.Error(t, err)
		require.True(t, evt.IsReservedEntityIdErr(err))

		// Out-of-band snapshot writes refuse all the same, on backends that offer them.
		if snapshotter, ok := repo.(interface {
			PutSnapshot(
				context.Context, evt.EntityType, evt.EntityID, []byte, evt.EventSequence, evt.EventSequence,
			) error
		}); ok {
			err = snapshotter.PutSnapshot(ctx, "test", id, []byte(`{"id":"x"}`), 1, 1)
			require.Error(t, err, "an out-of-band snapshot of %q must be refused", id)
			require.True(t, evt.IsReservedEntityIdErr(err), "expected a ReservedEntityIdError, got %v", err)
		}

		// Compaction writes no new rows but is refused all the same.
		compactor, ok := repo.(evt.Compactor)
		if !ok {
			continue
		}

		_, err = compactor.CompactBelow(ctx, id, 1)
		require.Error(t, err)
		require.True(t, evt.IsReservedEntityIdErr(err))
	}
}

func testStreamAllEventsHonorsFilter(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	alphaType := evt.EntityType(ns + "-alpha")
	betaType := evt.EntityType(ns + "-beta")
	alphaID := evt.EntityID(ns + "-alpha-1")
	betaID := evt.EntityID(ns + "-beta-1")

	commitEvents(t, repo, alphaID, alphaType, 0, &test.EntityCreated{ID: alphaID, Value: "a"})
	commitEvents(t, repo, betaID, betaType, 0, &test.EntityCreated{ID: betaID, Value: "b"})

	// Unfiltered stream returns every event in the store; assert membership rather than an absolute
	// count so a shared table with residue from other cases stays valid.
	all := drainEvents(t, repo.StreamAllEvents(ctx, evt.StreamFilter{}))
	require.True(t, containsEntity(all, alphaID), "unfiltered stream should include the alpha event")
	require.True(t, containsEntity(all, betaID), "unfiltered stream should include the beta event")

	// A unique entity type isolates this run's events even on a shared table.
	alpha := drainEvents(t, repo.StreamAllEvents(ctx, evt.StreamFilter{EntityType: alphaType}))
	require.Len(t, alpha, 1)
	require.Equal(t, alphaID, alpha[0].EntityID)
	require.Equal(t, alphaType, alpha[0].EntityType)
}

func testStreamEntitiesHonorsFilter(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	alphaType := evt.EntityType(ns + "-alpha")
	betaType := evt.EntityType(ns + "-beta")
	alphaID := evt.EntityID(ns + "-alpha-1")
	betaID := evt.EntityID(ns + "-beta-1")

	commitEvents(t, repo, alphaID, alphaType, 0, &test.EntityCreated{ID: alphaID, Value: "a"})
	commitEvents(t, repo, betaID, betaType, 0, &test.EntityCreated{ID: betaID, Value: "b"})

	apply := applyEvent()

	// The unfiltered scan may sweep residue from other cases or backends sharing the store, whose
	// events need not deserialize as a test.Entity. Tolerate those errors and assert only that our
	// two entities are present; the unique-type filtered scan below is the strict reconstitution
	// check.
	all := drainEntitiesLenient(repo.StreamEntities(ctx, evt.StreamFilter{}, apply))
	require.Contains(t, all, alphaID, "unfiltered stream should include the alpha entity")
	require.Contains(t, all, betaID, "unfiltered stream should include the beta entity")

	alpha := drainEntities(t, repo.StreamEntities(ctx, evt.StreamFilter{EntityType: alphaType}, apply))
	require.Equal(t, []evt.EntityID{alphaID}, alpha)
}

func testCommitStream(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	id := evt.EntityID(ns + "-1")
	serialized := serialize(t, id, "test", 0, &test.EntityCreated{ID: id, Value: "one"})

	channel := make(chan result.Result[evt.SerializedResult], 1)
	channel <- result.Ok(evt.SerializedResult{Events: serialized})
	close(channel)

	errs := repo.CommitStream(ctx, channel) //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	require.Empty(t, errs)

	events, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Len(t, events, 1)
}

func testSnapshotRoundTrip(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	id := evt.EntityID(ns + "-1")

	missing, err := repo.GetSnapshot(ctx, id)
	require.NoError(t, err)
	require.Nil(t, missing)

	serialized := serialize(t, id, "test", 0, &test.EntityCreated{ID: id, Value: "one"})
	payload := []byte(fmt.Sprintf(`{"id":%q,"value":"one"}`, id))

	err = repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: serialized}, "test", id, payload, 1)
	require.NoError(t, err)

	snapshot, err := repo.GetSnapshot(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Equal(t, id, snapshot.EntityID)
	require.Equal(t, evt.EntityType("test"), snapshot.EntityType)
	require.Equal(t, payload, snapshot.Payload)
}

func testOptimisticConcurrency(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	id := evt.EntityID(ns + "-1")
	commitEvents(t, repo, id, "test", 0, &test.EntityCreated{ID: id, Value: "one"})

	// Re-committing sequence 1 must be rejected: the (entityID, sequence) pair already exists.
	duplicate := serialize(t, id, "test", 0, &test.EntityCreated{ID: id, Value: "conflict"})

	err := repo.Commit(ctx, evt.SerializedResult{Events: duplicate})
	require.Error(t, err, "expected a conflict committing a duplicate sequence")
}

func testCompactedFloor(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	compactor, ok := repo.(evt.Compactor)
	if !ok {
		t.Skip("repository does not implement evt.Compactor")
	}

	id := evt.EntityID(ns + "-1")
	serialized := serialize(t, id, "test", 0,
		&test.EntityCreated{ID: id, Value: "one"},
		&test.EntityUpdated{ID: id, Value: "two"},
	)
	payload := []byte(fmt.Sprintf(`{"id":%q,"value":"two"}`, id))
	require.NoError(t, repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: serialized}, "test", id, payload, 1))

	deleted, err := compactor.CompactBelow(ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, 2, deleted)

	// A writer that loaded before the snapshot would commit sequence 2 again. Its key is gone, so
	// only the snapshot floor rejects it.
	stale := serialize(t, id, "test", 1, &test.EntityUpdated{ID: id, Value: "stale"})
	require.Error(t, repo.Commit(ctx, evt.SerializedResult{Events: stale}), "a commit below the floor must be rejected")

	streamed := make(chan result.Result[evt.SerializedResult], 1)
	streamed <- result.Ok(evt.SerializedResult{Events: stale})
	close(streamed)
	require.NotEmpty(t, repo.CommitStream(ctx, streamed), "a streamed commit below the floor must be rejected") //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion

	events, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Empty(t, events, "the rejected commit writes nothing")

	next := serialize(t, id, "test", 2, &test.EntityUpdated{ID: id, Value: "three"})
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: next}))
}

func testOutOfOrderEvents(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	compactor, ok := repo.(evt.Compactor)
	if !ok {
		t.Skip("repository does not implement evt.Compactor")
	}

	id := evt.EntityID(ns + "-1")
	serialized := serialize(t, id, "test", 0,
		&test.EntityCreated{ID: id, Value: "one"},
		&test.EntityUpdated{ID: id, Value: "two"},
	)
	payload := []byte(fmt.Sprintf(`{"id":%q,"value":"two"}`, id))
	require.NoError(t, repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: serialized}, "test", id, payload, 1))

	_, err := compactor.CompactBelow(ctx, id, 2)
	require.NoError(t, err)

	// The head condition checks the first event's sequence, so [3, 2] would recreate compacted sequence 2.
	later := serialize(t, id, "test", 1,
		&test.EntityUpdated{ID: id, Value: "a"},
		&test.EntityUpdated{ID: id, Value: "b"},
	)
	reversed := evt.SerializedResult{Events: []evt.SerializedEvent{later[1], later[0]}}
	require.Error(t, repo.CommitWithSnapshot(ctx, reversed, "test", id, payload, 2), "a snapshot commit out of order")
	require.Error(t, repo.Commit(ctx, reversed), "a commit out of order")

	// Loads reject a stream with a gap, so a commit must not create one. A fresh entity keeps the
	// compacted floor out of the way.
	fresh := evt.EntityID(ns + "-gap")
	gapped := serialize(t, fresh, "test", 0,
		&test.EntityCreated{ID: fresh, Value: "one"},
		&test.EntityUpdated{ID: fresh, Value: "three"},
	)
	gapped[1].Sequence = 3
	require.Error(t, repo.Commit(ctx, evt.SerializedResult{Events: gapped}), "a commit that skips a sequence")

	// Sequences start at 1: DynamoDB keeps the snapshot at sk = 0.
	zero := serialize(t, fresh, "test", 0, &test.EntityCreated{ID: fresh, Value: "zero"})
	zero[0].Sequence = 0
	require.Error(t, repo.Commit(ctx, evt.SerializedResult{Events: zero}), "a commit at sequence 0")

	stored, err := repo.GetEvents(ctx, fresh)
	require.NoError(t, err)
	require.Empty(t, stored, "the rejected commits write nothing")

	events, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Empty(t, events, "rejected commits write nothing")
}

// --- helpers ---

// nsCounter disambiguates namespaces generated within the same nanosecond.
var nsCounter atomic.Uint64

// newNamespace returns a token unique to this call, used to prefix entity IDs and types so cases
// stay isolated even when several share one durable store across runs.
func newNamespace() string {
	return fmt.Sprintf("conf-%d-%d", time.Now().UnixNano(), nsCounter.Add(1))
}

// NewNamespaceForTest exposes namespace generation to integration test doubles that reproduce a
// suite case in isolation.
func NewNamespaceForTest() string { return newNamespace() }

// SerializeForTest exposes event serialization to integration test doubles that reproduce a suite
// case in isolation.
func SerializeForTest(
	t *testing.T,
	id evt.EntityID,
	entityType evt.EntityType,
	fromSequence evt.EventSequence,
	events ...evt.Event,
) []evt.SerializedEvent {
	return serialize(t, id, entityType, fromSequence, events...)
}

// serialize prepares events for an entity and overrides the serialized EntityType so a single Go
// event type can stand in for multiple logical entity types in filter tests.
func serialize(
	t *testing.T,
	id evt.EntityID,
	entityType evt.EntityType,
	fromSequence evt.EventSequence,
	events ...evt.Event,
) []evt.SerializedEvent {
	t.Helper()

	serialized, err := evt.SerializeEvents(events, fromSequence, id, evt.Metadata{})
	require.NoError(t, err)

	for i := range serialized {
		serialized[i].EntityType = entityType
	}

	return serialized
}

// commitEvents serializes and commits events for an entity with the given (overridden) entity type.
func commitEvents(
	t *testing.T,
	repo evt.Repository,
	id evt.EntityID,
	entityType evt.EntityType,
	fromSequence evt.EventSequence,
	events ...evt.Event,
) {
	t.Helper()

	serialized := serialize(t, id, entityType, fromSequence, events...)
	require.NoError(t, repo.Commit(context.Background(), evt.SerializedResult{Events: serialized}))
}

// applyEvent returns an applyEvent callback that reconstitutes a test.Entity from serialized events.
// It deserializes by event Type, so it works regardless of the (possibly overridden) EntityType.
func applyEvent() func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error) {
	return func(_ context.Context, se evt.SerializedEvent, current evt.Entity) (evt.Entity, error) {
		if current == nil {
			current = test.NewEntity(se.EntityID)
		}

		event, err := current.DeserializeEvent(se)
		if err != nil {
			return nil, err
		}

		if err := current.Apply(event); err != nil {
			return nil, err
		}

		return current, nil
	}
}

func drainEvents(t *testing.T, channel <-chan result.Result[[]evt.SerializedEvent]) []evt.SerializedEvent {
	t.Helper()

	var events []evt.SerializedEvent
	for r := range channel {
		batch, err := r.Unwrap()
		require.NoError(t, err)
		events = append(events, batch...)
	}

	return events
}

func drainEntities(t *testing.T, channel <-chan result.Result[evt.Entity]) []evt.EntityID {
	t.Helper()

	var ids []evt.EntityID
	for r := range channel {
		entity, err := r.Unwrap()
		require.NoError(t, err)
		if entity == nil {
			continue
		}
		ids = append(ids, entity.GetID())
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	return ids
}

// drainEntitiesLenient collects the IDs of successfully reconstituted entities, ignoring per-entity
// errors. It is for whole-store scans that may encounter unrelated residue from other cases or
// backends sharing a durable store.
func drainEntitiesLenient(channel <-chan result.Result[evt.Entity]) []evt.EntityID {
	var ids []evt.EntityID
	for r := range channel {
		entity, err := r.Unwrap()
		if err != nil || entity == nil {
			continue
		}
		ids = append(ids, entity.GetID())
	}

	return ids
}

func containsEntity(events []evt.SerializedEvent, id evt.EntityID) bool {
	for _, e := range events {
		if e.EntityID == id {
			return true
		}
	}

	return false
}

func requireAscendingSequences(t *testing.T, events []evt.SerializedEvent) {
	t.Helper()

	for i := 1; i < len(events); i++ {
		require.Greater(t, events[i].Sequence, events[i-1].Sequence,
			"events must be returned in ascending sequence order")
	}
}

// --- transactional contract cases (C1/C2) ---

func testMultiStreamCommit(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	idA, idB := evt.EntityID(ns+"-a"), evt.EntityID(ns+"-b")
	eventsA := serialize(t, idA, "test", 0,
		&test.EntityCreated{ID: idA, Value: "one"},
		&test.EntityUpdated{ID: idA, Value: "two"},
	)
	eventsB := serialize(t, idB, "test", 0, &test.EntityCreated{ID: idB, Value: "sibling"})

	// Deliberately declared out of bytewise stream order: the commit pins the order itself.
	err := repo.Commit(ctx, evt.SerializedResult{Events: append(eventsB, eventsA...)})
	require.NoError(t, err)

	storedA, err := repo.GetEvents(ctx, idA)
	require.NoError(t, err)
	require.Len(t, storedA, 2)
	require.Equal(t, evt.EventSequence(1), eventsA[0].Sequence)
	require.Equal(t, evt.EventSequence(2), eventsA[1].Sequence)

	storedB, err := repo.GetEvents(ctx, idB)
	require.NoError(t, err)
	require.Len(t, storedB, 1)
}

func testMultiStreamAtomicity(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	idA, idB := evt.EntityID(ns+"-a"), evt.EntityID(ns+"-b")
	commitEvents(t, repo, idA, "test", 0,
		&test.EntityCreated{ID: idA, Value: "one"},
		&test.EntityUpdated{ID: idA, Value: "two"},
	)

	eventsB := serialize(t, idB, "test", 0, &test.EntityCreated{ID: idB, Value: "sibling"})
	colliding := serialize(t, idA, "test", 1, &test.EntityUpdated{ID: idA, Value: "reused"})
	err := repo.Commit(ctx, evt.SerializedResult{Events: append(eventsB, colliding...)})
	require.Error(t, err, "a commit that collides on one stream must fail")

	// All-or-nothing: the sibling stream that shared the failed commit must be untouched.
	events, err := repo.GetEvents(ctx, idB)
	require.NoError(t, err)
	require.Empty(t, events)
}

func testReadFence(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	idA, idC := evt.EntityID(ns+"-a"), evt.EntityID(ns+"-c")
	commitEvents(t, repo, idA, "test", 0,
		&test.EntityCreated{ID: idA, Value: "one"},
		&test.EntityUpdated{ID: idA, Value: "two"},
	)

	// A fence at the observed head passes while the transaction writes another stream. A fence
	// on an absent stream (head 0) passes too.
	fenced := evt.ReadFence{EntityID: idA, EntityType: "test", Head: 2}
	err := repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, idC, "test", 0, &test.EntityCreated{ID: idC, Value: "writer"}),
		Fences: []evt.ReadFence{fenced, {EntityID: evt.EntityID(ns + "-absent"), EntityType: "test"}},
	})
	require.NoError(t, err)

	// Another writer moves the fenced stream past the head.
	commitEvents(t, repo, idA, "test", 2, &test.EntityUpdated{ID: idA, Value: "moved"})

	// A stale fence now fails the whole commit, and the failing item carries the fence.
	events, err := repo.GetEvents(ctx, idC)
	require.NoError(t, err)
	before := len(events)
	err = repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, idC, "test", evt.EventSequence(before),
			&test.EntityUpdated{ID: idC, Value: "later"}),
		Fences: []evt.ReadFence{fenced},
	})
	require.Error(t, err)

	var conflict *evt.FenceConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, idA, conflict.Fence.EntityID)
	require.Equal(t, evt.EventSequence(2), conflict.Fence.Head)
	var conflictError *evt.ConflictError
	require.ErrorAs(t, err, &conflictError, "a fence conflict is an optimistic-concurrency conflict")
	require.False(t, conflict.Fence.CallerStamped)
	require.True(t, policy.IsTransient(err), "an engine-filled fence conflict retries like an event-put conflict")

	// The same stale fence stamped by a caller is a stale-fence rejection, not a retryable
	// conflict.
	stamped := fenced
	stamped.CallerStamped = true
	err = repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, idC, "test", evt.EventSequence(before),
			&test.EntityUpdated{ID: idC, Value: "later"}),
		Fences: []evt.ReadFence{stamped},
	})
	require.ErrorAs(t, err, &conflict)
	require.True(t, conflict.Fence.CallerStamped)
	require.False(t, policy.IsTransient(err), "a caller-stamped fence conflict is not retried")

	// Nothing from the failed commit was written.
	events, err = repo.GetEvents(ctx, idC)
	require.NoError(t, err)
	require.Len(t, events, before)
}

func testCommitTooLarge(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	// The feature-marker check, the rebuild-lock check, the head update, and 97 event puts are
	// exactly 100 actions, so a 97-event commit fits on every backend. Every emitted action counts
	// against the budget.
	idFits := evt.EntityID(ns + "-fits")
	fitting := make([]evt.Event, 0, 97)
	for i := range 97 {
		fitting = append(fitting, &test.EntityUpdated{ID: idFits, Value: fmt.Sprintf("value-%d", i)})
	}
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: serialize(t, idFits, "test", 0, fitting...)}))

	// Appending to an existing stream costs the same single head update as creating it, so the
	// two control checks, the head update, and 97 event puts fill the 100 actions: 97 fit, 98 do not.
	idGrows := evt.EntityID(ns + "-grows")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, idGrows, "test", 0, &test.EntityCreated{ID: idGrows, Value: "first"}),
	}))

	growing := func(count int) []evt.Event {
		out := make([]evt.Event, 0, count)
		for i := range count {
			out = append(out, &test.EntityUpdated{ID: idGrows, Value: fmt.Sprintf("value-%d", i)})
		}

		return out
	}

	tooMany := repo.Commit(ctx, evt.SerializedResult{Events: serialize(t, idGrows, "test", 1, growing(98)...)})
	var tooLargeAppend *evt.CommitTooLargeError
	require.ErrorAs(t, tooMany, &tooLargeAppend)
	require.Equal(t, 101, tooLargeAppend.Items)

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: serialize(t, idGrows, "test", 1, growing(97)...)}))

	idA := evt.EntityID(ns + "-a")
	// 101 event puts plus the two control checks and the head update exceed the 100-item budget.
	events := make([]evt.Event, 0, 101)
	for i := range 101 {
		events = append(events, &test.EntityUpdated{ID: idA, Value: fmt.Sprintf("value-%d", i)})
	}
	serialized := serialize(t, idA, "test", 0, events...)

	err := repo.Commit(ctx, evt.SerializedResult{Events: serialized})
	var tooLarge *evt.CommitTooLargeError
	require.ErrorAs(t, err, &tooLarge)

	stored, err := repo.GetEvents(ctx, idA)
	require.NoError(t, err)
	require.Empty(t, stored, "an oversized commit is refused before any write")
}

func testFenceCompactionABA(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	compactor, ok := repo.(evt.Compactor)
	if !ok {
		t.Skip("repository does not compact")
	}

	idA, idC := evt.EntityID(ns+"-a"), evt.EntityID(ns+"-c")
	events := serialize(t, idA, "test", 0,
		&test.EntityCreated{ID: idA, Value: "one"},
		&test.EntityUpdated{ID: idA, Value: "two"},
		&test.EntityUpdated{ID: idA, Value: "three"},
	)
	payload, err := json.Marshal(test.NewEntity(idA))
	require.NoError(t, err)
	require.NoError(t, repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: events}, "test", idA, payload, 1))

	// Compaction deletes the events the snapshot covers, which empties the slot a slot check would
	// read — but the head only increases, so the stale fence still fails against the head.
	_, err = compactor.CompactBelow(ctx, idA, 3)
	require.NoError(t, err)

	err = repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, idC, "test", 0, &test.EntityCreated{ID: idC, Value: "writer"}),
		Fences: []evt.ReadFence{{EntityID: idA, EntityType: "test", Head: 1}},
	})
	var conflict *evt.FenceConflictError
	require.ErrorAs(t, err, &conflict)
}

func testRejectsFencesForRetention(
	t *testing.T,
	repo evt.Repository,
	declare func(context.Context, evt.Repository, evt.EntityType) error,
) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	// The retention type is namespaced to this run so a shared manifest's declarations from other
	// tests never collide.
	entityType := evt.EntityType(ns + "-retained")
	idA := evt.EntityID(ns + "-a")
	require.NoError(t, declare(ctx, repo, entityType))

	err := repo.Commit(ctx, evt.SerializedResult{
		Fences: []evt.ReadFence{{EntityID: idA, EntityType: entityType, Head: 0}},
	})
	var refused *evt.RetentionConflictError
	require.ErrorAs(t, err, &refused)
	require.Equal(t, entityType, refused.EntityType)
}

// projectorOnlyGroup is a backend-neutral stand-in for a projector's operations. A repository
// must refuse the commit before it ever interprets the group.
type projectorOnlyGroup struct{}

func (projectorOnlyGroup) Len() int                             { return 1 }
func (projectorOnlyGroup) StorageType() evt.StorageType         { return "conformance" }
func (projectorOnlyGroup) TransactionType() evt.TransactionType { return "conformance" }
func (projectorOnlyGroup) HandleError(err error, _ int) error   { return err }
func (g projectorOnlyGroup) Merge(evt.TransactionGroup) (evt.TransactionGroup, error) {
	return g, nil
}

func testRejectsProjectorOnlyCommits(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	idA := evt.EntityID(ns + "-a")

	// Projector operations without a written event would leave view state no replay can rebuild.
	for name, result := range map[string]evt.SerializedResult{
		"operations only": {Transaction: evt.Transaction{projectorOnlyGroup{}}},
		"operations with a fence": {
			Transaction: evt.Transaction{projectorOnlyGroup{}},
			Fences:      []evt.ReadFence{{EntityID: idA, EntityType: "test"}},
		},
	} {
		err := repo.Commit(ctx, result)

		var refused *evt.ProjectorOnlyCommitError
		require.ErrorAs(t, err, &refused, name)
		require.Equal(t, 1, refused.Operations, name)
	}
}

func testRejectsFirstSnapshotAboveOne(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	idA := evt.EntityID(ns + "-a")
	events := serialize(t, idA, "test", 0,
		&test.EntityCreated{ID: idA, Value: "one"},
		&test.EntityUpdated{ID: idA, Value: "two"},
	)

	// A stream's first snapshot must have sequence 1: every backend refuses one that skips ahead.
	err := repo.Commit(ctx, evt.SerializedResult{
		Events: events,
		Snapshots: []evt.SerializedSnapshot{{
			EntityType: "test", EntityID: idA, Sequence: 3, EventSequence: 2, Payload: []byte(`{}`),
		}},
	})
	require.Error(t, err)

	stored, err := repo.GetEvents(ctx, idA)
	require.NoError(t, err)
	require.Empty(t, stored, "a refused commit writes nothing")

	snapshot, err := repo.GetSnapshot(ctx, idA)
	require.NoError(t, err)
	require.Nil(t, snapshot)

	// The same commit with snapshot sequence 1 is accepted.
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: events,
		Snapshots: []evt.SerializedSnapshot{{
			EntityType: "test", EntityID: idA, Sequence: 1, EventSequence: 2, Payload: []byte(`{}`),
		}},
	}))
}

func testProjectorBudget(t *testing.T, repo evt.Repository, group func(int) evt.TransactionGroup) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	// The two control checks, one event put, and the head update leave 96 actions for
	// projector operations: 96 fit exactly and 97 do not.
	idFits, idOver := evt.EntityID(ns+"-fits"), evt.EntityID(ns+"-over")

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events:      serialize(t, idFits, "test", 0, &test.EntityCreated{ID: idFits, Value: "fits"}),
		Transaction: evt.Transaction{group(96)},
	}))

	err := repo.Commit(ctx, evt.SerializedResult{
		Events:      serialize(t, idOver, "test", 0, &test.EntityCreated{ID: idOver, Value: "over"}),
		Transaction: evt.Transaction{group(97)},
	})
	var tooLarge *evt.CommitTooLargeError
	require.ErrorAs(t, err, &tooLarge)
	require.Equal(t, 101, tooLarge.Items)

	stored, err := repo.GetEvents(ctx, idOver)
	require.NoError(t, err)
	require.Empty(t, stored, "an oversized commit is refused before any write")
}

func testRejectsSequenceGaps(t *testing.T, repo evt.Repository, supportsSnapshots bool) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	created := func(id evt.EntityID) evt.Event { return &test.EntityCreated{ID: id, Value: "one"} }
	updated := func(id evt.EntityID) evt.Event { return &test.EntityUpdated{ID: id, Value: "next"} }

	// A first event above sequence 1 on a stream with no events is a gap.
	idNew := evt.EntityID(ns + "-new")
	err := repo.Commit(ctx, evt.SerializedResult{Events: serialize(t, idNew, "test", 2, created(idNew))})
	require.ErrorIs(t, err, evt.ErrSequenceGap)
	var gap *evt.SequenceGapError
	require.ErrorAs(t, err, &gap)
	require.Equal(t, idNew, gap.EntityID)
	require.Equal(t, evt.EventSequence(0), gap.After)
	require.Equal(t, evt.EventSequence(3), gap.Found)
	stored, err := repo.GetEvents(ctx, idNew)
	require.NoError(t, err)
	require.Empty(t, stored, "a refused gap writes nothing")

	// Skipping a sequence on an existing stream is a gap; continuing from the head is not.
	idA := evt.EntityID(ns + "-a")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: serialize(t, idA, "test", 0, created(idA))}))

	err = repo.Commit(ctx, evt.SerializedResult{Events: serialize(t, idA, "test", 2, updated(idA))})
	require.ErrorAs(t, err, &gap)
	require.Equal(t, evt.EventSequence(1), gap.After)
	require.Equal(t, evt.EventSequence(3), gap.Found)

	// One stream of a multi-stream commit with a gap fails the whole commit.
	idB := evt.EntityID(ns + "-b")
	err = repo.Commit(ctx, evt.SerializedResult{Events: append(
		serialize(t, idB, "test", 0, created(idB)),
		serialize(t, idA, "test", 2, updated(idA))...,
	)})
	require.ErrorIs(t, err, evt.ErrSequenceGap)
	stored, err = repo.GetEvents(ctx, idB)
	require.NoError(t, err)
	require.Empty(t, stored, "the clean stream of the failed commit is untouched")

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: serialize(t, idA, "test", 1, updated(idA))}))

	// Compaction can delete every event a snapshot covers, including the head: the snapshot's
	// event sequence is then the head, so the stream still continues from it and still refuses a gap.
	compactor, compacts := repo.(evt.Compactor)
	if !supportsSnapshots || !compacts {
		return
	}

	idC := evt.EntityID(ns + "-c")
	payload, err := json.Marshal(test.NewEntity(idC))
	require.NoError(t, err)
	require.NoError(t, repo.CommitWithSnapshot(ctx, evt.SerializedResult{
		Events: serialize(t, idC, "test", 0, created(idC), updated(idC), updated(idC)),
	}, "test", idC, payload, 1))

	deleted, err := compactor.CompactBelow(ctx, idC, 3)
	require.NoError(t, err)
	require.Equal(t, 3, deleted, "the head event is compacted away")

	err = repo.Commit(ctx, evt.SerializedResult{Events: serialize(t, idC, "test", 4, updated(idC))})
	require.ErrorIs(t, err, evt.ErrSequenceGap, "a gap above the snapshot's head is still refused")

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: serialize(t, idC, "test", 3, updated(idC))}),
		"the stream continues from the snapshot's head")
}
