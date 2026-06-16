// Package conformance provides a backend-neutral contract test suite for evt.Repository
// implementations. Run it against any backend — the in-memory repository, the DynamoDB repository,
// or a future SQL/PostgreSQL repository — to verify the implementation honors the storage
// invariants the framework relies on:
//
//   - per-entity sequence ordering: GetEvents returns an entity's events in ascending sequence;
//   - read isolation by entity: GetEvents/GetLatestEvents return only the requested entity;
//   - snapshot consistency: a written snapshot is read back intact (when supported);
//   - backend-neutral filtering: StreamAllEvents/StreamEntities honor evt.StreamFilter;
//   - optimistic concurrency: a duplicate (entityID, sequence) commit is rejected (when supported).
//
// Each backend wires the suite from its own test package, supplying a factory that returns a fresh,
// empty repository and a SuiteOptions describing which optional guarantees it provides. The
// in-memory backend, for example, is a permissive test double that does not enforce optimistic
// concurrency, so it leaves that option false.
//
// Backends that share one durable store across subtests (the DynamoDB integration suite reuses a
// single table) are still safe: every case namespaces its entity IDs and entity types per run, and
// the whole-table stream assertions check membership and unique-type filters rather than absolute
// table counts, so residue from other cases or prior runs cannot make a case fail.
package conformance

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
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

	t.Run("GetEventsIsolatesEntities", func(t *testing.T) {
		testGetEventsIsolatesEntities(t, newRepo())
	})

	t.Run("GetLatestEvents", func(t *testing.T) {
		testGetLatestEvents(t, newRepo())
	})

	t.Run("StreamAllEventsHonorsFilter", func(t *testing.T) {
		testStreamAllEventsHonorsFilter(t, newRepo())
	})

	t.Run("StreamEntitiesHonorsFilter", func(t *testing.T) {
		testStreamEntitiesHonorsFilter(t, newRepo())
	})

	t.Run("CommitStream", func(t *testing.T) {
		testCommitStream(t, newRepo())
	})

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

	errs := repo.CommitStream(ctx, channel)
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

// --- helpers ---

// nsCounter disambiguates namespaces generated within the same nanosecond.
var nsCounter atomic.Uint64

// newNamespace returns a token unique to this call, used to prefix entity IDs and types so cases
// stay isolated even when several share one durable store across runs.
func newNamespace() string {
	return fmt.Sprintf("conf-%d-%d", time.Now().UnixNano(), nsCounter.Add(1))
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
