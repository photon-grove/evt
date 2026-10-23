package conformance

import (
	"context"
	"errors"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

// InitializationOptions wires the store-initialization cases to a backend.
type InitializationOptions struct {
	// RefusesUninitialized indicates the handles are writers that do not initialize a store
	// themselves (the DynamoDB default): every write path refuses an uninitialized store with
	// *evt.StoreNotInitializedError and writes nothing. The in-memory and PostgreSQL writers
	// initialize a fresh store on their first write, so they leave it false.
	RefusesUninitialized bool

	// Manifest reads the store's feature manifest, nil when it has none. Mandatory.
	Manifest func(ctx context.Context) (*evt.FeatureManifest, error)

	// Unmigrate takes the stream-heads declaration back from the store, as SuiteOptions.Unmigrate
	// does, and returns the function that declares it again. Mandatory.
	Unmigrate func(ctx context.Context, repo evt.Repository) (restore func(), err error)

	// BypassHead, when set, appends an event without a head item (SuiteOptions.BypassHead), so the
	// populated store that initialization refuses also holds a stream with no head item.
	BypassHead func(ctx context.Context, repo evt.Repository, event evt.SerializedEvent) error
}

// RunInitializationSuite verifies explicit store initialization (evt.StoreInitializer) against two
// handles over an empty store that has never been written to: no events, no snapshots, and no
// marker row. Both handles must implement evt.StoreInitializer, and the store must be fresh: a
// durable backend needs its own empty table or schema.
//
// With RefusesUninitialized, every write path first refuses the store and writes nothing. Then, on
// every backend: initialization declares stream-heads at version 1, a second initialization by
// either handle declares nothing, both handles write, and initializing a store that holds data
// but does not declare the feature is refused with *evt.StreamHeadsNotMigratedError and declares
// nothing, as a lazily initializing writer's verification does.
func RunInitializationSuite(t *testing.T, fresh, other evt.Repository, opts InitializationOptions) {
	t.Helper()
	ctx := context.Background()

	require.NotNil(t, opts.Manifest, "InitializationOptions.Manifest is mandatory")
	require.NotNil(t, opts.Unmigrate, "InitializationOptions.Unmigrate is mandatory")

	initializer, ok := fresh.(evt.StoreInitializer)
	require.True(t, ok, "%T must implement evt.StoreInitializer", fresh)

	otherInitializer, ok := other.(evt.StoreInitializer)
	require.True(t, ok, "%T must implement evt.StoreInitializer", other)

	manifest, err := opts.Manifest(ctx)
	require.NoError(t, err)
	require.Nil(t, manifest, "the store starts with no marker row")

	ns := newNamespace()
	id := evt.EntityID(ns + "-initialized")

	if opts.RefusesUninitialized {
		testUninitializedStoreRefusesWrites(t, fresh, id, opts)
	}

	declared, err := initializer.Initialize(ctx)
	require.NoError(t, err)
	require.True(t, declared, "the first initialization declares")

	manifest, err = opts.Manifest(ctx)
	require.NoError(t, err)
	require.NotNil(t, manifest)
	require.EqualValues(t, 1, manifest.Version, "the initialization creates the row at version 1")
	require.Equal(t, map[string]evt.FeatureFormat{evt.FeatureStreamHeads: evt.StreamHeadsFormat}, manifest.Features)

	for name, repo := range map[string]evt.StoreInitializer{"the same handle": initializer, "another handle": otherInitializer} {
		declared, err = repo.Initialize(ctx)
		require.NoError(t, err, name)
		require.False(t, declared, "a second initialization by %s declares nothing", name)
	}

	commitEvents(t, fresh, id, "test", 0, &test.EntityCreated{ID: id, Value: "one"})
	commitEvents(t, other, id, "test", 1, &test.EntityUpdated{ID: id, Value: "two"})

	stored, err := fresh.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Len(t, stored, 2, "both writers write to the initialized store")

	declared, err = initializer.Initialize(ctx)
	require.NoError(t, err)
	require.False(t, declared, "initializing a store that holds data and declares the feature declares nothing")

	manifest, err = opts.Manifest(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, manifest.Version, "idempotent initialization leaves the manifest version alone")

	restore := testInitializingAPopulatedStoreIsRefused(t, fresh, initializer, ns, opts)

	// Declared again (the migration's hook), the store takes writes. The first commit after a
	// declaration may fail its marker check and revalidate, which is transient and retryable.
	restore()

	next := serialize(t, id, "test", 2, &test.EntityUpdated{ID: id, Value: "three"})

	err = fresh.Commit(ctx, evt.SerializedResult{Events: next})
	if errors.Is(err, evt.ErrFeatureManifestChanged) {
		err = fresh.Commit(ctx, evt.SerializedResult{Events: next})
	}

	require.NoError(t, err, "the writer writes once the feature is declared again")
}

// Every write path refuses an uninitialized store with *evt.StoreNotInitializedError, and none of
// them writes anything: no event, snapshot, outcome, or marker row.
func testUninitializedStoreRefusesWrites(t *testing.T, repo evt.Repository, id evt.EntityID, opts InitializationOptions) {
	t.Helper()
	ctx := context.Background()

	notInitialized := func(err error, what string) {
		t.Helper()
		require.ErrorIs(t, err, evt.ErrStoreNotInitialized, what)
		require.True(t, evt.IsStoreNotInitializedErr(err), what)
		require.False(t, evt.IsStreamHeadsNotMigratedErr(err), "%s: an uninitialized store is not an unmigrated one", what)
		require.False(t, evt.IsOptimisticConflict(err), "%s: refusal is not a conflict", what)
		require.False(t, policy.IsTransient(err), "%s: refusal is not transient", what)
	}

	events := serialize(t, id, "test", 0, &test.EntityCreated{ID: id, Value: "one"})
	notInitialized(repo.Commit(ctx, evt.SerializedResult{Events: events}), "commit")
	notInitialized(repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: events}, "test", id, []byte(`{"id":"x"}`), 1), "commit with snapshot")

	command := evt.CommandID(string(id) + "-outcome-only")
	fenced := evt.CommandID(string(id) + "-fenced")
	if outcomes, ok := repo.(evt.OutcomeRepository); ok {
		notInitialized(outcomes.Commit(ctx, evt.SerializedResult{
			Outcome: &evt.Outcome{CommandID: command, Status: evt.OutcomeAccepted, Digest: "digest"},
		}), "outcome-only commit")
		notInitialized(outcomes.Commit(ctx, evt.SerializedResult{
			Fences:  []evt.ReadFence{{EntityID: id, EntityType: "test"}},
			Outcome: &evt.Outcome{CommandID: fenced, Status: evt.OutcomeAccepted, Digest: "digest"},
		}), "fenced outcome-only commit")
	}

	if writer, ok := repo.(snapshotWriter); ok {
		notInitialized(writer.PutSnapshot(ctx, "test", id, []byte(`{"id":"x"}`), 1, 1), "put snapshot")
	}

	if compactor, ok := repo.(evt.Compactor); ok {
		_, err := compactor.CompactBelow(ctx, id, 1)
		notInitialized(err, "compaction")
	}

	manifest, err := opts.Manifest(ctx)
	require.NoError(t, err)
	require.Nil(t, manifest, "a refused write declares nothing")

	stored, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Empty(t, stored, "a refused write stores no event")

	snapshot, err := repo.GetSnapshot(ctx, id)
	require.NoError(t, err)
	require.Nil(t, snapshot, "a refused write stores no snapshot")

	if outcomes, ok := repo.(evt.OutcomeRepository); ok {
		for _, commandID := range []evt.CommandID{command, fenced} {
			outcome, gerr := outcomes.GetOutcome(ctx, commandID)
			require.NoError(t, gerr)
			require.Nil(t, outcome, "a refused write records no outcome")
		}
	}
}

// A store that holds data without declaring stream-heads (the declaration taken back, and, with
// BypassHead, a stream with no head item) is refused, and nothing is declared. It returns the
// function that declares the feature again.
func testInitializingAPopulatedStoreIsRefused(
	t *testing.T,
	repo evt.Repository,
	initializer evt.StoreInitializer,
	ns string,
	opts InitializationOptions,
) func() {
	t.Helper()
	ctx := context.Background()

	restore, err := opts.Unmigrate(ctx, repo)
	require.NoError(t, err)
	t.Cleanup(restore)

	if opts.BypassHead != nil {
		headless := evt.EntityID(ns + "-headless")
		events := serialize(t, headless, "test", 0, &test.EntityCreated{ID: headless, Value: "one"})
		require.NoError(t, opts.BypassHead(ctx, repo, events[0]))
	}

	before, err := opts.Manifest(ctx)
	require.NoError(t, err)

	_, err = initializer.Initialize(ctx)
	require.ErrorIs(t, err, evt.ErrStreamHeadsNotMigrated, "initializing a populated store that does not declare stream-heads")

	after, err := opts.Manifest(ctx)
	require.NoError(t, err)
	require.Equal(t, before, after, "a refused initialization declares nothing")

	return restore
}
