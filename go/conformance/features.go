package conformance

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/test"
)

// FeatureMarkerOptions gives the feature-marker cases the backend's marker (ADR 0004, C9). Both
// hooks are mandatory: every backend keeps the SYSTEM#FEATURES manifest.
type FeatureMarkerOptions struct {
	// Declare applies change to the stored manifest in one declaration transaction, bumping its
	// version by exactly one, through a marker over repo's storage.
	Declare func(ctx context.Context, repo evt.Repository, change func(*evt.FeatureManifest)) (evt.FeatureManifest, error)

	// Manifest reads the stored manifest with a consistent read, or nil when no row exists.
	Manifest func(ctx context.Context, repo evt.Repository) (*evt.FeatureManifest, error)
}

// snapshotPutter is the out-of-band snapshot write the durable repositories offer.
type snapshotPutter interface {
	PutSnapshot(
		ctx context.Context,
		entityType evt.EntityType,
		entityID evt.EntityID,
		payload []byte,
		snapshotSequence evt.EventSequence,
		eventSequence evt.EventSequence,
	) error
}

// RunFeatureMarkerSuite runs the feature-marker cases (ADR 0004, C9), the Go counterpart of Rust's
// photon_evt::conformance::feature_marker_cases. first and second are two writers over the same
// store that each keep their own cached validation: the refusal must bind a writer whose cached
// validation predates another writer's declaration.
//
// The cases declare features no writer supports, which refuse every commit to the store until they
// are retracted, so a durable backend should run them on a store of its own. The suite clears every
// declaration except stream-heads before and after it runs.
func RunFeatureMarkerSuite(t *testing.T, first, second evt.Repository, opts FeatureMarkerOptions) {
	t.Helper()

	require.NotNil(t, opts.Declare, "FeatureMarkerOptions.Declare is mandatory")
	require.NotNil(t, opts.Manifest, "FeatureMarkerOptions.Manifest is mandatory")

	env := markerEnv{first: first, second: second, opts: opts}

	env.clear(t)
	t.Cleanup(func() { env.clear(t) })

	t.Run("DeclarationRefusesBothWritersThenRevalidates", func(t *testing.T) { testDeclarationRefuses(t, env) })
	t.Run("HistoryDependentRefusesCompactionAcrossWriters", func(t *testing.T) { testHistoryDependentAcrossWriters(t, env) })
	t.Run("RetentionRoundTripsAndHeadsSkipTheMarker", func(t *testing.T) { testRetentionRoundTrip(t, env) })
	t.Run("RetentionRefusesFencesAcrossWriters", func(t *testing.T) { testRetentionRefusesFences(t, env) })
	t.Run("OneDeclarationBumpsTheVersionOnce", func(t *testing.T) { testOneBumpPerDeclaration(t, env) })
}

type markerEnv struct {
	first, second evt.Repository
	opts          FeatureMarkerOptions
}

func (e markerEnv) declare(t *testing.T, repo evt.Repository, change func(*evt.FeatureManifest)) evt.FeatureManifest {
	t.Helper()

	declared, err := e.opts.Declare(context.Background(), repo, change)
	require.NoError(t, err)

	return declared
}

func (e markerEnv) manifest(t *testing.T, repo evt.Repository) evt.FeatureManifest {
	t.Helper()

	manifest, err := e.opts.Manifest(context.Background(), repo)
	require.NoError(t, err)
	require.NotNil(t, manifest, "the store has a feature-marker row")

	return *manifest
}

// clear leaves stream-heads as the only declaration, so no writer refuses the store.
func (e markerEnv) clear(t *testing.T) {
	t.Helper()

	e.declare(t, e.first, func(manifest *evt.FeatureManifest) {
		manifest.Features = map[string]evt.FeatureFormat{evt.FeatureStreamHeads: evt.StreamHeadsFormat}
		manifest.HistoryDependent = nil
		manifest.Retention = nil
	})

	features, err := e.manifest(t, e.second).FeaturesJSON()
	require.NoError(t, err)
	require.JSONEq(t, fmt.Sprintf(`{%q:%d}`, evt.FeatureStreamHeads, evt.StreamHeadsFormat), features)
}

// revalidating runs write, running it once more when the writer's cached manifest version went
// stale: a writer whose cached validation predates a declaration first observes the changed
// manifest (a retryable error), and must then revalidate.
func revalidating(write func() error) error {
	err := write()
	if errors.Is(err, evt.ErrFeatureManifestChanged) {
		err = write()
	}

	return err
}

// commitOrRefused commits result, revalidating once when the writer's cached version went stale.
func commitOrRefused(ctx context.Context, repo evt.Repository, result evt.SerializedResult) error {
	return revalidating(func() error { return repo.Commit(ctx, result) })
}

// commitWithSnapshot commits events with a snapshot, revalidating once when the writer's cached
// version went stale.
func commitWithSnapshot(
	ctx context.Context,
	repo evt.Repository,
	events []evt.SerializedEvent,
	entityType evt.EntityType,
	id evt.EntityID,
	payload []byte,
) error {
	return revalidating(func() error {
		return repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: events}, entityType, id, payload, 1)
	})
}

func testDeclarationRefuses(t *testing.T, env markerEnv) {
	ctx := context.Background()
	ns := newNamespace()
	entityType := evt.EntityType(ns + "-marker")
	id := evt.EntityID(ns + "-marker")

	created := serialize(t, id, entityType, 0,
		&test.AggregateCreated{ID: id, Value: "one"},
		&test.AggregateUpdated{ID: id, Value: "two"},
	)
	payload := []byte(fmt.Sprintf(`{"id":%q,"value":"two"}`, id))
	require.NoError(t, commitWithSnapshot(ctx, env.first, created, entityType, id, payload))

	// Both writers validate (and cache) the manifest before anything is declared.
	require.NoError(t, commitOrRefused(ctx, env.second, evt.SerializedResult{
		Events: serialize(t, id, entityType, 2, &test.AggregateUpdated{ID: id, Value: "three"}),
	}))

	before := env.manifest(t, env.first).Version
	declared := env.declare(t, env.second, func(manifest *evt.FeatureManifest) {
		manifest.Features["journal-compaction"] = 2
	})
	require.Equal(t, before+1, declared.Version, "a declaration bumps the version by exactly one")

	next := evt.SerializedResult{Events: serialize(t, id, entityType, 3, &test.AggregateUpdated{ID: id, Value: "four"})}

	var unsupported *evt.UnsupportedFeatureError

	// Declaring never counts as validation, on the declaring writer or on one whose cached
	// validation predates the declaration.
	for name, writer := range map[string]evt.Repository{"the declaring writer": env.second, "the cached writer": env.first} {
		err := commitOrRefused(ctx, writer, next)
		require.ErrorAs(t, err, &unsupported, "a commit by %s", name)
		require.Contains(t, unsupported.Unsupported, evt.UnsupportedPair{Name: "journal-compaction", Format: 2})

		if putter, ok := writer.(snapshotPutter); ok {
			err = revalidating(func() error { return putter.PutSnapshot(ctx, entityType, id, payload, 2, 3) })

			require.True(t, evt.IsUnsupportedFeatureErr(err), "a snapshot write by %s: got %v", name, err)
		}

		if compactor, ok := writer.(evt.Compactor); ok {
			err = revalidating(func() error {
				_, cerr := compactor.CompactBelow(ctx, id, 2)

				return cerr
			})
			require.True(t, evt.IsUnsupportedFeatureErr(err), "compaction by %s: got %v", name, err)
		}
	}

	events, err := env.first.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Len(t, events, 3, "the refused writes wrote and deleted nothing")

	snapshot, err := env.first.GetSnapshot(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Equal(t, evt.EventSequence(1), snapshot.Sequence, "the refused snapshot writes left the floor alone")

	// A declaration that touches only a durable set does not bless the unsupported feature.
	env.declare(t, env.first, func(manifest *evt.FeatureManifest) {
		manifest.Retention = append(manifest.Retention, entityType)
	})
	require.True(t, evt.IsUnsupportedFeatureErr(commitOrRefused(ctx, env.first, next)),
		"a durable-set-only declaration keeps refusing the unsupported log")

	// Retracting the feature revalidates both writers, which commit again.
	env.declare(t, env.second, func(manifest *evt.FeatureManifest) {
		delete(manifest.Features, "journal-compaction")
		manifest.Retention = slices.DeleteFunc(manifest.Retention, func(retained evt.EntityType) bool { return retained == entityType })
	})
	require.NoError(t, commitOrRefused(ctx, env.first, next))
	require.NoError(t, commitOrRefused(ctx, env.second, evt.SerializedResult{
		Events: serialize(t, id, entityType, 4, &test.AggregateUpdated{ID: id, Value: "five"}),
	}))

	events, err = env.second.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Len(t, events, 5, "both writers commit once the feature is retracted")
}

func testHistoryDependentAcrossWriters(t *testing.T, env markerEnv) {
	ctx := context.Background()
	ns := newNamespace()
	entityType := evt.EntityType(ns + "-history")
	id := evt.EntityID(ns + "-history")

	compactor, ok := env.first.(evt.Compactor)
	if !ok {
		t.Skip("repository does not implement evt.Compactor")
	}

	created := serialize(t, id, entityType, 0,
		&test.AggregateCreated{ID: id, Value: "one"},
		&test.AggregateUpdated{ID: id, Value: "two"},
	)
	payload := []byte(fmt.Sprintf(`{"id":%q,"value":"two"}`, id))
	require.NoError(t, commitWithSnapshot(ctx, env.first, created, entityType, id, payload))

	// The set is durable: a writer that never registered a history-dependent projector refuses
	// compaction all the same.
	env.declare(t, env.second, func(manifest *evt.FeatureManifest) {
		manifest.HistoryDependent = append(manifest.HistoryDependent, entityType)
	})

	_, err := compactor.CompactBelow(ctx, id, 2)
	var refused *evt.HistoryDependentProjectionError
	require.ErrorAs(t, err, &refused)
	require.Equal(t, entityType, refused.EntityType)

	events, err := env.first.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Len(t, events, 2, "the refused compaction deleted nothing")

	env.declare(t, env.second, func(manifest *evt.FeatureManifest) {
		manifest.HistoryDependent = slices.DeleteFunc(manifest.HistoryDependent, func(declared evt.EntityType) bool { return declared == entityType })
	})

	deleted, err := compactor.CompactBelow(ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, 2, deleted, "compaction proceeds once the type leaves the set")
}

func testRetentionRoundTrip(t *testing.T, env markerEnv) {
	ctx := context.Background()
	ns := newNamespace()
	retained := []evt.EntityType{evt.EntityType(ns + "-b"), evt.EntityType(ns + "-a")}

	env.declare(t, env.second, func(manifest *evt.FeatureManifest) {
		manifest.Retention = append(manifest.Retention, retained...)
		manifest.HistoryDependent = append(manifest.HistoryDependent, retained[0])
	})

	manifest := env.manifest(t, env.first)
	for _, entityType := range retained {
		require.Contains(t, manifest.Retention, entityType, "the declared retention set reads back from the other writer")
	}
	require.Contains(t, manifest.HistoryDependent, retained[0])

	text, err := manifest.RetentionJSON()
	require.NoError(t, err)
	require.Less(t, strings.Index(text, string(retained[1])), strings.Index(text, string(retained[0])), "the stored set is sorted")

	env.declare(t, env.first, func(manifest *evt.FeatureManifest) {
		manifest.Retention = slices.DeleteFunc(manifest.Retention, func(declared evt.EntityType) bool { return slices.Contains(retained, declared) })
		manifest.HistoryDependent = slices.DeleteFunc(manifest.HistoryDependent, func(declared evt.EntityType) bool { return slices.Contains(retained, declared) })
	})

	manifest = env.manifest(t, env.second)
	for _, entityType := range retained {
		require.NotContains(t, manifest.Retention, entityType, "the retracted retention set")
		require.NotContains(t, manifest.HistoryDependent, entityType, "the retracted historyDependent set")
	}

	// The marker row lives in the reserved SYSTEM# namespace, which entity heads and enumeration
	// skip.
	if heads, ok := env.first.(evt.EntityHeadStreamer); ok {
		all, err := heads.StreamEntityHeads(ctx, "")
		require.NoError(t, err)

		for id := range all {
			require.False(t, evt.IsReservedEntityID(id), "entity heads yielded the engine row %q", id)
		}
	}

	for _, event := range drainEvents(t, env.first.StreamAllEvents(ctx, evt.StreamFilter{})) {
		require.False(t, evt.IsReservedEntityID(event.EntityID), "enumeration yielded the engine row %q", event.EntityID)
	}
}

func testRetentionRefusesFences(t *testing.T, env markerEnv) {
	ctx := context.Background()
	ns := newNamespace()
	entityType := evt.EntityType(ns + "-retained")
	fence := evt.SerializedResult{Fences: []evt.ReadFence{{EntityID: evt.EntityID(ns + "-a"), EntityType: entityType}}}

	if evt.RequireTransactions(env.first) != nil {
		t.Skip("repository does not commit read fences")
	}

	// Both writers accept the fence while nothing declares the type.
	require.NoError(t, commitOrRefused(ctx, env.first, fence))
	require.NoError(t, commitOrRefused(ctx, env.second, fence))

	env.declare(t, env.second, func(manifest *evt.FeatureManifest) {
		manifest.Retention = append(manifest.Retention, entityType)
	})

	// Neither writer configured retention for the type: the durable set alone refuses the fence.
	for name, writer := range map[string]evt.Repository{"the declaring writer": env.second, "the cached writer": env.first} {
		var refused *evt.RetentionConflictError
		require.ErrorAs(t, commitOrRefused(ctx, writer, fence), &refused, "a fence by %s", name)
		require.Equal(t, entityType, refused.EntityType)
	}

	env.declare(t, env.first, func(manifest *evt.FeatureManifest) {
		manifest.Retention = slices.DeleteFunc(manifest.Retention, func(declared evt.EntityType) bool { return declared == entityType })
	})
	require.NoError(t, commitOrRefused(ctx, env.second, fence), "the fence passes once the type leaves the set")
}

func testOneBumpPerDeclaration(t *testing.T, env markerEnv) {
	ns := newNamespace()
	before := env.manifest(t, env.first).Version

	// Two features in one declaration bump the version once, so every cached writer revalidates
	// once, not once per feature.
	declared := env.declare(t, env.second, func(manifest *evt.FeatureManifest) {
		manifest.Features[ns+"-one"] = 1
		manifest.Features[ns+"-two"] = 1
	})
	require.Equal(t, before+1, declared.Version)
	require.Equal(t, before+1, env.manifest(t, env.first).Version, "the other writer reads the same version")

	env.declare(t, env.first, func(manifest *evt.FeatureManifest) {
		delete(manifest.Features, ns+"-one")
		delete(manifest.Features, ns+"-two")
	})
}
