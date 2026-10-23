package conformance

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
	"github.com/photon-grove/evt/test"
)

// Cases of the per-stream head item (ADR 0007; storage spec, Stream Heads). Each runs on every
// backend that commits transactions; the ones that need a way to break a head on purpose or to
// un-migrate a store run when SuiteOptions supplies the hook.

// snapshotWriter is the out-of-band snapshot write the DynamoDB and PostgreSQL repositories offer.
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

// runStreamHeadCases runs the head cases a backend supports.
func runStreamHeadCases(t *testing.T, newRepo func() evt.Repository, opts SuiteOptions) {
	t.Helper()

	t.Run("StreamHeads", func(t *testing.T) {
		t.Run("FenceOnAnAbsentStream", func(t *testing.T) { testFenceOnAbsentStream(t, newRepo()) })
		t.Run("HeadItemsAreNotEvents", func(t *testing.T) { testHeadItemsAreNotEvents(t, newRepo()) })

		if opts.SupportsSnapshots {
			t.Run("CompactionKeepsTheHead", func(t *testing.T) { testCompactionKeepsTheHead(t, newRepo()) })

			if _, ok := newRepo().(snapshotWriter); ok {
				t.Run("SnapshotGuard", func(t *testing.T) { testSnapshotGuard(t, newRepo()) })
			}
		}

		if opts.BypassHead != nil {
			t.Run("DriftIsNotRetryable", func(t *testing.T) { testDriftIsNotRetryable(t, newRepo(), opts.BypassHead) })

			if opts.SupportsOutcomes {
				t.Run("DriftRecordsNoOutcome", func(t *testing.T) {
					repo, err := evt.RequireOutcomes(newRepo())
					require.NoError(t, err)
					testDriftRecordsNoOutcome(t, repo, opts.BypassHead)
				})
				t.Run("PrecedenceOrder", func(t *testing.T) {
					repo, err := evt.RequireOutcomes(newRepo())
					require.NoError(t, err)
					testPrecedenceOrder(t, repo, opts.BypassHead)
				})
			}
		}

		if opts.Unmigrate != nil {
			t.Run("UnmigratedStoreRefusesWrites", func(t *testing.T) {
				testUnmigratedStoreRefusesWrites(t, newRepo(), opts)
			})
		}
	})
}

// driftedStream commits two events to a fresh stream, then appends a third beneath the head: the
// head item says 2 and the log derives 3.
func driftedStream(
	t *testing.T,
	repo evt.Repository,
	bypass func(context.Context, evt.Repository, evt.SerializedEvent) error,
	name string,
) evt.EntityID {
	t.Helper()

	id := evt.EntityID(newNamespace() + "-" + name)
	commitEvents(t, repo, id, "test", 0,
		&test.EntityCreated{ID: id, Value: "one"},
		&test.EntityUpdated{ID: id, Value: "two"},
	)

	raw := serialize(t, id, "test", 2, &test.EntityUpdated{ID: id, Value: "bypass"})
	require.NoError(t, bypass(context.Background(), repo, raw[0]))

	stored, err := repo.GetEvents(context.Background(), id)
	require.NoError(t, err)
	require.Len(t, stored, 3, "the bypassing writer's event is in the log")

	return id
}

// A fence on a stream the reader saw empty (head 0) passes while the stream is absent and fails
// once any commit writes it.
func testFenceOnAbsentStream(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	idWriter, idWatched := evt.EntityID(ns+"-writer"), evt.EntityID(ns+"-watched")
	fence := evt.ReadFence{EntityID: idWatched, EntityType: "test", Head: 0}

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, idWriter, "test", 0, &test.EntityCreated{ID: idWriter, Value: "first"}),
		Fences: []evt.ReadFence{fence},
	}))

	commitEvents(t, repo, idWatched, "test", 0, &test.EntityCreated{ID: idWatched, Value: "created"})

	err := repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, idWriter, "test", 1, &test.EntityUpdated{ID: idWriter, Value: "second"}),
		Fences: []evt.ReadFence{fence},
	})

	var conflict *evt.FenceConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, idWatched, conflict.Fence.EntityID)
}

// A head item is concurrency metadata, not an event: no read, scan, or enumeration returns it as
// one, and a negative sequence never reaches it.
func testHeadItemsAreNotEvents(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	id := evt.EntityID(ns + "-stream")
	commitEvents(t, repo, id, "test", 0,
		&test.EntityCreated{ID: id, Value: "one"},
		&test.EntityUpdated{ID: id, Value: "two"},
		&test.EntityUpdated{ID: id, Value: "three"},
	)

	all, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Len(t, all, 3)

	latest, err := repo.GetLatestEvents(ctx, id, -1)
	require.NoError(t, err)
	require.Len(t, latest, 3, "a negative sequence reads as 0 and returns only events")

	for _, event := range drainEvents(t, repo.StreamAllEvents(ctx, evt.StreamFilter{})) {
		require.GreaterOrEqual(t, event.Sequence, evt.EventSequence(1), "no head item is yielded as an event")
	}

	streamed := 0
	for _, event := range drainEvents(t, repo.StreamAllEvents(ctx, evt.StreamFilter{})) {
		if event.EntityID == id {
			streamed++
		}
	}
	require.Equal(t, 3, streamed)

	require.Contains(t, drainEntitiesLenient(repo.StreamEntities(ctx, evt.StreamFilter{}, applyEvent())), id)

	if streamer, ok := repo.(evt.EntityHeadStreamer); ok {
		heads, err := streamer.StreamEntityHeads(ctx, "")
		require.NoError(t, err)
		require.Equal(t, evt.EventSequence(3), heads[id], "an entity's head is its highest event")

		for entityID, head := range heads {
			require.GreaterOrEqual(t, head, evt.EventSequence(1), "%s: a head item contributes no head of its own", entityID)
		}
	}
}

// Compaction never touches the head: a writer whose load is still current appends past a
// compaction without a conflict, while a writer that still believes in a compacted sequence
// conflicts.
func testCompactionKeepsTheHead(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	compactor, ok := repo.(evt.Compactor)
	if !ok {
		t.Skip("repository does not compact")
	}

	id := evt.EntityID(ns + "-compacted")
	events := serialize(t, id, "test", 0,
		&test.EntityCreated{ID: id, Value: "one"},
		&test.EntityUpdated{ID: id, Value: "two"},
		&test.EntityUpdated{ID: id, Value: "three"},
	)
	payload := []byte(`{"id":"x"}`)
	require.NoError(t, repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: events}, "test", id, payload, 1))

	// A writer prepared an append at sequence 4 before the compaction.
	prepared := serialize(t, id, "test", 3, &test.EntityUpdated{ID: id, Value: "four"})

	deleted, err := compactor.CompactBelow(ctx, id, 3)
	require.NoError(t, err)
	require.Equal(t, 3, deleted)

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: prepared}),
		"compaction does not move the head, so a current writer never conflicts with it")

	stale := serialize(t, id, "test", 2, &test.EntityUpdated{ID: id, Value: "stale"})
	err = repo.Commit(ctx, evt.SerializedResult{Events: stale})
	require.True(t, evt.IsOptimisticConflict(err), "a compacted sequence cannot be recreated: %v", err)
}

// An out-of-band snapshot never covers more than its stream's head (H3).
func testSnapshotGuard(t *testing.T, repo evt.Repository) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	writer, ok := repo.(snapshotWriter)
	require.True(t, ok)

	id := evt.EntityID(ns + "-guarded")
	commitEvents(t, repo, id, "test", 0,
		&test.EntityCreated{ID: id, Value: "one"},
		&test.EntityUpdated{ID: id, Value: "two"},
	)

	// Ahead of the head: refused as drift, and nothing is written.
	err := writer.PutSnapshot(ctx, "test", id, []byte(`{"id":"x"}`), 1, 5)

	var drift *evt.StreamHeadDriftError
	require.ErrorAs(t, err, &drift)
	require.Equal(t, id, drift.EntityID)
	require.Equal(t, evt.EventSequence(2), drift.Stored)
	require.False(t, drift.HeadAbsent)
	require.Equal(t, evt.EventSequence(5), drift.Derived)
	require.False(t, evt.IsOptimisticConflict(err), "drift is not a conflict")

	snapshot, err := repo.GetSnapshot(ctx, id)
	require.NoError(t, err)
	require.Nil(t, snapshot, "the refused snapshot was not written")

	// A stream that was never written has no head item to cover a snapshot.
	ghost := evt.EntityID(ns + "-ghost")
	err = writer.PutSnapshot(ctx, "test", ghost, []byte(`{"id":"x"}`), 1, 1)
	require.ErrorAs(t, err, &drift)
	require.True(t, drift.HeadAbsent)

	// A snapshot of an empty stream (event sequence 0) needs no head item.
	empty := evt.EntityID(ns + "-empty")
	require.NoError(t, writer.PutSnapshot(ctx, "test", empty, []byte(`{"id":"x"}`), 1, 0))

	snapshot, err = repo.GetSnapshot(ctx, empty)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Equal(t, evt.EventSequence(0), snapshot.EventSequence)

	// At the head it is written, and a regressing write is a silent no-op.
	require.NoError(t, writer.PutSnapshot(ctx, "test", id, []byte(`{"id":"two"}`), 1, 2))
	require.NoError(t, writer.PutSnapshot(ctx, "test", id, []byte(`{"id":"one"}`), 2, 1))

	snapshot, err = repo.GetSnapshot(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Equal(t, evt.EventSequence(2), snapshot.EventSequence)
	require.JSONEq(t, `{"id":"two"}`, string(snapshot.Payload))

	// The refused writes left the stream writable.
	commitEvents(t, repo, id, "test", 2, &test.EntityUpdated{ID: id, Value: "three"})
}

// A head that sits below the log is drift: it fails the stream's writes and its fences, is not a
// conflict, is not transient, and writes nothing.
func testDriftIsNotRetryable(
	t *testing.T,
	repo evt.Repository,
	bypass func(context.Context, evt.Repository, evt.SerializedEvent) error,
) {
	t.Helper()
	ctx := context.Background()

	id := driftedStream(t, repo, bypass, "drifted")

	err := repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, id, "test", 3, &test.EntityUpdated{ID: id, Value: "four"}),
	})

	var drift *evt.StreamHeadDriftError
	require.ErrorAs(t, err, &drift)
	require.ErrorIs(t, err, evt.ErrStreamHeadDrift)
	require.Equal(t, id, drift.EntityID)
	require.Equal(t, evt.EventSequence(2), drift.Stored)
	require.Equal(t, evt.EventSequence(3), drift.Derived)
	require.False(t, evt.IsOptimisticConflict(err), "re-preparing cannot repair drift")
	require.False(t, policy.IsTransient(err), "drift is never retried")

	// The writer that believes the head is right appends exactly where the bypassing writer did: its
	// head condition passes, and its event put finds an event above the head. That is drift too.
	err = repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, id, "test", 2, &test.EntityUpdated{ID: id, Value: "third"}),
	})
	require.ErrorAs(t, err, &drift)
	require.Equal(t, id, drift.EntityID)
	require.Equal(t, evt.EventSequence(2), drift.Stored)
	require.Equal(t, evt.EventSequence(3), drift.Derived)
	require.False(t, evt.IsOptimisticConflict(err))

	// A fence on the drifted stream is drift too, not a quiet pass or a stale-fence conflict.
	other := evt.EntityID(newNamespace() + "-other")
	err = repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, other, "test", 0, &test.EntityCreated{ID: other, Value: "writer"}),
		Fences: []evt.ReadFence{{EntityID: id, EntityType: "test", Head: 3}},
	})
	require.ErrorAs(t, err, &drift)
	require.Equal(t, id, drift.EntityID)

	written, err := repo.GetEvents(ctx, other)
	require.NoError(t, err)
	require.Empty(t, written, "a refused commit writes nothing")

	stored, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Len(t, stored, 3)
}

// Drift is not exhausted into a stale-fence rejection and records no outcome: fence-aware retry
// returns it at once, so the same command can run again after the repair.
func testDriftRecordsNoOutcome(
	t *testing.T,
	repo evt.OutcomeRepository,
	bypass func(context.Context, evt.Repository, evt.SerializedEvent) error,
) {
	t.Helper()

	env := newRetryEnv(t, repo, 5)

	_, err := env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1}, env.commandID("first"))
	require.NoError(t, err)

	raw, serr := evt.SerializeEvents([]evt.Event{&ledgerEvent{Kind: "deposited", Amount: 1}}, 1, env.id, evt.Metadata{})
	require.NoError(t, serr)
	raw[0].EntityType = ledgerType
	require.NoError(t, bypass(context.Background(), repo, raw[0]))

	id := env.commandID("drifted")
	_, err = env.run(t, &ledgerCommand{Kind: "deposit", Amount: 1}, id)

	var drift *evt.StreamHeadDriftError
	require.ErrorAs(t, err, &drift)
	require.Empty(t, env.delays, "drift is returned at once, with no backoff and no second attempt")
	env.requireNoOutcome(t, id)
}

// Failures rank marker, outcome put, drift, gap, then any other conflict (storage spec, Stream
// Heads, Classification), on every backend.
func testPrecedenceOrder(
	t *testing.T,
	repo evt.OutcomeRepository,
	bypass func(context.Context, evt.Repository, evt.SerializedEvent) error,
) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	drifted := driftedStream(t, repo, bypass, "drifted")

	// A stream with one event, which a commit starting at 3 skips past, and one a commit collides
	// with.
	gapped, raced := evt.EntityID(ns+"-gapped"), evt.EntityID(ns+"-raced")
	commitEvents(t, repo, gapped, "test", 0, &test.EntityCreated{ID: gapped, Value: "one"})
	commitEvents(t, repo, raced, "test", 0, &test.EntityCreated{ID: raced, Value: "one"})

	driftEvents := serialize(t, drifted, "test", 3, &test.EntityUpdated{ID: drifted, Value: "next"})
	gapEvents := serialize(t, gapped, "test", 2, &test.EntityUpdated{ID: gapped, Value: "skips"})
	raceEvents := serialize(t, raced, "test", 0, &test.EntityCreated{ID: raced, Value: "again"})

	// Drift outranks a gap and a conflict in the same commit.
	err := repo.Commit(ctx, evt.SerializedResult{Events: append(append(append([]evt.SerializedEvent{}, raceEvents...), gapEvents...), driftEvents...)})

	var drift *evt.StreamHeadDriftError
	require.ErrorAs(t, err, &drift)
	require.Equal(t, drifted, drift.EntityID)

	// A gap outranks a conflict.
	err = repo.Commit(ctx, evt.SerializedResult{Events: append(append([]evt.SerializedEvent{}, raceEvents...), gapEvents...)})
	require.ErrorIs(t, err, evt.ErrSequenceGap)
	require.NotErrorIs(t, err, evt.ErrStreamHeadDrift)

	// Any other conflict is still a conflict.
	err = repo.Commit(ctx, evt.SerializedResult{Events: raceEvents})
	require.True(t, evt.IsOptimisticConflict(err), "%v", err)
	require.NotErrorIs(t, err, evt.ErrSequenceGap)

	// A recorded outcome outranks drift: the row is where the answer lives.
	command := evt.CommandID(ns + "-command")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Outcome: &evt.Outcome{CommandID: command, Status: evt.OutcomeAccepted, Digest: "digest"},
	}))

	err = repo.Commit(ctx, evt.SerializedResult{
		Events:  driftEvents,
		Outcome: &evt.Outcome{CommandID: command, Status: evt.OutcomeAccepted, Digest: "digest"},
	})
	require.ErrorIs(t, err, evt.ErrOutcomeConflict)
	require.NotErrorIs(t, err, evt.ErrStreamHeadDrift)
}

// A store that holds data but does not declare stream-heads has streams with no head items, so
// every write path that validates the marker refuses it, and reads still work.
func testUnmigratedStoreRefusesWrites(t *testing.T, repo evt.Repository, opts SuiteOptions) {
	t.Helper()
	ctx := context.Background()
	ns := newNamespace()

	id := evt.EntityID(ns + "-legacy")
	events := serialize(t, id, "test", 0,
		&test.EntityCreated{ID: id, Value: "one"},
		&test.EntityUpdated{ID: id, Value: "two"},
	)
	require.NoError(t, repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: events}, "test", id, []byte(`{"id":"x"}`), 1))

	restore, err := opts.Unmigrate(ctx, repo)
	require.NoError(t, err)
	t.Cleanup(func() { restore() })

	notMigrated := func(err error, what string) {
		t.Helper()
		require.ErrorIs(t, err, evt.ErrStreamHeadsNotMigrated, what)
		require.True(t, evt.IsStreamHeadsNotMigratedErr(err), what)
		require.False(t, evt.IsOptimisticConflict(err), "%s: refusal is not a conflict", what)
		require.False(t, policy.IsTransient(err), "%s: refusal is not transient", what)
	}

	next := serialize(t, id, "test", 2, &test.EntityUpdated{ID: id, Value: "three"})
	notMigrated(repo.Commit(ctx, evt.SerializedResult{Events: next}), "commit")
	notMigrated(repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: next}, "test", id, []byte(`{"id":"x"}`), 2), "commit with snapshot")

	other := evt.EntityID(ns + "-new")
	notMigrated(repo.Commit(ctx, evt.SerializedResult{
		Events: serialize(t, other, "test", 0, &test.EntityCreated{ID: other, Value: "new"}),
	}), "a new stream")

	if outcomes, ok := repo.(evt.OutcomeRepository); ok {
		command := evt.CommandID(ns + "-outcome-only")
		notMigrated(outcomes.Commit(ctx, evt.SerializedResult{
			Outcome: &evt.Outcome{CommandID: command, Status: evt.OutcomeAccepted, Digest: "digest"},
		}), "outcome-only commit")

		stored, gerr := outcomes.GetOutcome(ctx, command)
		require.NoError(t, gerr)
		require.Nil(t, stored)
	}

	if writer, ok := repo.(snapshotWriter); ok {
		notMigrated(writer.PutSnapshot(ctx, "test", id, []byte(`{"id":"x"}`), 2, 2), "put snapshot")
	}

	if compactor, ok := repo.(evt.Compactor); ok {
		_, cerr := compactor.CompactBelow(ctx, id, 2)
		notMigrated(cerr, "compaction")
	}

	// Reads need no feature.
	stored, err := repo.GetEvents(ctx, id)
	require.NoError(t, err)
	require.Len(t, stored, 2)

	snapshot, err := repo.GetSnapshot(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, snapshot)

	// Declared again, the store writes. The first commit after a declaration may fail its marker
	// check and revalidate, which is a transient, retryable outcome.
	restore()

	err = repo.Commit(ctx, evt.SerializedResult{Events: next})
	if errors.Is(err, evt.ErrFeatureManifestChanged) {
		err = repo.Commit(ctx, evt.SerializedResult{Events: next})
	}
	require.NoError(t, err)
}
