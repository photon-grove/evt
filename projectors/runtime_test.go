package projectors_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/photon-grove/evt/projectors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubProjector is a configurable test double for the Projector interface.
type stubProjector struct {
	name       string
	processErr error
	failures   []projectors.BatchItemFailure
	called     int
	lastBatch  []projectors.StreamRecord
}

func (s *stubProjector) Name() string { return s.name }

func (s *stubProjector) Process(_ context.Context, records []projectors.StreamRecord) ([]projectors.BatchItemFailure, error) {
	s.called++
	s.lastBatch = records
	return s.failures, s.processErr
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestRuntime_ProcessRecords(t *testing.T) {
	proj := &stubProjector{name: "test-proj"}
	guard := projectors.NewInMemoryIdempotencyGuard()
	rt := projectors.NewRuntime(proj, guard, testLogger())

	records := []projectors.StreamRecord{
		{EventID: "evt-1", EntityID: "ent-1", EventType: "created"},
		{EventID: "evt-2", EntityID: "ent-2", EventType: "updated"},
	}

	failures, err := rt.Process(context.Background(), records)
	require.NoError(t, err)
	assert.Empty(t, failures)
	assert.Equal(t, 1, proj.called)
	assert.Len(t, proj.lastBatch, 2)
}

func TestRuntime_SkipsAlreadyProcessed(t *testing.T) {
	proj := &stubProjector{name: "test-proj"}
	guard := projectors.NewInMemoryIdempotencyGuard()
	rt := projectors.NewRuntime(proj, guard, testLogger())

	// Pre-mark one event as processed.
	require.NoError(t, guard.MarkProcessed(context.Background(), "test-proj", "evt-1"))

	records := []projectors.StreamRecord{
		{EventID: "evt-1", EntityID: "ent-1", EventType: "created"},
		{EventID: "evt-2", EntityID: "ent-2", EventType: "updated"},
	}

	failures, err := rt.Process(context.Background(), records)
	require.NoError(t, err)
	assert.Empty(t, failures)
	// Only evt-2 should have been passed to the projector.
	assert.Equal(t, 1, proj.called)
	assert.Len(t, proj.lastBatch, 1)
	assert.Equal(t, "evt-2", proj.lastBatch[0].EventID)
}

func TestRuntime_AllAlreadyProcessed(t *testing.T) {
	proj := &stubProjector{name: "test-proj"}
	guard := projectors.NewInMemoryIdempotencyGuard()
	rt := projectors.NewRuntime(proj, guard, testLogger())

	ctx := context.Background()
	require.NoError(t, guard.MarkProcessed(ctx, "test-proj", "evt-1"))

	records := []projectors.StreamRecord{
		{EventID: "evt-1"},
	}

	failures, err := rt.Process(ctx, records)
	require.NoError(t, err)
	assert.Empty(t, failures)
	assert.Equal(t, 0, proj.called, "projector should not be called when all events are skipped")
}

func TestRuntime_PartialBatchFailure(t *testing.T) {
	proj := &stubProjector{
		name: "test-proj",
		failures: []projectors.BatchItemFailure{
			{ItemIdentifier: "evt-2"},
		},
	}
	guard := projectors.NewInMemoryIdempotencyGuard()
	rt := projectors.NewRuntime(proj, guard, testLogger())

	records := []projectors.StreamRecord{
		{EventID: "evt-1"},
		{EventID: "evt-2"},
	}

	failures, err := rt.Process(context.Background(), records)
	require.NoError(t, err)
	require.Len(t, failures, 1)
	assert.Equal(t, "evt-2", failures[0].ItemIdentifier)

	// evt-1 should be marked processed, evt-2 should not.
	ctx := context.Background()
	processed, err := guard.IsProcessed(ctx, "test-proj", "evt-1")
	require.NoError(t, err)
	assert.True(t, processed)
	processed, err = guard.IsProcessed(ctx, "test-proj", "evt-2")
	require.NoError(t, err)
	assert.False(t, processed)
}

func TestRuntime_PermanentError_ReportsAllAsFailures(t *testing.T) {
	proj := &stubProjector{
		name:       "test-proj",
		processErr: projectors.NewPermanentError(errors.New("schema violation")),
	}
	guard := projectors.NewInMemoryIdempotencyGuard()
	rt := projectors.NewRuntime(proj, guard, testLogger())

	records := []projectors.StreamRecord{
		{EventID: "evt-1"},
		{EventID: "evt-2"},
	}

	failures, err := rt.Process(context.Background(), records)
	require.NoError(t, err, "permanent errors should not propagate as an error")
	assert.Len(t, failures, 2, "all records should be reported as failures for DLQ")
}

func TestRuntime_TransientError_PropagatesError(t *testing.T) {
	proj := &stubProjector{
		name:       "test-proj",
		processErr: context.DeadlineExceeded,
	}
	guard := projectors.NewInMemoryIdempotencyGuard()
	rt := projectors.NewRuntime(proj, guard, testLogger())

	records := []projectors.StreamRecord{
		{EventID: "evt-1"},
	}

	_, err := rt.Process(context.Background(), records)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}
