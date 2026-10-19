//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/postgres"
)

// The v0.8 to stream-heads migration against a real PostgreSQL server. Every test builds its own
// event log (a private events table and the relations derived from it) the v0.8 way: events and
// snapshots with no head rows and no marker.

type migrationLog struct {
	repo      *postgres.Repository
	pool      *pgxpool.Pool
	events    string
	snapshots string
	heads     string
}

func newMigrationLog(ctx context.Context, t *testing.T) migrationLog {
	t.Helper()

	repo, _, name := newIsolatedEventLog(ctx, t)

	return migrationLog{
		repo: repo, pool: newPool(ctx, t),
		events: name, snapshots: name + "_snapshots", heads: name + "__evt_stream_heads",
	}
}

// event inserts an event as v0.8 wrote it: no head row.
func (l migrationLog) event(ctx context.Context, t *testing.T, id string, sequence int) {
	t.Helper()

	_, err := l.pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (entity_id, sequence, event_id, event_type, version, entity_type, payload)
		 VALUES ($1, $2, $3, 't:event', 1, 'scratch', '{}')`, l.events),
		id, sequence, fmt.Sprintf("%s#%d", id, sequence))
	require.NoError(t, err)
}

func (l migrationLog) snapshot(ctx context.Context, t *testing.T, id string, snapshotSeq, eventSeq int) {
	t.Helper()

	_, err := l.pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (entity_id, entity_type, sequence, event_sequence, payload)
		 VALUES ($1, 'scratch', $2, $3, '{}')`, l.snapshots), id, snapshotSeq, eventSeq)
	require.NoError(t, err)
}

// seed writes the fixture streams of a v0.8 store and returns the head each must get.
func (l migrationLog) seed(ctx context.Context, t *testing.T) map[string]int {
	t.Helper()

	for sequence := 1; sequence <= 3; sequence++ {
		l.event(ctx, t, "a-events-only", sequence)
	}

	// Compacted down to the snapshot: no event remains.
	l.snapshot(ctx, t, "b-snapshot-only", 2, 5)

	// Compacted through 4, events 5 and 6 after it.
	l.snapshot(ctx, t, "c-compacted", 3, 4)
	l.event(ctx, t, "c-compacted", 5)
	l.event(ctx, t, "c-compacted", 6)

	// An out-of-band snapshot ahead of the events: the derived head is the snapshot's.
	l.snapshot(ctx, t, "d-snapshot-ahead", 1, 9)
	l.event(ctx, t, "d-snapshot-ahead", 7)
	l.event(ctx, t, "d-snapshot-ahead", 8)

	// A snapshot of an empty stream: the derived head is 0, and head rows start at 1.
	l.snapshot(ctx, t, "e-empty-snapshot", 1, 0)

	return map[string]int{"a-events-only": 3, "b-snapshot-only": 5, "c-compacted": 6, "d-snapshot-ahead": 9}
}

func (l migrationLog) storedHeads(ctx context.Context, t *testing.T) map[string]int {
	t.Helper()

	rows, err := l.pool.Query(ctx, fmt.Sprintf(`SELECT entity_id, head FROM %s`, l.heads))
	require.NoError(t, err)
	defer rows.Close()

	heads := map[string]int{}

	for rows.Next() {
		var (
			id   string
			head int
		)

		require.NoError(t, rows.Scan(&id, &head))
		heads[id] = head
	}

	require.NoError(t, rows.Err())

	return heads
}

func (l migrationLog) setHead(ctx context.Context, t *testing.T, id string, head int) {
	t.Helper()

	_, err := l.pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (entity_id, entity_type, head) VALUES ($1, 'scratch', $2)
		 ON CONFLICT (entity_id) DO UPDATE SET head = EXCLUDED.head`, l.heads), id, head)
	require.NoError(t, err)
}

func (l migrationLog) requireDeclared(ctx context.Context, t *testing.T, declared bool) {
	t.Helper()

	manifest, err := l.repo.FeatureMarker(nil).Manifest(ctx)
	require.NoError(t, err)

	if declared {
		require.NotNil(t, manifest)
		require.True(t, manifest.DeclaresStreamHeads())

		return
	}

	require.True(t, manifest == nil || !manifest.DeclaresStreamHeads(), "stream-heads must not be declared")
}

func migrationEvents(id string, first, count int) []evt.SerializedEvent {
	events := make([]evt.SerializedEvent, 0, count)

	for i := range count {
		sequence := evt.EventSequence(first + i)
		events = append(events, evt.SerializedEvent{
			ID: evt.GetEventID(evt.EntityID(id), sequence), Sequence: sequence, Type: "t:event", Version: 1,
			EntityID: evt.EntityID(id), EntityType: "scratch", Payload: []byte(`{}`),
			Metadata: evt.Metadata{Region: "us-west-2", Timestamp: "2026-10-12T00:00:00Z"},
		})
	}

	return events
}

func findingKinds(findings []evt.HeadFinding) map[string]evt.HeadFindingKind {
	out := make(map[string]evt.HeadFindingKind, len(findings))
	for _, finding := range findings {
		out[string(finding.EntityID)] = finding.Kind
	}

	return out
}

func Test_MigrateStreamHeads_BackfillsAV08Store(t *testing.T) {
	ctx := context.Background()
	log := newMigrationLog(ctx, t)
	want := log.seed(ctx, t)

	// Before the migration a head-aware writer refuses the populated, undeclared store.
	err := log.repo.Commit(ctx, evt.SerializedResult{Events: migrationEvents("a-events-only", 4, 1)})
	require.ErrorIs(t, err, evt.ErrStreamHeadsNotMigrated)

	before, err := log.repo.VerifyStreamHeads(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(5), before.Streams)
	require.Equal(t, int64(4), before.Total)

	for _, finding := range before.Findings {
		require.Equal(t, evt.HeadMissing, finding.Kind, finding.String())
	}

	report, err := log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	require.True(t, report.Declared)
	require.True(t, report.Manifest.DeclaresStreamHeads())
	require.Equal(t, int64(5), report.Streams)
	require.Equal(t, int64(4), report.Inserted)

	require.Equal(t, want, log.storedHeads(ctx, t))
	log.requireDeclared(ctx, t, true)

	var entityType string

	require.NoError(t, log.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT entity_type FROM %s WHERE entity_id = 'c-compacted'`, log.heads)).Scan(&entityType))
	require.Equal(t, "scratch", entityType)

	// The head table now serves entity heads.
	heads, err := log.repo.StreamEntityHeads(ctx, "")
	require.NoError(t, err)
	require.Len(t, heads, 4)
	require.Equal(t, evt.EventSequence(9), heads["d-snapshot-ahead"])

	verified, err := log.repo.VerifyStreamHeads(ctx)
	require.NoError(t, err)
	require.True(t, verified.OK(), "%v", verified.Findings)

	_, err = log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{ConfirmWritersStopped: true})
	require.ErrorIs(t, err, evt.ErrHeadsAlreadyDeclared)
}

func Test_MigrateStreamHeads_HeadAwareWritersWorkAfterwards(t *testing.T) {
	ctx := context.Background()
	log := newMigrationLog(ctx, t)
	log.seed(ctx, t)

	_, err := log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)

	writer := postgres.NewRepository(log.pool,
		postgres.WithEventsTable(log.events), postgres.WithSnapshotsTable(log.snapshots))

	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: migrationEvents("a-events-only", 4, 2)}))
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: migrationEvents("b-snapshot-only", 6, 1)}))
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: migrationEvents("c-compacted", 7, 1)}))
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: migrationEvents("d-snapshot-ahead", 10, 1)}))
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: migrationEvents("e-empty-snapshot", 1, 1)}))

	require.Equal(t, map[string]int{
		"a-events-only": 5, "b-snapshot-only": 6, "c-compacted": 7, "d-snapshot-ahead": 10, "e-empty-snapshot": 1,
	}, log.storedHeads(ctx, t))

	// A commit that skips the head's successor is a conflict, not a success.
	err = writer.Commit(ctx, evt.SerializedResult{Events: migrationEvents("a-events-only", 4, 1)})
	require.True(t, evt.IsOptimisticConflict(err), "%v", err)

	// A fence on a backfilled stream passes until a commit moves the stream.
	fenced := evt.ReadFence{EntityID: "c-compacted", EntityType: "scratch", Head: 7}
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{
		Events: migrationEvents("new-stream", 1, 1), Fences: []evt.ReadFence{fenced},
	}))

	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: migrationEvents("c-compacted", 8, 1)}))

	err = writer.Commit(ctx, evt.SerializedResult{
		Events: migrationEvents("new-stream", 2, 1), Fences: []evt.ReadFence{fenced},
	})

	var conflict *evt.FenceConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, evt.EntityID("c-compacted"), conflict.Fence.EntityID)

	// An older writer that appends without a head row leaves it stale; the next head-aware append
	// to that sequence is refused as drift.
	log.event(ctx, t, "a-events-only", 6)

	err = writer.Commit(ctx, evt.SerializedResult{Events: migrationEvents("a-events-only", 6, 1)})

	var drift *evt.StreamHeadDriftError
	require.ErrorAs(t, err, &drift)
	require.Equal(t, evt.EntityID("a-events-only"), drift.EntityID)

	report, err := log.repo.VerifyStreamHeads(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]evt.HeadFindingKind{"a-events-only": evt.HeadLower}, findingKinds(report.Findings))
}

func Test_MigrateStreamHeads_PreconditionsRefuseBeforeWriting(t *testing.T) {
	ctx := context.Background()

	t.Run("writers must be confirmed stopped", func(t *testing.T) {
		log := newMigrationLog(ctx, t)
		log.seed(ctx, t)

		_, err := log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{})
		require.ErrorIs(t, err, evt.ErrWritersNotConfirmed)

		_, err = log.repo.RepairStreamHeads(ctx, postgres.StreamHeadsRepairOptions{})
		require.ErrorIs(t, err, evt.ErrWritersNotConfirmed)

		require.Empty(t, log.storedHeads(ctx, t))
		log.requireDeclared(ctx, t, false)
	})

	t.Run("a store that declares journal is refused", func(t *testing.T) {
		log := newMigrationLog(ctx, t)
		log.seed(ctx, t)

		_, err := log.repo.FeatureMarker(nil).Declare(ctx, func(m *evt.FeatureManifest) { m.Features["journal"] = 1 })
		require.NoError(t, err)

		_, err = log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{ConfirmWritersStopped: true})
		require.True(t, evt.IsUnsupportedFeatureErr(err), "%v", err)

		_, err = log.repo.RepairStreamHeads(ctx, postgres.StreamHeadsRepairOptions{ConfirmWritersStopped: true})
		require.True(t, evt.IsUnsupportedFeatureErr(err), "%v", err)

		_, err = log.repo.VerifyStreamHeads(ctx)
		require.True(t, evt.IsUnsupportedFeatureErr(err), "%v", err)

		require.Empty(t, log.storedHeads(ctx, t))
		log.requireDeclared(ctx, t, false)
	})

	t.Run("a dry run counts and rolls back", func(t *testing.T) {
		log := newMigrationLog(ctx, t)
		log.seed(ctx, t)

		report, err := log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{DryRun: true})
		require.NoError(t, err)
		require.True(t, report.DryRun)
		require.False(t, report.Declared)
		require.Equal(t, int64(5), report.Streams)
		require.Equal(t, int64(4), report.Inserted)

		require.Empty(t, log.storedHeads(ctx, t))
		log.requireDeclared(ctx, t, false)
	})
}

// The migration is one transaction: a store with any mismatching head row is left exactly as it
// was, with no head row inserted and no declaration.
func Test_MigrateStreamHeads_AbortsInOneTransaction(t *testing.T) {
	ctx := context.Background()
	log := newMigrationLog(ctx, t)
	want := log.seed(ctx, t)

	// Rows an earlier development build left behind: one too low, one too high, one orphan.
	log.setHead(ctx, t, "a-events-only", 1)
	log.setHead(ctx, t, "b-snapshot-only", 7)
	log.setHead(ctx, t, "ghost", 4)

	before := log.storedHeads(ctx, t)

	report, err := log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{ConfirmWritersStopped: true})

	var unverified *evt.HeadsNotVerifiedError
	require.ErrorAs(t, err, &unverified)
	require.Equal(t, 3, unverified.Total)
	require.Equal(t, map[string]evt.HeadFindingKind{
		"a-events-only": evt.HeadLower, "b-snapshot-only": evt.HeadHigher, "ghost": evt.HeadOrphan,
	}, findingKinds(unverified.Findings))
	require.False(t, report.Declared)

	require.Equal(t, before, log.storedHeads(ctx, t), "the aborted transaction inserted nothing")
	log.requireDeclared(ctx, t, false)

	// The repair fixes the lower and the missing; the higher and the orphan stay for the operator.
	repaired, err := log.repo.RepairStreamHeads(ctx, postgres.StreamHeadsRepairOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	require.Equal(t, int64(2), repaired.Inserted, "c-compacted and d-snapshot-ahead")
	require.Equal(t, int64(1), repaired.Raised)
	require.Equal(t, map[string]evt.HeadFindingKind{
		"b-snapshot-only": evt.HeadHigher, "ghost": evt.HeadOrphan,
	}, findingKinds(repaired.Left))

	lowered, err := log.repo.RepairStreamHeads(ctx, postgres.StreamHeadsRepairOptions{
		ConfirmWritersStopped: true, AllowLower: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), lowered.Lowered)
	require.Equal(t, map[string]evt.HeadFindingKind{"ghost": evt.HeadOrphan}, findingKinds(lowered.Left))

	// An orphan has nothing to lower to; the operator removes it, and the rerun finishes.
	_, err = log.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE entity_id = 'ghost'`, log.heads))
	require.NoError(t, err)

	_, err = log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)

	require.Equal(t, want, log.storedHeads(ctx, t))
	log.requireDeclared(ctx, t, true)
}

func Test_MigrateStreamHeads_VerifyRepairAndAllowLower(t *testing.T) {
	ctx := context.Background()
	log := newMigrationLog(ctx, t)
	want := log.seed(ctx, t)

	_, err := log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)

	_, err = log.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE entity_id = 'a-events-only'`, log.heads)) // missing
	require.NoError(t, err)

	log.setHead(ctx, t, "c-compacted", 4)       // lower
	log.setHead(ctx, t, "d-snapshot-ahead", 12) // higher
	log.setHead(ctx, t, "e-empty-snapshot", 2)  // a head over a snapshot of nothing

	// An older writer creates a stream after the declaration.
	log.event(ctx, t, "z-stray", 1)

	report, err := log.repo.VerifyStreamHeads(ctx)
	require.NoError(t, err)
	require.False(t, report.OK())
	require.Equal(t, map[string]evt.HeadFindingKind{
		"a-events-only":    evt.HeadMissing,
		"c-compacted":      evt.HeadLower,
		"d-snapshot-ahead": evt.HeadHigher,
		"e-empty-snapshot": evt.HeadOrphan,
		"z-stray":          evt.HeadMissing,
	}, findingKinds(report.Findings))

	dry, err := log.repo.RepairStreamHeads(ctx, postgres.StreamHeadsRepairOptions{DryRun: true})
	require.NoError(t, err)
	require.Equal(t, map[string]evt.HeadFindingKind{
		"a-events-only": evt.HeadMissing, "c-compacted": evt.HeadLower, "z-stray": evt.HeadMissing,
	}, findingKinds(dry.Repaired), "a dry run lists what a repair would fix")
	require.Equal(t, map[string]evt.HeadFindingKind{
		"d-snapshot-ahead": evt.HeadHigher, "e-empty-snapshot": evt.HeadOrphan,
	}, findingKinds(dry.Left), "and what it would leave")

	dryLower, err := log.repo.RepairStreamHeads(ctx, postgres.StreamHeadsRepairOptions{DryRun: true, AllowLower: true})
	require.NoError(t, err)
	require.Len(t, dryLower.Repaired, 4)
	require.Equal(t, map[string]evt.HeadFindingKind{"e-empty-snapshot": evt.HeadOrphan}, findingKinds(dryLower.Left))

	unchanged, err := log.repo.VerifyStreamHeads(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(5), unchanged.Total, "a dry run changes nothing")

	repaired, err := log.repo.RepairStreamHeads(ctx, postgres.StreamHeadsRepairOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	require.Equal(t, int64(2), repaired.Inserted)
	require.Equal(t, int64(1), repaired.Raised)
	require.Equal(t, int64(0), repaired.Lowered)
	require.Equal(t, map[string]evt.HeadFindingKind{
		"d-snapshot-ahead": evt.HeadHigher, "e-empty-snapshot": evt.HeadOrphan,
	}, findingKinds(repaired.Left))

	require.Equal(t, 12, log.storedHeads(ctx, t)["d-snapshot-ahead"], "never lowered without -allow-lower")

	lowered, err := log.repo.RepairStreamHeads(ctx, postgres.StreamHeadsRepairOptions{
		ConfirmWritersStopped: true, AllowLower: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), lowered.Lowered)

	_, err = log.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE entity_id = 'e-empty-snapshot'`, log.heads))
	require.NoError(t, err)

	final, err := log.repo.VerifyStreamHeads(ctx)
	require.NoError(t, err)
	require.True(t, final.OK(), "%v", final.Findings)

	want["z-stray"] = 1
	require.Equal(t, want, log.storedHeads(ctx, t))
}

// A database written by v0.8 has no head table until EnsureSchema runs. Verification of it reports
// every stream as missing instead of failing, and the migration creates the table.
func Test_MigrateStreamHeads_WithoutAHeadTable(t *testing.T) {
	ctx := context.Background()
	log := newMigrationLog(ctx, t)
	want := log.seed(ctx, t)

	_, err := log.pool.Exec(ctx, "DROP TABLE "+log.heads)
	require.NoError(t, err)

	report, err := log.repo.VerifyStreamHeads(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(4), report.Total)

	dry, err := log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{DryRun: true})
	require.NoError(t, err)
	require.Equal(t, int64(4), dry.Inserted)

	_, err = log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	require.Equal(t, want, log.storedHeads(ctx, t))
}

// The migration holds a SHARE lock on the event tables, so a v0.8 writer with a transaction open
// finishes first and its stream is backfilled, and one that arrives meanwhile waits.
func Test_MigrateStreamHeads_HoldsOffOlderWriters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	log := newMigrationLog(ctx, t)
	log.seed(ctx, t)

	// An older writer is mid-transaction on the events table.
	tx, err := log.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // ended by the commit below on the success path

	_, err = tx.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (entity_id, sequence, event_id, event_type, version, entity_type, payload)
		 VALUES ('y-inflight', 1, 'y-inflight#1', 't:event', 1, 'scratch', '{}')`, log.events))
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() {
		_, merr := log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{ConfirmWritersStopped: true})
		done <- merr
	}()

	select {
	case err = <-done:
		t.Fatalf("the migration finished while an older writer's transaction was open: %v", err)
	case <-time.After(750 * time.Millisecond):
	}

	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, <-done)

	heads := log.storedHeads(ctx, t)
	require.Equal(t, 1, heads["y-inflight"], "the migration saw the stream the older writer committed")

	verified, err := log.repo.VerifyStreamHeads(ctx)
	require.NoError(t, err)
	require.True(t, verified.OK(), "%v", verified.Findings)
}

func Test_MigrateStreamHeads_AnEmptyStoreMigrates(t *testing.T) {
	ctx := context.Background()
	log := newMigrationLog(ctx, t)

	report, err := log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	require.True(t, report.Declared)
	require.Equal(t, int64(0), report.Streams)
	log.requireDeclared(ctx, t, true)
}

// The dry runs and the verification are read-only snapshots: they take no lock, so they neither wait
// behind a writer's open transaction nor make writers wait.
func Test_MigrateStreamHeads_DryRunsTakeNoLock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log := newMigrationLog(ctx, t)
	log.seed(ctx, t)

	tx, err := log.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // the transaction is only held open

	_, err = tx.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (entity_id, sequence, event_id, event_type, version, entity_type, payload)
		 VALUES ('y-inflight', 1, 'y-inflight#1', 't:event', 1, 'scratch', '{}')`, log.events))
	require.NoError(t, err)

	short, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()

	migrated, err := log.repo.MigrateStreamHeads(short, postgres.StreamHeadsMigrateOptions{DryRun: true})
	require.NoError(t, err)
	require.Equal(t, int64(4), migrated.Inserted, "the open transaction's stream is not visible to the snapshot")

	_, err = log.repo.RepairStreamHeads(short, postgres.StreamHeadsRepairOptions{DryRun: true})
	require.NoError(t, err)

	_, err = log.repo.VerifyStreamHeads(short)
	require.NoError(t, err)

	require.NoError(t, tx.Commit(ctx))
}

// A pre-v0.9 stream whose ID falls in a reserved namespace is hidden from head-aware code, so the
// migration refuses to backfill or declare around it until it is renamed, and leaves the database
// as it found it.
func Test_MigrateStreamHeads_RefusesAStreamInAReservedNamespace(t *testing.T) {
	ctx := context.Background()
	log := newMigrationLog(ctx, t)
	log.seed(ctx, t)

	log.event(ctx, t, "SYSTEM#account", 1)
	log.snapshot(ctx, t, "command-outcome#legacy", 1, 3)

	for _, dryRun := range []bool{true, false} {
		_, err := log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{
			ConfirmWritersStopped: true, DryRun: dryRun,
		})

		var reserved *evt.ReservedStreamsError
		require.ErrorAs(t, err, &reserved)
		require.Equal(t, []evt.EntityID{"SYSTEM#account", "command-outcome#legacy"}, reserved.IDs)
	}

	require.Empty(t, log.storedHeads(ctx, t))
	log.requireDeclared(ctx, t, false)

	verified, err := log.repo.VerifyStreamHeads(ctx)
	require.NoError(t, err)
	require.Equal(t, []evt.EntityID{"SYSTEM#account", "command-outcome#legacy"}, verified.ReservedStreams)

	// Renamed (here: removed), the migration goes through.
	_, err = log.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE entity_id = 'SYSTEM#account'`, log.events))
	require.NoError(t, err)

	_, err = log.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE entity_id = 'command-outcome#legacy'`, log.snapshots))
	require.NoError(t, err)

	_, err = log.repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	log.requireDeclared(ctx, t, true)
}

// The lists in a report are capped at 1000, the classification is not: a higher or orphaned head
// sorted past the cap is still counted, and a repair dry run still says it would leave it.
func Test_MigrateStreamHeads_ClassifiesPastTheReportCap(t *testing.T) {
	ctx := context.Background()
	log := newMigrationLog(ctx, t)

	// 1,100 streams with no head row sort before the two that matter.
	_, err := log.pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (entity_id, sequence, event_id, event_type, version, entity_type, payload)
		 SELECT 'm-' || lpad(i::text, 5, '0'), 1, 'e' || i, 't:event', 1, 'scratch', '{}'
		 FROM generate_series(1, 1100) AS i`, log.events))
	require.NoError(t, err)

	log.event(ctx, t, "zz-higher", 1)
	log.setHead(ctx, t, "zz-higher", 5)
	log.setHead(ctx, t, "zz-orphan", 2)

	verified, err := log.repo.VerifyStreamHeads(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1102), verified.Total)
	require.Len(t, verified.Findings, 1000, "the list is capped")
	require.Equal(t, map[evt.HeadFindingKind]int64{
		evt.HeadMissing: 1100, evt.HeadHigher: 1, evt.HeadOrphan: 1,
	}, verified.Counts, "the counts are not")

	dry, err := log.repo.RepairStreamHeads(ctx, postgres.StreamHeadsRepairOptions{DryRun: true})
	require.NoError(t, err)
	require.Equal(t, int64(1100), dry.RepairedTotal)
	require.Equal(t, int64(2), dry.LeftTotal, "the higher and the orphaned head are past the capped list")

	dryLower, err := log.repo.RepairStreamHeads(ctx, postgres.StreamHeadsRepairOptions{DryRun: true, AllowLower: true})
	require.NoError(t, err)
	require.Equal(t, int64(1101), dryLower.RepairedTotal)
	require.Equal(t, int64(1), dryLower.LeftTotal)

	repaired, err := log.repo.RepairStreamHeads(ctx, postgres.StreamHeadsRepairOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	require.Equal(t, int64(1100), repaired.Inserted)
	require.Equal(t, int64(2), repaired.LeftTotal)
	require.Len(t, repaired.Left, 2)
}
