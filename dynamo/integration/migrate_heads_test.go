//go:build integration

package integration

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/internal/eventjson"
)

// The v0.8 to stream-heads migration against the local emulator. Every test gets a private table
// without a stream (newIsolatedEventLog) that is deleted afterward, and every store is built the
// v0.8 way: raw event and snapshot items with no head item and no marker.

// migrationNow is the fixed clock of the migration tests: items with a ttl before it have expired.
const migrationNow = 1_791_763_200

func migrationClock() time.Time { return time.Unix(migrationNow, 0) }

func migrationFor(repo *dynamo.Repository, progress func(dynamo.HeadsMigrationPhase)) *dynamo.HeadsMigration {
	return repo.HeadsMigration(dynamo.HeadsMigrationConfig{
		// Three items per page splits nearly every partition across a page boundary.
		PageSize:    3,
		Concurrency: 2,
		Now:         migrationClock,
		Progress:    progress,
	})
}

func number(n int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(n, 10)}
}

// v08Event is an event item as v0.8 wrote it. A ttl of 0 writes none.
func v08Event(t *testing.T, id string, sequence int, ttl int64) map[string]types.AttributeValue {
	t.Helper()

	metadata, err := eventjson.MarshalMetadata(evt.Metadata{Region: "us-west-2", Timestamp: "2026-10-12T00:00:00Z"})
	require.NoError(t, err)

	item := map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: id},
		"sk":         number(int64(sequence)),
		"type":       &types.AttributeValueMemberS{Value: "t:event"},
		"version":    number(1),
		"entityType": &types.AttributeValueMemberS{Value: "scratch"},
		"payload":    &types.AttributeValueMemberS{Value: "{}"},
		"metadata":   &types.AttributeValueMemberS{Value: string(metadata)},
	}

	if ttl != 0 {
		item["ttl"] = number(ttl)
	}

	return item
}

// v08Snapshot is an inline snapshot item (sk = 0) covering events through eventSeq.
func v08Snapshot(id string, snapshotSeq, eventSeq int, ttl int64) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: id},
		"sk":         number(0),
		"seq":        number(int64(snapshotSeq)),
		"eventSeq":   number(int64(eventSeq)),
		"entityType": &types.AttributeValueMemberS{Value: "scratch"},
		"payload":    &types.AttributeValueMemberS{Value: "{}"},
	}

	if ttl != 0 {
		item["ttl"] = number(ttl)
	}

	return item
}

func rawPut(ctx context.Context, t *testing.T, repo *dynamo.Repository, items ...map[string]types.AttributeValue) {
	t.Helper()

	for _, item := range items {
		_, err := repoClient().PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(repo.EventsTable), Item: item})
		require.NoError(t, err)
	}
}

func rawDeleteHead(ctx context.Context, t *testing.T, repo *dynamo.Repository, id string) {
	t.Helper()

	_, err := repoClient().DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(repo.EventsTable),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: id},
			"sk": number(-1),
		},
	})
	require.NoError(t, err)
}

func rawSetHead(ctx context.Context, t *testing.T, repo *dynamo.Repository, id string, head int, ttl int64) {
	t.Helper()

	item := map[string]types.AttributeValue{
		"pk":   &types.AttributeValueMemberS{Value: id},
		"sk":   number(-1),
		"head": number(int64(head)),
	}

	if ttl != 0 {
		item["ttl"] = number(ttl)
	}

	rawPut(ctx, t, repo, item)
}

// v08Expectation is a stream of the v0.8 fixture store and the head item the backfill must write.
type v08Expectation struct {
	id      string
	items   []map[string]types.AttributeValue
	head    int   // 0: no head item is written
	headTTL int64 // 0: the head item has no ttl
}

// seedV08Store writes a store as v0.8 left it. The items of all streams are written round-robin,
// so the partitions are interleaved as well as split by page boundaries.
func seedV08Store(ctx context.Context, t *testing.T, repo *dynamo.Repository) []v08Expectation {
	t.Helper()

	soon := func(seconds int64) int64 { return migrationNow + seconds }

	streams := []v08Expectation{
		{id: "a-events-only", head: 3, items: []map[string]types.AttributeValue{
			v08Event(t, "a-events-only", 1, 0), v08Event(t, "a-events-only", 2, 0), v08Event(t, "a-events-only", 3, 0),
		}},
		// Compacted down to the snapshot: no event remains.
		{id: "b-snapshot-only", head: 5, items: []map[string]types.AttributeValue{
			v08Snapshot("b-snapshot-only", 2, 5, 0),
		}},
		// Compacted through 4, events 5 and 6 after it.
		{id: "c-compacted", head: 6, items: []map[string]types.AttributeValue{
			v08Snapshot("c-compacted", 3, 4, 0), v08Event(t, "c-compacted", 5, 0), v08Event(t, "c-compacted", 6, 0),
		}},
		// An out-of-band snapshot ahead of the events: the derived head is the snapshot's.
		{id: "d-snapshot-ahead", head: 9, items: []map[string]types.AttributeValue{
			v08Snapshot("d-snapshot-ahead", 1, 9, 0), v08Event(t, "d-snapshot-ahead", 7, 0), v08Event(t, "d-snapshot-ahead", 8, 0),
		}},
		// A snapshot of an empty stream: the derived head is 0, and head items start at 1.
		{id: "e-empty-snapshot", head: 0, items: []map[string]types.AttributeValue{
			v08Snapshot("e-empty-snapshot", 1, 0, 0),
		}},
		// A retained stream whose items carry different ttls: the head item takes the largest,
		// the snapshot's here.
		{id: "f-retained", head: 3, headTTL: soon(300), items: []map[string]types.AttributeValue{
			v08Event(t, "f-retained", 1, soon(100)), v08Event(t, "f-retained", 2, soon(200)),
			v08Event(t, "f-retained", 3, soon(250)), v08Snapshot("f-retained", 1, 2, soon(300)),
		}},
		// The early events of a retained stream have expired but are not deleted yet: they are
		// ignored, and the head item takes the survivor's ttl.
		{id: "g-retained-partly-expired", head: 3, headTTL: soon(50), items: []map[string]types.AttributeValue{
			v08Event(t, "g-retained-partly-expired", 1, soon(-10)), v08Event(t, "g-retained-partly-expired", 2, soon(-5)),
			v08Event(t, "g-retained-partly-expired", 3, soon(50)),
		}},
		// Everything has expired: nothing is left to count.
		{id: "h-all-expired", head: 0, items: []map[string]types.AttributeValue{
			v08Event(t, "h-all-expired", 1, soon(-1)), v08Event(t, "h-all-expired", 2, soon(-1)),
		}},
		// One item expires and one never does: a head item with the larger ttl would be deleted
		// under the stream, so it gets none.
		{id: "i-mixed-ttl", head: 2, items: []map[string]types.AttributeValue{
			v08Event(t, "i-mixed-ttl", 1, soon(10)), v08Event(t, "i-mixed-ttl", 2, 0),
		}},
		{id: "j-retained-snapshot-outlives", head: 4, headTTL: soon(500), items: []map[string]types.AttributeValue{
			v08Snapshot("j-retained-snapshot-outlives", 1, 3, soon(500)), v08Event(t, "j-retained-snapshot-outlives", 4, soon(100)),
		}},
	}

	// Engine rows in the reserved namespaces are not streams.
	reserved := []map[string]types.AttributeValue{
		{
			"pk": &types.AttributeValueMemberS{Value: "command-outcome#c1"}, "sk": number(1),
			"status": &types.AttributeValueMemberS{Value: "accepted"},
		},
		{
			"pk": &types.AttributeValueMemberS{Value: "SYSTEM#REBUILD"}, "sk": number(0),
			"owner": &types.AttributeValueMemberS{Value: "job"}, "leaseExpiresAt": number(migrationNow + 60),
		},
	}

	var queue []map[string]types.AttributeValue

	for round := 0; ; round++ {
		added := false

		for _, stream := range streams {
			if round < len(stream.items) {
				queue = append(queue, stream.items[round])
				added = true
			}
		}

		if round < len(reserved) {
			queue = append(queue, reserved[round])
			added = true
		}

		if !added {
			break
		}
	}

	rawPut(ctx, t, repo, queue...)

	return streams
}

// requireHeads asserts every stream of the fixture store has exactly the expected head item.
func requireHeads(ctx context.Context, t *testing.T, repo *dynamo.Repository, streams []v08Expectation) {
	t.Helper()

	for _, stream := range streams {
		head, ttl, present := storedHead(ctx, t, repo, stream.id)
		if stream.head == 0 {
			require.False(t, present, "%s needs no head item", stream.id)

			continue
		}

		require.True(t, present, "%s has no head item", stream.id)
		require.Equal(t, stream.head, head, "%s head", stream.id)
		require.Equal(t, stream.headTTL, int64(ttl), "%s head ttl", stream.id)
	}

	for _, id := range []string{"SYSTEM#REBUILD", "command-outcome#c1"} {
		_, _, present := storedHead(ctx, t, repo, id)
		require.False(t, present, "the reserved namespace %s is skipped", id)
	}
}

func requireDeclared(ctx context.Context, t *testing.T, repo *dynamo.Repository, declared bool) {
	t.Helper()

	manifest, err := repo.FeatureMarker(nil).Manifest(ctx)
	require.NoError(t, err)

	if !declared {
		require.True(t, manifest == nil || !manifest.DeclaresStreamHeads(), "stream-heads must not be declared")

		return
	}

	require.NotNil(t, manifest)
	require.True(t, manifest.DeclaresStreamHeads())
}

func kinds(findings []evt.HeadFinding) map[string]evt.HeadFindingKind {
	out := make(map[string]evt.HeadFindingKind, len(findings))
	for _, finding := range findings {
		out[string(finding.EntityID)] = finding.Kind
	}

	return out
}

func Test_MigrateHeads_BackfillsAV08Store(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)
	streams := seedV08Store(ctx, t, repo)

	// Before the migration a head-aware writer refuses the populated, undeclared table.
	err := repo.Commit(ctx, evt.SerializedResult{Events: scratchEvents("a-events-only", 4, 1)})
	require.ErrorIs(t, err, evt.ErrStreamHeadsNotMigrated)

	// Verification before the backfill reports every stream that needs a head item as missing.
	before, err := migrationFor(repo, nil).Verify(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, before.Streams)
	require.Len(t, before.Findings, 8)

	for _, finding := range before.Findings {
		require.Equal(t, evt.HeadMissing, finding.Kind, finding.String())
	}

	var phases []dynamo.HeadsMigrationPhase

	report, err := migrationFor(repo, func(p dynamo.HeadsMigrationPhase) { phases = append(phases, p) }).
		Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)

	require.Equal(t, []dynamo.HeadsMigrationPhase{
		dynamo.PhaseBackfill, dynamo.PhaseVerify, dynamo.PhaseVerifyBeforeDeclare,
		dynamo.PhaseDeclare, dynamo.PhaseVerifyAfterDeclare,
	}, phases)

	require.True(t, report.Declared)
	require.True(t, report.Manifest.DeclaresStreamHeads())
	require.Equal(t, int64(1), report.Manifest.Version)
	require.True(t, report.Verify.OK())
	require.Equal(t, dynamo.HeadsBackfillReport{
		Streams: 10, Empty: 1, ZeroHead: 1, Written: 8, MixedTTL: 1,
	}, report.Backfill)

	requireHeads(ctx, t, repo, streams)
	requireDeclared(ctx, t, repo, true)

	// A second run is refused: the store is migrated.
	_, err = migrationFor(repo, nil).Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})
	require.ErrorIs(t, err, evt.ErrHeadsAlreadyDeclared)

	after, err := migrationFor(repo, nil).Verify(ctx)
	require.NoError(t, err)
	require.True(t, after.OK(), "%v", after.Findings)
	require.Empty(t, after.Warnings)
}

func Test_MigrateHeads_HeadAwareWritersWorkAfterwards(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)
	seedV08Store(ctx, t, repo)

	_, err := migrationFor(repo, nil).Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)

	// A fresh head-aware writer appends to a backfilled stream: the head item continues from the
	// backfilled head, whether the stream ended in events, a snapshot, or both.
	writer := dynamo.NewRepository(repoClient(), repo.EventsTable)
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: scratchEvents("a-events-only", 4, 2)}))
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: scratchEvents("b-snapshot-only", 6, 1)}))
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: scratchEvents("c-compacted", 7, 1)}))
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: scratchEvents("d-snapshot-ahead", 10, 1)}))
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: scratchEvents("e-empty-snapshot", 1, 1)}))

	for id, want := range map[string]int{
		"a-events-only": 5, "b-snapshot-only": 6, "c-compacted": 7, "d-snapshot-ahead": 10, "e-empty-snapshot": 1,
	} {
		head, _, present := storedHead(ctx, t, repo, id)
		require.True(t, present, id)
		require.Equal(t, want, head, id)
	}

	// A commit that skips the head's successor is a conflict, not a success.
	err = writer.Commit(ctx, evt.SerializedResult{Events: scratchEvents("a-events-only", 4, 1)})
	require.True(t, evt.IsOptimisticConflict(err), "%v", err)

	// Fences: a read fence on a backfilled stream passes while the stream is unchanged and fails
	// once a commit moves it.
	fenced := evt.ReadFence{EntityID: "c-compacted", EntityType: "scratch", Head: 7}
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{
		Events: scratchEvents("new-stream", 1, 1),
		Fences: []evt.ReadFence{fenced},
	}))

	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: scratchEvents("c-compacted", 8, 1)}))

	err = writer.Commit(ctx, evt.SerializedResult{
		Events: scratchEvents("new-stream", 2, 1),
		Fences: []evt.ReadFence{fenced},
	})

	var conflict *evt.FenceConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, evt.EntityID("c-compacted"), conflict.Fence.EntityID)

	// Drift: an older writer that appends without the head item leaves the stream's head stale,
	// and the next head-aware append to that sequence is refused as drift, never retried.
	rawPut(ctx, t, repo, v08Event(t, "a-events-only", 6, 0))

	err = writer.Commit(ctx, evt.SerializedResult{Events: scratchEvents("a-events-only", 6, 1)})

	var drift *evt.StreamHeadDriftError
	require.ErrorAs(t, err, &drift)
	require.Equal(t, evt.EntityID("a-events-only"), drift.EntityID)

	// ... and the doctor sees it.
	report, err := migrationFor(repo, nil).Verify(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]evt.HeadFindingKind{"a-events-only": evt.HeadLower}, kinds(report.Findings))
}

func Test_MigrateHeads_RetainedHeadsKeepTheirTTLForAHeadAwareWriter(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)
	seedV08Store(ctx, t, repo)

	_, err := migrationFor(repo, nil).Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)

	// The writer that loaded the stream learns the head's ttl and appends under the never-lower
	// rule, so the head item keeps the largest ttl.
	retention := dynamo.Retention{"scratch": time.Hour}
	writer := dynamo.NewRepository(repoClient(), repo.EventsTable).WithRetention(retention).WithClock(migrationClock)

	_, err = writer.GetEvents(ctx, "f-retained")
	require.NoError(t, err)
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: scratchEvents("f-retained", 4, 1)}))

	head, ttl, present := storedHead(ctx, t, repo, "f-retained")
	require.True(t, present)
	require.Equal(t, 4, head)
	require.Equal(t, int(migrationNow+3600), ttl, "the append raised the head ttl and never lowered it")
}

func Test_MigrateHeads_PreconditionsRefuseBeforeWriting(t *testing.T) {
	ctx := context.Background()

	t.Run("writers must be confirmed stopped", func(t *testing.T) {
		repo, _ := newIsolatedEventLog(ctx, t)
		streams := seedV08Store(ctx, t, repo)

		_, err := migrationFor(repo, nil).Migrate(ctx, dynamo.HeadsMigrateOptions{})
		require.ErrorIs(t, err, evt.ErrWritersNotConfirmed)

		_, err = migrationFor(repo, nil).Repair(ctx, dynamo.HeadsRepairOptions{})
		require.ErrorIs(t, err, evt.ErrWritersNotConfirmed)

		for _, stream := range streams {
			_, _, present := storedHead(ctx, t, repo, stream.id)
			require.False(t, present, stream.id)
		}

		requireDeclared(ctx, t, repo, false)
	})

	t.Run("a table that declares journal is refused", func(t *testing.T) {
		repo, marker := newIsolatedEventLog(ctx, t)
		streams := seedV08Store(ctx, t, repo)

		_, err := marker.Declare(ctx, func(m *evt.FeatureManifest) { m.Features["journal"] = 1 })
		require.NoError(t, err)

		migration := migrationFor(repo, nil)

		_, err = migration.Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})
		require.True(t, evt.IsUnsupportedFeatureErr(err), "%v", err)

		_, err = migration.Repair(ctx, dynamo.HeadsRepairOptions{ConfirmWritersStopped: true})
		require.True(t, evt.IsUnsupportedFeatureErr(err), "%v", err)

		_, err = migration.Verify(ctx)
		require.True(t, evt.IsUnsupportedFeatureErr(err), "%v", err)

		for _, stream := range streams {
			_, _, present := storedHead(ctx, t, repo, stream.id)
			require.False(t, present, stream.id)
		}

		manifest, err := marker.Manifest(ctx)
		require.NoError(t, err)
		require.False(t, manifest.DeclaresStreamHeads())
	})

	t.Run("a dry run counts and writes nothing", func(t *testing.T) {
		repo, _ := newIsolatedEventLog(ctx, t)
		streams := seedV08Store(ctx, t, repo)

		report, err := migrationFor(repo, nil).Migrate(ctx, dynamo.HeadsMigrateOptions{DryRun: true})
		require.NoError(t, err)
		require.Equal(t, dynamo.HeadsBackfillReport{
			DryRun: true, Streams: 10, Empty: 1, ZeroHead: 1, Written: 8, MixedTTL: 1,
		}, report.Backfill)
		require.False(t, report.Declared)

		for _, stream := range streams {
			_, _, present := storedHead(ctx, t, repo, stream.id)
			require.False(t, present, stream.id)
		}

		requireDeclared(ctx, t, repo, false)
	})
}

// failingWrites passes calls through until failAfter successful UpdateItem calls, then fails them
// all: an interruption partway through the backfill.
type failingWrites struct {
	dynamo.Client
	failAfter int64
	calls     atomic.Int64
}

var errInterrupted = errors.New("interrupted")

func (f *failingWrites) UpdateItem(
	ctx context.Context,
	params *dynamodb.UpdateItemInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.UpdateItemOutput, error) {
	if f.calls.Add(1) > f.failAfter {
		return nil, errInterrupted
	}

	return f.Client.UpdateItem(ctx, params, options...)
}

func Test_MigrateHeads_ResumesAfterAnInterruption(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)
	streams := seedV08Store(ctx, t, repo)

	flaky := &failingWrites{Client: repoClient(), failAfter: 3}
	interrupted := dynamo.NewRepository(flaky, repo.EventsTable).HeadsMigration(dynamo.HeadsMigrationConfig{
		PageSize: 3, Concurrency: 1, Now: migrationClock,
	})

	_, err := interrupted.Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})
	require.ErrorIs(t, err, errInterrupted)

	written := 0

	for _, stream := range streams {
		if _, _, present := storedHead(ctx, t, repo, stream.id); present {
			written++
		}
	}

	require.Equal(t, 3, written, "the interruption left a partial backfill")
	requireDeclared(ctx, t, repo, false)

	// A partial backfill cannot be mistaken for a finished one.
	partial, err := migrationFor(repo, nil).Verify(ctx)
	require.NoError(t, err)
	require.Len(t, partial.Findings, 5)

	report, err := migrationFor(repo, nil).Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	require.Equal(t, 3, report.Backfill.Present, "the rerun leaves the head items already written")
	require.Equal(t, 5, report.Backfill.Written)
	require.True(t, report.Declared)

	requireHeads(ctx, t, repo, streams)
}

func Test_MigrateHeads_VerifyRepairAndAllowLower(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)
	streams := seedV08Store(ctx, t, repo)

	_, err := migrationFor(repo, nil).Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)

	// Drift, as a stray older writer or an operator slip would leave it.
	rawDeleteHead(ctx, t, repo, "a-events-only")                      // missing
	rawSetHead(ctx, t, repo, "c-compacted", 4, 0)                     // lower
	rawSetHead(ctx, t, repo, "d-snapshot-ahead", 12, 0)               // higher
	rawSetHead(ctx, t, repo, "f-retained", 3, migrationNow+10)        // ttl below the stream's
	rawSetHead(ctx, t, repo, "b-snapshot-only", 5, migrationNow+1000) // a ttl on a stream that never expires
	rawSetHead(ctx, t, repo, "e-empty-snapshot", 2, 0)                // a head over a snapshot of nothing
	rawSetHead(ctx, t, repo, "no-such-stream", 5, 0)                  // an orphan that never expires
	rawSetHead(ctx, t, repo, "expiring-orphan", 5, migrationNow+60)   // an orphan that will expire

	migration := migrationFor(repo, nil)

	report, err := migration.Verify(ctx)
	require.NoError(t, err)
	require.False(t, report.OK())
	require.Equal(t, map[string]evt.HeadFindingKind{
		"a-events-only":    evt.HeadMissing,
		"c-compacted":      evt.HeadLower,
		"d-snapshot-ahead": evt.HeadHigher,
		"f-retained":       evt.HeadTTLLow,
		"b-snapshot-only":  evt.HeadTTLLow,
		"e-empty-snapshot": evt.HeadHigher,
		"no-such-stream":   evt.HeadOrphan,
	}, kinds(report.Findings))
	require.Equal(t, map[string]evt.HeadFindingKind{"expiring-orphan": evt.HeadOrphan}, kinds(report.Warnings))

	for _, finding := range report.Findings {
		switch finding.EntityID {
		case "c-compacted":
			require.Equal(t, evt.EventSequence(4), finding.Stored)
			require.Equal(t, evt.EventSequence(6), finding.Derived)
		case "d-snapshot-ahead":
			require.Equal(t, evt.EventSequence(12), finding.Stored)
			require.Equal(t, evt.EventSequence(9), finding.Derived)
		}
	}

	// A dry run changes nothing.
	dry, err := migration.Repair(ctx, dynamo.HeadsRepairOptions{DryRun: true, AllowLower: true})
	require.NoError(t, err)
	require.Len(t, dry.Repaired, 5)

	again, err := migration.Verify(ctx)
	require.NoError(t, err)
	require.Len(t, again.Findings, 7)

	// Repair raises, creates, and fixes the ttl, and leaves the heads that are too high alone.
	repaired, err := migration.Repair(ctx, dynamo.HeadsRepairOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	require.Len(t, repaired.Repaired, 4)
	require.Equal(t, map[string]evt.HeadFindingKind{
		"d-snapshot-ahead": evt.HeadHigher, "e-empty-snapshot": evt.HeadHigher, "no-such-stream": evt.HeadOrphan,
	}, kinds(repaired.Left))

	head, _, _ := storedHead(ctx, t, repo, "a-events-only")
	require.Equal(t, 3, head)

	head, _, _ = storedHead(ctx, t, repo, "c-compacted")
	require.Equal(t, 6, head)

	_, retainedTTL, _ := storedHead(ctx, t, repo, "f-retained")
	require.Equal(t, int(migrationNow+300), retainedTTL)

	_, ttl, present := storedHead(ctx, t, repo, "b-snapshot-only")
	require.True(t, present)
	require.Zero(t, ttl, "a head item with a ttl over items that never expire loses the ttl")

	head, _, _ = storedHead(ctx, t, repo, "d-snapshot-ahead")
	require.Equal(t, 12, head, "a high head is never lowered without -allow-lower")

	// --allow-lower lowers the high head to the derived head. A head over a snapshot of nothing has
	// no derived head to lower to (head items start at 1), so it is reported and left.
	lowered, err := migration.Repair(ctx, dynamo.HeadsRepairOptions{ConfirmWritersStopped: true, AllowLower: true})
	require.NoError(t, err)
	require.Len(t, lowered.Repaired, 1)
	require.Equal(t, map[string]evt.HeadFindingKind{
		"e-empty-snapshot": evt.HeadHigher, "no-such-stream": evt.HeadOrphan,
	}, kinds(lowered.Left))

	head, _, _ = storedHead(ctx, t, repo, "d-snapshot-ahead")
	require.Equal(t, 9, head)

	rawDeleteHead(ctx, t, repo, "e-empty-snapshot")
	rawDeleteHead(ctx, t, repo, "no-such-stream")
	rawDeleteHead(ctx, t, repo, "expiring-orphan")

	final, err := migration.Verify(ctx)
	require.NoError(t, err)
	require.True(t, final.OK(), "%v", final.Findings)
	require.Empty(t, final.Warnings)

	requireHeads(ctx, t, repo, streams)
}

func Test_MigrateHeads_RefusesToDeclareOverAnExistingMismatch(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)
	seedV08Store(ctx, t, repo)

	// A head item left by a development build that ran against the table: too low.
	rawSetHead(ctx, t, repo, "a-events-only", 1, 0)

	report, err := migrationFor(repo, nil).Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})

	var unverified *evt.HeadsNotVerifiedError
	require.ErrorAs(t, err, &unverified)
	require.ErrorIs(t, err, evt.ErrHeadsNotVerified)
	require.Equal(t, "verify", unverified.Phase)
	require.Equal(t, map[string]evt.HeadFindingKind{"a-events-only": evt.HeadLower}, kinds(unverified.Findings))
	require.False(t, report.Declared)
	requireDeclared(ctx, t, repo, false)

	// Backfill never lowers or raises an existing head item. The repair does, and the rerun finishes.
	_, err = migrationFor(repo, nil).Repair(ctx, dynamo.HeadsRepairOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)

	_, err = migrationFor(repo, nil).Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	requireDeclared(ctx, t, repo, true)
}

func Test_MigrateHeads_DeclareReverifiesAndRefusesAHeadlessStream(t *testing.T) {
	ctx := context.Background()

	t.Run("a stream appears before the declaration", func(t *testing.T) {
		repo, _ := newIsolatedEventLog(ctx, t)
		seedV08Store(ctx, t, repo)

		// A writer that was supposed to be stopped creates a stream after the first verification.
		report, err := migrationFor(repo, func(phase dynamo.HeadsMigrationPhase) {
			if phase == dynamo.PhaseVerifyBeforeDeclare {
				rawPut(ctx, t, repo, v08Event(t, "z-late", 1, 0))
			}
		}).Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})

		var unverified *evt.HeadsNotVerifiedError
		require.ErrorAs(t, err, &unverified)
		require.Equal(t, string(dynamo.PhaseVerifyBeforeDeclare), unverified.Phase)
		require.Equal(t, map[string]evt.HeadFindingKind{"z-late": evt.HeadMissing}, kinds(unverified.Findings))
		require.False(t, report.Declared)
		requireDeclared(ctx, t, repo, false)

		// A rerun backfills the late stream and declares.
		_, err = migrationFor(repo, nil).Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})
		require.NoError(t, err)

		head, _, present := storedHead(ctx, t, repo, "z-late")
		require.True(t, present)
		require.Equal(t, 1, head)
	})

	t.Run("a stray writer appends after the declaration", func(t *testing.T) {
		repo, _ := newIsolatedEventLog(ctx, t)
		seedV08Store(ctx, t, repo)

		report, err := migrationFor(repo, func(phase dynamo.HeadsMigrationPhase) {
			if phase == dynamo.PhaseVerifyAfterDeclare {
				rawPut(ctx, t, repo, v08Event(t, "a-events-only", 4, 0))
			}
		}).Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})

		var unverified *evt.HeadsNotVerifiedError
		require.ErrorAs(t, err, &unverified)
		require.Equal(t, string(dynamo.PhaseVerifyAfterDeclare), unverified.Phase)
		require.Equal(t, map[string]evt.HeadFindingKind{"a-events-only": evt.HeadLower}, kinds(unverified.Findings))
		require.True(t, report.Declared, "the declaration was made; the report says so")
		requireDeclared(ctx, t, repo, true)

		_, err = migrationFor(repo, nil).Repair(ctx, dynamo.HeadsRepairOptions{ConfirmWritersStopped: true})
		require.NoError(t, err)

		verified, err := migrationFor(repo, nil).Verify(ctx)
		require.NoError(t, err)
		require.True(t, verified.OK())
	})
}

func Test_MigrateHeads_AnEmptyTableMigrates(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)

	report, err := migrationFor(repo, nil).Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	require.True(t, report.Declared)
	require.Equal(t, 0, report.Backfill.Streams)
	requireDeclared(ctx, t, repo, true)
}

// A pre-v0.9 stream whose ID falls in a reserved namespace is hidden from head-aware code, so the
// migration refuses to backfill or declare around it until it is renamed.
func Test_MigrateHeads_RefusesAStreamInAReservedNamespace(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)
	streams := seedV08Store(ctx, t, repo)

	rawPut(ctx, t, repo, v08Event(t, "SYSTEM#account", 1, 0), v08Event(t, "SYSTEM#account", 2, 0))

	for _, dryRun := range []bool{true, false} {
		_, err := migrationFor(repo, nil).Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true, DryRun: dryRun})

		var reserved *evt.ReservedStreamsError
		require.ErrorAs(t, err, &reserved)
		require.Equal(t, []evt.EntityID{"SYSTEM#account"}, reserved.IDs)
	}

	for _, stream := range streams {
		_, _, present := storedHead(ctx, t, repo, stream.id)
		require.False(t, present, "nothing is written before the refusal: %s", stream.id)
	}

	requireDeclared(ctx, t, repo, false)

	verified, err := migrationFor(repo, nil).Verify(ctx)
	require.NoError(t, err)
	require.Equal(t, []evt.EntityID{"SYSTEM#account"}, verified.ReservedStreams)

	// A stream appearing between the verifications also stops the declaration.
	_, err = repoClient().DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(repo.EventsTable),
		Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "SYSTEM#account"}, "sk": number(1)},
	})
	require.NoError(t, err)

	_, err = repoClient().DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(repo.EventsTable),
		Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "SYSTEM#account"}, "sk": number(2)},
	})
	require.NoError(t, err)

	_, err = migrationFor(repo, func(phase dynamo.HeadsMigrationPhase) {
		if phase == dynamo.PhaseVerifyBeforeDeclare {
			rawPut(ctx, t, repo, v08Event(t, "SYSTEM#late", 1, 0))
		}
	}).Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})
	require.ErrorIs(t, err, evt.ErrReservedStreams)
	requireDeclared(ctx, t, repo, false)
}

// refreshingClient raises a head item's ttl without moving its head just before the repair's own
// write to it, as a head-aware writer's out-of-band snapshot write does.
type refreshingClient struct {
	dynamo.Client
	ctx   context.Context
	table string
	id    string
	ttl   int64
}

func (c *refreshingClient) UpdateItem(
	ctx context.Context,
	params *dynamodb.UpdateItemInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.UpdateItemOutput, error) {
	if params.Key["pk"].(*types.AttributeValueMemberS).Value == c.id { //nolint:errcheck // the key is the test's own
		_, err := c.PutItem(c.ctx, &dynamodb.PutItemInput{
			TableName: aws.String(c.table),
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: c.id}, "sk": number(-1), "head": number(3), "ttl": number(c.ttl),
			},
		})
		if err != nil {
			return nil, err
		}
	}

	return c.Client.UpdateItem(ctx, params, options...)
}

// A repair that raises a head ttl never overwrites a larger one a writer set in the meantime.
func Test_MigrateHeads_RepairNeverLowersAConcurrentlyRefreshedTTL(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)
	seedV08Store(ctx, t, repo)

	_, err := migrationFor(repo, nil).Migrate(ctx, dynamo.HeadsMigrateOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)

	// f-retained needs a head ttl of now+300; its head item has a smaller one.
	rawSetHead(ctx, t, repo, "f-retained", 3, migrationNow+10)

	refreshed := int64(migrationNow + 9000)
	racing := dynamo.NewRepository(&refreshingClient{
		Client: repoClient(), ctx: ctx, table: repo.EventsTable, id: "f-retained", ttl: refreshed,
	}, repo.EventsTable).HeadsMigration(dynamo.HeadsMigrationConfig{Concurrency: 1, Now: migrationClock})

	report, err := racing.Repair(ctx, dynamo.HeadsRepairOptions{ConfirmWritersStopped: true})
	require.NoError(t, err)
	require.Equal(t, 1, report.Raced, "the repair's write lost its condition on the ttl it had seen")
	require.Empty(t, report.Remaining, "the larger ttl satisfies the stream, so nothing is left to repair")

	head, ttl, present := storedHead(ctx, t, repo, "f-retained")
	require.True(t, present)
	require.Equal(t, 3, head)
	require.Equal(t, int(refreshed), ttl, "the writer's larger ttl survived the repair")
}
