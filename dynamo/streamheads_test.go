package dynamo_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/internal/compat"
	"github.com/photon-grove/evt/policy"
)

// headEvent is one event of a stream.
func headEvent(id string, sequence evt.EventSequence, entityType evt.EntityType) evt.SerializedEvent {
	return evt.SerializedEvent{
		ID: evt.GetEventID(evt.EntityID(id), sequence), Sequence: sequence, Type: "t:event", Version: 1,
		EntityID: evt.EntityID(id), EntityType: entityType, Payload: []byte(`{}`),
	}
}

func headEvents(id string, first, count int, entityType evt.EntityType) []evt.SerializedEvent {
	events := make([]evt.SerializedEvent, 0, count)
	for i := range count {
		events = append(events, headEvent(id, evt.EventSequence(first+i), entityType))
	}

	return events
}

func headSnapshot(id string, sequence, eventSequence int) evt.SerializedSnapshot {
	return evt.SerializedSnapshot{
		EntityType: "t", EntityID: evt.EntityID(id), Sequence: evt.EventSequence(sequence),
		EventSequence: evt.EventSequence(eventSequence), Payload: []byte(`{}`),
	}
}

func outcomeFor(name string) *evt.Outcome {
	return &evt.Outcome{CommandID: evt.CommandID(name), Status: evt.OutcomeAccepted, Digest: "digest"}
}

// headOf reads a stream's head item back from the fake table.
func headOf(fake *compat.FakeDynamo, id string) (head int, ttl int, present bool) {
	for _, item := range fake.Items(id) {
		sk, ok := item["sk"].(*types.AttributeValueMemberN)
		if !ok || sk.Value != "-1" {
			continue
		}

		if attr, isNumber := item["head"].(*types.AttributeValueMemberN); isNumber {
			head, _ = strconv.Atoi(attr.Value) //nolint:errcheck // the fake stores numbers
		}

		if attr, isNumber := item["ttl"].(*types.AttributeValueMemberN); isNumber {
			ttl, _ = strconv.Atoi(attr.Value) //nolint:errcheck // the fake stores numbers
		}

		return head, ttl, true
	}

	return 0, 0, false
}

// Every worked example of the ADR's action-count table sends exactly the number of actions the
// table gives, marker and outcome put counted.
func TestCommit_ActionCountsFollowTheWorkedExamples(t *testing.T) {
	ctx := context.Background()

	// seed commits streams so the examples can append to and fence them.
	seed := func(t *testing.T, repo *dynamo.Repository, streams ...string) {
		t.Helper()

		for _, id := range streams {
			require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents(id, 1, 3, "t")}))
		}
	}

	names := func(prefix string, count int) []string {
		out := make([]string, 0, count)
		for i := range count {
			out = append(out, fmt.Sprintf("%s-%02d", prefix, i))
		}

		return out
	}

	appends := func(ids []string) []evt.SerializedEvent {
		events := make([]evt.SerializedEvent, 0, len(ids))
		for _, id := range ids {
			events = append(events, headEvent(id, 4, "t"))
		}

		return events
	}

	fences := func(ids []string) []evt.ReadFence {
		out := make([]evt.ReadFence, 0, len(ids))
		for _, id := range ids {
			out = append(out, evt.ReadFence{EntityID: evt.EntityID(id), EntityType: "t", Head: 3})
		}

		return out
	}

	tests := []struct {
		name    string
		prepare func(t *testing.T, repo *dynamo.Repository, name string) evt.SerializedResult
		actions int
	}{
		{"create one stream, 1 event, no snapshot (+ outcome)", func(_ *testing.T, _ *dynamo.Repository, name string) evt.SerializedResult {
			return evt.SerializedResult{Events: headEvents(name, 1, 1, "t"), Outcome: outcomeFor(name)}
		}, 4},
		{"append 1 event, no snapshot (+ outcome)", func(t *testing.T, repo *dynamo.Repository, name string) evt.SerializedResult {
			seed(t, repo, name)

			return evt.SerializedResult{Events: headEvents(name, 4, 1, "t"), Outcome: outcomeFor(name)}
		}, 4},
		{"append 1 event that writes a snapshot (+ outcome)", func(t *testing.T, repo *dynamo.Repository, name string) evt.SerializedResult {
			seed(t, repo, name)

			return evt.SerializedResult{
				Events: headEvents(name, 4, 1, "t"), Snapshots: []evt.SerializedSnapshot{headSnapshot(name, 1, 4)}, Outcome: outcomeFor(name),
			}
		}, 5},
		{"create with 5 events and a snapshot (+ outcome)", func(_ *testing.T, _ *dynamo.Repository, name string) evt.SerializedResult {
			return evt.SerializedResult{
				Events: headEvents(name, 1, 5, "t"), Snapshots: []evt.SerializedSnapshot{headSnapshot(name, 1, 5)}, Outcome: outcomeFor(name),
			}
		}, 9},
		{"10 single-event appends + 5 fenced reads", func(t *testing.T, repo *dynamo.Repository, name string) evt.SerializedResult {
			written, read := names(name+"-w", 10), names(name+"-r", 5)
			seed(t, repo, append(slices.Clone(written), read...)...)

			return evt.SerializedResult{Events: appends(written), Fences: fences(read), Outcome: outcomeFor(name)}
		}, 27},
		{"12 single-event appends + 9 fenced reads", func(t *testing.T, repo *dynamo.Repository, name string) evt.SerializedResult {
			written, read := names(name+"-w", 12), names(name+"-r", 9)
			seed(t, repo, append(slices.Clone(written), read...)...)

			return evt.SerializedResult{Events: appends(written), Fences: fences(read), Outcome: outcomeFor(name)}
		}, 35},
		{"1 single-event append + 30 fenced reads", func(t *testing.T, repo *dynamo.Repository, name string) evt.SerializedResult {
			written, read := names(name+"-w", 1), names(name+"-r", 30)
			seed(t, repo, append(slices.Clone(written), read...)...)

			return evt.SerializedResult{Events: appends(written), Fences: fences(read), Outcome: outcomeFor(name)}
		}, 34},
		{"the most fenced streams beside one single-event append", func(t *testing.T, repo *dynamo.Repository, name string) evt.SerializedResult {
			written, read := names(name+"-w", 1), names(name+"-r", 96)
			seed(t, repo, append(slices.Clone(written), read...)...)

			return evt.SerializedResult{Events: appends(written), Fences: fences(read), Outcome: outcomeFor(name)}
		}, 100},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := compat.NewFakeDynamo()
			repo := dynamo.NewRepository(fake, "events")
			name := fmt.Sprintf("count-%d", time.Now().UnixNano())

			commit := test.prepare(t, repo, name)
			sent := len(fake.Transactions())

			require.NoError(t, repo.Commit(ctx, commit))
			require.Len(t, fake.Transactions(), sent+1)
			require.Len(t, fake.Transactions()[sent], test.actions)
		})
	}
}

// The single-stream capacity for event and projector operations (marker counted, outcome not): a
// stream pays one action beyond its events, plus one when it also writes a snapshot.
func TestCommit_SingleStreamCapacity(t *testing.T) {
	ctx := context.Background()

	for name, test := range map[string]struct {
		appends   bool
		snapshot  bool
		fits      int
		tooLarger int
	}{
		"create":               {fits: 98},
		"append":               {appends: true, fits: 98},
		"create with snapshot": {snapshot: true, fits: 97},
		"append with snapshot": {appends: true, snapshot: true, fits: 97},
	} {
		t.Run(name, func(t *testing.T) {
			fake := compat.NewFakeDynamo()
			repo := dynamo.NewRepository(fake, "events")

			first := 1
			if test.appends {
				require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("cap", 1, 1, "t")}))
				first = 2
			}

			build := func(count int, stream string) evt.SerializedResult {
				result := evt.SerializedResult{Events: headEvents(stream, first, count, "t")}
				if test.snapshot {
					result.Snapshots = []evt.SerializedSnapshot{headSnapshot(stream, 1, first+count-1)}
				}

				return result
			}

			var tooLarge *evt.CommitTooLargeError
			require.ErrorAs(t, repo.Commit(ctx, build(test.fits+1, "cap")), &tooLarge)
			require.Equal(t, 101, tooLarge.Items)

			require.NoError(t, repo.Commit(ctx, build(test.fits, "cap")))
		})
	}
}

// The budget's estimate follows the actions a commit sends: the sizes it computes are the sizes of
// the marker check, event puts, snapshot puts, head updates, and fence checks it builds.
func TestCommit_BudgetMatchesTheActionsSent(t *testing.T) {
	ctx := context.Background()

	for name, retention := range map[string]dynamo.Retention{
		"without retention": nil,
		"with retention":    {"t": time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			capture := &capturingClient{FakeDynamo: compat.NewFakeDynamo()}
			clock := time.Unix(1_791_763_200, 0)
			repo := dynamo.NewRepository(capture, "events").WithClock(func() time.Time { return clock })
			if retention != nil {
				repo = repo.WithRetention(retention)
			}

			require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("budget-a", 1, 3, "t")}))
			require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("budget-fenced", 1, 2, "plain")}))

			commit := evt.SerializedResult{
				Events: append(headEvents("budget-a", 4, 2, "t"), headEvents("budget-b", 1, 1, "t")...),
				Snapshots: []evt.SerializedSnapshot{
					headSnapshot("budget-a", 1, 5),
				},
				Fences: nil,
			}
			if retention == nil {
				commit.Fences = []evt.ReadFence{{EntityID: "budget-fenced", EntityType: "plain", Head: 2}}
			}

			capture.sent = nil
			require.NoError(t, repo.Commit(ctx, commit))
			require.Len(t, capture.sent, 1)

			ttlFor := func(entityType evt.EntityType) int64 {
				if retention[entityType] > 0 {
					return clock.Add(retention[entityType]).Unix()
				}

				return 0
			}

			estimate, err := evt.TransactionItemSizesWithTTL(commit, ttlFor)
			require.NoError(t, err)

			// The marker check is budgeted by its key alone (its first action, 20 bytes); every other
			// action by the full rule.
			require.Equal(t, 20, estimate[0])
			estimate = estimate[1:]

			actual := make([]int, 0, len(capture.sent[0]))
			for _, item := range capture.sent[0][1:] {
				actual = append(actual, dynamo.TransactItemSize(item))
			}

			slices.Sort(estimate)
			slices.Sort(actual)
			require.Equal(t, actual, estimate)
		})
	}
}

// capturingClient records the raw TransactWriteItems requests the repository sends.
type capturingClient struct {
	*compat.FakeDynamo

	sent [][]types.TransactWriteItem
}

func (c *capturingClient) TransactWriteItems(
	ctx context.Context,
	params *dynamodb.TransactWriteItemsInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.TransactWriteItemsOutput, error) {
	c.sent = append(c.sent, slices.Clone(params.TransactItems))

	return c.FakeDynamo.TransactWriteItems(ctx, params, options...)
}

// A head item is rebuilt from the log alone: it is the stream's highest event, whatever snapshots
// and compaction did.
func TestCommit_MaintainsTheHeadItem(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	repo := dynamo.NewRepository(fake, "events")

	_, _, present := headOf(fake, "maintained")
	require.False(t, present)

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("maintained", 1, 2, "t")}))
	head, _, present := headOf(fake, "maintained")
	require.True(t, present)
	require.Equal(t, 2, head)

	require.NoError(t, repo.CommitWithSnapshot(ctx,
		evt.SerializedResult{Events: headEvents("maintained", 3, 2, "t")}, "t", "maintained", []byte(`{}`), 1))
	head, _, _ = headOf(fake, "maintained")
	require.Equal(t, 4, head)

	_, err := repo.CompactBelow(ctx, "maintained", 4)
	require.NoError(t, err)
	head, _, _ = headOf(fake, "maintained")
	require.Equal(t, 4, head, "compaction never moves or deletes the head item")

	// A failed commit leaves it alone.
	require.Error(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("maintained", 4, 1, "t")}))
	head, _, _ = headOf(fake, "maintained")
	require.Equal(t, 4, head)
}

// A retained stream's head ttl is at least every item's and is never lowered: a writer with a
// shorter retention keeps the head's longer ttl, and a conflict on the ttl condition heals on retry.
func TestCommit_NeverLowersTheHeadTTL(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	now := time.Unix(1_791_763_200, 0)
	clock := func() time.Time { return now }

	long := dynamo.NewRepository(fake, "events").
		WithRetention(dynamo.Retention{"scratch": 2 * time.Hour}).WithClock(clock)
	short := dynamo.NewRepository(fake, "events").
		WithRetention(dynamo.Retention{"scratch": time.Minute}).WithClock(clock)

	require.NoError(t, long.Commit(ctx, evt.SerializedResult{Events: headEvents("ttl-1", 1, 1, "scratch")}))
	_, longTTL, _ := headOf(fake, "ttl-1")
	require.Equal(t, int(now.Add(2*time.Hour).Unix()), longTTL)

	// The short-retention writer loaded the stream, so it raises its own ttl to the head's.
	_, err := short.GetEvents(ctx, "ttl-1")
	require.NoError(t, err)
	require.NoError(t, short.Commit(ctx, evt.SerializedResult{Events: headEvents("ttl-1", 2, 1, "scratch")}))

	head, ttl, _ := headOf(fake, "ttl-1")
	require.Equal(t, 2, head)
	require.Equal(t, longTTL, ttl, "the head ttl never goes down")

	// An unseen writer: a writer that did not load the stream conflicts on the ttl condition (its
	// ttl is below the stored one) instead of lowering it, and the retry, which loads, passes.
	stranger := dynamo.NewRepository(fake, "events").
		WithRetention(dynamo.Retention{"scratch": time.Minute}).WithClock(clock)

	err = stranger.Commit(ctx, evt.SerializedResult{Events: headEvents("ttl-1", 3, 1, "scratch")})
	require.ErrorIs(t, err, dynamo.ErrSnapshotRaceCondition)
	require.True(t, evt.IsOptimisticConflict(err))
	_, ttl, _ = headOf(fake, "ttl-1")
	require.Equal(t, longTTL, ttl)

	_, err = stranger.GetEvents(ctx, "ttl-1")
	require.NoError(t, err)
	require.NoError(t, stranger.Commit(ctx, evt.SerializedResult{Events: headEvents("ttl-1", 3, 1, "scratch")}))
	head, ttl, _ = headOf(fake, "ttl-1")
	require.Equal(t, 3, head)
	require.Equal(t, longTTL, ttl)

	// A type without retention never gets a ttl.
	require.NoError(t, long.Commit(ctx, evt.SerializedResult{Events: headEvents("plain-1", 1, 1, "plain")}))
	_, ttl, present := headOf(fake, "plain-1")
	require.True(t, present)
	require.Zero(t, ttl)
}

// An out-of-band snapshot of a retained stream refreshes the head ttl in the same transaction, and
// only when it would raise it.
func TestPutSnapshot_RefreshesARetainedHeadTTL(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	now := time.Unix(1_791_763_200, 0)

	writer := dynamo.NewRepository(fake, "events").
		WithRetention(dynamo.Retention{"scratch": time.Hour}).WithClock(func() time.Time { return now })
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: headEvents("refresh", 1, 3, "scratch")}))
	_, firstTTL, _ := headOf(fake, "refresh")

	// Later, a snapshot is written: its ttl is later than the head's, so the head follows it.
	later := now.Add(30 * time.Minute)
	snapshotter := dynamo.NewRepository(fake, "events").
		WithRetention(dynamo.Retention{"scratch": time.Hour}).WithClock(func() time.Time { return later })
	require.NoError(t, snapshotter.PutSnapshot(ctx, "scratch", "refresh", []byte(`{}`), 1, 3))

	_, refreshed, _ := headOf(fake, "refresh")
	require.Equal(t, int(later.Add(time.Hour).Unix()), refreshed)
	require.Greater(t, refreshed, firstTTL)

	// A snapshot whose ttl is below the head's: only the ttl condition fails, so the write retries
	// as a plain check and succeeds, leaving the head ttl alone.
	earlier := dynamo.NewRepository(fake, "events").
		WithRetention(dynamo.Retention{"scratch": time.Minute}).WithClock(func() time.Time { return now })
	require.NoError(t, earlier.PutSnapshot(ctx, "scratch", "refresh", []byte(`{"v":2}`), 2, 3))

	_, unchanged, _ := headOf(fake, "refresh")
	require.Equal(t, refreshed, unchanged)

	snapshot, err := earlier.GetSnapshot(ctx, "refresh")
	require.NoError(t, err)
	require.Equal(t, evt.SerializedSnapshot{
		EntityType: "scratch", EntityID: "refresh", Sequence: 2, EventSequence: 3, Payload: []byte(`{"v":2}`),
	}, *snapshot)

	// A snapshot above the head is still drift, retained or not.
	err = earlier.PutSnapshot(ctx, "scratch", "refresh", []byte(`{}`), 3, 9)

	var drift *evt.StreamHeadDriftError
	require.ErrorAs(t, err, &drift)
	require.Equal(t, evt.EventSequence(3), drift.Stored)
	require.Equal(t, evt.EventSequence(9), drift.Derived)
}

// The snapshot's guard can fail because the head is still behind it. If a concurrent writer then
// advances the head, the read-back finds a head that covers the snapshot, but with a ttl below the
// snapshot's: the refresh is retried instead of dropped, so the snapshot never outlives the head item.
func TestPutSnapshot_RetriesTheTTLRefreshWhenTheHeadAdvancesUnderIt(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	now := time.Unix(1_791_763_200, 0)
	retention := dynamo.Retention{"scratch": time.Hour}

	writer := dynamo.NewRepository(fake, "events").WithRetention(retention).WithClock(func() time.Time { return now })
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: headEvents("lagging", 1, 2, "scratch")}))

	// The snapshot covers event 3 and is stamped half an hour later than the stream's items.
	later := now.Add(30 * time.Minute)
	client := &interleavingClient{FakeDynamo: fake}
	snapshotter := dynamo.NewRepository(client, "events").WithRetention(retention).WithClock(func() time.Time { return later })

	// The head (2) is behind the snapshot (3), so the guard fails; a rival then appends event 3,
	// stamped with the earlier ttl, before the guard's failure is read back.
	client.onHeadRead = func() {
		require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: headEvents("lagging", 3, 1, "scratch")}))
	}

	require.NoError(t, snapshotter.PutSnapshot(ctx, "scratch", "lagging", []byte(`{}`), 1, 3))

	head, ttl, present := headOf(fake, "lagging")
	require.True(t, present)
	require.Equal(t, 3, head)
	require.Equal(t, int(later.Add(time.Hour).Unix()), ttl, "the head item must outlive the snapshot it covers")

	snapshot, err := snapshotter.GetSnapshot(ctx, "lagging")
	require.NoError(t, err)
	require.Equal(t, evt.EventSequence(3), snapshot.EventSequence)
}

// A retained stream whose head item expired before its events is a transient conflict, not drift,
// while every remaining item has expired. A residual that has not expired is drift.
func TestCommit_ExpiredResidualOfARetainedStreamHeals(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	now := time.Unix(1_791_763_200, 0)
	retention := dynamo.Retention{"scratch": time.Hour}

	repo := dynamo.NewRepository(fake, "events").WithRetention(retention).WithClock(func() time.Time { return now })
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("residual", 1, 2, "scratch")}))

	deleteHead := func() {
		t.Helper()

		_, err := fake.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{{
			Delete: &types.Delete{
				TableName: aws.String("events"),
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "residual"},
					"sk": &types.AttributeValueMemberN{Value: "-1"},
				},
			},
		}}})
		require.NoError(t, err)
	}

	// TTL deleted the head item first. The events are still there and have not expired: re-creating
	// the stream finds an event above the (absent) head, which is drift.
	deleteHead()

	err := repo.Commit(ctx, evt.SerializedResult{Events: headEvents("residual", 1, 1, "scratch")})

	var drift *evt.StreamHeadDriftError
	require.ErrorAs(t, err, &drift)
	require.True(t, drift.HeadAbsent)

	// Once every remaining item has expired, the same failure is a conflict that heals when TTL
	// deletes them.
	expired := dynamo.NewRepository(fake, "events").WithRetention(retention).
		WithClock(func() time.Time { return now.Add(3 * time.Hour) })

	err = expired.Commit(ctx, evt.SerializedResult{Events: headEvents("residual", 1, 1, "scratch")})
	require.NotErrorAs(t, err, &drift)
	require.ErrorIs(t, err, dynamo.ErrSnapshotRaceCondition)
	require.True(t, evt.IsOptimisticConflict(err))
	require.True(t, policy.IsTransient(err))

	// A fenced append (not create) behaves the same way: an expired residual, no head item.
	err = expired.Commit(ctx, evt.SerializedResult{Events: headEvents("residual", 3, 1, "scratch")})
	require.NotErrorAs(t, err, &drift)
	require.True(t, evt.IsOptimisticConflict(err))
}

// interleavingClient runs a callback after the failure-path probe derived a stream's head and
// before it reads the head item, as a commit landing between the two reads.
type interleavingClient struct {
	*compat.FakeDynamo

	onHeadRead func()
}

func (c *interleavingClient) GetItem(
	ctx context.Context,
	params *dynamodb.GetItemInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.GetItemOutput, error) {
	if sk, ok := params.Key["sk"].(*types.AttributeValueMemberN); ok && sk.Value == "-1" && c.onHeadRead != nil {
		callback := c.onHeadRead
		c.onHeadRead = nil
		callback()
	}

	return c.FakeDynamo.GetItem(ctx, params, options...)
}

// A commit that lands between the probe's derivation and its head read raises the head above what
// the probe derived. Re-deriving finds the events, so it is an ordinary conflict and never drift.
func TestProbe_AConcurrentCommitIsAConflictNotDrift(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	client := &interleavingClient{FakeDynamo: fake}
	repo := dynamo.NewRepository(client, "events")
	rival := dynamo.NewRepository(fake, "events")

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("race", 1, 2, "t")}))

	// The writer is about to append 3, but the head is already 3 by the time it fails.
	require.NoError(t, rival.Commit(ctx, evt.SerializedResult{Events: headEvents("race", 3, 1, "t")}))

	client.onHeadRead = func() {
		require.NoError(t, rival.Commit(ctx, evt.SerializedResult{Events: headEvents("race", 4, 1, "t")}))
	}

	err := repo.Commit(ctx, evt.SerializedResult{Events: headEvents("race", 3, 1, "t")})
	require.True(t, evt.IsOptimisticConflict(err), "%v", err)
	require.NotErrorIs(t, err, evt.ErrStreamHeadDrift)
	require.NotErrorIs(t, err, evt.ErrSequenceGap)
}

// The deprecated batch path refuses outright.
func TestCommitStream_NeverWritesAnEvent(t *testing.T) {
	// Covered with the entry points in TestCommitStream_Refuses; this keeps the intent next to the
	// head tests: no event item can ever be written without its head update.
	fake := compat.NewFakeDynamo()
	repo := dynamo.NewRepository(fake, "events")

	require.NoError(t, repo.Commit(context.Background(), evt.SerializedResult{Events: headEvents("batch", 1, 1, "t")}))
	require.Len(t, fake.Items("batch"), 2, "the stream's head item and its event")
}

// Delete removes a stream's head item with its events, so the stream can be created again.
func TestDelete_RemovesTheHeadItemToo(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	repo := dynamo.NewRepository(fake, "events")

	events := headEvents("deleted", 1, 2, "t")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: events}))

	require.NoError(t, repo.Delete(ctx, events)) //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	require.Empty(t, fake.Items("deleted"))

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("deleted", 1, 1, "t")}))
}

// Scans ignore head items: an unfiltered scan decodes only events, and an orphan head item (no
// events, no snapshot) is not an entity.
func TestScans_IgnoreHeadItems(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	repo := dynamo.NewRepository(fake, "events")

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("scanned", 1, 2, "t")}))

	// An orphan head: the stream's events were deleted out of band.
	_, err := fake.PutItem(ctx, &dynamodb.PutItemInput{Item: map[string]types.AttributeValue{
		"pk":   &types.AttributeValueMemberS{Value: "orphan"},
		"sk":   &types.AttributeValueMemberN{Value: "-1"},
		"head": &types.AttributeValueMemberN{Value: "4"},
	}})
	require.NoError(t, err)

	var events []evt.SerializedEvent
	for batch := range repo.StreamAllEvents(ctx, evt.StreamFilter{}) {
		page, berr := batch.Unwrap()
		require.NoError(t, berr)
		events = append(events, page...)
	}

	require.Len(t, events, 2)
	for _, event := range events {
		require.Equal(t, evt.EntityID("scanned"), event.EntityID)
	}

	heads, err := repo.StreamEntityHeads(ctx, "")
	require.NoError(t, err)
	require.Equal(t, map[evt.EntityID]evt.EventSequence{"scanned": 2}, heads)
}

// A store with no marker and no data is declared by its first writer, in the ordinary declaration
// transaction, so a fresh table needs no step.
func TestCommit_AFreshStoreDeclaresStreamHeads(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	repo := dynamo.NewRepository(fake, "events")

	manifest, err := repo.FeatureMarker(nil).Manifest(ctx)
	require.NoError(t, err)
	require.Nil(t, manifest)

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("fresh", 1, 1, "t")}))

	manifest, err = repo.FeatureMarker(nil).Manifest(ctx)
	require.NoError(t, err)
	require.NotNil(t, manifest)
	require.Equal(t, int64(1), manifest.Version)
	require.Equal(t, map[string]evt.FeatureFormat{evt.FeatureStreamHeads: evt.StreamHeadsFormat}, manifest.Features)

	// Another process finds it declared and writes without declaring again.
	other := dynamo.NewRepository(fake, "events")
	require.NoError(t, other.Commit(ctx, evt.SerializedResult{Events: headEvents("fresh", 2, 1, "t")}))

	manifest, err = other.FeatureMarker(nil).Manifest(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), manifest.Version)
}

// declaringClient runs a callback right after the feature-marker declaration is written, as a
// writer that never reads the marker committing in the window around it.
type declaringClient struct {
	*compat.FakeDynamo

	afterDeclare func()
}

func (c *declaringClient) PutItem(
	ctx context.Context,
	params *dynamodb.PutItemInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.PutItemOutput, error) {
	out, err := c.FakeDynamo.PutItem(ctx, params, options...)
	if pk, ok := params.Item["pk"].(*types.AttributeValueMemberS); ok && err == nil &&
		pk.Value == "SYSTEM#FEATURES" && c.afterDeclare != nil {
		callback := c.afterDeclare
		c.afterDeclare = nil
		callback()
	}

	return out, err
}

func putRaw(t *testing.T, fake *compat.FakeDynamo, pk string, sk int, attrs map[string]types.AttributeValue) {
	t.Helper()

	item := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: pk},
		"sk": &types.AttributeValueMemberN{Value: strconv.Itoa(sk)},
	}
	maps.Copy(item, attrs)

	_, err := fake.PutItem(context.Background(), &dynamodb.PutItemInput{Item: item})
	require.NoError(t, err)
}

// The empty-store check and the declaration are two steps, so a writer that does not read the
// marker can commit a stream between them. The declaration is committed first and the store is
// looked at again afterward: that stream has no head item, so the declaration is retracted and
// the store is refused instead of being left declared over a stream it cannot guard.
func TestCommit_ARetractedDeclarationWhenAHeadlessStreamAppearsInTheWindow(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	client := &declaringClient{FakeDynamo: fake}
	repo := dynamo.NewRepository(client, "events")

	client.afterDeclare = func() {
		putRaw(t, fake, "early", 1, map[string]types.AttributeValue{
			"entityType": &types.AttributeValueMemberS{Value: "t"},
			"payload":    &types.AttributeValueMemberS{Value: "{}"},
		})
	}

	err := repo.Commit(ctx, evt.SerializedResult{Events: headEvents("late", 1, 1, "t")})
	require.ErrorIs(t, err, evt.ErrStreamHeadsNotMigrated)

	// Nothing of the refused commit was written, and the store still does not declare the feature.
	require.Empty(t, fake.Items("late"))

	manifest, err := repo.FeatureMarker(nil).Manifest(ctx)
	require.NoError(t, err)
	require.NotNil(t, manifest)
	require.NotContains(t, manifest.Features, evt.FeatureStreamHeads)

	// It stays refused: the store is populated and undeclared.
	err = dynamo.NewRepository(fake, "events").Commit(ctx, evt.SerializedResult{Events: headEvents("later", 1, 1, "t")})
	require.ErrorIs(t, err, evt.ErrStreamHeadsNotMigrated)
}

// Another head-aware writer's stream, created in the same window, is not a reason to retract.
func TestCommit_AHeadedStreamInTheWindowKeepsTheDeclaration(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	client := &declaringClient{FakeDynamo: fake}
	repo := dynamo.NewRepository(client, "events")

	client.afterDeclare = func() {
		putRaw(t, fake, "peer", -1, map[string]types.AttributeValue{"head": &types.AttributeValueMemberN{Value: "1"}})
		putRaw(t, fake, "peer", 1, map[string]types.AttributeValue{
			"entityType": &types.AttributeValueMemberS{Value: "t"},
			"payload":    &types.AttributeValueMemberS{Value: "{}"},
		})
	}

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("late", 1, 1, "t")}))

	manifest, err := repo.FeatureMarker(nil).Manifest(ctx)
	require.NoError(t, err)
	require.Contains(t, manifest.Features, evt.FeatureStreamHeads)
}

// Engine rows alone do not make a store populated: a table holding only command outcomes or a
// rebuild lock still counts as fresh.
func TestCommit_EngineRowsAloneLeaveAStoreFresh(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()

	for _, pk := range []string{"SYSTEM#REBUILD", "command-outcome#abc"} {
		_, err := fake.PutItem(ctx, &dynamodb.PutItemInput{Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
			"sk": &types.AttributeValueMemberN{Value: "1"},
		}})
		require.NoError(t, err)
	}

	repo := dynamo.NewRepository(fake, "events")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("engine", 1, 1, "t")}))
}

// A store that holds data written before stream heads, with no declaration, is refused by every
// write path and is never declared by accident; the migration's declaration lets it write.
func TestWrites_AnUnmigratedStoreIsRefused(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()

	// What a v0.8 writer left: an event and a snapshot, no head item, no marker.
	for _, item := range []map[string]types.AttributeValue{
		{
			"pk": &types.AttributeValueMemberS{Value: "legacy"}, "sk": &types.AttributeValueMemberN{Value: "1"},
			"entityType": &types.AttributeValueMemberS{Value: "t"}, "type": &types.AttributeValueMemberS{Value: "t:event"},
			"version": &types.AttributeValueMemberN{Value: "1"}, "payload": &types.AttributeValueMemberS{Value: "{}"},
			"metadata": &types.AttributeValueMemberS{Value: "{}"},
		},
		{
			"pk": &types.AttributeValueMemberS{Value: "legacy"}, "sk": &types.AttributeValueMemberN{Value: "0"},
			"entityType": &types.AttributeValueMemberS{Value: "t"}, "seq": &types.AttributeValueMemberN{Value: "1"},
			"eventSeq": &types.AttributeValueMemberN{Value: "1"}, "payload": &types.AttributeValueMemberS{Value: "{}"},
		},
	} {
		_, err := fake.PutItem(ctx, &dynamodb.PutItemInput{Item: item})
		require.NoError(t, err)
	}

	repo := dynamo.NewRepository(fake, "events")
	sent := len(fake.Transactions())

	requireRefused := func(err error, what string) {
		t.Helper()
		require.ErrorIs(t, err, evt.ErrStreamHeadsNotMigrated, what)
		require.False(t, evt.IsOptimisticConflict(err), what)
		require.False(t, policy.IsTransient(err), what)
	}

	requireRefused(repo.Commit(ctx, evt.SerializedResult{Events: headEvents("legacy", 2, 1, "t")}), "commit")
	requireRefused(repo.Commit(ctx, evt.SerializedResult{Events: headEvents("new", 1, 1, "t")}), "a new stream")
	requireRefused(repo.Commit(ctx, evt.SerializedResult{Outcome: outcomeFor("only")}), "an outcome-only commit")
	requireRefused(repo.PutSnapshot(ctx, "t", "legacy", []byte(`{}`), 2, 1), "put snapshot")

	_, err := repo.CompactBelow(ctx, "legacy", 1)
	requireRefused(err, "compaction")

	require.Len(t, fake.Transactions(), sent, "nothing was sent")

	manifest, err := repo.FeatureMarker(nil).Manifest(ctx)
	require.NoError(t, err)
	require.Nil(t, manifest, "a populated store is never declared by a writer")

	// Reads need no feature.
	events, err := repo.GetEvents(ctx, "legacy")
	require.NoError(t, err)
	require.Len(t, events, 1)

	// The migration backfills head items, then declares: Validate no longer refuses either.
	require.Error(t, repo.FeatureMarker(nil).Validate(ctx))

	_, err = fake.PutItem(ctx, &dynamodb.PutItemInput{Item: map[string]types.AttributeValue{
		"pk":   &types.AttributeValueMemberS{Value: "legacy"},
		"sk":   &types.AttributeValueMemberN{Value: "-1"},
		"head": &types.AttributeValueMemberN{Value: "1"},
	}})
	require.NoError(t, err)

	declared, err := repo.FeatureMarker(nil).DeclareStreamHeads(ctx)
	require.NoError(t, err)
	require.True(t, declared.DeclaresStreamHeads())

	require.NoError(t, repo.FeatureMarker(nil).Validate(ctx))
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("legacy", 2, 1, "t")}))
}

// A negative sequence reads as 0, so it never reaches the head item.
func TestGetLatestEvents_ClampsANegativeSequence(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	repo := dynamo.NewRepository(fake, "events")

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("clamped", 1, 2, "t")}))

	events, err := repo.GetLatestEvents(ctx, "clamped", -5)
	require.NoError(t, err)
	require.Len(t, events, 2)
}

// failingHeadReadClient fails the probe's read of a head item.
type failingHeadReadClient struct {
	*compat.FakeDynamo

	err error
}

func (c *failingHeadReadClient) GetItem(
	ctx context.Context,
	params *dynamodb.GetItemInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.GetItemOutput, error) {
	if sk, ok := params.Key["sk"].(*types.AttributeValueMemberN); ok && sk.Value == "-1" && c.err != nil {
		return nil, c.err
	}

	return c.FakeDynamo.GetItem(ctx, params, options...)
}

// A probe whose own read fails says nothing about the stream: the read's error is returned, never
// a conflict or drift the probe did not establish.
func TestProbe_AFailedReadIsReturnedAsItself(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	client := &failingHeadReadClient{FakeDynamo: fake}
	repo := dynamo.NewRepository(client, "events")

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("read-fails", 1, 2, "t")}))
	require.NoError(t, dynamo.NewRepository(fake, "events").Commit(ctx,
		evt.SerializedResult{Events: headEvents("read-fails", 3, 1, "t")}))

	client.err = errors.New("head read failed")

	err := repo.Commit(ctx, evt.SerializedResult{Events: headEvents("read-fails", 3, 1, "t")})
	require.ErrorContains(t, err, "head read failed")
	require.False(t, evt.IsOptimisticConflict(err))
	require.NotErrorIs(t, err, evt.ErrStreamHeadDrift)
}

// A head above the derived head is drift for a type without retention (events are missing below
// it), and the expired-tail transient for a type with retention: an ordinary conflict.
func TestProbe_AHeadAboveTheLog(t *testing.T) {
	ctx := context.Background()

	for name, test := range map[string]struct {
		retention dynamo.Retention
		drift     bool
	}{
		"without retention": {drift: true},
		"with retention":    {retention: dynamo.Retention{"t": time.Hour}},
	} {
		t.Run(name, func(t *testing.T) {
			fake := compat.NewFakeDynamo()
			repo := dynamo.NewRepository(fake, "events")
			if test.retention != nil {
				repo = repo.WithRetention(test.retention)
			}

			require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("above", 1, 3, "t")}))

			// The head item says 5 while the log only reaches 3.
			_, err := fake.PutItem(ctx, &dynamodb.PutItemInput{Item: map[string]types.AttributeValue{
				"pk":   &types.AttributeValueMemberS{Value: "above"},
				"sk":   &types.AttributeValueMemberN{Value: "-1"},
				"head": &types.AttributeValueMemberN{Value: "5"},
			}})
			require.NoError(t, err)

			err = repo.Commit(ctx, evt.SerializedResult{Events: headEvents("above", 4, 1, "t")})
			if test.drift {
				var drift *evt.StreamHeadDriftError
				require.ErrorAs(t, err, &drift)
				require.Equal(t, evt.EventSequence(5), drift.Stored)
				require.Equal(t, evt.EventSequence(3), drift.Derived)

				return
			}

			require.True(t, evt.IsOptimisticConflict(err), "%v", err)
			require.NotErrorIs(t, err, evt.ErrStreamHeadDrift)
		})
	}
}
