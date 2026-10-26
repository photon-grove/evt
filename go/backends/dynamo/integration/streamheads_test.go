//go:build integration

package integration

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/dynamo"
	"github.com/photon-grove/evt/go/policy"
)

func scratchEvents(id string, first, count int) []evt.SerializedEvent {
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

// storedHead reads a stream's head item straight from the table.
func storedHead(ctx context.Context, t *testing.T, repo *dynamo.Repository, id string) (head, ttl int, present bool) {
	t.Helper()

	out, err := repoClient().GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(repo.EventsTable),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: id},
			"sk": &types.AttributeValueMemberN{Value: "-1"},
		},
	})
	require.NoError(t, err)

	if len(out.Item) == 0 {
		return 0, 0, false
	}

	if attr, ok := out.Item["head"].(*types.AttributeValueMemberN); ok {
		head, err = strconv.Atoi(attr.Value)
		require.NoError(t, err)
	}

	if attr, ok := out.Item["ttl"].(*types.AttributeValueMemberN); ok {
		ttl, err = strconv.Atoi(attr.Value)
		require.NoError(t, err)
	}

	return head, ttl, true
}

// A retained stream's head ttl never goes down, against the real expression evaluator: a writer
// with a shorter retention that loaded the stream keeps the head's longer ttl, and one that did not
// load it conflicts on the ttl condition and passes on the retry.
func Test_StreamHeads_RetainedTTLNeverLowers(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)
	now := time.Unix(1_791_763_200, 0)
	clock := func() time.Time { return now }

	long := repo.WithRetention(dynamo.Retention{"scratch": 2 * time.Hour}).WithClock(clock)
	short := repo.WithRetention(dynamo.Retention{"scratch": time.Minute}).WithClock(clock)

	require.NoError(t, long.Commit(ctx, evt.SerializedResult{Events: scratchEvents("ttl-1", 1, 1)}))
	_, longTTL, present := storedHead(ctx, t, repo, "ttl-1")
	require.True(t, present)
	require.Equal(t, int(now.Add(2*time.Hour).Unix()), longTTL)

	_, err := short.GetEvents(ctx, "ttl-1")
	require.NoError(t, err)
	require.NoError(t, short.Commit(ctx, evt.SerializedResult{Events: scratchEvents("ttl-1", 2, 1)}))

	head, ttl, _ := storedHead(ctx, t, repo, "ttl-1")
	require.Equal(t, 2, head)
	require.Equal(t, longTTL, ttl)

	// A separate repository has seen nothing of the stream (the ttl memory is per repository).
	stranger := dynamo.NewRepository(repoClient(), repo.EventsTable).WithLazyInitialization(true).
		WithRetention(dynamo.Retention{"scratch": time.Minute}).WithClock(clock)
	err = stranger.Commit(ctx, evt.SerializedResult{Events: scratchEvents("ttl-1", 3, 1)})
	require.True(t, evt.IsOptimisticConflict(err), "%v", err)
	require.NotErrorIs(t, err, evt.ErrStreamHeadDrift)

	_, err = stranger.GetEvents(ctx, "ttl-1")
	require.NoError(t, err)
	require.NoError(t, stranger.Commit(ctx, evt.SerializedResult{Events: scratchEvents("ttl-1", 3, 1)}))

	head, ttl, _ = storedHead(ctx, t, repo, "ttl-1")
	require.Equal(t, 3, head)
	require.Equal(t, longTTL, ttl)
}

// An out-of-band snapshot of a retained stream refreshes the head ttl in its transaction, and a
// snapshot whose ttl is below the head's retries as a plain check.
func Test_StreamHeads_PutSnapshotRefreshesARetainedHead(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)
	now := time.Unix(1_791_763_200, 0)

	writer := repo.WithRetention(dynamo.Retention{"scratch": time.Hour}).WithClock(func() time.Time { return now })
	require.NoError(t, writer.Commit(ctx, evt.SerializedResult{Events: scratchEvents("refresh", 1, 3)}))

	later := now.Add(30 * time.Minute)
	snapshotter := repo.WithRetention(dynamo.Retention{"scratch": time.Hour}).WithClock(func() time.Time { return later })
	require.NoError(t, snapshotter.PutSnapshot(ctx, "scratch", "refresh", []byte(`{}`), 1, 3))

	_, refreshed, _ := storedHead(ctx, t, repo, "refresh")
	require.Equal(t, int(later.Add(time.Hour).Unix()), refreshed)

	earlier := repo.WithRetention(dynamo.Retention{"scratch": time.Minute}).WithClock(func() time.Time { return now })
	require.NoError(t, earlier.PutSnapshot(ctx, "scratch", "refresh", []byte(`{"v":2}`), 2, 3))

	_, unchanged, _ := storedHead(ctx, t, repo, "refresh")
	require.Equal(t, refreshed, unchanged)

	snapshot, err := earlier.GetSnapshot(ctx, "refresh")
	require.NoError(t, err)
	require.Equal(t, evt.EventSequence(2), snapshot.Sequence)

	var drift *evt.StreamHeadDriftError
	require.ErrorAs(t, earlier.PutSnapshot(ctx, "scratch", "refresh", []byte(`{}`), 3, 9), &drift)
}

// TTL can delete the head item of a retained stream before its events. While every remaining item
// has expired, the failure is a conflict that heals; a residual that has not expired is drift.
func Test_StreamHeads_ExpiredResidualOfARetainedStream(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)
	now := time.Unix(1_791_763_200, 0)
	retention := dynamo.Retention{"scratch": time.Hour}

	live := repo.WithRetention(retention).WithClock(func() time.Time { return now })
	require.NoError(t, live.Commit(ctx, evt.SerializedResult{Events: scratchEvents("residual", 1, 2)}))

	// TTL deleted the head item first.
	_, err := repoClient().DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(repo.EventsTable),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "residual"},
			"sk": &types.AttributeValueMemberN{Value: "-1"},
		},
	})
	require.NoError(t, err)

	var drift *evt.StreamHeadDriftError
	require.ErrorAs(t, live.Commit(ctx, evt.SerializedResult{Events: scratchEvents("residual", 1, 1)}), &drift,
		"events that have not expired sit above an absent head")
	require.True(t, drift.HeadAbsent)

	expired := repo.WithRetention(retention).WithClock(func() time.Time { return now.Add(3 * time.Hour) })
	err = expired.Commit(ctx, evt.SerializedResult{Events: scratchEvents("residual", 1, 1)})
	require.NotErrorAs(t, err, &drift)
	require.True(t, evt.IsOptimisticConflict(err), "%v", err)
	require.True(t, policy.IsTransient(err))

	err = expired.Commit(ctx, evt.SerializedResult{Events: scratchEvents("residual", 3, 1)})
	require.NotErrorAs(t, err, &drift)
	require.True(t, evt.IsOptimisticConflict(err), "%v", err)
}

// A stale append is a conflict, never drift: the head moved because another writer committed. (The
// probe's interleaving with a commit is covered against a scripted client in the dynamo package.)
func Test_StreamHeads_AStaleAppendIsAConflictNotDrift(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: scratchEvents("race", 1, 2)}))

	// The writer expects the head at 2 but another commit moved it to 3 before the writer's commit.
	rival := repo.WithClock(time.Now)
	require.NoError(t, rival.Commit(ctx, evt.SerializedResult{Events: scratchEvents("race", 3, 1)}))

	err := repo.Commit(ctx, evt.SerializedResult{Events: scratchEvents("race", 3, 1)})
	require.True(t, evt.IsOptimisticConflict(err), "%v", err)
	require.NotErrorIs(t, err, evt.ErrStreamHeadDrift)
}
