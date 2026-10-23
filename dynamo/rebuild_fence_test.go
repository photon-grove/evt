package dynamo_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

var rebuildNow = time.Unix(1_790_000_000, 0)

func newFenceRepo(client dynamo.Client) *dynamo.Repository {
	return dynamo.NewRepository(client, "events").WithLazyInitialization(true).WithViewsTable("views").WithClock(func() time.Time { return rebuildNow })
}

// countingClient counts the reads and transactions that reach the table.
type countingClient struct {
	*compat.FakeDynamo

	gets         atomic.Int32
	queries      atomic.Int32
	transactions atomic.Int32
}

func (c *countingClient) GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	c.gets.Add(1)

	return c.FakeDynamo.GetItem(ctx, in, opts...)
}

func (c *countingClient) Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	c.queries.Add(1)

	return c.FakeDynamo.Query(ctx, in, opts...)
}

func (c *countingClient) TransactWriteItems(
	ctx context.Context,
	in *dynamodb.TransactWriteItemsInput,
	opts ...func(*dynamodb.Options),
) (*dynamodb.TransactWriteItemsOutput, error) {
	c.transactions.Add(1)

	return c.FakeDynamo.TransactWriteItems(ctx, in, opts...)
}

func oneEvent(t *testing.T, id evt.EntityID, sequence evt.EventSequence) []evt.SerializedEvent {
	t.Helper()

	events, err := evt.SerializeEvents([]evt.Event{&rebuildTestEvent{id: id}}, sequence-1, id, evt.Metadata{})
	require.NoError(t, err)

	for i := range events {
		events[i].EntityType = "t"
	}

	return events
}

type rebuildTestEvent struct{ id evt.EntityID }

func (*rebuildTestEvent) Type() evt.EventType        { return "t:happened" }
func (*rebuildTestEvent) Version() evt.EventVersion  { return 1 }
func (*rebuildTestEvent) EntityType() evt.EntityType { return "t" }
func (e *rebuildTestEvent) EntityID() evt.EntityID   { return e.id }

// Every commit starts with the marker check and then the rebuild-lock check, a bare
// attribute_not_exists(pk) condition on the lock row that carries no names or values.
func TestCommit_StartsWithTheRebuildLockCheck(t *testing.T) {
	fake := compat.NewFakeDynamo()
	repo := newFenceRepo(fake)

	require.NoError(t, repo.Commit(context.Background(), evt.SerializedResult{Events: oneEvent(t, "a", 1)}))

	calls := fake.Transactions()
	require.Len(t, calls, 1)

	marker, lock := calls[0][0], calls[0][1]
	require.Equal(t, "SYSTEM#FEATURES", marker.PK)
	require.Equal(t, compat.TransactionAction{
		Operation: "ConditionCheck", PK: "SYSTEM#REBUILD", SK: 0, Condition: "attribute_not_exists(pk)",
	}, lock)
}

// The marker check outranks the lock check: a writer whose cached manifest version went stale
// re-validates first, and only the retry meets the lock.
func TestCommit_MarkerFailureOutranksTheRebuildLock(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	repo := newFenceRepo(fake)

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: oneEvent(t, "a", 1)}))

	// Another writer declares a feature this writer models, bumping the manifest version, and a
	// rebuild takes the lock.
	_, err := dynamo.NewRepository(fake, "events").WithLazyInitialization(true).FeatureMarker(nil).Declare(ctx, func(manifest *evt.FeatureManifest) {
		manifest.Retention = append(manifest.Retention, "other")
	})
	require.NoError(t, err)

	_, err = repo.RebuildFence().Begin(ctx, "job", time.Minute)
	require.NoError(t, err)

	err = repo.Commit(ctx, evt.SerializedResult{Events: oneEvent(t, "a", 2)})
	require.ErrorIs(t, err, evt.ErrFeatureManifestChanged)
	require.NotErrorIs(t, err, evt.ErrRebuildInProgress)
	require.True(t, policy.IsTransient(err))

	err = repo.Commit(ctx, evt.SerializedResult{Events: oneEvent(t, "a", 2)})
	require.ErrorIs(t, err, evt.ErrRebuildInProgress)
}

// A refused commit is classified from the cancellation reasons alone: the failure-path probe that
// tells a head conflict from drift never runs when the lock check failed.
func TestCommit_RebuildLockFailureRunsNoProbe(t *testing.T) {
	ctx := context.Background()
	client := &countingClient{FakeDynamo: compat.NewFakeDynamo()}
	repo := newFenceRepo(client)

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: oneEvent(t, "a", 1)}))

	lease, err := repo.RebuildFence().Begin(ctx, "job", time.Minute)
	require.NoError(t, err)

	reads := client.gets.Load() + client.queries.Load()

	// Sequence 1 again: the head update fails as well as the lock check.
	err = repo.Commit(ctx, evt.SerializedResult{Events: oneEvent(t, "a", 1)})
	require.ErrorIs(t, err, evt.ErrRebuildInProgress)
	require.False(t, evt.IsOptimisticConflict(err))
	require.Equal(t, reads, client.gets.Load()+client.queries.Load(), "no probe reads the stream")

	// With the lock gone the same commit is the conflict it always was.
	_, err = repo.RebuildFence().End(ctx, lease)
	require.NoError(t, err)

	err = repo.Commit(ctx, evt.SerializedResult{Events: oneEvent(t, "a", 1)})
	require.Error(t, err)
	require.NotErrorIs(t, err, evt.ErrRebuildInProgress)
	require.True(t, evt.IsOptimisticConflict(err))
}

func TestRebuildFence_ExactlyOneConcurrentBeginWins(t *testing.T) {
	ctx := context.Background()
	repo := newFenceRepo(compat.NewFakeDynamo())

	var (
		wg     sync.WaitGroup
		wins   atomic.Int32
		losers atomic.Int32
	)

	for i := range 12 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, err := repo.RebuildFence().Begin(ctx, fmt.Sprintf("worker-%d", i), time.Minute)

			switch {
			case err == nil:
				wins.Add(1)
			case evt.IsRebuildLockHeldErr(err):
				losers.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	wg.Wait()

	require.EqualValues(t, 1, wins.Load())
	require.EqualValues(t, 11, losers.Load())

	generation, err := repo.RebuildFence().Generation(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, generation.Value)
}

// alwaysCancelled cancels every fence transaction on a failed condition, as if another operation
// kept winning the race.
type alwaysCancelled struct {
	*compat.FakeDynamo

	sent atomic.Int32
}

func (c *alwaysCancelled) TransactWriteItems(
	_ context.Context,
	_ *dynamodb.TransactWriteItemsInput,
	_ ...func(*dynamodb.Options),
) (*dynamodb.TransactWriteItemsOutput, error) {
	c.sent.Add(1)

	return nil, &types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{
		{Code: aws.String("ConditionalCheckFailed")}, {Code: aws.String("None")},
	}}
}

func TestRebuildFence_BeginGivesUpAfterEightLostRaces(t *testing.T) {
	client := &alwaysCancelled{FakeDynamo: compat.NewFakeDynamo()}
	repo := newFenceRepo(client)

	_, err := repo.RebuildFence().Begin(context.Background(), "job", time.Minute)

	var conflict *evt.RebuildConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, evt.MaxRebuildBeginAttempts, conflict.Attempts)
	require.EqualValues(t, evt.MaxRebuildBeginAttempts, client.sent.Load())
	require.True(t, policy.IsTransient(err))
}

func TestRebuildFence_ReadsAreAlwaysConsistentAndRefuseCorruptItems(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()

	var consistent []bool

	repo := newFenceRepo(&readRecorder{FakeDynamo: fake, consistent: &consistent}).WithConsistentRead(false)

	_, err := repo.RebuildFence().State(ctx)
	require.NoError(t, err)
	_, err = repo.RebuildFence().Generation(ctx)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true}, consistent, "whatever the repository's read-consistency setting")

	for name, item := range map[string]map[string]types.AttributeValue{
		"a generation with no value": {
			"pk": &types.AttributeValueMemberS{Value: evt.RebuildGenerationID}, "sk": &types.AttributeValueMemberN{Value: "0"},
		},
		"a force-release flag that is not a boolean": {
			"pk": &types.AttributeValueMemberS{Value: evt.RebuildGenerationID}, "sk": &types.AttributeValueMemberN{Value: "0"},
			"value": &types.AttributeValueMemberN{Value: "3"}, "forceReleased": &types.AttributeValueMemberS{Value: "yes"},
		},
	} {
		corrupt := compat.NewFakeDynamo()
		corrupt.SeedItem(item)

		_, err = newFenceRepo(corrupt).RebuildFence().Generation(ctx)
		require.Error(t, err, name)
	}

	broken := compat.NewFakeDynamo()
	broken.SeedItem(map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: evt.RebuildLockID}, "sk": &types.AttributeValueMemberN{Value: "0"},
		"owner": &types.AttributeValueMemberS{Value: "job"},
	})

	_, err = newFenceRepo(broken).RebuildFence().State(ctx)
	require.Error(t, err, "a lock with no lease")

	for name, flag := range map[string]types.AttributeValue{
		"a force-released lock item flagged false":    &types.AttributeValueMemberBOOL{Value: false},
		"a force-released lock item with a text flag": &types.AttributeValueMemberS{Value: "true"},
	} {
		flagged := compat.NewFakeDynamo()
		flagged.SeedItem(map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: evt.RebuildLockID}, "sk": &types.AttributeValueMemberN{Value: "0"},
			"forceReleased": flag,
		})

		_, err = newFenceRepo(flagged).RebuildFence().State(ctx)
		require.Error(t, err, name)
	}
}

// A force-release leaves a lock item in place of the lock, so the commit's lock check keeps
// failing at no extra action, while State reports no lock and Begin acquires from it.
func TestRebuildFence_ForceReleaseKeepsCommitsRefused(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	repo := newFenceRepo(fake)
	fence := repo.RebuildFence()

	lease, err := fence.Begin(ctx, "job", time.Hour)
	require.NoError(t, err)

	_, err = fence.ForceRelease(ctx, "job")
	require.NoError(t, err)

	state, err := fence.State(ctx)
	require.NoError(t, err)
	require.Nil(t, state.Lock)
	require.Equal(t, evt.RebuildGeneration{Value: lease.Generation + 2, ForceReleased: true}, state.Generation)

	items := fake.Items(evt.RebuildLockID)
	require.Len(t, items, 1, "the force-released lock item stays")
	require.Equal(t, &types.AttributeValueMemberBOOL{Value: true}, items[0]["forceReleased"])
	require.NotContains(t, items[0], "owner")

	err = repo.Commit(ctx, evt.SerializedResult{Events: oneEvent(t, "a", 1)})
	require.ErrorIs(t, err, evt.ErrRebuildInProgress)

	calls := fake.Transactions()
	refused := calls[len(calls)-1]
	require.Len(t, refused, 4, "marker check, lock check, head update, event put: no extra action")
	require.Equal(t, "SYSTEM#REBUILD", refused[1].PK)
	require.Equal(t, "attribute_not_exists(pk)", refused[1].Condition)

	recovery, err := fence.Begin(ctx, "recovery", time.Hour)
	require.NoError(t, err)

	_, err = fence.End(ctx, recovery)
	require.NoError(t, err)
	require.Empty(t, fake.Items(evt.RebuildLockID))
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: oneEvent(t, "a", 1)}))
}

// readRecorder records whether each read asked for consistency.
type readRecorder struct {
	*compat.FakeDynamo

	consistent *[]bool
}

func (r *readRecorder) GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	*r.consistent = append(*r.consistent, aws.ToBool(in.ConsistentRead))

	return r.FakeDynamo.GetItem(ctx, in, opts...)
}

// A guarded batch needs a views table, fails the guard before the views, and names the view whose
// own condition failed.
func TestRebuildFence_GuardedViewWritesFailInOrder(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	client := &countingClient{FakeDynamo: fake}

	put := func(sk string, owner evt.EntityID) evt.ProjectedView {
		return evt.ProjectedView{Projector: "rebuild", Op: evt.PutView("rebuilt", sk, owner, "t", []byte(`{}`))}
	}

	// No views table: refused before any request.
	bare := dynamo.NewRepository(client, "events").WithLazyInitialization(true).WithClock(func() time.Time { return rebuildNow })

	lease, err := bare.RebuildFence().Begin(ctx, "job", time.Minute)
	require.NoError(t, err)

	sent := client.transactions.Load()

	var unconfigured *evt.ViewsNotConfiguredError
	require.ErrorAs(t, bare.RebuildFence().WriteViews(ctx, lease, []evt.ProjectedView{put("a", "o")}), &unconfigured)
	require.Equal(t, sent, client.transactions.Load())

	repo := newFenceRepo(client)
	fence := repo.RebuildFence()

	// The guard is the last action of the one transaction.
	require.NoError(t, fence.WriteViews(ctx, lease, []evt.ProjectedView{put("a", "owner-a"), put("b", "owner-a")}))

	calls := fake.Transactions()
	last := calls[len(calls)-1]
	require.Len(t, last, 3)
	require.Equal(t, compat.TransactionAction{
		Operation: "ConditionCheck", PK: "SYSTEM#REBUILD", SK: 0,
		Condition: "#owner = :me AND #generation = :generation AND leaseExpiresAt > :now",
		Names:     map[string]string{"#owner": "owner", "#generation": "generation"},
		Values: map[string]any{
			":me":         map[string]any{"S": "job"},
			":generation": map[string]any{"N": "1"},
			":now":        map[string]any{"N": "1790000000"},
		},
	}, last[2])

	// A view whose own condition fails names its projector and operation, and writes nothing.
	err = fence.WriteViews(ctx, lease, []evt.ProjectedView{
		put("c", "owner-c"),
		{Projector: "rebuild", Operation: 1, Op: evt.PutView("rebuilt", "a", "owner-z", "t", []byte(`{}`))},
	})

	var failed *evt.ProjectorOperationError
	require.ErrorAs(t, err, &failed)
	require.Equal(t, 1, failed.Operation)
	require.False(t, errors.Is(err, evt.ErrRebuildInProgress))
	require.Len(t, fake.Views("rebuilt"), 2, "the first operation of the failed batch was not written")

	// When the guard fails as well, the lost lease wins: the worker must stop whatever else failed.
	_, err = fence.ForceRelease(ctx, "job")
	require.NoError(t, err)

	err = fence.WriteViews(ctx, lease, []evt.ProjectedView{
		{Projector: "rebuild", Op: evt.PutView("rebuilt", "a", "owner-z", "t", []byte(`{}`))},
	})
	require.True(t, evt.IsRebuildLeaseLostErr(err))
}

// The out-of-band snapshot write, compaction, and a rebuild's own lifecycle carry no lock check.
func TestRebuildLockOnlyGatesCommits(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	repo := newFenceRepo(fake)

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: oneEvent(t, "a", 1)}))

	_, err := repo.RebuildFence().Begin(ctx, "job", time.Hour)
	require.NoError(t, err)

	require.NoError(t, repo.PutSnapshot(ctx, "t", "a", []byte(`{}`), 1, 1), "an out-of-band snapshot is not a commit")

	// No transaction a fence-free path sent names the lock row, except the commit and the lifecycle.
	locks := 0

	for _, call := range fake.Transactions() {
		for _, action := range call {
			if action.PK == "SYSTEM#REBUILD" && action.Operation == "ConditionCheck" {
				locks++
			}
		}
	}

	require.Equal(t, 1, locks, "only the one commit carried the lock check")

	// Enumeration skips the fence's rows.
	var seen []string

	for batch := range repo.StreamAllEvents(ctx, evt.StreamFilter{}) {
		events, berr := batch.Unwrap()
		require.NoError(t, berr)

		for _, event := range events {
			seen = append(seen, string(event.EntityID))
		}
	}

	require.Equal(t, []string{"a"}, seen)
}

// tornReads answers the first read of the generation item as it was before a concurrent Begin
// landed, so the read of the lock row that follows shows a lock beside an even generation.
type tornReads struct {
	*compat.FakeDynamo

	torn atomic.Bool
}

func (c *tornReads) GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if pk, ok := in.Key["pk"].(*types.AttributeValueMemberS); ok && pk.Value == evt.RebuildGenerationID && !c.torn.Swap(true) {
		return &dynamodb.GetItemOutput{}, nil
	}

	return c.FakeDynamo.GetItem(ctx, in, opts...)
}

// The generation and the lock row are read one after the other, so a Begin that races another's
// commit can see a lock with no generation to match. That is a torn read, not a corrupt store: it
// reads again and reports the lock as held.
func TestRebuildFence_BeginReadsAgainAfterATornRead(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()

	_, err := newFenceRepo(fake).RebuildFence().Begin(ctx, "winner", time.Minute)
	require.NoError(t, err)

	_, err = newFenceRepo(&tornReads{FakeDynamo: fake}).RebuildFence().Begin(ctx, "loser", time.Minute)

	var held *evt.RebuildLockHeldError
	require.ErrorAs(t, err, &held)
	require.Equal(t, "winner", held.Owner)
}

// A state that persists across every read is reported, not repaired.
func TestRebuildFence_BeginReportsAStateNoTransactionProduces(t *testing.T) {
	fake := compat.NewFakeDynamo()
	fake.SeedItem(map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: evt.RebuildLockID}, "sk": &types.AttributeValueMemberN{Value: "0"},
		"owner": &types.AttributeValueMemberS{Value: "job"}, "generation": &types.AttributeValueMemberN{Value: "3"},
		"leaseExpiresAt": &types.AttributeValueMemberN{Value: "1790003600"},
	})

	client := &countingClient{FakeDynamo: fake}

	_, err := newFenceRepo(client).RebuildFence().Begin(context.Background(), "other", time.Minute)
	require.True(t, evt.IsRebuildStateInvalidErr(err))
	require.False(t, policy.IsTransient(err))
	require.Zero(t, client.transactions.Load(), "nothing was written")
}
