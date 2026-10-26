package compat

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
)

// The rebuild-fence vectors (C10) pin every request the DynamoDB lifecycle sends: the reads of the
// two fence items, the acquire, takeover, release, and force-release transactions, the renewal
// update, and a guarded view write, each with its expressions, name aliases, and values. Go
// generates them by running the real repository against a seeded table under a fixed clock; Rust
// builds the same requests from the same inputs and compares them item for item.

// RebuildFenceNow is the fixed clock of every rebuild-fence vector, in Unix seconds.
const RebuildFenceNow int64 = 1_790_000_000

// RebuildFenceVector is one lifecycle operation: the table it starts from, the call, the requests
// the repository sent, and what it returned.
type RebuildFenceVector struct {
	Name string `json:"name"`

	// Before is the state the table starts in: the generation item and the lock item. Both are
	// absent (null) in a store that never ran a rebuild.
	Before RebuildFenceState `json:"before"`

	// Operation is the lifecycle call: begin, renew, end, forceRelease, state, generation, or
	// writeViews.
	Operation string `json:"operation"`
	Owner     string `json:"owner,omitempty"`

	// LeaseSeconds is the lease in whole seconds (begin, renew).
	LeaseSeconds int64 `json:"leaseSeconds,omitempty"`

	// LeaseGeneration is the fencing generation of the lease the call presents (renew, end,
	// writeViews): the generation Begin returned to the caller.
	LeaseGeneration int64 `json:"leaseGeneration,omitempty"`

	// Calls are the requests the repository sent, in order.
	Calls []RebuildFenceCall `json:"calls"`

	// Result is what the call returned, and After the two items the table holds afterwards.
	Result RebuildFenceResult `json:"result"`
	After  RebuildFenceState  `json:"after"`
}

// RebuildFenceState is the stored form of the fence: the DynamoDB items, absent when null.
type RebuildFenceState struct {
	Generation map[string]any `json:"generation"`
	Lock       map[string]any `json:"lock"`
}

// RebuildFenceCall is one request: a read, an update, or a transaction with its ordered actions.
type RebuildFenceCall struct {
	// Call is GetItem, UpdateItem, or TransactWriteItems.
	Call string `json:"call"`

	// ConsistentRead is set on a GetItem, which must always be strongly consistent.
	ConsistentRead *bool `json:"consistentRead,omitempty"`

	// Actions are the transaction's actions in order; a GetItem carries its key in the single
	// action, and an UpdateItem its update.
	Actions []FeatureWriteItem `json:"actions"`
}

// RebuildFenceResult is the outcome of the call: the generation after (begin, end, forceRelease,
// generation, state), the lease expiry (begin, renew), the lock owner a state read found, and the
// error kind when it failed.
type RebuildFenceResult struct {
	Generation     *int64 `json:"generation,omitempty"`
	ForceReleased  bool   `json:"forceReleased,omitempty"`
	LeaseExpiresAt *int64 `json:"leaseExpiresAt,omitempty"`
	LockOwner      string `json:"lockOwner,omitempty"`
	Error          string `json:"error,omitempty"`
}

// rebuildCaptureDynamo records every call the lifecycle makes against the shared fake.
type rebuildCaptureDynamo struct {
	*FakeDynamo
	calls []RebuildFenceCall
}

func (c *rebuildCaptureDynamo) TransactWriteItems(
	ctx context.Context,
	in *dynamodb.TransactWriteItemsInput,
	opts ...func(*dynamodb.Options),
) (*dynamodb.TransactWriteItemsOutput, error) {
	capture := &featureCaptureDynamo{FakeDynamo: c.FakeDynamo}
	out, err := capture.TransactWriteItems(ctx, in, opts...)
	c.calls = append(c.calls, RebuildFenceCall{Call: "TransactWriteItems", Actions: capture.writes})

	return out, err
}

func (c *rebuildCaptureDynamo) UpdateItem(
	ctx context.Context,
	in *dynamodb.UpdateItemInput,
	opts ...func(*dynamodb.Options),
) (*dynamodb.UpdateItemOutput, error) {
	c.calls = append(c.calls, RebuildFenceCall{Call: "UpdateItem", Actions: []FeatureWriteItem{{
		Operation: "Update", TableName: aws.ToString(in.TableName), Key: DynamoItemJSON(in.Key),
		UpdateExpression: aws.ToString(in.UpdateExpression), ConditionExpression: aws.ToString(in.ConditionExpression),
		ExpressionAttributeNames: in.ExpressionAttributeNames, ExpressionAttributeValues: DynamoItemJSON(in.ExpressionAttributeValues),
	}}})

	return c.FakeDynamo.UpdateItem(ctx, in, opts...)
}

func (c *rebuildCaptureDynamo) GetItem(
	ctx context.Context,
	in *dynamodb.GetItemInput,
	opts ...func(*dynamodb.Options),
) (*dynamodb.GetItemOutput, error) {
	consistent := aws.ToBool(in.ConsistentRead)
	c.calls = append(c.calls, RebuildFenceCall{
		Call: "GetItem", ConsistentRead: &consistent,
		Actions: []FeatureWriteItem{{Operation: "Get", TableName: aws.ToString(in.TableName), Key: DynamoItemJSON(in.Key)}},
	})

	return c.FakeDynamo.GetItem(ctx, in, opts...)
}

var _ dynamo.Client = (*rebuildCaptureDynamo)(nil)

// rebuildSeed is a table state a vector starts from.
type rebuildSeed struct {
	generation *int64
	forced     bool
	// released seeds the lock item a force-release leaves in place of the lock.
	released   bool
	lockOwner  string
	lockGen    int64
	lockExpiry int64
}

func int64Ptr(n int64) *int64 { return &n }

func (seed rebuildSeed) apply(fake *FakeDynamo) {
	if seed.generation != nil {
		item := map[string]types.AttributeValue{
			"pk":    &types.AttributeValueMemberS{Value: evt.RebuildGenerationID},
			"sk":    &types.AttributeValueMemberN{Value: "0"},
			"value": &types.AttributeValueMemberN{Value: fmt.Sprint(*seed.generation)},
		}
		if seed.forced {
			item["forceReleased"] = &types.AttributeValueMemberBOOL{Value: true}
		}

		fake.SeedItem(item)
	}

	if seed.released {
		fake.SeedItem(map[string]types.AttributeValue{
			"pk":            &types.AttributeValueMemberS{Value: evt.RebuildLockID},
			"sk":            &types.AttributeValueMemberN{Value: "0"},
			"forceReleased": &types.AttributeValueMemberBOOL{Value: true},
		})
	}

	if seed.lockOwner != "" {
		fake.SeedItem(map[string]types.AttributeValue{
			"pk":             &types.AttributeValueMemberS{Value: evt.RebuildLockID},
			"sk":             &types.AttributeValueMemberN{Value: "0"},
			"owner":          &types.AttributeValueMemberS{Value: seed.lockOwner},
			"generation":     &types.AttributeValueMemberN{Value: fmt.Sprint(seed.lockGen)},
			"leaseExpiresAt": &types.AttributeValueMemberN{Value: fmt.Sprint(seed.lockExpiry)},
		})
	}
}

func rebuildFenceState(fake *FakeDynamo) RebuildFenceState {
	var state RebuildFenceState

	if items := fake.Items(evt.RebuildGenerationID); len(items) == 1 {
		state.Generation = DynamoItemJSON(items[0])
	}

	if items := fake.Items(evt.RebuildLockID); len(items) == 1 {
		state.Lock = DynamoItemJSON(items[0])
	}

	return state
}

// GenerateRebuildFenceVectors runs the lifecycle against seeded tables and records the requests.
func GenerateRebuildFenceVectors(ctx context.Context) ([]RebuildFenceVector, error) {
	now := RebuildFenceNow
	clock := func() time.Time { return time.Unix(now, 0) }

	type spec struct {
		name      string
		seed      rebuildSeed
		operation string
		owner     string
		lease     int64
		token     int64
	}

	held := func(generation int64, owner string, expiry int64) rebuildSeed {
		return rebuildSeed{generation: int64Ptr(generation), lockOwner: owner, lockGen: generation, lockExpiry: expiry}
	}

	// forceReleased is the state a force-release leaves: the generation odd and flagged, and the
	// force-released lock item in place of the lock.
	forceReleased := func(generation int64) rebuildSeed {
		return rebuildSeed{generation: int64Ptr(generation), forced: true, released: true}
	}

	specs := []spec{
		{name: "state-empty", operation: "state"},
		{name: "state-held", seed: held(5, "job-a", now+60), operation: "state"},
		{name: "state-force-released", seed: forceReleased(7), operation: "state"},
		{name: "generation-force-released", seed: forceReleased(7), operation: "generation"},
		{name: "acquire-first-rebuild", operation: "begin", owner: "job-a", lease: 60},
		{name: "acquire-from-even", seed: rebuildSeed{generation: int64Ptr(4)}, operation: "begin", owner: "job-a", lease: 90},
		{name: "acquire-from-force-released", seed: forceReleased(5), operation: "begin", owner: "job-b", lease: 60},
		{name: "takeover-expired", seed: held(5, "job-a", now-1), operation: "begin", owner: "job-b", lease: 60},
		{name: "held-is-refused", seed: held(5, "job-a", now), operation: "begin", owner: "job-b", lease: 60},
		{name: "renew", seed: held(5, "job-a", now+10), operation: "renew", owner: "job-a", lease: 120, token: 5},
		{name: "renew-lost", seed: held(5, "job-b", now+10), operation: "renew", owner: "job-a", lease: 120, token: 5},
		{name: "renew-stale-incarnation", seed: held(7, "job-a", now+10), operation: "renew", owner: "job-a", lease: 120, token: 5},
		{name: "release", seed: held(5, "job-a", now+10), operation: "end", owner: "job-a", token: 5},
		{
			name:      "release-clears-force-released",
			seed:      rebuildSeed{generation: int64Ptr(7), forced: true, lockOwner: "job-a", lockGen: 7, lockExpiry: now + 10},
			operation: "end", owner: "job-a", token: 7,
		},
		{name: "release-lost", seed: held(7, "job-b", now+10), operation: "end", owner: "job-a", token: 7},
		{name: "release-stale-incarnation", seed: held(7, "job-a", now+10), operation: "end", owner: "job-a", token: 5},
		{name: "force-release", seed: held(5, "job-a", now-100), operation: "forceRelease", owner: "job-a"},
		{name: "force-release-again", seed: forceReleased(7), operation: "forceRelease", owner: "job-a"},
		{name: "renew-after-force-release", seed: forceReleased(7), operation: "renew", owner: "job-a", lease: 120, token: 5},
		{name: "release-after-force-release", seed: forceReleased(7), operation: "end", owner: "job-a", token: 5},
		{name: "guarded-views-after-force-release", seed: forceReleased(7), operation: "writeViews", owner: "job-a", token: 5},
		{name: "guarded-views", seed: held(5, "job-a", now+10), operation: "writeViews", owner: "job-a", token: 5},
		{name: "guarded-views-lease-lost", seed: held(7, "job-b", now+10), operation: "writeViews", owner: "job-a", token: 7},
		{name: "guarded-views-stale-incarnation", seed: held(7, "job-a", now+10), operation: "writeViews", owner: "job-a", token: 5},
	}

	vectors := make([]RebuildFenceVector, 0, len(specs))

	for _, s := range specs {
		fake := NewFakeDynamo()
		capture := &rebuildCaptureDynamo{FakeDynamo: fake}

		s.seed.apply(fake)

		vector := RebuildFenceVector{
			Name: s.name, Before: rebuildFenceState(fake), Operation: s.operation, Owner: s.owner, LeaseSeconds: s.lease,
			LeaseGeneration: s.token,
		}

		repo := dynamo.NewRepository(capture, fakeTable).WithLazyInitialization(true).WithViewsTable("views").WithClock(clock)

		result, err := runRebuildFenceOperation(ctx, repo.RebuildFence(), s.operation, evt.RebuildLease{Owner: s.owner, Generation: s.token}, s.lease)
		if err != nil {
			return nil, fmt.Errorf("rebuild fence vector %s: %w", s.name, err)
		}

		vector.Calls = capture.calls
		vector.Result = result
		vector.After = rebuildFenceState(fake)

		vectors = append(vectors, vector)
	}

	return vectors, nil
}

// runRebuildFenceOperation performs one lifecycle call and summarizes its result by error kind.
func runRebuildFenceOperation(
	ctx context.Context,
	fence evt.RebuildFence,
	operation string,
	held evt.RebuildLease,
	leaseSeconds int64,
) (RebuildFenceResult, error) {
	owner := held.Owner
	var result RebuildFenceResult

	lease := time.Duration(leaseSeconds) * time.Second

	setGeneration := func(generation evt.RebuildGeneration) {
		result.Generation = &generation.Value
		result.ForceReleased = generation.ForceReleased
	}

	var err error

	switch operation {
	case "state":
		var state evt.RebuildState

		if state, err = fence.State(ctx); err == nil {
			setGeneration(state.Generation)

			if state.Lock != nil {
				result.LockOwner = state.Lock.Owner
				result.LeaseExpiresAt = int64Ptr(state.Lock.LeaseExpiresAt.Unix())
			}
		}

	case "generation":
		var generation evt.RebuildGeneration

		if generation, err = fence.Generation(ctx); err == nil {
			setGeneration(generation)
		}

	case "begin":
		var started evt.RebuildLease

		if started, err = fence.Begin(ctx, owner, lease); err == nil {
			result.Generation = &started.Generation
			result.LeaseExpiresAt = int64Ptr(started.LeaseExpiresAt.Unix())
		}

	case "renew":
		var renewed evt.RebuildLease

		if renewed, err = fence.Renew(ctx, held, lease); err == nil {
			result.LeaseExpiresAt = int64Ptr(renewed.LeaseExpiresAt.Unix())
		}

	case "end":
		var generation evt.RebuildGeneration

		if generation, err = fence.End(ctx, held); err == nil {
			setGeneration(generation)
		}

	case "forceRelease":
		var generation evt.RebuildGeneration

		if generation, err = fence.ForceRelease(ctx, owner); err == nil {
			setGeneration(generation)
		}

	case "writeViews":
		err = fence.WriteViews(ctx, held, []evt.ProjectedView{
			{Projector: "rebuild", Op: evt.PutView("rebuilt", "row-1", "acct-1", "account", []byte(`{"n":1}`))},
			{Projector: "rebuild", Op: evt.DeleteView("rebuilt", "row-2", "acct-2")},
		})

	default:
		return result, fmt.Errorf("unknown operation %q", operation)
	}

	result.Error = rebuildErrorKind(err)

	return result, nil
}

// rebuildErrorKind names the typed error a lifecycle call failed with, or "" for success.
func rebuildErrorKind(err error) string {
	switch {
	case err == nil:
		return ""
	case evt.IsRebuildLockHeldErr(err):
		return "lockHeld"
	case evt.IsRebuildLeaseLostErr(err):
		return "leaseLost"
	case evt.IsRebuildStateInvalidErr(err):
		return "stateInvalid"
	case evt.IsRebuildConflictErr(err):
		return "conflict"
	default:
		return "other: " + err.Error()
	}
}
