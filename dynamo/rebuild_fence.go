package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/photon-grove/evt"
)

// The rebuild fence's rows and expressions, pinned by the storage spec (Rebuild Fence). Every
// attribute name an expression touches was audited against DynamoDB's reserved words: value and
// owner are reserved, so they are reached through the #value and #owner aliases.
const (
	rebuildLockSK       = int64(0)
	rebuildGenerationSK = int64(0)

	rebuildOwnerAttr      = "owner"
	rebuildLeaseAttr      = "leaseExpiresAt"
	rebuildValueAttr      = "value"
	rebuildForcedAttr     = "forceReleased"
	rebuildOwnerAlias     = "#owner"
	rebuildValueAlias     = "#value"
	rebuildNoLockHeld     = "attribute_not_exists(pk)"
	rebuildAcquireUpdate  = "SET #value = if_not_exists(#value, :zero) + :increment"
	rebuildAcquireCond    = "attribute_not_exists(#value) OR #value = :seenEven OR (#value = :seenOdd AND forceReleased = :true)"
	rebuildTakeoverCond   = "leaseExpiresAt < :now AND #owner = :observedOwner"
	rebuildTakeoverUpdate = "SET #value = #value + :two"
	rebuildSeenOddCond    = "#value = :seenOdd"
	rebuildRenewUpdate    = "SET leaseExpiresAt = :expiry"
	rebuildOwnerCond      = "#owner = :owner"
	rebuildReleaseCond    = "#owner = :me"
	rebuildReleaseUpdate  = "SET #value = #value + :one REMOVE forceReleased"
	rebuildForceCond      = "#owner = :requestedOwner"
	rebuildForceUpdate    = "SET #value = #value + :two, forceReleased = :true"
	rebuildGuardCond      = "#owner = :me AND leaseExpiresAt > :now"
)

// rebuildLockKey and rebuildGenerationKey are the primary keys of the fence's two items.
func rebuildLockKey() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: evt.RebuildLockID},
		"sk": &types.AttributeValueMemberN{Value: strconv.FormatInt(rebuildLockSK, 10)},
	}
}

func rebuildGenerationKey() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: evt.RebuildGenerationID},
		"sk": &types.AttributeValueMemberN{Value: strconv.FormatInt(rebuildGenerationSK, 10)},
	}
}

func rebuildNumber(value int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(value, 10)}
}

func rebuildOwnerNames() map[string]string {
	return map[string]string{rebuildOwnerAlias: rebuildOwnerAttr}
}

func rebuildValueNames() map[string]string {
	return map[string]string{rebuildValueAlias: rebuildValueAttr}
}

// rebuildLockCheck is the commit-time rebuild-lock check, the second action of every commit
// (storage spec, Writes): a ConditionCheck that fails whenever a lock row exists, an expired one
// included. It carries no expression names or values.
func (repo *Repository) rebuildLockCheck() types.TransactWriteItem {
	return types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
		TableName:           &repo.EventsTable,
		Key:                 rebuildLockKey(),
		ConditionExpression: aws.String(rebuildNoLockHeld),
	}}
}

// RebuildFence returns the rebuild fence over this repository's table (evt.RebuildFenceProvider).
// Leases are read from the repository's clock (WithClock).
func (repo *Repository) RebuildFence() evt.RebuildFence {
	return &rebuildFence{repo: repo}
}

var (
	_ evt.RebuildFenceProvider = (*Repository)(nil)
	_ evt.RebuildFence         = (*rebuildFence)(nil)
)

// rebuildFence is the DynamoDB evt.RebuildFence.
type rebuildFence struct{ repo *Repository }

// getItem reads one fence item with a strongly consistent read, whatever the repository's
// read-consistency setting: two eventually consistent reads can return the same stale settled
// generation while a rebuild holds the lock.
func (f *rebuildFence) getItem(
	ctx context.Context,
	name string,
	key map[string]types.AttributeValue,
) (map[string]types.AttributeValue, error) {
	out, err := f.repo.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &f.repo.EventsTable,
		ConsistentRead: aws.Bool(true),
		Key:            key,
	})
	if err != nil {
		return nil, fmt.Errorf("rebuild fence: reading %s: %w", name, err)
	}

	return out.Item, nil
}

// Generation reads the generation item.
func (f *rebuildFence) Generation(ctx context.Context) (evt.RebuildGeneration, error) {
	item, err := f.getItem(ctx, evt.RebuildGenerationID, rebuildGenerationKey())
	if err != nil {
		return evt.RebuildGeneration{}, err
	}

	return generationFromItem(item)
}

func generationFromItem(item map[string]types.AttributeValue) (evt.RebuildGeneration, error) {
	if len(item) == 0 {
		return evt.RebuildGeneration{}, nil
	}

	value, ok := int64Attr(item, rebuildValueAttr)
	if !ok {
		return evt.RebuildGeneration{}, errors.New("rebuild fence: the generation item has no numeric value")
	}

	forced := false
	if attr, present := item[rebuildForcedAttr]; present {
		flag, isBool := attr.(*types.AttributeValueMemberBOOL)
		if !isBool {
			return evt.RebuildGeneration{}, errors.New("rebuild fence: forceReleased is not a boolean")
		}

		forced = flag.Value
	}

	return evt.RebuildGeneration{Value: value, ForceReleased: forced}, nil
}

func lockFromItem(item map[string]types.AttributeValue) (*evt.RebuildLock, error) {
	if len(item) == 0 {
		return nil, nil
	}

	owner, err := stringAttr(item, rebuildOwnerAttr)
	if err != nil {
		return nil, fmt.Errorf("rebuild fence: %w", err)
	}

	expiry, ok := int64Attr(item, rebuildLeaseAttr)
	if !ok {
		return nil, errors.New("rebuild fence: the lock item has no numeric leaseExpiresAt")
	}

	return &evt.RebuildLock{Owner: owner, LeaseExpiresAt: time.Unix(expiry, 0)}, nil
}

// State reads the generation, then the lock row.
func (f *rebuildFence) State(ctx context.Context) (evt.RebuildState, error) {
	generation, err := f.Generation(ctx)
	if err != nil {
		return evt.RebuildState{}, err
	}

	item, err := f.getItem(ctx, evt.RebuildLockID, rebuildLockKey())
	if err != nil {
		return evt.RebuildState{}, err
	}

	lock, err := lockFromItem(item)
	if err != nil {
		return evt.RebuildState{}, err
	}

	return evt.RebuildState{Generation: generation, Lock: lock}, nil
}

// lockItem is the full lock item an acquire or a takeover puts.
func lockItem(owner string, expiry int64) map[string]types.AttributeValue {
	key := rebuildLockKey()

	return map[string]types.AttributeValue{
		"pk":             key["pk"],
		"sk":             key["sk"],
		rebuildOwnerAttr: &types.AttributeValueMemberS{Value: owner},
		rebuildLeaseAttr: rebuildNumber(expiry),
	}
}

// Begin acquires the lock or takes over an expired lease, one transaction either way, and decides
// again from a fresh read when the transaction loses a race.
func (f *rebuildFence) Begin(ctx context.Context, owner string, lease time.Duration) (evt.RebuildLease, error) {
	if err := evt.CheckRebuildOwner(owner); err != nil {
		return evt.RebuildLease{}, err
	}

	seconds, err := evt.RebuildLeaseSeconds(lease)
	if err != nil {
		return evt.RebuildLease{}, err
	}

	var invalid error

	for attempt := 1; attempt <= evt.MaxRebuildBeginAttempts; attempt++ {
		state, serr := f.State(ctx)
		if serr != nil {
			return evt.RebuildLease{}, serr
		}

		now := f.repo.nowOrDefault()

		plan, perr := evt.PlanRebuildBegin(state, now)
		if evt.IsRebuildStateInvalidErr(perr) {
			// The two items are read one after the other, so a transition that lands between the
			// reads shows a state no transaction produced. Read again; only a state that persists
			// is reported.
			invalid = perr

			continue
		}

		if perr != nil {
			return evt.RebuildLease{}, perr
		}

		invalid = nil

		expiry := now.Unix() + seconds

		_, terr := f.repo.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: f.beginItems(plan, owner, now.Unix(), expiry),
		})
		if terr == nil {
			return evt.RebuildLease{
				Owner:          owner,
				Generation:     state.Generation.Value + plan.Increment,
				LeaseExpiresAt: time.Unix(expiry, 0),
			}, nil
		}

		if !raceLost(terr) {
			return evt.RebuildLease{}, fmt.Errorf("rebuild fence: begin: %w", terr)
		}
	}

	if invalid != nil {
		return evt.RebuildLease{}, invalid
	}

	return evt.RebuildLease{}, wrapClassifiedError(&evt.RebuildConflictError{Attempts: evt.MaxRebuildBeginAttempts})
}

// raceLost reports whether a cancelled fence transaction lost to another operation: a failed
// condition, or another transaction writing the same items.
func raceLost(err error) bool {
	if failed, _ := HasConditionalCheckFailure(err); failed {
		return true
	}

	return hasTransactionConflict(err)
}

// beginItems builds the acquire's or the takeover's two actions.
func (f *rebuildFence) beginItems(plan evt.RebuildPlan, owner string, now, expiry int64) []types.TransactWriteItem {
	if plan.Kind == evt.RebuildTakeover {
		return []types.TransactWriteItem{
			{Put: &types.Put{
				TableName:                &f.repo.EventsTable,
				Item:                     lockItem(owner, expiry),
				ConditionExpression:      aws.String(rebuildTakeoverCond),
				ExpressionAttributeNames: rebuildOwnerNames(),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":now":           rebuildNumber(now),
					":observedOwner": &types.AttributeValueMemberS{Value: plan.ObservedOwner},
				},
			}},
			{Update: &types.Update{
				TableName:                &f.repo.EventsTable,
				Key:                      rebuildGenerationKey(),
				UpdateExpression:         aws.String(rebuildTakeoverUpdate),
				ConditionExpression:      aws.String(rebuildSeenOddCond),
				ExpressionAttributeNames: rebuildValueNames(),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":two":     rebuildNumber(2),
					":seenOdd": rebuildNumber(plan.SeenOdd),
				},
			}},
		}
	}

	return []types.TransactWriteItem{
		{Put: &types.Put{
			TableName:           &f.repo.EventsTable,
			Item:                lockItem(owner, expiry),
			ConditionExpression: aws.String(rebuildNoLockHeld),
		}},
		{Update: &types.Update{
			TableName:                &f.repo.EventsTable,
			Key:                      rebuildGenerationKey(),
			UpdateExpression:         aws.String(rebuildAcquireUpdate),
			ConditionExpression:      aws.String(rebuildAcquireCond),
			ExpressionAttributeNames: rebuildValueNames(),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":zero":      rebuildNumber(0),
				":increment": rebuildNumber(plan.Increment),
				":seenEven":  rebuildNumber(plan.SeenEven),
				":seenOdd":   rebuildNumber(plan.SeenOdd),
				":true":      &types.AttributeValueMemberBOOL{Value: true},
			},
		}},
	}
}

// Renew extends the holder's lease with one conditional update of the lock item.
func (f *rebuildFence) Renew(ctx context.Context, owner string, lease time.Duration) (time.Time, error) {
	if err := evt.CheckRebuildOwner(owner); err != nil {
		return time.Time{}, err
	}

	seconds, err := evt.RebuildLeaseSeconds(lease)
	if err != nil {
		return time.Time{}, err
	}

	expiry := f.repo.nowOrDefault().Unix() + seconds

	_, err = f.repo.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                &f.repo.EventsTable,
		Key:                      rebuildLockKey(),
		UpdateExpression:         aws.String(rebuildRenewUpdate),
		ConditionExpression:      aws.String(rebuildOwnerCond),
		ExpressionAttributeNames: rebuildOwnerNames(),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":expiry": rebuildNumber(expiry),
			":owner":  &types.AttributeValueMemberS{Value: owner},
		},
	})
	if err != nil {
		var failed *types.ConditionalCheckFailedException
		if errors.As(err, &failed) {
			return time.Time{}, &evt.RebuildLeaseLostError{Owner: owner}
		}

		return time.Time{}, fmt.Errorf("rebuild fence: renew: %w", err)
	}

	return time.Unix(expiry, 0), nil
}

// End releases the lock and restores the settled generation in one transaction.
func (f *rebuildFence) End(ctx context.Context, owner string) (evt.RebuildGeneration, error) {
	if err := evt.CheckRebuildOwner(owner); err != nil {
		return evt.RebuildGeneration{}, err
	}

	seen, err := f.heldGeneration(ctx, owner)
	if err != nil {
		return evt.RebuildGeneration{}, err
	}

	err = f.release(ctx, owner, rebuildReleaseCond, rebuildReleaseUpdate, ":me", 1, seen.Value, false)
	if err != nil {
		return evt.RebuildGeneration{}, err
	}

	return evt.RebuildGeneration{Value: seen.Value + 1}, nil
}

// ForceRelease deletes the named owner's lock and leaves the generation odd and flagged.
func (f *rebuildFence) ForceRelease(ctx context.Context, owner string) (evt.RebuildGeneration, error) {
	if err := evt.CheckRebuildOwner(owner); err != nil {
		return evt.RebuildGeneration{}, err
	}

	seen, err := f.heldGeneration(ctx, owner)
	if err != nil {
		return evt.RebuildGeneration{}, err
	}

	err = f.release(ctx, owner, rebuildForceCond, rebuildForceUpdate, ":requestedOwner", 2, seen.Value, true)
	if err != nil {
		return evt.RebuildGeneration{}, err
	}

	return evt.RebuildGeneration{Value: seen.Value + 2, ForceReleased: true}, nil
}

// heldGeneration reads the generation a release asserts. A settled generation means no rebuild
// holds the fence, so the caller cannot be the holder.
func (f *rebuildFence) heldGeneration(ctx context.Context, owner string) (evt.RebuildGeneration, error) {
	generation, err := f.Generation(ctx)
	if err != nil {
		return evt.RebuildGeneration{}, err
	}

	if generation.Settled() {
		return evt.RebuildGeneration{}, &evt.RebuildLeaseLostError{Owner: owner}
	}

	return generation, nil
}

// release runs a release's or a force-release's transaction: delete the owner's lock row and bump
// the generation from the odd value observed.
func (f *rebuildFence) release(
	ctx context.Context,
	owner, lockCondition, update, ownerPlaceholder string,
	increment, seenOdd int64,
	forced bool,
) error {
	values := map[string]types.AttributeValue{
		":seenOdd": rebuildNumber(seenOdd),
	}

	if increment == 1 {
		values[":one"] = rebuildNumber(1)
	} else {
		values[":two"] = rebuildNumber(2)
	}

	if forced {
		values[":true"] = &types.AttributeValueMemberBOOL{Value: true}
	}

	_, err := f.repo.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{Delete: &types.Delete{
				TableName:                &f.repo.EventsTable,
				Key:                      rebuildLockKey(),
				ConditionExpression:      aws.String(lockCondition),
				ExpressionAttributeNames: rebuildOwnerNames(),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					ownerPlaceholder: &types.AttributeValueMemberS{Value: owner},
				},
			}},
			{Update: &types.Update{
				TableName:                 &f.repo.EventsTable,
				Key:                       rebuildGenerationKey(),
				UpdateExpression:          aws.String(update),
				ConditionExpression:       aws.String(rebuildSeenOddCond),
				ExpressionAttributeNames:  rebuildValueNames(),
				ExpressionAttributeValues: values,
			}},
		},
	})
	if err == nil {
		return nil
	}

	// Only a takeover or a force-release changes the generation of a held lock, and both replace
	// or remove the lock row: a failed condition means the caller no longer holds it.
	if failed, _ := HasConditionalCheckFailure(err); failed {
		return &evt.RebuildLeaseLostError{Owner: owner}
	}

	return wrapClassifiedError(fmt.Errorf("rebuild fence: release: %w", err))
}

// guard is the guard ConditionCheck of a rebuild view write: the caller still holds an
// unexpired lease.
func (f *rebuildFence) guard(owner string, now int64) types.TransactWriteItem {
	return types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
		TableName:                &f.repo.EventsTable,
		Key:                      rebuildLockKey(),
		ConditionExpression:      aws.String(rebuildGuardCond),
		ExpressionAttributeNames: rebuildOwnerNames(),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":me":  &types.AttributeValueMemberS{Value: owner},
			":now": rebuildNumber(now),
		},
	}}
}

// WriteViews writes one batch of view operations and the lease guard as the last action of a
// single transaction.
func (f *rebuildFence) WriteViews(ctx context.Context, owner string, views []evt.ProjectedView) error {
	if err := evt.CheckRebuildOwner(owner); err != nil {
		return err
	}

	if err := evt.CheckRebuildViewBatch(views); err != nil {
		return err
	}

	items, err := f.repo.buildViewItems(views)
	if err != nil {
		return err
	}

	items = append(items, f.guard(owner, f.repo.nowOrDefault().Unix()))

	_, err = f.repo.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if err == nil {
		return nil
	}

	codes := cancellationCodes(err)

	// The guard is the last action. A lost lease outranks a view condition: the worker must stop
	// whatever else failed.
	if conditionFailed(codes, len(items)-1) {
		return &evt.RebuildLeaseLostError{Owner: owner}
	}

	if failure := viewReasonFailure(views, 0, codes, err); failure != nil {
		return failure
	}

	return wrapClassifiedError(fmt.Errorf("rebuild fence: writing views: %w", err))
}
