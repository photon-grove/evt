package dynamo

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
)

// GetSnapshot gets the latest Snapshot for an Entity instance.
// Snapshots are stored inline in the event-log table at sk=0.
func (repo *Repository) GetSnapshot(
	ctx context.Context,
	entityID evt.EntityID,
) (*evt.SerializedSnapshot, error) {
	return repo.getSnapshot(ctx, entityID, repo.consistentRead)
}

// getSnapshot reads the inline snapshot with the given read consistency.
func (repo *Repository) getSnapshot(
	ctx context.Context,
	entityID evt.EntityID,
	consistent bool,
) (*evt.SerializedSnapshot, error) {
	input := dynamodb.GetItemInput{
		TableName:      &repo.EventsTable,
		ConsistentRead: aws.Bool(consistent),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: string(entityID)},
			"sk": &types.AttributeValueMemberN{Value: "0"},
		},
	}

	snapshot, err := repo.querySnapshot(ctx, input)
	if err != nil {
		return nil, err
	}

	return snapshot, nil
}

// PutSnapshot writes a snapshot, guarded by the stream's head item and the mandatory
// feature-marker check, in one transaction (marker check, head item, snapshot put).
// Snapshots are stored inline in the event-log table at sk=0.
// This is intended for use by projectors and background processes that need to
// write snapshots outside of the transactional command handler flow.
//
// The write is monotonic in eventSeq: it never overwrites an existing snapshot with one whose
// EventSequence is lower. The sk=0 snapshot is the durable floor for both snapshot-aware load and
// CompactBelow — if a stale/background writer could regress it below already-compacted events, a
// later load would seed from the older snapshot and query events that compaction has deleted,
// rebuilding with missing history. A regressing write is treated as a no-op (the existing, newer
// snapshot already covers it), so background catch-up writers do not see spurious errors.
//
// The write also keeps invariant H3 (a snapshot never covers more than the stream's head): the
// head item must be at least the snapshot's EventSequence, or absent when that is 0. A snapshot
// ahead of the stream, or on a stream with no head item, would let a writer that loaded earlier
// append events beneath a snapshot that already covers them, so it is refused with
// *evt.StreamHeadDriftError instead of being written. For an entity type with retention the same
// transaction keeps the head item's ttl at least the snapshot's, never lowering it.
func (repo *Repository) PutSnapshot(
	ctx context.Context,
	entityType evt.EntityType,
	entityID evt.EntityID,
	payload []byte,
	snapshotSequence evt.EventSequence,
	eventSequence evt.EventSequence,
) error {
	if err := repo.checkFeatureMarkerTable(); err != nil {
		return err
	}

	if err := evt.CheckReservedEntityID(entityID); err != nil {
		return err
	}

	// Mirror the events' retention so a background/catch-up snapshot of a policy'd type expires
	// with its stream instead of lingering as an orphan sk=0 row; omitempty drops it otherwise.
	ttl := repo.ttlFor(entityType)

	snapshot := Snapshot{
		PK:            entityID,
		SK:            0,
		Sequence:      snapshotSequence,
		EventSequence: eventSequence,
		EntityType:    entityType,
		Payload:       string(payload),
		TTL:           ttl,
	}

	item, err := repo.marshalMap(snapshot)
	if err != nil {
		return fmt.Errorf("error marshalling snapshot to map: %w", err)
	}

	put := &types.Put{
		TableName: &repo.EventsTable,
		Item:      item,
		// Monotonic floor: allow the write only when no snapshot exists yet or the stored snapshot's
		// EventSequence is not ahead of this one. This prevents the sk=0 floor from regressing below
		// compacted events.
		ConditionExpression: aws.String("attribute_not_exists(eventSeq) OR eventSeq <= :new"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":new": &types.AttributeValueMemberN{Value: strconv.Itoa(int(eventSequence))},
		},
	}

	err = repo.putSnapshotGuarded(ctx, entityID, eventSequence, ttl, put, true, 0)
	if err == nil {
		repo.ttlFloors.observe(entityID, ttl)
	}

	return err
}

// putSnapshotGuarded sends the snapshot transaction: the marker check, the head guard, and the
// snapshot put. refreshTTL says whether the guard also refreshes a retained head item's ttl; it is
// false only on the retry that follows a guard failure after which the stored head ttl already
// covers the snapshot's. retries counts the guard failures this call chain has already absorbed.
func (repo *Repository) putSnapshotGuarded(
	ctx context.Context,
	entityID evt.EntityID,
	eventSequence evt.EventSequence,
	ttl int64,
	put *types.Put,
	refreshTTL bool,
	retries int,
) error {
	check, err := repo.features.checkItem(ctx)
	if err != nil {
		return err
	}

	guard := repo.buildSnapshotGuard(entityID, eventSequence, ttl, refreshTTL)

	_, err = repo.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{check, guard, {Put: put}},
	})
	if err == nil {
		return nil
	}

	if failed, index := HasConditionalCheckFailure(err); failed {
		switch index {
		case 0:
			return repo.features.handleCheckFailure(ctx, err)

		case 1:
			return repo.snapshotGuardFailed(ctx, entityID, eventSequence, ttl, put, refreshTTL && guard.Update != nil, retries)

		case 2:
			// A newer snapshot already covers this regressing maintenance write.
			return nil
		}
	}

	return fmt.Errorf("failed to put snapshot: %w", err)
}

// maxSnapshotGuardRetries bounds how often a snapshot write re-sends its guard after a concurrent
// writer moved the head item under it.
const maxSnapshotGuardRetries = 4

// snapshotGuardFailed classifies a failed head guard by reading the head item back.
//
// For a retained stream the refreshing update has two conditions (the head covers the snapshot,
// and its stored ttl is not above the snapshot's), and it can fail on either. The read decides
// which: when the head covers the snapshot, the stored ttl is what matters. A ttl at least the
// snapshot's needs no refresh, so the write is retried as a plain check. A lower or missing ttl
// means the guard failed while the head was still behind and a concurrent writer then advanced it
// with a shorter ttl: the refresh is retried, because a plain check would leave the snapshot
// outliving the head item. Otherwise the snapshot is ahead of the stream, or the stream has no
// head item: drift.
func (repo *Repository) snapshotGuardFailed(
	ctx context.Context,
	entityID evt.EntityID,
	eventSequence evt.EventSequence,
	ttl int64,
	put *types.Put,
	refreshed bool,
	retries int,
) error {
	head, err := repo.readHeadItem(ctx, entityID)
	if err != nil {
		return wrapClassifiedError(err)
	}

	if refreshed && head.present && head.head >= eventSequence {
		if retries >= maxSnapshotGuardRetries {
			return &policy.ClassifiedError{
				Class: policy.ClassTransient,
				Err: fmt.Errorf(
					"dynamo: the head item of %s kept moving under its snapshot write", entityID),
			}
		}

		return repo.putSnapshotGuarded(ctx, entityID, eventSequence, ttl, put, head.ttl < ttl, retries+1)
	}

	derived, err := repo.deriveHead(ctx, entityID)
	if err != nil {
		return wrapClassifiedError(err)
	}

	return &evt.StreamHeadDriftError{
		EntityID:   entityID,
		Stored:     head.head,
		HeadAbsent: !head.present,
		// The head the log would derive once the snapshot is written.
		Derived: max(derived, eventSequence),
	}
}

// Submit the given QueryInput to retrieve Snapshot records from the DynamoDB table
func (repo *Repository) querySnapshot(
	ctx context.Context,
	queryInput dynamodb.GetItemInput,
) (*evt.SerializedSnapshot, error) {
	result, err := repo.client.GetItem(ctx, &queryInput)
	if err != nil {
		return nil, err
	}
	if len(result.Item) == 0 {
		return nil, nil
	}

	snapshot := Snapshot{}

	err = repo.unmarshalMap(result.Item, &snapshot)
	if err != nil {
		return nil, err
	}

	repo.ttlFloors.observe(snapshot.PK, snapshot.TTL)

	return &evt.SerializedSnapshot{
		EntityType:    snapshot.EntityType,
		EntityID:      snapshot.PK,
		Sequence:      snapshot.Sequence,
		EventSequence: snapshot.EventSequence,
		Payload:       []byte(snapshot.Payload),
	}, nil
}
