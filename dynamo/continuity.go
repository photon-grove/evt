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

// ContinuationCondition guards a written stream's first new event: the event just before it still
// exists. Together with the event put's attribute_not_exists(sk), it makes a commit continue
// exactly from the stored head, so no sequence is ever skipped.
const ContinuationCondition = "attribute_exists(sk)"

// PinnedFloorCondition replaces FloorCondition when the stream's head event was compacted away: the
// snapshot's eventSeq is the head, so it must still equal the sequence before the commit's first.
const PinnedFloorCondition = "eventSeq = :prev"

// PinnedNextSnapshotCondition is NextSnapshotCondition for the same compacted-head case.
const PinnedNextSnapshotCondition = "seq = :seq AND eventSeq = :prev"

// continuation records how a written stream's first sequence was tied to the stored head.
type continuation struct {
	// previous is the commit's first sequence minus one: the head the writer believes in.
	previous evt.EventSequence
	// viaSnapshot is true when the snapshot ends exactly at the head, so compaction may delete the
	// head event at any time; the snapshot's eventSeq (equal to previous) pins the head instead of
	// the event item.
	viaSnapshot bool
}

// firstSequences returns each written stream's first sequence in the events' order.
func firstSequences(events []evt.SerializedEvent) ([]evt.EntityID, map[evt.EntityID]evt.EventSequence) {
	first := make(map[evt.EntityID]evt.EventSequence)
	var order []evt.EntityID

	for _, event := range events {
		current, seen := first[event.EntityID]
		if !seen {
			order = append(order, event.EntityID)
		}
		if !seen || event.Sequence < current {
			first[event.EntityID] = event.Sequence
		}
	}

	return order, first
}

// observeContinuations reads, strongly consistent, how each written stream whose first sequence is
// above 1 continues from its stored head, and refuses a gap with *evt.SequenceGapError before any
// write. A stream's head is its highest event or, once compaction deleted every covered event, its
// snapshot's eventSeq. The commit then pins what was observed with conditions, so a head that moves
// between this read and the write fails the transaction as a conflict instead of committing a gap.
//
// Which item pins the head depends on the snapshot. Compaction deletes events [1, through] where
// through can equal the snapshot's eventSeq, so a head event the snapshot covers can vanish at any
// moment: a stream whose snapshot ends exactly at the head (eventSeq == first-1) is pinned through
// the snapshot, which compaction never changes. Otherwise the head event lies above the snapshot,
// compaction cannot touch it, and it is pinned by an existence check.
func (repo *Repository) observeContinuations(
	ctx context.Context,
	events []evt.SerializedEvent,
) (map[evt.EntityID]continuation, error) {
	order, first := firstSequences(events)

	var pins map[evt.EntityID]continuation

	for _, entityID := range order {
		if first[entityID] <= 1 {
			continue
		}

		previous := first[entityID] - 1

		snapshot, err := repo.getSnapshot(ctx, entityID, true)
		if err != nil {
			return nil, fmt.Errorf("dynamo: reading the snapshot of %s: %w", entityID, err)
		}

		pin := continuation{previous: previous}

		switch {
		case snapshot != nil && snapshot.EventSequence == previous:
			pin.viaSnapshot = true

		case snapshot != nil && snapshot.EventSequence > previous:
			// The stream moved past the writer's head; the floor check would refuse the commit.
			return nil, wrapClassifiedError(fmt.Errorf("stream %s: %w", entityID, ErrSnapshotRaceCondition))

		default:
			head, gerr := repo.client.GetItem(ctx, &dynamodb.GetItemInput{
				TableName:      &repo.EventsTable,
				ConsistentRead: aws.Bool(true),
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: string(entityID)},
					"sk": &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(previous), 10)},
				},
				ProjectionExpression: aws.String("sk"),
			})
			if gerr != nil {
				return nil, fmt.Errorf("dynamo: reading the head event of %s: %w", entityID, gerr)
			}

			if len(head.Item) == 0 {
				return nil, repo.sequenceGap(ctx, entityID, first[entityID], snapshot)
			}
		}

		if pins == nil {
			pins = make(map[evt.EntityID]continuation)
		}
		pins[entityID] = pin
	}

	return pins, nil
}

// sequenceGap builds the typed gap error with the stream's true stored head.
func (repo *Repository) sequenceGap(
	ctx context.Context,
	entityID evt.EntityID,
	first evt.EventSequence,
	snapshot *evt.SerializedSnapshot,
) error {
	head := evt.EventSequence(0)
	if snapshot != nil {
		head = snapshot.EventSequence
	}

	latest, err := repo.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &repo.EventsTable,
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("pk = :pk AND sk > :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: string(entityID)},
			":sk": &types.AttributeValueMemberN{Value: "0"},
		},
		ProjectionExpression: aws.String("sk"),
		ScanIndexForward:     aws.Bool(false),
		Limit:                aws.Int32(1),
	})
	if err == nil && latest != nil && len(latest.Items) > 0 {
		if sk, ok := latest.Items[0]["sk"].(*types.AttributeValueMemberN); ok {
			if parsed, perr := strconv.ParseInt(sk.Value, 10, 64); perr == nil && evt.EventSequence(parsed) > head {
				head = evt.EventSequence(parsed)
			}
		}
	}

	return &policy.ClassifiedError{
		Class: policy.ClassTransient,
		Err:   &evt.SequenceGapError{EntityID: entityID, After: head, Found: first},
	}
}

// buildContinuationCheck returns the ConditionCheck on the event just before a stream's first new
// event.
func (repo *Repository) buildContinuationCheck(entityID evt.EntityID, previous evt.EventSequence) types.TransactWriteItem {
	return types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
		TableName: &repo.EventsTable,
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: string(entityID)},
			"sk": &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(previous), 10)},
		},
		ConditionExpression: aws.String(ContinuationCondition),
	}}
}
