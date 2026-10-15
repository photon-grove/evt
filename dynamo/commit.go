package dynamo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/internal/eventjson"
	"github.com/photon-grove/evt/result" //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
)

// Commit writes serialized events to the events table.
func (repo *Repository) Commit(
	ctx context.Context,
	result evt.SerializedResult,
) error {
	if err := repo.checkFeatureMarkerTable(); err != nil {
		return err
	}

	if len(result.Events) == 0 {
		return nil
	}

	if err := evt.CheckReservedEntityIDs(result.Events); err != nil {
		return err
	}

	if err := checkContiguous(result.Events); err != nil {
		return err
	}

	// Generate Put transactions
	writeItems, _, err := repo.buildEventPutTransactions(result.Events)
	if err != nil {
		return err
	}

	writeItems = append(writeItems, repo.buildFloorChecks(result.Events)...)

	// Commit the transactions to DynamoDB
	return repo.commitEventsWithTransaction(ctx, writeItems, result.Transaction)
}

// FloorCondition rejects a commit whose first event is at or below the stored snapshot's
// eventSeq. Compaction deletes the events a snapshot covers, so attribute_not_exists(sk) alone
// would let a writer that loaded before the snapshot recreate a compacted sequence.
const FloorCondition = "attribute_not_exists(eventSeq) OR eventSeq < :first"

// FirstSnapshotCondition guards the first snapshot put in a commit (snapshot sequence 1): no
// snapshot exists yet.
const FirstSnapshotCondition = "attribute_not_exists(seq)"

// NextSnapshotCondition guards later snapshot puts in a commit: the stored snapshot is still the one
// the writer loaded (:seq), and the commit starts above its floor (:first).
const NextSnapshotCondition = "seq = :seq AND eventSeq < :first"

// checkContiguous rejects events whose sequences are not contiguous (each one more than the last)
// for each entity. The floor check reads the first event and the snapshot records the last, so a
// commit like [6, 5] after compacting through 5 would recreate sequence 5, and loads reject a
// stream with a gap such as [1, 3]. Sequences start at 1: DynamoDB keeps the snapshot at sk = 0 and
// reads events after it.
func checkContiguous(events []evt.SerializedEvent) error {
	last := make(map[evt.EntityID]evt.EventSequence, 1)

	for _, event := range events {
		previous, seen := last[event.EntityID]
		if !seen && event.Sequence < 1 {
			return fmt.Errorf("dynamo: event %s has sequence %d; sequences start at 1", event.ID, event.Sequence)
		}
		// Nothing follows the maximum sequence. Checking it first keeps previous+1 from wrapping.
		if seen && (previous == math.MaxInt || event.Sequence != previous+1) {
			return fmt.Errorf("dynamo: event %s follows sequence %d; sequences must be contiguous", event.ID, previous)
		}
		last[event.EntityID] = event.Sequence
	}

	return nil
}

// buildFloorChecks returns one ConditionCheck on each entity's snapshot item (sk = 0) that the
// commit starts above the snapshot's eventSeq.
func (repo *Repository) buildFloorChecks(events []evt.SerializedEvent) []types.TransactWriteItem {
	first := make(map[evt.EntityID]evt.EventSequence)
	order := make([]evt.EntityID, 0, 1)

	for _, event := range events {
		current, seen := first[event.EntityID]
		if !seen {
			order = append(order, event.EntityID)
		}
		if !seen || event.Sequence < current {
			first[event.EntityID] = event.Sequence
		}
	}

	checks := make([]types.TransactWriteItem, 0, len(order))
	for _, entityID := range order {
		checks = append(checks, types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
			TableName: &repo.EventsTable,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: string(entityID)},
				"sk": &types.AttributeValueMemberN{Value: "0"},
			},
			ConditionExpression: aws.String(FloorCondition),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":first": &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(first[entityID]), 10)},
			},
		}})
	}

	return checks
}

// CommitStream commits a stream of events using batches under the 100-item limit. Each batch
// carries one floor check per entity (see FloorCondition), built when the batch is flushed: a
// transaction cannot touch an entity's snapshot item twice.
//
// Deprecated: Cut in ADR-0006 (K1). Commit batched commands through the normal commit path
// instead. It will be removed in the next minor release.
func (repo *Repository) CommitStream(
	ctx context.Context,
	channel <-chan result.Result[evt.SerializedResult],
) []error {
	if err := repo.checkFeatureMarkerTable(); err != nil {
		// Drain the channel so producers never block on a refused stream.
		for range channel { //nolint:revive // Drain without reading so producers never block.
			continue
		}

		return []error{err}
	}

	var errs []error

	var (
		batch       []types.TransactWriteItem
		batchEvents []evt.SerializedEvent
	)

	// Every batch reserves one item for the mandatory feature-marker check.
	const markerItems = 1

	flush := func() {
		if len(batch) == 0 {
			return
		}

		items := append(batch, repo.buildFloorChecks(batchEvents)...)
		if err := repo.commitEventsWithTransaction(ctx, items, nil); err != nil {
			errs = append(errs, err)
		}

		batch, batchEvents = nil, nil
	}

	for result := range channel {
		serializedResult, err := result.Unwrap()
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if rerr := evt.CheckReservedEntityIDs(serializedResult.Events); rerr != nil {
			errs = append(errs, rerr)
			continue
		}

		if cerr := checkContiguous(serializedResult.Events); cerr != nil {
			errs = append(errs, cerr)
			continue
		}

		items, _, err := repo.buildEventPutTransactions(serializedResult.Events)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// Keep the batches under 100 items total, with transactions included
		transactItems, err := toDynamoItems(serializedResult.Transaction)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// Combine the two, with included transaction items first
		combined := append(transactItems, items...)

		// Guard against a single result that already exceeds the 100-item transaction limit
		if size := len(combined) + countEntities(serializedResult.Events) + markerItems; size > 100 {
			errs = append(errs, fmt.Errorf(
				"single result produces %d items, exceeding DynamoDB 100-item transaction limit",
				size,
			))
			continue
		}

		// Flush first if adding this result, and its floor checks, would overrun the limit
		merged := append(append([]evt.SerializedEvent{}, batchEvents...), serializedResult.Events...)
		if len(batch)+len(combined)+countEntities(merged)+markerItems > 100 {
			flush()
		}

		batch = append(batch, combined...)
		batchEvents = append(batchEvents, serializedResult.Events...)
	}

	// Flush the final items from the batch
	flush()

	return errs
}

// countEntities returns the number of distinct entities, which is the number of floor checks a
// batch of these events needs.
func countEntities(events []evt.SerializedEvent) int {
	seen := make(map[evt.EntityID]struct{}, len(events))
	for _, event := range events {
		seen[event.EntityID] = struct{}{}
	}

	return len(seen)
}

// CommitWithSnapshot commits events and a snapshot in a single transaction.
func (repo *Repository) CommitWithSnapshot(
	ctx context.Context,
	result evt.SerializedResult,
	entityType evt.EntityType,
	entityID evt.EntityID,
	payload []byte,
	currentSnapshot evt.EventSequence,
) error {
	if err := repo.checkFeatureMarkerTable(); err != nil {
		return err
	}

	if err := evt.CheckReservedEntityID(entityID); err != nil {
		return err
	}

	if err := evt.CheckReservedEntityIDs(result.Events); err != nil {
		return err
	}

	if err := checkContiguous(result.Events); err != nil {
		return err
	}

	// Generate both the regular Put transactions, and a Put for the Snapshot
	writeItems, err := repo.buildSnapshotPutTransactions(result.Events, entityType, entityID, payload, currentSnapshot)
	if err != nil {
		return err
	}

	// Commit the transactions to DynamoDB
	return repo.commitEventsWithTransaction(ctx, writeItems, result.Transaction)
}

// Generate the TransactWriteItems needed to Put new Events to the DynamoDB table
func (repo *Repository) buildEventPutTransactions(serializedEvents []evt.SerializedEvent) ([]types.TransactWriteItem, evt.EventSequence, error) {
	currentSequence := evt.EventSequence(0)
	transactions := make([]types.TransactWriteItem, 0, len(serializedEvents))

	for _, event := range serializedEvents {
		// Keep track of the latest Event sequence number
		currentSequence = event.Sequence

		// Do the same for the Metadata
		metadataBytes, err := eventjson.MarshalMetadata(event.Metadata)
		if err != nil {
			return nil, currentSequence, err
		}

		// Create the serialized representation of a DynamoDB Event. TTL is non-zero only when the
		// entity type has a retention policy; otherwise omitempty drops the attribute entirely.
		evt := Event{
			PK:         event.EntityID,
			SK:         event.Sequence,
			EntityType: event.EntityType,
			Version:    event.Version,
			Type:       event.Type,
			Payload:    string(event.Payload),
			Metadata:   string(metadataBytes),
			TTL:        repo.ttlFor(event.EntityType),
		}

		// Convert to a map of AttributeValues
		item, err := repo.marshalMap(evt)
		if err != nil {
			return nil, currentSequence, fmt.Errorf("error marshalling item to map: %w", err)
		}

		// Include it within a TransactWriteItem statement that ensures the given sequence
		// number doesn't already exist (optimistic locking)
		putItem := types.Put{
			TableName:           &repo.EventsTable,
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(sk)"),
		}

		transactions = append(transactions, types.TransactWriteItem{Put: &putItem})
	}

	return transactions, currentSequence, nil
}

// Generate the TransactWriteItems needed to Put new Snapshots to the DynamoDB table
func (repo *Repository) buildSnapshotPutTransactions(
	serializedEvents []evt.SerializedEvent,
	entityType evt.EntityType,
	entityID evt.EntityID,
	payload []byte,
	currentSnapshot evt.EventSequence,
) ([]types.TransactWriteItem, error) {
	if len(serializedEvents) == 0 {
		return nil, errors.New("commit with snapshot requires at least one event")
	}

	expectedSnapshot := evt.EventSequence(int(currentSnapshot) - 1)

	// Generate the initial Put transactions that are needed to commit the Events themselves
	transactions, currentSequence, err := repo.buildEventPutTransactions(serializedEvents)
	if err != nil {
		return nil, err
	}

	// Create the serialized representation of a DynamoDB Snapshot (inline at sk=0). TTL mirrors the
	// events' policy so a snapshot never outlives the stream it summarizes; omitempty drops it for
	// un-policed entity types.
	event := Snapshot{
		PK:            entityID,
		SK:            0,
		Sequence:      currentSnapshot,
		EventSequence: currentSequence,
		EntityType:    entityType,
		Payload:       string(payload),
		TTL:           repo.ttlFor(entityType),
	}

	// Convert to a map of AttributeValues
	item, err := repo.marshalMap(event)
	if err != nil {
		return nil, fmt.Errorf("error marshalling item to map: %w", err)
	}

	// Include it within a TransactWriteItem statement that ensures the existing Snapshot matches
	// the previous Snapshot number (none for the first) and that the commit starts above its floor.
	// A separate floor ConditionCheck is not possible here: a transaction cannot touch sk=0 twice.
	// Snapshots are stored inline in the event-log table at sk=0.
	// A snapshot sequence below 1 has no valid predecessor, so it takes the second branch and fails
	// its condition.
	putItem := types.Put{
		TableName:           &repo.EventsTable,
		Item:                item,
		ConditionExpression: aws.String(FirstSnapshotCondition),
	}
	if expectedSnapshot != 0 {
		putItem.ConditionExpression = aws.String(NextSnapshotCondition)
		putItem.ExpressionAttributeValues = map[string]types.AttributeValue{
			":seq":   &types.AttributeValueMemberN{Value: strconv.Itoa(int(expectedSnapshot))},
			":first": &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(serializedEvents[0].Sequence), 10)},
		}
	}

	transactions = append(transactions, types.TransactWriteItem{Put: &putItem})

	return transactions, nil
}

// Commit the given TransactWriteItems to the DynamoDB table
func (repo *Repository) commitEventsWithTransaction(
	ctx context.Context,
	writeItems []types.TransactWriteItem,
	transaction evt.Transaction,
) error {
	transactItems, err := toDynamoItems(transaction)
	if err != nil {
		return err
	}

	// Every v0.9 commit carries the feature-marker ConditionCheck, ordered before every other
	// item so the transaction layout stays pinned across implementations (storage spec, Writes).
	check, cerr := repo.features.checkItem(ctx)
	if cerr != nil {
		return cerr
	}

	control := []types.TransactWriteItem{check}

	// Combine the two, with included transaction items first
	combined := make([]types.TransactWriteItem, 0, len(control)+len(transactItems)+len(writeItems))
	combined = append(combined, control...)
	combined = append(combined, transactItems...)
	combined = append(combined, writeItems...)

	count := len(combined)
	if count > 100 {
		// Limit increased from 25 to 100:
		// https://aws.amazon.com/about-aws/whats-new/2022/09/amazon-dynamodb-supports-100-actions-per-transaction/
		return fmt.Errorf("too many operations: %d, DynamoDb supports only up to 100 operations per transactions", count)
	}

	input := dynamodb.TransactWriteItemsInput{TransactItems: combined}
	_, err = repo.client.TransactWriteItems(ctx, &input)
	if err != nil {
		return repo.classifyCommitFailure(ctx, transaction, transactItems, len(control), err)
	}

	return nil
}

// classifyCommitFailure routes a failed commit by the transaction item that failed. Control items
// (the feature-marker check) are prepended to the transaction, so their failure is classified
// separately and the remaining indices shift by their count.
func (repo *Repository) classifyCommitFailure(
	ctx context.Context,
	transaction evt.Transaction,
	transactItems []types.TransactWriteItem,
	controlCount int,
	err error,
) error {
	if ok, i := HasConditionalCheckFailure(err); ok && i < controlCount {
		return repo.features.handleCheckFailure(ctx, err)
	}

	return repo.handleConditionalCheckFailure(ctx, transaction, transactItems, controlCount, err)
}
