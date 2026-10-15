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
	"github.com/photon-grove/evt/policy"
	"github.com/photon-grove/evt/result" //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
)

// Commit writes serialized events to the events table. A multi-stream commit (ADR 0004, C1)
// appends to any number of streams in one transaction: every written stream keeps its event-put
// and snapshot-floor protections, and read-only streams are fenced (ADR 0004, C2). The whole
// commit is validated and budgeted before any write; it is never split.
func (repo *Repository) Commit(
	ctx context.Context,
	result evt.SerializedResult,
) error {
	if err := repo.checkFeatureMarkerTable(); err != nil {
		return err
	}

	if len(result.Events) == 0 && len(result.Snapshots) == 0 && len(result.Fences) == 0 {
		return nil
	}

	normalized, err := evt.NormalizeCommit(result)
	if err != nil {
		return err
	}

	// Fences are refused outright for entity types durably declared with retention: TTL deletes
	// reopen fenced slots without leaving a successor snapshot, so the two checks cannot protect
	// them. The manifest is read from the table, not from this instance's configuration.
	if len(normalized.Fences) > 0 {
		manifest, merr := repo.features.Manifest(ctx)
		if merr != nil {
			return merr
		}
		if ferr := evt.CheckFenceRetention(normalized.Fences, manifest); ferr != nil {
			return ferr
		}
	}

	streamItems, layout, err := repo.buildStreamItems(normalized)
	if err != nil {
		return err
	}

	return repo.commitStreamItems(ctx, normalized, streamItems, layout)
}

// commitLayout records where each kind of item sits in the built transaction, so a failed commit
// is classified by the item that failed (the cancellation reason index).
type commitLayout struct {
	// streamIndexes maps each event, floor-check, and snapshot item index to its stream.
	streamIndexes map[int]evt.EntityID
	// fenceIndexes maps each fence-check item index (slot and companion) to its fence.
	fenceIndexes map[int]evt.ReadFence
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

// buildStreamItems builds the transaction items for every written stream and every read fence,
// in the pinned order: per written stream in bytewise stream-ID order its event puts (ascending
// sequence) followed by its snapshot put or floor check; then per fenced read-only stream its
// slot check and snapshot companion. The layout records each item's index for classification.
func (repo *Repository) buildStreamItems(
	result evt.SerializedResult,
) ([]types.TransactWriteItem, commitLayout, error) {
	layout := commitLayout{
		streamIndexes: make(map[int]evt.EntityID),
		fenceIndexes:  make(map[int]evt.ReadFence),
	}

	// Events arrive normalized: grouped per stream, sorted bytewise by stream ID, ascending
	// sequence within each stream.
	snapshotByStream := make(map[evt.EntityID]evt.SerializedSnapshot, len(result.Snapshots))
	for _, snapshot := range result.Snapshots {
		snapshotByStream[snapshot.EntityID] = snapshot
	}

	var items []types.TransactWriteItem

	position := 0
	for position < len(result.Events) {
		streamID := result.Events[position].EntityID
		end := position
		for end < len(result.Events) && result.Events[end].EntityID == streamID {
			end++
		}
		stream := result.Events[position:end]

		puts, _, err := repo.buildEventPutTransactions(stream)
		if err != nil {
			return nil, layout, err
		}
		for _, put := range puts {
			layout.streamIndexes[len(items)] = streamID
			items = append(items, put)
		}

		if snapshot, ok := snapshotByStream[streamID]; ok {
			snapshotPut, err := repo.buildSnapshotPut(stream, snapshot)
			if err != nil {
				return nil, layout, err
			}
			layout.streamIndexes[len(items)] = streamID
			items = append(items, snapshotPut)
		} else {
			floor := repo.buildFloorChecks(stream)
			layout.streamIndexes[len(items)] = streamID
			items = append(items, floor...)
		}

		position = end
	}

	for _, fence := range result.Fences {
		slot, companion, err := repo.buildFenceChecks(fence)
		if err != nil {
			return nil, layout, err
		}
		layout.fenceIndexes[len(items)] = fence
		items = append(items, slot)
		layout.fenceIndexes[len(items)] = fence
		items = append(items, companion)
	}

	return items, layout, nil
}

// buildFenceChecks returns a fenced stream's two ConditionChecks: the slot check on
// (pk, sk = head + 1), and the snapshot companion on (pk, sk = 0). The companion is always
// included — any store instance can snapshot the stream and compaction deletes whatever a
// durable snapshot covers, so no interval configuration durably disables compaction.
func (repo *Repository) buildFenceChecks(fence evt.ReadFence) (types.TransactWriteItem, types.TransactWriteItem, error) {
	slot := types.ConditionCheck{
		TableName: &repo.EventsTable,
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: string(fence.EntityID)},
			"sk": &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(fence.Head)+1, 10)},
		},
		ConditionExpression: aws.String("attribute_not_exists(sk)"),
	}

	companion := types.ConditionCheck{
		TableName: &repo.EventsTable,
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: string(fence.EntityID)},
			"sk": &types.AttributeValueMemberN{Value: "0"},
		},
		ConditionExpression: aws.String("attribute_not_exists(sk) OR eventSeq <= :head"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":head": &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(fence.Head), 10)},
		},
	}

	slotItem := types.TransactWriteItem{ConditionCheck: &slot}
	companionItem := types.TransactWriteItem{ConditionCheck: &companion}

	return slotItem, companionItem, nil
}

// buildSnapshotPut builds the snapshot Put for one stream of a multi-stream commit, carrying the
// floor condition in its own condition: a transaction cannot touch sk = 0 twice.
func (repo *Repository) buildSnapshotPut(
	stream []evt.SerializedEvent,
	snapshot evt.SerializedSnapshot,
) (types.TransactWriteItem, error) {
	item, err := repo.marshalMap(Snapshot{
		PK:            snapshot.EntityID,
		SK:            0,
		Sequence:      snapshot.Sequence,
		EventSequence: snapshot.EventSequence,
		EntityType:    snapshot.EntityType,
		Payload:       string(snapshot.Payload),
		TTL:           repo.ttlFor(snapshot.EntityType),
	})
	if err != nil {
		return types.TransactWriteItem{}, fmt.Errorf("error marshalling item to map: %w", err)
	}

	put := types.Put{
		TableName:           &repo.EventsTable,
		Item:                item,
		ConditionExpression: aws.String(FirstSnapshotCondition),
	}
	// A snapshot sequence below 1 has no valid predecessor: the second branch applies with a
	// negative :seq, and NextSnapshotCondition fails.
	if expected := snapshot.Sequence - 1; expected != 0 {
		put.ConditionExpression = aws.String(NextSnapshotCondition)
		put.ExpressionAttributeValues = map[string]types.AttributeValue{
			":seq":   &types.AttributeValueMemberN{Value: strconv.Itoa(int(expected))},
			":first": &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(stream[0].Sequence), 10)},
		}
	}

	return types.TransactWriteItem{Put: &put}, nil
}

// CommitStream drains a channel of serialized results, committing each batch as one transaction.
// It carries one floor check per entity (see FloorCondition), built when the batch is flushed: a
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

		if err = evt.CheckReservedEntityIDs(serializedResult.Events); err != nil {
			errs = append(errs, err)
			continue
		}

		if err = checkContiguous(serializedResult.Events); err != nil {
			errs = append(errs, err)
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

	if len(result.Events) == 0 {
		return errors.New("dynamo: commit with snapshot requires at least one event")
	}

	if err := evt.CheckReservedEntityID(entityID); err != nil {
		return err
	}

	// Route through the multi-stream commit path: the snapshot is written in the same transaction
	// as its events, carrying the floor condition in its own condition.
	result.Snapshots = append(result.Snapshots, evt.SerializedSnapshot{
		EntityType:    entityType,
		EntityID:      entityID,
		Sequence:      currentSnapshot,
		EventSequence: result.Events[len(result.Events)-1].Sequence,
		Payload:       payload,
	})

	return repo.Commit(ctx, result)
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
	return repo.commitTransactionItems(ctx, transaction, writeItems, commitLayout{})
}

// commitStreamItems sends one multi-stream commit: the feature-marker check, the projector
// operations, then the stream and fence items, budgeted as a whole and never split.
func (repo *Repository) commitStreamItems(
	ctx context.Context,
	result evt.SerializedResult,
	streamItems []types.TransactWriteItem,
	layout commitLayout,
) error {
	transactItems, err := toDynamoItems(result.Transaction)
	if err != nil {
		return err
	}

	// The preflight covers every item the transaction carries, with the projector operations
	// sized exactly from the items this backend will write.
	sizes, serr := evt.TransactionItemSizesWithTTL(result, repo.ttlFor)
	if serr != nil {
		return serr
	}
	for _, item := range transactItems {
		sizes = append(sizes, transactItemSize(item))
	}
	if berr := evt.CommitBudget(sizes); berr != nil {
		return berr
	}

	return repo.commitTransactionItems(ctx, result.Transaction, streamItems, layout)
}

// commitTransactionItems assembles the final transaction — feature-marker check first, then the
// projector operations, then the stream and fence items — checks the full budget, sends it, and
// classifies a failure by the item that failed.
func (repo *Repository) commitTransactionItems(
	ctx context.Context,
	transaction evt.Transaction,
	streamItems []types.TransactWriteItem,
	layout commitLayout,
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

	combined := make([]types.TransactWriteItem, 0, len(control)+len(transactItems)+len(streamItems))
	combined = append(combined, control...)
	combined = append(combined, transactItems...)
	combined = append(combined, streamItems...)

	count := len(combined)
	if count > 100 {
		// Limit increased from 25 to 100:
		// https://aws.amazon.com/about-aws/whats-new/2022/09/amazon-dynamodb-supports-100-actions-per-transaction/
		return &evt.CommitTooLargeError{Items: count, Bytes: math.MaxInt, LargestItem: math.MaxInt}
	}

	input := dynamodb.TransactWriteItemsInput{TransactItems: combined}
	_, err = repo.client.TransactWriteItems(ctx, &input)
	if err != nil {
		return repo.classifyCommitFailure(ctx, transaction, transactItems, layout, err)
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
	layout commitLayout,
	err error,
) error {
	ok, i := HasConditionalCheckFailure(err)
	if !ok {
		return wrapClassifiedError(err)
	}

	if i == 0 {
		return repo.features.handleCheckFailure(ctx, err)
	}

	// The layout's indexes are relative to the stream items, which sit after the control items
	// and the projector operations in the combined transaction.
	streamIndex := i - len(transactItems) - 1
	if fence, failed := layout.fenceIndexes[streamIndex]; failed {
		return policy.FenceConflict(fence)
	}

	// A failed condition on a written stream's own items is an ordinary conflict.
	if _, written := layout.streamIndexes[streamIndex]; written {
		return wrapClassifiedError(ErrSnapshotRaceCondition)
	}

	// Projector items sit between the control items and the stream items.
	offset := 1
	if i-offset < len(transactItems) {
		for _, group := range transaction {
			if i-offset < group.Len() {
				return group.HandleError(err, i-offset)
			}
			offset += group.Len()
		}
		return wrapClassifiedError(err)
	}

	return wrapClassifiedError(ErrSnapshotRaceCondition)
}
