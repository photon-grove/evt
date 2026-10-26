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

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/internal/eventjson"
)

// Commit writes serialized events to the events table. A multi-stream commit (ADR 0004, C1)
// appends to any number of streams in one transaction. Each written stream is protected by one
// conditional update of its head item (ADR 0007) and read-only streams are fenced by one check of
// theirs (ADR 0004, C2). The whole commit is validated and budgeted before any write; it is never
// split.
func (repo *Repository) Commit(
	ctx context.Context,
	result evt.SerializedResult,
) error {
	if err := repo.checkFeatureMarkerTable(); err != nil {
		return err
	}

	// Projector operations need a backing event in the same commit (ADR-0012); refuse them even
	// when nothing else would be written.
	if err := evt.CheckProjectorOperations(result); err != nil {
		return err
	}

	if len(result.Events) == 0 && len(result.Snapshots) == 0 && len(result.Fences) == 0 && result.Outcome == nil {
		return nil
	}

	normalized, err := evt.NormalizeCommit(result)
	if err != nil {
		return err
	}

	// Fences are refused outright for entity types with retention: a retained stream's head item
	// carries a ttl, and TTL deletes are lazy, so a fence on it is not incarnation-proof. The
	// refusal is the union of this repository's own WithRetention configuration, which needs no
	// I/O, and the durable manifest read from the table, which binds writers configured
	// differently. Declare retained types in the marker too, so other processes refuse them.
	if len(normalized.Fences) > 0 {
		retained := func(entityType evt.EntityType) bool { return repo.retention[entityType] > 0 }
		if ferr := evt.CheckFenceRetentionWith(normalized.Fences, nil, retained); ferr != nil {
			return ferr
		}

		manifest, merr := repo.features.Manifest(ctx)
		if merr != nil {
			return merr
		}
		if ferr := evt.CheckFenceRetentionWith(normalized.Fences, manifest, retained); ferr != nil {
			return ferr
		}
	}

	streamItems, layout, err := repo.buildStreamItems(normalized)
	if err != nil {
		return err
	}

	return repo.commitStreamItems(ctx, normalized, streamItems, layout)
}

// AtomicTransactions declares that Commit writes every stream, snapshot, and fence of a result in
// one TransactWriteItems call (evt.TransactionalRepository).
func (repo *Repository) AtomicTransactions() {}

// commitLayout records where each kind of item sits in the built transaction, so a failed commit
// is classified by the item that failed (the cancellation reason index). Every index is relative
// to the stream items, which follow the control items and the projector operations.
type commitLayout struct {
	// streams are the written streams in transaction order.
	streams []streamLayout
	// fences are the fence checks in transaction order.
	fences []fenceLayout
	// views are the commit-wide view operations, which follow the transaction groups' operations
	// and precede the stream items.
	views []evt.ProjectedView
	// outcomeIndex is the outcome put's index among the stream items; outcomeID is its command.
	// The outcome put is the last action of the transaction.
	outcomeIndex int
	hasOutcome   bool
	outcomeID    evt.CommandID
}

// buildStreamItems builds the transaction items for every written stream and every read fence,
// in the pinned order (storage spec, Writes): per written stream in bytewise stream-ID order its
// head update, its event puts (ascending sequence), then its snapshot put when the commit writes
// one; then one head check per fenced read-only stream in bytewise order; then the outcome put.
// The layout records each item's index for classification.
func (repo *Repository) buildStreamItems(
	result evt.SerializedResult,
) ([]types.TransactWriteItem, commitLayout, error) {
	var layout commitLayout

	// Events arrive normalized: grouped per stream, sorted bytewise by stream ID, ascending
	// sequence within each stream. One ttl per entity type serves the whole commit.
	ttls := repo.commitTTLs(result)
	ttlOf := func(entityType evt.EntityType) int64 { return ttls[entityType] }

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

		first, last := stream[0].Sequence, stream[len(stream)-1].Sequence
		entityType := stream[0].EntityType

		placed := streamLayout{
			id:            streamID,
			entityType:    entityType,
			first:         first,
			headIndex:     len(items),
			snapshotIndex: -1,
		}

		items = append(items, repo.buildHeadUpdate(streamID, first, last, repo.headTTL(streamID, first, ttls[entityType])))

		puts, _, err := repo.buildEventPutTransactions(stream, ttlOf)
		if err != nil {
			return nil, layout, err
		}

		placed.eventsStart = len(items)
		items = append(items, puts...)
		placed.eventsEnd = len(items)

		if snapshot, ok := snapshotByStream[streamID]; ok {
			snapshotPut, err := repo.buildSnapshotPut(snapshot, ttlOf)
			if err != nil {
				return nil, layout, err
			}

			placed.snapshotIndex = len(items)
			items = append(items, snapshotPut)
		}

		layout.streams = append(layout.streams, placed)
		position = end
	}

	for _, fence := range result.Fences {
		layout.fences = append(layout.fences, fenceLayout{index: len(items), fence: fence})
		items = append(items, repo.buildFenceCheck(fence))
	}

	// The outcome row is the last action: its condition fails only when another writer recorded
	// this command ID first, which the engine resolves by reading the row.
	if result.Outcome != nil {
		put, err := repo.buildOutcomePut(*result.Outcome)
		if err != nil {
			return nil, layout, err
		}

		layout.outcomeIndex, layout.hasOutcome, layout.outcomeID = len(items), true, result.Outcome.CommandID
		items = append(items, put)
	}

	return items, layout, nil
}

// buildSnapshotPut builds the snapshot Put for one stream of a multi-stream commit. Its condition
// is the sequence chain only: continuity and the floor belong to the head update, because the head
// condition head = first - 1 and invariant H3 (a snapshot never covers more than the head) imply
// snapshot.eventSeq < first.
func (repo *Repository) buildSnapshotPut(
	snapshot evt.SerializedSnapshot,
	ttlOf func(evt.EntityType) int64,
) (types.TransactWriteItem, error) {
	item, err := repo.marshalMap(Snapshot{
		PK:            snapshot.EntityID,
		SK:            0,
		Sequence:      snapshot.Sequence,
		EventSequence: snapshot.EventSequence,
		EntityType:    snapshot.EntityType,
		Payload:       string(snapshot.Payload),
		TTL:           ttlOf(snapshot.EntityType),
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
			":seq": &types.AttributeValueMemberN{Value: strconv.Itoa(int(expected))},
		}
	}

	return types.TransactWriteItem{Put: &put}, nil
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

	if err := evt.CheckProjectorOperations(result); err != nil {
		return err
	}

	if len(result.Events) == 0 {
		return errors.New("dynamo: commit with snapshot requires at least one event")
	}

	if err := evt.CheckReservedEntityID(entityID); err != nil {
		return err
	}

	// Route through the multi-stream commit path: the snapshot is written in the same transaction
	// as its events, after the stream's head update.
	result.Snapshots = append(result.Snapshots, evt.SerializedSnapshot{
		EntityType:    entityType,
		EntityID:      entityID,
		Sequence:      currentSnapshot,
		EventSequence: result.Events[len(result.Events)-1].Sequence,
		Payload:       payload,
	})

	return repo.Commit(ctx, result)
}

// Generate the TransactWriteItems needed to Put new Events to the DynamoDB table. ttlOf supplies
// the ttl an event of an entity type carries (0 for none).
func (repo *Repository) buildEventPutTransactions(
	serializedEvents []evt.SerializedEvent,
	ttlOf func(evt.EntityType) int64,
) ([]types.TransactWriteItem, evt.EventSequence, error) {
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
			TTL:        ttlOf(event.EntityType),
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

	viewItems, err := repo.buildViewItems(result.Views)
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
	for _, item := range viewItems {
		sizes = append(sizes, transactItemSize(item))
	}
	if berr := evt.CommitBudget(sizes); berr != nil {
		return berr
	}

	layout.views = result.Views

	return repo.commitTransactionItems(ctx, result.Transaction, streamItems, layout)
}

// commitTransactionItems assembles the final transaction — feature-marker check first, then the
// projector operations (the transaction groups', then the commit-wide views'), then the stream and
// fence items — checks the full budget, sends it, and classifies a failure by the item that failed.
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

	viewItems, err := repo.buildViewItems(layout.views)
	if err != nil {
		return err
	}

	// Every v0.9 commit carries the feature-marker ConditionCheck, ordered before every other
	// item so the transaction layout stays pinned across implementations (storage spec, Writes).
	check, cerr := repo.features.checkItem(ctx)
	if cerr != nil {
		return cerr
	}

	// The rebuild-lock check (C10, action group 2) sits right after the marker check and before
	// the projector operations: it fails whenever any rebuild holds the lock, an expired one
	// included (storage spec, Writes).
	control := []types.TransactWriteItem{check, repo.rebuildLockCheck()}

	combined := make([]types.TransactWriteItem, 0, len(control)+len(transactItems)+len(viewItems)+len(streamItems))
	combined = append(combined, control...)
	combined = append(combined, transactItems...)
	combined = append(combined, viewItems...)
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

// controlItems is the number of control checks every commit starts with: the feature-marker check
// and the rebuild-lock check (storage spec, Writes). The transaction groups' operations, the view
// operations, and the stream items follow them.
const controlItems = 2

// classifyCommitFailure routes a failed commit by the transaction items that failed, ranked
// marker, rebuild lock, outcome put, stream head drift, sequence gap, any other conflict on a
// stream or a fence, then projector operations. Control items are prepended to the transaction,
// so their failure is classified separately and the remaining indices shift by their count. The
// projector items — the transaction groups', then the commit-wide views' — sit between the control
// items and the stream items.
func (repo *Repository) classifyCommitFailure(
	ctx context.Context,
	transaction evt.Transaction,
	transactItems []types.TransactWriteItem,
	layout commitLayout,
	err error,
) error {
	ok, i := HasConditionalCheckFailure(err)
	if !ok {
		// A view item can fail for a reason that is not a condition (an invalid item); no retry can
		// fix it, so it is the projector's typed error.
		if failure := viewReasonFailure(layout.views, controlItems+len(transactItems), cancellationCodes(err), err); failure != nil {
			return failure
		}

		// Another transaction was writing the same items: an optimistic-concurrency conflict, as
		// in Rust, so fence-aware retry re-prepares the command.
		if hasTransactionConflict(err) {
			return wrapClassifiedError(fmt.Errorf("%w: %w", ErrTransactionConflict, err))
		}

		// A commit that carries an outcome and fails indeterminately is resolved by re-reading the
		// outcome row (ambiguous-write recovery); other failures are definitive refusals.
		if layout.hasOutcome && indeterminateCommit(err) {
			return &evt.AmbiguousWriteError{CommandID: layout.outcomeID, Cause: err}
		}

		return wrapClassifiedError(err)
	}

	if i == 0 {
		return repo.features.handleCheckFailure(ctx, err)
	}

	codes := cancellationCodes(err)

	// A failed rebuild-lock check outranks everything below it: a rebuild holds the lock, so the
	// commit is refused whatever else its cancellation reasons say, and no failure-path probe runs.
	// It is transient (the lock may be released, taken over, or force-released) and never a
	// conflict: fence-aware retry runs the command again after a backoff and returns this error
	// when the attempts run out.
	if conditionFailed(codes, 1) {
		return wrapClassifiedError(&evt.RebuildInProgressError{})
	}

	// The layout's indexes are relative to the stream items, which sit after the control items
	// and the projector operations in the combined transaction.
	viewBase := controlItems + len(transactItems)
	base := viewBase + len(layout.views)

	// A lost outcome put outranks every other item's failure but the marker: the command already
	// has an outcome, and re-preparing it as a generic conflict can never resolve that. Reading the
	// row does.
	if layout.hasOutcome && conditionFailed(codes, base+layout.outcomeIndex) {
		return &evt.OutcomeConflictError{CommandID: layout.outcomeID}
	}

	// Drift, then a gap, then any other conflict on a written stream or a fence (storage spec,
	// Stream Heads, Classification). Every failed stream and fence is evaluated before ranking.
	if classified := repo.classifyStreamFailures(ctx, layout, codes, base); classified != nil {
		return classified
	}

	// Only projector operations failed: the first in transaction order is reported, so a projector
	// condition surfaces only when nothing ranked above it did.
	if i-controlItems < len(transactItems) {
		offset := controlItems
		for _, group := range transaction {
			if i-offset < group.Len() {
				return group.HandleError(err, i-offset)
			}
			offset += group.Len()
		}

		return wrapClassifiedError(err)
	}

	if failure := viewReasonFailure(layout.views, viewBase, codes, err); failure != nil {
		return failure
	}

	return wrapClassifiedError(ErrSnapshotRaceCondition)
}

// viewReasonFailure returns the typed error of the first view operation, in transaction order,
// whose item failed for a reason a retry cannot fix, or nil. base is the index of the first view
// item in the combined transaction.
func viewReasonFailure(views []evt.ProjectedView, base int, codes []string, cause error) error {
	for index, view := range views {
		if base+index >= len(codes) {
			break
		}

		if failure := viewFailure(view, codes[base+index], cause); failure != nil {
			return failure
		}
	}

	return nil
}
