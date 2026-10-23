package dynamo

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/internal/streamheads"
)

// The per-stream head item (ADR 0007; storage spec, Stream Heads) lives at (pk, sk = -1) and holds
// `head`, the highest event sequence ever committed to the stream, and, for entity types with
// retention, a `ttl` at least that of every other item of the stream. Every commit conditions on
// it for concurrency instead of on a continuation check, a floor check, or a fence's slot check
// and snapshot companion.
//
// The condition text is shared with the byte budget and the compatibility harness through
// internal/streamheads.

// Conditions of the commit layout, exported so tooling and tests can name them. Snapshot puts in a
// commit carry the sequence chain only; the head update owns continuity and the floor.
const (
	// HeadCreateCondition guards a stream's first commit: no head item exists yet.
	HeadCreateCondition = streamheads.CreateCondition

	// HeadAppendCondition guards an append: the head is still the sequence before the commit's first.
	HeadAppendCondition = streamheads.AppendCondition

	// FirstSnapshotCondition guards the first snapshot put in a commit (snapshot sequence 1): no
	// snapshot exists yet.
	FirstSnapshotCondition = "attribute_not_exists(seq)"

	// NextSnapshotCondition guards later snapshot puts in a commit: the stored snapshot is still the
	// one the writer loaded (:seq).
	NextSnapshotCondition = "seq = :seq"
)

// headKey is the key of a stream's head item.
func (repo *Repository) headKey(entityID evt.EntityID) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: string(entityID)},
		"sk": &types.AttributeValueMemberN{Value: streamheads.SortKeyText},
	}
}

func numberValue(n int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(n, 10)}
}

// buildHeadUpdate builds a written stream's head update, the first action of its group. A stream
// that starts at sequence 1 creates its head item under attribute_not_exists(head); one that
// appends conditions on head = first - 1, so the commit continues exactly from the stored head and
// nothing is skipped. For an entity type with retention (ttl != 0) the update also raises the head
// item's ttl, and its condition makes lowering impossible: if the stored ttl is already above
// ttl the update fails as an ordinary conflict and the retry, which reloads the stream and so
// learns the larger ttl, passes (storage spec, Stream Heads, the never-lower rule).
func (repo *Repository) buildHeadUpdate(
	entityID evt.EntityID,
	first, last evt.EventSequence,
	ttl int64,
) types.TransactWriteItem {
	update := &types.Update{
		TableName:           &repo.EventsTable,
		Key:                 repo.headKey(entityID),
		UpdateExpression:    aws.String(streamheads.UpdateExpression),
		ConditionExpression: aws.String(streamheads.CreateCondition),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":last": numberValue(int64(last)),
		},
	}

	if first > 1 {
		update.ConditionExpression = aws.String(streamheads.AppendCondition)
		update.ExpressionAttributeValues[":prev"] = numberValue(int64(first) - 1)
	}

	if ttl != 0 {
		update.UpdateExpression = aws.String(streamheads.RetainedUpdateExpression)
		update.ExpressionAttributeNames = map[string]string{streamheads.TTLPlaceholder: streamheads.TTLName}
		update.ExpressionAttributeValues[":t"] = numberValue(ttl)

		if first > 1 {
			update.ConditionExpression = aws.String(streamheads.RetainedAppendCondition)
		}
	}

	return types.TransactWriteItem{Update: update}
}

// buildFenceCheck builds a fenced read-only stream's single ConditionCheck on its head item: the
// stream must still be at the head the reader saw (head = :head), or still have no head item when
// the reader saw no events. Because the head only increases, a matching value cannot have been
// reached by a different history, so no snapshot companion is needed (the compaction ABA does not
// exist).
func (repo *Repository) buildFenceCheck(fence evt.ReadFence) types.TransactWriteItem {
	check := &types.ConditionCheck{
		TableName:           &repo.EventsTable,
		Key:                 repo.headKey(fence.EntityID),
		ConditionExpression: aws.String(streamheads.FenceAbsentCondition),
	}

	if fence.Head >= 1 {
		check.ConditionExpression = aws.String(streamheads.FenceCondition)
		check.ExpressionAttributeValues = map[string]types.AttributeValue{
			":head": numberValue(int64(fence.Head)),
		}
	}

	return types.TransactWriteItem{ConditionCheck: check}
}

// buildSnapshotGuard builds the head item's action in an out-of-band snapshot write's transaction
// (invariant H3: a snapshot never covers more than the head). refreshTTL is true for an entity
// type with retention, whose head item the write must also keep alive: the snapshot is stamped
// with a ttl at call time and would otherwise outlive the head item.
//
//   - eventSequence > 0, no retention: a ConditionCheck with head >= :new.
//   - eventSequence > 0, retention: an Update with head >= :new that raises the ttl, never lowering it.
//   - eventSequence == 0: the head item may be absent (a snapshot of an empty stream), so it is a
//     ConditionCheck with attribute_not_exists(head) OR head >= :new even for a retained type,
//     because an Update would create a head item with no head attribute.
func (repo *Repository) buildSnapshotGuard(
	entityID evt.EntityID,
	eventSequence evt.EventSequence,
	ttl int64,
	refreshTTL bool,
) types.TransactWriteItem {
	values := map[string]types.AttributeValue{":new": numberValue(int64(eventSequence))}

	if eventSequence == 0 {
		return types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
			TableName:                 &repo.EventsTable,
			Key:                       repo.headKey(entityID),
			ConditionExpression:       aws.String(streamheads.GuardEmptyCondition),
			ExpressionAttributeValues: values,
		}}
	}

	if !refreshTTL || ttl == 0 {
		return types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
			TableName:                 &repo.EventsTable,
			Key:                       repo.headKey(entityID),
			ConditionExpression:       aws.String(streamheads.GuardCondition),
			ExpressionAttributeValues: values,
		}}
	}

	values[":t"] = numberValue(ttl)

	return types.TransactWriteItem{Update: &types.Update{
		TableName:                 &repo.EventsTable,
		Key:                       repo.headKey(entityID),
		UpdateExpression:          aws.String(streamheads.GuardRetainedUpdateExpression),
		ConditionExpression:       aws.String(streamheads.GuardRetainedCondition),
		ExpressionAttributeNames:  map[string]string{streamheads.TTLPlaceholder: streamheads.TTLName},
		ExpressionAttributeValues: values,
	}}
}

// retained reports whether this repository stamps a ttl on entities of the type.
func (repo *Repository) retained(entityType evt.EntityType) bool {
	return repo != nil && repo.retention[entityType] > 0
}

// ttlFloors remembers, per stream, the largest ttl among the items this repository loaded for it.
// A retained stream's head item carries a ttl at least that of every other item and is never
// lowered, so a commit raises the head ttl to the larger of its own ttl and what the writer saw;
// the load API does not expose item ttls, so the repository observes them as it decodes. The map
// is bounded: when it is full the oldest knowledge is dropped wholesale, which costs at most one
// extra conflict-and-retry.
type ttlFloors struct {
	mu     sync.Mutex
	floors map[evt.EntityID]int64
}

// maxTTLFloors bounds the per-repository ttl memory.
const maxTTLFloors = 4096

func newTTLFloors() *ttlFloors {
	return &ttlFloors{floors: make(map[evt.EntityID]int64)}
}

// observe records a loaded item's ttl for its stream. A ttl of 0 (no ttl attribute) is ignored.
func (f *ttlFloors) observe(entityID evt.EntityID, ttl int64) {
	if f == nil || ttl <= 0 {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if current, ok := f.floors[entityID]; ok {
		if ttl > current {
			f.floors[entityID] = ttl
		}

		return
	}

	if len(f.floors) >= maxTTLFloors {
		clear(f.floors)
	}

	f.floors[entityID] = ttl
}

// floor returns the largest ttl observed for the stream, or 0.
func (f *ttlFloors) floor(entityID evt.EntityID) int64 {
	if f == nil {
		return 0
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	return f.floors[entityID]
}

// commitTTLs returns, for every entity type the commit writes, the one ttl the commit stamps on its
// events, snapshots, and head updates. It is computed once so a commit that straddles a second
// boundary cannot give its head update a ttl below one of its own items.
func (repo *Repository) commitTTLs(result evt.SerializedResult) map[evt.EntityType]int64 {
	ttls := make(map[evt.EntityType]int64)

	note := func(entityType evt.EntityType) {
		if _, seen := ttls[entityType]; !seen {
			ttls[entityType] = repo.ttlFor(entityType)
		}
	}

	for _, event := range result.Events {
		note(event.EntityType)
	}

	for _, snapshot := range result.Snapshots {
		note(snapshot.EntityType)
	}

	return ttls
}

// headTTL returns the ttl for a stream's head update. A create (first sequence 1) stamps the
// commit's own ttl; an append stamps the larger of the commit's own ttl and the largest ttl this
// repository loaded for the stream, because the head ttl is never lowered. Zero when the type has no
// retention.
func (repo *Repository) headTTL(entityID evt.EntityID, first evt.EventSequence, commitTTL int64) int64 {
	if commitTTL == 0 || first <= 1 {
		return commitTTL
	}

	return max(commitTTL, repo.ttlFloors.floor(entityID))
}

// headItem is a stream's head item as read back.
type headItem struct {
	present bool
	head    evt.EventSequence
	ttl     int64
}

// readHeadItem reads the head item with a strongly consistent GetItem, absent as head 0.
func (repo *Repository) readHeadItem(ctx context.Context, entityID evt.EntityID) (headItem, error) {
	out, err := repo.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &repo.EventsTable,
		ConsistentRead: aws.Bool(true),
		Key:            repo.headKey(entityID),
	})
	if err != nil {
		return headItem{}, fmt.Errorf("dynamo: reading the head item of %s: %w", entityID, err)
	}

	if len(out.Item) == 0 {
		return headItem{}, nil
	}

	head, _ := numAttr(out.Item, "head")
	ttl, _ := numAttr(out.Item, "ttl")

	repo.ttlFloors.observe(entityID, int64(ttl))

	return headItem{present: true, head: evt.EventSequence(head), ttl: int64(ttl)}, nil
}

// deriveHead derives a stream's head from its log with strongly consistent reads: the larger of
// the snapshot item's eventSeq and the highest event sort key (invariant H4).
func (repo *Repository) deriveHead(ctx context.Context, entityID evt.EntityID) (evt.EventSequence, error) {
	snapshot, err := repo.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            &repo.EventsTable,
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("eventSeq"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: string(entityID)},
			"sk": &types.AttributeValueMemberN{Value: "0"},
		},
	})
	if err != nil {
		return 0, fmt.Errorf("dynamo: reading the snapshot of %s: %w", entityID, err)
	}

	var derived evt.EventSequence
	if eventSeq, ok := numAttr(snapshot.Item, "eventSeq"); ok {
		derived = evt.EventSequence(eventSeq)
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
	if err != nil {
		return 0, fmt.Errorf("dynamo: reading the highest event of %s: %w", entityID, err)
	}

	if latest != nil && len(latest.Items) > 0 {
		if sk, ok := numAttr(latest.Items[0], "sk"); ok {
			derived = max(derived, evt.EventSequence(sk))
		}
	}

	return derived, nil
}
