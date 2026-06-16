package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/projectors"
)

// DefaultHeadsProjectorName is the stable projector name used for idempotency keys and telemetry.
const DefaultHeadsProjectorName = "entity-heads"

// Compile-time checks: the event-log scan and the heads table both satisfy the change-detection
// reader contract, and the heads table also serves as a stream projector.
var (
	_ evt.EntityHeadStreamer = (*Repository)(nil)
	_ evt.EntityHeadStreamer = (*HeadStore)(nil)
	_ evt.EntityHeadVisitor  = (*HeadStore)(nil)
	_ projectors.Projector   = (*HeadStore)(nil)
)

// StreamEntityHeads implements evt.EntityHeadStreamer by scanning the event log directly. It folds
// the log down to each entity's head: the larger of the highest event sort key (sk >= 1) and the
// snapshot's recorded eventSeq (the sk=0 row), so a stream whose early events were compacted away
// still reports the sequence its snapshot covers. It is the dependency-free fallback (and the
// backfill source for a HeadStore): correct, but it reads the whole event log every call — DynamoDB
// bills a Scan by item size regardless of projection. Prefer a HeadStore, backed by a small heads
// table maintained by the heads projector, for the steady-state change-detection path.
func (repo *Repository) StreamEntityHeads(
	ctx context.Context,
	entityType evt.EntityType,
) (map[evt.EntityID]evt.EventSequence, error) {
	heads := make(map[evt.EntityID]evt.EventSequence)

	input := dynamodb.ScanInput{
		TableName:            &repo.EventsTable,
		ConsistentRead:       aws.Bool(repo.consistentRead),
		ProjectionExpression: aws.String("pk, sk, eventSeq"),
	}

	if entityType != "" {
		input.FilterExpression = aws.String("entityType = :et")
		input.ExpressionAttributeValues = map[string]types.AttributeValue{
			":et": &types.AttributeValueMemberS{Value: string(entityType)},
		}
	}

	p := dynamodb.NewScanPaginator(repo.client, &input)

	for p.HasMorePages() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		if page == nil {
			continue
		}

		for _, item := range page.Items {
			pkAttr, ok := item["pk"].(*types.AttributeValueMemberS)
			if !ok {
				continue
			}

			seq, ok := headSeqFromItem(item)
			if !ok {
				continue
			}

			if id := evt.EntityID(pkAttr.Value); seq > heads[id] {
				heads[id] = seq
			}
		}
	}

	return heads, nil
}

// headSeqFromItem returns the sequence an event-log row contributes to its entity's head. An event
// row (sk >= 1) contributes its sk; a snapshot row (sk = 0) contributes its recorded eventSeq so a
// compacted stream still reports the sequence its snapshot covers. It returns false for rows that
// carry neither (defensive — should not occur in a well-formed log).
func headSeqFromItem(item map[string]types.AttributeValue) (evt.EventSequence, bool) {
	sk, ok := numAttr(item, "sk")
	if !ok {
		return 0, false
	}

	if sk > 0 {
		return evt.EventSequence(sk), true
	}

	// sk == 0: snapshot row. Its eventSeq records the highest event it captured.
	if eventSeq, ok := numAttr(item, "eventSeq"); ok {
		return evt.EventSequence(eventSeq), true
	}

	return 0, false
}

// numAttr reads an integer DynamoDB Number attribute, returning false when absent or non-numeric.
func numAttr(item map[string]types.AttributeValue, name string) (int, bool) {
	attr, ok := item[name].(*types.AttributeValueMemberN)
	if !ok {
		return 0, false
	}

	n, err := strconv.Atoi(attr.Value)
	if err != nil {
		return 0, false
	}

	return n, true
}

// HeadStore maintains and reads a heads table: one row per entity (pk = entity ID) recording the
// highest event sequence observed for that entity. It is the change-detection backing for
// incremental projection rebuilds, and it is filled the same way every other read model is — by a
// projector on the event stream — so it adds no work to the commit path.
//
//   - As a projectors.Projector it upserts heads from the stream (the SNS->SQS path other
//     projectors use), monotonically, so re-deliveries and out-of-order events are no-ops.
//   - As an evt.EntityHeadStreamer it reads the heads back with a cheap scan of the small heads
//     table instead of the full event log.
type HeadStore struct {
	client         Client
	headsTable     string
	name           string
	consistentRead bool
}

// NewHeadStore builds a HeadStore over the given heads table. Reads default to eventually
// consistent (half the RCU cost): change detection is inherently re-runnable, and a head that
// lags a beat only defers an entity's reprojection to the next rebuild, never skips it. Derive a
// strongly consistent variant with WithConsistentRead(true) when a read must reflect the latest
// projector write.
func NewHeadStore(client Client, headsTable string) *HeadStore {
	return &HeadStore{
		client:         client,
		headsTable:     headsTable,
		name:           DefaultHeadsProjectorName,
		consistentRead: false,
	}
}

// WithConsistentRead returns a shallow copy of the HeadStore with the consistent-read setting
// updated. When true, StreamEntityHeads uses strongly consistent reads; when false (the default),
// it uses eventually consistent reads at half the RCU cost. The original HeadStore is not
// modified, so a strong- and an eventual-consistency variant can be derived from the same base.
func (h *HeadStore) WithConsistentRead(consistent bool) *HeadStore {
	c := *h
	c.consistentRead = consistent
	return &c
}

// Name implements projectors.Projector.
func (h *HeadStore) Name() string {
	return h.name
}

// Process implements projectors.Projector, upserting each record's entity head. A failed upsert is
// reported as a per-record partial-batch failure so Lambda retries just that record; because the
// upsert is monotonic and idempotent, a retry (or a re-delivery) is always safe.
func (h *HeadStore) Process(
	ctx context.Context,
	records []projectors.StreamRecord,
) ([]projectors.BatchItemFailure, error) {
	var failures []projectors.BatchItemFailure

	for _, rec := range records {
		if err := h.upsertHead(ctx, rec.EntityID, evt.EntityType(rec.EntityType), evt.EventSequence(rec.Sequence)); err != nil {
			failures = append(failures, projectors.BatchItemFailure{ItemIdentifier: rec.EventID})
		}
	}

	return failures, nil
}

// upsertHead advances an entity's head to seq, never regressing it. The monotonic condition makes
// the write idempotent and out-of-order-safe: a re-delivered or stale event fails the condition and
// is treated as a no-op success. Writes are keyed by entity ID, so they spread across partitions
// like the event log itself — no hot partition.
func (h *HeadStore) upsertHead(
	ctx context.Context,
	entityID string,
	entityType evt.EntityType,
	seq evt.EventSequence,
) error {
	// Snapshot rows (sequence 0) and malformed records carry no head to record.
	if entityID == "" || seq <= 0 {
		return nil
	}

	input := &dynamodb.UpdateItemInput{
		TableName: &h.headsTable,
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: entityID},
		},
		UpdateExpression:    aws.String("SET headSeq = :seq, entityType = :et"),
		ConditionExpression: aws.String("attribute_not_exists(headSeq) OR headSeq < :seq"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":seq": &types.AttributeValueMemberN{Value: strconv.Itoa(int(seq))},
			":et":  &types.AttributeValueMemberS{Value: string(entityType)},
		},
	}

	if _, err := h.client.UpdateItem(ctx, input); err != nil {
		// The stored head is already at or ahead of this sequence; the write would regress it, so
		// skip it. This is the expected path for duplicate and out-of-order deliveries.
		var condFailed *types.ConditionalCheckFailedException
		if errors.As(err, &condFailed) {
			return nil
		}

		return fmt.Errorf("upserting head for entity %s: %w", entityID, err)
	}

	return nil
}

// StreamEntityHeads implements evt.EntityHeadStreamer by scanning the heads table. The heads table
// holds one small row per entity, so this reads only a few MB regardless of event-log size — the
// cheap change-detection path. The scan is eventually consistent by default (see NewHeadStore);
// use WithConsistentRead(true) for a strongly consistent read. entityType, when non-empty,
// restricts the result to that type.
func (h *HeadStore) StreamEntityHeads(
	ctx context.Context,
	entityType evt.EntityType,
) (map[evt.EntityID]evt.EventSequence, error) {
	heads := make(map[evt.EntityID]evt.EventSequence)

	err := h.StreamEntityHeadsFunc(ctx, entityType, func(id evt.EntityID, seq evt.EventSequence) error {
		heads[id] = seq
		return nil
	})
	if err != nil {
		return nil, err
	}

	return heads, nil
}

// StreamEntityHeadsFunc implements evt.EntityHeadVisitor: it pages the heads table and invokes visit
// once per row without ever holding more than one page in memory, so a rebuild that enumerates
// through it has a memory ceiling that does not grow with the entity count. This is what the
// map-returning StreamEntityHeads cannot offer — the map is O(entities) by construction.
//
// Constant memory is sound here because the heads table holds exactly one row per entity: the rows
// are already unique, so enumeration needs no dedup set (unlike an event-log scan, where a partition
// key repeats per event), and the paginator resumes naturally from each page's LastEvaluatedKey.
// The scan is eventually consistent by default (see NewHeadStore); use WithConsistentRead(true) for
// a strongly consistent read. entityType, when non-empty, restricts enumeration to that type.
func (h *HeadStore) StreamEntityHeadsFunc(
	ctx context.Context,
	entityType evt.EntityType,
	visit func(evt.EntityID, evt.EventSequence) error,
) error {
	input := dynamodb.ScanInput{
		TableName:      &h.headsTable,
		ConsistentRead: aws.Bool(h.consistentRead),
	}

	if entityType != "" {
		input.FilterExpression = aws.String("entityType = :et")
		input.ExpressionAttributeValues = map[string]types.AttributeValue{
			":et": &types.AttributeValueMemberS{Value: string(entityType)},
		}
	}

	p := dynamodb.NewScanPaginator(h.client, &input)

	for p.HasMorePages() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		page, err := p.NextPage(ctx)
		if err != nil {
			return err
		}
		if page == nil {
			continue
		}

		for _, item := range page.Items {
			pkAttr, ok := item["pk"].(*types.AttributeValueMemberS)
			if !ok {
				continue
			}

			seq, ok := numAttr(item, "headSeq")
			if !ok {
				continue
			}

			if err := visit(evt.EntityID(pkAttr.Value), evt.EventSequence(seq)); err != nil {
				return err
			}
		}
	}

	return nil
}

// Backfill seeds the heads table from a source of entity heads — typically a Repository whose
// StreamEntityHeads scans the event log — for entities that predate the heads projector. It upserts
// each head with the same monotonic condition as live projection, so it is safe to run concurrently
// with the projector and safe to re-run. Pass a non-empty entityType to scope the source read and
// to record that type on the written rows; call once per type to fully populate the type attribute.
// It returns the number of heads written.
func (h *HeadStore) Backfill(
	ctx context.Context,
	source evt.EntityHeadStreamer,
	entityType evt.EntityType,
) (int, error) {
	heads, err := source.StreamEntityHeads(ctx, entityType)
	if err != nil {
		return 0, fmt.Errorf("reading source heads: %w", err)
	}

	written := 0
	for id, seq := range heads {
		if err := h.upsertHead(ctx, string(id), entityType, seq); err != nil {
			return written, err
		}

		written++
	}

	return written, nil
}
