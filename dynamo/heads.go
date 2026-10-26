package dynamo

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
)

// Compile-time check: the event-log scan satisfies the change-detection reader contract.
var (
	_ evt.EntityHeadStreamer = (*Repository)(nil)
)

// StreamEntityHeads implements evt.EntityHeadStreamer by scanning the event log directly. It folds
// the log down to each entity's head: the larger of the highest event sort key (sk >= 1) and the
// snapshot's recorded eventSeq (the sk=0 row), so a stream whose early events were compacted away
// still reports the sequence its snapshot covers.
//
// This is the supported change-detection path (ADR-0006, K4), but it reads the whole event log on
// every call — DynamoDB bills a Scan by item size regardless of projection — so budget for the
// scan cost on each change-detection pass, and keep the passes infrequent relative to commits.
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

			id := evt.EntityID(pkAttr.Value)
			if evt.IsReservedEntityID(id) {
				// Reserved namespaces hold engine-managed rows, never ordinary aggregates.
				continue
			}

			seq, ok := headSeqFromItem(item)
			if !ok {
				continue
			}

			if seq > heads[id] {
				heads[id] = seq
			}
		}
	}

	return heads, nil
}

// headSeqFromItem returns the sequence an event-log row contributes to its entity's head. An event
// row (sk >= 1) contributes its sk; a snapshot row (sk = 0) contributes its recorded eventSeq so a
// compacted stream still reports the sequence its snapshot covers. It returns false for rows that
// carry neither (defensive — should not occur in a well-formed log). A head item (sk = -1, ADR 0007)
// contributes nothing: it is derived from the log, not part of it.
func headSeqFromItem(item map[string]types.AttributeValue) (evt.EventSequence, bool) {
	sk, ok := numAttr(item, "sk")
	if !ok || sk < 0 {
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
