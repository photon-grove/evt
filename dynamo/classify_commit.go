package dynamo

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
)

// A failed commit is classified by the failing items' cancellation-reason indexes. Where several
// items failed, the engine ranks them and every backend reports the same one (storage spec,
// Stream Heads, Classification). Highest first:
//
//  1. the feature-marker check (revalidate and retry), then the rebuild-lock check
//     (RebuildInProgress, transient), handled by the caller;
//  2. the outcome put (OutcomeConflict), handled by the caller;
//  3. *evt.StreamHeadDriftError;
//  4. *evt.SequenceGapError;
//  5. any other conflict: a head update, an event put, a snapshot put, or a fence check (a fence
//     carries its provenance), then projector conditions.
//
// A failed head update, or a failed fence check, is told apart by the failure-path probe below.

// cancellationCodes returns the cancellation reason code of every transaction item, "" where the
// reason carries none.
func cancellationCodes(err error) []string {
	var canceled *types.TransactionCanceledException
	if !errors.As(err, &canceled) {
		return nil
	}

	codes := make([]string, len(canceled.CancellationReasons))
	for i, reason := range canceled.CancellationReasons {
		codes[i] = aws.ToString(reason.Code)
	}

	return codes
}

// conditionFailed reports whether the item at index failed its condition.
func conditionFailed(codes []string, index int) bool {
	return index >= 0 && index < len(codes) && codes[index] == "ConditionalCheckFailed"
}

// conditionPassed reports whether the item at index is known to have passed: DynamoDB reports
// "None" for the items of a cancelled transaction that were not the cause. A reason such as
// TransactionConflict or throttling, or no reason at all, says nothing about the item's condition.
func conditionPassed(codes []string, index int) bool {
	return index >= 0 && index < len(codes) && codes[index] == "None"
}

// streamVerdict is the classification of one failed stream or fence.
type streamVerdict int

const (
	// verdictNone: the stream's items did not fail.
	verdictNone streamVerdict = iota

	// verdictConflict: another writer moved the stream; reload and retry.
	verdictConflict

	// verdictGap: the commit starts above the stored head plus one.
	verdictGap

	// verdictDrift: the head item disagrees with the log (H2 to H4 are broken).
	verdictDrift
)

// headVerdict is the outcome of the failure-path probe for one stream.
type headVerdict struct {
	kind verdictKind

	// stored and derived are the head item and the derived head the probe read; present is false
	// when the stream has no head item.
	stored  evt.EventSequence
	present bool
	derived evt.EventSequence
}

// verdictKind is what the probe's reads say about the stream.
type verdictKind int

const (
	// probeHealthy: the head item equals the derived head.
	probeHealthy verdictKind = iota

	// probeRace: a commit landed between the reads, or (retention) the log's tail expired: an
	// ordinary conflict that heals on retry.
	probeRace

	// probeDrift: the head item disagrees with the log.
	probeDrift
)

// probeStream classifies a failed head update or fence check by reading the stream, with strongly
// consistent reads and no ReturnValues on the transaction, so both languages do exactly this:
//
//  1. derive the head D1 (the larger of the snapshot's eventSeq and the highest event sk);
//  2. read the head item as S, absent as 0;
//  3. S < D1: an event or snapshot sits above the head, which is drift;
//  4. S = D1: no drift;
//  5. S > D1: derive again as D2. D2 < S is drift (events are missing below the head); otherwise a
//     commit landed between the reads, an ordinary conflict.
//
// For an entity type with retention, a head above the derived head and a head item absent while
// every remaining item has expired are the expired-tail transient, a conflict that heals once TTL
// deletes the residue, not drift.
func (repo *Repository) probeStream(
	ctx context.Context,
	entityID evt.EntityID,
	retained bool,
) (headVerdict, error) {
	first, err := repo.deriveHead(ctx, entityID)
	if err != nil {
		return headVerdict{}, err
	}

	head, err := repo.readHeadItem(ctx, entityID)
	if err != nil {
		return headVerdict{}, err
	}

	verdict := headVerdict{stored: head.head, present: head.present, derived: first}

	switch {
	case head.head < first:
		if retained && !head.present {
			expired, eerr := repo.remainingItemsExpired(ctx, entityID)
			if eerr != nil {
				return headVerdict{}, eerr
			}

			if expired {
				verdict.kind = probeRace

				return verdict, nil
			}
		}

		verdict.kind = probeDrift

	case head.head == first:
		verdict.kind = probeHealthy

	case retained:
		// The head outlived part of the log it counted: TTL deleted the tail first.
		verdict.kind = probeRace

	default:
		second, derr := repo.deriveHead(ctx, entityID)
		if derr != nil {
			return headVerdict{}, derr
		}

		verdict.derived = second

		if second < head.head {
			verdict.kind = probeDrift
		} else {
			verdict.kind = probeRace
		}
	}

	return verdict, nil
}

// remainingItemsExpired reports whether every item still stored for the stream (the snapshot and
// the events) carries a ttl that has passed, so TTL will delete it. An item without a ttl never
// expires.
func (repo *Repository) remainingItemsExpired(ctx context.Context, entityID evt.EntityID) (bool, error) {
	now := repo.nowOrDefault().Unix()

	expired := func(item map[string]types.AttributeValue) bool {
		ttl, ok := numAttr(item, "ttl")

		return ok && int64(ttl) <= now
	}

	snapshot, err := repo.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            &repo.EventsTable,
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("#ttl"),
		ExpressionAttributeNames: map[string]string{
			"#ttl": "ttl",
		},
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: string(entityID)},
			"sk": &types.AttributeValueMemberN{Value: "0"},
		},
	})
	if err != nil {
		return false, fmt.Errorf("dynamo: reading the snapshot of %s: %w", entityID, err)
	}

	if len(snapshot.Item) > 0 && !expired(snapshot.Item) {
		return false, nil
	}

	pages := dynamodb.NewQueryPaginator(repo.client, &dynamodb.QueryInput{
		TableName:              &repo.EventsTable,
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("pk = :pk AND sk > :sk"),
		ExpressionAttributeNames: map[string]string{
			"#ttl": "ttl",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: string(entityID)},
			":sk": &types.AttributeValueMemberN{Value: "0"},
		},
		ProjectionExpression: aws.String("sk, #ttl"),
	})

	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return false, fmt.Errorf("dynamo: reading the events of %s: %w", entityID, err)
		}

		for _, item := range page.Items {
			if !expired(item) {
				return false, nil
			}
		}
	}

	return true, nil
}

// classifyStream classifies one written stream of a cancelled commit.
func (repo *Repository) classifyStream(
	ctx context.Context,
	stream streamLayout,
	codes []string,
	base int,
) (streamVerdict, error) {
	retained := repo.retained(stream.entityType)
	headIndex := base + stream.headIndex

	switch {
	case conditionFailed(codes, headIndex):
		verdict, err := repo.probeStream(ctx, stream.id, retained)
		if err != nil {
			return verdictNone, err
		}

		switch {
		case verdict.kind == probeDrift:
			return verdictDrift, repo.driftError(stream.id, verdict)

		case verdict.kind == probeHealthy && stream.first > 1 && verdict.stored < stream.first-1:
			// The head the commit continues from is above what is stored: a gap, not a race.
			return verdictGap, &policy.ClassifiedError{
				Class: policy.ClassTransient,
				Err:   &evt.SequenceGapError{EntityID: stream.id, After: verdict.stored, Found: stream.first},
			}
		}

		return verdictConflict, nil

	case conditionPassed(codes, headIndex) && repo.anyFailed(codes, base, stream.eventsStart, stream.eventsEnd):
		// The head update passed and an event put failed: an event exists above the head. For a
		// type with retention, TTL can delete the head before an expired event, so a create passes
		// its head condition and fails on the residual row. That heals when TTL deletes the
		// residue; anything else is drift.
		verdict, err := repo.probeStream(ctx, stream.id, retained)
		if err != nil {
			return verdictNone, err
		}

		if retained {
			expired, eerr := repo.remainingItemsExpired(ctx, stream.id)
			if eerr != nil {
				return verdictNone, eerr
			}

			if expired {
				return verdictConflict, nil
			}
		}

		return verdictDrift, repo.driftError(stream.id, verdict)

	case repo.anyFailed(codes, base, stream.eventsStart, stream.eventsEnd),
		stream.snapshotIndex >= 0 && conditionFailed(codes, base+stream.snapshotIndex):
		// A snapshot put that fails while the head update passed is an ordinary conflict: an
		// out-of-band snapshot can bump the snapshot sequence without moving the head.
		return verdictConflict, nil
	}

	return verdictNone, nil
}

func (repo *Repository) anyFailed(codes []string, base, start, end int) bool {
	for index := start; index < end; index++ {
		if conditionFailed(codes, base+index) {
			return true
		}
	}

	return false
}

// driftError builds the typed drift error from what the probe read.
func (repo *Repository) driftError(entityID evt.EntityID, verdict headVerdict) error {
	return &evt.StreamHeadDriftError{
		EntityID:   entityID,
		Stored:     verdict.stored,
		HeadAbsent: !verdict.present,
		Derived:    verdict.derived,
	}
}

// classifyFence classifies one failed fence check.
func (repo *Repository) classifyFence(ctx context.Context, fence evt.ReadFence) (streamVerdict, error) {
	verdict, err := repo.probeStream(ctx, fence.EntityID, false)
	if err != nil {
		return verdictNone, err
	}

	if verdict.kind == probeDrift {
		return verdictDrift, repo.driftError(fence.EntityID, verdict)
	}

	return verdictConflict, nil
}

// classifyStreamFailures ranks the failures of a cancelled commit's written streams and fences:
// drift, then gap, then the first conflict in transaction order. It returns nil when none of the
// stream or fence items failed (a projector condition did).
func (repo *Repository) classifyStreamFailures(
	ctx context.Context,
	layout commitLayout,
	codes []string,
	base int,
) error {
	var (
		drift, gap, conflict error
	)

	note := func(verdict streamVerdict, detail error, conflictError func() error) {
		switch verdict {
		case verdictDrift:
			if drift == nil {
				drift = detail
			}

		case verdictGap:
			if gap == nil {
				gap = detail
			}

		case verdictConflict:
			if conflict == nil {
				conflict = conflictError()
			}
		}
	}

	for _, stream := range layout.streams {
		verdict, detail := repo.classifyStream(ctx, stream, codes, base)
		if verdict == verdictNone && detail != nil {
			// The probe itself failed; the commit did not apply, so a transient read error is
			// retryable and anything else is returned as is.
			return wrapClassifiedError(fmt.Errorf("dynamo: classifying the failed commit of %s: %w", stream.id, detail))
		}

		note(verdict, detail, func() error {
			// Name the stream whose own item failed, not just the commit: a multi-stream commit can
			// collide on any of its streams. errors.Is(err, ErrSnapshotRaceCondition) still matches.
			return wrapClassifiedError(fmt.Errorf("stream %s: %w", stream.id, ErrSnapshotRaceCondition))
		})
	}

	for _, placed := range layout.fences {
		if !conditionFailed(codes, base+placed.index) {
			continue
		}

		verdict, detail := repo.classifyFence(ctx, placed.fence)
		if verdict == verdictNone && detail != nil {
			return wrapClassifiedError(fmt.Errorf("dynamo: classifying the failed fence of %s: %w", placed.fence.EntityID, detail))
		}

		note(verdict, detail, func() error { return policy.FenceConflict(placed.fence) })
	}

	switch {
	case drift != nil:
		return drift
	case gap != nil:
		return gap
	}

	return conflict
}

// fenceLayout places a fence check among the stream items.
type fenceLayout struct {
	index int
	fence evt.ReadFence
}

// streamLayout records where a written stream's items sit among the stream items, so a failed
// action maps back to the stream that owns it.
type streamLayout struct {
	id         evt.EntityID
	entityType evt.EntityType

	// first is the commit's first sequence for the stream.
	first evt.EventSequence

	// headIndex is the head update's index among the stream items; the event puts occupy
	// [eventsStart, eventsEnd); snapshotIndex is the snapshot put's index, or -1.
	headIndex     int
	eventsStart   int
	eventsEnd     int
	snapshotIndex int
}
