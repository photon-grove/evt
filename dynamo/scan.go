package dynamo

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
)

// scanTable scans the DynamoDB events table to replay all Events and rebuild a projection/view,
// emitting each page of Events on the returned channel.
//
// When the repository is configured with WithScanSegments(n > 1), the table is swept with a
// DynamoDB parallel Scan: n worker goroutines each scan one segment concurrently and interleave
// their pages onto the shared channel. The channel is closed once every segment finishes (or one
// of them fails). Ordering across segments and pages is not guaranteed, so consumers that need to
// group items (e.g. StreamEntities) must not assume any particular order.
func (repo *Repository) scanTable(
	ctx context.Context,
	input dynamodb.ScanInput,
) <-chan result.Result[[]evt.SerializedEvent] {
	channel := make(chan result.Result[[]evt.SerializedEvent])

	segments := repo.scanSegmentCount()

	go func() {
		defer close(channel)

		var wg sync.WaitGroup
		for segment := 0; segment < segments; segment++ {
			wg.Add(1)

			go func(segment int) {
				defer wg.Done()

				repo.scanSegment(ctx, input, segments, segment, channel)
			}(segment)
		}

		wg.Wait()
	}()

	return channel
}

// scanSegment paginates a single Scan segment and forwards each decoded page to the channel. For a
// non-parallel scan (totalSegments <= 1) the Segment/TotalSegments parameters are left unset.
func (repo *Repository) scanSegment(
	ctx context.Context,
	input dynamodb.ScanInput,
	totalSegments int,
	segment int,
	channel chan<- result.Result[[]evt.SerializedEvent],
) {
	if totalSegments > 1 {
		// Both values are bounded by scanSegmentCount() (<= maxScanSegments), so the int32
		// conversions cannot overflow.
		input.Segment = aws.Int32(int32(segment))             //nolint:gosec // bounded by maxScanSegments
		input.TotalSegments = aws.Int32(int32(totalSegments)) //nolint:gosec // bounded by maxScanSegments
	}

	p := dynamodb.NewScanPaginator(repo.client, &input)

	for p.HasMorePages() {
		if ctx.Err() != nil {
			repo.send(ctx, channel, result.Err[[]evt.SerializedEvent](ctx.Err()))
			return
		}

		pgResult, err := p.NextPage(ctx)
		if err != nil {
			repo.send(ctx, channel, result.Err[[]evt.SerializedEvent](err))
			return
		}
		if pgResult == nil {
			return
		}

		serializedEvents, err := repo.decodeScanPage(pgResult.Items)
		if err != nil {
			repo.send(ctx, channel, result.Err[[]evt.SerializedEvent](err))
			return
		}

		if !repo.send(ctx, channel, result.Ok(serializedEvents)) {
			return
		}
	}
}

// decodeScanPage turns a page of raw Scan items into SerializedEvents, skipping inline snapshots.
func (repo *Repository) decodeScanPage(items []map[string]types.AttributeValue) ([]evt.SerializedEvent, error) {
	serializedEvents := make([]evt.SerializedEvent, 0, len(items))

	for _, item := range items {
		event := Event{}
		if err := repo.unmarshalMap(item, &event); err != nil {
			return nil, err
		}

		if event.SK == 0 {
			// Skip inline snapshots stored at sk=0
			continue
		}

		var metadata evt.Metadata
		if err := json.Unmarshal([]byte(event.Metadata), &metadata); err != nil {
			return nil, err
		}

		serializedEvents = append(serializedEvents, evt.SerializedEvent{
			ID:         evt.GetEventID(event.PK, event.SK),
			EntityType: event.EntityType,
			EntityID:   event.PK,
			Sequence:   event.SK,
			Type:       event.Type,
			Version:    event.Version,
			Payload:    []byte(event.Payload),
			Metadata:   metadata,
		})
	}

	return serializedEvents, nil
}

// send delivers a result on the channel unless the context is cancelled first, returning false when
// the caller should stop producing. It keeps concurrent segment workers from blocking forever on an
// abandoned consumer.
func (repo *Repository) send(
	ctx context.Context,
	channel chan<- result.Result[[]evt.SerializedEvent],
	value result.Result[[]evt.SerializedEvent],
) bool {
	select {
	case channel <- value:
		return true
	case <-ctx.Done():
		return false
	}
}
