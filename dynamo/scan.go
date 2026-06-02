package dynamo

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
)

// Scan the entire DynamoDB table to replay all Events and rebuild a projection/view
func (repo *Repository) scanTable(
	ctx context.Context,
	input dynamodb.ScanInput,
) <-chan result.Result[[]evt.SerializedEvent] {
	p := dynamodb.NewScanPaginator(repo.client, &input)
	channel := make(chan result.Result[[]evt.SerializedEvent])

	go func() {
		defer close(channel)

		for p.HasMorePages() {
			pgResult, err := p.NextPage(ctx)
			if err != nil {
				channel <- result.Err[[]evt.SerializedEvent](err)
				return
			}
			if pgResult == nil {
				return
			}

			// Set up a container for this batch of Events
			var serializedEvents []evt.SerializedEvent

			for _, item := range pgResult.Items {
				event := Event{}
				if err = repo.unmarshalMap(item, &event); err != nil {
					channel <- result.Err[[]evt.SerializedEvent](err)
					return
				}

				if event.SK == 0 {
					// Skip inline snapshots stored at sk=0
					continue
				}

				var metadata evt.Metadata
				if err = json.Unmarshal([]byte(event.Metadata), &metadata); err != nil {
					channel <- result.Err[[]evt.SerializedEvent](err)
					return
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

			channel <- result.Ok(serializedEvents)
		}
	}()

	return channel
}
