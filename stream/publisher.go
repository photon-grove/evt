package stream

import (
	"context"
)

// Publisher publishes records to a stream.
type Publisher[Record any] interface {
	// Publish publishes records to the stream.
	Publish(ctx context.Context, records []Record) error
}

// PublishFromStream combines a Handler and Publisher to process events from one stream and publish
// to another.
type PublishFromStream[Event any, Record any] struct {
	handler   Handler[Event, Record]
	publisher Publisher[Record]
}

// NewPublishFromStream creates a new PublishFromStream.
func NewPublishFromStream[Event any, Record any](handler Handler[Event, Record], publisher Publisher[Record]) *PublishFromStream[Event, Record] {
	return &PublishFromStream[Event, Record]{
		handler:   handler,
		publisher: publisher,
	}
}

// Handle handles an event and publishes the resulting records.
func (p *PublishFromStream[Event, Record]) Handle(ctx context.Context, event Event) error {
	records, err := p.handler.Handle(ctx, event)
	if err != nil {
		return err
	}

	return p.publisher.Publish(ctx, records)
}
