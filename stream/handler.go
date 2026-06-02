package stream

import (
	"context"
)

// Handler processes events and converts them to records for publishing.
type Handler[Event, Record any] interface {
	// Handle processes an event and returns records to publish.
	Handle(ctx context.Context, event Event) ([]Record, error)
}
