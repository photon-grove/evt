package projectors

import (
	"context"
	"fmt"
	"sync"
)

// IdempotencyGuard tracks which events have been successfully processed by a
// given projector, preventing duplicate application during retries or replays.
type IdempotencyGuard interface {
	// IsProcessed returns true if the event was already successfully processed
	// by the named projector.
	IsProcessed(ctx context.Context, projectorName string, eventID string) (bool, error)

	// MarkProcessed records that the named projector successfully processed the event.
	MarkProcessed(ctx context.Context, projectorName string, eventID string) error
}

// InMemoryIdempotencyGuard is a thread-safe in-memory implementation of
// IdempotencyGuard, suitable for testing and single-invocation Lambda contexts.
type InMemoryIdempotencyGuard struct {
	mu        sync.RWMutex
	processed map[string]struct{}
}

// NewInMemoryIdempotencyGuard creates a new empty in-memory guard.
func NewInMemoryIdempotencyGuard() *InMemoryIdempotencyGuard {
	return &InMemoryIdempotencyGuard{
		processed: make(map[string]struct{}),
	}
}

func idempotencyKey(projectorName, eventID string) string {
	return fmt.Sprintf("%s:%s", projectorName, eventID)
}

// IsProcessed checks if an event was already processed.
func (g *InMemoryIdempotencyGuard) IsProcessed(_ context.Context, projectorName string, eventID string) (bool, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	_, ok := g.processed[idempotencyKey(projectorName, eventID)]
	return ok, nil
}

// MarkProcessed records successful processing of an event.
func (g *InMemoryIdempotencyGuard) MarkProcessed(_ context.Context, projectorName string, eventID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.processed[idempotencyKey(projectorName, eventID)] = struct{}{}
	return nil
}

// Reset clears all recorded processing state. Useful in tests and replay scenarios.
func (g *InMemoryIdempotencyGuard) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.processed = make(map[string]struct{})
}
