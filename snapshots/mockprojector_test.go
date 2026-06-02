package snapshots_test

import (
	"context"

	"github.com/photon-grove/evt"
)

// MockProjector for testing projector orchestration
type MockProjector struct {
	projectError error
	called       bool
	lastEvents   []evt.Event
}

func (p *MockProjector) Project(_ context.Context, _ evt.Entity, events []evt.Event) (evt.TransactionGroup, error) {
	p.called = true
	p.lastEvents = events
	if p.projectError != nil {
		return nil, p.projectError
	}
	return nil, nil
}
