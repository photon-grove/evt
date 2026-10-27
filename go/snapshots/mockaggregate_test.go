package snapshots_test

import (
	"context"
	"encoding/json"
	"errors"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/test"
)

// MockAggregate for testing error scenarios
type MockAggregate struct {
	ID                 evt.EntityID `json:"id"`
	handleError        error
	applyError         error
	snapshotError      bool
	manyEvents         bool
	serializationError bool
	projectors         []evt.EventProjector
}

func (m *MockAggregate) GetID() evt.EntityID {
	return m.ID
}

func (m *MockAggregate) Type() evt.EntityType {
	return "MockEntity"
}

func (m *MockAggregate) DeserializeEvent(_ evt.SerializedEvent) (evt.Event, error) {
	return nil, errors.New("not implemented")
}

func (m *MockAggregate) EventUpcasters() []evt.EventUpcaster {
	return nil
}

func (m *MockAggregate) Projectors() []evt.EventProjector {
	return m.projectors
}

func (m *MockAggregate) Handle(_ context.Context, _ evt.Command) (evt.CommandResult, error) {
	if m.handleError != nil {
		return evt.CommandResult{}, m.handleError
	}

	if m.manyEvents {
		// Return many events to trigger "too many events" error
		manyEvents := make([]evt.Event, 5)
		for i := range manyEvents {
			manyEvents[i] = &test.AggregateCreated{
				ID:    m.ID,
				Value: "test",
			}
		}
		return evt.CommandResult{Events: manyEvents}, nil
	}

	if m.serializationError {
		// Return an event that can't be serialized (contains channel which can't be marshaled to JSON)
		return evt.CommandResult{
			Events: []evt.Event{&UnserialisableEvent{}},
		}, nil
	}

	return evt.CommandResult{
		Events: []evt.Event{
			&test.AggregateCreated{
				ID:    m.ID,
				Value: "test",
			},
		},
	}, nil
}

func (m *MockAggregate) Apply(_ evt.Event) error {
	if m.applyError != nil {
		return m.applyError
	}
	return nil
}

func (m *MockAggregate) MarshalJSON() ([]byte, error) {
	if m.snapshotError {
		return nil, errors.New("snapshot generation failed")
	}
	return json.Marshal(map[string]interface{}{
		"ID":    m.ID,
		"Value": "test",
	})
}

func (m *MockAggregate) UnmarshalJSON(_ []byte) error {
	return nil
}

// SetIDMockAggregate is a MockAggregate that implements SetID for testing the
// store's entity ID override behavior.
type SetIDMockAggregate struct {
	MockAggregate
	setIDCalled bool
	setIDValue  evt.EntityID
}

func (m *SetIDMockAggregate) SetID(id evt.EntityID) {
	m.setIDCalled = true
	m.setIDValue = id
	m.ID = id
}

// MockCommand for testing command error scenarios
type MockCommand struct {
	handleError error
}

func (m *MockCommand) Type() evt.CommandType {
	return "MockCommand"
}

func (m *MockCommand) EntityType() evt.EntityType {
	return "MockEntity"
}

// UnserialisableEvent contains fields that can't be JSON marshaled
type UnserialisableEvent struct {
	Channel chan string `json:"channel"` // Channels can't be marshaled to JSON
}

func (u *UnserialisableEvent) Type() evt.EventType {
	return "UnserialisableEvent"
}

func (u *UnserialisableEvent) Version() evt.EventVersion {
	return 1
}

func (u *UnserialisableEvent) EntityType() evt.EntityType {
	return "MockEntity"
}

func (u *UnserialisableEvent) EntityID() evt.EntityID {
	return "mock-id"
}
