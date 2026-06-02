package snapshots_test

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/test"
)

// MockEntity for testing error scenarios
type MockEntity struct {
	evt.BaseEntity
	handleError        error
	applyError         error
	snapshotError      bool
	manyEvents         bool
	serializationError bool
	projectors         []evt.EventProjector
}

func (m *MockEntity) GetID() evt.EntityID {
	return m.ID
}

func (m *MockEntity) Type() evt.EntityType {
	return "MockEntity"
}

func (m *MockEntity) DeserializeEvent(_ evt.SerializedEvent) (evt.Event, error) {
	return nil, errors.New("not implemented")
}

func (m *MockEntity) EventUpcasters() []evt.EventUpcaster {
	return nil
}

func (m *MockEntity) Projectors() []evt.EventProjector {
	return m.projectors
}

func (m *MockEntity) Base() evt.BaseEntity {
	return m.BaseEntity
}

func (m *MockEntity) Handle(_ context.Context, _ evt.Command) (evt.CommandResult, error) {
	if m.handleError != nil {
		return evt.CommandResult{}, m.handleError
	}

	if m.manyEvents {
		// Return many events to trigger "too many events" error
		manyEvents := make([]evt.Event, 5)
		for i := range manyEvents {
			manyEvents[i] = &test.EntityCreated{
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
			&test.EntityCreated{
				ID:    m.ID,
				Value: "test",
			},
		},
	}, nil
}

func (m *MockEntity) Apply(_ evt.Event) error {
	if m.applyError != nil {
		return m.applyError
	}
	return nil
}

func (m *MockEntity) MarshalJSON() ([]byte, error) {
	if m.snapshotError {
		return nil, errors.New("snapshot generation failed")
	}
	return json.Marshal(map[string]interface{}{
		"ID":    m.ID,
		"Value": "test",
	})
}

func (m *MockEntity) UnmarshalJSON(_ []byte) error {
	return nil
}

// SetIDMockEntity is a MockEntity that implements SetID for testing the
// store's entity ID override behavior.
type SetIDMockEntity struct {
	MockEntity
	setIDCalled bool
	setIDValue  evt.EntityID
}

func (m *SetIDMockEntity) SetID(id evt.EntityID) {
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
