package evt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCommand is a test implementation of Command for testing
type TestCommandType struct {
	Name       string
	EntityID   EntityID
	EntityName EntityType
}

func (c TestCommandType) Type() CommandType {
	return CommandType(c.Name)
}

func (c TestCommandType) EntityType() EntityType {
	return c.EntityName
}

func Test_CommandType_String(t *testing.T) {
	ct := CommandType("CreateUser")
	require.Equal(t, "CreateUser", string(ct))

	// Test different command type values
	testCases := []struct {
		name     string
		input    CommandType
		expected string
	}{
		{"Simple type", "Create", "Create"},
		{"Namespaced type", "User.Create", "User.Create"},
		{"Underscore type", "create_user", "create_user"},
		{"Empty type", "", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, string(tc.input))
		})
	}
}

func Test_CommandID_String(t *testing.T) {
	id := CommandID("cmd-123-abc")
	require.Equal(t, "cmd-123-abc", string(id))

	// Test different ID formats
	testCases := []struct {
		name     string
		input    CommandID
		expected string
	}{
		{"UUID format", "550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440000"},
		{"Simple ID", "cmd-123", "cmd-123"},
		{"Empty ID", "", ""},
		{"Complex ID", "user:create:batch-1", "user:create:batch-1"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, string(tc.input))
		})
	}
}

func Test_Command_Interface(t *testing.T) {
	cmd := TestCommandType{
		Name:       "TestCommand",
		EntityID:   "entity-123",
		EntityName: "TestEntity",
	}

	// Test interface methods
	require.Equal(t, CommandType("TestCommand"), cmd.Type())
	require.Equal(t, EntityType("TestEntity"), cmd.EntityType())

	// Test that it can be used as Command interface
	var c Command = cmd
	require.Equal(t, CommandType("TestCommand"), c.Type())
	require.Equal(t, EntityType("TestEntity"), c.EntityType())
}

func Test_CommandResult_Creation(t *testing.T) {
	// Test with empty events
	result := CommandResult{}
	require.Nil(t, result.Events)
	require.Nil(t, result.Transaction)

	// Test with events
	mockEvent := MockEvent{
		EventType:    "TestEvent",
		EventVersion: 1,
		EntType:      "TestEntity",
		EntID:        "entity-1",
	}

	result = CommandResult{
		Events: []Event{mockEvent},
	}
	require.Len(t, result.Events, 1)
	require.Equal(t, EventType("TestEvent"), result.Events[0].Type())
}

func Test_CommandResult_WithTransaction(t *testing.T) {
	mockEvent := MockEvent{
		EventType: "TestEvent",
	}

	// Test with nil transaction
	result := CommandResult{
		Events:      []Event{mockEvent},
		Transaction: nil,
	}
	require.Nil(t, result.Transaction)

	// Test with empty transaction
	result = CommandResult{
		Events:      []Event{mockEvent},
		Transaction: Transaction{},
	}
	require.Empty(t, result.Transaction)
}

func Test_CommandResult_MultipleEvents(t *testing.T) {
	event1 := MockEvent{EventType: "Event1", EntID: "e1"}
	event2 := MockEvent{EventType: "Event2", EntID: "e1"}
	event3 := MockEvent{EventType: "Event3", EntID: "e1"}

	result := CommandResult{
		Events: []Event{event1, event2, event3},
	}

	require.Len(t, result.Events, 3)
	require.Equal(t, EventType("Event1"), result.Events[0].Type())
	require.Equal(t, EventType("Event2"), result.Events[1].Type())
	require.Equal(t, EventType("Event3"), result.Events[2].Type())
}

func Test_CommandTypes_ZeroValues(t *testing.T) {
	var ct CommandType
	var cid CommandID

	require.Equal(t, "", string(ct))
	require.Equal(t, "", string(cid))

	var result CommandResult
	require.Nil(t, result.Events)
	require.Nil(t, result.Transaction)
}
