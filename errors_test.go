package evt

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test types for testing errors
type TestCommand struct {
	Name string
	Data interface{}
}

func (c TestCommand) Type() CommandType {
	return CommandType(c.Name)
}

func (c TestCommand) EntityType() EntityType {
	return "TestEntity"
}

func (c TestCommand) EntityID() EntityID {
	return EntityID("test-entity-1")
}

type TestEvent struct {
	EType     EventType
	EVersion  EventVersion
	EntType   EntityType
	EntID     EntityID
	EventData interface{}
}

func (e TestEvent) Type() EventType {
	return e.EType
}

func (e TestEvent) Version() EventVersion {
	return e.EVersion
}

func (e TestEvent) EntityType() EntityType {
	return e.EntType
}

func (e TestEvent) EntityID() EntityID {
	return e.EntID
}

func Test_NotFoundError_Creation(t *testing.T) {
	msg := "Record not found with ID: test-123"
	err := NewNotFoundError(msg)

	require.NotNil(t, err)
	require.IsType(t, &NotFoundError{}, err)
	require.Equal(t, msg, err.msg)
}

func Test_NotFoundError_Error(t *testing.T) {
	msg := "User with email test@example.com not found"
	err := NewNotFoundError(msg)

	require.Equal(t, msg, err.Error())
}

func Test_NotFoundError_ErrorMessage(t *testing.T) {
	testCases := []struct {
		name string
		msg  string
	}{
		{"Simple message", "Not found"},
		{"Empty message", ""},
		{"Long message", "This is a very long error message that describes exactly what was not found and why it could not be located in the system"},
		{"Message with special chars", "Resource not found: ID='123-abc' Type=\"user\" Status=404"},
		{"Unicode message", "找不到用户"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := NewNotFoundError(tc.msg)
			require.Equal(t, tc.msg, err.Error())
		})
	}
}

func Test_BadCommandError_Creation(t *testing.T) {
	cmd := TestCommand{
		Name: "TestCommand",
		Data: map[string]interface{}{"key": "value"},
	}

	err := NewBadCommandError(cmd)

	require.NotNil(t, err)
	require.IsType(t, &BadCommandError{}, err)
	require.Equal(t, cmd, err.Command)
}

func Test_BadCommandError_Error(t *testing.T) {
	cmd := TestCommand{
		Name: "InvalidCommand",
		Data: "test data",
	}

	err := NewBadCommandError(cmd)
	expectedMsg := fmt.Sprintf("Command not recognized: %v", cmd)

	require.Equal(t, expectedMsg, err.Error())
}

func Test_BadCommandError_DifferentCommandTypes(t *testing.T) {
	testCases := []struct {
		name    string
		command Command
	}{
		{
			"String command",
			TestCommand{Name: "StringCmd", Data: "test"},
		},
		{
			"Map command",
			TestCommand{Name: "MapCmd", Data: map[string]string{"key": "value"}},
		},
		{
			"Nil data command",
			TestCommand{Name: "NilCmd", Data: nil},
		},
		{
			"Complex data command",
			TestCommand{Name: "ComplexCmd", Data: struct {
				ID   int
				Name string
			}{ID: 123, Name: "test"}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := NewBadCommandError(tc.command)
			require.NotNil(t, err)
			require.Equal(t, tc.command, err.Command)
			require.Contains(t, err.Error(), "Command not recognized:")
		})
	}
}

func Test_BadEventError_Creation(t *testing.T) {
	event := TestEvent{
		EType:     "TestEvent",
		EVersion:  1,
		EntType:   "TestEntity",
		EntID:     "test-1",
		EventData: "test data",
	}

	err := NewBadEventError(event)

	require.NotNil(t, err)
	require.IsType(t, &BadEventError{}, err)
	require.Equal(t, event, err.Event)
}

func Test_BadEventError_Error(t *testing.T) {
	event := TestEvent{
		EType:    "InvalidEvent",
		EVersion: 2,
		EntType:  "User",
		EntID:    "user-123",
	}

	err := NewBadEventError(event)
	expectedMsg := fmt.Sprintf("Event not recognized: %v", event)

	require.Equal(t, expectedMsg, err.Error())
}

func Test_BadEventError_DifferentEventTypes(t *testing.T) {
	testCases := []struct {
		name  string
		event Event
	}{
		{
			"Basic event",
			TestEvent{EType: "Basic", EVersion: 1, EntType: "Entity", EntID: "1"},
		},
		{
			"Event with complex data",
			TestEvent{
				EType:    "Complex",
				EVersion: 2,
				EntType:  "User",
				EntID:    "user-456",
				EventData: map[string]interface{}{
					"nested": map[string]string{"key": "value"},
					"array":  []int{1, 2, 3},
				},
			},
		},
		{
			"Event with nil data",
			TestEvent{EType: "NilData", EVersion: 3, EntType: "Order", EntID: "order-789", EventData: nil},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := NewBadEventError(tc.event)
			require.NotNil(t, err)
			require.Equal(t, tc.event, err.Event)
			require.Contains(t, err.Error(), "Event not recognized:")
		})
	}
}

func Test_ErrorTypes_TypeAssertion(t *testing.T) {
	// Test that we can properly type require our custom errors

	// NotFoundError
	var notFoundErr error = NewNotFoundError("test message")
	nfErr, ok := notFoundErr.(*NotFoundError)
	require.True(t, ok)
	require.Equal(t, "test message", nfErr.Error())

	// BadCommandError
	cmd := TestCommand{Name: "TestCmd"}
	var badCmdErr error = NewBadCommandError(cmd)
	bcErr, ok := badCmdErr.(*BadCommandError)
	require.True(t, ok)
	require.Equal(t, cmd, bcErr.Command)

	// BadEventError
	event := TestEvent{EType: "TestEvent"}
	var badEventErr error = NewBadEventError(event)
	beErr, ok := badEventErr.(*BadEventError)
	require.True(t, ok)
	require.Equal(t, event, beErr.Event)

}

func Test_ErrorTypes_ErrorInterface(t *testing.T) {
	// Test that all our error types properly implement the error interface
	var err error

	// NotFoundError
	err = NewNotFoundError("not found")
	require.NotEmpty(t, err.Error())

	// BadCommandError
	err = NewBadCommandError(TestCommand{Name: "TestCmd"})
	require.NotEmpty(t, err.Error())

	// BadEventError
	err = NewBadEventError(TestEvent{EType: "TestEvent"})
	require.NotEmpty(t, err.Error())

}

func Test_ErrorTypes_Equality(t *testing.T) {
	// Test error equality for same error instances
	msg := "test message"
	err1 := NewNotFoundError(msg)
	err2 := NewNotFoundError(msg)

	// They should have the same message but be different instances
	require.Equal(t, err1.Error(), err2.Error())
	require.NotSame(t, err1, err2)

	// Test command error equality
	cmd := TestCommand{Name: "TestCmd", Data: "data"}
	cmdErr1 := NewBadCommandError(cmd)
	cmdErr2 := NewBadCommandError(cmd)

	require.Equal(t, cmdErr1.Error(), cmdErr2.Error())
	require.Equal(t, cmdErr1.Command, cmdErr2.Command)
	require.NotSame(t, cmdErr1, cmdErr2)
}

func Test_ErrorTypes_NilHandling(t *testing.T) {
	// Test that our constructors handle edge cases gracefully

	// Test with nil command (should not panic, but might not be meaningful)
	// Note: We can't actually pass nil to NewBadCommandError due to Go's type system,
	// but we can test with zero values
	zeroCmd := TestCommand{}
	cmdErr := NewBadCommandError(zeroCmd)
	require.NotNil(t, cmdErr)
	require.NotEmpty(t, cmdErr.Error())

}

func Test_IsNotFoundErr(t *testing.T) {
	require.False(t, IsNotFoundErr(nil))

	err := NewNotFoundError("nope")
	require.True(t, IsNotFoundErr(err))

	wrapped := fmt.Errorf("wrapped: %w", err)
	require.True(t, IsNotFoundErr(wrapped))

	require.False(t, IsNotFoundErr(errors.New("other")))
}

func Test_ConflictError(t *testing.T) {
	require.False(t, IsConflictErr(nil))

	err := NewConflictError("conflict")
	require.Equal(t, "conflict", err.Error())
	require.True(t, IsConflictErr(err))

	wrapped := fmt.Errorf("wrapped: %w", err)
	require.True(t, IsConflictErr(wrapped))

	require.False(t, IsConflictErr(errors.New("other")))
}
