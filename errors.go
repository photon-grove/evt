package evt

import (
	"errors"
	"fmt"
)

// A NotFoundError indicates a required record was not found
type NotFoundError struct {
	msg string
}

// NewNotFoundError creates a new instance of a NotFoundError from a message
func NewNotFoundError(msg string) *NotFoundError {
	return &NotFoundError{msg}
}

// IsNotFoundErr returns true when err is a NotFoundError.
func IsNotFoundErr(err error) bool {
	var notFound *NotFoundError
	return errors.As(err, &notFound)
}

// Error returns the error message string
func (e *NotFoundError) Error() string {
	return e.msg
}

// BadCommandError means an invalid Command was sent to an Entity
type BadCommandError struct {
	Command Command
}

// NewBadCommandError creates a new instance of a BadCommandError
func NewBadCommandError(command Command) *BadCommandError {
	return &BadCommandError{command}
}

// Error provides the error message
func (e *BadCommandError) Error() string {
	return fmt.Sprintf(
		"Command not recognized: %v",
		e.Command,
	)
}

// BadEventError is returned when an invalid Event is sent to an
type BadEventError struct {
	Event Event
}

// NewBadEventError creates a new instance of a BadEventError
func NewBadEventError(event Event) *BadEventError {
	return &BadEventError{event}
}

// Error returns the error message string
func (e *BadEventError) Error() string {
	return fmt.Sprintf(
		"Event not recognized: %v",
		e.Event,
	)
}

// ConflictError indicates a domain-level conflict (e.g., duplicate resource)
type ConflictError struct {
	msg string
}

// NewConflictError creates a new ConflictError with the given message.
func NewConflictError(msg string) *ConflictError {
	return &ConflictError{msg}
}

// IsConflictErr returns true when err is a ConflictError.
func IsConflictErr(err error) bool {
	var c *ConflictError
	return errors.As(err, &c)
}

// Error returns the error message string
func (e *ConflictError) Error() string {
	return e.msg
}

// NoOpError indicates a command would produce no changes.
type NoOpError struct {
	msg string
}

// NewNoOpError creates a new NoOpError with the given message.
func NewNoOpError(msg string) *NoOpError {
	return &NoOpError{msg}
}

// IsNoOpErr returns true when err is a NoOpError.
func IsNoOpErr(err error) bool {
	var n *NoOpError
	return errors.As(err, &n)
}

// Error returns the error message string.
func (e *NoOpError) Error() string {
	return e.msg
}

// DuplicateCommandError indicates a command with the same CommandID has already been processed.
// Callers should treat this as an idempotent success rather than a failure.
type DuplicateCommandError struct {
	CommandID CommandID
}

// NewDuplicateCommandError creates a new DuplicateCommandError.
func NewDuplicateCommandError(id CommandID) *DuplicateCommandError {
	return &DuplicateCommandError{CommandID: id}
}

// IsDuplicateCommandErr returns true when err is a DuplicateCommandError.
func IsDuplicateCommandErr(err error) bool {
	var d *DuplicateCommandError
	return errors.As(err, &d)
}

// Error returns the error message string.
func (e *DuplicateCommandError) Error() string {
	return fmt.Sprintf("duplicate command: %s", e.CommandID)
}

// ReplayStrictnessError indicates replay failed closed while upcasting or deserializing an event.
type ReplayStrictnessError struct {
	EventType EventType
	Version   EventVersion
	Phase     string
	Err       error
}

// Error returns the underlying error message.
func (e *ReplayStrictnessError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *ReplayStrictnessError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// IsReplayStrictnessErr returns true when err is a ReplayStrictnessError.
func IsReplayStrictnessErr(err error) bool {
	var replayErr *ReplayStrictnessError
	return errors.As(err, &replayErr)
}
