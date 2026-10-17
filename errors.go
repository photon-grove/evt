package evt

import (
	"errors"
	"fmt"
)

// ErrInvalidArgument identifies programmer errors in repository arguments.
var ErrInvalidArgument = errors.New("invalid argument")

// InvalidArgumentError reports an invalid argument before any storage request.
type InvalidArgumentError struct {
	Argument string
	Message  string
}

// Error returns the argument and validation failure.
func (e *InvalidArgumentError) Error() string {
	return fmt.Sprintf("%s: %s: %s", ErrInvalidArgument, e.Argument, e.Message)
}

// Unwrap keeps invalid arguments matchable with errors.Is(err, ErrInvalidArgument).
func (e *InvalidArgumentError) Unwrap() error {
	return ErrInvalidArgument
}

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

// ErrOptimisticConflict matches, with errors.Is, a failure that means another writer moved state
// this attempt read or wrote: a colliding event sequence, a moved snapshot, a failed fence, a
// continuity gap, or a concurrent transaction. Reloading and deciding again can succeed. A backend
// reports such failures so that errors.Is(err, ErrOptimisticConflict) or IsConflictErr matches;
// fence-aware retry (ADR 0004, C5) re-prepares a command on exactly these.
var ErrOptimisticConflict = errors.New("optimistic concurrency conflict")

// IsOptimisticConflict reports whether err is an optimistic-concurrency conflict: it matches
// ErrOptimisticConflict, or is (or wraps) a ConflictError, which includes SequenceGapError and
// FenceConflictError. Apply it to errors from loading and committing only. A domain error an
// entity's Handle returns can be a ConflictError too (a duplicate resource) and is not a
// concurrency conflict.
func IsOptimisticConflict(err error) bool {
	return err != nil && (errors.Is(err, ErrOptimisticConflict) || IsConflictErr(err))
}

// ErrSequenceGap identifies a stream whose stored events skip a sequence while loading: another
// writer snapshotted and compacted the stream between the snapshot read and the event read, or
// events expired. Match it with errors.Is; the same error also matches IsConflictErr.
var ErrSequenceGap = errors.New("stream sequence gap")

// SequenceGapError reports the sequences a loaded stream skipped. It is a conflict (reloading
// resolves it) and matches ErrSequenceGap.
type SequenceGapError struct {
	EntityID EntityID
	// After is the last sequence applied before the gap.
	After EventSequence
	// Found is the next stored sequence, which is not After + 1.
	Found EventSequence
}

// Error returns the error message string.
func (e *SequenceGapError) Error() string {
	return fmt.Sprintf(
		"stream %s skips from sequence %d to %d: it was compacted during the load, or events expired",
		e.EntityID, e.After, e.Found,
	)
}

// Unwrap keeps the gap matchable as a conflict (IsConflictErr).
func (e *SequenceGapError) Unwrap() error { return NewConflictError(e.Error()) }

// Is makes errors.Is(err, ErrSequenceGap) match.
func (e *SequenceGapError) Is(target error) bool { return target == ErrSequenceGap }

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
