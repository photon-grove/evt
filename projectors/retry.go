package projectors

import (
	"context"
	"errors"
	"io"
	"net"
)

// RetryClassification categorizes an error for retry vs DLQ routing.
type RetryClassification string

const (
	// RetryTransient indicates a temporary failure that should be retried.
	RetryTransient RetryClassification = "transient"

	// RetryPermanent indicates a non-recoverable failure that should be sent to the DLQ.
	RetryPermanent RetryClassification = "permanent"

	// RetryUnknown indicates an unclassified error that should be retried with backoff.
	RetryUnknown RetryClassification = "unknown"
)

// PermanentError wraps an error to indicate it should never be retried.
// Projectors can return this to force DLQ routing.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string {
	return e.Err.Error()
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

// NewPermanentError wraps err so that ClassifyError returns RetryPermanent.
func NewPermanentError(err error) *PermanentError {
	return &PermanentError{Err: err}
}

// ClassifyError inspects err and returns a retry classification.
// Transient errors (network, context deadline, temporary I/O) are retried.
// PermanentErrors are routed to the DLQ.
// Everything else is classified as unknown (retry with backoff).
func ClassifyError(err error) RetryClassification {
	if err == nil {
		return RetryTransient
	}

	var permanent *PermanentError
	if errors.As(err, &permanent) {
		return RetryPermanent
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return RetryTransient
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return RetryTransient
	}

	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.ErrClosedPipe) {
		return RetryTransient
	}

	return RetryUnknown
}
