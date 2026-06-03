// Package policy provides shared retry and failure-classification primitives for evt-backed handlers.
package policy

import (
	stderrors "errors"
)

// Class identifies whether an error should be retried.
type Class string

const (
	// ClassUnknown is used when an error doesn't match a known policy rule.
	ClassUnknown Class = "unknown"
	// ClassTransient indicates retryable failures.
	ClassTransient Class = "transient"
	// ClassPermanent indicates non-retryable failures.
	ClassPermanent Class = "permanent"
)

// Classifier maps an error into a retry/failure class.
type Classifier func(error) Class

// Classify maps errors already wrapped with ClassifiedError into their stored
// class. Backend-specific packages can provide richer classifiers.
func Classify(err error) Class {
	if err == nil {
		return ClassUnknown
	}

	var classified *ClassifiedError
	if stderrors.As(err, &classified) {
		return classified.Class
	}

	return ClassUnknown
}

// IsTransient reports whether an error should be retried.
func IsTransient(err error) bool {
	return Classify(err) == ClassTransient
}

// IsPermanent reports whether an error should be dropped to DLQ without retries.
func IsPermanent(err error) bool {
	return Classify(err) == ClassPermanent
}

// ClassifiedError wraps an error with its failure class so callers can inspect
// the classification without re-calling Classify.
type ClassifiedError struct {
	Class Class
	Err   error
}

// Error returns the underlying error message.
func (e *ClassifiedError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

// Unwrap returns the underlying error.
func (e *ClassifiedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// IsClassifiedErr returns true when err is a ClassifiedError.
func IsClassifiedErr(err error) bool {
	var ce *ClassifiedError
	return stderrors.As(err, &ce)
}

// WrapClassified classifies err and wraps it in a ClassifiedError.
func WrapClassified(err error) *ClassifiedError {
	if err == nil {
		return nil
	}
	return &ClassifiedError{Class: Classify(err), Err: err}
}

// AllTransient reports whether every provided error is transient.
func AllTransient(errs []error) bool {
	if len(errs) == 0 {
		return false
	}
	for _, err := range errs {
		if !IsTransient(err) {
			return false
		}
	}
	return true
}
