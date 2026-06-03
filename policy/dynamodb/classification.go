// Package dynamodb provides DynamoDB-specific retry classification for evt policy.
package dynamodb

import (
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/policy"
)

// Classify maps known DynamoDB backend errors into transient/permanent classes.
func Classify(err error) policy.Class {
	return dynamo.ClassifyError(err)
}

// IsTransient reports whether an error should be retried by DynamoDB-backed commit paths.
func IsTransient(err error) bool {
	return Classify(err) == policy.ClassTransient
}

// IsPermanent reports whether an error should be dropped without retries.
func IsPermanent(err error) bool {
	return Classify(err) == policy.ClassPermanent
}

// WrapClassified classifies err and wraps it in a policy.ClassifiedError.
func WrapClassified(err error) *policy.ClassifiedError {
	if err == nil {
		return nil
	}

	return &policy.ClassifiedError{Class: Classify(err), Err: err}
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

// DefaultConfig returns the generic policy defaults with DynamoDB classification installed.
func DefaultConfig() policy.Config {
	cfg := policy.DefaultConfig()
	cfg.Classify = Classify
	cfg.IsRetryable = IsTransient

	return cfg
}
