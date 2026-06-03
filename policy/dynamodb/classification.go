// Package dynamodb provides DynamoDB-specific retry classification for evt policy.
package dynamodb

import (
	stderrors "errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/policy"
)

// Classify maps known DynamoDB backend errors into transient/permanent classes.
func Classify(err error) policy.Class {
	if err == nil {
		return policy.ClassUnknown
	}

	if stderrors.Is(err, dynamo.ErrSnapshotRaceCondition) {
		return policy.ClassTransient
	}

	var canceled *types.TransactionCanceledException
	if stderrors.As(err, &canceled) {
		if isAnyCancellationCode(canceled, map[string]struct{}{
			"ConditionalCheckFailed":        {},
			"TransactionConflict":           {},
			"ProvisionedThroughputExceeded": {},
			"ThrottlingError":               {},
		}) {
			return policy.ClassTransient
		}
		if isAnyCancellationCode(canceled, map[string]struct{}{
			"ValidationError": {},
			"AccessDenied":    {},
		}) {
			return policy.ClassPermanent
		}
	}

	return policy.Classify(err)
}

func isAnyCancellationCode(err *types.TransactionCanceledException, set map[string]struct{}) bool {
	if err == nil {
		return false
	}

	for _, reason := range err.CancellationReasons {
		if reason.Code == nil {
			continue
		}
		if _, ok := set[*reason.Code]; ok {
			return true
		}
	}

	return false
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
