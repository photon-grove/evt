package dynamo

import (
	stderrors "errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt/policy"
)

// ClassifyError maps known DynamoDB backend errors into transient/permanent classes.
func ClassifyError(err error) policy.Class {
	if err == nil {
		return policy.ClassUnknown
	}

	if stderrors.Is(err, ErrSnapshotRaceCondition) {
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

func wrapClassifiedError(err error) error {
	if err == nil {
		return nil
	}

	class := ClassifyError(err)
	if class == policy.ClassUnknown {
		return err
	}

	return &policy.ClassifiedError{Class: class, Err: err}
}
