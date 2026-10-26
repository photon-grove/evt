package dynamo

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt/policy"
)

func canceledWith(code string) error {
	return &types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{{Code: aws.String(code)}}}
}

func TestClassifyError(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want policy.Class
	}{
		"nil":                      {nil, policy.ClassUnknown},
		"snapshot race":            {ErrSnapshotRaceCondition, policy.ClassTransient},
		"wrapped snapshot race":    {fmt.Errorf("wrap: %w", ErrSnapshotRaceCondition), policy.ClassTransient},
		"conditional check failed": {canceledWith("ConditionalCheckFailed"), policy.ClassTransient},
		"transaction conflict":     {canceledWith("TransactionConflict"), policy.ClassTransient},
		"throttled":                {canceledWith("ThrottlingError"), policy.ClassTransient},
		"provisioned throughput":   {canceledWith("ProvisionedThroughputExceeded"), policy.ClassTransient},
		"validation":               {canceledWith("ValidationError"), policy.ClassPermanent},
		"access denied":            {canceledWith("AccessDenied"), policy.ClassPermanent},
		"unknown":                  {errors.New("boom"), policy.ClassUnknown},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, ClassifyError(tc.err))
		})
	}
}
