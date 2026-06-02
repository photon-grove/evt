package policy

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt/dynamo"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Class
	}{
		{name: "nil", err: nil, want: ClassUnknown},
		{name: "snapshot race", err: dynamo.ErrSnapshotRaceCondition, want: ClassTransient},
		{name: "wrapped snapshot race", err: fmt.Errorf("wrap: %w", dynamo.ErrSnapshotRaceCondition), want: ClassTransient},
		{name: "transaction conflict", err: txErr("TransactionConflict"), want: ClassTransient},
		{name: "validation", err: txErr("ValidationError"), want: ClassPermanent},
		{name: "unknown", err: errors.New("boom"), want: ClassUnknown},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Classify(tc.err)
			if got != tc.want {
				t.Fatalf("Classify() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestAllTransient(t *testing.T) {
	if AllTransient(nil) {
		t.Fatal("expected nil slice to return false")
	}
	if !AllTransient([]error{dynamo.ErrSnapshotRaceCondition, txErr("TransactionConflict")}) {
		t.Fatal("expected all transient errors to return true")
	}
	if AllTransient([]error{dynamo.ErrSnapshotRaceCondition, txErr("ValidationError")}) {
		t.Fatal("expected mixed classes to return false")
	}
}

func txErr(code string) error {
	return &types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{{Code: aws.String(code)}}}
}
