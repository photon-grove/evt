package dynamo

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
	"github.com/stretchr/testify/require"
)

// mockTransGroup implements TransactionGroup for testing handleConditionalCheckFailure
type mockTransGroup struct {
	length   int
	errToRet error
}

func (g *mockTransGroup) ToWriteItems() []types.TransactWriteItem                  { return nil }
func (g *mockTransGroup) MergeDynamo(TransactionGroup) (TransactionGroup, error)   { return nil, nil }
func (g *mockTransGroup) TransactionType() evt.TransactionType                     { return "Mock" }
func (g *mockTransGroup) StorageType() evt.StorageType                             { return StorageType }
func (g *mockTransGroup) Len() int                                                 { return g.length }
func (g *mockTransGroup) HandleError(_ error, _ int) error                         { return g.errToRet }
func (g *mockTransGroup) Merge(evt.TransactionGroup) (evt.TransactionGroup, error) { return nil, nil }

func Test_handleConditionalCheckFailure_NoConditionalCheck(t *testing.T) {
	repo := &Repository{}
	ctx := context.Background()

	// Regular error without conditional check failure
	originalErr := errors.New("some error")
	result := repo.handleConditionalCheckFailure(ctx, nil, nil, originalErr)
	require.Equal(t, originalErr, result)
}

func Test_handleConditionalCheckFailure_RaceCondition(t *testing.T) {
	repo := &Repository{}
	ctx := context.Background()

	// Conditional check failure outside transaction items range
	conditionalErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{Code: aws.String("ConditionalCheckFailed")},
		},
	}

	// Empty transactItems means the failure is for events (index 0 >= 0)
	// but we have no transaction groups, so it's a race condition
	result := repo.handleConditionalCheckFailure(ctx, nil, nil, conditionalErr)
	require.ErrorIs(t, result, ErrSnapshotRaceCondition)

	var classified *policy.ClassifiedError
	require.ErrorAs(t, result, &classified)
	require.Equal(t, policy.ClassTransient, classified.Class)
}

func Test_handleConditionalCheckFailure_ClassifiesTransientTransactionConflict(t *testing.T) {
	repo := &Repository{}
	ctx := context.Background()

	transactionErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{Code: aws.String("TransactionConflict")},
		},
	}

	result := repo.handleConditionalCheckFailure(ctx, nil, nil, transactionErr)
	require.ErrorIs(t, result, transactionErr)

	var classified *policy.ClassifiedError
	require.ErrorAs(t, result, &classified)
	require.Equal(t, policy.ClassTransient, classified.Class)
}

func Test_handleConditionalCheckFailure_TransactionGroupHandlesError(t *testing.T) {
	repo := &Repository{}
	ctx := context.Background()

	expectedErr := errors.New("handled by group")
	mockGroup := &mockTransGroup{length: 2, errToRet: expectedErr}

	transaction := evt.Transaction{mockGroup}

	// Two transaction items for the mock group
	transactItems := []types.TransactWriteItem{
		{Put: &types.Put{}},
		{Put: &types.Put{}},
	}

	conditionalErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{Code: aws.String("ConditionalCheckFailed")},
		},
	}

	result := repo.handleConditionalCheckFailure(ctx, transaction, transactItems, conditionalErr)
	require.Equal(t, expectedErr, result)
}

func Test_handleConditionalCheckFailure_MultipleGroups(t *testing.T) {
	repo := &Repository{}
	ctx := context.Background()

	expectedErr := errors.New("second group error")
	group1 := &mockTransGroup{length: 2, errToRet: errors.New("first group")}
	group2 := &mockTransGroup{length: 3, errToRet: expectedErr}

	transaction := evt.Transaction{group1, group2}

	// 5 total transaction items
	transactItems := []types.TransactWriteItem{
		{Put: &types.Put{}},
		{Put: &types.Put{}},
		{Put: &types.Put{}},
		{Put: &types.Put{}},
		{Put: &types.Put{}},
	}

	// Failure at index 3 (which is in group2)
	conditionalErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{Code: aws.String("None")},
			{Code: aws.String("None")},
			{Code: aws.String("None")},
			{Code: aws.String("ConditionalCheckFailed")},
		},
	}

	result := repo.handleConditionalCheckFailure(ctx, transaction, transactItems, conditionalErr)
	require.Equal(t, expectedErr, result)
}

func Test_handleConditionalCheckFailure_FailureBeyondTransactionGroups(t *testing.T) {
	repo := &Repository{}
	ctx := context.Background()

	mockGroup := &mockTransGroup{length: 1, errToRet: errors.New("group error")}
	transaction := evt.Transaction{mockGroup}

	// More transact items than the group handles
	transactItems := []types.TransactWriteItem{
		{Put: &types.Put{}},
		{Put: &types.Put{}},
		{Put: &types.Put{}},
	}

	// Failure at index 2 which is beyond the transaction group (length 1)
	conditionalErr := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{Code: aws.String("None")},
			{Code: aws.String("None")},
			{Code: aws.String("ConditionalCheckFailed")},
		},
	}

	result := repo.handleConditionalCheckFailure(ctx, transaction, transactItems, conditionalErr)
	require.ErrorIs(t, result, conditionalErr)

	var classified *policy.ClassifiedError
	require.ErrorAs(t, result, &classified)
	require.Equal(t, policy.ClassTransient, classified.Class)
}

func Test_handleConditionalCheckFailure_IndexCalculation(t *testing.T) {
	repo := &Repository{}
	ctx := context.Background()

	// Test that the index is correctly calculated relative to the group
	group1Err := errors.New("group 1 error")
	group2Err := errors.New("group 2 error")

	group1 := &mockTransGroup{length: 3, errToRet: group1Err}
	group2 := &mockTransGroup{length: 2, errToRet: group2Err}

	transaction := evt.Transaction{group1, group2}

	transactItems := []types.TransactWriteItem{
		{Put: &types.Put{}},
		{Put: &types.Put{}},
		{Put: &types.Put{}},
		{Put: &types.Put{}},
		{Put: &types.Put{}},
	}

	testCases := []struct {
		name         string
		failureIndex int
		expectedErr  error
	}{
		{"Index 0 in group1", 0, group1Err},
		{"Index 1 in group1", 1, group1Err},
		{"Index 2 in group1", 2, group1Err},
		{"Index 3 in group2", 3, group2Err},
		{"Index 4 in group2", 4, group2Err},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reasons := make([]types.CancellationReason, tc.failureIndex+1)
			for i := range reasons {
				reasons[i] = types.CancellationReason{Code: aws.String("None")}
			}
			reasons[tc.failureIndex] = types.CancellationReason{Code: aws.String("ConditionalCheckFailed")}

			conditionalErr := &types.TransactionCanceledException{
				CancellationReasons: reasons,
			}

			result := repo.handleConditionalCheckFailure(ctx, transaction, transactItems, conditionalErr)
			require.Equal(t, tc.expectedErr, result)
		})
	}
}
