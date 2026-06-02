package evt

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockTransactionGroup implements TransactionGroup for testing
type MockTransactionGroup struct {
	Ops               []string
	Storage           StorageType
	TransType         TransactionType
	MergeError        bool
	ShouldHandleError bool
}

func (m *MockTransactionGroup) Len() int {
	return len(m.Ops)
}

func (m *MockTransactionGroup) Merge(with TransactionGroup) (TransactionGroup, error) {
	if m.MergeError {
		return nil, errors.New("merge error")
	}

	other, ok := with.(*MockTransactionGroup)
	if !ok {
		return nil, errors.New("type mismatch")
	}

	merged := &MockTransactionGroup{
		Ops:               append(m.Ops, other.Ops...),
		Storage:           m.Storage,
		TransType:         m.TransType,
		MergeError:        m.MergeError,
		ShouldHandleError: m.ShouldHandleError,
	}
	return merged, nil
}

func (m *MockTransactionGroup) StorageType() StorageType {
	return m.Storage
}

func (m *MockTransactionGroup) TransactionType() TransactionType {
	return m.TransType
}

func (m *MockTransactionGroup) HandleError(_ error, _ int) error {
	if m.ShouldHandleError {
		return errors.New("handle error")
	}
	return nil
}

func TestMerge(t *testing.T) {
	group1 := &MockTransactionGroup{Ops: []string{"op1"}}
	group2 := &MockTransactionGroup{Ops: []string{"op2"}}

	// Test merging two valid groups
	merged, err := Merge(group1, group2)
	require.NoError(t, err)
	require.NotNil(t, merged)
	assert.Equal(t, 2, merged.Len())

	// Test nil group1
	merged, err = Merge(nil, group2)
	require.NoError(t, err)
	assert.Equal(t, group2, merged)

	// Test nil group2
	merged, err = Merge(group1, nil)
	require.NoError(t, err)
	assert.Equal(t, group1, merged)

	// Test both nil
	merged, err = Merge(nil, nil)
	require.NoError(t, err)
	assert.Nil(t, merged)
}

func TestMerge_Error(t *testing.T) {
	group1 := &MockTransactionGroup{Ops: []string{"op1"}, MergeError: true}
	group2 := &MockTransactionGroup{Ops: []string{"op2"}}

	_, err := Merge(group1, group2)
	require.Error(t, err)
	assert.Equal(t, "merge error", err.Error())
}

func TestTransactionGroup_Interface(t *testing.T) {
	// Verify MockTransactionGroup implements TransactionGroup
	var _ TransactionGroup = &MockTransactionGroup{}

	group := &MockTransactionGroup{
		Storage:   "dynamo",
		TransType: "write",
	}

	assert.Equal(t, StorageType("dynamo"), group.StorageType())
	assert.Equal(t, TransactionType("write"), group.TransactionType())
}

func TestMergeTransactionGroups(t *testing.T) {
	group1 := &MockTransactionGroup{Ops: []string{"op1"}}
	group2 := &MockTransactionGroup{Ops: []string{"op2"}}

	// nil handling
	merged, err := MergeTransactionGroups(nil, group2)
	require.NoError(t, err)
	require.Equal(t, group2, merged)

	merged, err = MergeTransactionGroups(group1, nil)
	require.NoError(t, err)
	require.Equal(t, group1, merged)

	merged, err = MergeTransactionGroups(nil, nil)
	require.NoError(t, err)
	require.Nil(t, merged)

	// happy path delegates to TransactionGroup.Merge
	merged, err = MergeTransactionGroups(group1, group2)
	require.NoError(t, err)
	require.NotNil(t, merged)
	assert.Equal(t, 2, merged.Len())
}

func TestMergeTransactions(t *testing.T) {
	g1 := &MockTransactionGroup{Ops: []string{"op1"}}
	g2 := &MockTransactionGroup{Ops: []string{"op2"}}

	// empty handling
	require.Equal(t, Transaction(nil), MergeTransactions(nil, nil))
	require.Equal(t, Transaction{g1}, MergeTransactions(nil, Transaction{g1}))
	require.Equal(t, Transaction{g1}, MergeTransactions(Transaction{g1}, nil))
	require.Equal(t, Transaction{g1}, MergeTransactions(Transaction{g1}, Transaction{}))
	require.Equal(t, Transaction{g2}, MergeTransactions(Transaction{}, Transaction{g2}))

	// append behavior
	out := MergeTransactions(Transaction{g1}, Transaction{g2})
	require.Len(t, out, 2)
	require.Equal(t, g1, out[0])
	require.Equal(t, g2, out[1])
}
