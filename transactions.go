package evt

// A TransactionType is a type of operation included in the Transaction.
type TransactionType string

// A Transaction is a collection of grouped operations that should be rolled back together in the
// event of failure.
type Transaction = []TransactionGroup

// A TransactionGroup is a group of operations included in a Transaction
type TransactionGroup interface {
	Len() int
	Merge(with TransactionGroup) (TransactionGroup, error)
	StorageType() StorageType
	TransactionType() TransactionType
	HandleError(error, int) error
}

// MergeTransactionGroups takes two TransactionGroups to merge together, and returns a new TransactionGroup that
// contains the operations from both TransactionGroups (or either, if one is nil)
func MergeTransactionGroups(group, with TransactionGroup) (TransactionGroup, error) {
	if group == nil {
		return with, nil
	}
	if with == nil {
		return group, nil
	}

	return group.Merge(with)
}

// MergeTransactions takes two Transactions to merge together, and returns a new Transaction that
// contains the groups from both Transactions (or either, if one is nil)
func MergeTransactions(transaction, with Transaction) Transaction {
	if len(transaction) == 0 {
		return with
	}
	if len(with) == 0 {
		return transaction
	}

	return append(transaction, with...)
}
