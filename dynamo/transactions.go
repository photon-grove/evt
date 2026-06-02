package dynamo

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
)

// Transactions
// ------------

// A Transaction is a collection of operations organized into TransactionGroups that should be
// executed together. If any one operation fails, they should all fail.
type Transaction = []TransactionGroup

// A TransactionGroup is a set of related operations that should be included in a Transaction
type TransactionGroup interface {
	ToWriteItems() []types.TransactWriteItem
	MergeDynamo(with TransactionGroup) (TransactionGroup, error)

	// For compliance with the e.Transaction interface
	TransactionType() evt.TransactionType
	StorageType() evt.StorageType
	Len() int
	HandleError(err error, i int) error
	Merge(with evt.TransactionGroup) (evt.TransactionGroup, error)
}

// StorageType indicates the type of storage used to execute Dynamo transactions
const StorageType evt.StorageType = "DynamoDB"
