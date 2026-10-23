package dynamo

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	evt "github.com/photon-grove/evt/go"
)

// Transactions
// ------------

// A TransactionGroup is a set of related operations that should be included in an evt.Transaction
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
