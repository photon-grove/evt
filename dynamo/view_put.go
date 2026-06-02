package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/photon-grove/evt"
)

const viewPutTransactionType evt.TransactionType = "DynamoViewPut"

// ViewPutGroup batches DynamoDB put operations for entity views.
type ViewPutGroup struct {
	tableName string
	items     []types.TransactWriteItem
}

// NewViewPutGroup creates a new view put group for the given table name.
func NewViewPutGroup(tableName string, items []types.TransactWriteItem) *ViewPutGroup {
	return &ViewPutGroup{tableName: tableName, items: items}
}

// ToWriteItems exposes the DynamoDB transactions for this group.
func (g *ViewPutGroup) ToWriteItems() []types.TransactWriteItem {
	if g == nil {
		return nil
	}

	return g.items
}

// Merge implements the generic evt.TransactionGroup interface.
func (g *ViewPutGroup) Merge(with evt.TransactionGroup) (evt.TransactionGroup, error) {
	if with == nil {
		return g, nil
	}

	other, ok := with.(*ViewPutGroup)
	if !ok {
		return nil, fmt.Errorf("cannot merge view group with %T", with)
	}

	return g.merge(other)
}

// MergeDynamo merges two Dynamo-backed transaction groups.
func (g *ViewPutGroup) MergeDynamo(with TransactionGroup) (TransactionGroup, error) {
	if with == nil {
		return g, nil
	}

	other, ok := with.(*ViewPutGroup)
	if !ok {
		return nil, fmt.Errorf("cannot merge view group with %T", with)
	}

	return g.merge(other)
}

// TransactionType identifies this transaction group.
func (g *ViewPutGroup) TransactionType() evt.TransactionType {
	return viewPutTransactionType
}

// StorageType identifies the backing store.
func (g *ViewPutGroup) StorageType() evt.StorageType {
	return StorageType
}

// Len returns the number of write operations in the group.
func (g *ViewPutGroup) Len() int {
	if g == nil {
		return 0
	}

	return len(g.items)
}

// HandleError returns the original error for Dynamo transactional failures.
func (g *ViewPutGroup) HandleError(err error, _ int) error {
	return err
}

func (g *ViewPutGroup) merge(other *ViewPutGroup) (*ViewPutGroup, error) {
	if g == nil {
		return other, nil
	}

	if other == nil {
		return g, nil
	}

	if g.tableName != other.tableName {
		return nil, fmt.Errorf("cannot merge view groups targeting different tables (%s vs %s)", g.tableName, other.tableName)
	}

	merged := &ViewPutGroup{
		tableName: g.tableName,
		items:     make([]types.TransactWriteItem, 0, len(g.items)+len(other.items)),
	}

	merged.items = append(merged.items, g.items...)
	merged.items = append(merged.items, other.items...)

	return merged, nil
}
