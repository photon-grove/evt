package dynamo

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	evttest "github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

type badJSONEntity struct {
	evt.BaseEntity
	F func() `json:"f"`
}

func (e *badJSONEntity) Type() evt.EntityType { return "bad" }
func (e *badJSONEntity) GetID() evt.EntityID  { return e.ID }
func (e *badJSONEntity) Base() evt.BaseEntity { return e.BaseEntity }
func (e *badJSONEntity) EventUpcasters() []evt.EventUpcaster {
	return nil
}
func (e *badJSONEntity) Projectors() []evt.EventProjector { return nil }
func (e *badJSONEntity) Handle(context.Context, evt.Command) (evt.CommandResult, error) {
	return evt.CommandResult{}, nil
}
func (e *badJSONEntity) Apply(evt.Event) error { return nil }
func (e *badJSONEntity) DeserializeEvent(evt.SerializedEvent) (evt.Event, error) {
	return nil, nil
}

type emptyIDEntity struct{}

func (e emptyIDEntity) Type() evt.EntityType                { return "x" }
func (e emptyIDEntity) GetID() evt.EntityID                 { return "" }
func (e emptyIDEntity) Base() evt.BaseEntity                { return evt.BaseEntity{} }
func (e emptyIDEntity) EventUpcasters() []evt.EventUpcaster { return nil }
func (e emptyIDEntity) Projectors() []evt.EventProjector    { return nil }
func (e emptyIDEntity) Handle(context.Context, evt.Command) (evt.CommandResult, error) {
	return evt.CommandResult{}, nil
}
func (e emptyIDEntity) Apply(evt.Event) error                                   { return nil }
func (e emptyIDEntity) DeserializeEvent(evt.SerializedEvent) (evt.Event, error) { return nil, nil }

type otherTG struct{}

func (otherTG) ToWriteItems() []types.TransactWriteItem                  { return nil }
func (otherTG) MergeDynamo(TransactionGroup) (TransactionGroup, error)   { return nil, nil }
func (otherTG) TransactionType() evt.TransactionType                     { return "Other" }
func (otherTG) StorageType() evt.StorageType                             { return StorageType }
func (otherTG) Len() int                                                 { return 0 }
func (otherTG) HandleError(err error, _ int) error                       { return err }
func (otherTG) Merge(evt.TransactionGroup) (evt.TransactionGroup, error) { return nil, nil }

func Test_ViewProjector_Project_GuardsAndSuccess(t *testing.T) {
	projector := NewViewProjector("views-table")
	p, ok := projector.(*ViewProjector)
	require.True(t, ok)

	// guard rails
	group, err := p.Project(context.Background(), nil, nil)
	require.NoError(t, err)
	require.Nil(t, group)

	group, err = p.Project(context.Background(), emptyIDEntity{}, nil)
	require.NoError(t, err)
	require.Nil(t, group)

	// JSON marshal error
	group, err = p.Project(context.Background(), &badJSONEntity{BaseEntity: evt.NewEntity("e1")}, nil)
	require.Error(t, err)
	require.Nil(t, group)

	// success
	entity := evttest.NewEntity("e2")
	group, err = p.Project(context.Background(), entity, nil)
	require.NoError(t, err)
	require.NotNil(t, group)
	require.Equal(t, viewPutTransactionType, group.TransactionType())
	require.Equal(t, StorageType, group.StorageType())
	require.Equal(t, 1, group.Len())

	tg, ok := group.(TransactionGroup)
	require.True(t, ok)
	writeItems := tg.ToWriteItems()
	require.Len(t, writeItems, 1)
	require.NotNil(t, writeItems[0].Put)
	require.Equal(t, "views-table", *writeItems[0].Put.TableName)
}

func Test_MarshalViewToItem(t *testing.T) {
	encoder := attributevalue.NewEncoder(func(opts *attributevalue.EncoderOptions) { opts.TagKey = tagKey })

	_, err := MarshalViewToItem(encoder, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "serialized view is nil")

	item, err := MarshalViewToItem(encoder, &evt.SerializedView{
		PK:         "pk1",
		EntityID:   "e1",
		EntityType: "t1",
		Payload:    []byte(`{"x":1}`),
	})
	require.NoError(t, err)
	require.Contains(t, item, "pk")
	require.Contains(t, item, "entityID")
	require.Contains(t, item, "entityType")
	require.Contains(t, item, "payload")
}

func Test_ViewPutGroup_MergeAndAccessors(t *testing.T) {
	var nilGroup *ViewPutGroup
	require.Nil(t, nilGroup.ToWriteItems())
	require.Equal(t, 0, nilGroup.Len())

	g1 := NewViewPutGroup("t", []types.TransactWriteItem{{}})
	g2 := NewViewPutGroup("t", []types.TransactWriteItem{{}, {}})

	require.Equal(t, viewPutTransactionType, g1.TransactionType())
	require.Equal(t, StorageType, g1.StorageType())
	require.Equal(t, 1, g1.Len())
	require.Equal(t, 2, g2.Len())
	errX := errors.New("x")
	require.Equal(t, errX, g1.HandleError(errX, 0))

	merged, err := g1.Merge(nil)
	require.NoError(t, err)
	require.Equal(t, g1, merged)

	_, err = g1.Merge(&struct{ evt.TransactionGroup }{})
	require.Error(t, err)

	merged, err = g1.Merge(g2)
	require.NoError(t, err)
	require.Equal(t, 3, merged.Len())

	// MergeDynamo
	mergedD, err := g1.MergeDynamo(nil)
	require.NoError(t, err)
	require.Equal(t, g1, mergedD)

	mergedD, err = g1.MergeDynamo(g2)
	require.NoError(t, err)
	require.Equal(t, 3, mergedD.Len())

	_, err = g1.MergeDynamo(otherTG{})
	require.Error(t, err)

	// table mismatch
	_, err = g1.Merge(NewViewPutGroup("other", nil))
	require.Error(t, err)
	require.Contains(t, err.Error(), "different tables")

	// nil receiver merge path
	merged, err = nilGroup.Merge(g1)
	require.NoError(t, err)
	require.Equal(t, g1, merged)
}
