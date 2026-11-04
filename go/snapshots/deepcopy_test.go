package snapshots

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
)

type embeddedBase struct {
	Labels map[string]string
	Tags   []string
	hidden map[string]string
}

type embeddingAggregate struct {
	embeddedBase

	Name string
}

func TestDeepCopy_CopiesTheFieldsAnUnexportedEmbeddedStructPromotes(t *testing.T) {
	original := embeddingAggregate{
		embeddedBase: embeddedBase{Labels: map[string]string{"a": "1"}, Tags: []string{"x"}, hidden: map[string]string{}},
		Name:         "n",
	}

	dup, ok := deepCopy(reflect.ValueOf(original), copied{}).Interface().(embeddingAggregate)
	require.True(t, ok)
	dup.Labels["a"] = "changed"
	dup.Tags[0] = "changed"

	require.Equal(t, "1", original.Labels["a"], "a promoted map must not be aliased")
	require.Equal(t, "x", original.Tags[0], "a promoted slice must not be aliased")
	require.Equal(t, "n", dup.Name)
}

func TestDeepCopy_ASelfReferencingMapOrSliceTerminates(t *testing.T) {
	selfMap := map[string]any{}
	selfMap["self"] = selfMap

	mapCopy, ok := deepCopy(reflect.ValueOf(selfMap), copied{}).Interface().(map[string]any)
	require.True(t, ok)
	require.Equal(t, reflect.ValueOf(mapCopy).Pointer(), reflect.ValueOf(mapCopy["self"]).Pointer(),
		"the copy refers to itself, as the original does")
	require.NotEqual(t, reflect.ValueOf(selfMap).Pointer(), reflect.ValueOf(mapCopy).Pointer())

	selfSlice := []any{nil}
	selfSlice[0] = selfSlice

	sliceCopy, ok := deepCopy(reflect.ValueOf(selfSlice), copied{}).Interface().([]any)
	require.True(t, ok)
	require.Equal(t, reflect.ValueOf(sliceCopy).Pointer(), reflect.ValueOf(sliceCopy[0]).Pointer())
	require.NotEqual(t, reflect.ValueOf(selfSlice).Pointer(), reflect.ValueOf(sliceCopy).Pointer())
}

type backReferenceBase struct {
	Owner *backReferencingAggregate
	Self  any
}

// backReferencingAggregate satisfies evt.Aggregate through a nil embedded interface; only its
// fields matter here.
type backReferencingAggregate struct {
	evt.Aggregate
	backReferenceBase

	Count int
}

func TestCaptureAggregate_APointerBackToTheAggregateStaysOne(t *testing.T) {
	aggregate := &backReferencingAggregate{}
	aggregate.Owner = aggregate
	aggregate.Self = aggregate

	reset, ok := captureAggregate(aggregate)
	require.True(t, ok)

	aggregate.Count = 7
	aggregate.Owner = nil
	reset()

	require.Zero(t, aggregate.Count)
	require.Same(t, aggregate, aggregate.Owner, "restored to the caller's aggregate, not a detached copy")
	require.Same(t, aggregate, aggregate.Self)
}
