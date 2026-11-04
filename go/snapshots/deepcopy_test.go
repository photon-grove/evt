package snapshots

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
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

	dup := deepCopy(reflect.ValueOf(original), copied{}).Interface().(embeddingAggregate)
	dup.Labels["a"] = "changed"
	dup.Tags[0] = "changed"

	require.Equal(t, "1", original.Labels["a"], "a promoted map must not be aliased")
	require.Equal(t, "x", original.Tags[0], "a promoted slice must not be aliased")
	require.Equal(t, "n", dup.Name)
}

func TestDeepCopy_ASelfReferencingMapOrSliceTerminates(t *testing.T) {
	selfMap := map[string]any{}
	selfMap["self"] = selfMap

	mapCopy := deepCopy(reflect.ValueOf(selfMap), copied{}).Interface().(map[string]any)
	require.Equal(t, reflect.ValueOf(mapCopy).Pointer(), reflect.ValueOf(mapCopy["self"]).Pointer(),
		"the copy refers to itself, as the original does")
	require.NotEqual(t, reflect.ValueOf(selfMap).Pointer(), reflect.ValueOf(mapCopy).Pointer())

	selfSlice := []any{nil}
	selfSlice[0] = selfSlice

	sliceCopy := deepCopy(reflect.ValueOf(selfSlice), copied{}).Interface().([]any)
	require.Equal(t, reflect.ValueOf(sliceCopy).Pointer(), reflect.ValueOf(sliceCopy[0]).Pointer())
	require.NotEqual(t, reflect.ValueOf(selfSlice).Pointer(), reflect.ValueOf(sliceCopy).Pointer())
}
