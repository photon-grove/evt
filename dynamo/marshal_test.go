package dynamo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_marshalMap_RejectsNonMapAttributeValue(t *testing.T) {
	repo := NewRepository(nil, "events")

	// A scalar encodes to a non-M AttributeValue. It must be rejected with an error
	// rather than returning a nil map with a nil error.
	_, err := repo.marshalMap(42)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected AttributeValue type")
}

func Test_marshalMap_EncodesStruct(t *testing.T) {
	repo := NewRepository(nil, "events")

	item, err := repo.marshalMap(struct {
		A string `json:"a"`
	}{A: "x"})
	require.NoError(t, err)
	require.Contains(t, item, "a")
}
