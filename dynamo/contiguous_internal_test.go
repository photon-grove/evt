package dynamo

import (
	"math"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/stretchr/testify/require"
)

// The maximum sequence has no successor, so a following event must not wrap to a negative one.
func TestCheckContiguousRejectsOverflow(t *testing.T) {
	events := []evt.SerializedEvent{
		{EntityID: "a", Sequence: math.MaxInt},
		{EntityID: "a", Sequence: math.MinInt},
	}

	require.Error(t, checkContiguous(events))
	require.NoError(t, checkContiguous(events[:1]))
}
