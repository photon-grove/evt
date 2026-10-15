package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClassifyReserved(t *testing.T) {
	stream, ok := classifyReserved("SYSTEM#account", "SYSTEM#", 7)
	require.True(t, ok)
	require.Equal(t, reservedStream{prefix: "SYSTEM#", id: "account", rows: 7}, stream)

	stream, ok = classifyReserved("command-outcome#cmd-1", "command-outcome#", 3)
	require.True(t, ok)
	require.Equal(t, reservedStream{prefix: "command-outcome#", id: "cmd-1", rows: 3}, stream)

	// Ordinary entity IDs are not reserved.
	_, ok = classifyReserved("account#1", "SYSTEM#", 1)
	require.False(t, ok)

	// Engine-written marker rows are not pre-existing streams.
	for _, engineID := range []string{"SYSTEM#FEATURES", "SYSTEM#REBUILD"} {
		_, ok = classifyReserved(engineID, "SYSTEM#", 1)
		require.False(t, ok, engineID)
	}
}
