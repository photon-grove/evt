package evt_test

import (
	"testing"

	evt "github.com/photon-grove/evt/go"
	"github.com/stretchr/testify/require"
)

func TestReservedNamespacePrefixesCannotChangeGuards(t *testing.T) {
	prefixes := evt.ReservedNamespacePrefixes()
	for i := range prefixes {
		prefixes[i] = "account#"
	}

	for _, id := range []evt.EntityID{"SYSTEM#FEATURES", "command-outcome#cmd"} {
		require.True(t, evt.IsReservedEntityID(id))
		require.True(t, evt.IsReservedEntityIdErr(evt.CheckReservedEntityID(id)))
	}
	require.False(t, evt.IsReservedEntityID("account#1"))
	require.Equal(t, []string{evt.SystemNamespacePrefix, evt.CommandOutcomeNamespacePrefix}, evt.ReservedNamespacePrefixes())
}
