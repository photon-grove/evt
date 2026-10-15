//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/stretchr/testify/require"
)

func Test_FeatureMarkerRefusesRetargetedRepository(t *testing.T) {
	ctx := context.Background()
	source, marker := newIsolatedEventLog(ctx, t)
	writer := source.WithFeatureMarker(marker)
	commitOne(ctx, t, writer, "account", 0) // Populate the marker cache before retargeting.
	destination, declaration := newIsolatedEventLog(ctx, t)
	_, err := declaration.Declare(ctx, func(m *evt.FeatureManifest) { m.Features[evt.FeatureJournal] = 1 })
	require.NoError(t, err)

	writer.EventsTable = destination.EventsTable
	events, err := source.GetEvents(ctx, "account")
	require.NoError(t, err)
	var mismatch *evt.FeatureMarkerTableMismatchError
	require.ErrorAs(t, writer.Commit(ctx, evt.SerializedResult{Events: events}), &mismatch)
	require.ErrorAs(t, writer.PutSnapshot(ctx, "account", "account", []byte(`{}`), 1, 1), &mismatch)
	_, err = writer.CompactBelow(ctx, "account", 1)
	require.ErrorAs(t, err, &mismatch)
	stored, err := destination.GetEvents(ctx, "account")
	require.NoError(t, err)
	require.Empty(t, stored)
	snapshot, err := destination.GetSnapshot(ctx, "account")
	require.NoError(t, err)
	require.Nil(t, snapshot)
}
