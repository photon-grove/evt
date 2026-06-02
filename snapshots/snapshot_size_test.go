package snapshots

import (
	"testing"

	"github.com/photon-grove/evt"
	"github.com/stretchr/testify/require"
)

func Test_WithSnapshotSize_Override(t *testing.T) {
	store := NewStore(nil, 5)
	store.WithSnapshotSize(evt.EntityType("portfolio"), 20)

	size := store.effectiveSnapshotSize(evt.EntityType("portfolio"))
	require.Equal(t, 20, size)
}

func Test_WithSnapshotSize_FallbackToDefault(t *testing.T) {
	store := NewStore(nil, 5)
	store.WithSnapshotSize(evt.EntityType("portfolio"), 20)

	size := store.effectiveSnapshotSize(evt.EntityType("order"))
	require.Equal(t, 5, size)
}

func Test_WithSnapshotSize_MultipleOverrides(t *testing.T) {
	store := NewStore(nil, 5)
	store.WithSnapshotSize(evt.EntityType("portfolio"), 20).
		WithSnapshotSize(evt.EntityType("order"), 10)

	require.Equal(t, 20, store.effectiveSnapshotSize(evt.EntityType("portfolio")))
	require.Equal(t, 10, store.effectiveSnapshotSize(evt.EntityType("order")))
	require.Equal(t, 5, store.effectiveSnapshotSize(evt.EntityType("unknown")))
}

func Test_WithSnapshotSize_OverrideReplace(t *testing.T) {
	store := NewStore(nil, 5)
	store.WithSnapshotSize(evt.EntityType("portfolio"), 20)
	store.WithSnapshotSize(evt.EntityType("portfolio"), 30)

	require.Equal(t, 30, store.effectiveSnapshotSize(evt.EntityType("portfolio")))
}
