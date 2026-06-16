//go:build integration

package integration

import (
	"context"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/projectors"
	"github.com/stretchr/testify/require"
)

const headsTable = "evt-local-entity-heads"

// TestHeadStore_ProjectAndRead exercises the heads projector + reader against the local emulator,
// verifying the real DynamoDB monotonic-update semantics: advancing sequences set the head, while
// stale and out-of-order deliveries fail the condition and are treated as no-ops (not errors).
//
// The heads table is shared across runs, but the monotonic upsert is idempotent, so fixed entity
// IDs converge to the same head however many times this runs; assertions read specific IDs rather
// than the whole table to stay isolated from other tests' rows.
func (s *DynamoEventsIntegrationSuite) TestHeadStore_ProjectAndRead() {
	ctx := context.Background()
	store := dynamo.NewHeadStore(s.client, headsTable)

	id := evt.EntityID("heads-it-project")

	rec := func(seq int) projectors.StreamRecord {
		return projectors.StreamRecord{
			EventID:    string(id) + ":" + string(rune('0'+seq)),
			EntityID:   string(id),
			EntityType: "heads-it-widget",
			Sequence:   seq,
		}
	}

	failures, err := store.Process(ctx, []projectors.StreamRecord{rec(1), rec(3)})
	require.NoError(s.T(), err)
	require.Empty(s.T(), failures)

	// Out-of-order (2 after 3) and a duplicate (3) must be silent no-ops, not failures.
	failures, err = store.Process(ctx, []projectors.StreamRecord{rec(2), rec(3)})
	require.NoError(s.T(), err)
	require.Empty(s.T(), failures)

	heads, err := store.StreamEntityHeads(ctx, "")
	require.NoError(s.T(), err)
	require.Equal(s.T(), evt.EventSequence(3), heads[id])

	// The entityType filter restricts the scan to matching rows.
	typed, err := store.StreamEntityHeads(ctx, "heads-it-widget")
	require.NoError(s.T(), err)
	require.Equal(s.T(), evt.EventSequence(3), typed[id])
}
