//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/conformance"
	"github.com/photon-grove/evt/postgres"
)

// A malformed stored outcome row is a corrupt record, never replayed as a result. The table's own
// CHECK constraints refuse some malformations; the rest only the reader can catch.
func Test_MalformedOutcomeRowsAreCorrupt(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)
	repo := postgres.NewRepository(pool)
	table := "evt_events__evt_command_outcomes"

	tests := []struct {
		name string
		sql  string
	}{
		{"empty digest", `INSERT INTO %s (command_id, status, digest, events) VALUES ($1, 'accepted', '', '["a:1"]')`},
		{"event ID without a sequence", `INSERT INTO %s (command_id, status, digest, events) VALUES ($1, 'accepted', 'd', '["a"]')`},
		{"event ID with a zero sequence", `INSERT INTO %s (command_id, status, digest, events) VALUES ($1, 'accepted', 'd', '["a:0"]')`},
		{"events that are not an array of strings", `INSERT INTO %s (command_id, status, digest, events) VALUES ($1, 'accepted', 'd', '{"a":1}')`},
		{"rejected with events", `INSERT INTO %s (command_id, status, digest, events, rejection, code) VALUES ($1, 'rejected', 'd', '["a:1"]', '{"code":"c","message":"m","details":null}', 'c')`},
		{"rejected whose code column disagrees", `INSERT INTO %s (command_id, status, digest, rejection, code) VALUES ($1, 'rejected', 'd', '{"code":"c","message":"m","details":null}', 'other')`},
		{"rejected with an empty rejection code", `INSERT INTO %s (command_id, status, digest, rejection, code) VALUES ($1, 'rejected', 'd', '{"code":"","message":"m","details":null}', '')`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id := evt.CommandID(fmt.Sprintf("%s-malformed", conformance.NewNamespaceForTest()))

			_, err := pool.Exec(ctx, fmt.Sprintf(test.sql, table), string(id))
			require.NoError(t, err)

			stored, err := repo.GetOutcome(ctx, id)
			require.ErrorIs(t, err, evt.ErrCorruptOutcome)
			require.Nil(t, stored)
		})
	}
}
