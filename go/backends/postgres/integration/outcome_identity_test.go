//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/postgres"
)

// An outcomes table created before outcomes recorded their identity gains the columns on the next
// EnsureSchema: its rows read back with no identity, and new rows record theirs.
func Test_OutcomeIdentityColumnsUpgradeAnExistingTable(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)

	base := fmt.Sprintf("p%x", time.Now().UnixNano())
	repo := postgres.NewRepository(pool, postgres.WithEventsTable(base), postgres.WithSnapshotsTable(base+"s"))
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, fmt.Sprintf( //nolint:errcheck // test-only teardown
			"DROP TABLE IF EXISTS %[1]s, %[1]ss, %[1]s__evt_command_outcomes, %[1]s__evt_features, %[1]s__evt_rebuild_lock, %[1]s__evt_rebuild_generation, %[1]s__evt_stream_heads",
			base))
	})
	require.NoError(t, repo.EnsureSchema(ctx))

	// Back to the table's earlier shape, with a row written by an older writer.
	_, err := pool.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %[1]s__evt_command_outcomes DROP COLUMN command_type, DROP COLUMN targets, DROP COLUMN principal;
		 INSERT INTO %[1]s__evt_command_outcomes (command_id, status, digest) VALUES ('legacy', 'accepted', 'legacy-digest')`,
		base))
	require.NoError(t, err)

	require.NoError(t, repo.EnsureSchema(ctx))
	require.NoError(t, repo.EnsureSchema(ctx), "the upgrade is idempotent")

	legacy, err := repo.GetOutcome(ctx, "legacy")
	require.NoError(t, err)
	require.NotNil(t, legacy)
	require.Nil(t, legacy.Identity, "an older row records no identity")

	principal := evt.PrincipalID("user:42")
	identity := &evt.RecordedIdentity{Type: "account:open", Targets: []evt.EntityID{"acct-1"}, Principal: &principal}
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Outcome: &evt.Outcome{CommandID: "recorded", Status: evt.OutcomeAccepted, Digest: "d", Identity: identity},
	}))

	recorded, err := repo.GetOutcome(ctx, "recorded")
	require.NoError(t, err)
	require.Equal(t, identity, recorded.Identity)
}
