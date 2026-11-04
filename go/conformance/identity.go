package conformance

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/snapshots"
)

// testRecordedIdentity covers the identity an outcome row records beside its digest, and the
// lookup by command ID and principal it backs.
func testRecordedIdentity(t *testing.T, repo evt.OutcomeRepository) {
	t.Run("EveryOutcomeRecordsItsIdentity", func(t *testing.T) { testIdentityRecorded(t, repo) })
	t.Run("LookupByPrincipal", func(t *testing.T) { testResolveForPrincipal(t, repo) })
	t.Run("RowWithoutIdentityFailsClosed", func(t *testing.T) { testIdentityLegacyRow(t, repo) })
	t.Run("IdentityIsBounded", func(t *testing.T) { testIdentityBound(t, repo) })
}

func withPrincipal(id evt.CommandID, principal string) evt.Metadata {
	metadata := withCommand(id)
	value := evt.PrincipalID(principal)
	metadata.PrincipalID = &value

	return metadata
}

func principalOf(value string) *evt.PrincipalID {
	principal := evt.PrincipalID(value)

	return &principal
}

func storedIdentity(t *testing.T, repo evt.OutcomeRepository, id evt.CommandID) *evt.RecordedIdentity {
	t.Helper()

	stored, err := repo.GetOutcome(context.Background(), id)
	require.NoError(t, err)
	require.NotNil(t, stored, "an outcome row for %s", id)

	return stored.Identity
}

func testIdentityRecorded(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newOutcomeEnv(t, repo, 0)

	// A single-aggregate command: its entity is its one target.
	single := env.commandID("single")
	_, err := env.store.ExecuteWithOutcome(ctx, newLedger(env.id), env.id,
		&ledgerCommand{Kind: "deposit", Amount: 1}, withPrincipal(single, "user:42"))
	require.NoError(t, err)
	require.Equal(t, &evt.RecordedIdentity{Type: "ledger:deposit", Targets: []evt.EntityID{env.id}, Principal: principalOf("user:42")},
		storedIdentity(t, repo, single))

	// A transaction's targets are deduplicated and sorted, as the digest takes them, whatever it
	// writes; a command without a principal records none.
	first, second := evt.EntityID(env.ns+"-b"), evt.EntityID(env.ns+"-a")
	multi := env.commandID("multi")
	identity := evt.CommandIdentity{Type: "ledger:copy", Targets: []evt.EntityID{first, second, first}}
	_, err = env.store.ExecuteTransaction(ctx, identity, withCommand(multi), func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
		if readErr := tx.Read(ctx, newLedger(first), first); readErr != nil {
			return nil, readErr
		}

		return nil, tx.Append(first, &ledgerEvent{Kind: "deposited", Amount: 1})
	})
	require.NoError(t, err)
	require.Equal(t, &evt.RecordedIdentity{Type: "ledger:copy", Targets: []evt.EntityID{second, first}},
		storedIdentity(t, repo, multi))

	// A rejection and an outcome-only no-op record theirs too.
	rejected := env.commandID("rejected")
	refused := &domainRejection{evt.Rejection{Code: "taken", Message: "taken"}}
	_, err = env.store.ExecuteTransaction(ctx, evt.CommandIdentity{Type: "ledger:claim", Targets: []evt.EntityID{first}},
		withPrincipal(rejected, "user:7"), func(context.Context, *snapshots.Transaction) ([]byte, error) { return nil, refused })
	require.NoError(t, err)
	require.Equal(t, &evt.RecordedIdentity{Type: "ledger:claim", Targets: []evt.EntityID{first}, Principal: principalOf("user:7")},
		storedIdentity(t, repo, rejected))

	noop := env.commandID("noop")
	_, err = env.store.ExecuteTransaction(ctx, evt.CommandIdentity{Type: "ledger:audit"}, withCommand(noop),
		func(context.Context, *snapshots.Transaction) ([]byte, error) { return nil, nil })
	require.NoError(t, err)
	require.Equal(t, &evt.RecordedIdentity{Type: "ledger:audit", Targets: []evt.EntityID{}}, storedIdentity(t, repo, noop))

	// A replay returns the identity it recorded.
	replayed, err := env.store.ExecuteWithOutcome(ctx, newLedger(env.id), env.id,
		&ledgerCommand{Kind: "deposit", Amount: 1}, withPrincipal(single, "user:42"))
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, storedIdentity(t, repo, single), replayed.Identity)
}

func testResolveForPrincipal(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newOutcomeEnv(t, repo, 0)

	owned, anonymous := env.commandID("owned"), env.commandID("anonymous")

	first, err := env.store.ExecuteWithOutcome(ctx, newLedger(env.id), env.id,
		&ledgerCommand{Kind: "deposit", Amount: 1}, withPrincipal(owned, "user:42"))
	require.NoError(t, err)

	_, err = env.store.ExecuteWithOutcome(ctx, newLedger(env.id), env.id,
		&ledgerCommand{Kind: "deposit", Amount: 2}, withCommand(anonymous))
	require.NoError(t, err)

	found, err := env.store.ResolveCommandForPrincipal(ctx, owned, principalOf("user:42"))
	require.NoError(t, err)
	require.True(t, found.Replayed)
	require.Equal(t, first.Outcome, found.Outcome, "the stored outcome, exactly")

	found, err = env.store.ResolveCommandForPrincipal(ctx, anonymous, nil)
	require.NoError(t, err)
	require.True(t, found.Replayed, "no principal matches no principal")

	for name, ask := range map[string]struct {
		id        evt.CommandID
		principal *evt.PrincipalID
	}{
		"another principal":               {owned, principalOf("user:7")},
		"no principal for a principal's":  {owned, nil},
		"a principal for one without one": {anonymous, principalOf("user:42")},
	} {
		found, err = env.store.ResolveCommandForPrincipal(ctx, ask.id, ask.principal)
		require.ErrorIs(t, err, evt.ErrOutcomePrincipalMismatch, name)
		require.Nil(t, found, "%s: the stored outcome is never returned", name)

		var mismatch *evt.OutcomePrincipalMismatchError
		require.ErrorAs(t, err, &mismatch)
		require.True(t, mismatch.Recorded, name)
		require.Equal(t, ask.id, mismatch.CommandID)
	}

	absent, err := env.store.ResolveCommandForPrincipal(ctx, env.commandID("absent"), principalOf("user:42"))
	require.NoError(t, err)
	require.Nil(t, absent)
}

func testIdentityLegacyRow(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newOutcomeEnv(t, repo, 0)

	// A row written before outcomes recorded their identity: the repository stores what it is
	// given, so an outcome-only commit without an identity stands in for one.
	legacy := env.commandID("legacy")
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Outcome: &evt.Outcome{CommandID: legacy, Status: evt.OutcomeAccepted, Digest: "legacy-digest"},
	}))

	stored, err := repo.GetOutcome(ctx, legacy)
	require.NoError(t, err)
	require.Nil(t, stored.Identity, "a row without the fields decodes with no identity")

	_, err = env.store.ResolveCommandForPrincipal(ctx, legacy, principalOf("user:42"))

	var mismatch *evt.OutcomePrincipalMismatchError
	require.ErrorAs(t, err, &mismatch)
	require.False(t, mismatch.Recorded, "an unrecorded identity fails closed")

	_, err = env.store.ResolveCommandForPrincipal(ctx, legacy, nil)
	require.ErrorIs(t, err, evt.ErrOutcomePrincipalMismatch, "even for a caller with no principal")
}

func testIdentityBound(t *testing.T, repo evt.OutcomeRepository) {
	ctx := context.Background()
	env := newOutcomeEnv(t, repo, 0)

	// Forty targets of 2,000 bytes each: well past the 64 KiB the recorded identity may take.
	targets := make([]evt.EntityID, 0, 40)
	for i := range 40 {
		targets = append(targets, evt.EntityID(env.ns+"-"+strings.Repeat(string(rune('a'+i%26)), 2000)+string(rune('A'+i/26))))
	}

	id := env.commandID("large")
	_, err := env.store.ExecuteTransaction(ctx, evt.CommandIdentity{Type: "ledger:audit", Targets: targets}, withCommand(id),
		func(context.Context, *snapshots.Transaction) ([]byte, error) { return nil, nil })

	var tooLarge *evt.OutcomeTooLargeError
	require.ErrorAs(t, err, &tooLarge)
	require.Equal(t, "identity", tooLarge.What)

	stored, err := repo.GetOutcome(ctx, id)
	require.NoError(t, err)
	require.Nil(t, stored, "nothing is written")
}
