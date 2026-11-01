package conformance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
)

// RunViewRepositorySuite runs the evt.ViewRepository cases every backend shares against the
// repository newRepo returns. The cases namespace their keys and entity types, so a durable table
// can be shared across runs.
func RunViewRepositorySuite(t *testing.T, newRepo func() evt.ViewRepository) {
	t.Helper()

	t.Run("ViewRepository/RefusesNUL", func(t *testing.T) {
		testViewRepositoryRefusesNUL(t, newRepo())
	})
}

// testViewRepositoryRefusesNUL checks that U+0000 in any text of a row, or in a key or entity type
// a read names, is ErrInvalidArgument, and that a batch holding such a row writes nothing.
// PostgreSQL text cannot hold U+0000, so every backend refuses it alike.
func testViewRepositoryRefusesNUL(t *testing.T, repo evt.ViewRepository) {
	ctx := context.Background()
	ns := newNamespace()
	pk, entityType := ns+"-a", evt.EntityType(ns+"-t")

	row := func(pk, sk string, owner evt.EntityID, entityType evt.EntityType, payload string) *evt.SerializedView {
		return &evt.SerializedView{PK: pk, SK: sk, EntityID: owner, EntityType: entityType, Payload: []byte(payload)}
	}
	owner := evt.EntityID("owner-" + pk)

	rows := []*evt.SerializedView{
		row(pk+"\x00", evt.DefaultViewSK, owner, entityType, "{}"),
		row(pk, "s\x00k", owner, entityType, "{}"),
		row(pk, evt.DefaultViewSK, "own\x00er", entityType, "{}"),
		row(pk, evt.DefaultViewSK, owner, entityType+"\x00", "{}"),
		row(pk, evt.DefaultViewSK, owner, entityType, "{\"a\":\"\x00\"}"),
	}
	for _, bad := range rows {
		require.ErrorIs(t, repo.PutView(ctx, bad), evt.ErrInvalidArgument, "%q", bad)
	}

	if writer, ok := repo.(interface {
		PutViews(context.Context, []*evt.SerializedView) error
	}); ok {
		good := row(pk, "good", owner, entityType, "{}")
		require.ErrorIs(t, writer.PutViews(ctx, []*evt.SerializedView{good, rows[4]}), evt.ErrInvalidArgument, "a batch with U+0000 in a payload")
	}

	listed, err := repo.ListViewsByPK(ctx, pk)
	require.NoError(t, err)
	require.Empty(t, listed, "nothing was written")

	nulPK := pk + "\x00"
	_, err = repo.GetView(ctx, nulPK)
	require.ErrorIs(t, err, evt.ErrInvalidArgument, "a read of such a pk")

	_, err = repo.ListViewsByPK(ctx, nulPK)
	require.ErrorIs(t, err, evt.ErrInvalidArgument, "a listing of such a pk")

	_, err = repo.ListViewsByPKPaged(ctx, nulPK, 1, "")
	require.ErrorIs(t, err, evt.ErrInvalidArgument, "a page of such a pk")

	_, err = repo.ListViewsByEntityType(ctx, entityType+"\x00")
	require.ErrorIs(t, err, evt.ErrInvalidArgument, "a listing of such an entity type")

	_, err = repo.ListViewsByEntityTypePaged(ctx, entityType+"\x00", 1, "")
	require.ErrorIs(t, err, evt.ErrInvalidArgument, "a page of such an entity type")

	if deleter, ok := repo.(interface {
		DeleteView(context.Context, string, string) error
	}); ok {
		require.ErrorIs(t, deleter.DeleteView(ctx, nulPK, evt.DefaultViewSK), evt.ErrInvalidArgument, "a delete of such a pk")
	}

	if reader, ok := repo.(interface {
		BatchGetViews(context.Context, []string) ([]*evt.SerializedView, error)
	}); ok {
		_, err = reader.BatchGetViews(ctx, []string{pk, nulPK})
		require.ErrorIs(t, err, evt.ErrInvalidArgument, "a batch read naming such a pk")
	}
}
