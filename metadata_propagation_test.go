package evt_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/mem"
	evttest "github.com/photon-grove/evt/test"
)

// regionPtr returns a pointer to the region string, the way callers pass a region.
func regionPtr(region string) *string {
	return &region
}

func Test_Metadata_V09Propagation(t *testing.T) {
	ctx := context.Background()
	repo := mem.NewRepository()
	store := mem.NewStoreFromRepo(repo)
	entity := evttest.NewEntity("e1")

	metadata := evt.NewMetadata(ctx, regionPtr("us-east-1"),
		evt.WithCommandID("cmd-0001"),
		evt.WithPrincipalID("user:42"),
		evt.WithCausationID("cmd-0000"),
		evt.WithExtension("requestSource", "cli"),
	)

	require.NoError(t, store.Execute(ctx, entity, "e1", &evttest.ReplaceEntity{Value: "v1"}, metadata))

	events, err := repo.GetEvents(ctx, "e1")
	require.NoError(t, err)
	require.Len(t, events, 1)

	stored := events[0].Metadata
	require.NotNil(t, stored.PrincipalID)
	require.Equal(t, evt.PrincipalID("user:42"), *stored.PrincipalID)
	require.NotNil(t, stored.CausationID)
	require.Equal(t, evt.CommandID("cmd-0000"), *stored.CausationID)
	require.Equal(t, map[string]string{"requestSource": "cli"}, stored.Extensions)

	// Without the options the stored bytes carry the defaults.
	entity = evttest.NewEntity("e2")
	require.NoError(t, store.Execute(ctx, entity, "e2", &evttest.ReplaceEntity{Value: "v1"}, evt.NewMetadata(ctx, nil)))
	require.NoError(t, store.Execute(ctx, entity, "e2", &evttest.ReplaceEntity{Value: "v2"},
		evt.NewMetadata(ctx, nil, evt.WithCommandID("cmd-0002"))))

	events, err = repo.GetEvents(ctx, "e2")
	require.NoError(t, err)
	require.Len(t, events, 2)

	data, err := json.Marshal(events[0].Metadata)
	require.NoError(t, err)
	require.Contains(t, string(data), `"principalId":null`)
	require.Contains(t, string(data), `"causationId":null`)
	require.Contains(t, string(data), `"extensions":{}`)

	// The second command's causationId defaults to its command ID.
	data, err = json.Marshal(events[1].Metadata)
	require.NoError(t, err)
	require.Contains(t, string(data), `"causationId":"cmd-0002"`)
}
