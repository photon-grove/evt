package evt_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/mem"
	"github.com/photon-grove/evt/go/internal/eventjson"
	evttest "github.com/photon-grove/evt/go/test"
)

// regionPtr returns a pointer to the region string, the way callers pass a region.
func regionPtr(region string) *string {
	return &region
}

func Test_Metadata_V09Propagation(t *testing.T) {
	ctx := context.Background()
	repo := mem.NewRepository()
	store := mem.NewStoreFromRepo(repo)
	aggregate := evttest.NewAggregate("e1")

	metadata := evt.NewMetadata(ctx, regionPtr("us-east-1"),
		evt.WithCommandID("cmd-0001"),
		evt.WithPrincipalID("user:42"),
		evt.WithCausationID("cmd-0000"),
		evt.WithExtension("requestSource", "cli"),
	)

	require.NoError(t, store.Execute(ctx, aggregate, "e1", &evttest.ReplaceAggregate{Value: "v1"}, metadata))

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
	aggregate = evttest.NewAggregate("e2")
	require.NoError(t, store.Execute(ctx, aggregate, "e2", &evttest.ReplaceAggregate{Value: "v1"}, evt.NewMetadata(ctx, nil)))
	require.NoError(t, store.Execute(ctx, aggregate, "e2", &evttest.ReplaceAggregate{Value: "v2"},
		evt.NewMetadata(ctx, nil, evt.WithCommandID("cmd-0002"))))

	events, err = repo.GetEvents(ctx, "e2")
	require.NoError(t, err)
	require.Len(t, events, 2)

	data, err := eventjson.MarshalMetadata(events[0].Metadata)
	require.NoError(t, err)
	require.Contains(t, string(data), `"principalId":null`)
	require.Contains(t, string(data), `"causationId":null`)
	require.Contains(t, string(data), `"extensions":{}`)

	// The second command's causationId defaults to its command ID.
	data, err = eventjson.MarshalMetadata(events[1].Metadata)
	require.NoError(t, err)
	require.Contains(t, string(data), `"causationId":"cmd-0002"`)
}
