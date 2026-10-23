//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"testing"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/dynamo"
	"github.com/photon-grove/evt/go/test"
	"github.com/stretchr/testify/require"
)

const viewsTableName = "evt-local-entity-views"

func (s *DynamoEventsIntegrationSuite) Test_Views_PutGet() {
	ctx := context.Background()
	repo := dynamo.NewViewRepository(s.client, viewsTableName)

	entityID := evt.EntityID(newID())
	entityType := evt.EntityType("ViewEntity-" + newID())

	payload, err := json.Marshal(map[string]string{
		"id":    string(entityID),
		"value": "test-value",
	})
	require.NoError(s.T(), err)

	view := &evt.SerializedView{
		PK:         string(entityID),
		EntityID:   entityID,
		EntityType: entityType,
		Payload:    payload,
	}

	err = repo.PutView(ctx, view)
	require.NoError(s.T(), err)

	fetched, err := repo.GetView(ctx, string(entityID))
	require.NoError(s.T(), err)
	require.NotNil(s.T(), fetched)
	require.Equal(s.T(), view.PK, fetched.PK)
	require.Equal(s.T(), view.EntityID, fetched.EntityID)
	require.Equal(s.T(), view.EntityType, fetched.EntityType)
	require.Equal(s.T(), view.Payload, fetched.Payload)
}

func (s *DynamoEventsIntegrationSuite) Test_Views_GetMissing() {
	ctx := context.Background()
	repo := dynamo.NewViewRepository(s.client, viewsTableName)

	missingPK := newID()

	fetched, err := repo.GetView(ctx, missingPK)
	require.NoError(s.T(), err)
	require.Nil(s.T(), fetched)
}

func (s *DynamoEventsIntegrationSuite) Test_Views_ListByEntityType() {
	ctx := context.Background()
	repo := dynamo.NewViewRepository(s.client, viewsTableName)

	entityType := evt.EntityType("ListEntity-" + newID())
	otherType := evt.EntityType("OtherEntity-" + newID())

	pk1 := newID()
	pk2 := newID()
	pkOther := newID()

	view1 := &evt.SerializedView{PK: pk1, EntityID: evt.EntityID(pk1), EntityType: entityType, Payload: []byte(`{"id":"` + pk1 + `"}`)}
	view2 := &evt.SerializedView{PK: pk2, EntityID: evt.EntityID(pk2), EntityType: entityType, Payload: []byte(`{"id":"` + pk2 + `"}`)}
	viewOther := &evt.SerializedView{PK: pkOther, EntityID: evt.EntityID(pkOther), EntityType: otherType, Payload: []byte(`{"id":"` + pkOther + `"}`)}

	require.NoError(s.T(), repo.PutView(ctx, view1))
	require.NoError(s.T(), repo.PutView(ctx, view2))
	require.NoError(s.T(), repo.PutView(ctx, viewOther))

	views, err := repo.ListViewsByEntityType(ctx, entityType)
	require.NoError(s.T(), err)
	require.NotEmpty(s.T(), views)

	seen := map[string]bool{}
	for _, view := range views {
		require.Equal(s.T(), entityType, view.EntityType)
		seen[view.PK] = true
	}

	require.True(s.T(), seen[pk1])
	require.True(s.T(), seen[pk2])
	require.False(s.T(), seen[pkOther])
}

// Test_Views_CommitThenListPage reads a partition page straight after each commit that changes it
// (put, overwrite, delete) and sees the new state: the partition listing is a strongly consistent
// base-table query, so a request handler can commit and read its own write. The emulator is always
// consistent, so the request shape itself is pinned by Test_ViewRepository_ReadConsistency.
func Test_Views_CommitThenListPage(t *testing.T) {
	ctx := context.Background()
	events, _ := newIsolatedEventLog(ctx, t)
	client := isolatedClient
	viewsTable := newIsolatedViewsTable(ctx, t, client)

	writer := dynamo.NewRepository(client, events.EventsTable).WithViewsTable(viewsTable)
	views := dynamo.NewViewRepository(client, viewsTable)

	id := evt.EntityID("ryw-" + newID())
	pk := "ryw#" + string(id)

	commit := func(sequence evt.EventSequence, op evt.ViewOperation) {
		t.Helper()

		serialized, err := evt.SerializeEvents(
			[]evt.Event{&test.AggregateCreated{ID: id, Value: "v"}}, sequence, id, evt.Metadata{},
		)
		require.NoError(t, err)
		for i := range serialized {
			serialized[i].EntityType = "test"
		}

		require.NoError(t, writer.Commit(ctx, evt.SerializedResult{
			Events: serialized,
			Views:  []evt.ProjectedView{{Projector: "ryw", Op: op}},
		}))
	}

	page := func() []*evt.SerializedView {
		t.Helper()

		result, err := views.ListViewsByPKPaged(ctx, pk, 10, "")
		require.NoError(t, err)

		return result.Views
	}

	commit(0, evt.PutView(pk, "row", id, "test", []byte(`{"v":1}`)))
	rows := page()
	require.Len(t, rows, 1, "the committed row")
	require.JSONEq(t, `{"v":1}`, string(rows[0].Payload))

	commit(1, evt.PutView(pk, "row", id, "test", []byte(`{"v":2}`)))
	rows = page()
	require.Len(t, rows, 1, "the overwritten row")
	require.JSONEq(t, `{"v":2}`, string(rows[0].Payload))

	commit(2, evt.DeleteView(pk, "row", id))
	require.Empty(t, page(), "the deleted row")
}
