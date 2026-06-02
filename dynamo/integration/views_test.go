//go:build integration

package integration

import (
	"context"
	"encoding/json"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
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
