//go:build integration

package integration

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *DynamoEventsIntegrationSuite) Test_Schema_EventItemStructure() {
	ctx := context.Background()
	s.SetupEntity(evt.EntityID(newID()), 2)
	metadata := s.getMetadata(ctx)

	// Create and commit an event
	result, err := s.entity.Handle(ctx, &test.CreateEntity{Value: "test-value"})
	require.NoError(s.T(), err)
	_, err = s.store.Commit(ctx, result, s.eventContext, metadata)
	require.NoError(s.T(), err)

	// Fetch directly from DynamoDB to verify schema
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("evt-local-event-log"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: string(s.entityID)},
			"sk": &types.AttributeValueMemberN{Value: "1"}, // Sequence 1
		},
	})
	require.NoError(s.T(), err)
	require.NotNil(s.T(), output.Item)

	item := output.Item

	// Validate PK/SK
	pk, ok := item["pk"].(*types.AttributeValueMemberS)
	require.True(s.T(), ok)
	assert.Equal(s.T(), string(s.entityID), pk.Value)

	sk, ok := item["sk"].(*types.AttributeValueMemberN)
	require.True(s.T(), ok)
	assert.Equal(s.T(), "1", sk.Value)

	// Validate other fields
	typ, ok := item["type"].(*types.AttributeValueMemberS)
	require.True(s.T(), ok)
	assert.Equal(s.T(), string(test.CreatedEvent), typ.Value)

	ver, ok := item["version"].(*types.AttributeValueMemberN)
	require.True(s.T(), ok)
	assert.Equal(s.T(), "1", ver.Value)

	entType, ok := item["entityType"].(*types.AttributeValueMemberS)
	require.True(s.T(), ok)
	assert.Equal(s.T(), string(test.EntityType), entType.Value)

	// Validate Payload is JSON string
	payloadAttr, ok := item["payload"].(*types.AttributeValueMemberS)
	require.True(s.T(), ok)
	payloadStr := payloadAttr.Value
	var payloadMap map[string]interface{}
	err = json.Unmarshal([]byte(payloadStr), &payloadMap)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "test-value", payloadMap["value"])
	assert.Equal(s.T(), string(s.entityID), payloadMap["id"])

	// Validate Metadata is JSON string
	metaAttr, ok := item["metadata"].(*types.AttributeValueMemberS)
	require.True(s.T(), ok)
	metaStr := metaAttr.Value
	var metaMap map[string]interface{}
	err = json.Unmarshal([]byte(metaStr), &metaMap)
	require.NoError(s.T(), err)
	// Origin field uses json:"origin" tag
	origin, ok := metaMap["origin"].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "testing", origin["source"])
}

func (s *DynamoEventsIntegrationSuite) Test_Schema_SnapshotItemStructure() {
	ctx := context.Background()
	// Snapshot size 2. We need 2 events to trigger snapshot.
	s.SetupEntity(evt.EntityID(newID()), 2)
	metadata := s.getMetadata(ctx)

	// Use Execute to ensure proper state/sequence handling
	// Event 1
	err := s.store.Execute(ctx, s.entity, s.entityID, &test.CreateEntity{Value: "val1"}, metadata)
	require.NoError(s.T(), err)

	// Event 2 (triggers snapshot)
	err = s.store.Execute(ctx, s.entity, s.entityID, &test.ReplaceEntity{Value: "val2"}, metadata)
	require.NoError(s.T(), err)

	// Fetch inline snapshot from the event-log table at sk=0
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("evt-local-event-log"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: string(s.entityID)},
			"sk": &types.AttributeValueMemberN{Value: "0"},
		},
		ConsistentRead: aws.Bool(true),
	})
	require.NoError(s.T(), err)
	require.NotNil(s.T(), output.Item, "Snapshot should exist")

	item := output.Item

	// Validate Schema
	pk, ok := item["pk"].(*types.AttributeValueMemberS)
	require.True(s.T(), ok)
	assert.Equal(s.T(), string(s.entityID), pk.Value)

	sk, ok := item["sk"].(*types.AttributeValueMemberN)
	require.True(s.T(), ok)
	assert.Equal(s.T(), "0", sk.Value) // Inline snapshot at sk=0

	seq, ok := item["seq"].(*types.AttributeValueMemberN)
	require.True(s.T(), ok)
	assert.Equal(s.T(), "1", seq.Value) // First snapshot

	eventSeq, ok := item["eventSeq"].(*types.AttributeValueMemberN)
	require.True(s.T(), ok)
	assert.Equal(s.T(), "2", eventSeq.Value) // At event sequence 2

	entType, ok := item["entityType"].(*types.AttributeValueMemberS)
	require.True(s.T(), ok)
	assert.Equal(s.T(), string(test.EntityType), entType.Value)

	// Validate Payload
	payloadAttr, ok := item["payload"].(*types.AttributeValueMemberS)
	require.True(s.T(), ok)
	payloadStr := payloadAttr.Value
	var payloadMap map[string]interface{}
	err = json.Unmarshal([]byte(payloadStr), &payloadMap)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "val2", payloadMap["value"])
}

func (s *DynamoEventsIntegrationSuite) Test_Schema_OptimisticLocking() {
	ctx := context.Background()
	s.SetupEntity(evt.EntityID(newID()), 2)
	metadata := s.getMetadata(ctx)

	// Commit event 1
	res1, err := s.entity.Handle(ctx, &test.CreateEntity{Value: "val1"})
	require.NoError(s.T(), err)
	_, err = s.store.Commit(ctx, res1, s.eventContext, metadata)
	require.NoError(s.T(), err)

	// Try to commit event 1 AGAIN (duplicate sequence)
	_, err = s.store.Commit(ctx, res1, s.eventContext, metadata)
	require.Error(s.T(), err)

	// Verify it is a race condition error (wrapper around ConditionalCheckFailedException)
	assert.Contains(s.T(), err.Error(), "race condition")
}
