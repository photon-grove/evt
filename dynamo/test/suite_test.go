package test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	mocktest "github.com/stretchr/testify/mock"

	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/dynamo/mock" //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	"github.com/stretchr/testify/suite"
)

// RepositorySuite is the Dynamo EventRepository unit test suite
type RepositorySuite struct {
	suite.Suite
	client *mock.Client
	repo   *dynamo.Repository
}

// Run the Dynamo EventRepository suite
func Test_RepositorySuite(t *testing.T) {
	suite.Run(t, new(RepositorySuite))
}

// declaredMarker is the feature marker of a store that declares stream-heads, the only state a
// head-aware writer writes to (ADR 0007).
func declaredMarker() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk":       &types.AttributeValueMemberS{Value: "SYSTEM#FEATURES"},
		"sk":       &types.AttributeValueMemberN{Value: "0"},
		"version":  &types.AttributeValueMemberN{Value: "1"},
		"features": &types.AttributeValueMemberS{Value: `{"stream-heads":1}`},
	}
}

// markerCheck is the commit's mandatory marker check against the declared marker above.
func markerCheck(table string) types.TransactWriteItem {
	return types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
		TableName: aws.String(table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "SYSTEM#FEATURES"},
			"sk": &types.AttributeValueMemberN{Value: "0"},
		},
		ConditionExpression: aws.String("attribute_not_exists(pk) OR version = :v"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":v": &types.AttributeValueMemberN{Value: "1"},
		},
	}}
}

// rebuildLockCheck is the commit's mandatory rebuild-lock check, the second action of every commit.
func rebuildLockCheck(table string) types.TransactWriteItem {
	return types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
		TableName: aws.String(table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "SYSTEM#REBUILD"},
			"sk": &types.AttributeValueMemberN{Value: "0"},
		},
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	}}
}

// controlChecks are the two control checks that open every commit.
func controlChecks(table string) []types.TransactWriteItem {
	return []types.TransactWriteItem{markerCheck(table), rebuildLockCheck(table)}
}

// Use the mocked TestDynamoClient to initialize the Dynamo EventRepository
func (s *RepositorySuite) SetupTest() {
	s.client = mock.NewClient()
	s.repo = dynamo.NewRepository(s.client, "test-events").WithLazyInitialization(true)
	// Ordinary repository tests use a store that declares stream-heads and nothing else. Keep this
	// default scoped to the marker key so entity reads and the explicit FeatureMarkerSuite stay strict.
	s.client.On("GetItem", mocktest.Anything, mocktest.MatchedBy(func(in *dynamodb.GetItemInput) bool {
		pk, pkOK := in.Key["pk"].(*types.AttributeValueMemberS)
		sk, skOK := in.Key["sk"].(*types.AttributeValueMemberN)
		return pkOK && skOK && pk.Value == "SYSTEM#FEATURES" && sk.Value == "0" &&
			aws.ToString(in.TableName) == "test-events" && aws.ToBool(in.ConsistentRead)
	}), mocktest.Anything).Return(&dynamodb.GetItemOutput{Item: declaredMarker()}, nil).Maybe()
}
