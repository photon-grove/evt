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

func absentMarkerCheck(table string) types.TransactWriteItem {
	return types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
		TableName: aws.String(table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "SYSTEM#FEATURES"},
			"sk": &types.AttributeValueMemberN{Value: "0"},
		},
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	}}
}

// Use the mocked TestDynamoClient to initialize the Dynamo EventRepository
func (s *RepositorySuite) SetupTest() {
	s.client = mock.NewClient()
	s.repo = dynamo.NewRepository(s.client, "test-events")
	// Ordinary repository tests use a table with no feature declarations. Keep this default
	// scoped to the marker key so entity reads and the explicit FeatureMarkerSuite stay strict.
	s.client.On("GetItem", mocktest.Anything, mocktest.MatchedBy(func(in *dynamodb.GetItemInput) bool {
		pk, pkOK := in.Key["pk"].(*types.AttributeValueMemberS)
		sk, skOK := in.Key["sk"].(*types.AttributeValueMemberN)
		return pkOK && skOK && pk.Value == "SYSTEM#FEATURES" && sk.Value == "0" &&
			aws.ToString(in.TableName) == "test-events" && aws.ToBool(in.ConsistentRead)
	}), mocktest.Anything).Return(&dynamodb.GetItemOutput{}, nil).Maybe()
}
