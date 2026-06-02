//go:build integration

// Package integration provides integration tests for the Dynamo event sourcing Repository
package integration

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/awsclients"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/snapshots"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// DynamoEventsIntegrationSuite tests DynamoDB against the local AWS emulator
type DynamoEventsIntegrationSuite struct {
	suite.Suite
	client       *dynamodb.Client
	repo         *dynamo.Repository
	awsRegion    string
	store        evt.Store
	entityType   evt.EntityType
	entityID     evt.EntityID
	entity       *test.Entity
	eventContext evt.Context
}

// TestDynamoEventsIntegrationSuite runs the test suite
func Test_DynamoEventsIntegrationSuite(t *testing.T) {
	suite.Run(t, new(DynamoEventsIntegrationSuite))
}

func (s *DynamoEventsIntegrationSuite) SetupSuite() {
	ctx := context.Background()

	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		awsRegion = "us-west-2"
	}
	s.awsRegion = awsRegion

	endpoint := awsclients.ResolveLocalEndpoint()
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}

	awsCfg, err := awsclients.NewConfig(ctx, awsRegion, endpoint)
	require.NoError(s.T(), err)
	client := dynamodb.NewFromConfig(*awsCfg)

	s.client = client
}

func (s *DynamoEventsIntegrationSuite) SetupTest() {
	s.repo = dynamo.NewRepository(s.client, "evt-local-event-log")
}

func (s *DynamoEventsIntegrationSuite) SetupEntity(entityID evt.EntityID, snapshotSize int) {
	ctx := context.Background()

	s.store = snapshots.NewStore(s.repo, snapshotSize)
	s.entityID = entityID
	s.entity = test.NewEntity(entityID)
	s.entityType = s.entity.Type()

	eventContext, err := s.store.LoadEntity(ctx, s.entity, entityID)
	require.NoError(s.T(), err)

	s.eventContext = eventContext
}

func (s *DynamoEventsIntegrationSuite) getMetadata(ctx context.Context) evt.Metadata {
	return evt.NewMetadata(
		ctx,
		&s.awsRegion,
		evt.WithOrigin(evt.Origin{Source: "testing", Endpoint: "Testing"}),
	)
}
