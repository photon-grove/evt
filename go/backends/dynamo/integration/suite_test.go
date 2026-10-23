//go:build integration

// Package integration provides integration tests for the Dynamo event sourcing Repository
package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/dynamo"
	"github.com/photon-grove/evt/go/snapshots"
	"github.com/photon-grove/evt/go/test"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// callTimeout bounds every DynamoDB call the integration tests make.
const callTimeout = 30 * time.Second

// DynamoEventsIntegrationSuite tests DynamoDB against the local AWS emulator
type DynamoEventsIntegrationSuite struct {
	suite.Suite
	client     *dynamodb.Client
	repo       *dynamo.Repository
	awsRegion  string
	store      evt.Store
	entityType evt.EntityType
	entityID   evt.EntityID
	aggregate  *test.Aggregate
	cc         evt.CommandContext
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

	endpoint := resolveLocalEndpoint()
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}

	awsCfg, err := newAWSConfig(ctx, awsRegion, endpoint)
	require.NoError(s.T(), err)
	client := dynamodb.NewFromConfig(*awsCfg)

	s.client = client
}

// SetupTest gives every test an event log of its own. The emulator copies each table a
// transaction touches so it can roll the transaction back, which makes every commit cost the size
// of the whole table: tests sharing one table (and the stream the Terraform table enables) slow
// each other down and balloon the emulator's memory.
func (s *DynamoEventsIntegrationSuite) SetupTest() {
	repo, _ := newIsolatedEventLog(context.Background(), s.T())
	s.repo = repo
	s.client = isolatedClient
}

func (s *DynamoEventsIntegrationSuite) SetupAggregate(entityID evt.EntityID, snapshotSize int) {
	ctx := context.Background()

	s.store = snapshots.NewStore(s.repo, snapshotSize)
	s.entityID = entityID
	s.aggregate = test.NewAggregate(entityID)
	s.entityType = s.aggregate.Type()

	cc, err := s.store.LoadAggregate(ctx, s.aggregate, entityID)
	require.NoError(s.T(), err)

	s.cc = cc
}

func (s *DynamoEventsIntegrationSuite) getMetadata(ctx context.Context) evt.Metadata {
	metadata := evt.NewMetadata(
		ctx,
		&s.awsRegion,
		evt.WithOrigin(evt.Origin{Source: "testing", Endpoint: "Testing"}),
	)

	// Decoded events carry an empty, non-nil extensions map.
	metadata.Extensions = map[string]string{}

	return metadata
}

func resolveLocalEndpoint() string {
	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		return endpoint
	}

	return os.Getenv("LOCALSTACK_ENDPOINT")
}

func newAWSConfig(ctx context.Context, region, localEndpoint string) (*aws.Config, error) {
	if localEndpoint == "" {
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			return nil, err
		}

		return &cfg, nil
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithRetryMaxAttempts(1),
		// No call outlives callTimeout: a stalled emulator fails the test that hit it.
		config.WithHTTPClient(awshttp.NewBuildableClient().WithTimeout(callTimeout)),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				SessionToken:    "test",
				Source:          "local emulator credentials",
			},
		}),
		config.WithBaseEndpoint(localEndpoint),
	)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
