//go:build integration

package integration

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/conformance"
	"github.com/photon-grove/evt/dynamo"
	"github.com/stretchr/testify/require"
)

// Test_RepositoryConformance runs the backend-neutral storage contract suite against the DynamoDB
// repository via the local AWS emulator. The suite namespaces its data per run, so it is safe to
// share the integration event-log table with the other integration tests. DynamoDB enforces
// optimistic concurrency through conditional writes and supports snapshots, so both options are on.
func Test_RepositoryConformance(t *testing.T) {
	ctx := context.Background()

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-west-2"
	}

	endpoint := resolveLocalEndpoint()
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}

	cfg, err := newAWSConfig(ctx, region, endpoint)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(*cfg)

	newRepo := func() evt.Repository {
		return dynamo.NewRepository(client, "evt-local-event-log")
	}

	conformance.RunRepositorySuite(t, newRepo, conformance.SuiteOptions{
		SupportsSnapshots:             true,
		EnforcesOptimisticConcurrency: true,
	})
}
