//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

	marker := dynamo.NewRepository(client, "evt-local-event-log").FeatureMarker(nil)

	conformance.RunRepositorySuite(t, newRepo, conformance.SuiteOptions{
		SupportsSnapshots:             true,
		EnforcesOptimisticConcurrency: true,
		SupportsTransactions:          true,
		SupportsOutcomes:              true,
		ProjectorGroup: func(operations int) evt.TransactionGroup {
			items := make([]types.TransactWriteItem, 0, operations)
			for i := range operations {
				items = append(items, types.TransactWriteItem{Put: &types.Put{
					TableName: aws.String("evt-local-entity-views"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("conformance-budget#%d#%d", time.Now().UnixNano(), i)},
						"sk": &types.AttributeValueMemberS{Value: "VIEW"},
					},
				}})
			}

			return dynamo.NewViewPutGroup("evt-local-entity-views", items)
		},
		DeclareRetention: func(ctx context.Context, _ evt.Repository, entityType evt.EntityType) error {
			_, err := marker.Declare(ctx, func(manifest *evt.FeatureManifest) {
				manifest.Retention = append(manifest.Retention, entityType)
			})

			return err
		},
	})
}
