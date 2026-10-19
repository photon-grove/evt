//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/conformance"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/internal/compat"
)

// Test_RepositoryConformance runs the backend-neutral storage contract suite against the DynamoDB
// repository via the local AWS emulator. The suite namespaces its data per run, and it has an
// event-log table to itself. DynamoDB enforces optimistic concurrency through conditional writes
// and supports snapshots, so both options are on.
func Test_RepositoryConformance(t *testing.T) {
	ctx := context.Background()

	// The suite runs against an event log of its own, with no stream (see SetupTest on the
	// integration suite for why).
	events, marker := newIsolatedEventLog(ctx, t)
	client := isolatedClient

	newRepo := func() evt.Repository {
		return dynamo.NewRepository(client, events.EventsTable)
	}

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
		RefusesCommitStream: true,
		BypassHead: func(ctx context.Context, _ evt.Repository, event evt.SerializedEvent) error {
			return compat.DynamoBypassHead(client, events.EventsTable)(ctx, event)
		},
		Unmigrate: func(ctx context.Context, _ evt.Repository) (func(), error) {
			_, err := marker.Declare(ctx, func(manifest *evt.FeatureManifest) {
				delete(manifest.Features, evt.FeatureStreamHeads)
			})

			return func() {
				// Declared again, the store is migrated: the cases that follow write to it.
				_, _ = marker.DeclareStreamHeads(context.Background()) //nolint:errcheck // best-effort restore of the shared test table
			}, err
		},
	})
}
