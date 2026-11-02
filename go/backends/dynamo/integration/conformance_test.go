//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/dynamo"
	"github.com/photon-grove/evt/go/conformance"
	"github.com/photon-grove/evt/go/internal/compat"
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

	// Commit-wide view operations go to a views table of their own, which the suite reads back.
	viewsTable := newIsolatedViewsTable(ctx, t, client)

	newRepo := func() evt.Repository {
		return dynamo.NewRepository(client, events.EventsTable).WithLazyInitialization(true).WithViewsTable(viewsTable)
	}

	conformance.RunRepositorySuite(t, newRepo, conformance.SuiteOptions{
		SupportsSnapshots:             true,
		EnforcesOptimisticConcurrency: true,
		SupportsTransactions:          true,
		SupportsOutcomes:              true,
		SupportsViews:                 true,
		ReadView: func(ctx context.Context, _ evt.Repository, pk, sk string) (*evt.SerializedView, error) {
			output, err := client.GetItem(ctx, &dynamodb.GetItemInput{
				TableName:      aws.String(viewsTable),
				ConsistentRead: aws.Bool(true),
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: pk},
					"sk": &types.AttributeValueMemberS{Value: sk},
				},
			})
			if err != nil || len(output.Item) == 0 {
				return nil, err
			}

			text := func(name string) string {
				if value, ok := output.Item[name].(*types.AttributeValueMemberS); ok {
					return value.Value
				}

				return ""
			}

			return &evt.SerializedView{
				PK: pk, SK: sk, EntityID: evt.EntityID(text("entityID")), EntityType: evt.EntityType(text("entityType")),
				Payload: []byte(text("payload")),
			}, nil
		},
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
		WithRebuildClock: func(repo evt.Repository, now func() time.Time) evt.Repository {
			return repo.(*dynamo.Repository).WithClock(now) //nolint:errcheck,forcetypeassert // newRepo returns a *dynamo.Repository
		},
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

// Test_StoreInitializationConformance runs the store-initialization cases on a table of its own,
// with the default (non-lazy) writers: they refuse the uninitialized table, and only Initialize
// scans it.
func Test_StoreInitializationConformance(t *testing.T) {
	ctx := context.Background()

	events, marker := newIsolatedEventLog(ctx, t)
	client := isolatedClient

	conformance.RunInitializationSuite(t,
		dynamo.NewRepository(client, events.EventsTable),
		dynamo.NewRepository(client, events.EventsTable),
		conformance.InitializationOptions{
			RefusesUninitialized: true,
			Manifest:             marker.Manifest,
			Unmigrate: func(ctx context.Context, _ evt.Repository) (func(), error) {
				_, err := marker.Declare(ctx, func(manifest *evt.FeatureManifest) {
					delete(manifest.Features, evt.FeatureStreamHeads)
				})

				return func() {
					_, _ = marker.DeclareStreamHeads(context.Background()) //nolint:errcheck // best-effort restore of a test-only table
				}, err
			},
			BypassHead: func(ctx context.Context, _ evt.Repository, event evt.SerializedEvent) error {
				return compat.DynamoBypassHead(client, events.EventsTable)(ctx, event)
			},
		})
}

// Test_FeatureMarkerConformance runs the feature-marker cases on a table of their own: they declare
// features no writer supports, which would refuse every other test's commits on a shared table. The
// two writers are separate repositories over the same table, each with its own cached validation.
func Test_FeatureMarkerConformance(t *testing.T) {
	ctx := context.Background()

	events, _ := newIsolatedEventLog(ctx, t)
	client := isolatedClient

	newRepo := func() *dynamo.Repository {
		return dynamo.NewRepository(client, events.EventsTable).WithLazyInitialization(true)
	}

	conformance.RunFeatureMarkerSuite(t, newRepo(), newRepo(), conformance.FeatureMarkerOptions{
		Declare: func(ctx context.Context, repo evt.Repository, change func(*evt.FeatureManifest)) (evt.FeatureManifest, error) {
			return repo.(*dynamo.Repository).FeatureMarker(nil).Declare(ctx, change) //nolint:errcheck,forcetypeassert // the suite is handed *dynamo.Repository writers
		},
		Manifest: func(ctx context.Context, repo evt.Repository) (*evt.FeatureManifest, error) {
			return repo.(*dynamo.Repository).FeatureMarker(nil).Manifest(ctx) //nolint:errcheck,forcetypeassert // the suite is handed *dynamo.Repository writers
		},
	})
}

// Test_ViewRepositoryConformance runs the shared view repository cases against a views table of
// their own.
func Test_ViewRepositoryConformance(t *testing.T) {
	ctx := context.Background()
	client := dynamodb.NewFromConfig(newClientConfig(t))
	viewsTable := newIsolatedViewsTable(ctx, t, client)

	conformance.RunViewRepositorySuite(t, func() evt.ViewRepository {
		return dynamo.NewViewRepository(client, viewsTable)
	})
}
