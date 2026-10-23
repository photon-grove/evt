package dynamo_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/dynamo"
	"github.com/photon-grove/evt/go/conformance"
	"github.com/photon-grove/evt/go/internal/compat"
)

// The rebuild-fence cases run against the real DynamoDB repository over an in-memory table that
// evaluates exactly the expressions the repository sends, so the request path is exercised without
// an emulator. The same cases run against the emulator in the integration suite.
func Test_RebuildFenceConformance(t *testing.T) {
	fake := compat.NewFakeDynamo()
	views := "views"

	newRepo := func() evt.Repository { return dynamo.NewRepository(fake, "events").WithViewsTable(views) }
	marker := dynamo.NewRepository(fake, "events").FeatureMarker(nil)

	conformance.RunRebuildFenceSuite(t, newRepo, conformance.SuiteOptions{
		SupportsOutcomes: true,
		SupportsViews:    true,
		ReadView: func(_ context.Context, _ evt.Repository, pk, sk string) (*evt.SerializedView, error) {
			for _, item := range fake.Views(pk) {
				row, ok := item["sk"].(*types.AttributeValueMemberS)
				if !ok || row.Value != sk {
					continue
				}

				text := func(name string) string {
					if value, isText := item[name].(*types.AttributeValueMemberS); isText {
						return value.Value
					}

					return ""
				}

				return &evt.SerializedView{
					PK: pk, SK: sk, EntityID: evt.EntityID(text("entityID")), EntityType: evt.EntityType(text("entityType")),
					Payload: []byte(text("payload")),
				}, nil
			}

			return nil, nil
		},
		DeclareRetention: func(ctx context.Context, _ evt.Repository, entityType evt.EntityType) error {
			_, err := marker.Declare(ctx, func(manifest *evt.FeatureManifest) {
				manifest.Retention = append(manifest.Retention, entityType)
			})

			return err
		},
		BypassHead: func(ctx context.Context, _ evt.Repository, event evt.SerializedEvent) error {
			return compat.DynamoBypassHead(fake, "events")(ctx, event)
		},
		WithRebuildClock: func(repo evt.Repository, now func() time.Time) evt.Repository {
			return repo.(*dynamo.Repository).WithClock(now) //nolint:errcheck,forcetypeassert // newRepo returns a *dynamo.Repository
		},
	})
}
