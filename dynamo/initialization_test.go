package dynamo_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/conformance"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/internal/compat"
)

// scanCountingClient counts the Scans it serves.
type scanCountingClient struct {
	*compat.FakeDynamo

	scans atomic.Int64
}

func (c *scanCountingClient) Scan(
	ctx context.Context,
	params *dynamodb.ScanInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.ScanOutput, error) {
	c.scans.Add(1)

	return c.FakeDynamo.Scan(ctx, params, options...)
}

// The store-initialization cases run against the real DynamoDB repository over an in-memory table,
// with the default (non-lazy) writers: they refuse the uninitialized store, and only Initialize
// scans it.
func Test_StoreInitializationConformance(t *testing.T) {
	fake := compat.NewFakeDynamo()
	writers := &scanCountingClient{FakeDynamo: fake}
	marker := dynamo.NewRepository(fake, "events").FeatureMarker(nil)

	conformance.RunInitializationSuite(t,
		dynamo.NewRepository(fake, "events"),
		dynamo.NewRepository(writers, "events"),
		conformance.InitializationOptions{
			RefusesUninitialized: true,
			Manifest:             marker.Manifest,
			Unmigrate: func(ctx context.Context, _ evt.Repository) (func(), error) {
				_, err := marker.Declare(ctx, func(manifest *evt.FeatureManifest) {
					delete(manifest.Features, evt.FeatureStreamHeads)
				})

				return func() {
					_, _ = marker.DeclareStreamHeads(context.Background()) //nolint:errcheck // the in-memory table cannot fail
				}, err
			},
			BypassHead: func(ctx context.Context, _ evt.Repository, event evt.SerializedEvent) error {
				return compat.DynamoBypassHead(fake, "events")(ctx, event)
			},
		})

	require.Zero(t, writers.scans.Load(), "the second writer wrote and initialized nothing it had to scan for")
}

// A writer never scans: on an uninitialized store every write path refuses it without one, and
// on an initialized store it commits without one. Initialize is the only path that scans.
func TestWriters_NeverScan(t *testing.T) {
	ctx := context.Background()
	client := &scanCountingClient{FakeDynamo: compat.NewFakeDynamo()}
	repo := dynamo.NewRepository(client, "events")

	err := repo.Commit(ctx, evt.SerializedResult{Events: headEvents("acct", 1, 1, "t")})
	require.ErrorIs(t, err, evt.ErrStoreNotInitialized)

	err = repo.FeatureMarker(nil).Validate(ctx)
	require.ErrorIs(t, err, evt.ErrStoreNotInitialized, "a pure validation refuses too")
	require.Zero(t, client.scans.Load(), "a refused writer does not scan")

	declared, err := repo.Initialize(ctx)
	require.NoError(t, err)
	require.True(t, declared)
	require.NotZero(t, client.scans.Load(), "initialization verifies the store with a Scan")

	scans := client.scans.Load()

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: headEvents("acct", 1, 2, "t")}))
	require.NoError(t, dynamo.NewRepository(client, "events").Commit(ctx,
		evt.SerializedResult{Events: headEvents("acct", 3, 1, "t")}))

	declared, err = repo.Initialize(ctx)
	require.NoError(t, err)
	require.False(t, declared, "initialization is idempotent")
	require.Equal(t, scans, client.scans.Load(), "writes and a repeated initialization do not scan")
}

// Lazy initialization keeps the first writer's own declaration, for tests and local use.
func TestWithLazyInitialization_TheFirstWriteInitializes(t *testing.T) {
	ctx := context.Background()
	client := &scanCountingClient{FakeDynamo: compat.NewFakeDynamo()}
	strict := dynamo.NewRepository(client, "events")
	lazy := strict.WithLazyInitialization(true)

	require.NoError(t, lazy.Commit(ctx, evt.SerializedResult{Events: headEvents("acct", 1, 1, "t")}))
	require.NotZero(t, client.scans.Load())

	manifest, err := strict.FeatureMarker(nil).Manifest(ctx)
	require.NoError(t, err)
	require.True(t, manifest.DeclaresStreamHeads())

	// The original handle is unchanged: it still does not initialize, and now finds the store
	// initialized.
	require.NoError(t, strict.Commit(ctx, evt.SerializedResult{Events: headEvents("acct", 2, 1, "t")}))

	// Switched off again, an uninitialized store is refused.
	other := &scanCountingClient{FakeDynamo: compat.NewFakeDynamo()}
	off := dynamo.NewRepository(other, "events").WithLazyInitialization(true).WithLazyInitialization(false)
	err = off.Commit(ctx, evt.SerializedResult{Events: headEvents("acct", 1, 1, "t")})
	require.ErrorIs(t, err, evt.ErrStoreNotInitialized)
	require.Zero(t, other.scans.Load())
}

// Initialization over a store that holds data without a declaration is refused like the lazy
// writer's verification, and writes nothing; a populated store whose marker row holds other
// features keeps them.
func TestInitialize_RefusesAPopulatedStore(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	repo := dynamo.NewRepository(fake, "events")

	putRaw(t, fake, "legacy", 1, map[string]types.AttributeValue{
		"entityType": &types.AttributeValueMemberS{Value: "t"},
		"payload":    &types.AttributeValueMemberS{Value: "{}"},
	})

	declared, err := repo.Initialize(ctx)
	require.ErrorIs(t, err, evt.ErrStreamHeadsNotMigrated)
	require.False(t, declared)

	manifest, err := repo.FeatureMarker(nil).Manifest(ctx)
	require.NoError(t, err)
	require.Nil(t, manifest, "a refused initialization declares nothing")

	err = repo.Commit(ctx, evt.SerializedResult{Events: headEvents("legacy", 2, 1, "t")})
	require.ErrorIs(t, err, evt.ErrStoreNotInitialized)

	// A manifest that declares a feature this writer does not model is refused before any scan.
	_, err = repo.FeatureMarker(nil).Declare(ctx, func(manifest *evt.FeatureManifest) {
		manifest.Features["journal"] = 1
	})
	require.NoError(t, err)

	_, err = repo.Initialize(ctx)
	require.True(t, evt.IsUnsupportedFeatureErr(err), "%v", err)
}

// The retraction window: a stream with no head item that appears between the empty-store check
// and the declaration retracts the declaration, for an explicit initialization as for a lazy
// writer's.
func TestInitialize_RetractsWhenAHeadlessStreamAppearsInTheWindow(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	client := &declaringClient{FakeDynamo: fake}
	repo := dynamo.NewRepository(client, "events")

	client.afterDeclare = func() {
		putRaw(t, fake, "early", 1, map[string]types.AttributeValue{
			"entityType": &types.AttributeValueMemberS{Value: "t"},
			"payload":    &types.AttributeValueMemberS{Value: "{}"},
		})
	}

	declared, err := repo.Initialize(ctx)
	require.ErrorIs(t, err, evt.ErrStreamHeadsNotMigrated)
	require.False(t, declared)

	manifest, err := repo.FeatureMarker(nil).Manifest(ctx)
	require.NoError(t, err)
	require.NotNil(t, manifest)
	require.False(t, manifest.DeclaresStreamHeads(), "the declaration is retracted")
}
