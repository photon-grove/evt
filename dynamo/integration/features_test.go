//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

// The C9 feature-marker and C8 reserved-namespace integration tests run against the local AWS
// emulator. The feature manifest is table-global state, so each test runs against its own event
// log: the declarations, the per-commit check, the writer refusals, and the enumeration skips are
// asserted against a table this run created and deletes afterward.

// newIsolatedEventLog creates a fresh event-log table for one test and returns the repository and
// a feature marker over it. The table is deleted when the test finishes.
func newIsolatedEventLog(ctx context.Context, t *testing.T) (*dynamo.Repository, *dynamo.FeatureMarker) {
	t.Helper()

	client := dynamodb.NewFromConfig(newClientConfig(t))
	name := fmt.Sprintf("evt-it-%d", time.Now().UnixNano())

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(name),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = client.DeleteTable(cleanupCtx, &dynamodb.DeleteTableInput{TableName: aws.String(name)}) //nolint:errcheck // best-effort teardown of a test-only table
	})

	require.Eventually(t, func() bool {
		out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
		return err == nil && out.Table != nil && out.Table.TableStatus == types.TableStatusActive
	}, 10*time.Second, 100*time.Millisecond)

	repo := dynamo.NewRepository(client, name)
	isolatedClient = client

	return repo, repo.FeatureMarker(nil)
}

// isolatedClient holds the client of the most recently created isolated event log, so follow-up
// assertions in the same test can address its items.
var isolatedClient *dynamodb.Client

// repoClient returns the isolated event log's client.
func repoClient() *dynamodb.Client { return isolatedClient }

// newClientConfig builds an AWS config pointed at the local emulator, mirroring the suite setup.
func newClientConfig(t *testing.T) aws.Config {
	t.Helper()

	endpoint := resolveLocalEndpoint()
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}

	cfg, err := newAWSConfig(context.Background(), "us-west-2", endpoint)
	require.NoError(t, err)

	return *cfg
}

// commitOne serializes and commits one event for the entity on the repository.
func commitOne(ctx context.Context, t *testing.T, repo *dynamo.Repository, id evt.EntityID, sequence evt.EventSequence) {
	t.Helper()

	metadata := evt.NewMetadata(ctx, aws.String("us-west-2"))
	serialized, err := evt.SerializeEvents(
		[]evt.Event{&test.EntityCreated{ID: id, Value: "v"}}, sequence, id, metadata)
	require.NoError(t, err)
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: serialized}))
}

// Test_FeatureMarker_DeclareAndVersionBump asserts the declaration protocol end to end: a create
// writes version 1, every later declaration adds exactly one, and the sets round-trip.
func Test_FeatureMarker_DeclareAndVersionBump(t *testing.T) {
	ctx := context.Background()
	repo, marker := newIsolatedEventLog(ctx, t)

	manifest, err := marker.Manifest(ctx)
	require.NoError(t, err)
	require.Nil(t, manifest, "a fresh table declares nothing")

	declared, err := marker.Declare(ctx, func(m *evt.FeatureManifest) {
		m.Features["journal"] = 1
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), declared.Version)

	declared, err = marker.Declare(ctx, func(m *evt.FeatureManifest) {
		m.Features["journal-compaction"] = 1
		m.HistoryDependent = []evt.EntityType{"audit-feed"}
		m.Retention = []evt.EntityType{"session"}
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), declared.Version, "one declaration adds exactly one to the version")

	stored, err := marker.Manifest(ctx)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, int64(2), stored.Version)
	require.Equal(t, map[string]evt.FeatureFormat{"journal": 1, "journal-compaction": 1}, stored.Features)
	require.Equal(t, []evt.EntityType{"audit-feed"}, stored.HistoryDependent)
	require.Equal(t, []evt.EntityType{"session"}, stored.Retention)

	// The marker validates the declared set against the supported pairs.
	supported := repo.FeatureMarker(evt.SupportedFeatures{"journal": 1, "journal-compaction": 1})
	require.NoError(t, supported.Validate(ctx))

	// Removing an entity type from a set is a declaration in the same protocol.
	declared, err = marker.Declare(ctx, func(m *evt.FeatureManifest) {
		m.HistoryDependent = nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), declared.Version)

	stored, err = marker.Manifest(ctx)
	require.NoError(t, err)
	require.Empty(t, stored.HistoryDependent)
	require.Equal(t, []evt.EntityType{"session"}, stored.Retention)
}

// Test_FeatureMarker_CommitRefusesUnsupportedFeature asserts the end-to-end refusal: after another
// writer declares an extension this Go writer does not model, its next commit fails the marker
// check, re-validates, and refuses before writing.
func Test_FeatureMarker_CommitRefusesUnsupportedFeature(t *testing.T) {
	ctx := context.Background()
	repo, marker := newIsolatedEventLog(ctx, t)
	enforcing := repo.WithFeatureMarker(marker)

	id := evt.EntityID("acct-1")
	commitOne(ctx, t, enforcing, id, 0)

	// A Rust service declares the journal extension; this writer supports no extensions. The
	// declaration runs through its own marker instance, the way an independent writer would.
	otherWriter := repo.FeatureMarker(nil)
	_, err := otherWriter.Declare(ctx, func(m *evt.FeatureManifest) {
		m.Features["journal"] = 1
	})
	require.NoError(t, err)

	other := evt.EntityID("acct-2")
	serialized, serr := evt.SerializeEvents([]evt.Event{&test.EntityCreated{ID: other, Value: "v"}},
		0, other, evt.NewMetadata(ctx, aws.String("us-west-2")))
	require.NoError(t, serr)

	err = enforcing.Commit(ctx, evt.SerializedResult{Events: serialized})
	require.Error(t, err)
	require.True(t, evt.IsUnsupportedFeatureErr(err), "expected UnsupportedFeatureError, got %v", err)

	stored, gerr := repo.GetEvents(ctx, other)
	require.NoError(t, gerr)
	require.Empty(t, stored, "the refused commit writes nothing")
}

// Test_FeatureMarker_ChangedManifestRetries asserts a manifest change re-fails every writer cached
// at the old version: the failing check re-validates, and a writer that still supports the set
// retries against the new version.
func Test_FeatureMarker_ChangedManifestRetries(t *testing.T) {
	ctx := context.Background()
	repo, marker := newIsolatedEventLog(ctx, t)
	enforcing := repo.WithFeatureMarker(
		repo.FeatureMarker(evt.SupportedFeatures{"journal": 1}))

	id := evt.EntityID("acct-1")
	commitOne(ctx, t, enforcing, id, 0)

	// A set change (no feature change) bumps the version the same way.
	_, err := marker.Declare(ctx, func(m *evt.FeatureManifest) {
		m.Retention = []evt.EntityType{"session"}
	})
	require.NoError(t, err)

	// The next commit's cached condition fails; the re-validation still succeeds, so the error is
	// the transient changed-manifest error.
	serialized, serr := evt.SerializeEvents([]evt.Event{&test.EntityCreated{ID: id, Value: "v"}},
		2, id, evt.NewMetadata(ctx, aws.String("us-west-2")))
	require.NoError(t, serr)

	err = enforcing.Commit(ctx, evt.SerializedResult{Events: serialized})
	require.Error(t, err)
	require.True(t, errors.Is(err, evt.ErrFeatureManifestChanged), "expected ErrFeatureManifestChanged, got %v", err)

	// The retry commits against the freshly cached version.
	require.NoError(t, enforcing.Commit(ctx, evt.SerializedResult{Events: serialized}))

	events, gerr := repo.GetEvents(ctx, id)
	require.NoError(t, gerr)
	require.Len(t, events, 2)
}

// Test_FeatureMarker_CompactBelowRefusesHistoryDependent asserts the durable historyDependent
// refusal: compaction refuses an entity type in the manifest's set in every process that opens
// the log, whether or not it registered the projector.
func Test_FeatureMarker_CompactBelowRefusesHistoryDependent(t *testing.T) {
	ctx := context.Background()
	repo, marker := newIsolatedEventLog(ctx, t)
	enforcing := repo.WithFeatureMarker(marker)

	id := evt.EntityID("acct-1")
	metadata := evt.NewMetadata(ctx, aws.String("us-west-2"))
	serialized, err := evt.SerializeEvents([]evt.Event{&test.EntityCreated{ID: id, Value: "v"}}, 0, id, metadata)
	require.NoError(t, err)

	payload := []byte(`{"id":"acct-1","value":"v"}`)
	require.NoError(t, repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: serialized}, test.EntityType, id, payload, 1))

	_, err = marker.Declare(ctx, func(m *evt.FeatureManifest) {
		m.HistoryDependent = []evt.EntityType{test.EntityType}
	})
	require.NoError(t, err)

	_, err = enforcing.CompactBelow(ctx, id, 1)
	require.Error(t, err)
	require.True(t, evt.IsHistoryDependentProjectionErr(err), "expected HistoryDependentProjectionError, got %v", err)

	// A repository that does not enforce the manifest refuses all the same: the refusal reads the
	// row, not local configuration.
	plain := dynamo.NewRepository(repoClient(), repo.EventsTable)
	_, err = plain.CompactBelow(ctx, id, 1)
	require.Error(t, err)
	require.True(t, evt.IsHistoryDependentProjectionErr(err), "expected HistoryDependentProjectionError, got %v", err)
}

// Test_Namespaces_EnumerationSkipsEngineRows asserts enumeration, entity heads, and rebuild scans
// skip the reserved namespaces the engine (or another writer) wrote.
func Test_Namespaces_EnumerationSkipsEngineRows(t *testing.T) {
	ctx := context.Background()
	repo, marker := newIsolatedEventLog(ctx, t)

	id := evt.EntityID("acct-1")
	commitOne(ctx, t, repo, id, 0)

	// The engine writes the feature marker into the reserved namespace.
	_, err := marker.Declare(ctx, func(m *evt.FeatureManifest) {
		m.Features["journal"] = 1
	})
	require.NoError(t, err)

	// Another writer records a command outcome; this Go store has outcomes disabled, so the row is
	// written directly.
	_, err = repoClient().PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(repo.EventsTable),
		Item: map[string]types.AttributeValue{
			"pk":     &types.AttributeValueMemberS{Value: "command-outcome#cmd-0001"},
			"sk":     &types.AttributeValueMemberN{Value: "1"},
			"status": &types.AttributeValueMemberS{Value: "accepted"},
			"digest": &types.AttributeValueMemberS{Value: "00"},
		},
	})
	require.NoError(t, err)

	var events []evt.SerializedEvent
	for res := range repo.StreamAllEvents(ctx, evt.StreamFilter{}) {
		batch, uerr := res.Unwrap()
		require.NoError(t, uerr)
		events = append(events, batch...)
	}
	require.Len(t, events, 1)
	require.Equal(t, id, events[0].EntityID)

	heads, herr := repo.StreamEntityHeads(ctx, "")
	require.NoError(t, herr)
	require.Len(t, heads, 1, "engine rows are never entity heads")
	require.Contains(t, heads, id)

	rebuilt := 0
	for res := range repo.StreamEntitiesFromSnapshots(ctx, test.EntityType, seedIntegrationEntity, applyIntegrationEvent) {
		_, uerr := res.Unwrap()
		require.NoError(t, uerr)
		rebuilt++
	}
	require.Equal(t, 1, rebuilt)
}

// Test_Namespaces_ReservedWritePathsRefused asserts every entity-addressed write path refuses a
// reserved entity ID before any request: commits, snapshot writes, and compaction.
func Test_Namespaces_ReservedWritePathsRefused(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)

	reserved := []evt.EntityID{
		evt.EntityID(evt.SystemNamespacePrefix + "acct-1"),
		evt.EntityID(evt.CommandOutcomeNamespacePrefix + "cmd-1"),
	}

	for _, id := range reserved {
		metadata := evt.NewMetadata(ctx, aws.String("us-west-2"))
		serialized, serr := evt.SerializeEvents(
			[]evt.Event{&test.EntityCreated{ID: id, Value: "v"}}, 0, id, metadata)
		require.NoError(t, serr)

		err := repo.Commit(ctx, evt.SerializedResult{Events: serialized})
		require.Error(t, err)
		require.True(t, evt.IsReservedEntityIdErr(err), "expected ReservedEntityIdError, got %v", err)

		err = repo.PutSnapshot(ctx, "test", id, []byte("{}"), 1, 1)
		require.Error(t, err)
		require.True(t, evt.IsReservedEntityIdErr(err), "expected ReservedEntityIdError, got %v", err)

		_, err = repo.CompactBelow(ctx, id, 1)
		require.Error(t, err)
		require.True(t, evt.IsReservedEntityIdErr(err), "expected ReservedEntityIdError, got %v", err)

		// Nothing was written to the reserved key.
		out, gerr := repoClient().GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(repo.EventsTable),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: string(id)},
				"sk": &types.AttributeValueMemberN{Value: "0"},
			},
		})
		require.NoError(t, gerr)
		require.Empty(t, out.Item)
	}
}

// Test_GetRecentEvents_NewestFirst commits a stream and reads its last two events newest-first,
// then verifies a limit below 1 is refused before any request.
func Test_GetRecentEvents_NewestFirst(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)

	id := evt.EntityID("acct-1")
	for i := 1; i <= 5; i++ {
		metadata := evt.NewMetadata(ctx, aws.String("us-west-2"))
		serialized, err := evt.SerializeEvents(
			[]evt.Event{&test.EntityCreated{ID: id, Value: "v" + strconv.Itoa(i)}},
			evt.EventSequence(i-1), id, metadata)
		require.NoError(t, err)
		require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: serialized}))
	}

	all, err := repo.GetRecentEvents(ctx, id, 10)
	require.NoError(t, err)
	require.Len(t, all, 5)
	require.Greater(t, all[0].Sequence, all[len(all)-1].Sequence)

	_, err = repo.GetRecentEvents(ctx, id, 0)
	require.Error(t, err)

	empty, err := repo.GetRecentEvents(ctx, evt.EntityID("acct-2"), 3)
	require.NoError(t, err)
	require.Empty(t, empty)
}
