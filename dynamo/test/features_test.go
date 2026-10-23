package test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	dynamo_mock "github.com/photon-grove/evt/dynamo/internal/mock"
	"github.com/photon-grove/evt/policy"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// FeatureMarkerSuite exercises the C9 feature marker against the mocked DynamoDB client: the
// mandatory per-commit ConditionCheck, validation caching, declaration versioning, and the
// refusals (unsupported pair, history-dependent compaction).
type FeatureMarkerSuite struct {
	suite.Suite
}

// Run the feature-marker suite
func Test_FeatureMarkerSuite(t *testing.T) {
	suite.Run(t, new(FeatureMarkerSuite))
}

// markerItem builds a feature-marker item for a GetItem response.
func markerItem(version int64, features, historyDependent, retention string) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		"pk":       &types.AttributeValueMemberS{Value: "SYSTEM#FEATURES"},
		"sk":       &types.AttributeValueMemberN{Value: "0"},
		"version":  &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", version)},
		"features": &types.AttributeValueMemberS{Value: features},
	}

	if historyDependent != "" {
		item["historyDependent"] = &types.AttributeValueMemberS{Value: historyDependent}
	}

	if retention != "" {
		item["retention"] = &types.AttributeValueMemberS{Value: retention}
	}

	return item
}

// serializedEvent builds one event for the given entity.
func serializedEvent(entityID string) evt.SerializedEvent {
	return evt.SerializedEvent{
		EntityType: "test",
		EntityID:   evt.EntityID(entityID),
		Sequence:   1,
		Type:       "test-event",
		Version:    1,
		Payload:    []byte("{}"),
		Metadata:   evt.Metadata{Region: "us-west-2"},
	}
}

// expectCommit wires a successful TransactWriteItems call and captures the transaction input.
func expectCommit(t *testing.T, client *dynamo_mock.Client, captured *dynamodb.TransactWriteItemsInput) {
	t.Helper()

	client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return(new(dynamodb.TransactWriteItemsOutput), nil).
		Once().
		Run(func(args mock.Arguments) {
			input, ok := args.Get(1).(*dynamodb.TransactWriteItemsInput)
			require.True(t, ok, "expected TransactWriteItems input")
			*captured = *input
		})
}

// expectCommitFailure wires a failed TransactWriteItems whose first item fails its condition.
func expectCommitFailure(client *dynamo_mock.Client) {
	client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.TransactWriteItemsOutput)(nil), &types.TransactionCanceledException{
			CancellationReasons: []types.CancellationReason{
				{Code: aws.String("ConditionalCheckFailed")},
			},
		}).
		Once()
}

// expectManifest wires a GetItem on the feature-marker key returning the given item (nil = absent).
func expectManifest(client *dynamo_mock.Client, item map[string]types.AttributeValue) {
	out := &dynamodb.GetItemOutput{}
	if item != nil {
		out.Item = item
	}

	client.On("GetItem", mock.Anything, mock.MatchedBy(func(in *dynamodb.GetItemInput) bool {
		pk, ok := in.Key["pk"].(*types.AttributeValueMemberS)
		return ok && pk.Value == "SYSTEM#FEATURES"
	}), mock.Anything).Return(out, nil).Once()
}

// expectScan wires the strongly consistent emptiness scan a writer runs before it declares
// stream-heads on a store with no marker; pks are the partition keys the scan finds.
func expectScan(client *dynamo_mock.Client, pks ...string) {
	items := make([]map[string]types.AttributeValue, 0, len(pks))
	for _, pk := range pks {
		items = append(items, map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: pk}})
	}

	client.On("Scan", mock.Anything, mock.MatchedBy(func(in *dynamodb.ScanInput) bool {
		return aws.ToBool(in.ConsistentRead)
	}), mock.Anything).Return(&dynamodb.ScanOutput{Items: items}, nil).Once()
}

func (s *FeatureMarkerSuite) commit(repo *dynamo.Repository, entityID string) error {
	events := []evt.SerializedEvent{serializedEvent(entityID)}

	return repo.Commit(context.Background(), evt.SerializedResult{Events: events})
}

// Test_Commit_IncludesMarkerCheck asserts the per-commit ConditionCheck on SYSTEM#FEATURES is the
// transaction's first item, and that the validation is cached (no re-read on the next commit). A
// store with no marker and no data is declared by the writer itself.
func (s *FeatureMarkerSuite) Test_Commit_IncludesMarkerCheck() {
	client := dynamo_mock.NewClient()
	repo := dynamo.NewRepository(client, "test-events").
		WithFeatureMarker(dynamo.NewRepository(client, "test-events").FeatureMarker(nil))

	// First commit: no marker row exists and the scan finds only engine rows, so the writer
	// declares stream-heads (version 1) and validates against that version.
	expectManifest(client, nil)
	expectScan(client, "SYSTEM#REBUILD")
	expectManifest(client, nil)
	// The declaration is followed by a second look at the store, for a stream without a head item.
	expectScan(client, "SYSTEM#REBUILD")

	var declared *dynamodb.PutItemInput
	client.On("PutItem", mock.Anything, mock.Anything, mock.Anything).
		Return(new(dynamodb.PutItemOutput), nil).
		Once().
		Run(func(args mock.Arguments) {
			var ok bool
			declared, ok = args.Get(1).(*dynamodb.PutItemInput)
			require.True(s.T(), ok, "expected PutItem input")
		})

	var first dynamodb.TransactWriteItemsInput
	expectCommit(s.T(), client, &first)
	require.NoError(s.T(), s.commit(repo, "acct-1"))

	require.Equal(s.T(), "attribute_not_exists(pk)", aws.ToString(declared.ConditionExpression))
	require.Equal(s.T(), &types.AttributeValueMemberN{Value: "1"}, declared.Item["version"])
	require.Equal(s.T(), &types.AttributeValueMemberS{Value: `{"stream-heads":1}`}, declared.Item["features"])

	require.NotEmpty(s.T(), first.TransactItems)
	check := first.TransactItems[0].ConditionCheck
	require.NotNil(s.T(), check)
	require.Equal(s.T(), &types.AttributeValueMemberS{Value: "SYSTEM#FEATURES"}, check.Key["pk"])
	require.Equal(s.T(), &types.AttributeValueMemberN{Value: "0"}, check.Key["sk"])
	require.Equal(s.T(), "attribute_not_exists(pk) OR version = :v", aws.ToString(check.ConditionExpression))
	require.Equal(s.T(), &types.AttributeValueMemberN{Value: "1"}, check.ExpressionAttributeValues[":v"])

	// Second commit: the validation is cached; the same check is included without a re-read.
	var second dynamodb.TransactWriteItemsInput
	expectCommit(s.T(), client, &second)
	require.NoError(s.T(), s.commit(repo, "acct-2"))

	require.NotNil(s.T(), second.TransactItems[0].ConditionCheck)
	require.Equal(s.T(), "attribute_not_exists(pk) OR version = :v", aws.ToString(second.TransactItems[0].ConditionCheck.ConditionExpression))
	client.AssertExpectations(s.T())
}

// Test_Commit_RefusesAnUnmigratedStore asserts a store that holds data but does not declare
// stream-heads is refused before anything is written: its streams have no head items.
func (s *FeatureMarkerSuite) Test_Commit_RefusesAnUnmigratedStore() {
	for name, manifest := range map[string]map[string]types.AttributeValue{
		"no marker row":        nil,
		"a marker without it":  markerItem(2, "{}", "", ""),
		"a marker with others": markerItem(3, `{"journal":1}`, `["audit-feed"]`, ""),
	} {
		s.Run(name, func() {
			client := dynamo_mock.NewClient()
			repo := dynamo.NewRepository(client, "test-events").
				WithFeatureMarker(dynamo.NewRepository(client, "test-events").FeatureMarker(evt.SupportedFeatures{"journal": 1}))

			expectManifest(client, manifest)
			expectScan(client, "SYSTEM#REBUILD", "acct-0")

			err := s.commit(repo, "acct-1")
			require.Error(s.T(), err)
			require.True(s.T(), evt.IsStreamHeadsNotMigratedErr(err), "expected StreamHeadsNotMigratedError, got %v", err)
			require.ErrorIs(s.T(), err, evt.ErrStreamHeadsNotMigrated)
			client.AssertNotCalled(s.T(), "TransactWriteItems", mock.Anything, mock.Anything, mock.Anything)
			client.AssertNotCalled(s.T(), "PutItem", mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

// Test_Commit_ConditionsOnCachedVersion asserts a commit against an existing marker conditions on
// the version it validated.
func (s *FeatureMarkerSuite) Test_Commit_ConditionsOnCachedVersion() {
	client := dynamo_mock.NewClient()
	repo := dynamo.NewRepository(client, "test-events").
		WithFeatureMarker(dynamo.NewRepository(client, "test-events").FeatureMarker(nil))

	expectManifest(client, markerItem(3, `{"stream-heads":1}`, "", ""))

	var first dynamodb.TransactWriteItemsInput
	expectCommit(s.T(), client, &first)
	require.NoError(s.T(), s.commit(repo, "acct-1"))

	check := first.TransactItems[0].ConditionCheck
	require.Equal(s.T(), "attribute_not_exists(pk) OR version = :v", aws.ToString(check.ConditionExpression))
	require.Equal(s.T(), &types.AttributeValueMemberN{Value: "3"}, check.ExpressionAttributeValues[":v"])

	// Compaction reads the manifest with a consistent read of its own; no features configured, so
	// it only consumes the validation read above.
	client.AssertExpectations(s.T())
}

// Test_Commit_RefusesUnsupportedFeature asserts the first write against a manifest declaring a
// pair this writer does not model refuses before anything is written.
func (s *FeatureMarkerSuite) Test_Commit_RefusesUnsupportedFeature() {
	client := dynamo_mock.NewClient()
	repo := dynamo.NewRepository(client, "test-events").
		WithFeatureMarker(dynamo.NewRepository(client, "test-events").FeatureMarker(nil))

	expectManifest(client, markerItem(1, `{"journal":1}`, "", ""))

	err := s.commit(repo, "acct-1")
	require.Error(s.T(), err)
	require.True(s.T(), evt.IsUnsupportedFeatureErr(err), "expected UnsupportedFeatureError, got %v", err)
	client.AssertNotCalled(s.T(), "TransactWriteItems", mock.Anything, mock.Anything, mock.Anything)
}

// Test_Commit_CheckFailure_RevalidatesAndRefuses asserts a failed per-commit check re-validates:
// an unsupported pair surfaces as UnsupportedFeatureError.
func (s *FeatureMarkerSuite) Test_Commit_CheckFailure_RevalidatesAndRefuses() {
	client := dynamo_mock.NewClient()
	repo := dynamo.NewRepository(client, "test-events").
		WithFeatureMarker(dynamo.NewRepository(client, "test-events").FeatureMarker(nil))

	// First commit validates a migrated store and succeeds.
	expectManifest(client, markerItem(1, `{"stream-heads":1}`, "", ""))

	var first dynamodb.TransactWriteItemsInput
	expectCommit(s.T(), client, &first)
	require.NoError(s.T(), s.commit(repo, "acct-1"))

	// A declaration lands between the commits. The next commit's cached version condition
	// fails; the re-validation reads the manifest and refuses the unsupported pair.
	expectManifest(client, markerItem(2, `{"journal":1,"stream-heads":1}`, "", ""))
	expectCommitFailure(client)

	err := s.commit(repo, "acct-2")
	require.Error(s.T(), err)
	require.True(s.T(), evt.IsUnsupportedFeatureErr(err), "expected UnsupportedFeatureError, got %v", err)
}

// Test_Commit_CheckFailure_SupportedChangeIsTransient asserts a failed per-commit check against a
// manifest the writer still supports re-validates, caches the new version, and classifies the
// failure as transient so a retry follows.
func (s *FeatureMarkerSuite) Test_Commit_CheckFailure_SupportedChangeIsTransient() {
	client := dynamo_mock.NewClient()
	repo := dynamo.NewRepository(client, "test-events").
		WithFeatureMarker(dynamo.NewRepository(client, "test-events").FeatureMarker(evt.SupportedFeatures{"journal": 1}))

	expectManifest(client, markerItem(1, `{"journal":1,"stream-heads":1}`, "", ""))

	var first dynamodb.TransactWriteItemsInput
	expectCommit(s.T(), client, &first)
	require.NoError(s.T(), s.commit(repo, "acct-1"))

	// The manifest bumps to version 2 under the writer; the cached condition fails.
	expectManifest(client, markerItem(2, `{"journal":1,"stream-heads":1}`, "", ""))
	expectCommitFailure(client)

	err := s.commit(repo, "acct-2")
	require.Error(s.T(), err)
	require.True(s.T(), errors.Is(err, evt.ErrFeatureManifestChanged), "expected ErrFeatureManifestChanged, got %v", err)
	require.True(s.T(), policy.IsTransient(err), "the changed-manifest failure must be transient")

	// The retry reuses the freshly cached version without another read.
	var retried dynamodb.TransactWriteItemsInput
	expectCommit(s.T(), client, &retried)
	require.NoError(s.T(), s.commit(repo, "acct-2"))

	check := retried.TransactItems[0].ConditionCheck
	require.Equal(s.T(), &types.AttributeValueMemberN{Value: "2"}, check.ExpressionAttributeValues[":v"])
	client.AssertExpectations(s.T())
}

// Test_Declare_CreatesAndBumpsVersion asserts the declaration protocol: a create writes version 1
// against attribute_not_exists(pk); an update bumps by exactly one against version = :observed.
func (s *FeatureMarkerSuite) Test_Declare_CreatesAndBumpsVersion() {
	client := dynamo_mock.NewClient()
	marker := dynamo.NewRepository(client, "test-events").FeatureMarker(evt.SupportedFeatures{"journal": 1})

	// Create.
	expectManifest(client, nil)

	var createdPtr *dynamodb.PutItemInput
	client.On("PutItem", mock.Anything, mock.Anything, mock.Anything).
		Return(new(dynamodb.PutItemOutput), nil).
		Once().
		Run(func(args mock.Arguments) {
			var ok bool
			createdPtr, ok = args.Get(1).(*dynamodb.PutItemInput)
			require.True(s.T(), ok, "expected PutItem input")
		})

	manifest, err := marker.Declare(context.Background(), func(m *evt.FeatureManifest) {
		m.Features["journal"] = 1
	})
	require.NoError(s.T(), err)
	require.Equal(s.T(), int64(1), manifest.Version)
	require.Equal(s.T(), "attribute_not_exists(pk)", aws.ToString(createdPtr.ConditionExpression))
	require.Equal(s.T(), &types.AttributeValueMemberN{Value: "1"}, createdPtr.Item["version"])
	require.Equal(s.T(), &types.AttributeValueMemberS{Value: `{"journal":1}`}, createdPtr.Item["features"])

	// Update: one declaration adds exactly one to the version and writes the merged sets.
	expectManifest(client, markerItem(1, `{"journal":1}`, "", ""))

	var updatedPtr *dynamodb.PutItemInput
	client.On("PutItem", mock.Anything, mock.Anything, mock.Anything).
		Return(new(dynamodb.PutItemOutput), nil).
		Once().
		Run(func(args mock.Arguments) {
			var ok bool
			updatedPtr, ok = args.Get(1).(*dynamodb.PutItemInput)
			require.True(s.T(), ok, "expected PutItem input")
		})

	manifest, err = marker.Declare(context.Background(), func(m *evt.FeatureManifest) {
		m.Features["journal-compaction"] = 1
		m.HistoryDependent = []evt.EntityType{"audit-feed"}
	})
	require.NoError(s.T(), err)
	require.Equal(s.T(), int64(2), manifest.Version)
	require.Equal(s.T(), "version = :observed", aws.ToString(updatedPtr.ConditionExpression))
	require.Equal(s.T(), &types.AttributeValueMemberN{Value: "1"}, updatedPtr.ExpressionAttributeValues[":observed"])
	require.Equal(s.T(), &types.AttributeValueMemberN{Value: "2"}, updatedPtr.Item["version"])
	require.Equal(s.T(), &types.AttributeValueMemberS{Value: `{"journal":1,"journal-compaction":1}`}, updatedPtr.Item["features"])
	require.Equal(s.T(), &types.AttributeValueMemberS{Value: `["audit-feed"]`}, updatedPtr.Item["historyDependent"])
	require.NotContains(s.T(), updatedPtr.Item, "retention", "an empty set is omitted")

	client.AssertExpectations(s.T())
}

// A successful Declare must invalidate this instance's cached validation, even when it
// declares a feature the writer cannot use. Later set-only declarations preserve durable features.
func (s *FeatureMarkerSuite) Test_Declare_SameInstanceRevalidatesUnsupportedFeature() {
	ctx := context.Background()
	client := dynamo_mock.NewClient()
	marker := dynamo.NewRepository(client, "test-events").FeatureMarker(nil)
	repo := dynamo.NewRepository(client, "test-events").WithFeatureMarker(marker)

	expectManifest(client, markerItem(1, `{"stream-heads":1}`, "", ""))
	var first dynamodb.TransactWriteItemsInput
	expectCommit(s.T(), client, &first)
	require.NoError(s.T(), s.commit(repo, "acct-1"))

	expectManifest(client, nil)
	client.On("PutItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.PutItemOutput{}, nil).Once()
	_, err := marker.Declare(ctx, func(m *evt.FeatureManifest) { m.Features["journal"] = 1 })
	require.NoError(s.T(), err)

	expectManifest(client, markerItem(1, `{"journal":1}`, "", ""))
	err = s.commit(repo, "acct-2")
	require.True(s.T(), evt.IsUnsupportedFeatureErr(err), "expected unsupported feature refusal, got %v", err)
	client.AssertNumberOfCalls(s.T(), "TransactWriteItems", 1)

	expectManifest(client, markerItem(1, `{"journal":1}`, "", ""))
	client.On("PutItem", mock.Anything, mock.MatchedBy(func(in *dynamodb.PutItemInput) bool {
		features, featuresOK := in.Item["features"].(*types.AttributeValueMemberS)
		history, historyOK := in.Item["historyDependent"].(*types.AttributeValueMemberS)
		return featuresOK && historyOK && aws.ToString(in.ConditionExpression) == "version = :observed" &&
			features.Value == `{"journal":1}` && history.Value == `["audit-feed"]`
	}), mock.Anything).Return(&dynamodb.PutItemOutput{}, nil).Once()
	manifest, err := marker.Declare(ctx, func(m *evt.FeatureManifest) {
		m.HistoryDependent = []evt.EntityType{"audit-feed"}
	})
	require.NoError(s.T(), err)
	require.Equal(s.T(), int64(2), manifest.Version)
	require.Equal(s.T(), map[string]evt.FeatureFormat{"journal": 1}, manifest.Features)

	expectManifest(client, markerItem(2, `{"journal":1}`, `["audit-feed"]`, ""))
	err = s.commit(repo, "acct-3")
	require.True(s.T(), evt.IsUnsupportedFeatureErr(err), "set-only declaration must not erase unsupported durable features: %v", err)
	client.AssertNumberOfCalls(s.T(), "TransactWriteItems", 1)
	client.AssertExpectations(s.T())
}

// Test_CompactBelow_RefusesHistoryDependentType asserts compaction refuses an entity type declared
// history-dependent in the durable manifest, before deleting anything.
func (s *FeatureMarkerSuite) Test_CompactBelow_RefusesHistoryDependentType() {
	client := dynamo_mock.NewClient()
	repo := dynamo.NewRepository(client, "test-events").
		WithFeatureMarker(dynamo.NewRepository(client, "test-events").FeatureMarker(nil))

	// The snapshot read learns the entity's type.
	client.On("GetItem", mock.Anything, mock.MatchedBy(func(in *dynamodb.GetItemInput) bool {
		pk, ok := in.Key["pk"].(*types.AttributeValueMemberS)
		return ok && pk.Value == "acct-1"
	}), mock.Anything).Return(&dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "acct-1"},
		"sk":         &types.AttributeValueMemberN{Value: "0"},
		"entityType": &types.AttributeValueMemberS{Value: "session"},
		"sequence":   &types.AttributeValueMemberN{Value: "1"},
		"eventSeq":   &types.AttributeValueMemberN{Value: "5"},
		"payload":    &types.AttributeValueMemberS{Value: "{}"},
	}}, nil).Once()

	// The durable refusal reads the manifest.
	expectManifest(client, markerItem(4, `{"stream-heads":1}`, `["session"]`, ""))

	deleted, err := repo.CompactBelow(context.Background(), "acct-1", 5)
	require.Error(s.T(), err)
	require.True(s.T(), evt.IsHistoryDependentProjectionErr(err), "expected HistoryDependentProjectionError, got %v", err)
	require.Equal(s.T(), 0, deleted)
	client.AssertNotCalled(s.T(), "TransactWriteItems", mock.Anything, mock.Anything, mock.Anything)
}

// Test_CompactBelow_RefusesAnUnmigratedStore asserts compaction refuses a store that does not
// declare stream-heads before it reads or deletes anything.
func (s *FeatureMarkerSuite) Test_CompactBelow_RefusesAnUnmigratedStore() {
	client := dynamo_mock.NewClient()
	repo := dynamo.NewRepository(client, "test-events").
		WithFeatureMarker(dynamo.NewRepository(client, "test-events").FeatureMarker(nil))

	expectManifest(client, nil)

	deleted, err := repo.CompactBelow(context.Background(), "acct-1", 5)
	require.True(s.T(), evt.IsStreamHeadsNotMigratedErr(err), "expected StreamHeadsNotMigratedError, got %v", err)
	require.Equal(s.T(), 0, deleted)
	client.AssertExpectations(s.T())
	client.AssertNotCalled(s.T(), "TransactWriteItems", mock.Anything, mock.Anything, mock.Anything)
}

// Test_CompactBelow_ProceedsOnAMigratedStore asserts compaction proceeds when the manifest declares
// stream-heads and does not declare the entity's type history-dependent.
func (s *FeatureMarkerSuite) Test_CompactBelow_ProceedsOnAMigratedStore() {
	client := dynamo_mock.NewClient()
	repo := dynamo.NewRepository(client, "test-events").
		WithFeatureMarker(dynamo.NewRepository(client, "test-events").FeatureMarker(nil))

	client.On("GetItem", mock.Anything, mock.MatchedBy(func(in *dynamodb.GetItemInput) bool {
		pk, ok := in.Key["pk"].(*types.AttributeValueMemberS)
		return ok && pk.Value == "acct-1"
	}), mock.Anything).Return(&dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "acct-1"},
		"sk":         &types.AttributeValueMemberN{Value: "0"},
		"entityType": &types.AttributeValueMemberS{Value: "account"},
		"sequence":   &types.AttributeValueMemberN{Value: "1"},
		"eventSeq":   &types.AttributeValueMemberN{Value: "5"},
		"payload":    &types.AttributeValueMemberS{Value: "{}"},
	}}, nil).Once()

	expectManifest(client, markerItem(1, `{"stream-heads":1}`, "", ""))

	// Compaction proceeds past the refusal checks; the range query finds the events to delete.
	client.On("Query", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{
			{
				"pk": &types.AttributeValueMemberS{Value: "acct-1"},
				"sk": &types.AttributeValueMemberN{Value: "1"},
			},
		}}, nil).Once()

	client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.TransactWriteItemsOutput{}, nil).Once()

	deleted, err := repo.CompactBelow(context.Background(), "acct-1", 1)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, deleted)
	client.AssertExpectations(s.T())
}
