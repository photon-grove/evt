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
	dynamo_mock "github.com/photon-grove/evt/dynamo/mock" //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
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
func expectCommit(client *dynamo_mock.Client, captured *dynamodb.TransactWriteItemsInput) {
	client.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).
		Return(new(dynamodb.TransactWriteItemsOutput), nil).
		Once().
		Run(func(args mock.Arguments) {
			*captured = *args.Get(1).(*dynamodb.TransactWriteItemsInput)
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
		return in.Key["pk"].(*types.AttributeValueMemberS).Value == "SYSTEM#FEATURES"
	}), mock.Anything).Return(out, nil).Once()
}

func (s *FeatureMarkerSuite) commit(repo *dynamo.Repository, entityID string) error {
	events := []evt.SerializedEvent{serializedEvent(entityID)}

	return repo.Commit(context.Background(), evt.SerializedResult{Events: events})
}

// Test_Commit_IncludesMarkerCheck asserts the per-commit ConditionCheck on SYSTEM#FEATURES is the
// transaction's first item, and that the validation is cached (no re-read on the next commit).
func (s *FeatureMarkerSuite) Test_Commit_IncludesMarkerCheck() {
	client := dynamo_mock.NewClient()
	repo := dynamo.NewRepository(client, "test-events").
		WithFeatureMarker(dynamo.NewFeatureMarker(client, "test-events", nil))

	// First commit: no marker row exists; validate against attribute_not_exists(pk).
	expectManifest(client, nil)

	var first dynamodb.TransactWriteItemsInput
	expectCommit(client, &first)
	require.NoError(s.T(), s.commit(repo, "acct-1"))

	require.NotEmpty(s.T(), first.TransactItems)
	check := first.TransactItems[0].ConditionCheck
	require.NotNil(s.T(), check)
	require.Equal(s.T(), "SYSTEM#FEATURES", check.Key["pk"].(*types.AttributeValueMemberS).Value)
	require.Equal(s.T(), "0", check.Key["sk"].(*types.AttributeValueMemberN).Value)
	require.Equal(s.T(), "attribute_not_exists(pk)", aws.ToString(check.ConditionExpression))

	// Second commit: the validation is cached; the same check is included without a re-read.
	var second dynamodb.TransactWriteItemsInput
	expectCommit(client, &second)
	require.NoError(s.T(), s.commit(repo, "acct-2"))

	require.NotNil(s.T(), second.TransactItems[0].ConditionCheck)
	require.Equal(s.T(), "attribute_not_exists(pk)", aws.ToString(second.TransactItems[0].ConditionCheck.ConditionExpression))
	client.AssertExpectations(s.T())
}

// Test_Commit_ConditionsOnCachedVersion asserts a commit against an existing marker conditions on
// the version it validated.
func (s *FeatureMarkerSuite) Test_Commit_ConditionsOnCachedVersion() {
	client := dynamo_mock.NewClient()
	repo := dynamo.NewRepository(client, "test-events").
		WithFeatureMarker(dynamo.NewFeatureMarker(client, "test-events", nil))

	expectManifest(client, markerItem(3, "{}", "", ""))

	var first dynamodb.TransactWriteItemsInput
	expectCommit(client, &first)
	require.NoError(s.T(), s.commit(repo, "acct-1"))

	check := first.TransactItems[0].ConditionCheck
	require.Equal(s.T(), "attribute_not_exists(pk) OR version = :v", aws.ToString(check.ConditionExpression))
	require.Equal(s.T(), "3", check.ExpressionAttributeValues[":v"].(*types.AttributeValueMemberN).Value)

	// Compaction reads the manifest with a consistent read of its own; no features configured, so
	// it only consumes the validation read above.
	client.AssertExpectations(s.T())
}

// Test_Commit_RefusesUnsupportedFeature asserts the first write against a manifest declaring a
// pair this writer does not model refuses before anything is written.
func (s *FeatureMarkerSuite) Test_Commit_RefusesUnsupportedFeature() {
	client := dynamo_mock.NewClient()
	repo := dynamo.NewRepository(client, "test-events").
		WithFeatureMarker(dynamo.NewFeatureMarker(client, "test-events", nil))

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
		WithFeatureMarker(dynamo.NewFeatureMarker(client, "test-events", nil))

	// First commit validates an empty table and succeeds.
	expectManifest(client, nil)

	var first dynamodb.TransactWriteItemsInput
	expectCommit(client, &first)
	require.NoError(s.T(), s.commit(repo, "acct-1"))

	// A declaration lands between the commits. The next commit's cached "no marker" condition
	// fails; the re-validation reads the manifest and refuses the unsupported pair.
	expectManifest(client, markerItem(1, `{"journal":1}`, "", ""))
	expectCommitFailure(client)

	err := s.commit(repo, "acct-2")
	require.Error(s.T(), err)
	require.True(s.T(), evt.IsUnsupportedFeatureErr(err), "expected UnsupportedFeatureError, got %v", err)
}

// Test_Commit_CheckFailure_SupportedChangeIsTransient asserts a failed per-commit check against a
// manifest the writer still supports re-validates, caches the new version, and classifies the
// failure as transient so policy.ExecuteWithRetry retries.
func (s *FeatureMarkerSuite) Test_Commit_CheckFailure_SupportedChangeIsTransient() {
	client := dynamo_mock.NewClient()
	repo := dynamo.NewRepository(client, "test-events").
		WithFeatureMarker(dynamo.NewFeatureMarker(client, "test-events", evt.SupportedFeatures{"journal": 1}))

	expectManifest(client, markerItem(1, `{"journal":1}`, "", ""))

	var first dynamodb.TransactWriteItemsInput
	expectCommit(client, &first)
	require.NoError(s.T(), s.commit(repo, "acct-1"))

	// The manifest bumps to version 2 under the writer; the cached condition fails.
	expectManifest(client, markerItem(2, `{"journal":1}`, "", ""))
	expectCommitFailure(client)

	err := s.commit(repo, "acct-2")
	require.Error(s.T(), err)
	require.True(s.T(), errors.Is(err, evt.ErrFeatureManifestChanged), "expected ErrFeatureManifestChanged, got %v", err)
	require.True(s.T(), policy.IsTransient(err), "the changed-manifest failure must be transient")

	// The retry reuses the freshly cached version without another read.
	var retried dynamodb.TransactWriteItemsInput
	expectCommit(client, &retried)
	require.NoError(s.T(), s.commit(repo, "acct-2"))

	check := retried.TransactItems[0].ConditionCheck
	require.Equal(s.T(), "2", check.ExpressionAttributeValues[":v"].(*types.AttributeValueMemberN).Value)
	client.AssertExpectations(s.T())
}

// Test_Declare_CreatesAndBumpsVersion asserts the declaration protocol: a create writes version 1
// against attribute_not_exists(pk); an update bumps by exactly one against version = :observed.
func (s *FeatureMarkerSuite) Test_Declare_CreatesAndBumpsVersion() {
	client := dynamo_mock.NewClient()
	marker := dynamo.NewFeatureMarker(client, "test-events", evt.SupportedFeatures{"journal": 1})

	// Create.
	expectManifest(client, nil)

	var createdPtr *dynamodb.PutItemInput
	client.On("PutItem", mock.Anything, mock.Anything, mock.Anything).
		Return(new(dynamodb.PutItemOutput), nil).
		Once().
		Run(func(args mock.Arguments) {
			createdPtr = args.Get(1).(*dynamodb.PutItemInput)
		})

	manifest, err := marker.Declare(context.Background(), func(m *evt.FeatureManifest) {
		m.Features["journal"] = 1
	})
	require.NoError(s.T(), err)
	require.Equal(s.T(), int64(1), manifest.Version)
	require.Equal(s.T(), "attribute_not_exists(pk)", aws.ToString(createdPtr.ConditionExpression))
	require.Equal(s.T(), "1", createdPtr.Item["version"].(*types.AttributeValueMemberN).Value)
	require.Equal(s.T(), `{"journal":1}`, createdPtr.Item["features"].(*types.AttributeValueMemberS).Value)

	// Update: one declaration adds exactly one to the version and writes the merged sets.
	expectManifest(client, markerItem(1, `{"journal":1}`, "", ""))

	var updatedPtr *dynamodb.PutItemInput
	client.On("PutItem", mock.Anything, mock.Anything, mock.Anything).
		Return(new(dynamodb.PutItemOutput), nil).
		Once().
		Run(func(args mock.Arguments) {
			updatedPtr = args.Get(1).(*dynamodb.PutItemInput)
		})

	manifest, err = marker.Declare(context.Background(), func(m *evt.FeatureManifest) {
		m.Features["journal-compaction"] = 1
		m.HistoryDependent = []evt.EntityType{"audit-feed"}
	})
	require.NoError(s.T(), err)
	require.Equal(s.T(), int64(2), manifest.Version)
	require.Equal(s.T(), "version = :observed", aws.ToString(updatedPtr.ConditionExpression))
	require.Equal(s.T(), "1", updatedPtr.ExpressionAttributeValues[":observed"].(*types.AttributeValueMemberN).Value)
	require.Equal(s.T(), "2", updatedPtr.Item["version"].(*types.AttributeValueMemberN).Value)
	require.Equal(s.T(), `{"journal":1,"journal-compaction":1}`, updatedPtr.Item["features"].(*types.AttributeValueMemberS).Value)
	require.Equal(s.T(), `["audit-feed"]`, updatedPtr.Item["historyDependent"].(*types.AttributeValueMemberS).Value)
	require.NotContains(s.T(), updatedPtr.Item, "retention", "an empty set is omitted")

	client.AssertExpectations(s.T())
}

// A successful Declare must invalidate this instance's cached validation, even when it
// declares a feature the writer cannot use. Later set-only declarations preserve durable features.
func (s *FeatureMarkerSuite) Test_Declare_SameInstanceRevalidatesUnsupportedFeature() {
	ctx := context.Background()
	client := dynamo_mock.NewClient()
	marker := dynamo.NewFeatureMarker(client, "test-events", nil)
	repo := dynamo.NewRepository(client, "test-events").WithFeatureMarker(marker)

	expectManifest(client, nil)
	var first dynamodb.TransactWriteItemsInput
	expectCommit(client, &first)
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
		WithFeatureMarker(dynamo.NewFeatureMarker(client, "test-events", nil))

	// The snapshot read learns the entity's type.
	client.On("GetItem", mock.Anything, mock.MatchedBy(func(in *dynamodb.GetItemInput) bool {
		return in.Key["pk"].(*types.AttributeValueMemberS).Value == "acct-1"
	}), mock.Anything).Return(&dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "acct-1"},
		"sk":         &types.AttributeValueMemberN{Value: "0"},
		"entityType": &types.AttributeValueMemberS{Value: "session"},
		"sequence":   &types.AttributeValueMemberN{Value: "1"},
		"eventSeq":   &types.AttributeValueMemberN{Value: "5"},
		"payload":    &types.AttributeValueMemberS{Value: "{}"},
	}}, nil).Once()

	// The durable refusal reads the manifest.
	expectManifest(client, markerItem(4, "{}", `["session"]`, ""))

	deleted, err := repo.CompactBelow(context.Background(), "acct-1", 5)
	require.Error(s.T(), err)
	require.True(s.T(), evt.IsHistoryDependentProjectionErr(err), "expected HistoryDependentProjectionError, got %v", err)
	require.Equal(s.T(), 0, deleted)
	client.AssertNotCalled(s.T(), "TransactWriteItems", mock.Anything, mock.Anything, mock.Anything)
}

// Test_CompactBelow_ProceedsWithoutMarker asserts compaction proceeds when no manifest declares
// the entity's type.
func (s *FeatureMarkerSuite) Test_CompactBelow_ProceedsWithoutMarker() {
	client := dynamo_mock.NewClient()
	repo := dynamo.NewRepository(client, "test-events").
		WithFeatureMarker(dynamo.NewFeatureMarker(client, "test-events", nil))

	client.On("GetItem", mock.Anything, mock.MatchedBy(func(in *dynamodb.GetItemInput) bool {
		return in.Key["pk"].(*types.AttributeValueMemberS).Value == "acct-1"
	}), mock.Anything).Return(&dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "acct-1"},
		"sk":         &types.AttributeValueMemberN{Value: "0"},
		"entityType": &types.AttributeValueMemberS{Value: "account"},
		"sequence":   &types.AttributeValueMemberN{Value: "1"},
		"eventSeq":   &types.AttributeValueMemberN{Value: "5"},
		"payload":    &types.AttributeValueMemberS{Value: "{}"},
	}}, nil).Once()

	expectManifest(client, nil)

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
