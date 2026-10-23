package test

import (
	"context"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/dynamo"
	dynamo_mock "github.com/photon-grove/evt/go/backends/dynamo/internal/mock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// NamespacesSuite covers the C8 repository additions against the mocked DynamoDB client:
// latest(n) bounded newest-first reads, reserved-ID refusals on every write path, and the
// reserved-namespace skip in enumeration, heads, and compaction.
type NamespacesSuite struct {
	suite.Suite
}

// Run the namespaces suite
func Test_NamespacesSuite(t *testing.T) {
	suite.Run(t, new(NamespacesSuite))
}

func (s *NamespacesSuite) repo() (*dynamo_mock.Client, *dynamo.Repository) {
	client := dynamo_mock.NewClient()

	return client, dynamo.NewRepository(client, "test-events")
}

func nsEventItem(pk string, sk int64, withMetadata bool) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: pk},
		"sk":         &types.AttributeValueMemberN{Value: strconv.FormatInt(sk, 10)},
		"type":       &types.AttributeValueMemberS{Value: "test-event"},
		"entityType": &types.AttributeValueMemberS{Value: "test"},
		"version":    &types.AttributeValueMemberN{Value: "1"},
		"payload":    &types.AttributeValueMemberS{Value: "{}"},
	}

	if withMetadata {
		item["metadata"] = &types.AttributeValueMemberS{Value: `{"region":"us-west-2"}`}
	}

	return item
}

// Test_GetRecentEvents_NewestFirst asserts the bounded, newest-first read: one descending query
// with Limit=n, at most n events returned, snapshot excluded by the sk > 0 key condition.
func (s *NamespacesSuite) Test_GetRecentEvents_NewestFirst() {
	client, repo := s.repo()

	current := nsEventItem("acct-1", 3, true)
	current["metadata"] = &types.AttributeValueMemberS{Value: `{"commandId":"cmd-3","principalId":"user-1","causationId":"cmd-2","extensions":{"scope":"account"},"region":"us-west-2"}`}
	legacy := nsEventItem("acct-1", 2, true)
	legacy["metadata"] = &types.AttributeValueMemberS{Value: `{"commandId":"cmd-2","region":"us-west-2"}`}

	var input dynamodb.QueryInput
	client.On("Query", mock.Anything, mock.MatchedBy(func(in *dynamodb.QueryInput) bool {
		input = *in

		return true
	}), mock.Anything).
		Return(&dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{
			current,
			legacy,
		}}, nil).Once()

	recent, err := repo.GetRecentEvents(context.Background(), "acct-1", 2)
	require.NoError(s.T(), err)
	require.Len(s.T(), recent, 2)
	require.Equal(s.T(), evt.EventSequence(3), recent[0].Sequence)
	require.Equal(s.T(), evt.EventSequence(2), recent[1].Sequence)
	require.Equal(s.T(), evt.PrincipalID("user-1"), *recent[0].Metadata.PrincipalID)
	require.Equal(s.T(), evt.CommandID("cmd-2"), *recent[0].Metadata.CausationID)
	require.Equal(s.T(), map[string]string{"scope": "account"}, recent[0].Metadata.Extensions)
	// Reading legacy rows must not apply encode-time causation or extension defaults.
	require.Nil(s.T(), recent[1].Metadata.CausationID)
	require.Nil(s.T(), recent[1].Metadata.Extensions)

	require.Equal(s.T(), "pk = :pk AND sk > :sk", aws.ToString(input.KeyConditionExpression))
	require.True(s.T(), aws.ToBool(input.ScanIndexForward) == false)
	require.Equal(s.T(), int32(2), aws.ToInt32(input.Limit))
	require.Equal(s.T(), &types.AttributeValueMemberN{Value: "0"}, input.ExpressionAttributeValues[":sk"])
}

// Test_GetRecentEvents_PagesUntilLimit asserts the reader keeps paging when a page closes early
// (large items), collecting up to n items or until the stream runs out.
func (s *NamespacesSuite) Test_GetRecentEvents_PagesUntilLimit() {
	client, repo := s.repo()

	first := &dynamodb.QueryOutput{
		Items:            []map[string]types.AttributeValue{nsEventItem("acct-1", 4, true)},
		LastEvaluatedKey: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "acct-1"}},
	}
	second := &dynamodb.QueryOutput{
		Items: []map[string]types.AttributeValue{nsEventItem("acct-1", 3, true)},
	}

	client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(first, nil).Once()
	client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(second, nil).Once()

	recent, err := repo.GetRecentEvents(context.Background(), "acct-1", 2)
	require.NoError(s.T(), err)
	require.Len(s.T(), recent, 2)
	require.Equal(s.T(), evt.EventSequence(4), recent[0].Sequence)
	require.Equal(s.T(), evt.EventSequence(3), recent[1].Sequence)
}

// Test_GetRecentEvents_StopsAtLimit asserts the read stops once the window is full even when more
// pages remain.
func (s *NamespacesSuite) Test_GetRecentEvents_StopsAtLimit() {
	client, repo := s.repo()

	page := &dynamodb.QueryOutput{
		Items: []map[string]types.AttributeValue{
			nsEventItem("acct-1", 3, true),
			nsEventItem("acct-1", 2, true),
		},
		LastEvaluatedKey: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "acct-1"}},
	}

	client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(page, nil).Once()

	// The window is full after the first page, so no second request is made even though the page
	// carries a pagination key.
	recent, err := repo.GetRecentEvents(context.Background(), "acct-1", 2)
	require.NoError(s.T(), err)
	require.Len(s.T(), recent, 2)
	client.AssertNumberOfCalls(s.T(), "Query", 1)
}

// Test_GetRecentEvents_RejectsInvalidLimit asserts a limit below 1 fails before any request.
func (s *NamespacesSuite) Test_GetRecentEvents_RejectsInvalidLimit() {
	_, repo := s.repo()

	_, err := repo.GetRecentEvents(context.Background(), "acct-1", 0)
	require.Error(s.T(), err)
}

// Test_WritePaths_RefuseReservedEntityIds asserts every entity-addressed write path refuses a
// reserved entity ID before any request.
func (s *NamespacesSuite) Test_WritePaths_RefuseReservedEntityIds() {
	client, repo := s.repo()

	reserved := []string{"SYSTEM#acct-1", "command-outcome#cmd-1"}

	for _, id := range reserved {
		events := []evt.SerializedEvent{serializedEvent(id)}

		require.Error(s.T(), repo.Commit(context.Background(), evt.SerializedResult{Events: events}))
		require.Error(s.T(), repo.CommitWithSnapshot(
			context.Background(), evt.SerializedResult{Events: events}, "test", evt.EntityID(id), []byte("{}"), 1))

		require.Error(s.T(), repo.PutSnapshot(
			context.Background(), "test", evt.EntityID(id), []byte("{}"), 1, 1))

		compactor, ok := evt.Repository(repo).(evt.Compactor)
		require.True(s.T(), ok)
		_, err := compactor.CompactBelow(context.Background(), evt.EntityID(id), 1)
		require.Error(s.T(), err)
	}

	client.AssertNotCalled(s.T(), "TransactWriteItems", mock.Anything, mock.Anything, mock.Anything)
	client.AssertNotCalled(s.T(), "PutItem", mock.Anything, mock.Anything, mock.Anything)
	client.AssertNotCalled(s.T(), "BatchWriteItem", mock.Anything, mock.Anything, mock.Anything)
}

// Test_Scan_SkipsReservedNamespaces asserts enumeration skips engine rows: a command-outcome row
// (sk = 1, no metadata) and a SYSTEM# row never surface as events or entity IDs.
func (s *NamespacesSuite) Test_Scan_SkipsReservedNamespaces() {
	client, repo := s.repo()

	client.On("Scan", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.ScanOutput{Items: []map[string]types.AttributeValue{
			nsEventItem("acct-1", 1, true),
			// A command-outcome row carries no metadata; decoding it as an event would fail.
			{
				"pk":     &types.AttributeValueMemberS{Value: "command-outcome#cmd-1"},
				"sk":     &types.AttributeValueMemberN{Value: "1"},
				"status": &types.AttributeValueMemberS{Value: "accepted"},
				"digest": &types.AttributeValueMemberS{Value: "abc"},
			},
			// A pre-v0.9-style stream in the SYSTEM# namespace (sk = 1 here to prove the prefix
			// skip, not just the snapshot skip, is doing the work).
			{
				"pk":         &types.AttributeValueMemberS{Value: "SYSTEM#legacy"},
				"sk":         &types.AttributeValueMemberN{Value: "1"},
				"type":       &types.AttributeValueMemberS{Value: "legacy-event"},
				"entityType": &types.AttributeValueMemberS{Value: "legacy"},
				"version":    &types.AttributeValueMemberN{Value: "1"},
				"payload":    &types.AttributeValueMemberS{Value: "{}"},
				"metadata":   &types.AttributeValueMemberS{Value: `{"region":"us-west-2"}`},
			},
		}}, nil).Once()

	var events []evt.SerializedEvent
	for page := range repo.StreamAllEvents(context.Background(), evt.StreamFilter{}) {
		batch, err := page.Unwrap()
		require.NoError(s.T(), err)
		events = append(events, batch...)
	}

	require.Len(s.T(), events, 1)
	require.Equal(s.T(), evt.EntityID("acct-1"), events[0].EntityID)
}

// Test_Heads_SkipReservedNamespaces asserts the entity-heads scan ignores engine rows.
func (s *NamespacesSuite) Test_Heads_SkipReservedNamespaces() {
	client, repo := s.repo()

	client.On("Scan", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.ScanOutput{Items: []map[string]types.AttributeValue{
			nsEventItem("acct-1", 3, true),
			{
				"pk":     &types.AttributeValueMemberS{Value: "command-outcome#cmd-1"},
				"sk":     &types.AttributeValueMemberN{Value: "1"},
				"status": &types.AttributeValueMemberS{Value: "accepted"},
			},
		}}, nil).Once()

	heads, err := repo.StreamEntityHeads(context.Background(), "")
	require.NoError(s.T(), err)
	require.Len(s.T(), heads, 1)
	require.Contains(s.T(), heads, evt.EntityID("acct-1"))
}
