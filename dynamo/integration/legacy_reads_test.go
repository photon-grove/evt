//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/stretchr/testify/require"
)

func Test_LegacyReservedStreamsRemainReadable(t *testing.T) {
	ctx := context.Background()
	repo, _ := newIsolatedEventLog(ctx, t)
	commitOne(ctx, t, repo, "account", 0)
	commitOne(ctx, t, repo, "account", 1)
	require.NoError(t, repo.PutSnapshot(ctx, "test", "account", []byte(`{"value":"v"}`), 1, 1))
	rows, err := repoClient().Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String(repo.EventsTable),
		KeyConditionExpression:    aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: "account"}},
	})
	require.NoError(t, err)

	// The head item (sk = -1), the snapshot, and two events. Copying the head item into the
	// reserved partitions too shows it is neither an event nor a snapshot to any read.
	require.Len(t, rows.Items, 4)

	for _, id := range []evt.EntityID{"SYSTEM#account", "command-outcome#account"} {
		for _, item := range rows.Items {
			item["pk"] = &types.AttributeValueMemberS{Value: string(id)}
			_, err = repoClient().PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(repo.EventsTable), Item: item})
			require.NoError(t, err)
		}

		all, readErr := repo.GetEvents(ctx, id)
		require.NoError(t, readErr)
		require.Len(t, all, 2)
		require.Equal(t, id, all[0].EntityID)
		latest, readErr := repo.GetLatestEvents(ctx, id, 1)
		require.NoError(t, readErr)
		require.Len(t, latest, 1)
		require.Equal(t, evt.EventSequence(2), latest[0].Sequence)
		recent, readErr := repo.GetRecentEvents(ctx, id, 2)
		require.NoError(t, readErr)
		require.Len(t, recent, 2)
		require.Equal(t, evt.EventSequence(2), recent[0].Sequence)
		require.Equal(t, evt.EventSequence(1), recent[1].Sequence)
		snapshot, readErr := repo.GetSnapshot(ctx, id)
		require.NoError(t, readErr)
		require.NotNil(t, snapshot)
		require.Equal(t, id, snapshot.EntityID)
		require.Equal(t, evt.EventSequence(1), snapshot.EventSequence)
	}
}
